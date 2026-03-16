package optimizer

import (
	"fmt"
	"math"
	"regexp"
	"toy-optimizer/pkg/catalog"
	"toy-optimizer/pkg/logical"
	"toy-optimizer/pkg/physical"
)

var (
	// eqFilterRe matches equality filters like "Table.Col = Value" but NOT "Table.Col = OtherTable.Col"
	eqFilterRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*([^.]+)$`)
	// joinCondRe matches join conditions like "TableA.Col1 = TableB.Col2"
	joinCondRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`)
)

// PhysicalPlanner converts a LogicalNode tree into an optimized PhysicalNode tree.
type PhysicalPlanner struct {
	catalog *catalog.Catalog
}

func NewPhysicalPlanner(cat *catalog.Catalog) *PhysicalPlanner {
	return &PhysicalPlanner{catalog: cat}
}

// CreatePhysicalPlan recursively builds the cheapest physical plan.
// Returns an error if a table or column referenced in the logical node is missing from the catalog.
func (p *PhysicalPlanner) CreatePhysicalPlan(node logical.LogicalNode) (physical.PhysicalNode, error) {
	if node == nil {
		return nil, nil
	}

	switch n := node.(type) {
	case *logical.LogicalFilter:
		// Attempt to merge with a Scan into an IndexScan
		if scan, ok := n.Child.(*logical.LogicalScan); ok {
			return p.planScanWithFilter(n, scan)
		}

		// General filter application
		childPhys, err := p.CreatePhysicalPlan(n.Child)
		if err != nil {
			return nil, err
		}
		selectivity := CalculateSelectivity(n.Condition, p.catalog)
		return &physical.Selection{
			Condition:   n.Condition,
			Child:       childPhys,
			Selectivity: selectivity,
		}, nil

	case *logical.LogicalJoin:
		return p.planJoin(n)

	case *logical.LogicalScan:
		meta, ok := p.catalog.GetTable(n.TableName)
		if !ok {
			return nil, fmt.Errorf("table %q not found in catalog", n.TableName)
		}
		return &physical.SeqScan{
			TableName: n.TableName,
			RowCount:  meta.RowCount,
		}, nil

	case *logical.LogicalSort:
		childPhys, err := p.CreatePhysicalPlan(n.Child)
		if err != nil {
			return nil, err
		}
		return &physical.Sort{
			SortKey: n.SortKey,
			Child:   childPhys,
		}, nil

	case *logical.LogicalAggregate:
		return p.planAggregate(n)

	case *logical.LogicalLimit:
		return p.planLimit(n)

	default:
		return nil, nil
	}
}

func (p *PhysicalPlanner) planScanWithFilter(f *logical.LogicalFilter, s *logical.LogicalScan) (physical.PhysicalNode, error) {
	meta, ok := p.catalog.GetTable(s.TableName)
	if !ok {
		return nil, fmt.Errorf("table %q not found in catalog", s.TableName)
	}

	selectivity := CalculateSelectivity(f.Condition, p.catalog)

	seqScan := &physical.SeqScan{
		TableName: s.TableName,
		RowCount:  meta.RowCount,
	}

	physSeq := &physical.Selection{
		Condition:   f.Condition,
		Child:       seqScan,
		Selectivity: selectivity,
	}

	matches := eqFilterRe.FindAllStringSubmatch(f.Condition, -1)

	if len(matches) > 0 {
		col := matches[0][2]
		hasIndex := false
		for _, idx := range meta.Indexes {
			if idx == col {
				hasIndex = true
				break
			}
		}

		if hasIndex {
			indexScan := &physical.IndexScan{
				TableName:   s.TableName,
				IndexColumn: col,
				Value:       matches[0][3],
				TotalRows:   meta.RowCount,
				Selectivity: selectivity,
			}

			if indexScan.Cost() < physSeq.Cost() {
				return indexScan, nil
			}
		}
	}
	return physSeq, nil
}

func (p *PhysicalPlanner) planJoin(j *logical.LogicalJoin) (physical.PhysicalNode, error) {
	leftPhys, err := p.CreatePhysicalPlan(j.Left)
	if err != nil {
		return nil, err
	}
	rightPhys, err := p.CreatePhysicalPlan(j.Right)
	if err != nil {
		return nil, err
	}

	selectivity, err := p.calculateJoinSelectivity(j.Condition, leftPhys.Rows(), rightPhys.Rows())
	if err != nil {
		return nil, err
	}

	// NLJ candidates: competitive when left side produces very few rows
	// (e.g., single-row index lookup); otherwise HashJoin dominates O(N+M).
	nlj1 := &physical.NestedLoopJoin{
		Condition:       j.Condition,
		Left:            leftPhys,
		Right:           rightPhys,
		JoinSelectivity: selectivity,
	}

	nlj2 := &physical.NestedLoopJoin{
		Condition:       j.Condition,
		Left:            rightPhys,
		Right:           leftPhys,
		JoinSelectivity: selectivity,
	}

	hj1 := &physical.HashJoin{
		Condition:       j.Condition,
		Left:            leftPhys,
		Right:           rightPhys,
		JoinSelectivity: selectivity,
	}

	hj2 := &physical.HashJoin{
		Condition:       j.Condition,
		Left:            rightPhys,
		Right:           leftPhys,
		JoinSelectivity: selectivity,
	}

	// Compare all candidates and pick the cheapest
	candidates := []physical.PhysicalNode{nlj1, nlj2, hj1, hj2}
	var winner physical.PhysicalNode = nlj1
	minCost := nlj1.Cost()

	for _, c := range candidates[1:] {
		if c.Cost() < minCost {
			minCost = c.Cost()
			winner = c
		}
	}

	return winner, nil
}

func (p *PhysicalPlanner) planAggregate(agg *logical.LogicalAggregate) (physical.PhysicalNode, error) {
	childPhys, err := p.CreatePhysicalPlan(agg.Child)
	if err != nil {
		return nil, err
	}

	hashAgg := &physical.HashAggregate{
		GroupKeys: agg.GroupKeys,
		AggFuncs:  agg.AggFuncs,
		Child:     childPhys,
	}

	isSorted := false
	if s, ok := childPhys.(*physical.Sort); ok && len(agg.GroupKeys) > 0 && s.SortKey == agg.GroupKeys[0] {
		isSorted = true
	}

	var streamAgg physical.PhysicalNode
	if isSorted {
		streamAgg = &physical.StreamAggregate{
			GroupKeys: agg.GroupKeys,
			AggFuncs:  agg.AggFuncs,
			Child:     childPhys,
		}
	} else {
		// Needs an explicit Sort node
		if len(agg.GroupKeys) == 0 {
			// Scalar aggregate (no GROUP BY)
			return hashAgg, nil
		}
		sortNode := &physical.Sort{
			SortKey: agg.GroupKeys[0],
			Child:   childPhys,
		}
		streamAgg = &physical.StreamAggregate{
			GroupKeys: agg.GroupKeys,
			AggFuncs:  agg.AggFuncs,
			Child:     sortNode,
		}
	}

	if hashAgg.Cost() <= streamAgg.Cost() {
		return hashAgg, nil
	}
	return streamAgg, nil
}

func (p *PhysicalPlanner) planLimit(limit *logical.LogicalLimit) (physical.PhysicalNode, error) {
	// Optimization: Top-N (Limit + Sort)
	if sortNode, ok := limit.Child.(*logical.LogicalSort); ok {
		childPhys, err := p.CreatePhysicalPlan(sortNode.Child)
		if err != nil {
			return nil, err
		}
		topN := &physical.TopNSort{
			SortKey: sortNode.SortKey,
			Limit:   limit.Limit + limit.Offset,
			Child:   childPhys,
		}
		// If there's an offset, we still need a Limit node to skip rows
		if limit.Offset > 0 {
			return &physical.Limit{
				Limit:  limit.Limit,
				Offset: limit.Offset,
				Child:  topN,
			}, nil
		}
		return topN, nil
	}

	childPhys, err := p.CreatePhysicalPlan(limit.Child)
	if err != nil {
		return nil, err
	}
	return &physical.Limit{
		Limit:  limit.Limit,
		Offset: limit.Offset,
		Child:  childPhys,
	}, nil
}

func (p *PhysicalPlanner) calculateJoinSelectivity(condition string, leftRows, rightRows float64) (float64, error) {
	matches := joinCondRe.FindStringSubmatch(condition)

	maxNDV := 0.0
	if len(matches) > 0 {
		t1, c1 := matches[1], matches[2]
		t2, c2 := matches[3], matches[4]

		meta1, ok1 := p.catalog.GetTable(t1)
		if !ok1 {
			return 0, fmt.Errorf("table %q (from join condition) not found in catalog", t1)
		}
		meta2, ok2 := p.catalog.GetTable(t2)
		if !ok2 {
			return 0, fmt.Errorf("table %q (from join condition) not found in catalog", t2)
		}

		ndv1 := float64(meta1.ColumnNDVs[c1])
		ndv2 := float64(meta2.ColumnNDVs[c2])

		if ndv1 > maxNDV {
			maxNDV = ndv1
		}
		if ndv2 > maxNDV {
			maxNDV = ndv2
		}
	}

	if maxNDV == 0 {
		maxNDV = math.Max(leftRows, rightRows)
		if maxNDV == 0 {
			return 1.0, nil
		}
	}

	return 1.0 / maxNDV, nil
}
