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
	// eqFilterRe matches equality filters like "Table.Col = Value"
	eqFilterRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*(.+)`)
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
func (p *PhysicalPlanner) CreatePhysicalPlan(node logical.LogicalNode) physical.PhysicalNode {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *logical.LogicalFilter:
		// Attempt to merge with a Scan into an IndexScan
		if scan, ok := n.Child.(*logical.LogicalScan); ok {
			return p.planScanWithFilter(n, scan)
		}

		// General filter application
		childPhys := p.CreatePhysicalPlan(n.Child)
		selectivity := CalculateSelectivity(n.Condition, p.catalog)
		return &physical.Selection{
			Condition:   n.Condition,
			Child:       childPhys,
			Selectivity: selectivity,
		}

	case *logical.LogicalJoin:
		return p.planJoin(n)

	case *logical.LogicalScan:
		meta, ok := p.catalog.GetTable(n.TableName)
		if !ok {
			panic(fmt.Sprintf("table %q not found in catalog", n.TableName))
		}
		return &physical.SeqScan{
			TableName: n.TableName,
			RowCount:  meta.RowCount,
		}

	case *logical.LogicalSort:
		childPhys := p.CreatePhysicalPlan(n.Child)
		return &physical.Sort{
			SortKey: n.SortKey,
			Child:   childPhys,
		}

	case *logical.LogicalAggregate:
		return p.planAggregate(n)

	case *logical.LogicalLimit:
		return p.planLimit(n)

	default:
		return nil
	}
}

func (p *PhysicalPlanner) planScanWithFilter(f *logical.LogicalFilter, s *logical.LogicalScan) physical.PhysicalNode {
	meta, ok := p.catalog.GetTable(s.TableName)
	if !ok {
		panic(fmt.Sprintf("table %q not found in catalog", s.TableName))
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
				return indexScan
			}
		}
	}
	return physSeq
}

func (p *PhysicalPlanner) planJoin(j *logical.LogicalJoin) physical.PhysicalNode {
	leftPhys := p.CreatePhysicalPlan(j.Left)
	rightPhys := p.CreatePhysicalPlan(j.Right)

	selectivity := p.calculateJoinSelectivity(j.Condition, leftPhys.Rows(), rightPhys.Rows())

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

	return winner
}

func (p *PhysicalPlanner) planAggregate(agg *logical.LogicalAggregate) physical.PhysicalNode {
	childPhys := p.CreatePhysicalPlan(agg.Child)

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
			return hashAgg
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
		return hashAgg
	}
	return streamAgg
}

func (p *PhysicalPlanner) planLimit(limit *logical.LogicalLimit) physical.PhysicalNode {
	// Optimization: Top-N (Limit + Sort)
	if sortNode, ok := limit.Child.(*logical.LogicalSort); ok {
		childPhys := p.CreatePhysicalPlan(sortNode.Child)
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
			}
		}
		return topN
	}

	childPhys := p.CreatePhysicalPlan(limit.Child)
	return &physical.Limit{
		Limit:  limit.Limit,
		Offset: limit.Offset,
		Child:  childPhys,
	}
}

func (p *PhysicalPlanner) calculateJoinSelectivity(condition string, leftRows, rightRows float64) float64 {
	matches := joinCondRe.FindStringSubmatch(condition)

	maxNDV := 0.0
	if len(matches) > 0 {
		t1, c1 := matches[1], matches[2]
		t2, c2 := matches[3], matches[4]

		meta1, ok1 := p.catalog.GetTable(t1)
		if !ok1 {
			panic(fmt.Sprintf("table %q (from join condition) not found in catalog", t1))
		}
		meta2, ok2 := p.catalog.GetTable(t2)
		if !ok2 {
			panic(fmt.Sprintf("table %q (from join condition) not found in catalog", t2))
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
			return 1.0
		}
	}

	return 1.0 / maxNDV
}
