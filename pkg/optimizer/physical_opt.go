package optimizer

import (
	"regexp"
	"toy-optimizer/pkg/catalog"
	"toy-optimizer/pkg/logical"
	"toy-optimizer/pkg/physical"
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
		// Optimization: Check if this filter can be turned into an IndexScan
		if scan, ok := n.Child.(*logical.LogicalScan); ok {
			return p.planScanWithFilter(n, scan)
		}
		// Otherwise, wrap the child's physical plan (in this toy model, 
		// filters are often pushed down or merged into scans)
		return p.CreatePhysicalPlan(n.Child)

	case *logical.LogicalJoin:
		return p.planJoin(n)

	case *logical.LogicalScan:
		meta, _ := p.catalog.GetTable(n.TableName)
		return &physical.SeqScan{
			TableName: n.TableName,
			RowCount:  meta.RowCount,
		}

	default:
		return nil
	}
}

// planScanWithFilter chooses between SeqScan and IndexScan.
func (p *PhysicalPlanner) planScanWithFilter(f *logical.LogicalFilter, s *logical.LogicalScan) physical.PhysicalNode {
	meta, _ := p.catalog.GetTable(s.TableName)
	
	seqScan := &physical.SeqScan{
		TableName: s.TableName,
		RowCount:  meta.RowCount,
	}

	// Check if condition is "Table.Col = Value" and if Col has an index
	re := regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*(.+)`)
	matches := re.FindAllStringSubmatch(f.Condition, -1)
	
	if len(matches) > 0 {
		col := matches[0][2]
		val := matches[0][3]
		
		// Check if index exists for this column
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
				Value:       val,
				TotalRows:   meta.RowCount,
			}
			
			// Compare costs and pick the cheapest
			if indexScan.Cost() < seqScan.Cost() {
				return indexScan
			}
		}
	}

	return seqScan
}

// planJoin chooses the cheapest join order for NestedLoopJoin.
func (p *PhysicalPlanner) planJoin(j *logical.LogicalJoin) physical.PhysicalNode {
	// Recursively plan children
	leftPhys := p.CreatePhysicalPlan(j.Left)
	rightPhys := p.CreatePhysicalPlan(j.Right)

	// Try both orders: (A join B) and (B join A)
	order1 := &physical.NestedLoopJoin{
		Condition: j.Condition,
		Left:      leftPhys,
		Right:     rightPhys,
	}

	order2 := &physical.NestedLoopJoin{
		Condition: j.Condition,
		Left:      rightPhys,
		Right:     leftPhys,
	}

	if order1.Cost() <= order2.Cost() {
		return order1
	}
	return order2
}
