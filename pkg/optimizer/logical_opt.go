package optimizer

import (
	"toy-optimizer/pkg/logical"
)

// PushdownPredicates moves LogicalFilters as deep as possible in the tree.
// It returns a NEW tree, leaving the original one intact.
func PushdownPredicates(node logical.LogicalNode) logical.LogicalNode {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *logical.LogicalFilter:
		// Create a new filter and try to push it down
		newFilter := &logical.LogicalFilter{
			Condition: n.Condition,
			Child:     PushdownPredicates(n.Child),
		}
		return pushFilterDown(newFilter)

	case *logical.LogicalJoin:
		// Return a new Join with optimized children
		return &logical.LogicalJoin{
			Condition: n.Condition,
			Left:      PushdownPredicates(n.Left),
			Right:     PushdownPredicates(n.Right),
		}

	case *logical.LogicalScan:
		// Scans are leaves, just return as is
		return &logical.LogicalScan{TableName: n.TableName}

	case *logical.LogicalSort:
		return &logical.LogicalSort{
			SortKey: n.SortKey,
			Child:   PushdownPredicates(n.Child),
		}

	case *logical.LogicalAggregate:
		return &logical.LogicalAggregate{
			GroupKeys: n.GroupKeys,
			AggFuncs:  n.AggFuncs,
			Child:     PushdownPredicates(n.Child),
		}

	default:
		return n
	}
}

func pushFilterDown(f *logical.LogicalFilter) logical.LogicalNode {
	if f.Child == nil {
		return f
	}

	switch child := f.Child.(type) {
	case *logical.LogicalJoin:
		filterTables := f.ReferencedTables()
		leftTables := child.Left.ReferencedTables()
		rightTables := child.Right.ReferencedTables()

		if logical.AllTablesIn(filterTables, leftTables) {
			newLeft := &logical.LogicalFilter{
				Condition: f.Condition,
				Child:     child.Left,
			}
			return &logical.LogicalJoin{
				Condition: child.Condition,
				Left:      PushdownPredicates(newLeft),
				Right:     child.Right,
			}
		}

		if logical.AllTablesIn(filterTables, rightTables) {
			newRight := &logical.LogicalFilter{
				Condition: f.Condition,
				Child:     child.Right,
			}
			return &logical.LogicalJoin{
				Condition: child.Condition,
				Left:      child.Left,
				Right:     PushdownPredicates(newRight),
			}
		}
		return f

	case *logical.LogicalSort:
		// Transformation: Filter(Sort(Child)) -> Sort(Filter(Child))
		newFilter := &logical.LogicalFilter{
			Condition: f.Condition,
			Child:     child.Child,
		}
		return &logical.LogicalSort{
			SortKey: child.SortKey,
			// Recursively push the filter further down past the sort
			Child:   PushdownPredicates(newFilter),
		}

	default:
		// Cannot push down further
		return f
	}
}
