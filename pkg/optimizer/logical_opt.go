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
		// Scans are leaves, just return as is (scans are immutable anyway)
		return &logical.LogicalScan{TableName: n.TableName}

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

		// If all tables in filter are in the left child, push it left
		if logical.AllTablesIn(filterTables, leftTables) {
			// Create a NEW Join with a NEW Filter on the left side
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

		// If all tables in filter are in the right child, push it right
		if logical.AllTablesIn(filterTables, rightTables) {
			// Create a NEW Join with a NEW Filter on the right side
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

		// Otherwise, keep filter above join
		return f

	default:
		// Cannot push down further
		return f
	}
}
