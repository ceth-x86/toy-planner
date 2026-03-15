package optimizer

import (
	"toy-optimizer/pkg/logical"
)

// PushdownPredicates moves LogicalFilters as deep as possible in the tree.
func PushdownPredicates(node logical.LogicalNode) logical.LogicalNode {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *logical.LogicalFilter:
		// Try to push down the filter
		return pushFilterDown(n)
	case *logical.LogicalJoin:
		// Recursively optimize children
		n.Left = PushdownPredicates(n.Left)
		n.Right = PushdownPredicates(n.Right)
		return n
	default:
		return n
	}
}

func pushFilterDown(f *logical.LogicalFilter) logical.LogicalNode {
	if f.Child == nil {
		return f
	}

	// Optimize child first
	f.Child = PushdownPredicates(f.Child)

	switch child := f.Child.(type) {
	case *logical.LogicalJoin:
		filterTables := f.ReferencedTables()
		leftTables := child.Left.ReferencedTables()
		rightTables := child.Right.ReferencedTables()

		// If all tables in filter are in the left child, push it left
		if logical.AllTablesIn(filterTables, leftTables) {
			child.Left = &logical.LogicalFilter{
				Condition: f.Condition,
				Child:     child.Left,
			}
			// Re-optimize the left child after pushdown
			child.Left = PushdownPredicates(child.Left)
			return child
		}

		// If all tables in filter are in the right child, push it right
		if logical.AllTablesIn(filterTables, rightTables) {
			child.Right = &logical.LogicalFilter{
				Condition: f.Condition,
				Child:     child.Right,
			}
			// Re-optimize the right child after pushdown
			child.Right = PushdownPredicates(child.Right)
			return child
		}

		// Otherwise, keep filter above join
		return f

	default:
		// Cannot push down further (e.g. above Scan or another Filter)
		return f
	}
}
