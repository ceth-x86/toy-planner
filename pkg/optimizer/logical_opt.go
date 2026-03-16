package optimizer

import (
	"toy-optimizer/pkg/logical"
)

// MapChildren applies a function to all children of a node and returns a new node.
func MapChildren(node logical.LogicalNode, fn func(logical.LogicalNode) logical.LogicalNode) logical.LogicalNode {
	if node == nil {
		return nil
	}
	children := node.Children()
	if len(children) == 0 {
		return node.Copy(nil)
	}
	newChildren := make([]logical.LogicalNode, len(children))
	for i, child := range children {
		newChildren[i] = fn(child)
	}
	return node.Copy(newChildren)
}

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

	default:
		return MapChildren(node, PushdownPredicates)
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
		// Note: We intentionally do NOT push filters below Limit,
		// because filtering after limiting changes which rows are selected.
		return f
	}
}
