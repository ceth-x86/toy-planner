package optimizer

import (
	"fmt"
	"toy-optimizer/pkg/logical"
)

// DeducePredicates performs transitive closure on equality predicates.
// E.g., (A.id = B.id AND A.id = 42) -> adds B.id = 42
// It returns a NEW tree, leaving the original one intact.
//
// Limitation: only handles Filter(Join(...)) pattern at any nesting depth.
// Does NOT handle Filter separated from Join by intermediate operators (like Sort).
func DeducePredicates(node logical.LogicalNode) logical.LogicalNode {
	if node == nil {
		return nil
	}

	// For a toy model, we look for a Filter sitting above a Join.
	// This is where most deduction happens.
	filter, ok := node.(*logical.LogicalFilter)
	if !ok {
		// Recursively process children for other nodes
		return deduceRecursively(node)
	}

	join, ok := filter.Child.(*logical.LogicalJoin)
	if !ok {
		return deduceRecursively(node)
	}

	// Parse join condition: TableA.Col1 = TableB.Col2
	joinMatches := joinCondRe.FindStringSubmatch(join.Condition)
	if len(joinMatches) == 0 {
		return deduceRecursively(node)
	}

	t1, c1 := joinMatches[1], joinMatches[2]
	t2, c2 := joinMatches[3], joinMatches[4]

	// Parse filter condition: Table.Col = Value
	filterMatches := eqFilterRe.FindStringSubmatch(filter.Condition)
	if len(filterMatches) == 0 {
		return deduceRecursively(node)
	}

	ft, fc := filterMatches[1], filterMatches[2]
	fv := filterMatches[3]

	var deducedCondition string
	if ft == t1 && fc == c1 {
		// Filter matches left side of join -> deduce for right side
		deducedCondition = fmt.Sprintf("%s.%s = %s", t2, c2, fv)
	} else if ft == t2 && fc == c2 {
		// Filter matches right side of join -> deduce for left side
		deducedCondition = fmt.Sprintf("%s.%s = %s", t1, c1, fv)
	}

	if deducedCondition != "" {
		// Wrap the existing plan in a NEW Filter node.
		// PushdownPredicates will later move filters to their respective tables.
		return &logical.LogicalFilter{
			Condition: deducedCondition,
			Child: &logical.LogicalFilter{
				Condition: filter.Condition,
				Child:     deduceRecursively(join),
			},
		}
	}

	return deduceRecursively(node)
}

func deduceRecursively(node logical.LogicalNode) logical.LogicalNode {
	switch n := node.(type) {
	case *logical.LogicalFilter:
		return &logical.LogicalFilter{
			Condition: n.Condition,
			Child:     DeducePredicates(n.Child),
		}
	case *logical.LogicalJoin:
		return &logical.LogicalJoin{
			Condition: n.Condition,
			Left:      DeducePredicates(n.Left),
			Right:     DeducePredicates(n.Right),
		}
	case *logical.LogicalSort:
		return &logical.LogicalSort{
			SortKey: n.SortKey,
			Child:   DeducePredicates(n.Child),
		}
	case *logical.LogicalAggregate:
		return &logical.LogicalAggregate{
			GroupKeys: n.GroupKeys,
			AggFuncs:  n.AggFuncs,
			Child:     DeducePredicates(n.Child),
		}
	case *logical.LogicalLimit:
		return &logical.LogicalLimit{
			Limit:  n.Limit,
			Offset: n.Offset,
			Child:  DeducePredicates(n.Child),
		}
	case *logical.LogicalScan:
		return &logical.LogicalScan{
			TableName: n.TableName,
		}
	default:
		return n
	}
}
