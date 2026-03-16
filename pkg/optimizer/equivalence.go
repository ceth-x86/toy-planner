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
		return MapChildren(node, DeducePredicates)
	}

	join, ok := filter.Child.(*logical.LogicalJoin)
	if !ok {
		return MapChildren(node, DeducePredicates)
	}

	// Parse join condition: TableA.Col1 = TableB.Col2
	joinMatches := joinCondRe.FindStringSubmatch(join.Condition)
	if len(joinMatches) == 0 {
		return MapChildren(node, DeducePredicates)
	}

	t1, c1 := joinMatches[1], joinMatches[2]
	t2, c2 := joinMatches[3], joinMatches[4]

	// Parse filter condition: Table.Col = Value
	filterMatches := eqFilterRe.FindStringSubmatch(filter.Condition)
	if len(filterMatches) == 0 {
		return MapChildren(node, DeducePredicates)
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
				Child:     MapChildren(join, DeducePredicates),
			},
		}
	}

	return MapChildren(node, DeducePredicates)
}
