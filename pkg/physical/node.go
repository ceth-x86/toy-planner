package physical

import (
	"fmt"
	"sort"
)

// PhysicalNode represents an execution strategy.
// Implementations may assume non-nil children in Cost() and Rows() methods;
// the planning phase must guarantee this invariant.
type PhysicalNode interface {
	fmt.Stringer
	// Cost returns the estimated cost of executing this node and its children.
	Cost() float64
	// Rows returns the estimated number of rows produced by this node.
	Rows() float64
	// Explain returns a human-readable text representation of the plan tree.
	// Use indentLevel=0 for the root node.
	Explain(indentLevel int) string
}

const detailPrefix = "     "

// FormatAggFuncs returns a deterministic sorted representation of aggregate functions.
func FormatAggFuncs(aggFuncs map[string]string) string {
	aggs := make([]string, 0, len(aggFuncs))
	for col, f := range aggFuncs {
		aggs = append(aggs, fmt.Sprintf("%s(%s)", f, col))
	}
	sort.Strings(aggs)
	return fmt.Sprintf("%v", aggs)
}
