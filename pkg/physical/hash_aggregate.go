package physical

import (
	"fmt"
	"strings"
)

// HashAggregate uses a hash table to group rows and calculate aggregates.
type HashAggregate struct {
	GroupKeys []string
	AggFuncs  map[string]string
	Child     PhysicalNode
}

func (n *HashAggregate) Cost() float64 {
	// Cost = child.Cost + rows(child) * hash_penalty
	const hashPenalty = 1.5
	return n.Child.Cost() + (n.Child.Rows() * hashPenalty)
}

func (n *HashAggregate) Rows() float64 {
	// TODO: use NDV of GroupKeys from catalog for accurate estimate.
	// Current heuristic: assume grouping reduces rows by factor of 5.
	rows := n.Child.Rows() / 5.0
	if rows < 1 {
		return 1.0
	}
	return rows
}

func (n *HashAggregate) String() string { return n.Explain(0) }

func (n *HashAggregate) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> HashAggregate (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sGroup Key: %v", indentStr, detailPrefix, n.GroupKeys)
	line3 := fmt.Sprintf("%s%sAgg Funcs: %s", indentStr, detailPrefix, FormatAggFuncs(n.AggFuncs))
	// Child is assumed non-nil per PhysicalNode interface contract
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + line3 + "\n" + childStr
}
