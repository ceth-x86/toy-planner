package physical

import (
	"fmt"
	"strings"
)

// StreamAggregate processes data row-by-row.
// Strictly requires the incoming data to be pre-sorted by group_keys.
type StreamAggregate struct {
	GroupKeys []string
	AggFuncs  map[string]string
	Child     PhysicalNode
}

func (n *StreamAggregate) Cost() float64 {
	// Cost = child.Cost + rows(child) (very low overhead)
	return n.Child.Cost() + n.Child.Rows()
}

func (n *StreamAggregate) Rows() float64 {
	// TODO: use NDV of GroupKeys from catalog for accurate estimate.
	rows := n.Child.Rows() / 5.0
	if rows < 1 {
		return 1.0
	}
	return rows
}

func (n *StreamAggregate) String() string { return n.Explain(0) }

func (n *StreamAggregate) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> StreamAggregate (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sGroup Key: %v", indentStr, detailPrefix, n.GroupKeys)
	line3 := fmt.Sprintf("%s%sAgg Funcs: %s", indentStr, detailPrefix, FormatAggFuncs(n.AggFuncs))
	// Child is assumed non-nil per PhysicalNode interface contract
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + line3 + "\n" + childStr
}
