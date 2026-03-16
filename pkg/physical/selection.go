package physical

import (
	"fmt"
	"strings"
)

// Selection represents a physical filter operator applied to a stream.
type Selection struct {
	Condition   string
	Child       PhysicalNode
	Selectivity float64
}

func (n *Selection) Cost() float64 { return n.Child.Cost() }

func (n *Selection) Rows() float64 { return n.Child.Rows() * n.Selectivity }

func (n *Selection) String() string { return n.Explain(0) }

func (n *Selection) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> Selection (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sFilter: %s", indentStr, detailPrefix, n.Condition)
	// Child is assumed non-nil per PhysicalNode interface contract
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + childStr
}
