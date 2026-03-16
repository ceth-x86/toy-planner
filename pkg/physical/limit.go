package physical

import (
	"fmt"
	"strings"
)

// Limit operator stops execution once the limit is reached.
type Limit struct {
	Limit  int
	Offset int
	Child  PhysicalNode
}

func (n *Limit) Cost() float64 {
	// TODO: with lazy evaluation, cost could be proportional to Limit rows,
	// not the full child cost.
	return n.Child.Cost()
}

func (n *Limit) Rows() float64 {
	childRows := n.Child.Rows()
	available := childRows - float64(n.Offset)
	if available <= 0 {
		return 0
	}
	limit := float64(n.Limit)
	if available < limit {
		return available
	}
	return limit
}

func (n *Limit) String() string { return n.Explain(0) }

func (n *Limit) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> Limit (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sLimit: %d, Offset: %d", indentStr, detailPrefix, n.Limit, n.Offset)
	// Child is assumed non-nil
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + childStr
}
