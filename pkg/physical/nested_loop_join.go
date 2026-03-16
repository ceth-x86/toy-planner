package physical

import (
	"fmt"
	"strings"
)

// NestedLoopJoin implements a basic nested loop join.
type NestedLoopJoin struct {
	Condition       string
	Left            PhysicalNode
	Right           PhysicalNode
	JoinSelectivity float64
}

func (n *NestedLoopJoin) Cost() float64 {
	return n.Left.Cost() + (n.Left.Rows() * n.Right.Cost())
}

func (n *NestedLoopJoin) Rows() float64 {
	return (n.Left.Rows() * n.Right.Rows()) * n.JoinSelectivity
}

func (n *NestedLoopJoin) String() string { return n.Explain(0) }

func (n *NestedLoopJoin) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> NestedLoopJoin (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sJoin Filter: %s", indentStr, detailPrefix, n.Condition)
	// Children are assumed non-nil per PhysicalNode interface contract
	leftStr := n.Left.Explain(indentLevel + 1)
	rightStr := n.Right.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + leftStr + "\n" + rightStr
}
