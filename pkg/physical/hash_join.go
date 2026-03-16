package physical

import (
	"fmt"
	"strings"
)

// HashJoin builds a hash table from the left (build) side and probes with the right side.
type HashJoin struct {
	Condition       string
	Left            PhysicalNode
	Right           PhysicalNode
	JoinSelectivity float64
}

func (n *HashJoin) Cost() float64 {
	// Simplified model: ignores per-row probe cost.
	// In practice HashJoin will almost always beat NLJ for large inputs,
	// which mirrors real-world behavior.
	const hashBuildPenalty = 2.0
	return n.Left.Cost() + n.Right.Cost() + (n.Left.Rows() * hashBuildPenalty)
}

func (n *HashJoin) Rows() float64 {
	return (n.Left.Rows() * n.Right.Rows()) * n.JoinSelectivity
}

func (n *HashJoin) String() string { return n.Explain(0) }

func (n *HashJoin) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> HashJoin (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sHash Cond: %s", indentStr, detailPrefix, n.Condition)
	// Children are assumed non-nil per PhysicalNode interface contract
	leftStr := n.Left.Explain(indentLevel + 1)
	rightStr := n.Right.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + leftStr + "\n" + rightStr
}
