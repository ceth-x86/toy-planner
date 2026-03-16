package physical

import (
	"fmt"
	"math"
	"strings"
)

// TopNSort is a specialized sort that keeps only the top K rows using a heap.
type TopNSort struct {
	SortKey string
	Limit   int
	Child   PhysicalNode
}

func (n *TopNSort) Cost() float64 {
	// Cost = child.Cost + rows(child) * log2(limit)
	// This is much cheaper than a full sort for small limits.
	rows := n.Child.Rows()
	if rows <= 1 || n.Limit <= 1 {
		return n.Child.Cost() + 1.0
	}
	return n.Child.Cost() + (rows * math.Log2(float64(n.Limit)))
}

func (n *TopNSort) Rows() float64 {
	childRows := n.Child.Rows()
	limit := float64(n.Limit)
	if childRows < limit {
		return childRows
	}
	return limit
}

func (n *TopNSort) String() string { return n.Explain(0) }

func (n *TopNSort) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> TopNSort on %s (Cost: %.1f, Rows: %.0f)",
		indentStr, n.SortKey, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sLimit: %d", indentStr, detailPrefix, n.Limit)
	// Child is assumed non-nil
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + childStr
}
