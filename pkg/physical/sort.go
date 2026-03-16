package physical

import (
	"fmt"
	"math"
	"strings"
)

// Sort sorts the data stream by a key.
// Simplification: currently only supports a single sort key (column).
type Sort struct {
	SortKey string
	Child   PhysicalNode
}

func (n *Sort) Cost() float64 {
	// N * log2(N)
	rows := n.Child.Rows()
	sortCost := 0.0
	if rows > 1 { // guard: log2(0) is -Inf, log2(1) is 0
		sortCost = rows * math.Log2(rows)
	}
	return n.Child.Cost() + sortCost
}

func (n *Sort) Rows() float64 {
	return n.Child.Rows()
}

func (n *Sort) String() string { return n.Explain(0) }

func (n *Sort) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> Sort (Cost: %.1f, Rows: %.0f)",
		indentStr, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sSort Key: %s", indentStr, detailPrefix, n.SortKey)
	// Child is assumed non-nil per PhysicalNode interface contract
	childStr := n.Child.Explain(indentLevel + 1)
	return line1 + "\n" + line2 + "\n" + childStr
}
