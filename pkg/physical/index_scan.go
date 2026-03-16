package physical

import (
	"fmt"
	"math"
	"strings"
)

// IndexScan performs a scan using an index.
type IndexScan struct {
	TableName   string
	IndexColumn string
	Value       string
	TotalRows   int
	Selectivity float64
}

func (n *IndexScan) Cost() float64 {
	if n.TotalRows <= 1 {
		return 1.0
	}
	return math.Log2(float64(n.TotalRows))
}

func (n *IndexScan) Rows() float64 {
	return float64(n.TotalRows) * n.Selectivity
}

func (n *IndexScan) String() string { return n.Explain(0) }

func (n *IndexScan) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	line1 := fmt.Sprintf("%s-> IndexScan on %s (Cost: %.1f, Rows: %.0f)",
		indentStr, n.TableName, n.Cost(), n.Rows())
	line2 := fmt.Sprintf("%s%sFilter: %s = %s", indentStr, detailPrefix, n.IndexColumn, n.Value)
	return line1 + "\n" + line2
}
