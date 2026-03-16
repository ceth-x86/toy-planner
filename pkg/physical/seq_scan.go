package physical

import (
	"fmt"
	"strings"
)

// SeqScan performs a full table scan.
type SeqScan struct {
	TableName string
	RowCount  int
}

func (n *SeqScan) Cost() float64 { return float64(n.RowCount) }

func (n *SeqScan) Rows() float64 { return float64(n.RowCount) }

func (n *SeqScan) String() string { return n.Explain(0) }

func (n *SeqScan) Explain(indentLevel int) string {
	indentStr := strings.Repeat("    ", indentLevel)
	return fmt.Sprintf("%s-> SeqScan on %s (Cost: %.1f, Rows: %.0f)",
		indentStr, n.TableName, n.Cost(), n.Rows())
}
