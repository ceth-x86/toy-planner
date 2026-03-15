package physical

import (
	"fmt"
	"math"
	"strings"
)

// PhysicalNode represents an execution strategy.
type PhysicalNode interface {
	fmt.Stringer
	// Cost returns the estimated cost of executing this node and its children.
	Cost() float64
	// Rows returns the estimated number of rows produced by this node.
	Rows() float64
	// Explain returns a human-readable text representation of the plan tree.
	// Use indentLevel=0 for the root node.
	Explain(indentLevel int) string
}

const detailPrefix = "     "

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

// IndexScan performs a scan using an index.
type IndexScan struct {
	TableName   string
	IndexColumn string
	Value       string
	TotalRows   int
	Selectivity float64
}

func (n *IndexScan) Cost() float64 {
	if n.TotalRows <= 1 { return 1.0 }
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
	childStr := "nil"
	if n.Child != nil {
		childStr = n.Child.Explain(indentLevel + 1)
	}
	return line1 + "\n" + line2 + "\n" + childStr
}

// NestedLoopJoin implements a basic nested loop join.
type NestedLoopJoin struct {
	Condition        string
	Left             PhysicalNode
	Right            PhysicalNode
	JoinSelectivity  float64
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
	leftStr := "nil"
	if n.Left != nil {
		leftStr = n.Left.Explain(indentLevel + 1)
	}
	rightStr := "nil"
	if n.Right != nil {
		rightStr = n.Right.Explain(indentLevel + 1)
	}
	return line1 + "\n" + line2 + "\n" + leftStr + "\n" + rightStr
}
