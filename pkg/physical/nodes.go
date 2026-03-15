package physical

import (
	"fmt"
	"math"
	"strings"
)

// PhysicalNode represents an execution strategy.
type PhysicalNode interface {
	fmt.Stringer
	ToStringIndent(indent int) string
	Cost() float64
	Rows() float64
}

// SeqScan performs a full table scan.
type SeqScan struct {
	TableName string
	RowCount  int
}

func (n *SeqScan) Cost() float64 { return float64(n.RowCount) }
func (n *SeqScan) Rows() float64 { return float64(n.RowCount) }
func (n *SeqScan) String() string { return n.ToStringIndent(0) }
func (n *SeqScan) ToStringIndent(indent int) string {
	return fmt.Sprintf("%sSeqScan(%s) [cost=%.1f rows=%.1f]",
		strings.Repeat("  ", indent), n.TableName, n.Cost(), n.Rows())
}

// IndexScan performs a scan using an index.
type IndexScan struct {
	TableName   string
	IndexColumn string
	Value       string
	TotalRows   int
	Selectivity float64 // Selective power of this index lookup
}

func (n *IndexScan) Cost() float64 {
	if n.TotalRows <= 1 { return 1.0 }
	return math.Log2(float64(n.TotalRows))
}
func (n *IndexScan) Rows() float64 {
	return float64(n.TotalRows) * n.Selectivity
}
func (n *IndexScan) String() string { return n.ToStringIndent(0) }
func (n *IndexScan) ToStringIndent(indent int) string {
	return fmt.Sprintf("%sIndexScan(%s.%s=%s) [cost=%.1f rows=%.1f]",
		strings.Repeat("  ", indent), n.TableName, n.IndexColumn, n.Value, n.Cost(), n.Rows())
}

// Selection represents a physical filter operator applied to a stream.
type Selection struct {
	Condition   string
	Child       PhysicalNode
	Selectivity float64
}

func (n *Selection) Cost() float64 { return n.Child.Cost() } // Filter overhead is often ignored in toy models
func (n *Selection) Rows() float64 { return n.Child.Rows() * n.Selectivity }
func (n *Selection) String() string { return n.ToStringIndent(0) }
func (n *Selection) ToStringIndent(indent int) string {
	childStr := n.Child.ToStringIndent(indent + 1)
	return fmt.Sprintf("%sSelection(%s) [cost=%.1f rows=%.1f]\n%s",
		strings.Repeat("  ", indent), n.Condition, n.Cost(), n.Rows(), childStr)
}

// NestedLoopJoin implements a basic nested loop join.
type NestedLoopJoin struct {
	Condition        string
	Left             PhysicalNode
	Right            PhysicalNode
	JoinSelectivity  float64
}

func (n *NestedLoopJoin) Cost() float64 {
	// Formula: cost(left) + (rows(left) * cost(right))
	return n.Left.Cost() + (n.Left.Rows() * n.Right.Cost())
}
func (n *NestedLoopJoin) Rows() float64 {
	return (n.Left.Rows() * n.Right.Rows()) * n.JoinSelectivity
}
func (n *NestedLoopJoin) String() string { return n.ToStringIndent(0) }
func (n *NestedLoopJoin) ToStringIndent(indent int) string {
	leftStr := n.Left.ToStringIndent(indent + 1)
	rightStr := n.Right.ToStringIndent(indent + 1)
	return fmt.Sprintf("%sNestedLoopJoin(%s) [cost=%.1f rows=%.1f]\n%s\n%s",
		strings.Repeat("  ", indent), n.Condition, n.Cost(), n.Rows(), leftStr, rightStr)
}
