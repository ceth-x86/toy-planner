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

func (n *SeqScan) Cost() float64 {
	return float64(n.RowCount)
}

func (n *SeqScan) Rows() float64 {
	return float64(n.RowCount)
}

func (n *SeqScan) String() string {
	return n.ToStringIndent(0)
}

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
}

func (n *IndexScan) Cost() float64 {
	if n.TotalRows <= 1 {
		return 1.0
	}
	return math.Log2(float64(n.TotalRows))
}

func (n *IndexScan) Rows() float64 {
	// Assume index lookup returns 1 row for this toy model.
	return 1.0
}

func (n *IndexScan) String() string {
	return n.ToStringIndent(0)
}

func (n *IndexScan) ToStringIndent(indent int) string {
	return fmt.Sprintf("%sIndexScan(%s.%s=%s) [cost=%.1f rows=%.1f]",
		strings.Repeat("  ", indent), n.TableName, n.IndexColumn, n.Value, n.Cost(), n.Rows())
}

// NestedLoopJoin implements a basic nested loop join.
type NestedLoopJoin struct {
	Condition string
	Left      PhysicalNode
	Right     PhysicalNode
}

func (n *NestedLoopJoin) Cost() float64 {
	// Formula: cost(left) + (rows(left) * cost(right))
	return n.Left.Cost() + (n.Left.Rows() * n.Right.Cost())
}

func (n *NestedLoopJoin) Rows() float64 {
	// Assume join selectivity results in rows matching the left side.
	return n.Left.Rows()
}

func (n *NestedLoopJoin) String() string {
	return n.ToStringIndent(0)
}

func (n *NestedLoopJoin) ToStringIndent(indent int) string {
	leftStr := "nil"
	if n.Left != nil {
		leftStr = n.Left.ToStringIndent(indent + 1)
	}
	rightStr := "nil"
	if n.Right != nil {
		rightStr = n.Right.ToStringIndent(indent + 1)
	}
	return fmt.Sprintf("%sNestedLoopJoin(%s) [cost=%.1f rows=%.1f]\n%s\n%s",
		strings.Repeat("  ", indent), n.Condition, n.Cost(), n.Rows(), leftStr, rightStr)
}
