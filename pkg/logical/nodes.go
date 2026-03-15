package logical

import (
	"fmt"
	"strings"
)

// LogicalNode represents a node in the logical query plan.
type LogicalNode interface {
	String() string
	ToStringIndent(indent int) string
}

// LogicalScan represents a full scan of a table.
type LogicalScan struct {
	TableName string
}

func (n *LogicalScan) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalScan) ToStringIndent(indent int) string {
	return fmt.Sprintf("%sScan(%s)", strings.Repeat("  ", indent), n.TableName)
}

// LogicalFilter represents a filtering condition.
type LogicalFilter struct {
	Condition string
	Child     LogicalNode
}

func (n *LogicalFilter) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalFilter) ToStringIndent(indent int) string {
	childStr := "nil"
	if n.Child != nil {
		childStr = n.Child.ToStringIndent(indent + 1)
	}
	return fmt.Sprintf("%sFilter(%s)\n%s", strings.Repeat("  ", indent), n.Condition, childStr)
}

// LogicalJoin represents a join between two relations.
type LogicalJoin struct {
	Condition string
	Left      LogicalNode
	Right     LogicalNode
}

func (n *LogicalJoin) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalJoin) ToStringIndent(indent int) string {
	leftStr := "nil"
	if n.Left != nil {
		leftStr = n.Left.ToStringIndent(indent + 1)
	}
	rightStr := "nil"
	if n.Right != nil {
		rightStr = n.Right.ToStringIndent(indent + 1)
	}
	return fmt.Sprintf("%sJoin(%s)\n%s\n%s", strings.Repeat("  ", indent), n.Condition, leftStr, rightStr)
}
