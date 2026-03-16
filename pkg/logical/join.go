package logical

import (
	"fmt"
	"strings"
)

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

func (n *LogicalJoin) ReferencedTables() []string {
	tables := make(map[string]struct{})
	if n.Left != nil {
		for _, t := range n.Left.ReferencedTables() {
			tables[t] = struct{}{}
		}
	}
	if n.Right != nil {
		for _, t := range n.Right.ReferencedTables() {
			tables[t] = struct{}{}
		}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

// Copy expects exactly 2 children (matching Children()).
func (n *LogicalJoin) Copy(children []LogicalNode) LogicalNode {
	return &LogicalJoin{
		Condition: n.Condition,
		Left:      children[0],
		Right:     children[1],
	}
}

func (n *LogicalJoin) Children() []LogicalNode {
	return []LogicalNode{n.Left, n.Right}
}

func (n *LogicalJoin) SubtreeTables() []string {
	return n.ReferencedTables()
}
