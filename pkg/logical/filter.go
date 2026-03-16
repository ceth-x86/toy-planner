package logical

import (
	"fmt"
	"strings"
)

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

func (n *LogicalFilter) ReferencedTables() []string {
	matches := tableColRe.FindAllStringSubmatch(n.Condition, -1)
	tables := make(map[string]struct{})
	for _, match := range matches {
		tables[match[1]] = struct{}{}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

func (n *LogicalFilter) SubtreeTables() []string {
	tables := make(map[string]struct{})
	for _, t := range n.ReferencedTables() {
		tables[t] = struct{}{}
	}
	if n.Child != nil {
		for _, t := range n.Child.SubtreeTables() {
			tables[t] = struct{}{}
		}
	}
	result := make([]string, 0, len(tables))
	for t := range tables {
		result = append(result, t)
	}
	return result
}

// Copy expects exactly 1 child (matching Children()).
func (n *LogicalFilter) Copy(children []LogicalNode) LogicalNode {
	return &LogicalFilter{
		Condition: n.Condition,
		Child:     children[0],
	}
}

func (n *LogicalFilter) Children() []LogicalNode {
	return []LogicalNode{n.Child}
}
