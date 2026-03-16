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
