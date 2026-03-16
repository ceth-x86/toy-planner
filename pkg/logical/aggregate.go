package logical

import (
	"fmt"
	"sort"
	"strings"
)

// LogicalAggregate represents a request to group data and calculate aggregates.
type LogicalAggregate struct {
	GroupKeys []string
	AggFuncs  map[string]string // e.g., {"total": "SUM"}
	Child     LogicalNode
}

func (n *LogicalAggregate) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalAggregate) ToStringIndent(indent int) string {
	childStr := "nil"
	if n.Child != nil {
		childStr = n.Child.ToStringIndent(indent + 1)
	}

	aggs := make([]string, 0, len(n.AggFuncs))
	for col, f := range n.AggFuncs {
		aggs = append(aggs, fmt.Sprintf("%s(%s)", f, col))
	}
	sort.Strings(aggs) // deterministic output

	return fmt.Sprintf("%sAggregate(by %v, funcs: %v)\n%s",
		strings.Repeat("  ", indent), n.GroupKeys, aggs, childStr)
}

func (n *LogicalAggregate) ReferencedTables() []string {
	if n.Child == nil {
		return nil
	}
	return n.Child.ReferencedTables()
}
