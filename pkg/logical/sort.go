package logical

import (
	"fmt"
	"strings"
)

// LogicalSort represents an ordering operation.
// Simplification: currently only supports a single sort key (column).
type LogicalSort struct {
	SortKey string
	Child   LogicalNode
}

func (n *LogicalSort) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalSort) ToStringIndent(indent int) string {
	childStr := "nil"
	if n.Child != nil {
		childStr = n.Child.ToStringIndent(indent + 1)
	}
	return fmt.Sprintf("%sSort(by %s)\n%s", strings.Repeat("  ", indent), n.SortKey, childStr)
}

func (n *LogicalSort) ReferencedTables() []string {
	if n.Child == nil {
		return nil
	}
	return n.Child.ReferencedTables()
}

// Copy expects exactly 1 child (matching Children()).
func (n *LogicalSort) Copy(children []LogicalNode) LogicalNode {
	return &LogicalSort{
		SortKey: n.SortKey,
		Child:   children[0],
	}
}

func (n *LogicalSort) Children() []LogicalNode {
	return []LogicalNode{n.Child}
}

func (n *LogicalSort) SubtreeTables() []string {
	return n.ReferencedTables()
}
