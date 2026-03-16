package logical

import (
	"fmt"
	"strings"
)

// LogicalLimit represents a request to limit the number of output rows.
type LogicalLimit struct {
	Limit  int
	Offset int
	Child  LogicalNode
}

func (n *LogicalLimit) String() string {
	return n.ToStringIndent(0)
}

func (n *LogicalLimit) ToStringIndent(indent int) string {
	childStr := "nil"
	if n.Child != nil {
		childStr = n.Child.ToStringIndent(indent + 1)
	}
	return fmt.Sprintf("%sLimit(%d, offset %d)\n%s",
		strings.Repeat("  ", indent), n.Limit, n.Offset, childStr)
}

func (n *LogicalLimit) ReferencedTables() []string {
	if n.Child == nil {
		return nil
	}
	return n.Child.ReferencedTables()
}

// Copy expects exactly 1 child (matching Children()).
func (n *LogicalLimit) Copy(children []LogicalNode) LogicalNode {
	return &LogicalLimit{
		Limit:  n.Limit,
		Offset: n.Offset,
		Child:  children[0],
	}
}

func (n *LogicalLimit) Children() []LogicalNode {
	return []LogicalNode{n.Child}
}

func (n *LogicalLimit) SubtreeTables() []string {
	return n.ReferencedTables()
}
