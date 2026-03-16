package logical

import (
	"fmt"
	"strings"
)

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

func (n *LogicalScan) ReferencedTables() []string {
	return []string{n.TableName}
}

// Copy ignores children (Scan is a leaf node).
func (n *LogicalScan) Copy(children []LogicalNode) LogicalNode {
	return &LogicalScan{TableName: n.TableName}
}

func (n *LogicalScan) Children() []LogicalNode {
	return nil
}

func (n *LogicalScan) SubtreeTables() []string {
	return n.ReferencedTables()
}
