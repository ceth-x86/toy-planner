package logical

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// tableColRe extracts table and column names (e.g., "Users.id")
	tableColRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+`)
)

// LogicalNode represents a node in the logical query plan.
type LogicalNode interface {
	String() string
	ToStringIndent(indent int) string
	// ReferencedTables returns the tables involved in this node's subtree.
	ReferencedTables() []string
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

func (n *LogicalScan) ReferencedTables() []string {
	return []string{n.TableName}
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

// ContainsTable checks if a list of tables contains a specific target.
func ContainsTable(tables []string, target string) bool {
	for _, t := range tables {
		if t == target {
			return true
		}
	}
	return false
}

// AllTablesIn checks if all tables in 'subset' are present in 'superset'.
func AllTablesIn(subset, superset []string) bool {
	for _, s := range subset {
		if !ContainsTable(superset, s) {
			return false
		}
	}
	return true
}
