package logical

import (
	"regexp"
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
