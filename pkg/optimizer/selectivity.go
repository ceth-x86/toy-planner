package optimizer

import (
	"regexp"
	"strings"
	"toy-optimizer/pkg/catalog"
)

var (
	// tableColBaseRe extracts table and column names (e.g., "Users.id")
	tableColBaseRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`)
	// ineqFilterRe matches inequality filters like "Table.Col > Value"
	ineqFilterRe = regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*(>|<|>=|<=|!=)\s*(.+)`)
)

// CalculateSelectivity estimates the fraction of rows that satisfy a condition.
// It uses the catalog to look up table statistics based on the condition string.
func CalculateSelectivity(condition string, cat *catalog.Catalog) float64 {
	// 1. Extract table and column name (e.g., "Users.id")
	matches := tableColBaseRe.FindStringSubmatch(condition)
	if len(matches) == 0 {
		return 1.0
	}

	tableName := matches[1]
	colName := matches[2]

	meta, ok := cat.GetTable(tableName)
	if !ok {
		return 1.0 // Table not found
	}

	// 2. Check for Inequality/Difference: !=
	if strings.Contains(condition, "!=") {
		ndv, ok := meta.ColumnNDVs[colName]
		if ok && ndv > 0 {
			return 1.0 - (1.0 / float64(ndv))
		}
		return 0.9 // Default for "NOT EQUAL"
	}

	// 3. Check for Range Inequality: >, <, >=, <=
	// Note: We check for >= and <= BEFORE = to avoid misclassification
	if strings.ContainsAny(condition, "><") {
		return 0.33 // Heuristic for range filters
	}

	// 4. Check for Equality: Table.Col = Value
	if strings.Contains(condition, "=") {
		// Ensure it's not a join condition (which has another Table.Col on the right)
		rightPart := strings.Split(condition, "=")[1]
		if !tableColBaseRe.MatchString(rightPart) {
			ndv, ok := meta.ColumnNDVs[colName]
			if ok && ndv > 0 {
				return 1.0 / float64(ndv)
			}
			return 0.1 // Default for equality
		}
	}

	return 1.0
}
