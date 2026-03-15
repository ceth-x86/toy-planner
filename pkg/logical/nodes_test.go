package logical

import (
	"reflect"
	"sort"
	"testing"
)

func TestReferencedTables(t *testing.T) {
	tests := []struct {
		name     string
		node     LogicalNode
		expected []string
	}{
		{
			name:     "Scan",
			node:     &LogicalScan{TableName: "Users"},
			expected: []string{"Users"},
		},
		{
			name: "Filter with one table",
			node: &LogicalFilter{
				Condition: "Users.id = 42",
			},
			expected: []string{"Users"},
		},
		{
			name: "Filter with join condition",
			node: &LogicalFilter{
				Condition: "Users.id = Orders.user_id",
			},
			expected: []string{"Users", "Orders"},
		},
		{
			name: "Join",
			node: &LogicalJoin{
				Left:  &LogicalScan{TableName: "Users"},
				Right: &LogicalScan{TableName: "Orders"},
			},
			expected: []string{"Users", "Orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.node.ReferencedTables()
			sort.Strings(got)
			sort.Strings(tt.expected)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ReferencedTables() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestContainsTable(t *testing.T) {
	tables := []string{"Users", "Orders"}
	if !ContainsTable(tables, "Users") {
		t.Error("Expected to find Users")
	}
	if ContainsTable(tables, "Items") {
		t.Error("Expected not to find Items")
	}
}

func TestAllTablesIn(t *testing.T) {
	superset := []string{"Users", "Orders", "Items"}
	subset := []string{"Users", "Items"}
	if !AllTablesIn(subset, superset) {
		t.Errorf("Expected %v to be in %v", subset, superset)
	}
	
	notSubset := []string{"Users", "Prices"}
	if AllTablesIn(notSubset, superset) {
		t.Errorf("Expected %v not to be in %v", notSubset, superset)
	}
}
