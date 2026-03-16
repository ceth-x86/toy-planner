package logical

import (
	"reflect"
	"sort"
	"testing"
)

func TestLogicalFilter_ReferencedTables(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		expected  []string
	}{
		{
			name:      "Filter with one table",
			condition: "Users.id = 42",
			expected:  []string{"Users"},
		},
		{
			name:      "Filter with join condition",
			condition: "Users.id = Orders.user_id",
			expected:  []string{"Users", "Orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &LogicalFilter{Condition: tt.condition}
			got := node.ReferencedTables()
			sort.Strings(got)
			sort.Strings(tt.expected)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ReferencedTables() = %v, want %v", got, tt.expected)
			}
		})
	}
}
