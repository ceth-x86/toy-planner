package logical

import (
	"reflect"
	"sort"
	"testing"
)

func TestLogicalJoin_ReferencedTables(t *testing.T) {
	node := &LogicalJoin{
		Left:  &LogicalScan{TableName: "Users"},
		Right: &LogicalScan{TableName: "Orders"},
	}
	got := node.ReferencedTables()
	expected := []string{"Users", "Orders"}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ReferencedTables() = %v, want %v", got, expected)
	}
}
