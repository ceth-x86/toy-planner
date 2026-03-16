package logical

import (
	"reflect"
	"testing"
)

func TestLogicalSort_ReferencedTables(t *testing.T) {
	node := &LogicalSort{
		SortKey: "name",
		Child:   &LogicalScan{TableName: "Users"},
	}
	got := node.ReferencedTables()
	expected := []string{"Users"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ReferencedTables() = %v, want %v", got, expected)
	}
}

func TestLogicalSort_ReferencedTables_NilChild(t *testing.T) {
	node := &LogicalSort{SortKey: "name"}
	if got := node.ReferencedTables(); got != nil {
		t.Errorf("Expected nil for nil child, got %v", got)
	}
}
