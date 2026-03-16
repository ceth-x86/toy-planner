package logical

import (
	"reflect"
	"testing"
)

func TestLogicalAggregate_ReferencedTables(t *testing.T) {
	node := &LogicalAggregate{
		GroupKeys: []string{"user_id"},
		Child:     &LogicalScan{TableName: "Orders"},
	}
	got := node.ReferencedTables()
	expected := []string{"Orders"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ReferencedTables() = %v, want %v", got, expected)
	}
}

func TestLogicalAggregate_ReferencedTables_NilChild(t *testing.T) {
	node := &LogicalAggregate{GroupKeys: []string{"id"}}
	if got := node.ReferencedTables(); got != nil {
		t.Errorf("Expected nil for nil child, got %v", got)
	}
}
