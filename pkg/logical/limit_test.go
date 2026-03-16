package logical

import (
	"reflect"
	"testing"
)

func TestLogicalLimit_ReferencedTables(t *testing.T) {
	node := &LogicalLimit{
		Limit: 10,
		Child: &LogicalScan{TableName: "Users"},
	}
	got := node.ReferencedTables()
	expected := []string{"Users"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ReferencedTables() = %v, want %v", got, expected)
	}
}
