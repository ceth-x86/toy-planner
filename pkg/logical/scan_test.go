package logical

import (
	"reflect"
	"testing"
)

func TestLogicalScan_ReferencedTables(t *testing.T) {
	node := &LogicalScan{TableName: "Users"}
	got := node.ReferencedTables()
	expected := []string{"Users"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ReferencedTables() = %v, want %v", got, expected)
	}
}
