package physical

import (
	"math"
	"testing"
)

func TestSeqScanCost(t *testing.T) {
	node := &SeqScan{TableName: "Users", RowCount: 1000}
	if node.Cost() != 1000 {
		t.Errorf("Expected cost 1000, got %f", node.Cost())
	}
}

func TestIndexScanCost(t *testing.T) {
	node := &IndexScan{TableName: "Users", TotalRows: 1024}
	expected := math.Log2(1024)
	if node.Cost() != expected {
		t.Errorf("Expected cost %f, got %f", expected, node.Cost())
	}
}

func TestNestedLoopJoinCost(t *testing.T) {
	left := &IndexScan{TotalRows: 1024} // cost = 10, rows = 1
	right := &SeqScan{RowCount: 500}   // cost = 500
	
	join := &NestedLoopJoin{
		Condition: "id = user_id",
		Left:      left,
		Right:     right,
	}
	
	expectedCost := 10 + (1 * 500.0)
	if join.Cost() != expectedCost {
		t.Errorf("Expected cost %f, got %f", expectedCost, join.Cost())
	}
}
