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
	node := &IndexScan{TableName: "Users", TotalRows: 1024, Selectivity: 0.01}
	expected := math.Log2(1024)
	if node.Cost() != expected {
		t.Errorf("Expected cost %f, got %f", expected, node.Cost())
	}
	if node.Rows() != 10.24 {
		t.Errorf("Expected rows 10.24, got %f", node.Rows())
	}
}

func TestSelectionCostAndRows(t *testing.T) {
	scan := &SeqScan{TableName: "Users", RowCount: 1000}
	filter := &Selection{
		Condition:   "id = 1",
		Child:       scan,
		Selectivity: 0.1,
	}
	
	if filter.Cost() != 1000 {
		t.Errorf("Expected cost 1000 (inherited from scan), got %f", filter.Cost())
	}
	if filter.Rows() != 100 {
		t.Errorf("Expected 100 rows, got %f", filter.Rows())
	}
}

func TestNestedLoopJoinCost(t *testing.T) {
	left := &IndexScan{TotalRows: 1000, Selectivity: 0.001} // cost = ~10, rows = 1
	right := &SeqScan{RowCount: 500}                      // cost = 500
	
	join := &NestedLoopJoin{
		Condition:       "id = user_id",
		Left:            left,
		Right:           right,
		JoinSelectivity: 0.001,
	}
	
	expectedCost := left.Cost() + (left.Rows() * right.Cost())
	if join.Cost() != expectedCost {
		t.Errorf("Expected cost %f, got %f", expectedCost, join.Cost())
	}
}
