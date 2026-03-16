package physical

import (
	"testing"
)

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
