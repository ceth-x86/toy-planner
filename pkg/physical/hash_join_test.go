package physical

import (
	"testing"
)

func TestHashJoinCost(t *testing.T) {
	left := &SeqScan{RowCount: 100}
	right := &SeqScan{RowCount: 500}
	join := &HashJoin{
		Condition:       "A.id = B.id",
		Left:            left,
		Right:           right,
		JoinSelectivity: 0.01,
	}
	
	// cost = 100 + 500 + (100 * 2) = 800 (Left is build side)
	if join.Cost() != 800 {
		t.Errorf("Expected cost 800, got %f", join.Cost())
	}

	joinSwapped := &HashJoin{
		Condition:       "A.id = B.id",
		Left:            right,
		Right:           left,
		JoinSelectivity: 0.01,
	}
	// cost = 500 + 100 + (500 * 2) = 1600 (Right side is larger, higher cost if on the left)
	if joinSwapped.Cost() != 1600 {
		t.Errorf("Expected cost 1600 for swapped build side, got %f", joinSwapped.Cost())
	}
}

func TestHashJoinRows(t *testing.T) {
	left := &SeqScan{RowCount: 100}
	right := &SeqScan{RowCount: 500}
	join := &HashJoin{
		Left: left, Right: right,
		JoinSelectivity: 0.01,
	}
	// 100 * 500 * 0.01 = 500
	if join.Rows() != 500 {
		t.Errorf("Expected 500 rows, got %f", join.Rows())
	}
}

func TestHashJoinExplain(t *testing.T) {
	left := &SeqScan{TableName: "A", RowCount: 100}
	right := &SeqScan{TableName: "B", RowCount: 200}
	join := &HashJoin{
		Condition:       "A.id = B.id",
		Left:            left,
		Right:           right,
		JoinSelectivity: 0.1,
	}
	
	want := "-> HashJoin (Cost: 500.0, Rows: 2000)\n     Hash Cond: A.id = B.id\n    -> SeqScan on A (Cost: 100.0, Rows: 100)\n    -> SeqScan on B (Cost: 200.0, Rows: 200)"
	if got := join.Explain(0); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
