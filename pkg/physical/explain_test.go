package physical

import (
	"strings"
	"testing"
)

func TestExplainFormat_NestedLoopJoin(t *testing.T) {
	scanOrders := &SeqScan{TableName: "Orders", RowCount: 500}
	idxScanUsers := &IndexScan{TableName: "Users", IndexColumn: "id", Value: "42", TotalRows: 1000, Selectivity: 0.001}
	
	selection := &Selection{
		Condition:   "Users.id = 42",
		Child:       idxScanUsers,
		Selectivity: 1.0,
	}

	join := &NestedLoopJoin{
		Condition:       "Users.id = Orders.user_id",
		Left:            selection,
		Right:           scanOrders,
		JoinSelectivity: 0.001,
	}

	explainOutput := join.Explain(0)

	expectedLines := []string{
		"-> NestedLoopJoin (Cost: 510.0, Rows: 0)",
		"     Join Filter: Users.id = Orders.user_id",
		"    -> Selection (Cost: 10.0, Rows: 1)",
		"         Filter: Users.id = 42",
		"        -> IndexScan on Users (Cost: 10.0, Rows: 1)",
		"             Filter: id = 42",
		"    -> SeqScan on Orders (Cost: 500.0, Rows: 500)",
	}
	
	expected := strings.Join(expectedLines, "\n")
	if explainOutput != expected {
		t.Errorf("Explain output mismatch.\nExpected:\n%s\n\nGot:\n%s", expected, explainOutput)
	}
}

func TestExplainFormat_HashJoinAndSort(t *testing.T) {
	scanA := &SeqScan{TableName: "A", RowCount: 100}
	scanB := &SeqScan{TableName: "B", RowCount: 200}
	
	join := &HashJoin{
		Condition:       "A.id = B.id",
		Left:            scanA,
		Right:           scanB,
		JoinSelectivity: 0.1,
	}
	
	sort := &Sort{
		SortKey: "A.id",
		Child:   join,
	}

	explainOutput := sort.Explain(0)

	expectedLines := []string{
		"-> Sort (Cost: 22431.6, Rows: 2000)",
		"     Sort Key: A.id",
		"    -> HashJoin (Cost: 500.0, Rows: 2000)",
		"         Hash Cond: A.id = B.id",
		"        -> SeqScan on A (Cost: 100.0, Rows: 100)",
		"        -> SeqScan on B (Cost: 200.0, Rows: 200)",
	}
	
	expected := strings.Join(expectedLines, "\n")
	if explainOutput != expected {
		t.Errorf("Explain output mismatch.\nExpected:\n%s\n\nGot:\n%s", expected, explainOutput)
	}
}
