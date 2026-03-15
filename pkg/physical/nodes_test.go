package physical

import (
	"math"
	"strings"
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

func TestSeqScanExplain(t *testing.T) {
	n := &SeqScan{TableName: "Users", RowCount: 1000}
	want := "-> SeqScan on Users (Cost: 1000.0, Rows: 1000)"
	if got := n.Explain(0); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestIndexScanExplain(t *testing.T) {
	n := &IndexScan{TableName: "Users", IndexColumn: "id", Value: "1", TotalRows: 100, Selectivity: 0.01}
	want := "-> IndexScan on Users (Cost: 6.6, Rows: 1)\n     Filter: id = 1"
	if got := n.Explain(0); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExplainFormat(t *testing.T) {
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
