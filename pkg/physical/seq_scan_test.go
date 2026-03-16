package physical

import (
	"testing"
)

func TestSeqScanCost(t *testing.T) {
	node := &SeqScan{TableName: "Users", RowCount: 1000}
	if node.Cost() != 1000 {
		t.Errorf("Expected cost 1000, got %f", node.Cost())
	}
}

func TestSeqScanExplain(t *testing.T) {
	n := &SeqScan{TableName: "Users", RowCount: 1000}
	want := "-> SeqScan on Users (Cost: 1000.0, Rows: 1000)"
	if got := n.Explain(0); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
