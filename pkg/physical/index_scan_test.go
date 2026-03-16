package physical

import (
	"math"
	"testing"
)

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

func TestIndexScanExplain(t *testing.T) {
	n := &IndexScan{TableName: "Users", IndexColumn: "id", Value: "1", TotalRows: 100, Selectivity: 0.01}
	want := "-> IndexScan on Users (Cost: 6.6, Rows: 1)\n     Filter: id = 1"
	if got := n.Explain(0); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
