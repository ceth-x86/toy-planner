package physical

import (
	"math"
	"testing"
)

func TestTopNSortCost(t *testing.T) {
	scan := &SeqScan{RowCount: 1000}
	limit := 10
	topN := &TopNSort{SortKey: "id", Limit: limit, Child: scan}
	
	// cost = 1000 + (1000 * log2(10)) = 1000 + (1000 * 3.32) = 4321.9
	expected := 1000.0 + (1000.0 * math.Log2(float64(limit)))
	if math.Abs(topN.Cost()-expected) > 0.1 {
		t.Errorf("Expected cost %f, got %f", expected, topN.Cost())
	}
}

func TestTopNSortExplain(t *testing.T) {
	scan := &SeqScan{TableName: "Users", RowCount: 100}
	topN := &TopNSort{SortKey: "id", Limit: 5, Child: scan}
	
	want := "-> TopNSort on id (Cost: 332.2, Rows: 5)\n     Limit: 5\n    -> SeqScan on Users (Cost: 100.0, Rows: 100)"
	if got := topN.Explain(0); got != want {
		t.Errorf("Explain mismatch.\nGot:\n%s\n\nWant:\n%s", got, want)
	}
}
