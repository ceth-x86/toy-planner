package physical

import (
	"testing"
)

func TestHashAggregateCost(t *testing.T) {
	scan := &SeqScan{RowCount: 1000}
	agg := &HashAggregate{
		GroupKeys: []string{"city"},
		Child:     scan,
	}
	// cost = 1000 + (1000 * 1.5) = 2500
	if agg.Cost() != 2500 {
		t.Errorf("Expected cost 2500, got %f", agg.Cost())
	}
}

func TestHashAggregateRows(t *testing.T) {
	scan := &SeqScan{RowCount: 1000}
	agg := &HashAggregate{GroupKeys: []string{"city"}, Child: scan}
	// 1000 / 5 = 200
	if agg.Rows() != 200 {
		t.Errorf("Expected 200 rows, got %f", agg.Rows())
	}
}

func TestHashAggregateExplain(t *testing.T) {
	scan := &SeqScan{TableName: "Orders", RowCount: 1000}
	agg := &HashAggregate{
		GroupKeys: []string{"user_id"},
		AggFuncs:  map[string]string{"total": "SUM", "id": "COUNT"},
		Child:     scan,
	}
	
	want := "-> HashAggregate (Cost: 2500.0, Rows: 200)\n     Group Key: [user_id]\n     Agg Funcs: [COUNT(id) SUM(total)]\n    -> SeqScan on Orders (Cost: 1000.0, Rows: 1000)"
	if got := agg.Explain(0); got != want {
		t.Errorf("Explain mismatch.\nGot:\n%s\n\nWant:\n%s", got, want)
	}
}
