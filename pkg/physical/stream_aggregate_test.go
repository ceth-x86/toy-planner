package physical

import (
	"testing"
)

func TestStreamAggregateCost(t *testing.T) {
	scan := &SeqScan{RowCount: 1000}
	agg := &StreamAggregate{
		GroupKeys: []string{"city"},
		Child:     scan,
	}
	// cost = 1000 + 1000 = 2000
	if agg.Cost() != 2000 {
		t.Errorf("Expected cost 2000, got %f", agg.Cost())
	}
}

func TestStreamAggregateRows(t *testing.T) {
	scan := &SeqScan{RowCount: 1000}
	agg := &StreamAggregate{GroupKeys: []string{"city"}, Child: scan}
	// 1000 / 5 = 200
	if agg.Rows() != 200 {
		t.Errorf("Expected 200 rows, got %f", agg.Rows())
	}
}
