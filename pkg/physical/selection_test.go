package physical

import (
	"testing"
)

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
