package physical

import (
	"testing"
)

func TestSortCost(t *testing.T) {
	scan := &SeqScan{RowCount: 1024}
	sort := &Sort{SortKey: "id", Child: scan}
	
	// cost = 1024 + (1024 * log2(1024)) = 1024 + 10240 = 11264
	if sort.Cost() != 11264 {
		t.Errorf("Expected cost 11264, got %f", sort.Cost())
	}
}

func TestSortRows(t *testing.T) {
	scan := &SeqScan{RowCount: 100}
	sort := &Sort{SortKey: "id", Child: scan}
	if sort.Rows() != 100 {
		t.Errorf("Expected sort to pass through 100 rows, got %f", sort.Rows())
	}
}

func TestSortExplain(t *testing.T) {
	scan := &SeqScan{TableName: "Users", RowCount: 100}
	sort := &Sort{SortKey: "id", Child: scan}
	
	want := "-> Sort (Cost: 764.4, Rows: 100)\n     Sort Key: id\n    -> SeqScan on Users (Cost: 100.0, Rows: 100)"
	if got := sort.Explain(0); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
