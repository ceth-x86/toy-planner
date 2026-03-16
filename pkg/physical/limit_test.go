package physical

import (
	"testing"
)

func TestLimitRows(t *testing.T) {
	scan := &SeqScan{RowCount: 100}
	
	// Case 1: Limit < child rows
	limit := &Limit{Limit: 10, Offset: 0, Child: scan}
	if limit.Rows() != 10 {
		t.Errorf("Expected 10 rows, got %f", limit.Rows())
	}
	
	// Case 2: Limit > child rows
	limitLarge := &Limit{Limit: 500, Offset: 0, Child: scan}
	if limitLarge.Rows() != 100 {
		t.Errorf("Expected 100 rows (child bound), got %f", limitLarge.Rows())
	}

	// Case 3: Offset < child rows
	limitOffset := &Limit{Limit: 10, Offset: 20, Child: scan}
	// 100 - 20 = 80 available, limit 10 -> 10 rows
	if limitOffset.Rows() != 10 {
		t.Errorf("Expected 10 rows with offset, got %f", limitOffset.Rows())
	}

	// Case 4: Offset > child rows
	limitOffsetLarge := &Limit{Limit: 10, Offset: 150, Child: scan}
	if limitOffsetLarge.Rows() != 0 {
		t.Errorf("Expected 0 rows when offset > child rows, got %f", limitOffsetLarge.Rows())
	}

	// Case 5: Offset + Limit > child rows
	limitBoth := &Limit{Limit: 50, Offset: 80, Child: scan}
	// 100 - 80 = 20 available, limit 50 -> 20 rows
	if limitBoth.Rows() != 20 {
		t.Errorf("Expected 20 rows when available < limit, got %f", limitBoth.Rows())
	}
}

func TestLimitExplain(t *testing.T) {
	scan := &SeqScan{TableName: "Users", RowCount: 100}
	limit := &Limit{Limit: 5, Offset: 0, Child: scan}
	
	want := "-> Limit (Cost: 100.0, Rows: 5)\n     Limit: 5, Offset: 0\n    -> SeqScan on Users (Cost: 100.0, Rows: 100)"
	if got := limit.Explain(0); got != want {
		t.Errorf("Explain mismatch.\nGot:\n%s\n\nWant:\n%s", got, want)
	}
}
