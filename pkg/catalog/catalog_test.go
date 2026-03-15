package catalog

import (
	"testing"
)

func TestCatalog_RegisterAndGet(t *testing.T) {
	cat := NewCatalog()
	
	meta := TableMetadata{
		Name:     "Users",
		RowCount: 100,
		Columns:  []string{"id", "name"},
		Indexes:  []string{"id"},
	}
	
	cat.RegisterTable(meta)
	
	retrieved, ok := cat.GetTable("Users")
	if !ok {
		t.Fatal("Expected to find table 'Users'")
	}
	
	if retrieved.Name != meta.Name || retrieved.RowCount != meta.RowCount {
		t.Errorf("Retrieved metadata mismatch. Got %+v, want %+v", retrieved, meta)
	}
}

func TestCatalog_GetNonExistent(t *testing.T) {
	cat := NewCatalog()
	
	_, ok := cat.GetTable("NonExistent")
	if ok {
		t.Error("Expected not to find 'NonExistent' table")
	}
}
