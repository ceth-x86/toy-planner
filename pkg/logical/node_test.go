package logical

import (
	"testing"
)

func TestContainsTable(t *testing.T) {
	tables := []string{"Users", "Orders"}
	if !ContainsTable(tables, "Users") {
		t.Error("Expected to find Users")
	}
	if ContainsTable(tables, "Items") {
		t.Error("Expected not to find Items")
	}
}

func TestAllTablesIn(t *testing.T) {
	superset := []string{"Users", "Orders", "Items"}
	subset := []string{"Users", "Items"}
	if !AllTablesIn(subset, superset) {
		t.Errorf("Expected %v to be in %v", subset, superset)
	}
	
	notSubset := []string{"Users", "Prices"}
	if AllTablesIn(notSubset, superset) {
		t.Errorf("Expected %v not to be in %v", notSubset, superset)
	}
}
