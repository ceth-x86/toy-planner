package optimizer

import (
	"testing"
	"toy-optimizer/pkg/logical"
)

func TestDeducePredicates(t *testing.T) {
	// Filter(Users.id = 42) -> Join(Users.id = Orders.user_id)
	// Should deduce: Filter(Orders.user_id = 42) -> Filter(Users.id = 42) -> Join
	
	users := &logical.LogicalScan{TableName: "Users"}
	orders := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      users,
		Right:     orders,
	}
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     join,
	}

	result := DeducePredicates(filter)
	
	// Verify structure: Filter(deduced) -> Filter(original) -> Join
	outerFilter, ok := result.(*logical.LogicalFilter)
	if !ok {
		t.Fatalf("Expected outer Filter node, got %T", result)
	}
	if outerFilter.Condition != "Orders.user_id = 42" {
		t.Errorf("Expected deduced condition 'Orders.user_id = 42', got %q", outerFilter.Condition)
	}

	innerFilter, ok := outerFilter.Child.(*logical.LogicalFilter)
	if !ok {
		t.Fatalf("Expected inner Filter node (original), got %T", outerFilter.Child)
	}
	if innerFilter.Condition != "Users.id = 42" {
		t.Errorf("Expected original condition 'Users.id = 42', got %q", innerFilter.Condition)
	}

	if _, ok := innerFilter.Child.(*logical.LogicalJoin); !ok {
		t.Errorf("Expected Join below filters, got %T", innerFilter.Child)
	}
}

func TestDeducePredicates_Immutability(t *testing.T) {
	scanU := &logical.LogicalScan{TableName: "Users"}
	scanO := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      scanU,
		Right:     scanO,
	}
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     join,
	}

	originalStr := filter.String()
	_ = DeducePredicates(filter)

	if filter.String() != originalStr {
		t.Errorf("DeducePredicates mutated the original tree!\nWant:\n%s\nGot:\n%s", originalStr, filter.String())
	}
}
