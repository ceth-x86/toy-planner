package optimizer

import (
	"testing"
	"toy-optimizer/pkg/logical"
)

func TestPushdownPredicates(t *testing.T) {
	// Original: Filter(Users.id = 42) -> Join -> [Scan(Users), Scan(Orders)]
	usersScan := &logical.LogicalScan{TableName: "Users"}
	ordersScan := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan,
		Right:     ordersScan,
	}
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     join,
	}

	optimized := PushdownPredicates(filter)

	// Check if top node is Join
	joinResult, ok := optimized.(*logical.LogicalJoin)
	if !ok {
		t.Fatalf("Expected top node to be Join, got %T", optimized)
	}

	// Check if Left child is Filter
	leftFilter, ok := joinResult.Left.(*logical.LogicalFilter)
	if !ok {
		t.Errorf("Expected left child to be Filter, got %T", joinResult.Left)
	} else if leftFilter.Condition != "Users.id = 42" {
		t.Errorf("Expected filter condition 'Users.id = 42', got '%s'", leftFilter.Condition)
	}

	// Check if Right child is still Scan(Orders)
	_, ok = joinResult.Right.(*logical.LogicalScan)
	if !ok {
		t.Errorf("Expected right child to be Scan, got %T", joinResult.Right)
	}
}

func TestPushdownPredicatesRight(t *testing.T) {
	// Filter that should go to the RIGHT child
	usersScan := &logical.LogicalScan{TableName: "Users"}
	ordersScan := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan,
		Right:     ordersScan,
	}
	filter := &logical.LogicalFilter{
		Condition: "Orders.total > 100",
		Child:     join,
	}

	optimized := PushdownPredicates(filter)

	joinResult, ok := optimized.(*logical.LogicalJoin)
	if !ok {
		t.Fatalf("Expected top node to be Join")
	}

	// Right child should be Filter
	_, ok = joinResult.Right.(*logical.LogicalFilter)
	if !ok {
		t.Errorf("Expected right child to be Filter")
	}
}
