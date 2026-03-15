package optimizer

import (
	"testing"
	"toy-optimizer/pkg/logical"
)

func TestPushdownPredicates(t *testing.T) {
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

	joinResult, ok := optimized.(*logical.LogicalJoin)
	if !ok {
		t.Fatalf("Expected top node to be Join, got %T", optimized)
	}

	leftFilter, ok := joinResult.Left.(*logical.LogicalFilter)
	if !ok {
		t.Errorf("Expected left child to be Filter, got %T", joinResult.Left)
	} else if leftFilter.Condition != "Users.id = 42" {
		t.Errorf("Expected filter condition 'Users.id = 42', got '%s'", leftFilter.Condition)
	}
}

func TestPushdownPredicatesRight(t *testing.T) {
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

	_, ok = joinResult.Right.(*logical.LogicalFilter)
	if !ok {
		t.Errorf("Expected right child to be Filter")
	}
}

func TestLogicalOptimizer_Immutability(t *testing.T) {
	scanU := &logical.LogicalScan{TableName: "Users"}
	scanO := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      scanU,
		Right:     scanO,
	}
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 1",
		Child:     join,
	}

	originalStr := filter.String()
	optimized := PushdownPredicates(filter)

	if filter == optimized {
		t.Error("Optimized node is the same object as the original node")
	}

	if filter.String() != originalStr {
		t.Errorf("Original tree was mutated!")
	}
}

func TestPushdownPredicates_NestedJoins(t *testing.T) {
	users := &logical.LogicalScan{TableName: "Users"}
	orders := &logical.LogicalScan{TableName: "Orders"}
	items := &logical.LogicalScan{TableName: "Items"}
	
	join1 := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      users,
		Right:     orders,
	}
	
	join2 := &logical.LogicalJoin{
		Condition: "Orders.id = Items.order_id",
		Left:      join1,
		Right:     items,
	}
	
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 1",
		Child:     join2,
	}

	optimized := PushdownPredicates(filter)
	
	j2, ok := optimized.(*logical.LogicalJoin)
	if !ok || j2.Condition != "Orders.id = Items.order_id" {
		t.Fatalf("Expected top to be Join2, got %s", optimized.String())
	}
	
	j1, ok := j2.Left.(*logical.LogicalJoin)
	if !ok || j1.Condition != "Users.id = Orders.user_id" {
		t.Fatalf("Expected left of J2 to be Join1")
	}
	
	f, ok := j1.Left.(*logical.LogicalFilter)
	if !ok || f.Condition != "Users.id = 1" {
		t.Errorf("Filter didn't push down to the leaf Scan! Got: %s", j1.Left.String())
	}
}
