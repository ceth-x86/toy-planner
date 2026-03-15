package optimizer

import (
	"testing"
	"toy-optimizer/pkg/catalog"
	"toy-optimizer/pkg/logical"
	"toy-optimizer/pkg/physical"
)

func TestPhysicalPlanner_ScanOptimization(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:      "Users",
		RowCount:  1000,
		Indexes:   []string{"id"},
	})
	
	planner := NewPhysicalPlanner(cat)

	// Filter on indexed column -> should choose IndexScan
	f := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     &logical.LogicalScan{TableName: "Users"},
	}
	
	plan := planner.CreatePhysicalPlan(f)
	if _, ok := plan.(*physical.IndexScan); !ok {
		t.Errorf("Expected IndexScan, got %T", plan)
	}

	// Filter on non-indexed column -> should choose SeqScan
	f2 := &logical.LogicalFilter{
		Condition: "Users.name = 'Alice'",
		Child:     &logical.LogicalScan{TableName: "Users"},
	}
	plan2 := planner.CreatePhysicalPlan(f2)
	if _, ok := plan2.(*physical.SeqScan); !ok {
		t.Errorf("Expected SeqScan, got %T", plan2)
	}
}

func TestPhysicalPlanner_JoinOrdering(t *testing.T) {
	cat := catalog.NewCatalog()
	// Large table (Orders) and Small table (Users)
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 10, Indexes: []string{"id"}})
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 1000})
	
	planner := NewPhysicalPlanner(cat)

	// Join(Orders, Users)
	// NLJ cost = cost(left) + (rows(left) * cost(right))
	// NLJ(Users, Orders) = 10 + (10 * 1000) = 10010
	// NLJ(Orders, Users) = 1000 + (1000 * 10) = 11000
	// Thus, NLJ(Users, Orders) should be chosen.
	
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      &logical.LogicalScan{TableName: "Orders"},
		Right:     &logical.LogicalScan{TableName: "Users"},
	}

	plan := planner.CreatePhysicalPlan(join)
	nlj, ok := plan.(*physical.NestedLoopJoin)
	if !ok {
		t.Fatalf("Expected NestedLoopJoin")
	}

	// Check if Users is on the left
	leftScan, ok := nlj.Left.(*physical.SeqScan)
	if !ok || leftScan.TableName != "Users" {
		t.Errorf("Expected Users on the left for cheaper join order, got %v", nlj.Left)
	}
}
