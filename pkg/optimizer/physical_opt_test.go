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
		ColumnNDVs: map[string]int{"id": 1000},
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

	// Filter on non-indexed column -> should choose Selection over SeqScan
	f2 := &logical.LogicalFilter{
		Condition: "Users.name = 'Alice'",
		Child:     &logical.LogicalScan{TableName: "Users"},
	}
	plan2 := planner.CreatePhysicalPlan(f2)
	selection, ok := plan2.(*physical.Selection)
	if !ok {
		t.Errorf("Expected Selection node, got %T", plan2)
	} else if _, ok := selection.Child.(*physical.SeqScan); !ok {
		t.Errorf("Expected Selection to wrap SeqScan, got %T", selection.Child)
	}
}

func TestPhysicalPlanner_JoinOrdering(t *testing.T) {
	cat := catalog.NewCatalog()
	// Large table (Orders) and Small table (Users)
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 10, Indexes: []string{"id"}})
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 1000})
	
	planner := NewPhysicalPlanner(cat)

	// Join(Orders, Users)
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

func TestPhysicalPlanner_CardinalityEstimation(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:       "Users",
		RowCount:   1000,
		ColumnNDVs: map[string]int{"city": 10},
	})
	
	planner := NewPhysicalPlanner(cat)

	// Filter on low NDV column -> 1000 * (1/10) = 100 rows
	f := &logical.LogicalFilter{
		Condition: "Users.city = 'London'",
		Child:     &logical.LogicalScan{TableName: "Users"},
	}
	
	plan := planner.CreatePhysicalPlan(f)
	if plan.Rows() != 100 {
		t.Errorf("Expected 100 estimated rows, got %f", plan.Rows())
	}
}

func TestPhysicalPlanner_StackedFilters(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:       "Users",
		RowCount:   1000,
		ColumnNDVs: map[string]int{"city": 10, "gender": 2},
	})
	
	planner := NewPhysicalPlanner(cat)

	scan := &logical.LogicalScan{TableName: "Users"}
	filter2 := &logical.LogicalFilter{Condition: "Users.gender = 'F'", Child: scan}
	filter1 := &logical.LogicalFilter{Condition: "Users.city = 'London'", Child: filter2}
	
	plan := planner.CreatePhysicalPlan(filter1)
	if plan.Rows() != 50 {
		t.Errorf("Expected 50 estimated rows for stacked filters, got %f", plan.Rows())
	}
}

func TestPhysicalPlanner_ImmutableNodes(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:       "Users",
		RowCount:   1000,
		ColumnNDVs: map[string]int{"id": 1000},
	})
	cat.RegisterTable(catalog.TableMetadata{
		Name:       "Orders",
		RowCount:   1000,
		ColumnNDVs: map[string]int{"user_id": 1000},
	})
	
	planner := NewPhysicalPlanner(cat)

	usersScan := &logical.LogicalScan{TableName: "Users"}
	ordersScan := &logical.LogicalScan{TableName: "Orders"}
	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan,
		Right:     ordersScan,
	}
	filter := &logical.LogicalFilter{
		Condition: "Users.id = 1",
		Child:     join,
	}

	plan := planner.CreatePhysicalPlan(filter)
	selection, ok := plan.(*physical.Selection)
	if !ok {
		t.Fatalf("Expected top node to be Selection")
	}

	nlj, ok := selection.Child.(*physical.NestedLoopJoin)
	if !ok {
		t.Fatalf("Expected NestedLoopJoin")
	}

	if plan.Rows() != 1 {
		t.Errorf("Expected 1 final row, got %f", plan.Rows())
	}

	if nlj.Rows() != 1000 {
		t.Errorf("Join rows corrupted! Expected 1000, got %f", nlj.Rows())
	}
}

func TestPhysicalPlanner_MissingTablePanic(t *testing.T) {
	cat := catalog.NewCatalog()
	planner := NewPhysicalPlanner(cat)
	
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic on missing table")
		}
	}()
	
	scan := &logical.LogicalScan{TableName: "Unknown"}
	planner.CreatePhysicalPlan(scan)
}

func TestPhysicalPlanner_JoinSelectivityFallback(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 100})
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 1000})
	
	planner := NewPhysicalPlanner(cat)

	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      &logical.LogicalScan{TableName: "Users"},
		Right:     &logical.LogicalScan{TableName: "Orders"},
	}

	plan := planner.CreatePhysicalPlan(join)
	
	if plan.Rows() != 100 {
		t.Errorf("Expected fallback rows 100, got %f", plan.Rows())
	}
}

func TestPhysicalPlanner_FilterAboveJoin(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name: "Users", RowCount: 1000,
		ColumnNDVs: map[string]int{"id": 1000},
	})
	cat.RegisterTable(catalog.TableMetadata{
		Name: "Orders", RowCount: 5000,
		ColumnNDVs: map[string]int{"user_id": 1000, "total": 100},
	})

	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:  &logical.LogicalScan{TableName: "Users"},
		Right: &logical.LogicalScan{TableName: "Orders"},
	}
	filter := &logical.LogicalFilter{
		Condition: "Orders.total > 100", 
		Child: join,
	}

	planner := NewPhysicalPlanner(cat)
	plan := planner.CreatePhysicalPlan(filter)
	
	if plan.Rows() != 1650 {
		t.Errorf("Expected 1650 rows for filter above join, got %f", plan.Rows())
	}
}

func TestPhysicalPlanner_EdgeCases(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Empty", RowCount: 0})
	planner := NewPhysicalPlanner(cat)

	if planner.CreatePhysicalPlan(nil) != nil {
		t.Error("Expected nil for nil logical node")
	}

	scan := &logical.LogicalScan{TableName: "Empty"}
	plan := planner.CreatePhysicalPlan(scan)
	if plan.Cost() != 0 || plan.Rows() != 0 {
		t.Errorf("Empty table should have 0 cost and rows, got cost=%f rows=%f", plan.Cost(), plan.Rows())
	}
}
