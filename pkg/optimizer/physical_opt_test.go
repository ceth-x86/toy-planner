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
	// Users (10 rows), Orders (1000 rows).
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 10, Indexes: []string{"id"}, ColumnNDVs: map[string]int{"id": 10}})
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 1000, ColumnNDVs: map[string]int{"user_id": 10}})
	
	planner := NewPhysicalPlanner(cat)

	join := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      &logical.LogicalScan{TableName: "Orders"},
		Right:     &logical.LogicalScan{TableName: "Users"},
	}

	plan := planner.CreatePhysicalPlan(join)
	
	hj, ok := plan.(*physical.HashJoin)
	if !ok {
		t.Fatalf("Expected HashJoin, got %T", plan)
	}
	
	// Users (10 rows) should be the build side (Left) for cheapest HashJoin
	leftScan, ok := hj.Left.(*physical.SeqScan)
	if !ok || leftScan.TableName != "Users" {
		t.Errorf("Expected Users as build side (Left), got %v", hj.Left)
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

	// Verify join exists and rows are correct
	if plan.Rows() != 1 {
		t.Errorf("Expected 1 final row, got %f", plan.Rows())
	}

	if selection.Child.Rows() != 1000 {
		t.Errorf("Join rows corrupted! Expected 1000, got %f", selection.Child.Rows())
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

func TestPhysicalPlanner_HashJoinChoice(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "BigA", RowCount: 10000, ColumnNDVs: map[string]int{"id": 10000}})
	cat.RegisterTable(catalog.TableMetadata{Name: "BigB", RowCount: 10000, ColumnNDVs: map[string]int{"id": 10000}})
	
	planner := NewPhysicalPlanner(cat)

	join := &logical.LogicalJoin{
		Condition: "BigA.id = BigB.id",
		Left:      &logical.LogicalScan{TableName: "BigA"},
		Right:     &logical.LogicalScan{TableName: "BigB"},
	}

	plan := planner.CreatePhysicalPlan(join)
	
	if _, ok := plan.(*physical.HashJoin); !ok {
		t.Errorf("Expected HashJoin for large unindexed tables, got %T", plan)
	}
}

func TestPhysicalPlanner_Sort(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 100})
	planner := NewPhysicalPlanner(cat)

	sort := &logical.LogicalSort{
		SortKey: "name",
		Child:   &logical.LogicalScan{TableName: "Users"},
	}

	plan := planner.CreatePhysicalPlan(sort)
	s, ok := plan.(*physical.Sort)
	if !ok {
		t.Fatalf("Expected Sort physical node")
	}
	if s.SortKey != "name" {
		t.Errorf("Expected sort key 'name', got %s", s.SortKey)
	}
}

func TestPhysicalPlanner_TopNWithOffset(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 100})
	planner := NewPhysicalPlanner(cat)

	// LIMIT 5 OFFSET 10 ORDER BY total
	limit := &logical.LogicalLimit{
		Limit:  5,
		Offset: 10,
		Child: &logical.LogicalSort{
			SortKey: "total",
			Child:   &logical.LogicalScan{TableName: "Orders"},
		},
	}

	plan := planner.CreatePhysicalPlan(limit)
	
	// Should be Limit -> TopNSort
	outerLimit, ok := plan.(*physical.Limit)
	if !ok {
		t.Fatalf("Expected outer Limit node for Offset, got %T", plan)
	}
	if outerLimit.Offset != 10 {
		t.Errorf("Expected offset 10, got %d", outerLimit.Offset)
	}

	topN, ok := outerLimit.Child.(*physical.TopNSort)
	if !ok {
		t.Fatalf("Expected child TopNSort, got %T", outerLimit.Child)
	}
	if topN.Limit != 15 {
		t.Errorf("Expected TopNSort limit to be 15 (5+10), got %d", topN.Limit)
	}
}

func TestPhysicalPlanner_ScalarAggregate(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 100})
	planner := NewPhysicalPlanner(cat)

	// SELECT COUNT(*) FROM Orders (empty GroupKeys)
	agg := &logical.LogicalAggregate{
		GroupKeys: []string{},
		AggFuncs:  map[string]string{"*": "COUNT"},
		Child:     &logical.LogicalScan{TableName: "Orders"},
	}

	// Should not panic and should return a HashAggregate (scalar)
	plan := planner.CreatePhysicalPlan(agg)
	if _, ok := plan.(*physical.HashAggregate); !ok {
		t.Errorf("Expected HashAggregate for scalar aggregate, got %T", plan)
	}
}

func TestPhysicalPlanner_Aggregate(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 5000})
	planner := NewPhysicalPlanner(cat)

	agg := &logical.LogicalAggregate{
		GroupKeys: []string{"user_id"},
		AggFuncs:  map[string]string{"total": "SUM"},
		Child:     &logical.LogicalScan{TableName: "Orders"},
	}

	plan := planner.CreatePhysicalPlan(agg)
	// HashAgg cost = 5000 + (5000 * 1.5) = 12500
	// StreamAgg cost = 5000 + (5000 * log2(5000)) + 5000 = 10000 + ~61000 = large
	// HashAgg should be chosen.
	if _, ok := plan.(*physical.HashAggregate); !ok {
		t.Errorf("Expected HashAggregate, got %T", plan)
	}
}

func TestPhysicalPlanner_StreamAggregate_AlreadySorted(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Orders", RowCount: 100})
	planner := NewPhysicalPlanner(cat)

	// Sort(user_id) -> Aggregate(by user_id)
	agg := &logical.LogicalAggregate{
		GroupKeys: []string{"user_id"},
		AggFuncs:  map[string]string{"total": "SUM"},
		Child: &logical.LogicalSort{
			SortKey: "user_id",
			Child:   &logical.LogicalScan{TableName: "Orders"},
		},
	}

	plan := planner.CreatePhysicalPlan(agg)
	
	// Since child is Sort(user_id), planner should recognize it and choose StreamAggregate.
	// Cost(HashAgg) = (Cost(Sort)+100) + 100 * 1.5
	// Cost(StreamAgg) = (Cost(Sort)+100) + 100
	// StreamAgg should win.
	if _, ok := plan.(*physical.StreamAggregate); !ok {
		t.Errorf("Expected StreamAggregate for pre-sorted input, got %T", plan)
	}
}

func TestPhysicalPlanner_TopN(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 1000})
	planner := NewPhysicalPlanner(cat)

	// Limit(Sort(Scan)) -> TopNSort
	tree := &logical.LogicalLimit{
		Limit: 10,
		Child: &logical.LogicalSort{
			SortKey: "name",
			Child:   &logical.LogicalScan{TableName: "Users"},
		},
	}
	plan := planner.CreatePhysicalPlan(tree)
	topN, ok := plan.(*physical.TopNSort)
	if !ok {
		t.Fatalf("Expected TopNSort, got %T", plan)
	}
	if topN.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", topN.Limit)
	}
}

func TestPhysicalPlanner_LimitWithoutSort(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{Name: "Users", RowCount: 1000})
	planner := NewPhysicalPlanner(cat)

	// Limit(Scan) -> Limit (no TopN fusion)
	tree := &logical.LogicalLimit{
		Limit: 10,
		Child: &logical.LogicalScan{TableName: "Users"},
	}
	plan := planner.CreatePhysicalPlan(tree)
	if _, ok := plan.(*physical.Limit); !ok {
		t.Errorf("Expected Limit node, got %T", plan)
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
