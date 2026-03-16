package main

import (
	"fmt"
	"toy-optimizer/pkg/catalog"
	"toy-optimizer/pkg/logical"
	"toy-optimizer/pkg/optimizer"
)

func main() {
	// Step 1: Initialize Catalog with stats
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:     "Users",
		RowCount: 1000,
		Columns:  []string{"id", "name", "email"},
		Indexes:  []string{"id"},
		ColumnNDVs: map[string]int{
			"id": 1000,
		},
	})
	cat.RegisterTable(catalog.TableMetadata{
		Name:     "Orders",
		RowCount: 5000,
		Columns:  []string{"id", "user_id", "total"},
		Indexes:  []string{"id", "user_id"},
		ColumnNDVs: map[string]int{
			"id":      5000,
			"user_id": 1000,
		},
	})

	fmt.Println("Catalog initialized with tables: Users, Orders")

	planner := optimizer.NewPhysicalPlanner(cat)

	// --- EXAMPLE 1: Selective Filter (NestedLoopJoin expected) ---
	// SELECT * FROM Users JOIN Orders ON Users.id = Orders.user_id WHERE Users.id = 42 ORDER BY total
	fmt.Println("\n========================================================")
	fmt.Println("EXAMPLE 1: Selective Filter (NestedLoopJoin expected)")
	fmt.Println("========================================================")

	usersScan1 := &logical.LogicalScan{TableName: "Users"}
	ordersScan1 := &logical.LogicalScan{TableName: "Orders"}
	join1 := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan1,
		Right:     ordersScan1,
	}
	filter1 := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     join1,
	}
	sort1 := &logical.LogicalSort{
		SortKey: "Orders.total",
		Child:   filter1,
	}

	fmt.Println("\nInitial Logical Plan:")
	fmt.Println(sort1.String())

	// Consistent pipeline: Deduce -> Pushdown
	deduced1 := optimizer.DeducePredicates(sort1)
	optLogical1 := optimizer.PushdownPredicates(deduced1)
	fmt.Println("\nOptimized Logical Plan (after Deduction + Pushdown):")
	fmt.Println(optLogical1.String())

	physPlan1 := planner.CreatePhysicalPlan(optLogical1)
	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan1.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan1.Cost())

	// --- EXAMPLE 2: No Selective Filters (HashJoin expected) ---
	// SELECT * FROM Users JOIN Orders ON Users.id = Orders.user_id
	fmt.Println("\n========================================================")
	fmt.Println("EXAMPLE 2: No Selective Filters (HashJoin expected)")
	fmt.Println("========================================================")

	usersScan2 := &logical.LogicalScan{TableName: "Users"}
	ordersScan2 := &logical.LogicalScan{TableName: "Orders"}
	join2 := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan2,
		Right:     ordersScan2,
	}

	fmt.Println("\nInitial Logical Plan:")
	fmt.Println(join2.String())

	// Consistent pipeline
	deduced2 := optimizer.DeducePredicates(join2)
	optLogical2 := optimizer.PushdownPredicates(deduced2)
	fmt.Println("\nOptimized Logical Plan:")
	fmt.Println(optLogical2.String())

	physPlan2 := planner.CreatePhysicalPlan(optLogical2)
	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan2.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan2.Cost())

	// --- EXAMPLE 3: Aggregation (GROUP BY user_id) ---
	fmt.Println("\n========================================================")
	fmt.Println("EXAMPLE 3: Aggregation (GROUP BY user_id)")
	fmt.Println("========================================================")

	ordersScan3 := &logical.LogicalScan{TableName: "Orders"}
	agg3 := &logical.LogicalAggregate{
		GroupKeys: []string{"user_id"},
		AggFuncs:  map[string]string{"total": "SUM"},
		Child:     ordersScan3,
	}

	fmt.Println("\nInitial Logical Plan:")
	fmt.Println(agg3.String())

	// Consistent pipeline
	deduced3 := optimizer.DeducePredicates(agg3)
	optLogical3 := optimizer.PushdownPredicates(deduced3)
	physPlan3 := planner.CreatePhysicalPlan(optLogical3)

	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan3.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan3.Cost())

	// --- EXAMPLE 4: Top-N Optimization (Limit + Sort) ---
	// SELECT * FROM Users JOIN Orders ON Users.id = Orders.user_id 
	// WHERE Users.id = 42 ORDER BY total LIMIT 5
	fmt.Println("\n========================================================")
	fmt.Println("EXAMPLE 4: Top-N Optimization (Limit + Sort)")
	fmt.Println("========================================================")

	usersScan4 := &logical.LogicalScan{TableName: "Users"}
	ordersScan4 := &logical.LogicalScan{TableName: "Orders"}
	join4 := &logical.LogicalJoin{
		Condition: "Users.id = Orders.user_id",
		Left:      usersScan4,
		Right:     ordersScan4,
	}
	filter4 := &logical.LogicalFilter{
		Condition: "Users.id = 42",
		Child:     join4,
	}
	sort4 := &logical.LogicalSort{
		SortKey: "Orders.total",
		Child:   filter4,
	}
	limit4 := &logical.LogicalLimit{
		Limit:  5,
		Offset: 0,
		Child:  sort4,
	}

	fmt.Println("\nInitial Logical Plan:")
	fmt.Println(limit4.String())

	deduced4 := optimizer.DeducePredicates(limit4)
	optLogical4 := optimizer.PushdownPredicates(deduced4)
	fmt.Println("\nOptimized Logical Plan:")
	fmt.Println(optLogical4.String())

	physPlan4 := planner.CreatePhysicalPlan(optLogical4)

	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan4.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan4.Cost())
}
