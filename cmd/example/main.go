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

	optLogical1 := optimizer.PushdownPredicates(sort1)
	fmt.Println("\nOptimized Logical Plan (after Predicate Pushdown):")
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

	optLogical2 := optimizer.PushdownPredicates(join2)
	fmt.Println("\nOptimized Logical Plan (after Predicate Pushdown):")
	fmt.Println(optLogical2.String())

	physPlan2 := planner.CreatePhysicalPlan(optLogical2)

	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan2.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan2.Cost())

	// --- EXAMPLE 3: Aggregation (HashAggregate vs Sort + StreamAggregate) ---
	// SELECT user_id, SUM(total) FROM Orders GROUP BY user_id
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

	optLogical3 := optimizer.PushdownPredicates(agg3)
	physPlan3 := planner.CreatePhysicalPlan(optLogical3)

	fmt.Println("\nFinal Optimized Physical Plan (EXPLAIN):")
	fmt.Println(physPlan3.Explain(0))
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", physPlan3.Cost())
}
