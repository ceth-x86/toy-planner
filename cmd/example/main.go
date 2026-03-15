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
			"id": 1000, // Every user has unique ID
		},
	})
	cat.RegisterTable(catalog.TableMetadata{
		Name:     "Orders",
		RowCount: 5000,
		Columns:  []string{"id", "user_id", "total"},
		Indexes:  []string{"id", "user_id"},
		ColumnNDVs: map[string]int{
			"id":      5000,
			"user_id": 1000, // About 5 orders per user
		},
	})

	fmt.Println("Catalog initialized with tables: Users, Orders")

	// Step 2: Build Logical Plan
	// LogicalFilter("Users.id = 42", LogicalJoin("Users.id = Orders.user_id", LogicalScan("Users"), LogicalScan("Orders")))
	
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

	fmt.Println("\n--- Initial Logical Plan ---")
	fmt.Println(filter.String())

	// Step 3: Logical Optimization (Predicate Pushdown)
	optimizedLogical := optimizer.PushdownPredicates(filter)

	fmt.Println("\n--- Optimized Logical Plan (after Predicate Pushdown) ---")
	fmt.Println(optimizedLogical.String())

	// Step 4: Physical Planning (Cost-Based Selection with Cardinality Estimation)
	planner := optimizer.NewPhysicalPlanner(cat)
	finalPhysicalPlan := planner.CreatePhysicalPlan(optimizedLogical)

	fmt.Println("\n--- Final Optimized Physical Plan ---")
	fmt.Println(finalPhysicalPlan.String())
	fmt.Printf("\nTotal Estimated Cost: %.2f\n", finalPhysicalPlan.Cost())
}
