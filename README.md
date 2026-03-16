# Toy Query Optimizer

An educational SQL query optimizer implemented in Go. This project demonstrates how a database transforms a Logical Query Plan (AST) into an optimized Physical Execution Plan using rule-based heuristics and a cost-based model.

## Overview

The optimizer takes a high-level representation of a query and performs several optimization steps:
1.  **Deduction:** Uses **Equivalence Classes** to derive new predicates (e.g., `A.id = B.id` AND `A.id = 42` -> `B.id = 42`).
2.  **Logical Optimization:** Applies rules like **Predicate Pushdown** to move filters deep into the tree, even past Sort nodes.
3.  **Physical Planning:** Evaluates different execution strategies (e.g., `SeqScan` vs `IndexScan`, `NestedLoopJoin` vs `HashJoin`, `HashAgg` vs `StreamAgg`) and uses a **Cost Model** to select the cheapest path.
4.  **Top-N Fusion:** Merges `Sort` and `Limit` into a single high-performance `TopNSort` operator.
5.  **Visualization:** Generates a PostgreSQL-style `EXPLAIN` output for the final plan.

## Architecture

The project is divided into several Go packages:
- `pkg/catalog`: Mock database metadata (tables, row counts, and NDV statistics).
- `pkg/logical`: AST nodes for the logical plan (`Scan`, `Filter`, `Join`, `Sort`, `Aggregate`, `Limit`).
- `pkg/physical`: Execution nodes with cost estimation and EXPLAIN formatting.
- `pkg/optimizer`: The "brain" containing logical rules, equivalence deduction, and the physical planner.

## Features

- [x] Catalog: In-memory registry of table statistics (NDV supported).
- [x] Logical AST: Tree structure representing relational algebra.
- [x] Predicate Pushdown: Automatically moves WHERE clauses closer to the data source (pushed through Sort/Join).
- [x] Cost Model: Estimates execution cost based on table size and index availability.
- [x] Physical Planning: Automated selection of optimal execution strategies.
- [x] Cardinality Estimation: Selectivity rules for various operators (=, !=, >, etc).
- [x] Plan Visualization (EXPLAIN): Human-readable tree representation of the physical plan.
- [x] Advanced Physical Operators: HashJoin and Sort nodes.
- [x] Aggregation and Grouping: HashAggregate and StreamAggregate.
- [x] Equivalence Classes: Transitive closure for smart predicates.
- [x] LIMIT and Top-N Optimization: TopNSort and Limit operators (with OFFSET support).

## How to Run

To see the optimizer in action with the example queries:

```bash
go run cmd/example/main.go
```

## Testing

The project includes unit tests for all core components. To run them:

```bash
go test ./...
```

## Example Transformations

### 1. Selective Filter (Index Lookup + Join Ordering)
**Logical:** `SELECT * FROM Users JOIN Orders WHERE Users.id = 42 ORDER BY total`
```text
-> Sort (Cost: 22.3, Rows: 0)
     Sort Key: Orders.total
    -> NestedLoopJoin (Cost: 22.3, Rows: 0)
         Join Filter: Users.id = Orders.user_id
        -> IndexScan on Users (Cost: 10.0, Rows: 1)
             Filter: id = 42
        -> IndexScan on Orders (Cost: 12.3, Rows: 5)
             Filter: user_id = 42
```

### 2. Large Join (Hash Join Selection)
**Logical:** `SELECT * FROM Users JOIN Orders ON Users.id = Orders.user_id`
```text
-> HashJoin (Cost: 8000.0, Rows: 5000)
     Hash Cond: Users.id = Orders.user_id
    -> SeqScan on Users (Cost: 1000.0, Rows: 1000)
    -> SeqScan on Orders (Cost: 5000.0, Rows: 5000)
```

### 3. Aggregation (Hash Aggregate)
**Logical:** `SELECT user_id, SUM(total) FROM Orders GROUP BY user_id`
```text
-> HashAggregate (Cost: 12500.0, Rows: 1000)
     Group Key: [user_id]
     Agg Funcs: [SUM(total)]
    -> SeqScan on Orders (Cost: 5000.0, Rows: 5000)
```

### 4. Top-N Optimization
**Logical:** `... WHERE Users.id = 42 ORDER BY total LIMIT 5`
```text
-> TopNSort on Orders.total (Cost: 23.3, Rows: 0)
     Limit: 5
    -> NestedLoopJoin (Cost: 22.3, Rows: 0)
         Join Filter: Users.id = Orders.user_id
        -> IndexScan on Users (Cost: 10.0, Rows: 1)
             Filter: id = 42
        -> IndexScan on Orders (Cost: 12.3, Rows: 5)
             Filter: user_id = 42
```

## License
MIT
