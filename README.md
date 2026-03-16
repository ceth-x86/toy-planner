# Toy Query Optimizer

An educational SQL query optimizer implemented in Go. This project demonstrates how a database transforms a Logical Query Plan (AST) into an optimized Physical Execution Plan using rule-based heuristics and a cost-based model.

## Overview

The optimizer takes a high-level representation of a query and performs several optimization steps:
1.  **Logical Optimization:** Applies rules like **Predicate Pushdown** to move filters deep into the tree, even past Sort nodes.
2.  **Physical Planning:** Evaluates different execution strategies (e.g., `SeqScan` vs `IndexScan`, `NestedLoopJoin` vs `HashJoin`) and uses a **Cost Model** to select the cheapest path.
3.  **Visualization:** Generates a PostgreSQL-style `EXPLAIN` output for the final plan.

## Architecture

The project is divided into several Go packages:
- `pkg/catalog`: Mock database metadata (tables, row counts, and indexes).
- `pkg/logical`: AST nodes for the logical plan (`Scan`, `Filter`, `Join`, `Sort`).
- `pkg/physical`: Execution nodes with cost estimation and EXPLAIN formatting.
- `pkg/optimizer`: The "brain" containing logical rules and the physical planner.

## Features

- [x] Catalog: In-memory registry of table statistics (NDV supported).
- [x] Logical AST: Tree structure representing relational algebra.
- [x] Predicate Pushdown: Automatically moves WHERE clauses closer to the data source (pushed through Sort/Join).
- [x] Cost Model: Estimates execution cost based on table size and index availability.
- [x] Physical Planning: Automated selection of optimal execution strategies.
- [x] Cardinality Estimation: Selectivity rules for various operators (=, !=, >, etc).
- [x] Plan Visualization (EXPLAIN): Human-readable tree representation of the physical plan.
- [x] Advanced Physical Operators: HashJoin and Sort nodes.
- [ ] Aggregation and Grouping: HashAggregate and StreamAggregate.
- [ ] Equivalence Classes: Transitive closure for smart predicates.
- [ ] LIMIT and Top-N Optimization: TopNSort and Limit operators.

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

## Example Transformation

**Initial Logical Plan:**
```text
Sort(by Orders.total)
  Filter(Users.id = 42)
    Join(Users.id = Orders.user_id)
      Scan(Users)
      Scan(Orders)
```

**Optimized Logical Plan (after Predicate Pushdown):**
```text
Sort(by Orders.total)
  Join(Users.id = Orders.user_id)
    Filter(Users.id = 42)
      Scan(Users)
    Scan(Orders)
```

**Final Optimized Physical Plan (EXPLAIN):**
```text
-> Sort (Cost: 5021.6, Rows: 5)
     Sort Key: Orders.total
    -> NestedLoopJoin (Cost: 5010.0, Rows: 5)
         Join Filter: Users.id = Orders.user_id
        -> IndexScan on Users (Cost: 10.0, Rows: 1)
             Filter: id = 42
        -> SeqScan on Orders (Cost: 5000.0, Rows: 5000)
```

## License
MIT
