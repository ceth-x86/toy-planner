# Toy Query Optimizer

An educational SQL query optimizer implemented in Go. This project demonstrates how a database transforms a Logical Query Plan (AST) into an optimized Physical Execution Plan using rule-based heuristics and a cost-based model.

## Overview

The optimizer takes a high-level representation of a query and performs two main optimization steps:
1.  Logical Optimization: Applies rules like Predicate Pushdown to move filters as deep as possible in the tree.
2.  Physical Planning: (Upcoming) Evaluates different execution strategies (e.g., SeqScan vs IndexScan) and uses a Cost Model to select the cheapest path.

## Architecture

The project is divided into several Go packages:
- pkg/catalog: Mock database metadata (tables, row counts, and indexes).
- pkg/logical: AST nodes for the logical plan (Scan, Filter, Join).
- pkg/optimizer: The "brain" containing logical rules.

## Features

- [x] Catalog: In-memory registry of table statistics.
- [x] Logical AST: Tree structure representing relational algebra.
- [x] Predicate Pushdown: Automatically moves WHERE clauses closer to the data source.
- [x] Cost Model: Estimates execution cost based on table size and index availability.
- [x] Physical Planning: Selection of optimal execution strategies.
- [x] Cardinality Estimation: Selectivity rules and NDV integration.
- [x] Plan Visualization (EXPLAIN): Human-readable text representation of the physical plan.
- [ ] Aggregation and Grouping: HashAggregate and StreamAggregate.
- [ ] Advanced Physical Operators: HashJoin and Sort nodes.
- [ ] Equivalence Classes: Transitive closure for smart predicates.
- [ ] LIMIT and Top-N Optimization: TopNSort and Limit operators.

## How to Run

To see the optimizer in action with the example query:

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
Filter(Users.id = 42)
  Join(Users.id = Orders.user_id)
    Scan(Users)
    Scan(Orders)
```

**After Predicate Pushdown:**
```text
Join(Users.id = Orders.user_id)
  Filter(Users.id = 42)
    Scan(Users)
  Scan(Orders)
```

## License
MIT
