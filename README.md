# gql-to-sql

GQL (ISO/IEC 39075:2024) to SQL compiler for SQLite. Translates GQL graph queries into SQL and executes them against a relational schema that models property graphs.

```
GQL string → [gql-parser] → AST → [gql-transform] → SQL → [graph-executor] → results
```

## Quick Start

```rust
use graph_executor::GqlExecutor;

let executor = GqlExecutor::new_in_memory()?;

executor.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;

let result = executor.execute("MATCH (p:Person)-[:KNOWS]->(friend) RETURN p.name, friend.name")?;
```

## Crates

| Crate | Purpose |
|-------|---------|
| `gql-parser` | Lexer and recursive-descent parser producing a GQL AST |
| `gql-transform` | Transforms the AST into SQL strings, mapping patterns to JOINs and aliases |
| `graph-executor` | Orchestrates parse → transform → execute via `rusqlite`, manages the EAV schema |

## Storage Model

Two storage modes are supported:

### EAV (default)

The default mode uses an Entity-Attribute-Value schema:

- `nodes` / `edges` — core graph entities
- `node_labels` — label associations for pattern matching
- `property_keys` — shared property key dictionary
- `node_props_{int,text,real,bool,json}` — typed property tables for nodes
- `edge_props_{int,text,real,bool,json}` — typed property tables for edges

### Mapped Relational Tables

Map existing relational tables as graph nodes and edges via the Rust API. No schema migration required — query your existing tables with GQL.

```rust
use gql_transform::resolver::{ColumnMapping, MappedNodeResolver, ForeignKeyEdgeResolver, EdgeDef};

let mut executor = GqlExecutor::new_in_memory()?;

// Set up your relational tables
executor.connection().execute_batch(
    "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
     INSERT INTO people VALUES (1, 'Alice', 30);
     INSERT INTO people VALUES (2, 'Bob', 25);"
)?;

// Register as graph nodes
executor.register_node("Person", Box::new(MappedNodeResolver {
    table_name: "people".to_string(),
    id_col: "id".to_string(),
    label: "Person".to_string(),
    columns: vec![
        ColumnMapping { property_name: "name".to_string(), column_name: "name".to_string() },
        ColumnMapping { property_name: "age".to_string(), column_name: "age".to_string() },
    ],
}));

// Query with GQL
let result = executor.execute("MATCH (p:Person) WHERE p.age > 28 RETURN p.name")?;
```

Three edge resolver types:

| Type | When | Example |
|------|------|---------|
| EAV edge | Default | Uses the `edges` table |
| FK edge | `ForeignKeyEdgeResolver` | `tasks.assignee_id -> people.id` |
| Join-table edge | `JoinTableEdgeResolver` | `friendships(person_a_id, person_b_id)` |

## Supported GQL

- `MATCH` with node/edge patterns, multi-hop paths
- `CREATE` nodes and relationships with properties
- `SET` / `DELETE`
- `WITH` / `FOR x IN list`
- `RETURN` with expressions, aliases, aggregation
- `WHERE` filters (clause-level and inline in patterns)
- `EXPLAIN` (returns generated SQL)

## Running Tests

```bash
cargo test --workspace
```

## License

MIT
