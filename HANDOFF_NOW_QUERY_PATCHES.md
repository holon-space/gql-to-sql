# GQL-to-SQL: three patches to land the holon "Now query" in GQL canonical form

## Context

The holon project's `Now.org` page contains the canonical "what's unblocked, sorted, top 10" query. We want to express it in GQL once everything ships:

```gql
MATCH (b:block)
WHERE b.task_state = "TODO"
  AND b.gate = "G1"
  AND NOT EXISTS { (b)-[:blocked_by]->(blocker:block) WHERE blocker.task_state <> "DONE" }
  AND ("agent" IN b.tags OR NOT "human-only" IN b.tags)
RETURN b
ORDER BY b.priority, b.effort, b.id
LIMIT 10
```

Today this **does not compile or executes broken SQL**. Verified empirically against the holon-direct MCP on 2026-05-01. The interim plan is to ship `Now.org` in `holon_sql` form (which compiles cleanly through this same pipeline) and flip to `holon_gql` once the three patches below land. Track 1F in the holon plan at `/Users/martin/.claude/plans/please-read-users-martin-workspaces-pkm-fuzzy-hopper.md`.

## Schema context (holon)

The `block` table:

```sql
CREATE TABLE block (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    depth INTEGER NOT NULL DEFAULT 0,
    sort_key TEXT NOT NULL DEFAULT 'A0',
    content TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'text',
    source_language TEXT,
    source_name TEXT,
    name TEXT,
    properties TEXT,         -- JSON blob: task_state, priority, gate, effort, tags, ...
    marks TEXT,
    collapsed INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    block_type TEXT NOT NULL DEFAULT 'text',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    _change_origin TEXT
)
```

The to-be-created junction table (Track 1C in holon, will land before Now.org goes live):

```sql
CREATE TABLE task_blockers (
    blocked_id TEXT NOT NULL,
    blocker_id TEXT NOT NULL,
    PRIMARY KEY (blocked_id, blocker_id),
    FOREIGN KEY (blocked_id) REFERENCES block(id) ON DELETE CASCADE,
    FOREIGN KEY (blocker_id) REFERENCES block(id) ON DELETE CASCADE
)
```

`block_tags(block_id TEXT, tag TEXT)` similar shape.

Ground-truth SQL form (compiles cleanly today, executes once junctions exist):

```sql
SELECT b.*
FROM block b
WHERE json_extract(b.properties, '$.task_state') = 'TODO'
  AND json_extract(b.properties, '$.gate') = 'G1'
  AND NOT EXISTS (
    SELECT 1 FROM task_blockers tb
    JOIN block bl ON bl.id = tb.blocker_id
    WHERE tb.blocked_id = b.id
      AND COALESCE(json_extract(bl.properties, '$.task_state'), '') <> 'DONE'
  )
  AND (
    EXISTS (SELECT 1 FROM block_tags bt WHERE bt.block_id = b.id AND bt.tag = 'agent')
    OR NOT EXISTS (SELECT 1 FROM block_tags bt WHERE bt.block_id = b.id AND bt.tag = 'human-only')
  )
ORDER BY
  json_extract(b.properties, '$.priority'),
  json_extract(b.properties, '$.effort'),
  b.id
LIMIT 10
```

This is what each of the three patches below should be moving toward.

---

## Patch 1: Properties-as-JSON-extract for unknown fields

### Bug

When GQL references `b.<field>` and `<field>` is not a real column on the entity's table, the compiler emits the field as a quoted column reference. The query parses, compiles, and runs broken — SQLite errors with "no such column" at execution time.

### Empirical evidence

Input GQL:

```gql
MATCH (b:block) WHERE b.priority = "1" RETURN b LIMIT 5
```

Current output (broken):

```sql
SELECT _v0.* FROM block AS _v0 WHERE _v0."priority" = '1' LIMIT 5
```

Expected output:

```sql
SELECT _v0.* FROM block AS _v0 WHERE json_extract(_v0.properties, '$.priority') = '1' LIMIT 5
```

### Where to look

`crates/gql-transform/src/transform_match.rs` — the WHERE clause translator. Whatever turns GQL `b.priority` into SQL `_v0."priority"` needs to consult the schema (which it already does, since it knows `_v0` corresponds to `block` and presumably has access to the column list).

### Strategy

The compiler already knows the table's real columns via the schema registry / GraphSchema. Branch:

- If `<field>` is a real column → emit `_v0."<field>"` (current behavior).
- Else → emit `json_extract(_v0.properties, '$.<field>')` if the entity has a `properties` JSON column.
- If neither → fail at compile time (don't punt to runtime).

Which entities have a "properties JSON" column needs to be schema-registry-knowable. For holon today, only `block` does, but the design should generalize: a per-entity hook that says "for unknown fields, project from this column".

### Test fixtures

| GQL                                              | Expected SQL fragment                                          |
|--------------------------------------------------|----------------------------------------------------------------|
| `b.priority = "1"`                               | `json_extract(_v0.properties, '$.priority') = '1'`             |
| `b.task_state = "TODO" AND b.gate = "G1"`        | `json_extract(_v0.properties, '$.task_state') = 'TODO' AND json_extract(_v0.properties, '$.gate') = 'G1'` |
| `b.content_type = "text"` (real column)          | `_v0."content_type" = 'text'` (unchanged)                      |
| `ORDER BY b.priority, b.id`                      | `ORDER BY json_extract(_v0.properties, '$.priority'), _v0."id"` |

---

## Patch 2: `EXISTS { ... }` and `NOT EXISTS { ... }` subquery clauses

### Bug

Cypher-style `EXISTS { MATCH ... }` and `NOT EXISTS { MATCH ... }` clauses inside WHERE fail to compile.

### Empirical evidence

Input GQL:

```gql
MATCH (b:block)
WHERE NOT EXISTS { (b)-[:blocked_by]->(blocker:block) }
RETURN b LIMIT 5
```

Current output:

```
MCP error -32603: Query compilation failed: Failed to compile query
```

Same for `EXISTS { (b)-[:has_tag]->(t:block) }`. The path-pattern syntax inside the braces appears to be the trip-up.

Expected output (with patch 3 also landed for the edge dispatch):

```sql
SELECT _v0.* FROM block AS _v0
WHERE NOT EXISTS (
  SELECT 1 FROM task_blockers _v1
  JOIN block _v2 ON _v2.id = _v1.blocker_id
  WHERE _v1.blocked_id = _v0.id
)
LIMIT 5
```

The correlated subquery must reference the outer `_v0.id` via a `WHERE` predicate against the junction's source column.

### Where to look

`crates/gql-parser` — the WHERE-clause grammar. EXISTS-with-pattern is its own grammar rule in Cypher / GQL.

`crates/gql-transform/src/transform_match.rs` — once parsed, an EXISTS clause becomes a small nested MATCH that needs to compile as a correlated `EXISTS (SELECT 1 FROM ... WHERE outer.col = inner.col)` subquery. Reuse the existing pattern-to-FROM-clause translator; add the correlation predicate by detecting variables shared with the outer scope.

### Strategy

1. Parse: extend the WHERE expression grammar to recognize `EXISTS { <path-pattern> [WHERE <expr>] }` and `NOT EXISTS { ... }`. Yields an AST node like `ExistsPattern { pattern, optional_where, negated }`.
2. Transform: emit `[NOT] EXISTS (SELECT 1 FROM <pattern-as-from-clause> [WHERE <expr>] AND <correlation-predicate>)`. The correlation predicate is built by detecting which variables in the inner pattern refer to outer scope (`b` in the example above) and emitting `inner_alias.<col> = outer_alias.<col>`.

### Test fixtures

| GQL                                                                              | Expected SQL fragment |
|----------------------------------------------------------------------------------|-----------------------|
| `WHERE NOT EXISTS { (b)-[:blocked_by]->(blocker:block) }`                         | `WHERE NOT EXISTS (SELECT 1 FROM task_blockers _v1 JOIN block _v2 ON _v2.id = _v1.blocker_id WHERE _v1.blocked_id = _v0.id)` |
| `WHERE NOT EXISTS { (b)-[:blocked_by]->(blocker:block) WHERE blocker.task_state <> "DONE" }` | `WHERE NOT EXISTS (SELECT 1 FROM task_blockers _v1 JOIN block _v2 ON _v2.id = _v1.blocker_id WHERE _v1.blocked_id = _v0.id AND COALESCE(json_extract(_v2.properties, '$.task_state'), '') <> 'DONE')` |
| `WHERE EXISTS { (b)-[:has_tag]->(t:block) WHERE t.tag = "agent" }` (after a `block_tags` join-table is registered) | `WHERE EXISTS (SELECT 1 FROM block_tags _v1 WHERE _v1.block_id = _v0.id AND _v1.tag = 'agent')` |

---

## Patch 3: JoinTableEdgeResolver dispatch

### Bug

Edge traversal `MATCH (b)-[:edge_name]->(target)` always emits a JOIN against the generic EAV `edges` table:

```sql
JOIN edges _v1 ON _v1.source_id = _v0.id
JOIN block _v2 ON _v1.target_id = _v2.id
WHERE _v1.type = 'edge_name'
```

Two problems:

1. **Type mismatch**: `edges.source_id` is INTEGER referencing `nodes(id)`, but `block.id` is TEXT. The JOIN never matches anything for typed entities like `block`.
2. **Wrong target**: Holon registers typed junctions (e.g., `task_blockers(blocked_id, blocker_id)`) for specific edges via `JoinTableEdgeResolver`. The compiler should dispatch to those junctions when an edge has a registered resolver, not the EAV fallback.

### Empirical evidence

Input GQL:

```gql
MATCH (b:block)-[:parent]->(p:block) RETURN b, p LIMIT 5
```

Current output (broken at runtime due to type mismatch):

```sql
SELECT _v0.*, _v2.* FROM block AS _v0
JOIN edges AS _v1 ON _v1.source_id = _v0.id
JOIN block AS _v2 ON _v1.target_id = _v2.id
WHERE _v1.type = 'parent' LIMIT 5
```

Expected output for a join-table edge `:blocked_by → task_blockers(blocked_id, blocker_id)`:

```sql
SELECT _v0.*, _v2.* FROM block AS _v0
JOIN task_blockers AS _v1 ON _v1.blocked_id = _v0.id
JOIN block AS _v2 ON _v2.id = _v1.blocker_id
LIMIT 5
```

### Where to look

`crates/gql-transform/src/resolver.rs` (likely the edge-resolution dispatch).

H4 hypothesis in the holon repo (`crates/holon/examples/gql_join_table_resolver_h4.rs`) shows the resolver shape end-to-end against a populated DB — read that file for the working pattern.

### Strategy

The schema registry already contains edge definitions. Each registered edge declares one of:

- `ForeignKeyEdgeResolver { source_table, fk_column }` (already supported, the EAV fallback is a degenerate case)
- `JoinTableEdgeResolver { join_table, source_col, target_col }` (this patch)

When an edge is encountered in a MATCH:

1. Look up the edge in the registry.
2. If it has a `JoinTableEdgeResolver` → emit `JOIN <join_table> _v1 ON _v1.<source_col> = <outer>.<id>` and `JOIN <target_table> _v2 ON _v2.id = _v1.<target_col>`.
3. Else if `ForeignKeyEdgeResolver` → existing FK behavior.
4. Else (no resolver registered) → today's EAV fallback. Acceptable for now, but flag as a follow-up: the EAV emit is type-broken for entities whose ID isn't INTEGER, so it should probably be replaced or gated.

### Test fixtures (assume `:blocked_by` registered with JoinTableEdgeResolver against `task_blockers(blocked_id, blocker_id)`)

| GQL                                                          | Expected FROM/JOIN |
|--------------------------------------------------------------|--------------------|
| `MATCH (b:block)-[:blocked_by]->(blocker:block)`             | `FROM block _v0 JOIN task_blockers _v1 ON _v1.blocked_id = _v0.id JOIN block _v2 ON _v2.id = _v1.blocker_id` |
| `MATCH (b:block)-[:blocked_by*1..3]->(blocker:block)` (variable-length) | recursive CTE recursing on `task_blockers`; cycle-detection per H4. (Optional for this handoff — variable-length is already validated by H4 against the EAV form; transposing to junction is a follow-up if not trivial.) |

---

## How to test the patches

The holon project is connected to a `holon-direct` MCP server. From a Claude Code session in the holon repo, you can call `mcp__holon-direct__compile_query` and `mcp__holon-direct__execute_query` directly to verify both compilation and execution against a live database.

After all three patches:

1. Run the canonical now-query GQL form against the MCP. It should compile cleanly.
2. Compare the emitted SQL to the ground-truth SQL form (above). Ignoring alias names, they should be structurally equivalent.
3. Once `task_blockers` and `block_tags` exist (Track 1C in holon), execute both forms and assert identical result sets.

## Out of scope for this handoff

- Variable-length edge traversal `[*1..3]` against junction tables. H4 already validated this works against the EAV form via recursive CTE; transposing to typed junctions is a separate small follow-up.
- Performance: H7 in the holon hypotheses validated the SQL form at 100k tasks (~20ms p99). The GQL emitter should produce equivalent SQL; if you observe a regression vs the SQL canonical form, file it.
- Pure-EAV edges (with no registered resolver) remain type-broken for entities with TEXT primary keys. Out of scope here; revisit only if it bites.

## Cross-references

- Holon plan: `/Users/martin/.claude/plans/please-read-users-martin-workspaces-pkm-fuzzy-hopper.md` (Track 1F section)
- Empirical Phase 0 transcript: same plan file, "Phase 0 findings" section
- Working SQL reference: `/Users/martin/Workspaces/pkm/holon/crates/holon/examples/now_query_perf_h7.rs` lines 468-472
- Working JoinTableEdgeResolver reference: `/Users/martin/Workspaces/pkm/holon/crates/holon/examples/gql_join_table_resolver_h4.rs`
