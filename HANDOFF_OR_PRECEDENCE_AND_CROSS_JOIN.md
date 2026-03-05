# GQL-to-SQL: OR precedence bug + cross-join in comma-separated patterns

## Context

The GQL-to-SQL compiler is used to generate SQL for Turso materialized views (IVM). Two issues prevent a common query pattern from working correctly.

## The Query We Want

Navigate to a focused entity — which can be either a document (parent container) or a block (zoom-in). The focus target is stored in `current_focus.block_id` and can reference either a document URI or a block ID.

```gql
MATCH (cf:CurrentFocus), (root:Block)<-[:CHILD_OF*0..20]-(d:Block)
WHERE cf.region = 'main'
AND (root.parent_id = cf.block_id OR root.id = cf.block_id)
RETURN d
```

Semantics: find blocks whose parent is the focus target (document case) OR whose id matches the focus target (block zoom-in case), then traverse all descendants.

## Bug 1: OR expressions lack parentheses (SQL operator precedence)

The compiler emits:

```sql
WHERE ... AND _v1."parent_id" = _v0."block_id" OR _v1."id" = _v0."block_id"
```

This evaluates as `(... AND parent_id = block_id) OR (id = block_id)` due to AND binding tighter than OR. The OR branch bypasses all other WHERE conditions (like `region = 'main'`).

**Expected output:**

```sql
WHERE ... AND (_v1."parent_id" = _v0."block_id" OR _v1."id" = _v0."block_id")
```

**Fix:** When the GQL WHERE clause contains an OR expression mixed with AND conditions, the OR sub-expression must be wrapped in parentheses in the generated SQL. Look at how `add_where()` in `sql_builder.rs` joins conditions with `AND` — an OR expression passed as a single condition string needs its own parens.

The fix likely belongs in `transform_match.rs` where WHERE clause expressions are translated to SQL strings. When encountering an `OR` node in the GQL AST, wrap the entire OR expression in parentheses before passing it to `add_where()`.

## Bug 2: Comma-separated MATCH patterns produce `JOIN ON 1 = 1`

The query `MATCH (cf:CurrentFocus), (root:Block)` (two disconnected patterns) generates:

```sql
FROM current_focus AS _v0 JOIN blocks AS _v1 ON 1 = 1
```

This cross join is problematic:
1. **Turso IVM rejects it** for materialized views: "Only simple column references are supported in join conditions for incremental views"
2. Even for regular queries, it's a cartesian product filtered only by WHERE — potentially expensive

**Workaround available:** If the WHERE clause contains a condition that relates the two patterns (like `root.parent_id = cf.block_id`), the compiler could potentially rewrite the cross join into an inner join using that condition. But this is an optimization, not strictly required.

**Minimum fix for IVM compatibility:** When targeting materialized views, comma-separated patterns with cross-joins should either:
- Be rejected with a clear error message, or
- Have their join condition inferred from WHERE clause equalities between the two pattern variables

## Reproducer

Using the holon schema (tables: `current_focus(region, block_id, timestamp)`, `blocks(id, parent_id, content, ...)`):

```gql
-- Triggers both bugs:
MATCH (cf:CurrentFocus), (root:Block)<-[:CHILD_OF*0..20]-(d:Block)
WHERE cf.region = 'main'
AND (root.parent_id = cf.block_id OR root.id = cf.block_id)
RETURN d
```

Compiled SQL has wrong precedence AND cross-join `ON 1 = 1`.

## Priority

Bug 1 (OR precedence) is the higher priority — it produces silently wrong results. Bug 2 (cross-join) blocks Turso IVM usage but can be worked around by using FK edge definitions instead of comma patterns.
