use std::collections::HashMap;
use std::fmt::Write;

use gql_parser::{
    BinaryOp, Clause, CreateClause, Direction, ExistsExpr, Expr, ForClause, ListPredicateType,
    Literal, MatchClause, NodePattern, OrderByItem, Path, PathElement, PathType, Query, RelPattern,
    ReturnClause, ReturnItem, WithClause,
};

use crate::plan;
use crate::resolver::{EdgeResolver, GraphSchema, NodeResolver, RecursiveStep};
use crate::sql_builder::{escape_sql_string, JoinType, SqlBuilder, WriteBuilder};
use crate::TransformError;

struct TransformContext<'a> {
    /// Maps variable name -> SQL alias (e.g., "n" -> "_v0")
    var_aliases: HashMap<String, String>,
    /// Maps variable name -> variable kind
    var_kinds: HashMap<String, VarKind>,
    /// Variable names in registration order, for RETURN * expansion
    var_order: Vec<String>,
    alias_counter: usize,
    prop_counter: usize,
    /// Variable names in creation order, for pure-CREATE relationship references
    created_node_order: Vec<String>,
    /// Maps path variable name -> ordered list of (alias, kind) for elements in the path
    path_vars: HashMap<String, Vec<(String, VarKind)>>,
    /// Which node resolver each node variable uses
    node_resolvers: HashMap<String, &'a dyn NodeResolver>,
    /// Which edge resolver each edge variable uses
    edge_resolvers: HashMap<String, &'a dyn EdgeResolver>,
    /// MATCH WHERE conditions distributed per node variable by the planning phase.
    /// Each entry maps a variable name to the GQL AST conditions referencing that variable.
    /// These are transformed to SQL during lowering (node pattern processing and CTE seed construction).
    pending_node_filters: HashMap<String, Vec<&'a Expr>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum VarKind {
    Node,
    Edge,
    Value,
}

impl<'a> TransformContext<'a> {
    fn new() -> Self {
        Self {
            var_aliases: HashMap::new(),
            var_kinds: HashMap::new(),
            var_order: Vec::new(),
            alias_counter: 0,
            prop_counter: 0,
            created_node_order: Vec::new(),
            path_vars: HashMap::new(),
            node_resolvers: HashMap::new(),
            edge_resolvers: HashMap::new(),
            pending_node_filters: HashMap::new(),
        }
    }

    fn next_alias(&mut self) -> String {
        let alias = format!("_v{}", self.alias_counter);
        self.alias_counter += 1;
        alias
    }

    fn next_prop_index(&mut self) -> usize {
        let idx = self.prop_counter;
        self.prop_counter += 1;
        idx
    }

    fn register_node(&mut self, name: &str) -> String {
        if let Some(alias) = self.var_aliases.get(name) {
            return alias.clone();
        }
        let alias = self.next_alias();
        self.var_aliases.insert(name.to_string(), alias.clone());
        self.var_kinds.insert(name.to_string(), VarKind::Node);
        self.var_order.push(name.to_string());
        alias
    }

    fn register_edge(&mut self, name: &str) -> String {
        if let Some(alias) = self.var_aliases.get(name) {
            return alias.clone();
        }
        let alias = self.next_alias();
        self.var_aliases.insert(name.to_string(), alias.clone());
        self.var_kinds.insert(name.to_string(), VarKind::Edge);
        self.var_order.push(name.to_string());
        alias
    }

    fn get_alias(&self, name: &str) -> Option<&str> {
        self.var_aliases.get(name).map(|s| s.as_str())
    }

    fn get_kind(&self, name: &str) -> Option<VarKind> {
        self.var_kinds.get(name).copied()
    }

    fn get_node_resolver(&self, name: &str) -> Option<&'a dyn NodeResolver> {
        self.node_resolvers.get(name).copied()
    }

    fn get_edge_resolver(&self, name: &str) -> Option<&'a dyn EdgeResolver> {
        self.edge_resolvers.get(name).copied()
    }
}

pub fn transform_query<'a>(
    query: &'a Query,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut ctx = TransformContext::new();

    let mut has_match = false;
    let mut has_create = false;
    let mut has_return = false;
    let mut has_set = false;
    let mut has_delete = false;

    for clause in &query.clauses {
        match clause {
            Clause::Match(_) => has_match = true,
            Clause::Create(_) => has_create = true,
            Clause::Return(_) => has_return = true,
            Clause::Set(_) => has_set = true,
            Clause::Delete(_) => has_delete = true,
            _ => {}
        }
    }

    if has_create && !has_match {
        return transform_create_only(query, &mut ctx, schema);
    }

    if has_match && has_create {
        return transform_match_create(query, &mut ctx, schema);
    }

    if has_match && has_set {
        return transform_match_set(query, &mut ctx, schema);
    }

    if has_match && has_delete {
        return transform_match_delete(query, &mut ctx, schema);
    }

    if has_match && has_return {
        return transform_match_return(query, &mut ctx, schema);
    }

    if has_return && !has_match && !has_create {
        return transform_standalone_return(query, &mut ctx, schema);
    }

    Err(TransformError::UnsupportedClause(
        "unsupported clause combination".to_string(),
    ))
}

fn transform_match_return<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut builder = SqlBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => transform_match_clause(ctx, &mut builder, m, schema)?,
            Clause::Return(r) => transform_return_clause(ctx, &mut builder, r, schema)?,
            Clause::With(w) => transform_with_clause(ctx, &mut builder, w)?,
            _ => {
                return Err(TransformError::UnsupportedClause(format!(
                    "{clause:?} in MATCH+RETURN query"
                )));
            }
        }
    }

    Ok(builder.build())
}

fn transform_standalone_return<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut builder = SqlBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Return(r) => {
                if r.distinct {
                    builder.set_distinct(true);
                }
                for item in &r.items {
                    let expr_sql = transform_expr(ctx, &mut builder, &item.expr, schema)?;
                    if let Some(ref alias) = item.alias {
                        builder.add_select_aliased(&expr_sql, &format!("\"{alias}\""));
                    } else {
                        builder.add_select(&expr_sql);
                    }
                }
                transform_order_limit(ctx, &mut builder, &r.order_by, &r.skip, &r.limit, schema)?;
            }
            Clause::For(f) => {
                transform_for_clause(ctx, &mut builder, f, schema)?;
            }
            _ => {
                return Err(TransformError::UnsupportedClause(
                    "non-RETURN clause in standalone RETURN".to_string(),
                ));
            }
        }
    }

    Ok(builder.build())
}

fn transform_for_clause<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    f: &ForClause,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    let list_sql = transform_expr(ctx, builder, &f.list_expr, schema)?;
    let je_alias = format!("_je{}", ctx.alias_counter);
    ctx.alias_counter += 1;
    builder.set_from_aliased(&format!("json_each({list_sql})"), &je_alias);
    ctx.var_aliases
        .insert(f.variable.clone(), format!("{je_alias}.value"));
    ctx.var_kinds.insert(f.variable.clone(), VarKind::Value);
    ctx.var_order.push(f.variable.clone());
    Ok(())
}

fn transform_match_clause<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    m: &'a MatchClause,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    // Plan phase: distribute MATCH WHERE conditions to node variables.
    // This happens BEFORE any SQL generation, so variables don't need to be
    // registered yet — we only analyze AST references, not generate SQL.
    let plan = plan::plan_match_clause(m);

    for (var, conditions) in &plan.per_node {
        ctx.pending_node_filters
            .entry(var.to_string())
            .or_default()
            .extend(conditions);
    }

    // Lower phase: process patterns, generating SQL with all conditions available.
    for path in &m.pattern {
        transform_path(ctx, builder, path, m.optional, schema)?;
    }

    // Add general conditions (multi-variable or non-node conditions).
    // Variables are now registered, so transform_expr can resolve them.
    for cond in &plan.general {
        let sql = transform_expr(ctx, builder, cond, schema)?;
        builder.add_where(&sql);
    }

    ctx.pending_node_filters.clear();

    Ok(())
}

fn transform_path<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    path: &Path,
    optional: bool,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    if path.path_type != PathType::Normal {
        return transform_shortest_path(ctx, builder, path, schema);
    }

    let mut node_aliases: Vec<String> = Vec::new();
    let mut node_patterns: Vec<&NodePattern> = Vec::new();
    let mut skip_next_node = false;
    let mut path_element_aliases: Vec<(String, VarKind)> = Vec::new();

    for (i, element) in path.elements.iter().enumerate() {
        match element {
            PathElement::Node(node) => {
                if skip_next_node {
                    skip_next_node = false;
                    continue;
                }
                let alias = transform_node_pattern(ctx, builder, node, optional, schema)?;
                node_aliases.push(alias.clone());
                node_patterns.push(node);
                path_element_aliases.push((alias, VarKind::Node));
            }
            PathElement::Rel(rel) => {
                assert!(
                    i >= 1 && i + 1 < path.elements.len(),
                    "malformed path: Rel must be between two Nodes"
                );
                let source_alias = node_aliases[node_aliases.len() - 1].clone();
                let source_node = node_patterns[node_patterns.len() - 1];
                let target_node = match &path.elements[i + 1] {
                    PathElement::Node(n) => n,
                    _ => unreachable!("path must alternate Node-Rel-Node"),
                };
                skip_next_node = true;

                if rel.varlen.is_some() {
                    let target_alias = transform_varlen_segment(
                        ctx,
                        builder,
                        &source_alias,
                        source_node,
                        rel,
                        target_node,
                        optional,
                        schema,
                    )?;
                    node_aliases.push(target_alias.clone());
                    path_element_aliases.push((target_alias, VarKind::Node));
                    continue;
                }

                // Register edge alias first so we can build the target ON
                let edge_alias = match &rel.variable {
                    Some(name) => ctx.register_edge(name),
                    None => {
                        let a = ctx.next_alias();
                        ctx.var_kinds.insert(a.clone(), VarKind::Edge);
                        a
                    }
                };

                // Determine the target node alias (register but don't join yet)
                let target_alias_name = target_node.variable.as_deref().unwrap_or("");
                let target_already_exists =
                    !target_alias_name.is_empty() && ctx.get_alias(target_alias_name).is_some();

                // Resolve the edge
                let rel_type = rel.rel_types.first().map(|s| s.as_str());
                let resolver = schema.edge_resolver(rel_type);
                if let Some(ref name) = rel.variable {
                    ctx.edge_resolvers.insert(name.clone(), resolver);
                }

                // Determine the target alias (peek or existing)
                let target_alias_for_traverse = if !target_already_exists {
                    format!("_v{}", ctx.alias_counter)
                } else {
                    ctx.get_alias(target_alias_name).unwrap().to_string()
                };

                // Use the resolver to build edge traversal joins and conditions
                let (traverse_joins, traverse_conditions) = resolver.traverse_joins(
                    &source_alias,
                    &target_alias_for_traverse,
                    &edge_alias,
                    &rel.direction,
                    optional,
                );

                // Add edge table joins (e.g., JOIN edges for EAV, JOIN join_table for join-table edges)
                for j in &traverse_joins {
                    builder.add_join_aliased(j.join_type, &j.table, &j.alias, &j.on_condition);
                }

                let type_conditions = resolver.type_filter(&edge_alias, &rel.rel_types);
                for c in type_conditions {
                    builder.add_where(&c);
                }

                // Join the target node, using traverse conditions as the ON clause
                let target_on = if !traverse_conditions.is_empty() {
                    Some(traverse_conditions.join(" AND "))
                } else {
                    None
                };

                let target_alias = if !target_already_exists {
                    transform_node_pattern_with_on(
                        ctx,
                        builder,
                        target_node,
                        optional,
                        schema,
                        target_on.as_deref(),
                    )?
                } else {
                    let a = ctx.get_alias(target_alias_name).unwrap().to_string();
                    if let Some(on) = &target_on {
                        builder.add_where(on);
                    }
                    a
                };

                node_aliases.push(target_alias.clone());
                node_patterns.push(target_node);

                // Handle edge WHERE and property expressions
                if let Some(ref where_expr) = rel.where_expr {
                    let sql = transform_expr(ctx, builder, where_expr, schema)?;
                    builder.add_where(&sql);
                }

                if !edge_alias.is_empty() {
                    path_element_aliases.push((edge_alias, VarKind::Edge));
                }
                path_element_aliases.push((target_alias, VarKind::Node));
            }
        }
    }

    if let Some(ref path_var) = path.variable {
        ctx.path_vars.insert(path_var.clone(), path_element_aliases);
    }

    Ok(())
}

fn transform_shortest_path<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    path: &Path,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    assert!(
        path.elements.len() == 3,
        "shortestPath must have exactly Node-Rel-Node"
    );
    let source_node = match &path.elements[0] {
        PathElement::Node(n) => n,
        _ => unreachable!(),
    };
    let rel = match &path.elements[1] {
        PathElement::Rel(r) => r,
        _ => unreachable!(),
    };
    let target_node = match &path.elements[2] {
        PathElement::Node(n) => n,
        _ => unreachable!(),
    };

    let source_alias = transform_node_pattern(ctx, builder, source_node, false, schema)?;
    let target_alias = transform_node_pattern(ctx, builder, target_node, false, schema)?;

    let min_hops = rel.varlen.as_ref().and_then(|v| v.min_hops).unwrap_or(1) as i64;
    let max_hops = rel.varlen.as_ref().and_then(|v| v.max_hops).unwrap_or(10) as i64;

    let cte_name = format!("_sp{}", ctx.alias_counter);
    ctx.alias_counter += 1;

    let rel_type = rel.rel_types.first().map(|s| s.as_str());
    let resolver = schema.edge_resolver(rel_type);

    let build_step_sql = |step: RecursiveStep| -> String {
        let cycle = format!(
            "',' || {cte_name}.visited || ',' NOT LIKE '%,' || CAST({next} AS TEXT) || ',%'",
            next = step.next_node_expr
        );
        let mut conditions = vec![format!("{cte_name}.depth < {max_hops}"), cycle];
        conditions.extend(step.where_conditions);

        format!(
            "SELECT {next}, {cte_name}.depth + 1, \
             {cte_name}.visited || ',' || CAST({next} AS TEXT) \
             FROM {cte_name} \
             {from} \
             WHERE {where_clause}",
            next = step.next_node_expr,
            from = step.from_clause,
            where_clause = conditions.join(" AND "),
        )
    };

    let recursive_part = match rel.direction {
        Direction::Both | Direction::None => {
            let fwd = resolver.recursive_step(&cte_name, &Direction::Right, &rel.rel_types);
            let bwd = resolver.recursive_step(&cte_name, &Direction::Left, &rel.rel_types);
            format!("{} UNION ALL {}", build_step_sql(fwd), build_step_sql(bwd))
        }
        ref dir => {
            let step = resolver.recursive_step(&cte_name, dir, &rel.rel_types);
            build_step_sql(step)
        }
    };

    // CTE must be self-contained — it cannot reference outer query aliases.
    // Build seed conditions from ALL sources: inline props, node WHERE, and MATCH WHERE.
    let label = source_node.labels.first().map(|s| s.as_str());
    let source_table = schema.node_resolver(label).table();
    let seed_conditions = build_seed_conditions(ctx, builder, source_node, &source_alias, schema)?;
    let seed_where = if seed_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conditions.join(" AND "))
    };
    let cte_sql = format!(
        "SELECT {source_alias}.id AS node_id, 0 AS depth, \
         CAST({source_alias}.id AS TEXT) AS visited \
         FROM {source_table} AS {source_alias}{seed_where} \
         UNION ALL \
         {recursive_part}"
    );

    builder.add_cte_recursive(&cte_name, &cte_sql);

    if path.path_type == PathType::Shortest {
        builder.add_where(&format!(
            "{target_alias}.id IN (SELECT node_id FROM {cte_name} \
             WHERE depth >= {min_hops} AND depth <= {max_hops} \
             ORDER BY depth LIMIT 1)"
        ));
    } else {
        builder.add_where(&format!(
            "{target_alias}.id IN (SELECT node_id FROM {cte_name} \
             WHERE depth >= {min_hops} AND depth <= {max_hops} \
             AND depth = (SELECT MIN(depth) FROM {cte_name} \
             WHERE node_id = {target_alias}.id AND depth >= {min_hops} AND depth <= {max_hops}))"
        ));
    }

    if let Some(ref path_var) = path.variable {
        let mut elements = vec![(source_alias.clone(), VarKind::Node)];
        if let Some(ref name) = rel.variable {
            elements.push((ctx.register_edge(name), VarKind::Edge));
        }
        elements.push((target_alias, VarKind::Node));
        ctx.path_vars.insert(path_var.clone(), elements);
    }

    Ok(())
}

/// Build WHERE conditions for a CTE seed from a source node's inline properties.
/// Uses the node's resolver to produce column filters (e.g. `_seed."id" = 'foo'`).
fn build_seed_conditions<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    source_node: &NodePattern,
    seed_alias: &str,
    schema: &'a GraphSchema,
) -> Result<Vec<String>, TransformError> {
    let label = source_node.labels.first().map(|s| s.as_str());
    let resolver = schema.node_resolver(label);
    let source_table = resolver.table();
    let mut conditions = Vec::new();

    if let Some(ref props_expr) = source_node.properties {
        if let Expr::Map(pairs) = props_expr.as_ref() {
            for pair in pairs {
                let value_sql = transform_expr(ctx, builder, &pair.value, schema)?;
                let frag = resolver.property_filter(seed_alias, &pair.key, &value_sql, &pair.value);
                if frag.joins.is_empty() {
                    // Mapped nodes: simple column filters, safe inside CTE seed.
                    for c in frag.conditions {
                        conditions.push(c);
                    }
                } else {
                    // EAV nodes: filter needs property table joins that can't go inside
                    // the CTE seed. Convert to a self-contained subquery instead.
                    let mut subquery_parts = vec![format!(
                        "SELECT {seed_alias}.id FROM {source_table} AS {seed_alias}"
                    )];
                    for j in &frag.joins {
                        let kw = match j.join_type {
                            JoinType::Inner | JoinType::Cross => "JOIN",
                            JoinType::Left => "LEFT JOIN",
                        };
                        subquery_parts.push(format!(
                            " {kw} {} AS {} ON {}",
                            j.table, j.alias, j.on_condition
                        ));
                    }
                    if !frag.conditions.is_empty() {
                        subquery_parts.push(format!(" WHERE {}", frag.conditions.join(" AND ")));
                    }
                    conditions.push(format!("{seed_alias}.id IN ({})", subquery_parts.join("")));
                }
            }
        }
    }

    if let Some(ref where_expr) = source_node.where_expr {
        let sql = transform_expr(ctx, builder, where_expr, schema)?;
        conditions.push(sql);
    }

    // Include MATCH WHERE conditions assigned to this variable by the planning phase.
    // Clone to avoid borrow conflict with the mutable ctx reference in transform_expr.
    if let Some(ref var_name) = source_node.variable {
        let filters: Vec<&Expr> = ctx
            .pending_node_filters
            .get(var_name.as_str())
            .cloned()
            .unwrap_or_default();
        for filter in &filters {
            let sql = transform_expr(ctx, builder, filter, schema)?;
            conditions.push(sql);
        }
    }

    Ok(conditions)
}

fn transform_varlen_segment<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    source_alias: &str,
    source_node: &NodePattern,
    rel: &RelPattern,
    target_node: &NodePattern,
    optional: bool,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let varlen = rel.varlen.as_ref().unwrap();
    let min_hops = varlen.min_hops.unwrap_or(1) as i64;
    let max_hops = varlen.max_hops.unwrap_or(10) as i64;

    let cte_name = format!("_vl{}", ctx.alias_counter);
    ctx.alias_counter += 1;

    let rel_type = rel.rel_types.first().map(|s| s.as_str());
    let resolver = schema.edge_resolver(rel_type);

    // Carry source_id through the recursion so the CTE can equijoin to the
    // source node in the outer query. Turso IVM requires all JOINs to have
    // at least one equality condition on column references.
    let build_step_sql = |step: RecursiveStep| -> String {
        let cycle = format!(
            "',' || {cte_name}.visited || ',' NOT LIKE '%,' || CAST({next} AS TEXT) || ',%'",
            next = step.next_node_expr
        );
        let mut conditions = vec![format!("{cte_name}.depth < {max_hops}"), cycle];
        conditions.extend(step.where_conditions);

        format!(
            "SELECT {next}, {cte_name}.source_id, {cte_name}.depth + 1, \
             {cte_name}.visited || ',' || CAST({next} AS TEXT) \
             FROM {cte_name} \
             {from} \
             WHERE {where_clause}",
            next = step.next_node_expr,
            from = step.from_clause,
            where_clause = conditions.join(" AND "),
        )
    };

    let recursive_part = match rel.direction {
        Direction::Both | Direction::None => {
            let fwd = resolver.recursive_step(&cte_name, &Direction::Right, &rel.rel_types);
            let bwd = resolver.recursive_step(&cte_name, &Direction::Left, &rel.rel_types);
            format!("{} UNION ALL {}", build_step_sql(fwd), build_step_sql(bwd))
        }
        ref dir => {
            let step = resolver.recursive_step(&cte_name, dir, &rel.rel_types);
            build_step_sql(step)
        }
    };

    // CTE must be self-contained — it cannot reference outer query aliases.
    let label = source_node.labels.first().map(|s| s.as_str());
    let source_table = schema.node_resolver(label).table();
    let seed_conditions = build_seed_conditions(ctx, builder, source_node, source_alias, schema)?;
    let seed_where = if seed_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conditions.join(" AND "))
    };
    // Base case includes source_id = seed node's id (carried through recursion)
    let cte_sql = format!(
        "SELECT {source_alias}.id AS node_id, {source_alias}.id AS source_id, 0 AS depth, \
         CAST({source_alias}.id AS TEXT) AS visited \
         FROM {source_table} AS {source_alias}{seed_where} \
         UNION ALL \
         {recursive_part}"
    );

    builder.add_cte_recursive(&cte_name, &cte_sql);

    // Turso IVM requires every JOIN to have at least one equijoin condition.
    // Strategy:
    //   1. JOIN the CTE to source_alias via equijoin on source_id = source.id
    //   2. JOIN the target table to the CTE via equijoin on target.id = cte.node_id
    //   3. Depth bounds go in WHERE (non-equijoin conditions not allowed in ON)
    let cte_alias = format!("{cte_name}_j");
    let jt = if optional {
        JoinType::Left
    } else {
        JoinType::Inner
    };

    // Step 1: CTE equijoins to source node already in scope
    builder.add_join_aliased(
        jt,
        &cte_name,
        &cte_alias,
        &format!("{cte_alias}.source_id = {source_alias}.id"),
    );

    // Peek at what alias transform_node_pattern_with_on will assign.
    let target_alias_preview = match &target_node.variable {
        Some(name) => ctx
            .get_alias(name)
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("_v{}", ctx.alias_counter)),
        None => format!("_v{}", ctx.alias_counter),
    };

    // Step 2: Target table equijoins to CTE
    let target_join_on = format!("{target_alias_preview}.id = {cte_alias}.node_id");
    let target_alias = transform_node_pattern_with_on(
        ctx,
        builder,
        target_node,
        optional,
        schema,
        Some(&target_join_on),
    )?;
    assert_eq!(target_alias, target_alias_preview);

    // Step 3: Depth bounds in WHERE
    builder.add_where(&format!("{cte_alias}.depth >= {min_hops}"));
    builder.add_where(&format!("{cte_alias}.depth <= {max_hops}"));

    Ok(target_alias)
}

fn transform_node_pattern<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    node: &NodePattern,
    optional: bool,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    transform_node_pattern_with_on(ctx, builder, node, optional, schema, None)
}

fn transform_node_pattern_with_on<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    node: &NodePattern,
    optional: bool,
    schema: &'a GraphSchema,
    join_on: Option<&str>,
) -> Result<String, TransformError> {
    let (alias, is_new) = match &node.variable {
        Some(name) => {
            let already_exists = ctx.get_alias(name).is_some();
            let alias = ctx.register_node(name);
            (alias, !already_exists)
        }
        None => (ctx.next_alias(), true),
    };

    if !is_new {
        // Apply pending MATCH WHERE conditions even for already-registered nodes
        apply_pending_node_filters(ctx, builder, node, schema)?;
        return Ok(alias);
    }

    let label = node.labels.first().map(|s| s.as_str());
    let resolver = schema.node_resolver(label);

    if let Some(ref name) = node.variable {
        ctx.node_resolvers.insert(name.clone(), resolver);
    }

    let table = resolver.table();

    if !builder.has_from() {
        builder.set_from_aliased(table, &alias);
    } else if let Some(on) = join_on {
        let jt = if optional {
            JoinType::Left
        } else {
            JoinType::Inner
        };
        builder.add_join_aliased(jt, table, &alias, on);
    } else if optional {
        builder.add_join_aliased(JoinType::Left, table, &alias, "1=1");
    } else {
        builder.add_join_aliased(JoinType::Cross, table, &alias, "");
    }

    for (li, label_str) in node.labels.iter().enumerate() {
        let frag = resolver.label_joins(&alias, label_str, li);
        for j in &frag.joins {
            builder.add_join_aliased(j.join_type, &j.table, &j.alias, &j.on_condition);
        }
        for c in &frag.conditions {
            builder.add_where(c);
        }
    }

    if let Some(ref props_expr) = node.properties {
        if let Expr::Map(pairs) = props_expr.as_ref() {
            for pair in pairs {
                let value_sql = transform_expr(ctx, builder, &pair.value, schema)?;
                let frag = resolver.property_filter(&alias, &pair.key, &value_sql, &pair.value);
                for j in frag.joins {
                    builder.add_join_aliased(j.join_type, &j.table, &j.alias, &j.on_condition);
                }
                for c in frag.conditions {
                    builder.add_where(&c);
                }
            }
        }
    }

    if let Some(ref where_expr) = node.where_expr {
        let sql = transform_expr(ctx, builder, where_expr, schema)?;
        builder.add_where(&sql);
    }

    // Apply MATCH WHERE conditions assigned to this variable by the planning phase
    apply_pending_node_filters(ctx, builder, node, schema)?;

    Ok(alias)
}

fn apply_pending_node_filters<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    node: &NodePattern,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    if let Some(ref name) = node.variable {
        let filters: Vec<&Expr> = ctx
            .pending_node_filters
            .get(name.as_str())
            .cloned()
            .unwrap_or_default();
        for filter in &filters {
            let sql = transform_expr(ctx, builder, filter, schema)?;
            builder.add_where(&sql);
        }
    }
    Ok(())
}

fn transform_return_clause<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    r: &ReturnClause,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    if r.distinct {
        builder.set_distinct(true);
    }

    // Expand RETURN * into individual items for all bound variables
    let is_star = r.items.len() == 1 && matches!(&r.items[0].expr, Expr::Identifier(n) if n == "*");
    let expanded: Vec<ReturnItem>;
    let items: &[ReturnItem] = if is_star {
        expanded = ctx
            .var_order
            .iter()
            .filter(|name| ctx.var_aliases.contains_key(name.as_str()))
            .map(|name| ReturnItem {
                expr: Expr::Identifier(name.clone()),
                alias: Some(name.clone()),
            })
            .collect();
        &expanded
    } else {
        &r.items
    };

    let has_aggregate = items.iter().any(|item| expr_contains_aggregate(&item.expr));
    let mut non_aggregate_exprs = Vec::new();

    for item in items {
        let expr_sql = transform_return_item_expr(ctx, builder, &item.expr, schema)?;
        let alias = return_item_alias(item);
        if has_aggregate && !expr_contains_aggregate(&item.expr) {
            non_aggregate_exprs.push(expr_sql.clone());
        }
        if let Some(a) = alias {
            builder.add_select_aliased(&expr_sql, &format!("\"{a}\""));
        } else {
            builder.add_select(&expr_sql);
        }
    }

    for expr in &non_aggregate_exprs {
        builder.add_group_by(expr);
    }

    transform_order_limit(ctx, builder, &r.order_by, &r.skip, &r.limit, schema)?;
    Ok(())
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            matches!(
                name.to_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT" | "STDEV" | "STDEVP"
            ) || args.iter().any(expr_contains_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Not(inner) => expr_contains_aggregate(inner),
        Expr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            operand.as_ref().is_some_and(|e| expr_contains_aggregate(e))
                || when_clauses.iter().any(|w| {
                    expr_contains_aggregate(&w.condition) || expr_contains_aggregate(&w.result)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|e| expr_contains_aggregate(e))
        }
        _ => false,
    }
}

fn transform_with_clause(
    _ctx: &mut TransformContext,
    _builder: &mut SqlBuilder,
    _w: &WithClause,
) -> Result<(), TransformError> {
    Err(TransformError::UnsupportedClause("WITH".to_string()))
}

fn transform_order_limit<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    order_by: &[OrderByItem],
    skip: &Option<Box<Expr>>,
    limit: &Option<Box<Expr>>,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    for item in order_by {
        let expr_sql = match &item.expr {
            Expr::Identifier(name) if ctx.get_alias(name).is_none() => {
                format!("\"{name}\"")
            }
            _ => transform_expr(ctx, builder, &item.expr, schema)?,
        };
        builder.add_order_by(&expr_sql, item.descending);
    }

    if let Some(ref limit_expr) = limit {
        if let Expr::Literal(Literal::Integer(n)) = limit_expr.as_ref() {
            builder.set_limit(*n);
        }
    }
    if let Some(ref skip_expr) = skip {
        if let Expr::Literal(Literal::Integer(n)) = skip_expr.as_ref() {
            builder.set_offset(*n);
        }
    }

    Ok(())
}

fn transform_return_item_expr<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    expr: &Expr,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    match expr {
        Expr::Identifier(name) => {
            if let Some(elements) = ctx.path_vars.get(name) {
                return Ok(path_json_array(ctx, elements, schema));
            }
            let alias = ctx
                .get_alias(name)
                .ok_or_else(|| TransformError::Internal(format!("unknown variable: {name}")))?;
            match ctx.get_kind(name) {
                Some(VarKind::Node) => {
                    if schema.raw_return {
                        Ok(format!("{alias}.*"))
                    } else {
                        let resolver = ctx
                            .get_node_resolver(name)
                            .unwrap_or(schema.default_node_resolver.as_ref());
                        Ok(resolver.node_json_object(alias))
                    }
                }
                Some(VarKind::Edge) => {
                    if schema.raw_return {
                        Ok(format!("{alias}.*"))
                    } else {
                        let resolver = ctx
                            .get_edge_resolver(name)
                            .unwrap_or(schema.default_edge_resolver.as_ref());
                        Ok(resolver.edge_json_object(alias))
                    }
                }
                Some(VarKind::Value) => Ok(alias.to_string()),
                None => Ok(alias.to_string()),
            }
        }
        _ => transform_expr(ctx, builder, expr, schema),
    }
}

fn path_json_array<'a>(
    ctx: &TransformContext<'a>,
    elements: &[(String, VarKind)],
    schema: &'a GraphSchema,
) -> String {
    let default_node = schema.default_node_resolver.as_ref();
    let default_edge = schema.default_edge_resolver.as_ref();

    let parts: Vec<String> = elements
        .iter()
        .map(|(alias, kind)| match kind {
            VarKind::Node => {
                let resolver = ctx
                    .var_aliases
                    .iter()
                    .find(|(_, a)| a.as_str() == alias.as_str())
                    .and_then(|(name, _)| ctx.get_node_resolver(name))
                    .unwrap_or(default_node);
                format!(
                    "json_object('type', 'node', 'id', {a}.{id}, \
'labels', {labels})",
                    a = alias,
                    id = resolver.id_column(),
                    labels = resolver.labels_expr(alias),
                )
            }
            VarKind::Edge => {
                let resolver = ctx
                    .var_aliases
                    .iter()
                    .find(|(_, a)| a.as_str() == alias.as_str())
                    .and_then(|(name, _)| ctx.get_edge_resolver(name))
                    .unwrap_or(default_edge);
                resolver.edge_json_object(alias)
            }
            VarKind::Value => alias.to_string(),
        })
        .collect();
    format!("json_array({})", parts.join(", "))
}

fn return_item_alias(item: &ReturnItem) -> Option<String> {
    if let Some(ref alias) = item.alias {
        return Some(alias.clone());
    }
    match &item.expr {
        Expr::Property { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn transform_expr<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    expr: &Expr,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    match expr {
        Expr::Literal(lit) => Ok(transform_literal(lit)),

        Expr::Identifier(name) => {
            if let Some(elements) = ctx.path_vars.get(name) {
                return Ok(path_json_array(ctx, elements, schema));
            }
            let alias = ctx
                .get_alias(name)
                .ok_or_else(|| TransformError::Internal(format!("unknown variable: {name}")))?;
            if ctx.get_kind(name) == Some(VarKind::Value) {
                return Ok(alias.to_string());
            }
            Ok(format!("{alias}.id"))
        }

        Expr::Property { expr, name } => {
            transform_property_access(ctx, builder, expr, name, schema)
        }

        Expr::BinaryOp { op, left, right } => {
            let l = transform_expr(ctx, builder, left, schema)?;
            let r = transform_expr(ctx, builder, right, schema)?;
            let op_str = match op {
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Xor => {
                    return Ok(format!("(({l}) OR ({r})) AND NOT (({l}) AND ({r}))"));
                }
                BinaryOp::Eq => "=",
                BinaryOp::Neq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::Gt => ">",
                BinaryOp::Lte => "<=",
                BinaryOp::Gte => ">=",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
                BinaryOp::In => {
                    return Ok(format!("{l} IN ({r})"));
                }
                BinaryOp::StartsWith => {
                    return Ok(format!("{l} LIKE {r} || '%'"));
                }
                BinaryOp::EndsWith => {
                    return Ok(format!("{l} LIKE '%' || {r}"));
                }
                BinaryOp::Contains => {
                    return Ok(format!("{l} LIKE '%' || {r} || '%'"));
                }
                BinaryOp::RegexMatch => {
                    return Ok(format!("{l} REGEXP {r}"));
                }
            };
            Ok(format!("{l} {op_str} {r}"))
        }

        Expr::Not(inner) => {
            let s = transform_expr(ctx, builder, inner, schema)?;
            Ok(format!("NOT ({s})"))
        }

        Expr::NullCheck { expr, is_not_null } => {
            let s = transform_expr(ctx, builder, expr, schema)?;
            if *is_not_null {
                Ok(format!("{s} IS NOT NULL"))
            } else {
                Ok(format!("{s} IS NULL"))
            }
        }

        Expr::FunctionCall {
            name,
            args,
            distinct,
        } => {
            let upper_name = name.to_uppercase();
            let sql_name = match upper_name.as_str() {
                "TOSTRING" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("CAST({arg} AS TEXT)"));
                    }
                    upper_name
                }
                "TOINTEGER" | "TOINT" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("CAST({arg} AS INTEGER)"));
                    }
                    upper_name
                }
                "TOFLOAT" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("CAST({arg} AS REAL)"));
                    }
                    upper_name
                }
                "TOUPPER" => "UPPER".to_string(),
                "TOLOWER" => "LOWER".to_string(),
                "LEFT" => {
                    if args.len() == 2 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        let length = transform_expr(ctx, builder, &args[1], schema)?;
                        return Ok(format!("SUBSTR({arg}, 1, {length})"));
                    }
                    upper_name
                }
                "RIGHT" => {
                    if args.len() == 2 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        let length = transform_expr(ctx, builder, &args[1], schema)?;
                        return Ok(format!("SUBSTR({arg}, -{length})"));
                    }
                    upper_name
                }
                "SUBSTRING" => "SUBSTR".to_string(),
                "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "REVERSE" | "SPLIT" | "ABS" | "ROUND"
                | "SIGN" | "LENGTH" => upper_name,
                "COLLECT" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("json_group_array({arg})"));
                    }
                    "json_group_array".to_string()
                }
                "CEIL" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!(
                            "(CAST({arg} AS INTEGER) + ({arg} > CAST({arg} AS INTEGER)))"
                        ));
                    }
                    upper_name
                }
                "FLOOR" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!(
                            "(CAST({arg} AS INTEGER) - ({arg} < CAST({arg} AS INTEGER)))"
                        ));
                    }
                    upper_name
                }
                "RAND" | "RANDOM" => {
                    return Ok("(ABS(RANDOM()) / CAST(9223372036854775807 AS REAL))".to_string());
                }
                "SIZE" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("json_array_length({arg})"));
                    }
                    "json_array_length".to_string()
                }
                "HEAD" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("json_extract({arg}, '$[0]')"));
                    }
                    upper_name
                }
                "TAIL" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!("json_remove({arg}, '$[0]')"));
                    }
                    upper_name
                }
                "LAST" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!(
                            "json_extract({arg}, '$[' || (json_array_length({arg}) - 1) || ']')"
                        ));
                    }
                    upper_name
                }
                "KEYS" => {
                    if args.len() == 1 {
                        let arg = transform_expr(ctx, builder, &args[0], schema)?;
                        return Ok(format!(
                            "(SELECT json_group_array(key) FROM json_each({arg}))"
                        ));
                    }
                    upper_name
                }
                "TYPE" => {
                    if args.is_empty() {
                        return Err(TransformError::UnsupportedExpr(
                            "type() requires exactly one argument".to_string(),
                        ));
                    }
                    if args.len() == 1 {
                        if let Expr::Identifier(var_name) = &args[0] {
                            if let Some(alias) = ctx.get_alias(var_name) {
                                if ctx.get_kind(var_name) == Some(VarKind::Edge) {
                                    return Ok(format!("{alias}.type"));
                                }
                            }
                        }
                    }
                    "typeof".to_string()
                }
                "ID" => {
                    if args.len() == 1 {
                        if let Expr::Identifier(var_name) = &args[0] {
                            if let Some(alias) = ctx.get_alias(var_name) {
                                return Ok(format!("{alias}.id"));
                            }
                        }
                    }
                    "rowid".to_string()
                }
                "LABELS" => {
                    if args.len() == 1 {
                        if let Expr::Identifier(var_name) = &args[0] {
                            if let Some(alias) = ctx.get_alias(var_name) {
                                let resolver = ctx
                                    .get_node_resolver(var_name)
                                    .unwrap_or(schema.default_node_resolver.as_ref());
                                return Ok(resolver.labels_expr(alias));
                            }
                        }
                    }
                    upper_name
                }
                "PROPERTIES" => {
                    if args.len() == 1 {
                        if let Expr::Identifier(var_name) = &args[0] {
                            if let Some(alias) = ctx.get_alias(var_name) {
                                let resolver = ctx
                                    .get_node_resolver(var_name)
                                    .unwrap_or(schema.default_node_resolver.as_ref());
                                return Ok(resolver.all_properties_expr(alias));
                            }
                        }
                    }
                    upper_name
                }
                _ => upper_name,
            };

            let args_sql: Vec<String> = args
                .iter()
                .map(|a| transform_expr(ctx, builder, a, schema))
                .collect::<Result<_, _>>()?;

            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            Ok(format!("{sql_name}({distinct_str}{})", args_sql.join(", ")))
        }

        Expr::Parameter(name) => Ok(format!(":{name}")),

        Expr::List(items) => {
            let items_sql: Vec<String> = items
                .iter()
                .map(|e| transform_expr(ctx, builder, e, schema))
                .collect::<Result<_, _>>()?;
            Ok(format!("json_array({})", items_sql.join(", ")))
        }

        Expr::Map(pairs) => {
            let mut parts = Vec::new();
            for pair in pairs {
                let v = transform_expr(ctx, builder, &pair.value, schema)?;
                parts.push(format!("'{}', {v}", escape_sql_string(&pair.key)));
            }
            Ok(format!("json_object({})", parts.join(", ")))
        }

        Expr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            let mut sql = String::from("CASE");
            for wc in when_clauses {
                let cond = transform_expr(ctx, builder, &wc.condition, schema)?;
                let result = transform_expr(ctx, builder, &wc.result, schema)?;
                if let Some(ref op) = operand {
                    let op_sql = transform_expr(ctx, builder, op, schema)?;
                    let _ = write!(sql, " WHEN ({op_sql}) = ({cond}) THEN {result}");
                } else {
                    let _ = write!(sql, " WHEN {cond} THEN {result}");
                }
            }
            if let Some(ref el) = else_expr {
                let el_sql = transform_expr(ctx, builder, el, schema)?;
                let _ = write!(sql, " ELSE {el_sql}");
            }
            sql.push_str(" END");
            Ok(sql)
        }

        Expr::LabelExpr { expr, label } => {
            let var_name = match expr.as_ref() {
                Expr::Identifier(name) => name.as_str(),
                _ => "",
            };
            let inner = transform_expr(ctx, builder, expr, schema)?;
            let alias = inner.trim_end_matches(".id");
            let resolver = ctx
                .get_node_resolver(var_name)
                .unwrap_or(schema.default_node_resolver.as_ref());
            let escaped = escape_sql_string(label);
            if resolver.table() == "nodes" {
                Ok(format!(
                    "EXISTS (SELECT 1 FROM node_labels WHERE node_id = {alias}.id AND label = '{escaped}')"
                ))
            } else {
                Ok("1".to_string())
            }
        }

        Expr::Subscript { expr, index } => {
            let expr_sql = transform_expr(ctx, builder, expr, schema)?;
            let idx_sql = transform_expr(ctx, builder, index, schema)?;
            Ok(format!(
                "json_extract({expr_sql}, '$[' || CAST(CASE WHEN ({idx_sql}) < 0 THEN json_array_length({expr_sql}) + ({idx_sql}) ELSE ({idx_sql}) END AS INTEGER) || ']')"
            ))
        }

        Expr::Exists(exists_expr) => match exists_expr {
            ExistsExpr::Property(prop_expr) => {
                let sql = transform_expr(ctx, builder, prop_expr, schema)?;
                Ok(format!("{sql} IS NOT NULL"))
            }
            ExistsExpr::Pattern(_) => Err(TransformError::UnsupportedExpr(
                "EXISTS pattern".to_string(),
            )),
        },

        Expr::ListComprehension {
            variable,
            list_expr,
            where_expr,
            transform_expr: xform,
        } => {
            let list_sql = transform_expr(ctx, builder, list_expr, schema)?;
            let value_expr = match xform {
                Some(ref x) => transform_list_comp_expr(ctx, builder, x, variable, schema)?,
                None => "json_each.value".to_string(),
            };
            let filter = match where_expr {
                Some(ref w) => {
                    let w_sql = transform_list_comp_expr(ctx, builder, w, variable, schema)?;
                    format!(" WHERE {w_sql}")
                }
                None => String::new(),
            };
            Ok(format!(
                "(SELECT json_group_array({value_expr}) FROM json_each({list_sql}){filter})"
            ))
        }

        Expr::ListPredicate {
            pred_type,
            variable,
            list_expr,
            predicate,
        } => {
            let list_sql = transform_expr(ctx, builder, list_expr, schema)?;
            let pred_sql = transform_list_comp_expr(ctx, builder, predicate, variable, schema)?;

            match pred_type {
                ListPredicateType::Any => Ok(format!(
                    "EXISTS (SELECT 1 FROM json_each({list_sql}) WHERE {pred_sql})"
                )),
                ListPredicateType::All => Ok(format!(
                    "NOT EXISTS (SELECT 1 FROM json_each({list_sql}) WHERE NOT ({pred_sql}))"
                )),
                ListPredicateType::None => Ok(format!(
                    "NOT EXISTS (SELECT 1 FROM json_each({list_sql}) WHERE {pred_sql})"
                )),
                ListPredicateType::Single => Ok(format!(
                    "(SELECT COUNT(*) FROM json_each({list_sql}) WHERE {pred_sql}) = 1"
                )),
            }
        }

        Expr::MapProjection { base_expr, items } => {
            let mut parts = Vec::new();
            for item in items {
                let key = item
                    .key
                    .as_ref()
                    .unwrap_or_else(|| item.property.as_ref().unwrap());
                let prop_name = item.property.as_ref().unwrap();
                let base_var = match base_expr.as_ref() {
                    Expr::Identifier(name) => name.clone(),
                    _ => {
                        return Err(TransformError::Internal(
                            "map projection requires identifier base".into(),
                        ))
                    }
                };
                let prop_sql =
                    transform_property_access_by_name(ctx, builder, &base_var, prop_name, schema)?;
                parts.push(format!("'{key}', {prop_sql}"));
            }
            Ok(format!("json_object({})", parts.join(", ")))
        }

        Expr::Reduce {
            accumulator,
            initial_value,
            variable,
            list_expr,
            expression,
        } => {
            let init_sql = transform_expr(ctx, builder, initial_value, schema)?;
            let list_sql = transform_expr(ctx, builder, list_expr, schema)?;
            let item_ref = format!("json_extract({list_sql}, '$[' || idx || ']')");
            let expr_sql = transform_reduce_body(
                ctx,
                builder,
                expression,
                accumulator,
                variable,
                "acc",
                &item_ref,
                schema,
            )?;
            Ok(format!(
                "(WITH RECURSIVE _reduce AS (SELECT 0 AS idx, ({init_sql}) AS acc UNION ALL SELECT idx + 1, {expr_sql} FROM _reduce WHERE idx < json_array_length({list_sql})) SELECT acc FROM _reduce ORDER BY idx DESC LIMIT 1)"
            ))
        }

        _ => Err(TransformError::UnsupportedExpr(format!("{expr:?}"))),
    }
}

#[allow(clippy::too_many_arguments)]
fn transform_reduce_body<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    expr: &Expr,
    acc_name: &str,
    var_name: &str,
    acc_sql: &str,
    var_sql: &str,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    match expr {
        Expr::BinaryOp { op, left, right } => {
            let left_sql = transform_reduce_body(
                ctx, builder, left, acc_name, var_name, acc_sql, var_sql, schema,
            )?;
            let right_sql = transform_reduce_body(
                ctx, builder, right, acc_name, var_name, acc_sql, var_sql, schema,
            )?;
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Eq => "=",
                BinaryOp::Neq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::Gt => ">",
                BinaryOp::Lte => "<=",
                BinaryOp::Gte => ">=",
                _ => {
                    return Err(TransformError::Internal(format!(
                        "unsupported reduce op: {op:?}"
                    )))
                }
            };
            Ok(format!("({left_sql} {op_str} {right_sql})"))
        }
        Expr::Identifier(name) => {
            if name == acc_name {
                Ok(acc_sql.to_string())
            } else if name == var_name {
                Ok(var_sql.to_string())
            } else {
                transform_expr(ctx, builder, expr, schema)
            }
        }
        _ => transform_expr(ctx, builder, expr, schema),
    }
}

fn transform_list_comp_expr<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    expr: &Expr,
    bound_var: &str,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    match expr {
        Expr::Identifier(name) if name == bound_var => Ok("json_each.value".to_string()),
        Expr::BinaryOp { op, left, right } => {
            let l = transform_list_comp_expr(ctx, builder, left, bound_var, schema)?;
            let r = transform_list_comp_expr(ctx, builder, right, bound_var, schema)?;
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
                BinaryOp::Eq => "=",
                BinaryOp::Neq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::Gt => ">",
                BinaryOp::Lte => "<=",
                BinaryOp::Gte => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                _ => {
                    return Err(TransformError::UnsupportedExpr(format!(
                        "operator {op:?} in list comprehension"
                    )))
                }
            };
            Ok(format!("{l} {op_str} {r}"))
        }
        _ => transform_expr(ctx, builder, expr, schema),
    }
}

fn transform_literal(lit: &Literal) -> String {
    match lit {
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => format!("{f}"),
        Literal::String(s) => format!("'{}'", escape_sql_string(s)),
        Literal::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn unwind_property_chain(expr: &Expr, prop_name: &str) -> Option<(String, String, String)> {
    if let Expr::Property {
        expr: inner,
        name: first_prop,
    } = expr
    {
        let mut path_segments = vec![prop_name.to_string()];
        let mut current_expr = inner.as_ref();
        let mut current_key = first_prop.clone();

        loop {
            match current_expr {
                Expr::Identifier(var_name) => {
                    path_segments.reverse();
                    let json_path = format!("$.{}", path_segments.join("."));
                    return Some((var_name.clone(), current_key, json_path));
                }
                Expr::Property {
                    expr: deeper,
                    name: deeper_prop,
                } => {
                    path_segments.push(current_key);
                    current_key = deeper_prop.clone();
                    current_expr = deeper.as_ref();
                }
                _ => return None,
            }
        }
    }
    None
}

fn transform_property_access<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    expr: &Expr,
    prop_name: &str,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    // Nested property access (e.g., n.metadata.name) uses json_extract — no JOIN needed
    if let Some((var_name, json_key, json_path)) = unwind_property_chain(expr, prop_name) {
        let alias = ctx
            .get_alias(&var_name)
            .ok_or_else(|| TransformError::Internal(format!("unknown variable: {var_name}")))?;
        let is_edge = ctx.get_kind(&var_name) == Some(VarKind::Edge);

        if is_edge {
            let resolver = ctx
                .get_edge_resolver(&var_name)
                .unwrap_or(schema.default_edge_resolver.as_ref());
            return Ok(resolver.nested_property_expr(alias, &json_key, &json_path));
        }
        let resolver = ctx
            .get_node_resolver(&var_name)
            .unwrap_or(schema.default_node_resolver.as_ref());
        return Ok(resolver.nested_property_expr(alias, &json_key, &json_path));
    }

    // Subscript-then-property: n['metadata'].name
    if let Expr::Subscript {
        expr: sub_expr,
        index,
    } = expr
    {
        if let Expr::Identifier(var_name) = sub_expr.as_ref() {
            if let Expr::Literal(Literal::String(sub_key)) = index.as_ref() {
                let alias = ctx.get_alias(var_name).ok_or_else(|| {
                    TransformError::Internal(format!("unknown variable: {var_name}"))
                })?;
                let is_edge = ctx.get_kind(var_name) == Some(VarKind::Edge);
                let json_path = format!("$.{prop_name}");
                if is_edge {
                    let resolver = ctx
                        .get_edge_resolver(var_name)
                        .unwrap_or(schema.default_edge_resolver.as_ref());
                    return Ok(resolver.nested_property_expr(alias, sub_key, &json_path));
                }
                let resolver = ctx
                    .get_node_resolver(var_name)
                    .unwrap_or(schema.default_node_resolver.as_ref());
                return Ok(resolver.nested_property_expr(alias, sub_key, &json_path));
            }
        }
    }

    let alias = match expr {
        Expr::Identifier(name) => ctx
            .get_alias(name)
            .ok_or_else(|| TransformError::Internal(format!("unknown variable: {name}")))?
            .to_string(),
        _ => {
            return Err(TransformError::UnsupportedExpr(
                "nested property access".to_string(),
            ));
        }
    };

    let var_name = match expr {
        Expr::Identifier(name) => name.clone(),
        _ => String::new(),
    };

    let is_edge = ctx.get_kind(&var_name) == Some(VarKind::Edge);
    let prop_index = ctx.next_prop_index();

    let frag = if is_edge {
        let resolver = ctx
            .get_edge_resolver(&var_name)
            .unwrap_or(schema.default_edge_resolver.as_ref());
        resolver.property_expr(&alias, prop_name, prop_index)
    } else {
        let resolver = ctx
            .get_node_resolver(&var_name)
            .unwrap_or(schema.default_node_resolver.as_ref());
        resolver.property_expr(&alias, prop_name, prop_index)
    };

    for j in &frag.joins {
        builder.add_join_aliased(j.join_type, &j.table, &j.alias, &j.on_condition);
    }
    for c in &frag.conditions {
        builder.add_where(c);
    }

    Ok(frag.expr)
}

fn transform_property_access_by_name<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    var_name: &str,
    prop_name: &str,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let ident = Expr::Identifier(var_name.to_string());
    transform_property_access(ctx, builder, &ident, prop_name, schema)
}

// ===== CREATE-only transforms =====

fn transform_create_only<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut wb = WriteBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Create(c) => transform_create_clause(ctx, &mut wb, c, schema)?,
            Clause::Return(_) => {}
            _ => {
                return Err(TransformError::UnsupportedClause(format!(
                    "{clause:?} in CREATE-only query"
                )));
            }
        }
    }

    Ok(wb.build())
}

fn transform_create_clause<'a>(
    ctx: &mut TransformContext<'a>,
    wb: &mut WriteBuilder,
    c: &CreateClause,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    for path in &c.pattern {
        transform_create_path(ctx, wb, path, schema)?;
    }
    Ok(())
}

fn transform_create_path<'a>(
    ctx: &mut TransformContext<'a>,
    wb: &mut WriteBuilder,
    path: &Path,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    for element in &path.elements {
        if let PathElement::Node(node) = element {
            if let Some(ref name) = node.variable {
                if ctx.get_alias(name).is_some() {
                    continue;
                }
            }
            generate_node_create(ctx, wb, node, schema)?;
        }
    }

    for (i, element) in path.elements.iter().enumerate() {
        if let PathElement::Rel(rel) = element {
            let source = match &path.elements[i - 1] {
                PathElement::Node(n) => n,
                _ => unreachable!(),
            };
            let target = match &path.elements[i + 1] {
                PathElement::Node(n) => n,
                _ => unreachable!(),
            };
            generate_relationship_create(ctx, wb, rel, source, target, schema)?;
        }
    }

    Ok(())
}

fn generate_node_create<'a>(
    ctx: &mut TransformContext<'a>,
    wb: &mut WriteBuilder,
    node: &NodePattern,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    let label = node.labels.first().map(|s| s.as_str());
    let resolver = schema.node_resolver(label);

    let props: Vec<(&str, &Expr)> = if let Some(ref props_expr) = node.properties {
        if let Expr::Map(pairs) = props_expr.as_ref() {
            pairs.iter().map(|p| (p.key.as_str(), &p.value)).collect()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let label_str = label.unwrap_or("");
    let stmts = resolver.insert_sql(label_str, &props)?;

    if resolver.table() == "nodes" {
        wb.add_statement(&stmts[0]);

        if let Some(ref name) = node.variable {
            ctx.register_node(name);
            ctx.created_node_order.push(name.clone());
            ctx.node_resolvers.insert(name.clone(), resolver);
        }

        for stmt in &stmts[1..] {
            wb.add_statement(stmt);
        }

        let node_id_expr = "(SELECT MAX(id) FROM nodes)";
        for extra_label in node.labels.iter().skip(1) {
            let escaped = escape_sql_string(extra_label);
            wb.add_statement(&format!(
                "INSERT INTO node_labels (node_id, label) VALUES ({node_id_expr}, '{escaped}')"
            ));
        }
    } else {
        if let Some(ref name) = node.variable {
            ctx.register_node(name);
            ctx.created_node_order.push(name.clone());
            ctx.node_resolvers.insert(name.clone(), resolver);
        }

        for stmt in &stmts {
            wb.add_statement(stmt);
        }
    }

    Ok(())
}

fn node_id_sql<'a>(ctx: &TransformContext<'a>, node: &NodePattern) -> String {
    if let Some(ref name) = node.variable {
        let total = ctx.created_node_order.len();
        if let Some(pos) = ctx.created_node_order.iter().position(|n| n == name) {
            let offset = total - 1 - pos;
            if offset == 0 {
                "(SELECT MAX(id) FROM nodes)".to_string()
            } else {
                format!("((SELECT MAX(id) FROM nodes) - {offset})")
            }
        } else {
            "last_insert_rowid()".to_string()
        }
    } else {
        "last_insert_rowid()".to_string()
    }
}

fn generate_relationship_create<'a>(
    ctx: &mut TransformContext<'a>,
    wb: &mut WriteBuilder,
    rel: &RelPattern,
    source: &NodePattern,
    target: &NodePattern,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    let rel_type = if !rel.rel_types.is_empty() {
        escape_sql_string(&rel.rel_types[0])
    } else {
        String::new()
    };

    let source_ref = node_id_sql(ctx, source);
    let target_ref = node_id_sql(ctx, target);

    let (src_sql, tgt_sql) = match rel.direction {
        Direction::Left => (target_ref, source_ref),
        _ => (source_ref, target_ref),
    };

    let rel_type_key = rel.rel_types.first().map(|s| s.as_str());
    let resolver = schema.edge_resolver(rel_type_key);

    let stmts = resolver.create_sql(&src_sql, &tgt_sql, &rel_type);
    for stmt in stmts {
        wb.add_statement(&stmt);
    }

    if let Some(ref name) = rel.variable {
        ctx.register_edge(name);
        ctx.edge_resolvers.insert(name.clone(), resolver);
    }

    Ok(())
}

// ===== MATCH + CREATE =====

fn transform_match_create<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut match_builder = SqlBuilder::new();
    let mut wb = WriteBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => transform_match_clause(ctx, &mut match_builder, m, schema)?,
            Clause::Create(c) => {
                for path in &c.pattern {
                    for (i, element) in path.elements.iter().enumerate() {
                        if let PathElement::Rel(rel) = element {
                            let source = match &path.elements[i - 1] {
                                PathElement::Node(n) => n,
                                _ => unreachable!(),
                            };
                            let target = match &path.elements[i + 1] {
                                PathElement::Node(n) => n,
                                _ => unreachable!(),
                            };

                            let rel_type = if !rel.rel_types.is_empty() {
                                escape_sql_string(&rel.rel_types[0])
                            } else {
                                String::new()
                            };

                            let source_alias = source
                                .variable
                                .as_ref()
                                .and_then(|n| ctx.get_alias(n))
                                .ok_or_else(|| {
                                    TransformError::Internal(
                                        "source node not found in context".to_string(),
                                    )
                                })?;
                            let target_alias = target
                                .variable
                                .as_ref()
                                .and_then(|n| ctx.get_alias(n))
                                .ok_or_else(|| {
                                    TransformError::Internal(
                                        "target node not found in context".to_string(),
                                    )
                                })?;

                            let select_sql = match_builder.build();
                            let from_part = select_sql
                                .find(" FROM ")
                                .map(|pos| &select_sql[pos..])
                                .unwrap_or("");

                            wb.add_statement(&format!(
                                "INSERT INTO edges (source_id, target_id, type) SELECT {source_alias}.id, {target_alias}.id, '{rel_type}'{from_part}"
                            ));
                        }
                    }
                }
            }
            Clause::Return(_) => {}
            _ => {}
        }
    }

    Ok(wb.build())
}

// ===== MATCH + SET =====

fn transform_match_set<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut builder = SqlBuilder::new();
    let mut wb = WriteBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => transform_match_clause(ctx, &mut builder, m, schema)?,
            Clause::Set(s) => {
                for item in &s.items {
                    if let Expr::Property {
                        expr: ref target_expr,
                        name: ref prop_name,
                    } = *item.property
                    {
                        if let Expr::Identifier(ref var_name) = **target_expr {
                            let alias = ctx
                                .get_alias(var_name)
                                .ok_or_else(|| {
                                    TransformError::Internal(format!(
                                        "unknown variable: {var_name}"
                                    ))
                                })?
                                .to_string();
                            let var_name = var_name.clone();

                            let value_sql = transform_expr(ctx, &mut builder, &item.expr, schema)?;
                            let is_json = matches!(*item.expr, Expr::Map(_) | Expr::List(_));
                            let is_edge = ctx.get_kind(&var_name) == Some(VarKind::Edge);

                            let from_part = {
                                let sql = builder.build();
                                sql.find(" FROM ")
                                    .map(|pos| sql[pos..].to_string())
                                    .unwrap_or_default()
                            };

                            if is_edge {
                                let escaped_key = escape_sql_string(prop_name);
                                let table = if is_json {
                                    "edge_props_json"
                                } else {
                                    "edge_props_text"
                                };
                                wb.add_statement(&format!(
                                    "INSERT OR IGNORE INTO property_keys (key) VALUES ('{escaped_key}')"
                                ));
                                wb.add_statement(&format!(
                                    "INSERT OR REPLACE INTO {table} (edge_id, key_id, value) \
SELECT {alias}.id, (SELECT id FROM property_keys WHERE key = '{escaped_key}'), {value_sql} \
{from_part}"
                                ));
                            } else {
                                let resolver = ctx
                                    .get_node_resolver(&var_name)
                                    .unwrap_or(schema.default_node_resolver.as_ref());
                                let stmts = resolver.set_property_sql(
                                    &alias, &from_part, prop_name, &value_sql, is_json,
                                )?;
                                for stmt in stmts {
                                    wb.add_statement(&stmt);
                                }
                            }
                        }
                    }
                }
            }
            Clause::Return(_) => {}
            _ => {}
        }
    }

    Ok(wb.build())
}

// ===== MATCH + DELETE =====

fn transform_match_delete<'a>(
    query: &'a Query,
    ctx: &mut TransformContext<'a>,
    schema: &'a GraphSchema,
) -> Result<String, TransformError> {
    let mut builder = SqlBuilder::new();
    let mut wb = WriteBuilder::new();

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => transform_match_clause(ctx, &mut builder, m, schema)?,
            Clause::Delete(d) => {
                let full_sql = builder.build();
                let from_part = full_sql
                    .find(" FROM ")
                    .map(|pos| &full_sql[pos..])
                    .unwrap_or("");

                for var_name in &d.items {
                    let alias = ctx.get_alias(var_name).ok_or_else(|| {
                        TransformError::Internal(format!("unknown variable: {var_name}"))
                    })?;

                    let kind = ctx.get_kind(var_name).unwrap_or(VarKind::Node);

                    match kind {
                        VarKind::Node => {
                            let resolver = ctx
                                .get_node_resolver(var_name)
                                .unwrap_or(schema.default_node_resolver.as_ref());
                            let stmts = resolver.delete_sql(alias, from_part, d.detach);
                            for stmt in stmts {
                                wb.add_statement(&stmt);
                            }
                        }
                        VarKind::Edge => {
                            wb.add_statement(&format!(
                                "DELETE FROM edges WHERE id IN (SELECT {alias}.id{from_part})"
                            ));
                        }
                        VarKind::Value => continue,
                    }
                }
            }
            Clause::Return(_) => {}
            _ => {}
        }
    }

    Ok(wb.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::GraphSchema;
    use gql_parser::{
        Clause, CreateClause, Direction, Expr, Literal, MatchClause, NodePattern, Path,
        PathElement, PathType, Query, RelPattern, ReturnClause, ReturnItem,
    };

    fn make_match_return_query(match_clause: MatchClause, return_clause: ReturnClause) -> Query {
        Query {
            clauses: vec![Clause::Match(match_clause), Clause::Return(return_clause)],
            explain: false,
        }
    }

    fn person_node(var: &str) -> NodePattern {
        NodePattern {
            variable: Some(var.to_string()),
            labels: vec!["Person".to_string()],
            properties: None,
            where_expr: None,
        }
    }

    #[test]
    fn test_simple_match_return() {
        let schema = GraphSchema::default();
        let query = make_match_return_query(
            MatchClause {
                pattern: vec![Path {
                    elements: vec![PathElement::Node(person_node("n"))],
                    variable: None,
                    path_type: PathType::Normal,
                }],
                where_expr: None,
                optional: false,
                from_graph: None,
            },
            ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Property {
                        expr: Box::new(Expr::Identifier("n".to_string())),
                        name: "name".to_string(),
                    },
                    alias: None,
                }],
                order_by: vec![],
                skip: None,
                limit: None,
            },
        );

        let sql = transform_query(&query, &schema).unwrap();
        assert!(
            sql.contains("FROM nodes"),
            "SQL should contain FROM nodes: {sql}"
        );
        assert!(
            sql.contains("node_labels"),
            "SQL should join node_labels: {sql}"
        );
        assert!(
            sql.contains("Person"),
            "SQL should filter on Person label: {sql}"
        );
        // JOIN-based property lookup across all typed tables
        assert!(
            sql.contains("node_props_text"),
            "SQL should join node_props_text: {sql}"
        );
        assert!(
            sql.contains("node_props_int"),
            "SQL should join node_props_int: {sql}"
        );
        assert!(
            sql.contains("property_keys"),
            "SQL should join property_keys: {sql}"
        );
        assert!(
            sql.contains("COALESCE"),
            "SQL should COALESCE across typed prop tables: {sql}"
        );
    }

    #[test]
    fn test_multi_property_access() {
        let schema = GraphSchema::default();
        let query = make_match_return_query(
            MatchClause {
                pattern: vec![Path {
                    elements: vec![PathElement::Node(person_node("n"))],
                    variable: None,
                    path_type: PathType::Normal,
                }],
                where_expr: None,
                optional: false,
                from_graph: None,
            },
            ReturnClause {
                distinct: false,
                items: vec![
                    ReturnItem {
                        expr: Expr::Property {
                            expr: Box::new(Expr::Identifier("n".to_string())),
                            name: "name".to_string(),
                        },
                        alias: None,
                    },
                    ReturnItem {
                        expr: Expr::Property {
                            expr: Box::new(Expr::Identifier("n".to_string())),
                            name: "role".to_string(),
                        },
                        alias: None,
                    },
                ],
                order_by: vec![],
                skip: None,
                limit: None,
            },
        );

        let sql = transform_query(&query, &schema).unwrap();
        // Should have two separate JOIN groups with unique aliases
        assert!(
            sql.contains("_pk__v0_0") && sql.contains("_pk__v0_1"),
            "SQL should have two property key aliases: {sql}"
        );
        assert!(
            sql.contains("'name'") && sql.contains("'role'"),
            "SQL should filter on both property keys: {sql}"
        );
    }

    #[test]
    fn test_create_node() {
        let schema = GraphSchema::default();
        let query = Query {
            clauses: vec![Clause::Create(CreateClause {
                pattern: vec![Path {
                    elements: vec![PathElement::Node(NodePattern {
                        variable: Some("n".to_string()),
                        labels: vec!["Person".to_string()],
                        properties: Some(Box::new(Expr::Map(vec![
                            gql_parser::MapPair {
                                key: "name".to_string(),
                                value: Expr::Literal(Literal::String("Alice".to_string())),
                            },
                            gql_parser::MapPair {
                                key: "age".to_string(),
                                value: Expr::Literal(Literal::Integer(30)),
                            },
                        ]))),
                        where_expr: None,
                    })],
                    variable: None,
                    path_type: PathType::Normal,
                }],
            })],
            explain: false,
        };

        let sql = transform_query(&query, &schema).unwrap();
        assert!(
            sql.contains("INSERT INTO nodes"),
            "Should insert node: {sql}"
        );
        assert!(sql.contains("node_labels"), "Should insert label: {sql}");
        assert!(sql.contains("Person"), "Should include Person label: {sql}");
        assert!(
            sql.contains("property_keys"),
            "Should insert property keys: {sql}"
        );
        assert!(sql.contains("Alice"), "Should include Alice: {sql}");
    }

    #[test]
    fn test_create_with_relationship() {
        let schema = GraphSchema::default();
        let query = Query {
            clauses: vec![Clause::Create(CreateClause {
                pattern: vec![Path {
                    elements: vec![
                        PathElement::Node(NodePattern {
                            variable: Some("a".to_string()),
                            labels: vec!["Person".to_string()],
                            properties: Some(Box::new(Expr::Map(vec![gql_parser::MapPair {
                                key: "name".to_string(),
                                value: Expr::Literal(Literal::String("Alice".to_string())),
                            }]))),
                            where_expr: None,
                        }),
                        PathElement::Rel(RelPattern {
                            variable: Some("r".to_string()),
                            rel_types: vec!["KNOWS".to_string()],
                            properties: None,
                            where_expr: None,
                            direction: Direction::Right,
                            varlen: None,
                        }),
                        PathElement::Node(NodePattern {
                            variable: Some("b".to_string()),
                            labels: vec!["Person".to_string()],
                            properties: Some(Box::new(Expr::Map(vec![gql_parser::MapPair {
                                key: "name".to_string(),
                                value: Expr::Literal(Literal::String("Bob".to_string())),
                            }]))),
                            where_expr: None,
                        }),
                    ],
                    variable: None,
                    path_type: PathType::Normal,
                }],
            })],
            explain: false,
        };

        let sql = transform_query(&query, &schema).unwrap();
        assert!(
            sql.contains("INSERT INTO edges"),
            "Should insert edge: {sql}"
        );
        assert!(sql.contains("KNOWS"), "Should include KNOWS type: {sql}");
    }

    #[test]
    fn test_edge_traversal_ivm_compatible() {
        let gql = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS knower, b.name AS known";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &GraphSchema::default()).unwrap();
        // All JOINs must use column=column ON (IVM-compatible)
        assert!(
            sql.contains("JOIN edges AS _v1 ON _v1.source_id = _v0.id"),
            "edge join: {sql}"
        );
        assert!(
            sql.contains("JOIN nodes AS _v2 ON _v1.target_id = _v2.id"),
            "target join: {sql}"
        );
        // Literal filters must be in WHERE, not ON
        assert!(sql.contains("WHERE"), "must have WHERE: {sql}");
        assert!(
            sql.contains("_v1.type = 'KNOWS'"),
            "type filter in WHERE: {sql}"
        );
        // No CROSS JOIN or 1=1
        assert!(!sql.contains("1=1"), "no literal ON conditions: {sql}");
        assert!(!sql.contains("CROSS"), "no CROSS JOIN: {sql}");
    }

    #[test]
    fn test_fk_varlen_cte_base_case_is_self_contained() {
        use crate::resolver::*;

        // Schema matching holon's: blocks table with CHILD_OF FK edge
        let mut nodes: std::collections::HashMap<String, Box<dyn NodeResolver>> =
            std::collections::HashMap::new();
        nodes.insert(
            "Block".into(),
            Box::new(MappedNodeResolver {
                table_name: "blocks".into(),
                id_col: "id".into(),
                label: "Block".into(),
                columns: vec![
                    ColumnMapping {
                        property_name: "id".into(),
                        column_name: "id".into(),
                    },
                    ColumnMapping {
                        property_name: "parent_id".into(),
                        column_name: "parent_id".into(),
                    },
                    ColumnMapping {
                        property_name: "content".into(),
                        column_name: "content".into(),
                    },
                ],
            }),
        );

        let mut edges: std::collections::HashMap<String, EdgeDef> =
            std::collections::HashMap::new();
        edges.insert(
            "CHILD_OF".into(),
            EdgeDef {
                source_label: Some("Block".into()),
                target_label: Some("Block".into()),
                resolver: Box::new(ForeignKeyEdgeResolver {
                    fk_table: "blocks".into(),
                    fk_column: "parent_id".into(),
                    target_table: "blocks".into(),
                    target_id_column: "id".into(),
                }),
            },
        );

        let schema = GraphSchema {
            nodes,
            edges,
            default_node_resolver: Box::new(EavNodeResolver),
            default_edge_resolver: Box::new(EavEdgeResolver),
            raw_return: false,
        };

        // GQL: variable-length CHILD_OF path
        let gql = "MATCH (root:Block)<-[:CHILD_OF*1..3]-(d:Block) \
                    WHERE root.id = 'test-root-id' \
                    RETURN d.id, d.content LIMIT 5";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &schema).unwrap();

        // The recursive CTE base case must be self-contained — it must NOT
        // reference outer query aliases like _v0.id without its own FROM clause.
        // A CTE that does `SELECT _v0.id AS node_id` without FROM is invalid SQL
        // because _v0 is defined in the outer query, not in the CTE scope.
        // This causes Turso's actor to crash with "Actor response channel closed".
        let cte_start = sql
            .find("WITH RECURSIVE")
            .expect("should have recursive CTE");
        let cte_body_start = sql[cte_start..].find('(').unwrap() + cte_start + 1;
        let union_pos = sql[cte_body_start..].find("UNION ALL").unwrap() + cte_body_start;
        let base_case = &sql[cte_body_start..union_pos];

        // The base case references a table alias (e.g. _v0.id). For this to be
        // valid SQL, the alias must be defined within the CTE's own FROM clause.
        if base_case.contains("_v") {
            assert!(
                base_case.to_uppercase().contains("FROM"),
                "CTE base case references a table alias but has no FROM clause, \
                 making it an invalid correlated CTE.\nBase case: {base_case}\nFull SQL: {sql}"
            );
        }
    }

    #[test]
    fn test_fk_varlen_no_cross_join() {
        use crate::resolver::*;

        // Turso IVM requires simple column references in join conditions.
        // Variable-length paths must NOT produce `ON 1 = 1` (cross join) —
        // the target table must be joined via a proper column reference to the CTE.

        let mut nodes: std::collections::HashMap<String, Box<dyn NodeResolver>> =
            std::collections::HashMap::new();
        nodes.insert(
            "Block".into(),
            Box::new(MappedNodeResolver {
                table_name: "blocks".into(),
                id_col: "id".into(),
                label: "Block".into(),
                columns: vec![
                    ColumnMapping {
                        property_name: "id".into(),
                        column_name: "id".into(),
                    },
                    ColumnMapping {
                        property_name: "parent_id".into(),
                        column_name: "parent_id".into(),
                    },
                    ColumnMapping {
                        property_name: "content".into(),
                        column_name: "content".into(),
                    },
                ],
            }),
        );

        let mut edges: std::collections::HashMap<String, EdgeDef> =
            std::collections::HashMap::new();
        edges.insert(
            "CHILD_OF".into(),
            EdgeDef {
                source_label: Some("Block".into()),
                target_label: Some("Block".into()),
                resolver: Box::new(ForeignKeyEdgeResolver {
                    fk_table: "blocks".into(),
                    fk_column: "parent_id".into(),
                    target_table: "blocks".into(),
                    target_id_column: "id".into(),
                }),
            },
        );

        let schema = GraphSchema {
            nodes,
            edges,
            default_node_resolver: Box::new(EavNodeResolver),
            default_edge_resolver: Box::new(EavEdgeResolver),
            raw_return: false,
        };

        let gql = "MATCH (root:Block)<-[:CHILD_OF*1..20]-(d:Block) \
                    WHERE root.parent_id = 'holon-doc://test' \
                    RETURN d.id, d.parent_id, d.content";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &schema).unwrap();

        assert!(
            !sql.contains("ON 1 = 1") && !sql.contains("ON 1=1"),
            "Generated SQL must not contain cross joins (ON 1=1) — \
             Turso IVM requires simple column references in join conditions.\n\
             Generated SQL: {sql}"
        );

        // The target table should be joined to the CTE via a proper column reference
        assert!(
            sql.contains("JOIN") && sql.contains(".id = "),
            "Target table must be joined to CTE via column reference (e.g., target.id = cte.node_id).\n\
             Generated SQL: {sql}"
        );
    }

    #[test]
    fn test_fk_varlen_multihop_cte_seed_is_self_contained() {
        use crate::resolver::*;

        // Reproduces the Main Panel pattern: (cf:Focus)-[:FOCUSES_ON]->(root:Block)<-[:CHILD_OF*1..20]-(d:Block)
        // The CTE seed for the varlen path must reference `blocks` (root's table), NOT
        // `current_focus` (cf's table). And conditions on `cf` must NOT leak into the CTE seed.

        let mut nodes: std::collections::HashMap<String, Box<dyn NodeResolver>> =
            std::collections::HashMap::new();
        nodes.insert(
            "CurrentFocus".into(),
            Box::new(MappedNodeResolver {
                table_name: "current_focus".into(),
                id_col: "region".into(),
                label: "CurrentFocus".into(),
                columns: vec![
                    ColumnMapping {
                        property_name: "region".into(),
                        column_name: "region".into(),
                    },
                    ColumnMapping {
                        property_name: "block_id".into(),
                        column_name: "block_id".into(),
                    },
                ],
            }),
        );
        nodes.insert(
            "Block".into(),
            Box::new(MappedNodeResolver {
                table_name: "blocks".into(),
                id_col: "id".into(),
                label: "Block".into(),
                columns: vec![
                    ColumnMapping {
                        property_name: "id".into(),
                        column_name: "id".into(),
                    },
                    ColumnMapping {
                        property_name: "parent_id".into(),
                        column_name: "parent_id".into(),
                    },
                    ColumnMapping {
                        property_name: "content".into(),
                        column_name: "content".into(),
                    },
                ],
            }),
        );

        let mut edges: std::collections::HashMap<String, EdgeDef> =
            std::collections::HashMap::new();
        edges.insert(
            "FOCUSES_ON".into(),
            EdgeDef {
                source_label: Some("CurrentFocus".into()),
                target_label: Some("Block".into()),
                resolver: Box::new(ForeignKeyEdgeResolver {
                    fk_table: "current_focus".into(),
                    fk_column: "block_id".into(),
                    target_table: "blocks".into(),
                    target_id_column: "parent_id".into(),
                }),
            },
        );
        edges.insert(
            "CHILD_OF".into(),
            EdgeDef {
                source_label: Some("Block".into()),
                target_label: Some("Block".into()),
                resolver: Box::new(ForeignKeyEdgeResolver {
                    fk_table: "blocks".into(),
                    fk_column: "parent_id".into(),
                    target_table: "blocks".into(),
                    target_id_column: "id".into(),
                }),
            },
        );

        let schema = GraphSchema {
            nodes,
            edges,
            default_node_resolver: Box::new(EavNodeResolver),
            default_edge_resolver: Box::new(EavEdgeResolver),
            raw_return: false,
        };

        let gql =
            "MATCH (cf:CurrentFocus)-[:FOCUSES_ON]->(root:Block)<-[:CHILD_OF*1..20]-(d:Block) \
                    WHERE cf.region = 'main' \
                    RETURN d.id, d.parent_id, d.content";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &schema).unwrap();
        // CTE seed must use `blocks` (root's table), not `current_focus` (cf's table)
        let cte_start = sql
            .find("WITH RECURSIVE")
            .expect("should have recursive CTE");
        let cte_body_start = sql[cte_start..].find('(').unwrap() + cte_start + 1;
        let union_pos = sql[cte_body_start..].find("UNION ALL").unwrap() + cte_body_start;
        let base_case = &sql[cte_body_start..union_pos];

        assert!(
            base_case.contains("FROM blocks"),
            "CTE seed must use the varlen source node's table ('blocks'), \
             not the preceding node's table.\nBase case: {base_case}\nFull SQL: {sql}"
        );

        // cf.region condition must NOT appear in CTE seed (it belongs to the outer query)
        assert!(
            !base_case.contains("region"),
            "CTE seed must not contain conditions from other nodes (cf.region).\n\
             Base case: {base_case}\nFull SQL: {sql}"
        );

        // No cross joins
        assert!(
            !sql.contains("ON 1 = 1") && !sql.contains("ON 1=1"),
            "No cross joins allowed.\nGenerated SQL: {sql}"
        );
    }

    #[test]
    fn test_return_star() {
        let gql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN *";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &GraphSchema::default()).unwrap();
        // All three variables should appear as aliased columns
        assert!(sql.contains("\"a\""), "Should have column 'a': {sql}");
        assert!(sql.contains("\"r\""), "Should have column 'r': {sql}");
        assert!(sql.contains("\"b\""), "Should have column 'b': {sql}");
    }

    #[test]
    fn test_return_star_nodes_only() {
        let gql = "MATCH (n:Person) RETURN *";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let sql = transform_query(&query, &GraphSchema::default()).unwrap();
        assert!(sql.contains("\"n\""), "Should have column 'n': {sql}");
    }

    #[test]
    fn test_return_star_raw() {
        let gql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN *";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let schema = GraphSchema {
            raw_return: true,
            ..GraphSchema::default()
        };
        let sql = transform_query(&query, &schema).unwrap();
        assert!(sql.contains("_v0.*"), "Should have _v0.*: {sql}");
        assert!(sql.contains("_v1.*"), "Should have _v1.*: {sql}");
        assert!(sql.contains("_v2.*"), "Should have _v2.*: {sql}");
        assert!(
            !sql.contains("json_object"),
            "Should not have json_object: {sql}"
        );
    }

    #[test]
    fn test_return_var_raw() {
        let gql = "MATCH (a:Person) RETURN a";
        let parsed = gql_parser::parse(gql).unwrap();
        let query = match parsed {
            gql_parser::QueryOrUnion::Query(q) => q,
            _ => panic!("expected query"),
        };
        let schema = GraphSchema {
            raw_return: true,
            ..GraphSchema::default()
        };
        let sql = transform_query(&query, &schema).unwrap();
        assert!(sql.contains("_v0.*"), "Should have _v0.*: {sql}");
        assert!(
            !sql.contains("json_object"),
            "Should not have json_object: {sql}"
        );
    }
}
