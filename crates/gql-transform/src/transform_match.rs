use std::collections::HashMap;
use std::fmt::Write;

use gql_parser::{
    BinaryOp, Clause, CreateClause, Direction, ExistsExpr, Expr, ForClause, ListPredicateType,
    Literal, MatchClause, NodePattern, OrderByItem, Path, PathElement, PathType, Query, RelPattern,
    ReturnClause, ReturnItem, WithClause,
};

use crate::resolver::{EdgeResolver, GraphSchema, NodeResolver};
use crate::sql_builder::{escape_sql_string, JoinType, SqlBuilder, WriteBuilder};
use crate::TransformError;

struct TransformContext<'a> {
    /// Maps variable name -> SQL alias (e.g., "n" -> "_v0")
    var_aliases: HashMap<String, String>,
    /// Maps variable name -> variable kind
    var_kinds: HashMap<String, VarKind>,
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
            alias_counter: 0,
            prop_counter: 0,
            created_node_order: Vec::new(),
            path_vars: HashMap::new(),
            node_resolvers: HashMap::new(),
            edge_resolvers: HashMap::new(),
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
        alias
    }

    fn register_edge(&mut self, name: &str) -> String {
        if let Some(alias) = self.var_aliases.get(name) {
            return alias.clone();
        }
        let alias = self.next_alias();
        self.var_aliases.insert(name.to_string(), alias.clone());
        self.var_kinds.insert(name.to_string(), VarKind::Edge);
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

pub fn transform_query(query: &Query, schema: &GraphSchema) -> Result<String, TransformError> {
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
    query: &Query,
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
    query: &Query,
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
    Ok(())
}

fn transform_match_clause<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    m: &MatchClause,
    schema: &'a GraphSchema,
) -> Result<(), TransformError> {
    for path in &m.pattern {
        transform_path(ctx, builder, path, m.optional, schema)?;
    }

    if let Some(ref where_expr) = m.where_expr {
        let sql = transform_expr(ctx, builder, where_expr, schema)?;
        builder.add_where(&sql);
    }

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
                path_element_aliases.push((alias, VarKind::Node));
            }
            PathElement::Rel(rel) => {
                assert!(
                    i >= 1 && i + 1 < path.elements.len(),
                    "malformed path: Rel must be between two Nodes"
                );
                let source_alias = node_aliases[node_aliases.len() - 1].clone();
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

    let (join_col, select_col) = match rel.direction {
        Direction::Left => ("target_id", "source_id"),
        _ => ("source_id", "target_id"),
    };

    let type_filter = if !rel.rel_types.is_empty() {
        format!(
            " AND e.type IN ({})",
            rel.rel_types
                .iter()
                .map(|t| format!("'{}'", escape_sql_string(t)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };

    let cte_sql = format!(
        "SELECT {source_alias}.id AS node_id, 0 AS depth, \
         CAST({source_alias}.id AS TEXT) AS visited \
         UNION ALL \
         SELECT e.{select_col}, {cte_name}.depth + 1, \
         {cte_name}.visited || ',' || CAST(e.{select_col} AS TEXT) \
         FROM {cte_name} \
         JOIN edges e ON e.{join_col} = {cte_name}.node_id{type_filter} \
         WHERE {cte_name}.depth < {max_hops} \
         AND ',' || {cte_name}.visited || ',' NOT LIKE '%,' || CAST(e.{select_col} AS TEXT) || ',%'"
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

fn transform_varlen_segment<'a>(
    ctx: &mut TransformContext<'a>,
    builder: &mut SqlBuilder,
    source_alias: &str,
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

    let type_filter = if !rel.rel_types.is_empty() {
        format!(
            " AND e.type IN ({})",
            rel.rel_types
                .iter()
                .map(|t| format!("'{}'", escape_sql_string(t)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };

    let cycle_check = |col: &str| -> String {
        format!("',' || {cte_name}.visited || ',' NOT LIKE '%,' || CAST(e.{col} AS TEXT) || ',%'")
    };

    let recursive_part = match rel.direction {
        Direction::Both | Direction::None => {
            format!(
                "SELECT e.target_id, {cte_name}.depth + 1, \
                 {cte_name}.visited || ',' || CAST(e.target_id AS TEXT) \
                 FROM {cte_name} \
                 JOIN edges e ON e.source_id = {cte_name}.node_id{type_filter} \
                 WHERE {cte_name}.depth < {max_hops} \
                 AND {cycle_fwd} \
                 UNION ALL \
                 SELECT e.source_id, {cte_name}.depth + 1, \
                 {cte_name}.visited || ',' || CAST(e.source_id AS TEXT) \
                 FROM {cte_name} \
                 JOIN edges e ON e.target_id = {cte_name}.node_id{type_filter} \
                 WHERE {cte_name}.depth < {max_hops} \
                 AND {cycle_rev}",
                cycle_fwd = cycle_check("target_id"),
                cycle_rev = cycle_check("source_id"),
            )
        }
        Direction::Left => {
            format!(
                "SELECT e.source_id, {cte_name}.depth + 1, \
                 {cte_name}.visited || ',' || CAST(e.source_id AS TEXT) \
                 FROM {cte_name} \
                 JOIN edges e ON e.target_id = {cte_name}.node_id{type_filter} \
                 WHERE {cte_name}.depth < {max_hops} \
                 AND {cycle}",
                cycle = cycle_check("source_id"),
            )
        }
        Direction::Right => {
            format!(
                "SELECT e.target_id, {cte_name}.depth + 1, \
                 {cte_name}.visited || ',' || CAST(e.target_id AS TEXT) \
                 FROM {cte_name} \
                 JOIN edges e ON e.source_id = {cte_name}.node_id{type_filter} \
                 WHERE {cte_name}.depth < {max_hops} \
                 AND {cycle}",
                cycle = cycle_check("target_id"),
            )
        }
    };

    let cte_sql = format!(
        "SELECT {source_alias}.id AS node_id, 0 AS depth, \
         CAST({source_alias}.id AS TEXT) AS visited \
         UNION ALL \
         {recursive_part}"
    );

    builder.add_cte_recursive(&cte_name, &cte_sql);

    let target_alias = transform_node_pattern(ctx, builder, target_node, optional, schema)?;

    builder.add_where(&format!(
        "{target_alias}.id IN (SELECT node_id FROM {cte_name} \
         WHERE depth >= {min_hops} AND depth <= {max_hops})"
    ));

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

    Ok(alias)
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

    let has_aggregate = r.items.iter().any(|item| expr_contains_aggregate(&item.expr));
    let mut non_aggregate_exprs = Vec::new();

    for item in &r.items {
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
            operand
                .as_ref()
                .is_some_and(|e| expr_contains_aggregate(e))
                || when_clauses.iter().any(|w| {
                    expr_contains_aggregate(&w.condition)
                        || expr_contains_aggregate(&w.result)
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
                    let resolver = ctx
                        .get_node_resolver(name)
                        .unwrap_or(schema.default_node_resolver.as_ref());
                    Ok(resolver.node_json_object(alias))
                }
                Some(VarKind::Edge) => {
                    let resolver = ctx
                        .get_edge_resolver(name)
                        .unwrap_or(schema.default_edge_resolver.as_ref());
                    Ok(resolver.edge_json_object(alias))
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
        Expr::Property { expr, name } => {
            if let Expr::Identifier(var) = expr.as_ref() {
                return Some(format!("{var}.{name}"));
            }
            None
        }
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
    query: &Query,
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
    query: &Query,
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
    query: &Query,
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
    query: &Query,
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
        // JOIN-based property lookup
        assert!(
            sql.contains("node_props_text"),
            "SQL should join node_props_text: {sql}"
        );
        assert!(
            sql.contains("property_keys"),
            "SQL should join property_keys: {sql}"
        );
        assert!(
            !sql.contains("COALESCE"),
            "SQL should NOT contain scalar subquery COALESCE: {sql}"
        );
        assert!(
            !sql.contains("SELECT npt.value FROM"),
            "SQL should NOT contain scalar subquery: {sql}"
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
        // Should have two separate JOIN pairs with unique aliases
        assert!(
            sql.contains("_npt__v0_0"),
            "SQL should have first prop alias: {sql}"
        );
        assert!(
            sql.contains("_npt__v0_1"),
            "SQL should have second prop alias: {sql}"
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
}
