use std::collections::{HashMap, HashSet};

use gql_parser::{BinaryOp, Expr, MatchClause, Path, PathElement};

/// MATCH WHERE conditions distributed to node variables.
///
/// The planning phase walks the AST without generating SQL, distributing
/// conditions to the node variables they reference. This separates the
/// "what does the query mean" question from the "how to express it in SQL"
/// question, ensuring that CTE seed construction has access to ALL conditions
/// for a source node — including MATCH WHERE conditions that would otherwise
/// only be available after the CTE is already built.
pub struct DistributedConditions<'a> {
    /// Conditions referencing exactly one known node variable.
    pub per_node: HashMap<&'a str, Vec<&'a Expr>>,
    /// Conditions referencing multiple variables, no variables, or only edge variables.
    /// Does NOT include bridge conditions (those are in `bridges`).
    pub general: Vec<&'a Expr>,
    /// Conditions that bridge two disconnected paths (comma-separated patterns).
    /// Keyed by the index of the later path. These should be used as JOIN ON
    /// conditions instead of cross-joining.
    pub bridges: HashMap<usize, Vec<&'a Expr>>,
}

/// Plan a MATCH clause by distributing WHERE conditions to node variables.
///
/// Walks the pattern to discover node variable names, splits the MATCH WHERE
/// on top-level ANDs, then assigns each sub-condition to the single node
/// variable it references (or to `general` if it references zero or multiple).
pub fn plan_match_clause<'a>(m: &'a MatchClause) -> DistributedConditions<'a> {
    let known_vars = collect_node_variables(&m.pattern);
    let mut per_node: HashMap<&'a str, Vec<&'a Expr>> = HashMap::new();
    let mut general: Vec<&'a Expr> = Vec::new();

    if let Some(ref where_expr) = m.where_expr {
        let conditions = split_and_conditions(where_expr);
        for cond in conditions {
            let refs = referenced_variables(cond);
            let node_refs: Vec<&str> = refs
                .iter()
                .filter(|v| known_vars.contains(**v))
                .copied()
                .collect();

            if node_refs.len() == 1 {
                per_node.entry(node_refs[0]).or_default().push(cond);
            } else {
                general.push(cond);
            }
        }
    }

    // Identify bridge conditions: general conditions that span exactly two
    // disconnected paths. These can be used as JOIN ON conditions instead of
    // producing a cross-join.
    let mut bridges: HashMap<usize, Vec<&'a Expr>> = HashMap::new();
    let mut remaining_general: Vec<&'a Expr> = Vec::new();

    if m.pattern.len() > 1 {
        let path_vars: Vec<HashSet<&str>> = m
            .pattern
            .iter()
            .map(|p| collect_node_variables_from_path(p))
            .collect();

        for cond in &general {
            let refs = referenced_variables(cond);
            let mut touched_paths: HashSet<usize> = HashSet::new();
            for var in &refs {
                for (i, pv) in path_vars.iter().enumerate() {
                    if pv.contains(*var) {
                        touched_paths.insert(i);
                    }
                }
            }

            if touched_paths.len() == 2 {
                let later_path = *touched_paths.iter().max().unwrap();
                bridges.entry(later_path).or_default().push(cond);
            } else {
                remaining_general.push(cond);
            }
        }
        general = remaining_general;
    }

    DistributedConditions {
        per_node,
        general,
        bridges,
    }
}

/// Find all GQL variable names referenced in an expression.
pub fn referenced_variables<'a>(expr: &'a Expr) -> HashSet<&'a str> {
    let mut vars = HashSet::new();
    collect_variables(expr, &mut vars);
    vars
}

/// Split a WHERE expression on top-level ANDs into individual conditions.
pub fn split_and_conditions<'a>(expr: &'a Expr) -> Vec<&'a Expr> {
    let mut result = Vec::new();
    split_and_recursive(expr, &mut result);
    result
}

fn collect_node_variables<'a>(patterns: &'a [Path]) -> HashSet<&'a str> {
    let mut vars = HashSet::new();
    for path in patterns {
        vars.extend(collect_node_variables_from_path(path));
    }
    vars
}

fn collect_node_variables_from_path<'a>(path: &'a Path) -> HashSet<&'a str> {
    let mut vars = HashSet::new();
    for element in &path.elements {
        if let PathElement::Node(node) = element {
            if let Some(ref name) = node.variable {
                vars.insert(name.as_str());
            }
        }
    }
    vars
}

fn collect_variables<'a>(expr: &'a Expr, vars: &mut HashSet<&'a str>) {
    match expr {
        Expr::Property { expr, .. } => {
            if let Expr::Identifier(name) = expr.as_ref() {
                vars.insert(name.as_str());
            } else {
                collect_variables(expr, vars);
            }
        }
        Expr::Identifier(name) => {
            vars.insert(name.as_str());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_variables(left, vars);
            collect_variables(right, vars);
        }
        Expr::Not(inner) => collect_variables(inner, vars),
        Expr::NullCheck { expr, .. } => collect_variables(expr, vars),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_variables(arg, vars);
            }
        }
        Expr::LabelExpr { expr, .. } => collect_variables(expr, vars),
        Expr::List(items) => {
            for item in items {
                collect_variables(item, vars);
            }
        }
        Expr::Map(pairs) => {
            for pair in pairs {
                collect_variables(&pair.value, vars);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_variables(op, vars);
            }
            for wc in when_clauses {
                collect_variables(&wc.condition, vars);
                collect_variables(&wc.result, vars);
            }
            if let Some(el) = else_expr {
                collect_variables(el, vars);
            }
        }
        Expr::Subscript { expr, index } => {
            collect_variables(expr, vars);
            collect_variables(index, vars);
        }
        Expr::Exists(exists) => match exists {
            gql_parser::ExistsExpr::Property(e) => collect_variables(e, vars),
            gql_parser::ExistsExpr::Pattern { paths, where_expr } => {
                // Walk the inner pattern. Variables introduced inside the
                // pattern but absent from the outer scope will be filtered by
                // the caller's intersection with `known_vars` (plan.rs:30-92);
                // returning all of them here is correct.
                for p in paths {
                    for el in &p.elements {
                        if let gql_parser::PathElement::Node(n) = el {
                            if let Some(v) = &n.variable {
                                vars.insert(v.as_str());
                            }
                            if let Some(w) = &n.where_expr {
                                collect_variables(w, vars);
                            }
                        }
                    }
                }
                if let Some(w) = where_expr {
                    collect_variables(w, vars);
                }
            }
        },
        Expr::ListComprehension {
            list_expr,
            where_expr,
            transform_expr,
            ..
        } => {
            collect_variables(list_expr, vars);
            if let Some(w) = where_expr {
                collect_variables(w, vars);
            }
            if let Some(t) = transform_expr {
                collect_variables(t, vars);
            }
        }
        Expr::ListPredicate {
            list_expr,
            predicate,
            ..
        } => {
            collect_variables(list_expr, vars);
            collect_variables(predicate, vars);
        }
        Expr::Reduce {
            initial_value,
            list_expr,
            expression,
            ..
        } => {
            collect_variables(initial_value, vars);
            collect_variables(list_expr, vars);
            collect_variables(expression, vars);
        }
        Expr::MapProjection {
            base_expr, items, ..
        } => {
            collect_variables(base_expr, vars);
            for item in items {
                if let Some(e) = &item.expr {
                    collect_variables(e, vars);
                }
            }
        }
        Expr::PatternComprehension {
            where_expr,
            collect_expr,
            ..
        } => {
            if let Some(w) = where_expr {
                collect_variables(w, vars);
            }
            if let Some(c) = collect_expr {
                collect_variables(c, vars);
            }
        }
        Expr::Literal(_) | Expr::Parameter(_) => {}
    }
}

fn split_and_recursive<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        op: BinaryOp::And,
        left,
        right,
    } = expr
    {
        split_and_recursive(left, out);
        split_and_recursive(right, out);
    } else {
        out.push(expr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gql_parser::{Literal, MatchClause, NodePattern, Path, PathElement, PathType, RelPattern};

    #[test]
    fn test_referenced_variables_simple_property() {
        let expr = Expr::Property {
            expr: Box::new(Expr::Identifier("n".to_string())),
            name: "id".to_string(),
        };
        let vars = referenced_variables(&expr);
        assert_eq!(vars, HashSet::from(["n"]));
    }

    #[test]
    fn test_referenced_variables_binary_op() {
        let expr = Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Property {
                expr: Box::new(Expr::Identifier("root".to_string())),
                name: "id".to_string(),
            }),
            right: Box::new(Expr::Literal(Literal::String("x".to_string()))),
        };
        let vars = referenced_variables(&expr);
        assert_eq!(vars, HashSet::from(["root"]));
    }

    #[test]
    fn test_referenced_variables_multi() {
        let expr = Expr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Property {
                    expr: Box::new(Expr::Identifier("a".to_string())),
                    name: "x".to_string(),
                }),
                right: Box::new(Expr::Property {
                    expr: Box::new(Expr::Identifier("b".to_string())),
                    name: "y".to_string(),
                }),
            }),
            right: Box::new(Expr::Literal(Literal::Integer(1))),
        };
        let vars = referenced_variables(&expr);
        assert_eq!(vars, HashSet::from(["a", "b"]));
    }

    #[test]
    fn test_split_and_conditions() {
        let expr = Expr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expr::Literal(Literal::Integer(1))),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOp::And,
                left: Box::new(Expr::Literal(Literal::Integer(2))),
                right: Box::new(Expr::Literal(Literal::Integer(3))),
            }),
        };
        let parts = split_and_conditions(&expr);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_plan_distributes_single_var_conditions() {
        let m = MatchClause {
            pattern: vec![Path {
                elements: vec![
                    PathElement::Node(NodePattern {
                        variable: Some("root".to_string()),
                        labels: vec!["Block".to_string()],
                        properties: None,
                        where_expr: None,
                    }),
                    PathElement::Rel(RelPattern {
                        variable: None,
                        rel_types: vec!["CHILD_OF".to_string()],
                        properties: None,
                        where_expr: None,
                        direction: gql_parser::Direction::Left,
                        varlen: Some(gql_parser::VarLenRange {
                            min_hops: Some(1),
                            max_hops: Some(3),
                        }),
                    }),
                    PathElement::Node(NodePattern {
                        variable: Some("d".to_string()),
                        labels: vec!["Block".to_string()],
                        properties: None,
                        where_expr: None,
                    }),
                ],
                variable: None,
                path_type: PathType::Normal,
            }],
            where_expr: Some(Box::new(Expr::BinaryOp {
                op: BinaryOp::And,
                left: Box::new(Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Property {
                        expr: Box::new(Expr::Identifier("root".to_string())),
                        name: "id".to_string(),
                    }),
                    right: Box::new(Expr::Literal(Literal::String("x".to_string()))),
                }),
                right: Box::new(Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Property {
                        expr: Box::new(Expr::Identifier("d".to_string())),
                        name: "content".to_string(),
                    }),
                    right: Box::new(Expr::Literal(Literal::String("hello".to_string()))),
                }),
            })),
            optional: false,
            from_graph: None,
        };

        let plan = plan_match_clause(&m);
        assert_eq!(plan.per_node.len(), 2);
        assert_eq!(plan.per_node["root"].len(), 1);
        assert_eq!(plan.per_node["d"].len(), 1);
        assert!(plan.general.is_empty());
    }

    #[test]
    fn test_plan_multi_var_condition_goes_to_general() {
        let m = MatchClause {
            pattern: vec![Path {
                elements: vec![
                    PathElement::Node(NodePattern {
                        variable: Some("a".to_string()),
                        labels: vec![],
                        properties: None,
                        where_expr: None,
                    }),
                    PathElement::Rel(RelPattern {
                        variable: None,
                        rel_types: vec![],
                        properties: None,
                        where_expr: None,
                        direction: gql_parser::Direction::Right,
                        varlen: None,
                    }),
                    PathElement::Node(NodePattern {
                        variable: Some("b".to_string()),
                        labels: vec![],
                        properties: None,
                        where_expr: None,
                    }),
                ],
                variable: None,
                path_type: PathType::Normal,
            }],
            where_expr: Some(Box::new(Expr::BinaryOp {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Property {
                    expr: Box::new(Expr::Identifier("a".to_string())),
                    name: "x".to_string(),
                }),
                right: Box::new(Expr::Property {
                    expr: Box::new(Expr::Identifier("b".to_string())),
                    name: "y".to_string(),
                }),
            })),
            optional: false,
            from_graph: None,
        };

        let plan = plan_match_clause(&m);
        assert!(plan.per_node.is_empty());
        assert_eq!(plan.general.len(), 1);
    }
}
