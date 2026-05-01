use std::collections::HashMap;

use gql_parser::{Direction, Expr, Literal};

use crate::sql_builder::{escape_sql_string, JoinType};
use crate::TransformError;

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ---- Fragment types returned by resolvers ----

#[derive(Debug, Clone)]
pub struct JoinFragment {
    pub join_type: JoinType,
    pub table: String,
    pub alias: String,
    pub on_condition: String,
}

#[derive(Debug, Clone)]
pub struct PropertyFragment {
    pub expr: String,
    pub joins: Vec<JoinFragment>,
    pub conditions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FilterFragment {
    pub joins: Vec<JoinFragment>,
    pub conditions: Vec<String>,
}

// ---- Node resolver trait ----

pub trait NodeResolver: Send + Sync {
    fn table(&self) -> &str;
    fn id_column(&self) -> &str;

    fn label_joins(&self, alias: &str, label: &str, label_index: usize) -> PropertyFragment;

    fn property_filter(
        &self,
        alias: &str,
        property: &str,
        value_sql: &str,
        value_expr: &Expr,
    ) -> Result<FilterFragment, TransformError>;

    fn property_expr(
        &self,
        alias: &str,
        property: &str,
        prop_index: usize,
    ) -> Result<PropertyFragment, TransformError>;

    fn all_properties_expr(&self, alias: &str) -> String;
    fn labels_expr(&self, alias: &str) -> String;
    fn node_json_object(&self, alias: &str) -> String;

    fn insert_sql(
        &self,
        label: &str,
        props: &[(&str, &Expr)],
    ) -> Result<Vec<String>, TransformError>;

    fn set_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
        value_sql: &str,
        is_json: bool,
    ) -> Result<Vec<String>, TransformError>;

    fn delete_sql(&self, alias: &str, from_clause: &str, detach: bool) -> Vec<String>;

    fn remove_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
    ) -> Result<Vec<String>, TransformError>;

    fn remove_label_sql(&self, alias: &str, from_clause: &str, label: &str) -> Vec<String>;

    fn nested_property_expr(
        &self,
        alias: &str,
        json_key: &str,
        json_path: &str,
    ) -> Result<String, TransformError>;

    /// Returns true if `property` is a multi-valued property backed by an
    /// inverted-index table (Patch 4). The compiler dispatches IN/NOT-IN
    /// against such properties to a different codepath.
    fn is_multi_value_property(&self, _property: &str) -> bool {
        false
    }

    /// Generates SQL for `<member_sql> IN <alias>.<property>` when `property`
    /// is a multi-value property. Default impl returns an internal error.
    fn multi_value_membership_expr(
        &self,
        _alias: &str,
        property: &str,
        _member_sql: &str,
    ) -> Result<String, TransformError> {
        Err(TransformError::Internal(format!(
            "multi_value_membership_expr called on resolver that does not support property '{property}'"
        )))
    }

    /// Generates SQL projecting all members of `<alias>.<property>` as a JSON
    /// array when `property` is a multi-value property.
    fn multi_value_aggregate_expr(
        &self,
        _alias: &str,
        property: &str,
    ) -> Result<String, TransformError> {
        Err(TransformError::Internal(format!(
            "multi_value_aggregate_expr called on resolver that does not support property '{property}'"
        )))
    }
}

// ---- Edge resolver trait ----

#[derive(Debug, Clone)]
pub struct RecursiveStep {
    /// Expression for the next node ID (e.g., `e.target_id` or `_fk.id`)
    pub next_node_expr: String,
    /// FROM/JOIN clause (e.g., `JOIN edges e ON e.source_id = _vl0.node_id`)
    pub from_clause: String,
    /// Additional WHERE conditions (type filter, etc.)
    pub where_conditions: Vec<String>,
}

pub trait EdgeResolver: Send + Sync {
    fn table(&self) -> &str;

    fn traverse_joins(
        &self,
        source_alias: &str,
        target_alias: &str,
        edge_alias: &str,
        direction: &Direction,
        optional: bool,
    ) -> (Vec<JoinFragment>, Vec<String>);

    fn type_filter(&self, edge_alias: &str, rel_types: &[String]) -> Vec<String>;

    fn property_expr(&self, alias: &str, property: &str, prop_index: usize) -> PropertyFragment;
    fn all_properties_expr(&self, alias: &str) -> String;
    fn edge_json_object(&self, alias: &str) -> String;
    fn nested_property_expr(&self, alias: &str, json_key: &str, json_path: &str) -> String;

    fn create_sql(&self, source_id_expr: &str, target_id_expr: &str, rel_type: &str)
        -> Vec<String>;

    fn recursive_step(
        &self,
        cte_name: &str,
        direction: &Direction,
        rel_types: &[String],
    ) -> RecursiveStep {
        let type_filter = if !rel_types.is_empty() {
            let types = rel_types
                .iter()
                .map(|t| format!("'{}'", escape_sql_string(t)))
                .collect::<Vec<_>>()
                .join(", ");
            vec![format!("e.type IN ({})", types)]
        } else {
            vec![]
        };
        match direction {
            Direction::Left => RecursiveStep {
                next_node_expr: "e.source_id".to_string(),
                from_clause: format!("JOIN edges e ON e.target_id = {cte_name}.node_id"),
                where_conditions: type_filter,
            },
            _ => RecursiveStep {
                next_node_expr: "e.target_id".to_string(),
                from_clause: format!("JOIN edges e ON e.source_id = {cte_name}.node_id"),
                where_conditions: type_filter,
            },
        }
    }
}

// ---- GraphSchema registry ----

pub struct EdgeDef {
    pub source_label: Option<String>,
    pub target_label: Option<String>,
    pub resolver: Box<dyn EdgeResolver>,
}

pub struct GraphSchema {
    pub nodes: HashMap<String, Box<dyn NodeResolver>>,
    pub edges: HashMap<String, EdgeDef>,
    pub default_node_resolver: Box<dyn NodeResolver>,
    pub default_edge_resolver: Box<dyn EdgeResolver>,
    pub raw_return: bool,
}

impl Default for GraphSchema {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            default_node_resolver: Box::new(EavNodeResolver),
            default_edge_resolver: Box::new(EavEdgeResolver),
            raw_return: false,
        }
    }
}

impl GraphSchema {
    pub fn node_resolver(&self, label: Option<&str>) -> &dyn NodeResolver {
        label
            .and_then(|l| self.nodes.get(l))
            .map(|b| b.as_ref())
            .unwrap_or(self.default_node_resolver.as_ref())
    }

    pub fn edge_resolver(&self, rel_type: Option<&str>) -> &dyn EdgeResolver {
        rel_type
            .and_then(|t| self.edges.get(t))
            .map(|d| d.resolver.as_ref())
            .unwrap_or(self.default_edge_resolver.as_ref())
    }
}

// ---- EAV Node Resolver ----

pub struct EavNodeResolver;

impl NodeResolver for EavNodeResolver {
    fn table(&self) -> &str {
        "nodes"
    }

    fn id_column(&self) -> &str {
        "id"
    }

    fn label_joins(&self, alias: &str, label: &str, label_index: usize) -> PropertyFragment {
        let nl_alias = format!("_nl_{alias}_{label_index}");
        let escaped = escape_sql_string(label);
        PropertyFragment {
            expr: String::new(),
            joins: vec![JoinFragment {
                join_type: JoinType::Inner,
                table: "node_labels".to_string(),
                alias: nl_alias.clone(),
                on_condition: format!("{nl_alias}.node_id = {alias}.id"),
            }],
            conditions: vec![format!("{nl_alias}.label = '{escaped}'")],
        }
    }

    fn property_filter(
        &self,
        alias: &str,
        property: &str,
        value_sql: &str,
        value_expr: &Expr,
    ) -> Result<FilterFragment, TransformError> {
        let pk_alias = format!("_pk_{alias}_{property}");
        let prop_alias = format!("_pp_{alias}_{property}");

        let props_table = match value_expr {
            Expr::Literal(Literal::Integer(_)) => "node_props_int",
            Expr::Literal(Literal::Float(_)) => "node_props_real",
            Expr::Literal(Literal::Boolean(_)) => "node_props_bool",
            _ => "node_props_text",
        };

        let escaped_key = escape_sql_string(property);
        Ok(FilterFragment {
            joins: vec![
                JoinFragment {
                    join_type: JoinType::Inner,
                    table: props_table.to_string(),
                    alias: prop_alias.clone(),
                    on_condition: format!("{prop_alias}.node_id = {alias}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Inner,
                    table: "property_keys".to_string(),
                    alias: pk_alias.clone(),
                    on_condition: format!("{prop_alias}.key_id = {pk_alias}.id"),
                },
            ],
            conditions: vec![
                format!("{pk_alias}.key = '{escaped_key}'"),
                format!("{prop_alias}.value = {value_sql}"),
            ],
        })
    }

    fn property_expr(
        &self,
        alias: &str,
        property: &str,
        prop_index: usize,
    ) -> Result<PropertyFragment, TransformError> {
        let p = escape_sql_string(property);
        let pk = format!("_pk_{alias}_{prop_index}");
        let npt = format!("_npt_{alias}_{prop_index}");
        let npi = format!("_npi_{alias}_{prop_index}");
        let npr = format!("_npr_{alias}_{prop_index}");
        let npb = format!("_npb_{alias}_{prop_index}");
        let npj = format!("_npj_{alias}_{prop_index}");
        Ok(PropertyFragment {
            expr: format!(
                "COALESCE({npt}.value, {npi}.value, {npr}.value, {npb}.value, {npj}.value)"
            ),
            joins: vec![
                JoinFragment {
                    join_type: JoinType::Inner,
                    table: "property_keys".to_string(),
                    alias: pk.clone(),
                    on_condition: format!("{pk}.key = '{p}'"),
                },
                JoinFragment {
                    join_type: JoinType::Left,
                    table: "node_props_text".to_string(),
                    alias: npt.clone(),
                    on_condition: format!("{npt}.node_id = {alias}.id AND {npt}.key_id = {pk}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Left,
                    table: "node_props_int".to_string(),
                    alias: npi.clone(),
                    on_condition: format!("{npi}.node_id = {alias}.id AND {npi}.key_id = {pk}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Left,
                    table: "node_props_real".to_string(),
                    alias: npr.clone(),
                    on_condition: format!("{npr}.node_id = {alias}.id AND {npr}.key_id = {pk}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Left,
                    table: "node_props_bool".to_string(),
                    alias: npb.clone(),
                    on_condition: format!("{npb}.node_id = {alias}.id AND {npb}.key_id = {pk}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Left,
                    table: "node_props_json".to_string(),
                    alias: npj.clone(),
                    on_condition: format!("{npj}.node_id = {alias}.id AND {npj}.key_id = {pk}.id"),
                },
            ],
            conditions: vec![],
        })
    }

    fn all_properties_expr(&self, alias: &str) -> String {
        format!(
            "COALESCE((SELECT json_group_object(pk.key, COALESCE(\
(SELECT npt.value FROM node_props_text npt WHERE npt.node_id = {alias}.id AND npt.key_id = pk.id), \
(SELECT npi.value FROM node_props_int npi WHERE npi.node_id = {alias}.id AND npi.key_id = pk.id), \
(SELECT npr.value FROM node_props_real npr WHERE npr.node_id = {alias}.id AND npr.key_id = pk.id), \
(SELECT npb.value FROM node_props_bool npb WHERE npb.node_id = {alias}.id AND npb.key_id = pk.id), \
(SELECT json(npj.value) FROM node_props_json npj WHERE npj.node_id = {alias}.id AND npj.key_id = pk.id))) \
FROM property_keys pk WHERE \
EXISTS (SELECT 1 FROM node_props_text WHERE node_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM node_props_int WHERE node_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM node_props_real WHERE node_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM node_props_bool WHERE node_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM node_props_json WHERE node_id = {alias}.id AND key_id = pk.id)\
), json('{{{alias}}}'))"
        )
    }

    fn labels_expr(&self, alias: &str) -> String {
        format!(
            "COALESCE((SELECT json_group_array(label) FROM node_labels WHERE node_id = {alias}.id), json('[]'))"
        )
    }

    fn node_json_object(&self, alias: &str) -> String {
        format!(
            "json_object(\
'id', {a}.id, \
'labels', {labels}, \
'properties', {props})",
            a = alias,
            labels = self.labels_expr(alias),
            props = self.all_properties_expr(alias),
        )
    }

    fn insert_sql(
        &self,
        label: &str,
        props: &[(&str, &Expr)],
    ) -> Result<Vec<String>, TransformError> {
        let mut stmts = vec!["INSERT INTO nodes DEFAULT VALUES".to_string()];

        let node_id_expr = "(SELECT MAX(id) FROM nodes)";
        let escaped_label = escape_sql_string(label);
        stmts.push(format!(
            "INSERT INTO node_labels (node_id, label) VALUES ({node_id_expr}, '{escaped_label}')"
        ));

        for (key, value) in props {
            let escaped_key = escape_sql_string(key);
            let (suffix, value_sql) = eav_expr_to_sql_value(value)?;
            if suffix == "null" {
                continue;
            }
            stmts.push(format!(
                "INSERT OR IGNORE INTO property_keys (key) VALUES ('{escaped_key}')"
            ));
            stmts.push(format!(
                "INSERT OR REPLACE INTO node_props_{suffix} (node_id, key_id, value) VALUES (\
{node_id_expr}, \
(SELECT id FROM property_keys WHERE key = '{escaped_key}'), \
{value_sql})"
            ));
        }

        Ok(stmts)
    }

    fn set_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
        value_sql: &str,
        is_json: bool,
    ) -> Result<Vec<String>, TransformError> {
        let escaped_key = escape_sql_string(property);
        let table = if is_json {
            "node_props_json"
        } else {
            "node_props_text"
        };
        Ok(vec![
            format!("INSERT OR IGNORE INTO property_keys (key) VALUES ('{escaped_key}')"),
            format!(
                "INSERT OR REPLACE INTO {table} (node_id, key_id, value) \
SELECT {alias}.id, (SELECT id FROM property_keys WHERE key = '{escaped_key}'), {value_sql} \
{from_clause}"
            ),
        ])
    }

    fn delete_sql(&self, alias: &str, from_clause: &str, detach: bool) -> Vec<String> {
        let mut stmts = Vec::new();
        if detach {
            stmts.push(format!(
                "DELETE FROM edges WHERE source_id IN (SELECT {alias}.id{from_clause}) OR target_id IN (SELECT {alias}.id{from_clause})"
            ));
        }
        stmts.push(format!(
            "DELETE FROM nodes WHERE id IN (SELECT {alias}.id{from_clause})"
        ));
        stmts
    }

    fn remove_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
    ) -> Result<Vec<String>, TransformError> {
        let escaped_key = escape_sql_string(property);
        Ok([
            "node_props_text",
            "node_props_int",
            "node_props_real",
            "node_props_bool",
            "node_props_json",
        ]
        .iter()
        .map(|table| {
            format!(
                "DELETE FROM {table} WHERE node_id IN (SELECT {alias}.id{from_clause}) \
AND key_id = (SELECT id FROM property_keys WHERE key = '{escaped_key}')"
            )
        })
        .collect())
    }

    fn remove_label_sql(&self, alias: &str, from_clause: &str, label: &str) -> Vec<String> {
        let escaped = escape_sql_string(label);
        vec![format!(
            "DELETE FROM node_labels WHERE node_id IN (SELECT {alias}.id{from_clause}) \
AND label = '{escaped}'"
        )]
    }

    fn nested_property_expr(
        &self,
        alias: &str,
        json_key: &str,
        json_path: &str,
    ) -> Result<String, TransformError> {
        let escaped_key = escape_sql_string(json_key);
        Ok(format!(
            "json_extract(\
(SELECT json(npj.value) FROM node_props_json npj JOIN property_keys pk ON npj.key_id = pk.id WHERE npj.node_id = {alias}.id AND pk.key = '{escaped_key}'), \
'{json_path}')"
        ))
    }
}

// ---- EAV Edge Resolver ----

pub struct EavEdgeResolver;

impl EdgeResolver for EavEdgeResolver {
    fn table(&self) -> &str {
        "edges"
    }

    fn traverse_joins(
        &self,
        source_alias: &str,
        target_alias: &str,
        edge_alias: &str,
        direction: &Direction,
        optional: bool,
    ) -> (Vec<JoinFragment>, Vec<String>) {
        let (edge_on, target_on) = match direction {
            Direction::Left => (
                format!("{edge_alias}.target_id = {source_alias}.id"),
                format!("{edge_alias}.source_id = {target_alias}.id"),
            ),
            _ => (
                format!("{edge_alias}.source_id = {source_alias}.id"),
                format!("{edge_alias}.target_id = {target_alias}.id"),
            ),
        };

        let jt = if optional {
            JoinType::Left
        } else {
            JoinType::Inner
        };

        let joins = vec![JoinFragment {
            join_type: jt,
            table: "edges".to_string(),
            alias: edge_alias.to_string(),
            on_condition: edge_on,
        }];

        (joins, vec![target_on])
    }

    fn type_filter(&self, edge_alias: &str, rel_types: &[String]) -> Vec<String> {
        if rel_types.len() == 1 {
            let escaped = escape_sql_string(&rel_types[0]);
            vec![format!("{edge_alias}.type = '{escaped}'")]
        } else if rel_types.len() > 1 {
            let conditions: Vec<String> = rel_types
                .iter()
                .map(|t| {
                    let escaped = escape_sql_string(t);
                    format!("{edge_alias}.type = '{escaped}'")
                })
                .collect();
            vec![format!("({})", conditions.join(" OR "))]
        } else {
            vec![]
        }
    }

    fn property_expr(&self, alias: &str, property: &str, prop_index: usize) -> PropertyFragment {
        let p = escape_sql_string(property);
        let ept = format!("_ept_{alias}_{prop_index}");
        let pk = format!("_epk_{alias}_{prop_index}");
        PropertyFragment {
            expr: format!("{ept}.value"),
            joins: vec![
                JoinFragment {
                    join_type: JoinType::Inner,
                    table: "edge_props_text".to_string(),
                    alias: ept.clone(),
                    on_condition: format!("{ept}.edge_id = {alias}.id"),
                },
                JoinFragment {
                    join_type: JoinType::Inner,
                    table: "property_keys".to_string(),
                    alias: pk.clone(),
                    on_condition: format!("{ept}.key_id = {pk}.id"),
                },
            ],
            conditions: vec![format!("{pk}.key = '{p}'")],
        }
    }

    fn all_properties_expr(&self, alias: &str) -> String {
        format!(
            "COALESCE((SELECT json_group_object(pk.key, COALESCE(\
(SELECT ept.value FROM edge_props_text ept WHERE ept.edge_id = {alias}.id AND ept.key_id = pk.id), \
(SELECT epi.value FROM edge_props_int epi WHERE epi.edge_id = {alias}.id AND epi.key_id = pk.id), \
(SELECT epr.value FROM edge_props_real epr WHERE epr.edge_id = {alias}.id AND epr.key_id = pk.id), \
(SELECT epb.value FROM edge_props_bool epb WHERE epb.edge_id = {alias}.id AND epb.key_id = pk.id), \
(SELECT json(epj.value) FROM edge_props_json epj WHERE epj.edge_id = {alias}.id AND epj.key_id = pk.id))) \
FROM property_keys pk WHERE \
EXISTS (SELECT 1 FROM edge_props_text WHERE edge_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM edge_props_int WHERE edge_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM edge_props_real WHERE edge_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM edge_props_bool WHERE edge_id = {alias}.id AND key_id = pk.id) OR \
EXISTS (SELECT 1 FROM edge_props_json WHERE edge_id = {alias}.id AND key_id = pk.id)\
), json('{{{alias}}}'))"
        )
    }

    fn edge_json_object(&self, alias: &str) -> String {
        format!(
            "json_object(\
'id', {a}.id, \
'type', {a}.type, \
'startNodeId', {a}.source_id, \
'endNodeId', {a}.target_id, \
'properties', {props})",
            a = alias,
            props = self.all_properties_expr(alias),
        )
    }

    fn nested_property_expr(&self, alias: &str, json_key: &str, json_path: &str) -> String {
        let escaped_key = escape_sql_string(json_key);
        format!(
            "json_extract(\
(SELECT json(epj.value) FROM edge_props_json epj JOIN property_keys pk ON epj.key_id = pk.id WHERE epj.edge_id = {alias}.id AND pk.key = '{escaped_key}'), \
'{json_path}')"
        )
    }

    fn create_sql(
        &self,
        source_id_expr: &str,
        target_id_expr: &str,
        rel_type: &str,
    ) -> Vec<String> {
        let escaped = escape_sql_string(rel_type);
        vec![format!(
            "INSERT INTO edges (source_id, target_id, type) VALUES ({source_id_expr}, {target_id_expr}, '{escaped}')"
        )]
    }
}

// ---- Mapped Node Resolver (for relational tables) ----

pub struct ColumnMapping {
    pub property_name: String,
    pub column_name: String,
}

/// Inverted-index storage backing for a multi-valued property (Patch 4).
///
/// E.g. `tags` on `block` is logically a list-valued property, physically
/// stored as `block_tags(block_id, tag)`. Backing this lets `'x' IN b.tags`
/// compile to an index-friendly `EXISTS (SELECT 1 FROM block_tags WHERE
/// block_id = b.id AND tag = 'x')`.
#[derive(Debug, Clone)]
pub struct MultiValueBacking {
    pub table: String,
    pub source_column: String,
    pub value_column: String,
}

/// Classifies how a property name resolves to SQL for `MappedNodeResolver`.
enum ColumnRef<'a> {
    /// Property maps directly to a real column on the entity's table.
    Direct(String),
    /// Property is unmapped; project from a JSON extension column.
    JsonExtension { col: String, key: String },
    /// Property is multi-valued, backed by an inverted-index table (Patch 4).
    MultiValue(&'a MultiValueBacking),
}

pub struct MappedNodeResolver {
    pub table_name: String,
    pub id_col: String,
    pub label: String,
    pub columns: Vec<ColumnMapping>,
    /// Optional JSON column on the entity's table that backs unmapped property
    /// names. When set, `b.<unknown>` lowers to `json_extract({alias}.{col},
    /// '$.<unknown>')` rather than failing or emitting a broken bare-column
    /// reference. (Patch 1.)
    pub extension_column: Option<String>,
    /// Properties stored in inverted-index tables. (Patch 4.)
    pub multi_value_properties: HashMap<String, MultiValueBacking>,
}

impl NodeResolver for MappedNodeResolver {
    fn table(&self) -> &str {
        &self.table_name
    }

    fn id_column(&self) -> &str {
        &self.id_col
    }

    fn label_joins(&self, _alias: &str, _label: &str, _label_index: usize) -> PropertyFragment {
        PropertyFragment {
            expr: String::new(),
            joins: vec![],
            conditions: vec![],
        }
    }

    fn property_filter(
        &self,
        alias: &str,
        property: &str,
        value_sql: &str,
        _value_expr: &Expr,
    ) -> Result<FilterFragment, TransformError> {
        let cond = match self.column_for(property)? {
            ColumnRef::Direct(col) => {
                format!("{alias}.{} = {value_sql}", quote_ident(&col))
            }
            ColumnRef::JsonExtension { col, key } => {
                format!(
                    "json_extract({alias}.{}, '$.{}') = {value_sql}",
                    quote_ident(&col),
                    escape_sql_string(&key),
                )
            }
            ColumnRef::MultiValue(_) => {
                return Err(TransformError::UnsupportedExpr(format!(
                    "property '{property}' on '{}' is multi-valued — use IN (e.g. '<x> IN b.{property}'), not equality",
                    self.label
                )));
            }
        };
        Ok(FilterFragment {
            joins: vec![],
            conditions: vec![cond],
        })
    }

    fn property_expr(
        &self,
        alias: &str,
        property: &str,
        _prop_index: usize,
    ) -> Result<PropertyFragment, TransformError> {
        let expr = match self.column_for(property)? {
            ColumnRef::Direct(col) => format!("{alias}.{}", quote_ident(&col)),
            ColumnRef::JsonExtension { col, key } => format!(
                "json_extract({alias}.{}, '$.{}')",
                quote_ident(&col),
                escape_sql_string(&key),
            ),
            ColumnRef::MultiValue(backing) => {
                self.aggregate_sql(alias, backing)
            }
        };
        Ok(PropertyFragment {
            expr,
            joins: vec![],
            conditions: vec![],
        })
    }

    fn all_properties_expr(&self, alias: &str) -> String {
        let mut pairs: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                format!(
                    "'{}', {alias}.{}",
                    c.property_name,
                    quote_ident(&c.column_name)
                )
            })
            .collect();
        // Project multi-value properties as JSON arrays alongside scalar columns.
        for (prop_name, backing) in &self.multi_value_properties {
            let agg = self.aggregate_sql(alias, backing);
            pairs.push(format!("'{}', {agg}", escape_sql_string(prop_name)));
        }
        let mapped_obj = format!("json_object({})", pairs.join(", "));
        // If an extension column is configured, merge the JSON blob over the
        // mapped projection so unknown-but-stored properties surface in
        // `RETURN b`. `json_patch` is right-merge semantics in SQLite json1.
        match &self.extension_column {
            Some(col) => format!(
                "json_patch({mapped_obj}, COALESCE({alias}.{}, json('{{}}')))",
                quote_ident(col),
            ),
            None => mapped_obj,
        }
    }

    fn labels_expr(&self, _alias: &str) -> String {
        format!("json_array('{}')", escape_sql_string(&self.label))
    }

    fn node_json_object(&self, alias: &str) -> String {
        format!(
            "json_object('id', {alias}.{id}, 'labels', {labels}, 'properties', {props})",
            id = quote_ident(&self.id_col),
            labels = self.labels_expr(alias),
            props = self.all_properties_expr(alias),
        )
    }

    fn insert_sql(
        &self,
        _label: &str,
        props: &[(&str, &Expr)],
    ) -> Result<Vec<String>, TransformError> {
        let mut col_assignments: Vec<(String, String)> = Vec::new();
        let mut json_extension_pairs: Vec<(String, String)> = Vec::new();
        let mut multi_value_inserts: Vec<(MultiValueBacking, String)> = Vec::new();
        for (key, value) in props {
            let (_, val_sql) = eav_expr_to_sql_value(value)?;
            match self.column_for(key)? {
                ColumnRef::Direct(col) => {
                    col_assignments.push((col, val_sql));
                }
                ColumnRef::JsonExtension { col: _, key: json_key } => {
                    json_extension_pairs.push((json_key, val_sql));
                }
                ColumnRef::MultiValue(backing) => {
                    multi_value_inserts.push((backing.clone(), val_sql));
                }
            }
        }

        let mut col_names: Vec<String> = col_assignments
            .iter()
            .map(|(c, _)| quote_ident(c))
            .collect();
        let mut values: Vec<String> = col_assignments.into_iter().map(|(_, v)| v).collect();

        if !json_extension_pairs.is_empty() {
            let ext_col = self.extension_column.as_ref().expect(
                "JsonExtension ColumnRef returned without extension_column set",
            );
            let json_object_args: Vec<String> = json_extension_pairs
                .iter()
                .map(|(k, v)| format!("'{}', {v}", escape_sql_string(k)))
                .collect();
            col_names.push(quote_ident(ext_col));
            values.push(format!("json_object({})", json_object_args.join(", ")));
        }

        if !multi_value_inserts.is_empty() {
            // Multi-value property writes via CREATE require knowing the new
            // row's id, which is generated by the primary INSERT. This needs
            // a multi-statement plan that's beyond what insert_sql currently
            // returns (single primary INSERT); reject for now.
            return Err(TransformError::UnsupportedExpr(
                "CREATE writing to a multi-valued property is not yet supported"
                    .to_string(),
            ));
        }

        Ok(vec![format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&self.table_name),
            col_names.join(", "),
            values.join(", ")
        )])
    }

    fn set_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
        value_sql: &str,
        _is_json: bool,
    ) -> Result<Vec<String>, TransformError> {
        let table = quote_ident(&self.table_name);
        let id = quote_ident(&self.id_col);
        let stmt = match self.column_for(property)? {
            ColumnRef::Direct(col) => format!(
                "UPDATE {table} SET {} = {value_sql} WHERE {id} IN (SELECT {alias}.{id}{from_clause})",
                quote_ident(&col),
            ),
            ColumnRef::JsonExtension { col, key } => format!(
                "UPDATE {table} SET {ext} = json_set(COALESCE({ext}, json('{{}}')), '$.{}', {value_sql}) WHERE {id} IN (SELECT {alias}.{id}{from_clause})",
                escape_sql_string(&key),
                ext = quote_ident(&col),
            ),
            ColumnRef::MultiValue(_) => {
                return Err(TransformError::UnsupportedExpr(format!(
                    "SET on a multi-valued property '{property}' is not supported — use CREATE/DELETE on the backing edge instead"
                )));
            }
        };
        Ok(vec![stmt])
    }

    fn delete_sql(&self, alias: &str, from_clause: &str, _detach: bool) -> Vec<String> {
        let table = quote_ident(&self.table_name);
        let id = quote_ident(&self.id_col);
        vec![format!(
            "DELETE FROM {table} WHERE {id} IN (SELECT {alias}.{id}{from_clause})"
        )]
    }

    fn remove_property_sql(
        &self,
        alias: &str,
        from_clause: &str,
        property: &str,
    ) -> Result<Vec<String>, TransformError> {
        let table = quote_ident(&self.table_name);
        let id = quote_ident(&self.id_col);
        let stmt = match self.column_for(property)? {
            ColumnRef::Direct(col) => format!(
                "UPDATE {table} SET {} = NULL WHERE {id} IN (SELECT {alias}.{id}{from_clause})",
                quote_ident(&col),
            ),
            ColumnRef::JsonExtension { col, key } => format!(
                "UPDATE {table} SET {ext} = json_remove(COALESCE({ext}, json('{{}}')), '$.{}') WHERE {id} IN (SELECT {alias}.{id}{from_clause})",
                escape_sql_string(&key),
                ext = quote_ident(&col),
            ),
            ColumnRef::MultiValue(_) => {
                return Err(TransformError::UnsupportedExpr(format!(
                    "REMOVE on a multi-valued property '{property}' is not supported"
                )));
            }
        };
        Ok(vec![stmt])
    }

    fn remove_label_sql(&self, _alias: &str, _from_clause: &str, _label: &str) -> Vec<String> {
        vec![]
    }

    fn nested_property_expr(
        &self,
        alias: &str,
        json_key: &str,
        json_path: &str,
    ) -> Result<String, TransformError> {
        match self.column_for(json_key)? {
            ColumnRef::Direct(col) => Ok(format!(
                "json_extract({alias}.{}, '{json_path}')",
                quote_ident(&col),
            )),
            ColumnRef::JsonExtension { col, key } => Ok(format!(
                "json_extract({alias}.{}, '$.{}.{}')",
                quote_ident(&col),
                escape_sql_string(&key),
                // json_path is provided as `$.foo.bar`; strip the leading `$.`
                // so it becomes a sub-path of the extension column key.
                json_path.trim_start_matches("$.").trim_start_matches('$'),
            )),
            ColumnRef::MultiValue(_) => Err(TransformError::UnsupportedExpr(format!(
                "nested property access on multi-valued property '{json_key}' is not supported"
            ))),
        }
    }

    fn is_multi_value_property(&self, property: &str) -> bool {
        self.multi_value_properties.contains_key(property)
    }

    fn multi_value_membership_expr(
        &self,
        alias: &str,
        property: &str,
        member_sql: &str,
    ) -> Result<String, TransformError> {
        let backing = self.multi_value_properties.get(property).ok_or_else(|| {
            TransformError::Internal(format!(
                "multi_value_membership_expr called for non-multi-value property '{property}' on '{}'",
                self.label
            ))
        })?;
        let id = quote_ident(&self.id_col);
        Ok(format!(
            "EXISTS (SELECT 1 FROM {tbl} WHERE {tbl}.{src} = {alias}.{id} AND {tbl}.{val} = {member_sql})",
            tbl = quote_ident(&backing.table),
            src = quote_ident(&backing.source_column),
            val = quote_ident(&backing.value_column),
        ))
    }

    fn multi_value_aggregate_expr(
        &self,
        alias: &str,
        property: &str,
    ) -> Result<String, TransformError> {
        let backing = self.multi_value_properties.get(property).ok_or_else(|| {
            TransformError::Internal(format!(
                "multi_value_aggregate_expr called for non-multi-value property '{property}' on '{}'",
                self.label
            ))
        })?;
        Ok(self.aggregate_sql(alias, backing))
    }
}

impl MappedNodeResolver {
    fn column_for(&self, property: &str) -> Result<ColumnRef<'_>, TransformError> {
        if let Some(c) = self.columns.iter().find(|c| c.property_name == property) {
            return Ok(ColumnRef::Direct(c.column_name.clone()));
        }
        // The GQL identity `b.id` always projects the resolver's id_col.
        if property == "id" {
            return Ok(ColumnRef::Direct(self.id_col.clone()));
        }
        if let Some(backing) = self.multi_value_properties.get(property) {
            return Ok(ColumnRef::MultiValue(backing));
        }
        if let Some(col) = &self.extension_column {
            return Ok(ColumnRef::JsonExtension {
                col: col.clone(),
                key: property.to_string(),
            });
        }
        Err(TransformError::UnknownProperty {
            entity: self.label.clone(),
            property: property.to_string(),
        })
    }

    fn aggregate_sql(&self, alias: &str, backing: &MultiValueBacking) -> String {
        let id = quote_ident(&self.id_col);
        format!(
            "(SELECT COALESCE(json_group_array({tbl}.{val}), json('[]')) FROM {tbl} WHERE {tbl}.{src} = {alias}.{id})",
            tbl = quote_ident(&backing.table),
            src = quote_ident(&backing.source_column),
            val = quote_ident(&backing.value_column),
        )
    }
}

// ---- Foreign Key Edge Resolver ----

pub struct ForeignKeyEdgeResolver {
    pub fk_table: String,
    pub fk_column: String,
    pub target_table: String,
    pub target_id_column: String,
}

impl EdgeResolver for ForeignKeyEdgeResolver {
    fn table(&self) -> &str {
        &self.fk_table
    }

    fn traverse_joins(
        &self,
        source_alias: &str,
        target_alias: &str,
        _edge_alias: &str,
        direction: &Direction,
        _optional: bool,
    ) -> (Vec<JoinFragment>, Vec<String>) {
        let condition = match direction {
            Direction::Left => format!(
                "{target_alias}.{fk} = {source_alias}.{tid}",
                fk = self.fk_column,
                tid = self.target_id_column,
            ),
            _ => format!(
                "{source_alias}.{fk} = {target_alias}.{tid}",
                fk = self.fk_column,
                tid = self.target_id_column,
            ),
        };
        (vec![], vec![condition])
    }

    fn type_filter(&self, _edge_alias: &str, _rel_types: &[String]) -> Vec<String> {
        vec![]
    }

    fn property_expr(&self, _alias: &str, _property: &str, _prop_index: usize) -> PropertyFragment {
        PropertyFragment {
            expr: "NULL".to_string(),
            joins: vec![],
            conditions: vec![],
        }
    }

    fn all_properties_expr(&self, _alias: &str) -> String {
        "json('{}')".to_string()
    }

    fn edge_json_object(&self, _alias: &str) -> String {
        "json_object('id', NULL, 'type', NULL, 'properties', json('{}'))".to_string()
    }

    fn nested_property_expr(&self, _alias: &str, _json_key: &str, _json_path: &str) -> String {
        "NULL".to_string()
    }

    fn create_sql(
        &self,
        source_id_expr: &str,
        target_id_expr: &str,
        _rel_type: &str,
    ) -> Vec<String> {
        vec![format!(
            "UPDATE {table} SET {fk} = {target_id_expr} WHERE {id} = {source_id_expr}",
            table = self.fk_table,
            fk = self.fk_column,
            id = self.target_id_column,
        )]
    }

    fn recursive_step(
        &self,
        cte_name: &str,
        direction: &Direction,
        _rel_types: &[String],
    ) -> RecursiveStep {
        match direction {
            Direction::Left => {
                // Backward: given node_id (a parent), find rows whose FK points to it
                RecursiveStep {
                    next_node_expr: format!("_fk.{}", self.target_id_column),
                    from_clause: format!(
                        "JOIN {} _fk ON _fk.{} = {}.node_id",
                        self.fk_table, self.fk_column, cte_name
                    ),
                    where_conditions: vec![],
                }
            }
            _ => {
                // Forward: given node_id (in fk_table), follow fk_column to target
                RecursiveStep {
                    next_node_expr: format!("_fk.{}", self.fk_column),
                    from_clause: format!(
                        "JOIN {} _fk ON _fk.{} = {}.node_id",
                        self.fk_table, self.target_id_column, cte_name
                    ),
                    where_conditions: vec![],
                }
            }
        }
    }
}

// ---- Join Table Edge Resolver ----

/// Resolves an edge to an explicit junction table.
///
/// **Limitation (Patch 3 follow-up):** the emitted JOIN ON / WHERE clauses
/// hardcode `.id` for both the source and target node's primary key column.
/// In other words, this resolver currently assumes both endpoint node
/// resolvers have `id_column() == "id"`. Holon's `block` table satisfies this.
/// If you register a `JoinTableEdgeResolver` whose endpoints use a different
/// id column name, the emitted SQL will be silently wrong (no rows). The
/// proper fix is to thread the source/target `NodeResolver::id_column()` into
/// `traverse_joins` / `recursive_step`; deferred behind tests until a real
/// consumer needs it.
pub struct JoinTableEdgeResolver {
    pub join_table: String,
    pub source_column: String,
    pub target_column: String,
}

impl EdgeResolver for JoinTableEdgeResolver {
    fn table(&self) -> &str {
        &self.join_table
    }

    fn traverse_joins(
        &self,
        source_alias: &str,
        target_alias: &str,
        edge_alias: &str,
        direction: &Direction,
        optional: bool,
    ) -> (Vec<JoinFragment>, Vec<String>) {
        let (src_col, tgt_col) = match direction {
            Direction::Left => (&self.target_column, &self.source_column),
            _ => (&self.source_column, &self.target_column),
        };

        let join_type = if optional {
            JoinType::Left
        } else {
            JoinType::Inner
        };

        let join = JoinFragment {
            join_type,
            table: self.join_table.clone(),
            alias: edge_alias.to_string(),
            on_condition: format!("{edge_alias}.{src_col} = {source_alias}.id"),
        };

        let condition = format!("{edge_alias}.{tgt_col} = {target_alias}.id");

        (vec![join], vec![condition])
    }

    fn type_filter(&self, _edge_alias: &str, _rel_types: &[String]) -> Vec<String> {
        vec![]
    }

    fn property_expr(&self, _alias: &str, _property: &str, _prop_index: usize) -> PropertyFragment {
        PropertyFragment {
            expr: "NULL".to_string(),
            joins: vec![],
            conditions: vec![],
        }
    }

    fn all_properties_expr(&self, _alias: &str) -> String {
        "json('{}')".to_string()
    }

    fn edge_json_object(&self, _alias: &str) -> String {
        "json_object('id', NULL, 'type', NULL, 'properties', json('{}'))".to_string()
    }

    fn nested_property_expr(&self, _alias: &str, _json_key: &str, _json_path: &str) -> String {
        "NULL".to_string()
    }

    fn create_sql(
        &self,
        source_id_expr: &str,
        target_id_expr: &str,
        _rel_type: &str,
    ) -> Vec<String> {
        vec![format!(
            "INSERT INTO {} ({}, {}) VALUES ({}, {})",
            self.join_table, self.source_column, self.target_column, source_id_expr, target_id_expr,
        )]
    }

    fn recursive_step(
        &self,
        cte_name: &str,
        direction: &Direction,
        _rel_types: &[String],
    ) -> RecursiveStep {
        let (src_col, tgt_col) = match direction {
            Direction::Left => (&self.target_column, &self.source_column),
            _ => (&self.source_column, &self.target_column),
        };
        RecursiveStep {
            next_node_expr: format!("_jt.{}", tgt_col),
            from_clause: format!(
                "JOIN {} _jt ON _jt.{} = {}.node_id",
                self.join_table, src_col, cte_name
            ),
            where_conditions: vec![],
        }
    }
}

// ---- Helper: convert Expr to SQL value for EAV inserts ----

pub fn eav_expr_to_sql_value(value: &Expr) -> Result<(&'static str, String), TransformError> {
    match value {
        Expr::Literal(Literal::Integer(n)) => Ok(("int", n.to_string())),
        Expr::Literal(Literal::Float(f)) => Ok(("real", format!("{f}"))),
        Expr::Literal(Literal::Boolean(b)) => Ok(("bool", if *b { "1" } else { "0" }.to_string())),
        Expr::Literal(Literal::String(s)) => Ok(("text", format!("'{}'", escape_sql_string(s)))),
        Expr::Literal(Literal::Null) => Ok(("null", String::new())),
        Expr::Map(pairs) => {
            let mut parts = Vec::new();
            for pair in pairs {
                let (_, v) = eav_expr_to_sql_value(&pair.value)?;
                parts.push(format!("'{}', {v}", escape_sql_string(&pair.key)));
            }
            Ok(("json", format!("json_object({})", parts.join(", "))))
        }
        Expr::List(items) => {
            let items_sql: Vec<String> = items
                .iter()
                .map(|e| eav_expr_to_sql_value(e).map(|(_, v)| v))
                .collect::<Result<_, _>>()?;
            Ok(("json", format!("json_array({})", items_sql.join(", "))))
        }
        Expr::Parameter(name) => Ok(("text", format!(":{name}"))),
        _ => Err(TransformError::UnsupportedExpr(
            "non-literal property value in CREATE".to_string(),
        )),
    }
}
