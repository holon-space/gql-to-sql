use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;

use gql_parser::{parse, QueryOrUnion};
use gql_transform::resolver::{EdgeDef, GraphSchema, NodeResolver};
use gql_transform::transform;

use crate::schema::initialize_schema;

pub struct GqlExecutor {
    conn: Connection,
    schema: GraphSchema,
}

#[derive(Debug)]
pub enum GqlResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Modified {
        count: usize,
    },
    Explain(String),
    Empty,
}

#[derive(Debug)]
pub enum GqlError {
    Parse(String),
    Transform(String),
    Execute(String),
}

impl std::fmt::Display for GqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GqlError::Parse(s) => write!(f, "parse error: {s}"),
            GqlError::Transform(s) => write!(f, "transform error: {s}"),
            GqlError::Execute(s) => write!(f, "execution error: {s}"),
        }
    }
}

impl std::error::Error for GqlError {}

impl GqlExecutor {
    pub fn new_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        initialize_schema(&conn)?;
        register_udfs(&conn)?;
        Ok(Self {
            conn,
            schema: GraphSchema::default(),
        })
    }

    pub fn new_with_path(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        initialize_schema(&conn)?;
        register_udfs(&conn)?;
        Ok(Self {
            conn,
            schema: GraphSchema::default(),
        })
    }

    pub fn register_node(&mut self, label: &str, resolver: Box<dyn NodeResolver>) {
        self.schema.nodes.insert(label.to_string(), resolver);
    }

    pub fn register_edge(&mut self, rel_type: &str, def: EdgeDef) {
        self.schema.edges.insert(rel_type.to_string(), def);
    }

    pub fn execute(&self, cypher: &str) -> Result<GqlResult, GqlError> {
        let parsed = parse(cypher).map_err(|e| GqlError::Parse(e.to_string()))?;
        let query = match parsed {
            QueryOrUnion::Query(q) => q,
            QueryOrUnion::Union(_) => {
                return Err(GqlError::Transform(
                    "UNION queries not yet supported".to_string(),
                ));
            }
        };
        let sql =
            transform(&query, &self.schema).map_err(|e| GqlError::Transform(e.to_string()))?;

        if query.explain {
            return Ok(GqlResult::Explain(sql));
        }

        self.execute_sql(&sql)
    }

    pub fn execute_with_params(
        &self,
        cypher: &str,
        params_json: &str,
    ) -> Result<GqlResult, GqlError> {
        let parsed = parse(cypher).map_err(|e| GqlError::Parse(e.to_string()))?;
        let query = match parsed {
            QueryOrUnion::Query(q) => q,
            QueryOrUnion::Union(_) => {
                return Err(GqlError::Transform(
                    "UNION queries not yet supported".to_string(),
                ));
            }
        };
        let sql =
            transform(&query, &self.schema).map_err(|e| GqlError::Transform(e.to_string()))?;

        if query.explain {
            return Ok(GqlResult::Explain(sql));
        }

        let json_map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(params_json)
                .map_err(|e| GqlError::Execute(format!("invalid params JSON: {e}")))?;

        let params = json_map_to_named_params(&json_map);

        self.execute_sql_with_params(&sql, &params)
    }

    fn execute_sql_with_params(
        &self,
        sql: &str,
        params: &[(String, rusqlite::types::Value)],
    ) -> Result<GqlResult, GqlError> {
        let trimmed = sql.trim();
        if self.is_write_sql(trimmed) {
            return self.execute_write_with_params(trimmed, params);
        }
        self.execute_read_with_params(trimmed, params)
    }

    fn execute_sql(&self, sql: &str) -> Result<GqlResult, GqlError> {
        let trimmed = sql.trim();

        // Multiple statements (from CREATE / SET / DELETE transforms) are separated by "; "
        // If the SQL contains multiple statements, execute them as a batch.
        if self.is_write_sql(trimmed) {
            return self.execute_write(trimmed);
        }

        self.execute_read(trimmed)
    }

    fn is_write_sql(&self, sql: &str) -> bool {
        let upper = sql.trim_start().to_uppercase();
        upper.starts_with("INSERT") || upper.starts_with("UPDATE") || upper.starts_with("DELETE")
    }

    fn execute_write(&self, sql: &str) -> Result<GqlResult, GqlError> {
        // Split on "; " to handle multiple statements from WriteBuilder
        let statements: Vec<&str> = sql.split("; ").collect();
        let mut total_changes = 0usize;

        for stmt in &statements {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            self.conn
                .execute(stmt, [])
                .map_err(|e| GqlError::Execute(format!("{e} [SQL: {stmt}]")))?;
            total_changes += self.conn.changes() as usize;
        }

        Ok(GqlResult::Modified {
            count: total_changes,
        })
    }

    fn execute_read(&self, sql: &str) -> Result<GqlResult, GqlError> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| GqlError::Execute(format!("{e} [SQL: {sql}]")))?;

        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let rows: Vec<Vec<serde_json::Value>> = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value = row_value_to_json(row, i);
                    values.push(value);
                }
                Ok(values)
            })
            .map_err(|e| GqlError::Execute(format!("{e} [SQL: {sql}]")))?
            .filter_map(|r| r.ok())
            .collect();

        if rows.is_empty() && columns.is_empty() {
            Ok(GqlResult::Empty)
        } else {
            Ok(GqlResult::Rows { columns, rows })
        }
    }

    fn execute_write_with_params(
        &self,
        sql: &str,
        params: &[(String, rusqlite::types::Value)],
    ) -> Result<GqlResult, GqlError> {
        let statements: Vec<&str> = sql.split("; ").collect();
        let mut total_changes = 0usize;
        let param_refs: Vec<(&str, &dyn rusqlite::types::ToSql)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v as &dyn rusqlite::types::ToSql))
            .collect();

        for stmt_sql in &statements {
            let stmt_sql = stmt_sql.trim();
            if stmt_sql.is_empty() {
                continue;
            }
            let stmt_params: Vec<(&str, &dyn rusqlite::types::ToSql)> = param_refs
                .iter()
                .filter(|(name, _)| sql_contains_param(stmt_sql, name))
                .cloned()
                .collect();
            let mut stmt = self
                .conn
                .prepare(stmt_sql)
                .map_err(|e| GqlError::Execute(format!("{e} [SQL: {stmt_sql}]")))?;
            stmt.execute(stmt_params.as_slice())
                .map_err(|e| GqlError::Execute(format!("{e} [SQL: {stmt_sql}]")))?;
            total_changes += self.conn.changes() as usize;
        }

        Ok(GqlResult::Modified {
            count: total_changes,
        })
    }

    fn execute_read_with_params(
        &self,
        sql: &str,
        params: &[(String, rusqlite::types::Value)],
    ) -> Result<GqlResult, GqlError> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| GqlError::Execute(format!("{e} [SQL: {sql}]")))?;

        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let param_refs: Vec<(&str, &dyn rusqlite::types::ToSql)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v as &dyn rusqlite::types::ToSql))
            .filter(|(name, _)| sql_contains_param(sql, name))
            .collect();

        let rows: Vec<Vec<serde_json::Value>> = stmt
            .query_map(param_refs.as_slice(), |row| {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value = row_value_to_json(row, i);
                    values.push(value);
                }
                Ok(values)
            })
            .map_err(|e| GqlError::Execute(format!("{e} [SQL: {sql}]")))?
            .filter_map(|r| r.ok())
            .collect();

        if rows.is_empty() && columns.is_empty() {
            Ok(GqlResult::Empty)
        } else {
            Ok(GqlResult::Rows { columns, rows })
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

fn sql_contains_param(sql: &str, param_name: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = sql[start..].find(param_name) {
        let abs_pos = start + pos;
        let after = abs_pos + param_name.len();
        if after >= sql.len() {
            return true;
        }
        let next_char = sql.as_bytes()[after];
        if !next_char.is_ascii_alphanumeric() && next_char != b'_' {
            return true;
        }
        start = abs_pos + 1;
    }
    false
}

fn json_map_to_named_params(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(String, rusqlite::types::Value)> {
    map.iter()
        .map(|(key, val)| {
            let name = format!(":{key}");
            let sql_val = match val {
                serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        rusqlite::types::Value::Integer(i)
                    } else if let Some(f) = n.as_f64() {
                        rusqlite::types::Value::Real(f)
                    } else {
                        rusqlite::types::Value::Null
                    }
                }
                serde_json::Value::Bool(b) => {
                    rusqlite::types::Value::Integer(if *b { 1 } else { 0 })
                }
                serde_json::Value::Null => rusqlite::types::Value::Null,
                other => rusqlite::types::Value::Text(other.to_string()),
            };
            (name, sql_val)
        })
        .collect()
}

fn register_udfs(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function("REVERSE", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let s: String = ctx.get(0)?;
        Ok(s.chars().rev().collect::<String>())
    })?;

    conn.create_scalar_function("SQRT", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.sqrt())
    })?;

    conn.create_scalar_function("LOG", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.ln())
    })?;

    conn.create_scalar_function("LOG10", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.log10())
    })?;

    conn.create_scalar_function("E", 0, FunctionFlags::SQLITE_DETERMINISTIC, |_ctx| {
        Ok(std::f64::consts::E)
    })?;

    conn.create_scalar_function(
        "PAGERANK",
        -1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(0.0f64),
    )?;

    conn.create_scalar_function("GRAPH", 1, FunctionFlags::SQLITE_DETERMINISTIC, |_ctx| {
        Ok("default".to_string())
    })?;

    conn.create_scalar_function("JSON_GET", 2, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let json_val: String = ctx.get(0)?;
        let path: String = ctx.get(1)?;
        let json_path = if path.starts_with("$.") {
            path
        } else {
            format!("$.{path}")
        };
        let db = unsafe { ctx.get_connection()? };
        let result: rusqlite::Result<rusqlite::types::Value> = db.query_row(
            "SELECT json_extract(?, ?)",
            rusqlite::params![json_val, json_path],
            |row| row.get(0),
        );
        Ok(result.unwrap_or(rusqlite::types::Value::Null))
    })?;

    conn.create_scalar_function("JSON_KEYS", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let json_val: String = ctx.get(0)?;
        let db = unsafe { ctx.get_connection()? };
        let result: rusqlite::Result<String> = db.query_row(
            "SELECT json_group_array(key) FROM json_each(?)",
            rusqlite::params![json_val],
            |row| row.get(0),
        );
        Ok(result.unwrap_or_else(|_| "[]".to_string()))
    })?;

    conn.create_scalar_function("RANGE", -1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let argc = ctx.len();
        let start: i64 = ctx.get(0)?;
        let end: i64 = ctx.get(1)?;
        let step: i64 = if argc >= 3 { ctx.get(2)? } else { 1 };
        let mut arr = Vec::new();
        let mut i = start;
        while (step > 0 && i <= end) || (step < 0 && i >= end) {
            arr.push(i.to_string());
            i += step;
        }
        Ok(format!("[{}]", arr.join(",")))
    })?;

    conn.create_scalar_function("TOBOOLEAN", 1, FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let s: String = ctx.get(0)?;
        match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(1i64),
            _ => Ok(0i64),
        }
    })?;

    conn.create_scalar_function(
        "TOPPAGERANK",
        -1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok("[]".to_string()),
    )?;

    conn.create_scalar_function(
        "PERSONALIZEDPAGERANK",
        -1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok("[]".to_string()),
    )?;

    conn.create_scalar_function("LOCAL_GRAPH", 2, FunctionFlags::empty(), |ctx| {
        let node_id: i64 = ctx.get(0)?;
        let depth: i64 = ctx.get(1)?;
        let db = unsafe { ctx.get_connection()? };

        // Collect reachable node IDs within `depth` hops (bidirectional)
        let node_ids: Vec<i64> = if depth == 0 {
            vec![node_id]
        } else {
            let mut stmt = db.prepare(
                "WITH RECURSIVE reach(nid, d, visited) AS (\
                 SELECT ?1, 0, CAST(?1 AS TEXT) \
                 UNION ALL \
                 SELECT e.target_id, reach.d + 1, reach.visited || ',' || CAST(e.target_id AS TEXT) \
                 FROM reach JOIN edges e ON e.source_id = reach.nid \
                 WHERE reach.d < ?2 \
                 AND ',' || reach.visited || ',' NOT LIKE '%,' || CAST(e.target_id AS TEXT) || ',%' \
                 UNION ALL \
                 SELECT e.source_id, reach.d + 1, reach.visited || ',' || CAST(e.source_id AS TEXT) \
                 FROM reach JOIN edges e ON e.target_id = reach.nid \
                 WHERE reach.d < ?2 \
                 AND ',' || reach.visited || ',' NOT LIKE '%,' || CAST(e.source_id AS TEXT) || ',%') \
                 SELECT DISTINCT nid FROM reach",
            )?;
            let rows = stmt.query_map(rusqlite::params![node_id, depth], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };

        if node_ids.is_empty() {
            return Ok("{}".to_string());
        }

        // Build node JSON array
        let id_list = node_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let nodes_sql = format!(
            "SELECT json_group_array(json_object(\
             'id', n.id, \
             'labels', COALESCE((SELECT json_group_array(label) FROM node_labels WHERE node_id = n.id), json('[]')), \
             'properties', COALESCE((SELECT json_group_object(pk.key, COALESCE(\
             (SELECT npt.value FROM node_props_text npt WHERE npt.node_id = n.id AND npt.key_id = pk.id), \
             (SELECT npi.value FROM node_props_int npi WHERE npi.node_id = n.id AND npi.key_id = pk.id), \
             (SELECT npr.value FROM node_props_real npr WHERE npr.node_id = n.id AND npr.key_id = pk.id), \
             (SELECT npb.value FROM node_props_bool npb WHERE npb.node_id = n.id AND npb.key_id = pk.id), \
             (SELECT json(npj.value) FROM node_props_json npj WHERE npj.node_id = n.id AND npj.key_id = pk.id))) \
             FROM property_keys pk WHERE \
             EXISTS (SELECT 1 FROM node_props_text WHERE node_id = n.id AND key_id = pk.id) OR \
             EXISTS (SELECT 1 FROM node_props_int WHERE node_id = n.id AND key_id = pk.id) OR \
             EXISTS (SELECT 1 FROM node_props_real WHERE node_id = n.id AND key_id = pk.id) OR \
             EXISTS (SELECT 1 FROM node_props_bool WHERE node_id = n.id AND key_id = pk.id) OR \
             EXISTS (SELECT 1 FROM node_props_json WHERE node_id = n.id AND key_id = pk.id)), json('{{}}'))\
             )) FROM nodes n WHERE n.id IN ({id_list})"
        );
        let nodes_json: String = db.query_row(&nodes_sql, [], |row| row.get(0))?;

        // Build edge JSON array (edges between reachable nodes)
        let edges_sql = format!(
            "SELECT json_group_array(json_object(\
             'id', e.id, \
             'type', e.type, \
             'startNodeId', e.source_id, \
             'endNodeId', e.target_id, \
             'properties', json('{{}}') \
             )) FROM edges e \
             WHERE e.source_id IN ({id_list}) AND e.target_id IN ({id_list})"
        );
        let edges_json: String = db
            .query_row(&edges_sql, [], |row| row.get(0))
            .unwrap_or_else(|_| "[]".to_string());

        Ok(format!(
            "{{\"nodes\":{nodes_json},\"edges\":{edges_json}}}"
        ))
    })?;

    Ok(())
}

fn row_value_to_json(row: &rusqlite::Row, idx: usize) -> serde_json::Value {
    // Try integer first, then float, then string, then null
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(serde_json::Number::from(v));
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::Null;
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        // Try to parse as JSON object/array first
        if (v.starts_with('{') || v.starts_with('['))
            && serde_json::from_str::<serde_json::Value>(&v).is_ok()
        {
            return serde_json::from_str(&v).unwrap();
        }
        return serde_json::Value::String(v);
    }
    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let exec = GqlExecutor::new_in_memory().unwrap();
        // Verify schema exists
        let count: i64 = exec
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='nodes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_execute_write_sql() {
        let exec = GqlExecutor::new_in_memory().unwrap();
        let result = exec
            .execute_sql("INSERT INTO nodes DEFAULT VALUES")
            .unwrap();
        match result {
            GqlResult::Modified { count } => assert!(count > 0),
            _ => panic!("expected Modified result"),
        }
    }

    #[test]
    fn test_execute_read_sql() {
        let exec = GqlExecutor::new_in_memory().unwrap();
        exec.execute_sql("INSERT INTO nodes DEFAULT VALUES")
            .unwrap();
        let result = exec.execute_sql("SELECT id FROM nodes").unwrap();
        match result {
            GqlResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], serde_json::json!(1));
            }
            _ => panic!("expected Rows result"),
        }
    }

    #[test]
    fn test_execute_multi_statement_write() {
        let exec = GqlExecutor::new_in_memory().unwrap();
        let sql = "INSERT INTO nodes DEFAULT VALUES; INSERT INTO node_labels (node_id, label) VALUES (last_insert_rowid(), 'Person')";
        let result = exec.execute_sql(sql).unwrap();
        match result {
            GqlResult::Modified { count } => assert!(count >= 2),
            _ => panic!("expected Modified result"),
        }

        let label: String = exec
            .connection()
            .query_row(
                "SELECT label FROM node_labels WHERE node_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(label, "Person");
    }
}
