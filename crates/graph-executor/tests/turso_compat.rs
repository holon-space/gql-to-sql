use gql_parser::{parse, QueryOrUnion};
use gql_transform::transform_default;
use turso::{Builder, Connection, Value};

const SCHEMA_DDL: &str = "\
CREATE TABLE IF NOT EXISTS nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS edges (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  target_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS property_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS node_labels (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  label TEXT NOT NULL,
  PRIMARY KEY (node_id, label)
);

CREATE TABLE IF NOT EXISTS node_props_int (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value INTEGER NOT NULL,
  PRIMARY KEY (node_id, key_id)
);

CREATE TABLE IF NOT EXISTS node_props_text (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value TEXT NOT NULL,
  PRIMARY KEY (node_id, key_id)
);

CREATE TABLE IF NOT EXISTS node_props_real (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value REAL NOT NULL,
  PRIMARY KEY (node_id, key_id)
);

CREATE TABLE IF NOT EXISTS node_props_bool (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value INTEGER NOT NULL CHECK (value IN (0, 1)),
  PRIMARY KEY (node_id, key_id)
);

CREATE TABLE IF NOT EXISTS node_props_json (
  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value TEXT NOT NULL,
  PRIMARY KEY (node_id, key_id)
);

CREATE TABLE IF NOT EXISTS edge_props_int (
  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value INTEGER NOT NULL,
  PRIMARY KEY (edge_id, key_id)
);

CREATE TABLE IF NOT EXISTS edge_props_text (
  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value TEXT NOT NULL,
  PRIMARY KEY (edge_id, key_id)
);

CREATE TABLE IF NOT EXISTS edge_props_real (
  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value REAL NOT NULL,
  PRIMARY KEY (edge_id, key_id)
);

CREATE TABLE IF NOT EXISTS edge_props_bool (
  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value INTEGER NOT NULL CHECK (value IN (0, 1)),
  PRIMARY KEY (edge_id, key_id)
);

CREATE TABLE IF NOT EXISTS edge_props_json (
  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,
  key_id INTEGER NOT NULL REFERENCES property_keys(id),
  value TEXT NOT NULL,
  PRIMARY KEY (edge_id, key_id)
);
";

const SCHEMA_INDEXES: &str = "\
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id, type);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id, type);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(type);
CREATE INDEX IF NOT EXISTS idx_node_labels_label ON node_labels(label, node_id);
CREATE INDEX IF NOT EXISTS idx_property_keys_key ON property_keys(key);
CREATE INDEX IF NOT EXISTS idx_node_props_int_key_value ON node_props_int(key_id, value, node_id);
CREATE INDEX IF NOT EXISTS idx_node_props_text_key_value ON node_props_text(key_id, value, node_id);
CREATE INDEX IF NOT EXISTS idx_node_props_real_key_value ON node_props_real(key_id, value, node_id);
CREATE INDEX IF NOT EXISTS idx_node_props_bool_key_value ON node_props_bool(key_id, value, node_id);
CREATE INDEX IF NOT EXISTS idx_node_props_json_key_value ON node_props_json(key_id, node_id);
CREATE INDEX IF NOT EXISTS idx_edge_props_int_key_value ON edge_props_int(key_id, value, edge_id);
CREATE INDEX IF NOT EXISTS idx_edge_props_text_key_value ON edge_props_text(key_id, value, edge_id);
CREATE INDEX IF NOT EXISTS idx_edge_props_real_key_value ON edge_props_real(key_id, value, edge_id);
CREATE INDEX IF NOT EXISTS idx_edge_props_bool_key_value ON edge_props_bool(key_id, value, edge_id);
CREATE INDEX IF NOT EXISTS idx_edge_props_json_key_value ON edge_props_json(key_id, edge_id);
";

async fn setup_turso() -> Connection {
    let db = Builder::new_local(":memory:")
        .experimental_materialized_views(true)
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(SCHEMA_DDL).await.unwrap();
    conn.execute_batch(SCHEMA_INDEXES).await.unwrap();
    conn
}

fn gql_to_sql(gql: &str) -> String {
    let parsed = parse(gql).unwrap();
    let query = match parsed {
        QueryOrUnion::Query(q) => q,
        QueryOrUnion::Union(_) => panic!("UNION not supported in these tests"),
    };
    transform_default(&query).unwrap()
}

async fn exec_write(conn: &Connection, sql: &str) {
    for stmt in sql.split("; ") {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        conn.execute(stmt, ()).await.unwrap();
    }
}

async fn query_rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let mut result = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let mut vals = Vec::new();
        for i in 0..row.column_count() {
            vals.push(row.get_value(i).unwrap());
        }
        result.push(vals);
    }
    result
}

// ===== Basic SQL compatibility tests =====

#[tokio::test]
async fn test_create_node() {
    let conn = setup_turso().await;
    let sql = gql_to_sql("CREATE (n:Person {name: 'Alice', age: 30})");
    exec_write(&conn, &sql).await;
}

#[tokio::test]
async fn test_create_and_match() {
    let conn = setup_turso().await;

    let create_sql = gql_to_sql("CREATE (n:Person {name: 'Alice', age: 30})");
    exec_write(&conn, &create_sql).await;

    let read_sql = gql_to_sql("MATCH (n:Person) RETURN n.name");
    let rows = query_rows(&conn, &read_sql).await;

    assert!(!rows.is_empty(), "Should return at least one row");
    let has_alice = rows
        .iter()
        .any(|row| row.iter().any(|v| v == &Value::Text("Alice".to_string())));
    assert!(has_alice, "Should find Alice in results: {rows:?}");
}

#[tokio::test]
async fn test_create_multiple_nodes() {
    let conn = setup_turso().await;

    exec_write(&conn, &gql_to_sql("CREATE (a:Person {name: 'Alice'})")).await;
    exec_write(&conn, &gql_to_sql("CREATE (b:Person {name: 'Bob'})")).await;

    let read_sql = gql_to_sql("MATCH (n:Person) RETURN n.name");
    let rows = query_rows(&conn, &read_sql).await;
    assert_eq!(rows.len(), 2, "Should return two rows: {rows:?}");
}

#[tokio::test]
async fn test_match_with_where() {
    let conn = setup_turso().await;

    exec_write(
        &conn,
        &gql_to_sql("CREATE (a:Person {name: 'Alice', age: 30})"),
    )
    .await;
    exec_write(
        &conn,
        &gql_to_sql("CREATE (b:Person {name: 'Bob', age: 25})"),
    )
    .await;

    let read_sql = gql_to_sql("MATCH (n:Person) WHERE n.age > 28 RETURN n.name");
    let rows = query_rows(&conn, &read_sql).await;
    assert_eq!(
        rows.len(),
        1,
        "Should return one row for age > 28: {rows:?}"
    );
}

#[tokio::test]
async fn test_match_with_limit() {
    let conn = setup_turso().await;

    exec_write(&conn, &gql_to_sql("CREATE (a:Person {name: 'Alice'})")).await;
    exec_write(&conn, &gql_to_sql("CREATE (b:Person {name: 'Bob'})")).await;
    exec_write(&conn, &gql_to_sql("CREATE (c:Person {name: 'Charlie'})")).await;

    let read_sql = gql_to_sql("MATCH (n:Person) RETURN n.name LIMIT 2");
    let rows = query_rows(&conn, &read_sql).await;
    assert_eq!(rows.len(), 2, "Should return exactly 2 rows with LIMIT 2");
}

#[tokio::test]
async fn test_create_node_with_label_only() {
    let conn = setup_turso().await;
    let sql = gql_to_sql("CREATE (:Person)");
    exec_write(&conn, &sql).await;
}

#[tokio::test]
async fn test_create_with_boolean_property() {
    let conn = setup_turso().await;
    let sql = gql_to_sql("CREATE (n:Setting {name: 'debug', enabled: true})");
    exec_write(&conn, &sql).await;
}

#[tokio::test]
async fn test_create_with_float_property() {
    let conn = setup_turso().await;
    let sql = gql_to_sql("CREATE (n:Measurement {name: 'temp', value: 98.6})");
    exec_write(&conn, &sql).await;
}

// ===== Materialized view compatibility tests =====

async fn setup_matview_data(conn: &Connection) {
    exec_write(
        conn,
        &gql_to_sql("CREATE (a:Person {name: 'Alice', age: 30})"),
    )
    .await;
    exec_write(
        conn,
        &gql_to_sql("CREATE (b:Person {name: 'Bob', age: 25})"),
    )
    .await;
    exec_write(
        conn,
        &gql_to_sql("CREATE (c:Person {name: 'Charlie', age: 35})"),
    )
    .await;
}

#[tokio::test]
async fn test_matview_match_all() {
    let conn = setup_turso().await;
    setup_matview_data(&conn).await;

    let select_sql = gql_to_sql("MATCH (n:Person) RETURN n.name");
    let create_view = format!("CREATE MATERIALIZED VIEW test_view AS {select_sql}");
    conn.execute(&create_view, ()).await.unwrap();

    let rows = query_rows(&conn, "SELECT * FROM test_view").await;
    assert_eq!(rows.len(), 3, "Matview should have 3 rows: {rows:?}");

    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_text().unwrap().clone())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[tokio::test]
async fn test_matview_with_where() {
    let conn = setup_turso().await;
    setup_matview_data(&conn).await;

    let select_sql = gql_to_sql("MATCH (n:Person) WHERE n.age > 28 RETURN n.name");
    eprintln!("SELECT SQL: {select_sql}");
    let create_view = format!("CREATE MATERIALIZED VIEW test_view AS {select_sql}");
    conn.execute(&create_view, ()).await.unwrap();

    let direct_rows = query_rows(&conn, &select_sql).await;
    eprintln!("DIRECT: {direct_rows:?}");

    let rows = query_rows(&conn, "SELECT * FROM test_view").await;
    eprintln!("MATVIEW: {rows:?}");
    assert_eq!(
        rows.len(),
        2,
        "Matview should have 2 rows (age > 28): {rows:?}"
    );

    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_text().unwrap().clone())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_matview_with_aggregation() {
    let conn = setup_turso().await;

    exec_write(
        &conn,
        &gql_to_sql("CREATE (a:Person {name: 'Alice', age: 30})"),
    )
    .await;
    exec_write(
        &conn,
        &gql_to_sql("CREATE (b:Animal {name: 'Fido', age: 5})"),
    )
    .await;
    exec_write(
        &conn,
        &gql_to_sql("CREATE (c:Person {name: 'Bob', age: 25})"),
    )
    .await;

    let select_sql = gql_to_sql("MATCH (n:Person) RETURN count(n) AS person_count");
    let create_view = format!("CREATE MATERIALIZED VIEW test_view AS {select_sql}");
    conn.execute(&create_view, ()).await.unwrap();

    let rows = query_rows(&conn, "SELECT * FROM test_view").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Value::Integer(2),
        "Should count 2 persons: {rows:?}"
    );
}
