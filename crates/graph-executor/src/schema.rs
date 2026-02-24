use rusqlite::Connection;

pub fn initialize_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SCHEMA_DDL)?;
    conn.execute_batch(SCHEMA_INDEXES)?;
    Ok(())
}

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
  value TEXT NOT NULL CHECK (json_valid(value)),
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
  value TEXT NOT NULL CHECK (json_valid(value)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_schema_initialization() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        initialize_schema(&conn).unwrap();

        // Verify tables exist by querying sqlite_master
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"nodes".to_string()));
        assert!(tables.contains(&"edges".to_string()));
        assert!(tables.contains(&"property_keys".to_string()));
        assert!(tables.contains(&"node_labels".to_string()));
        assert!(tables.contains(&"node_props_int".to_string()));
        assert!(tables.contains(&"node_props_text".to_string()));
        assert!(tables.contains(&"node_props_real".to_string()));
        assert!(tables.contains(&"node_props_bool".to_string()));
        assert!(tables.contains(&"node_props_json".to_string()));
        assert!(tables.contains(&"edge_props_int".to_string()));
        assert!(tables.contains(&"edge_props_text".to_string()));
        assert!(tables.contains(&"edge_props_real".to_string()));
        assert!(tables.contains(&"edge_props_bool".to_string()));
        assert!(tables.contains(&"edge_props_json".to_string()));
    }

    #[test]
    fn test_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        initialize_schema(&conn).unwrap();
        initialize_schema(&conn).unwrap(); // should not fail
    }

    #[test]
    fn test_basic_node_insert() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        initialize_schema(&conn).unwrap();

        conn.execute("INSERT INTO nodes DEFAULT VALUES", [])
            .unwrap();
        let node_id: i64 = conn
            .query_row("SELECT last_insert_rowid()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(node_id, 1);

        conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?1, ?2)",
            rusqlite::params![node_id, "Person"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO property_keys (key) VALUES (?1)",
            rusqlite::params!["name"],
        )
        .unwrap();

        let key_id: i64 = conn
            .query_row(
                "SELECT id FROM property_keys WHERE key = 'name'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        conn.execute(
            "INSERT INTO node_props_text (node_id, key_id, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![node_id, key_id, "Alice"],
        )
        .unwrap();

        let name: String = conn
            .query_row(
                "SELECT npt.value FROM node_props_text npt \
                 JOIN property_keys pk ON npt.key_id = pk.id \
                 WHERE npt.node_id = ?1 AND pk.key = 'name'",
                rusqlite::params![node_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "Alice");
    }

    #[test]
    fn test_cascade_delete() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        initialize_schema(&conn).unwrap();

        conn.execute("INSERT INTO nodes DEFAULT VALUES", [])
            .unwrap();
        let node_id: i64 = conn
            .query_row("SELECT last_insert_rowid()", [], |row| row.get(0))
            .unwrap();

        conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?1, 'Test')",
            rusqlite::params![node_id],
        )
        .unwrap();

        conn.execute(
            "DELETE FROM nodes WHERE id = ?1",
            rusqlite::params![node_id],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM node_labels WHERE node_id = ?1",
                rusqlite::params![node_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
