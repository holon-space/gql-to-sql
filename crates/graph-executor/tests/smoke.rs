use gql_transform::resolver::{
    ColumnMapping, EdgeDef, ForeignKeyEdgeResolver, JoinTableEdgeResolver, MappedNodeResolver,
    MultiValueBacking,
};

fn setup_block_hierarchy() -> graph_executor::GqlExecutor {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE blocks (id TEXT PRIMARY KEY, content TEXT, parent_id TEXT REFERENCES blocks(id));
             INSERT INTO blocks VALUES ('root', 'Root block', NULL);
             INSERT INTO blocks VALUES ('mid',  'Middle block', 'root');
             INSERT INTO blocks VALUES ('leaf', 'Leaf block', 'mid');",
        )
        .unwrap();

    exec.register_node(
        "Block",
        Box::new(MappedNodeResolver {
            table_name: "blocks".to_string(),
            id_col: "id".to_string(),
            label: "Block".to_string(),
            columns: vec![
                ColumnMapping {
                    property_name: "content".to_string(),
                    column_name: "content".to_string(),
                },
                ColumnMapping {
                    property_name: "parent_id".to_string(),
                    column_name: "parent_id".to_string(),
                },
            ],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );
    exec.register_edge(
        "CHILD_OF",
        EdgeDef {
            source_label: Some("Block".to_string()),
            target_label: Some("Block".to_string()),
            resolver: Box::new(ForeignKeyEdgeResolver {
                fk_table: "blocks".to_string(),
                fk_column: "parent_id".to_string(),
                target_table: "blocks".to_string(),
                target_id_column: "id".to_string(),
            }),
        },
    );

    exec
}
use graph_executor::{GqlExecutor, GqlResult};

#[test]
fn test_create_node() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    let result = exec.execute("CREATE (n:Person {name: 'Alice', age: 30})");
    assert!(result.is_ok(), "CREATE should succeed: {result:?}");
}

#[test]
fn test_create_and_match() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();

    let result = exec.execute("MATCH (n:Person) RETURN n.name").unwrap();
    match result {
        GqlResult::Rows { columns, rows } => {
            assert!(!rows.is_empty(), "Should return at least one row");
            // The column should be named "n.name"
            assert!(
                columns.iter().any(|c| c.contains("name")),
                "Column should contain 'name': {columns:?}"
            );
            // At least one row should contain "Alice"
            let has_alice = rows.iter().any(|row| {
                row.iter()
                    .any(|v| v == &serde_json::Value::String("Alice".to_string()))
            });
            assert!(has_alice, "Should find Alice in results: {rows:?}");
        }
        other => panic!("Expected Rows result, got: {other:?}"),
    }
}

#[test]
fn test_create_multiple_nodes() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    exec.execute("CREATE (b:Person {name: 'Bob'})").unwrap();

    let result = exec.execute("MATCH (n:Person) RETURN n.name").unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2, "Should return two rows: {rows:?}");
        }
        other => panic!("Expected Rows result, got: {other:?}"),
    }
}

#[test]
fn test_create_relationship() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    exec.execute("CREATE (b:Person {name: 'Bob'})").unwrap();

    let result = exec.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    assert!(
        result.is_ok(),
        "Creating relationship should succeed: {result:?}"
    );
}

#[test]
fn test_match_with_where() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    exec.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = exec
        .execute("MATCH (n:Person) WHERE n.age > 28 RETURN n.name")
        .unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(
                rows.len(),
                1,
                "Should return one row for age > 28: {rows:?}"
            );
        }
        other => panic!("Expected Rows result, got: {other:?}"),
    }
}

#[test]
fn test_match_return_node() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let result = exec.execute("MATCH (n:Person) RETURN n").unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "Should return one row");
            // The result should be a JSON object with id, labels, properties
            let node = &rows[0][0];
            assert!(node.is_object(), "Node should be a JSON object: {node:?}");
            assert!(node.get("id").is_some(), "Node should have id");
            assert!(node.get("labels").is_some(), "Node should have labels");
        }
        other => panic!("Expected Rows result, got: {other:?}"),
    }
}

#[test]
fn test_create_node_with_label_only() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    let result = exec.execute("CREATE (:Person)");
    assert!(
        result.is_ok(),
        "CREATE with label only should succeed: {result:?}"
    );
}

#[test]
fn test_explain() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    let result = exec.execute("EXPLAIN MATCH (n:Person) RETURN n.name");
    match result {
        Ok(GqlResult::Explain(sql)) => {
            assert!(
                sql.contains("nodes"),
                "EXPLAIN should show generated SQL containing 'nodes': {sql}"
            );
        }
        other => panic!("Expected Explain result, got: {other:?}"),
    }
}

#[test]
fn test_create_with_boolean_property() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    let result = exec.execute("CREATE (n:Setting {name: 'debug', enabled: true})");
    assert!(
        result.is_ok(),
        "CREATE with boolean property should succeed: {result:?}"
    );
}

#[test]
fn test_create_with_float_property() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    let result = exec.execute("CREATE (n:Measurement {name: 'temp', value: 98.6})");
    assert!(
        result.is_ok(),
        "CREATE with float property should succeed: {result:?}"
    );
}

#[test]
fn test_match_with_limit() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    exec.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    exec.execute("CREATE (c:Person {name: 'Charlie'})").unwrap();

    let result = exec
        .execute("MATCH (n:Person) RETURN n.name LIMIT 2")
        .unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2, "Should return exactly 2 rows with LIMIT 2");
        }
        other => panic!("Expected Rows result, got: {other:?}"),
    }
}

// ===== MappedNodeResolver integration tests =====

#[test]
fn test_mapped_node_match() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, content TEXT, priority INTEGER);
             INSERT INTO tasks VALUES (1, 'Buy milk', 1);
             INSERT INTO tasks VALUES (2, 'Write code', 3);
             INSERT INTO tasks VALUES (3, 'Clean house', 2);",
        )
        .unwrap();

    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![
                ColumnMapping {
                    property_name: "content".to_string(),
                    column_name: "content".to_string(),
                },
                ColumnMapping {
                    property_name: "priority".to_string(),
                    column_name: "priority".to_string(),
                },
            ],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );

    let result = exec.execute("MATCH (t:Task) RETURN t.content").unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3, "Should return 3 tasks: {rows:?}");
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_mapped_node_where_filter() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, content TEXT, priority INTEGER);
             INSERT INTO tasks VALUES (1, 'Buy milk', 1);
             INSERT INTO tasks VALUES (2, 'Write code', 3);",
        )
        .unwrap();

    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![
                ColumnMapping {
                    property_name: "content".to_string(),
                    column_name: "content".to_string(),
                },
                ColumnMapping {
                    property_name: "priority".to_string(),
                    column_name: "priority".to_string(),
                },
            ],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );

    let result = exec
        .execute("MATCH (t:Task) WHERE t.priority > 2 RETURN t.content")
        .unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0],
                serde_json::Value::String("Write code".to_string())
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_mapped_node_inline_props() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, content TEXT, priority INTEGER);
             INSERT INTO tasks VALUES (1, 'Buy milk', 1);
             INSERT INTO tasks VALUES (2, 'Write code', 3);",
        )
        .unwrap();

    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![
                ColumnMapping {
                    property_name: "content".to_string(),
                    column_name: "content".to_string(),
                },
                ColumnMapping {
                    property_name: "priority".to_string(),
                    column_name: "priority".to_string(),
                },
            ],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );

    let result = exec
        .execute("MATCH (t:Task {content: 'Buy milk'}) RETURN t.priority")
        .unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!(1));
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_mapped_node_return_full() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, content TEXT);
             INSERT INTO tasks VALUES (1, 'Buy milk');",
        )
        .unwrap();

    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![ColumnMapping {
                property_name: "content".to_string(),
                column_name: "content".to_string(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );

    let result = exec.execute("MATCH (t:Task) RETURN t").unwrap();
    match result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            let node = &rows[0][0];
            assert!(node.is_object(), "Should be JSON object: {node:?}");
            assert_eq!(node["id"], serde_json::json!(1));
            assert_eq!(node["labels"], serde_json::json!(["Task"]));
            assert_eq!(node["properties"]["content"], serde_json::json!("Buy milk"));
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_mixed_eav_and_mapped() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, content TEXT);
             INSERT INTO tasks VALUES (1, 'Buy milk');",
        )
        .unwrap();

    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![ColumnMapping {
                property_name: "content".to_string(),
                column_name: "content".to_string(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );

    // Create an EAV node (Person)
    exec.execute("CREATE (p:Person {name: 'Alice'})").unwrap();

    // Query EAV node
    let result = exec.execute("MATCH (p:Person) RETURN p.name").unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::Value::String("Alice".to_string()));
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }

    // Query mapped node
    let result = exec.execute("MATCH (t:Task) RETURN t.content").unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0],
                serde_json::Value::String("Buy milk".to_string())
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

// ===== Cross-structure traversal tests =====

#[test]
fn test_cross_structure_fk_traversal() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO people VALUES (1, 'Alice');
             INSERT INTO people VALUES (2, 'Bob');
             CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, assignee_id INTEGER);
             INSERT INTO tasks VALUES (1, 'Fix bug', 1);
             INSERT INTO tasks VALUES (2, 'Write docs', 2);
             INSERT INTO tasks VALUES (3, 'Deploy', 1);",
        )
        .unwrap();

    exec.register_node(
        "Person",
        Box::new(MappedNodeResolver {
            table_name: "people".to_string(),
            id_col: "id".to_string(),
            label: "Person".to_string(),
            columns: vec![ColumnMapping {
                property_name: "name".to_string(),
                column_name: "name".to_string(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );
    exec.register_node(
        "Task",
        Box::new(MappedNodeResolver {
            table_name: "tasks".to_string(),
            id_col: "id".to_string(),
            label: "Task".to_string(),
            columns: vec![ColumnMapping {
                property_name: "title".to_string(),
                column_name: "title".to_string(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );
    exec.register_edge(
        "ASSIGNED_TO",
        EdgeDef {
            source_label: Some("Task".to_string()),
            target_label: Some("Person".to_string()),
            resolver: Box::new(ForeignKeyEdgeResolver {
                fk_table: "tasks".to_string(),
                fk_column: "assignee_id".to_string(),
                target_table: "people".to_string(),
                target_id_column: "id".to_string(),
            }),
        },
    );

    // Basic FK edge traversal
    let result = exec
        .execute("MATCH (t:Task)-[:ASSIGNED_TO]->(p:Person) RETURN t.title, p.name")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3, "Should have 3 task-person pairs");
            let mut pairs: Vec<(String, String)> = rows
                .iter()
                .map(|r| {
                    (
                        r[0].as_str().unwrap().to_string(),
                        r[1].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            pairs.sort();
            assert_eq!(
                pairs,
                vec![
                    ("Deploy".to_string(), "Alice".to_string()),
                    ("Fix bug".to_string(), "Alice".to_string()),
                    ("Write docs".to_string(), "Bob".to_string()),
                ]
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }

    // Filtered traversal
    let result = exec
        .execute("MATCH (t:Task)-[:ASSIGNED_TO]->(p:Person) WHERE p.name = 'Alice' RETURN t.title")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut titles: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            titles.sort();
            assert_eq!(titles, vec!["Deploy", "Fix bug"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }

    // Aggregation across structures
    let result = exec
        .execute("MATCH (t:Task)-[:ASSIGNED_TO]->(p:Person) RETURN p.name, count(t) AS task_count")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut pairs: Vec<(String, i64)> = rows
                .iter()
                .map(|r| (r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap()))
                .collect();
            pairs.sort();
            assert_eq!(
                pairs,
                vec![("Alice".to_string(), 2), ("Bob".to_string(), 1)]
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_cross_structure_join_table_traversal() {
    let mut exec = GqlExecutor::new_in_memory().unwrap();

    exec.connection()
        .execute_batch(
            "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO people VALUES (1, 'Alice');
             INSERT INTO people VALUES (2, 'Bob');
             INSERT INTO people VALUES (3, 'Charlie');
             CREATE TABLE friendships (person_a_id INTEGER, person_b_id INTEGER);
             INSERT INTO friendships VALUES (1, 2);
             INSERT INTO friendships VALUES (1, 3);
             INSERT INTO friendships VALUES (2, 3);",
        )
        .unwrap();

    exec.register_node(
        "Person",
        Box::new(MappedNodeResolver {
            table_name: "people".to_string(),
            id_col: "id".to_string(),
            label: "Person".to_string(),
            columns: vec![ColumnMapping {
                property_name: "name".to_string(),
                column_name: "name".to_string(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );
    exec.register_edge(
        "FRIENDS_WITH",
        EdgeDef {
            source_label: Some("Person".to_string()),
            target_label: Some("Person".to_string()),
            resolver: Box::new(JoinTableEdgeResolver {
                join_table: "friendships".to_string(),
                source_column: "person_a_id".to_string(),
                target_column: "person_b_id".to_string(),
            }),
        },
    );

    // Join table traversal
    let result = exec
        .execute("MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) RETURN a.name, b.name")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut pairs: Vec<(String, String)> = rows
                .iter()
                .map(|r| {
                    (
                        r[0].as_str().unwrap().to_string(),
                        r[1].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            pairs.sort();
            assert_eq!(
                pairs,
                vec![
                    ("Alice".to_string(), "Bob".to_string()),
                    ("Alice".to_string(), "Charlie".to_string()),
                    ("Bob".to_string(), "Charlie".to_string()),
                ]
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }

    // Filtered: Alice's friends
    let result = exec
        .execute(
            "MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
        )
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut names: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            names.sort();
            assert_eq!(names, vec!["Bob", "Charlie"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }

    // Aggregation: friend count
    let result = exec
        .execute(
            "MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) RETURN a.name, count(b) AS friend_count",
        )
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut pairs: Vec<(String, i64)> = rows
                .iter()
                .map(|r| (r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap()))
                .collect();
            pairs.sort();
            assert_eq!(
                pairs,
                vec![("Alice".to_string(), 2), ("Bob".to_string(), 1)]
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

// ===== FK variable-length path tests =====

#[test]
fn test_fk_varlen_forward_walk_up_parents() {
    let exec = setup_block_hierarchy();

    // leaf -[:CHILD_OF*1..5]-> should walk up: leaf→mid (parent_id), mid→root (parent_id)
    let result = exec
        .execute("MATCH (a:Block {id: 'leaf'})-[:CHILD_OF*1..5]->(b:Block) RETURN b.id")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut ids: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["mid", "root"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_fk_varlen_backward_walk_down_descendants() {
    let exec = setup_block_hierarchy();

    // root <-[:CHILD_OF*1..10]- should find children/descendants: mid, leaf
    let result = exec
        .execute("MATCH (a:Block {id: 'root'})<-[:CHILD_OF*1..10]-(b:Block) RETURN b.id")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut ids: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["leaf", "mid"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_fk_varlen_cycle_detection() {
    let exec = setup_block_hierarchy();

    // Create a cycle: root -> mid -> leaf -> root
    exec.connection()
        .execute("UPDATE blocks SET parent_id = 'leaf' WHERE id = 'root'", [])
        .unwrap();

    // Should terminate and not loop forever
    let result = exec
        .execute("MATCH (a:Block {id: 'leaf'})-[:CHILD_OF*1..10]->(b:Block) RETURN b.id")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut ids: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            assert_eq!(ids, vec!["mid", "root"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_eav_varlen_still_works() {
    let exec = GqlExecutor::new_in_memory().unwrap();
    exec.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    exec.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    exec.execute("CREATE (c:Person {name: 'Charlie'})").unwrap();
    exec.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    exec.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .unwrap();

    let result = exec
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person) RETURN b.name")
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut names: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            names.sort();
            assert_eq!(names, vec!["Bob", "Charlie"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

// ===== Patch 3: JoinTableEdgeResolver gap-fill (reverse direction + varlen) =====

fn setup_friendships_join_table() -> GqlExecutor {
    let mut exec = GqlExecutor::new_in_memory().unwrap();
    exec.connection()
        .execute_batch(
            "CREATE TABLE people (id TEXT PRIMARY KEY, name TEXT);
             INSERT INTO people VALUES ('alice',   'Alice');
             INSERT INTO people VALUES ('bob',     'Bob');
             INSERT INTO people VALUES ('charlie', 'Charlie');
             INSERT INTO people VALUES ('dave',    'Dave');
             CREATE TABLE friendships (a_id TEXT, b_id TEXT);
             INSERT INTO friendships VALUES ('alice',   'bob');
             INSERT INTO friendships VALUES ('bob',     'charlie');
             INSERT INTO friendships VALUES ('charlie', 'dave');",
        )
        .unwrap();
    exec.register_node(
        "Person",
        Box::new(MappedNodeResolver {
            table_name: "people".into(),
            id_col: "id".into(),
            label: "Person".into(),
            columns: vec![ColumnMapping {
                property_name: "name".into(),
                column_name: "name".into(),
            }],
            extension_column: None,
            multi_value_properties: std::collections::HashMap::new(),
        }),
    );
    exec.register_edge(
        "FRIENDS_WITH",
        EdgeDef {
            source_label: Some("Person".into()),
            target_label: Some("Person".into()),
            resolver: Box::new(JoinTableEdgeResolver {
                join_table: "friendships".into(),
                source_column: "a_id".into(),
                target_column: "b_id".into(),
            }),
        },
    );
    exec
}

#[test]
fn test_join_table_reverse_direction() {
    let exec = setup_friendships_join_table();
    // bob's "back-friends": who has bob as their forward friend? → alice.
    let result = exec
        .execute(
            "MATCH (b:Person)<-[:FRIENDS_WITH]-(a:Person) WHERE b.name = 'Bob' RETURN a.name",
        )
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let names: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            assert_eq!(names, vec!["Alice"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

#[test]
fn test_join_table_variable_length() {
    let exec = setup_friendships_join_table();
    // alice -[*1..3]-> {bob, charlie, dave} via the friendships chain.
    let result = exec
        .execute(
            "MATCH (a:Person)-[:FRIENDS_WITH*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
        )
        .unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let mut names: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            names.sort();
            assert_eq!(names, vec!["Bob", "Charlie", "Dave"]);
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}

// ===== End-to-end: literal Now.org canonical query against in-memory data =====

#[test]
fn test_canonical_now_query_end_to_end() {
    // Exercises all four patches against a real (in-memory) SQLite database:
    //   P1: extension column (priority/effort/task_state/gate live in
    //       block.properties JSON, not as columns)
    //   P2: NOT EXISTS { ... } over the blocker relationship
    //   P3: JoinTableEdgeResolver dispatch for :blocked_by → task_blockers
    //   P4: 'agent' IN b.tags / NOT 'human-only' IN b.tags via inverted index
    let mut exec = GqlExecutor::new_in_memory().unwrap();
    exec.connection()
        .execute_batch(
            "CREATE TABLE block (
                 id TEXT PRIMARY KEY,
                 content_type TEXT,
                 properties TEXT
             );
             CREATE TABLE task_blockers (
                 blocked_id TEXT NOT NULL,
                 blocker_id TEXT NOT NULL,
                 PRIMARY KEY (blocked_id, blocker_id)
             );
             CREATE TABLE block_tags (
                 block_id TEXT NOT NULL,
                 tag      TEXT NOT NULL,
                 PRIMARY KEY (block_id, tag)
             );
             CREATE INDEX idx_block_tags_tag ON block_tags(tag, block_id);

             -- Candidates (TODO + G1, no active blockers, agent-or-not-human-only):
             INSERT INTO block VALUES ('a', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 2, 'effort', 3));
             INSERT INTO block VALUES ('b', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 1, 'effort', 5));
             INSERT INTO block VALUES ('d', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 1, 'effort', 2));
             INSERT INTO block VALUES ('f', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 3, 'effort', 1));
             -- Filtered out:
             INSERT INTO block VALUES ('c', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 1, 'effort', 1));   -- only human-only tag
             INSERT INTO block VALUES ('e', 'text', json_object('task_state', 'TODO', 'gate', 'G1', 'priority', 1, 'effort', 1));   -- has TODO blocker
             INSERT INTO block VALUES ('g', 'text', json_object('task_state', 'TODO', 'gate', 'G2', 'priority', 1, 'effort', 1));   -- wrong gate
             INSERT INTO block VALUES ('h', 'text', json_object('task_state', 'DONE', 'gate', 'G1', 'priority', 1, 'effort', 1));   -- already done
             -- Blockers (referenced from e):
             INSERT INTO block VALUES ('blocker_active', 'text', json_object('task_state', 'TODO'));
             INSERT INTO block VALUES ('blocker_done',   'text', json_object('task_state', 'DONE'));

             -- Tag rows:
             INSERT INTO block_tags VALUES ('b', 'agent');
             INSERT INTO block_tags VALUES ('c', 'human-only');
             INSERT INTO block_tags VALUES ('d', 'agent');
             INSERT INTO block_tags VALUES ('d', 'human-only');
             -- a, e, f, g, h have no tags → 'NOT human-only IN tags' is true

             -- Blocker relationships:
             INSERT INTO task_blockers VALUES ('e', 'blocker_active');  -- e is actively blocked
             INSERT INTO task_blockers VALUES ('f', 'blocker_done');    -- f's blocker is DONE
             ",
        )
        .unwrap();

    let mut multi: std::collections::HashMap<String, MultiValueBacking> =
        std::collections::HashMap::new();
    multi.insert(
        "tags".into(),
        MultiValueBacking {
            table: "block_tags".into(),
            source_column: "block_id".into(),
            value_column: "tag".into(),
        },
    );
    exec.register_node(
        "block",
        Box::new(MappedNodeResolver {
            table_name: "block".into(),
            id_col: "id".into(),
            label: "block".into(),
            columns: vec![
                ColumnMapping {
                    property_name: "id".into(),
                    column_name: "id".into(),
                },
                ColumnMapping {
                    property_name: "content_type".into(),
                    column_name: "content_type".into(),
                },
            ],
            extension_column: Some("properties".into()),
            multi_value_properties: multi,
        }),
    );
    exec.register_edge(
        "blocked_by",
        EdgeDef {
            source_label: Some("block".into()),
            target_label: Some("block".into()),
            resolver: Box::new(JoinTableEdgeResolver {
                join_table: "task_blockers".into(),
                source_column: "blocked_id".into(),
                target_column: "blocker_id".into(),
            }),
        },
    );

    // Literal Now.org canonical query — no source rewrites.
    let gql = "MATCH (b:block) \
WHERE b.task_state = 'TODO' \
  AND b.gate = 'G1' \
  AND NOT EXISTS { (b)-[:blocked_by]->(blocker:block) WHERE blocker.task_state <> 'DONE' } \
  AND ('agent' IN b.tags OR NOT ('human-only' IN b.tags)) \
RETURN b.id \
ORDER BY b.priority, b.effort, b.id \
LIMIT 10";

    let result = exec.execute(gql).unwrap();
    match &result {
        GqlResult::Rows { rows, .. } => {
            let ids: Vec<String> = rows
                .iter()
                .map(|r| r[0].as_str().unwrap().to_string())
                .collect();
            // Expected ordering: ASC by priority, then effort, then id.
            //   d: (1, 2) → first
            //   b: (1, 5)
            //   a: (2, 3)
            //   f: (3, 1) → last
            // Filtered out: c (only human-only), e (active blocker), g (wrong gate), h (DONE).
            assert_eq!(
                ids,
                vec!["d", "b", "a", "f"],
                "canonical query must return four candidates in priority/effort/id order"
            );
        }
        other => panic!("Expected Rows, got: {other:?}"),
    }
}
