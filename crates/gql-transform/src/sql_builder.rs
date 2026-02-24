use std::fmt::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Cross,
}

pub struct SqlBuilder {
    ctes: Vec<(String, String, bool)>, // (name, query, recursive)
    select: Vec<String>,
    from: Option<String>,
    joins: Vec<String>,
    where_conditions: Vec<String>,
    group_by: Vec<String>,
    order_by: Vec<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    distinct: bool,
}

impl SqlBuilder {
    pub fn new() -> Self {
        Self {
            ctes: Vec::new(),
            select: Vec::new(),
            from: None,
            joins: Vec::new(),
            where_conditions: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
        }
    }

    pub fn add_cte(&mut self, name: &str, query: &str) {
        self.ctes.push((name.to_string(), query.to_string(), false));
    }

    pub fn add_cte_recursive(&mut self, name: &str, query: &str) {
        self.ctes.push((name.to_string(), query.to_string(), true));
    }

    pub fn add_select(&mut self, expr: &str) {
        self.select.push(expr.to_string());
    }

    pub fn add_select_aliased(&mut self, expr: &str, alias: &str) {
        self.select.push(format!("{expr} AS {alias}"));
    }

    pub fn set_from(&mut self, table: &str) {
        self.from = Some(table.to_string());
    }

    pub fn set_from_aliased(&mut self, table: &str, alias: &str) {
        self.from = Some(format!("{table} AS {alias}"));
    }

    pub fn add_join(&mut self, join_type: JoinType, table: &str, on: &str) {
        // Turso doesn't support CROSS JOIN, so we use JOIN ... ON 1=1 instead
        let keyword = match join_type {
            JoinType::Inner | JoinType::Cross => "JOIN",
            JoinType::Left => "LEFT JOIN",
        };
        let condition = if join_type == JoinType::Cross {
            "1=1"
        } else {
            on
        };
        self.joins
            .push(format!(" {keyword} {table} ON {condition}"));
    }

    pub fn add_join_aliased(&mut self, join_type: JoinType, table: &str, alias: &str, on: &str) {
        // Turso doesn't support CROSS JOIN, so we use JOIN ... ON 1=1 instead
        let keyword = match join_type {
            JoinType::Inner | JoinType::Cross => "JOIN",
            JoinType::Left => "LEFT JOIN",
        };
        let condition = if join_type == JoinType::Cross {
            "1=1"
        } else {
            on
        };
        self.joins
            .push(format!(" {keyword} {table} AS {alias} ON {condition}"));
    }

    pub fn add_join_raw(&mut self, raw_join: &str) {
        self.joins.push(raw_join.to_string());
    }

    pub fn add_where(&mut self, condition: &str) {
        self.where_conditions.push(condition.to_string());
    }

    pub fn add_group_by(&mut self, expr: &str) {
        self.group_by.push(expr.to_string());
    }

    pub fn add_order_by(&mut self, expr: &str, desc: bool) {
        if desc {
            self.order_by.push(format!("{expr} DESC"));
        } else {
            self.order_by.push(expr.to_string());
        }
    }

    pub fn set_limit(&mut self, n: i64) {
        self.limit = Some(n);
    }

    pub fn set_offset(&mut self, n: i64) {
        self.offset = Some(n);
    }

    pub fn set_distinct(&mut self, d: bool) {
        self.distinct = d;
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    pub fn build(&self) -> String {
        let mut sql = String::with_capacity(256);

        // CTEs
        if !self.ctes.is_empty() {
            let any_recursive = self.ctes.iter().any(|(_, _, r)| *r);
            if any_recursive {
                sql.push_str("WITH RECURSIVE ");
            } else {
                sql.push_str("WITH ");
            }
            for (i, (name, query, _)) in self.ctes.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let _ = write!(sql, "{name} AS ({query})");
            }
            sql.push(' ');
        }

        // SELECT
        if self.distinct {
            sql.push_str("SELECT DISTINCT ");
        } else {
            sql.push_str("SELECT ");
        }
        if self.select.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.select.join(", "));
        }

        // FROM
        if let Some(ref from) = self.from {
            sql.push_str(" FROM ");
            sql.push_str(from);

            // JOINs
            for join in &self.joins {
                sql.push_str(join);
            }
        }

        // WHERE
        if !self.where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_conditions.join(" AND "));
        }

        // GROUP BY
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // ORDER BY
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        // LIMIT
        match (self.limit, self.offset) {
            (Some(l), _) => {
                let _ = write!(sql, " LIMIT {l}");
            }
            (None, Some(_)) => {
                sql.push_str(" LIMIT -1");
            }
            _ => {}
        }

        // OFFSET
        if let Some(o) = self.offset {
            let _ = write!(sql, " OFFSET {o}");
        }

        sql
    }
}

impl Default for SqlBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WriteBuilder {
    statements: Vec<String>,
}

impl WriteBuilder {
    pub fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    pub fn add_statement(&mut self, sql: &str) {
        self.statements.push(sql.to_string());
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn build(&self) -> String {
        self.statements.join("; ")
    }
}

impl Default for WriteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut b = SqlBuilder::new();
        b.add_select("a.id");
        b.set_from("nodes AS a");
        assert_eq!(b.build(), "SELECT a.id FROM nodes AS a");
    }

    #[test]
    fn test_select_with_join_and_where() {
        let mut b = SqlBuilder::new();
        b.add_select("a.id");
        b.set_from("nodes AS a");
        b.add_join(JoinType::Inner, "node_labels AS nl", "nl.node_id = a.id");
        b.add_where("nl.label = 'Person'");
        assert_eq!(
            b.build(),
            "SELECT a.id FROM nodes AS a JOIN node_labels AS nl ON nl.node_id = a.id WHERE nl.label = 'Person'"
        );
    }

    #[test]
    fn test_distinct_limit_offset() {
        let mut b = SqlBuilder::new();
        b.set_distinct(true);
        b.add_select("x");
        b.set_from("t");
        b.set_limit(10);
        b.set_offset(5);
        assert_eq!(b.build(), "SELECT DISTINCT x FROM t LIMIT 10 OFFSET 5");
    }

    #[test]
    fn test_offset_without_limit() {
        let mut b = SqlBuilder::new();
        b.add_select("x");
        b.set_from("t");
        b.set_offset(5);
        assert_eq!(b.build(), "SELECT x FROM t LIMIT -1 OFFSET 5");
    }

    #[test]
    fn test_write_builder() {
        let mut wb = WriteBuilder::new();
        wb.add_statement("INSERT INTO nodes DEFAULT VALUES");
        wb.add_statement(
            "INSERT INTO node_labels (node_id, label) VALUES (last_insert_rowid(), 'Person')",
        );
        let sql = wb.build();
        assert!(sql.contains("INSERT INTO nodes"));
        assert!(sql.contains("; INSERT INTO node_labels"));
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("O'Brien"), "O''Brien");
        assert_eq!(escape_sql_string("hello"), "hello");
    }

    #[test]
    fn test_cte() {
        let mut b = SqlBuilder::new();
        b.add_cte("cte1", "SELECT 1");
        b.add_select("*");
        b.set_from("cte1");
        assert_eq!(b.build(), "WITH cte1 AS (SELECT 1) SELECT * FROM cte1");
    }

    #[test]
    fn test_order_by() {
        let mut b = SqlBuilder::new();
        b.add_select("name");
        b.set_from("t");
        b.add_order_by("name", false);
        b.add_order_by("age", true);
        assert_eq!(b.build(), "SELECT name FROM t ORDER BY name, age DESC");
    }

    #[test]
    fn test_group_by() {
        let mut b = SqlBuilder::new();
        b.add_select("label");
        b.add_select("COUNT(*)");
        b.set_from("node_labels");
        b.add_group_by("label");
        assert_eq!(
            b.build(),
            "SELECT label, COUNT(*) FROM node_labels GROUP BY label"
        );
    }
}
