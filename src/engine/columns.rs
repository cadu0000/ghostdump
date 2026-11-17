//! Column orders read from the `CREATE TABLE` statements of a dump, for the
//! `INSERT ... VALUES` statements that have no column list.

use std::collections::HashMap;

use crate::engine::config::CompiledConfig;
use crate::parser::{TableChange, TableName};

/// What is known about the columns of a table with rules.
#[derive(Debug, PartialEq)]
pub enum Columns<'a> {
    /// The columns a row fills, in order.
    Known(&'a [String]),
    /// The table was created in a way that does not show its columns, or a
    /// later statement may have changed them.
    Unknown,
    /// No `CREATE TABLE` for the table came before.
    Missing,
}

/// The column order of each table with rules, from the statements read so
/// far. It holds one entry per table with rules, so it grows with the rules,
/// not with the rows.
#[derive(Debug, Default)]
pub struct ColumnOrders {
    /// By [`TableName::key`]; `None` when the order is unknown.
    tables: HashMap<String, Option<Vec<String>>>,
}

impl ColumnOrders {
    /// Takes what a statement changed. Returns whether what is known about
    /// a table with rules changed.
    pub fn apply(&mut self, change: TableChange, config: &CompiledConfig) -> bool {
        let has_rules = |table: &TableName| {
            config
                .rules_for(table.schema.as_deref(), &table.table)
                .is_some()
        };
        match change {
            TableChange::Created { table, columns } if has_rules(&table) => {
                self.tables.insert(table.key, columns);
                true
            }
            TableChange::Created { .. } => false,
            TableChange::Changed(tables) => {
                let mut changed = false;
                for table in tables.into_iter().filter(|table| has_rules(table)) {
                    self.tables.insert(table.key, None);
                    changed = true;
                }
                changed
            }
            TableChange::Unknown => {
                self.tables.values_mut().for_each(|columns| *columns = None);
                !self.tables.is_empty()
            }
        }
    }

    pub fn columns(&self, key: &str) -> Columns<'_> {
        match self.tables.get(key) {
            Some(Some(columns)) => Columns::Known(columns),
            Some(None) => Columns::Unknown,
            None => Columns::Missing,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::config::Config;
    use crate::parser::{SqlDialect, Statement};

    fn config() -> CompiledConfig {
        toml::from_str::<Config>(
            r#"
            [tables.users]
            columns = [{ name = "name", strategy = "nullify" }]
        "#,
        )
        .unwrap()
        .compile()
        .unwrap()
    }

    fn apply(orders: &mut ColumnOrders, sql: &str) -> bool {
        let change = Statement::parse(sql.as_bytes(), SqlDialect::Postgres)
            .table_change()
            .expect("a statement that changes tables");
        orders.apply(change, &config())
    }

    #[test]
    fn test_orders_follow_create_alter_and_drop() {
        let mut orders = ColumnOrders::default();
        assert_eq!(orders.columns("users"), Columns::Missing);

        assert!(apply(
            &mut orders,
            "CREATE TABLE users (id int, name text);"
        ));
        let expected = ["id".to_string(), "name".to_string()];
        assert_eq!(orders.columns("users"), Columns::Known(&expected));

        // Only tables with rules are kept.
        assert!(!apply(&mut orders, "CREATE TABLE other (a int);"));
        assert_eq!(orders.columns("other"), Columns::Missing);

        assert!(apply(&mut orders, "ALTER TABLE users ADD COLUMN age int;"));
        assert_eq!(orders.columns("users"), Columns::Unknown);

        assert!(apply(&mut orders, "CREATE TABLE users (name text);"));
        assert!(apply(&mut orders, "DROP TABLE IF EXISTS other, users;"));
        assert_eq!(orders.columns("users"), Columns::Unknown);
    }
}
