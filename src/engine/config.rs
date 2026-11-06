use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;

use crate::engine::mask::Masker;
use crate::engine::strategy::StrategyConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub tables: HashMap<String, TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnConfig {
    pub name: String,

    /// What to do with NULL values of this column.
    #[serde(default)]
    pub null_policy: NullPolicy,

    #[serde(flatten)]
    pub strategy: StrategyConfig,
}

/// `keep` (the default) writes NULL as NULL. `generate` masks NULL values too,
/// with strategies that do not depend on the original value.
#[derive(Debug, Default, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    #[default]
    Keep,
    Generate,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
/// Rules validated and compiled, ready to be applied.
#[derive(Debug)]
pub struct CompiledConfig {
    pub tables: HashMap<String, Vec<CompiledColumn>>,
}

#[derive(Debug)]
pub struct CompiledColumn {
    pub name: String,
    pub null_policy: NullPolicy,
    pub masker: Masker,
}

/// A rule that cannot be compiled, with the table and column it belongs to.
#[derive(Debug, PartialEq)]
pub struct ConfigError {
    pub table: String,
    pub column: String,
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "table `{}`, column `{}`: {}",
            self.table, self.column, self.message
        )
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Validates every rule and builds its masker, before any data is read.
    pub fn compile(self) -> Result<CompiledConfig, ConfigError> {
        let mut tables = HashMap::with_capacity(self.tables.len());

        for (table, table_config) in self.tables {
            let mut columns = Vec::with_capacity(table_config.columns.len());
            for column in table_config.columns {
                let masker = column.strategy.compile().map_err(|message| ConfigError {
                    table: table.clone(),
                    column: column.name.clone(),
                    message,
                })?;
                columns.push(CompiledColumn {
                    name: column.name,
                    null_policy: column.null_policy,
                    masker,
                });
            }
            tables.insert(table, columns);
        }

        Ok(CompiledConfig { tables })
    }
}

impl CompiledConfig {
    /// No rules: the dump is copied as it is.
    pub fn empty() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Whether any column uses a strategy that needs the secret key.
    pub fn uses_secret(&self) -> bool {
        self.tables
            .values()
            .flatten()
            .any(|column| column.masker.uses_secret())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_policy_defaults_to_keep() {
        let config: Config = toml::from_str(
            r#"
            [tables.t]
            columns = [
                { name = "a", strategy = "faker_name" },
                { name = "b", strategy = "faker_name", null_policy = "generate" },
            ]
            "#,
        )
        .unwrap();
        let columns = &config.tables["t"].columns;
        assert_eq!(columns[0].null_policy, NullPolicy::Keep);
        assert_eq!(columns[1].null_policy, NullPolicy::Generate);
    }

    #[test]
    fn test_compile_errors_name_table_and_column() {
        let config: Config = toml::from_str(
            r#"
            [tables.users]
            columns = [{ name = "salary", strategy = "dp_laplace", epsilon = 1.0, sensitivity = 1.0 }]
            "#,
        )
        .unwrap();
        let err = config.compile().unwrap_err();
        assert_eq!(err.table, "users");
        assert_eq!(err.column, "salary");
        assert!(
            err.to_string()
                .starts_with("table `users`, column `salary`: ")
        );
    }

    #[test]
    fn test_uses_secret() {
        let config: Config = toml::from_str(
            r#"
            [tables.t]
            columns = [{ name = "id", strategy = "hmac" }]
            "#,
        )
        .unwrap();
        assert!(config.compile().unwrap().uses_secret());
        assert!(!CompiledConfig::empty().uses_secret());
    }
}
