use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

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
impl Config {
    /// Whether any column uses a strategy that needs the secret key.
    pub fn uses_secret(&self) -> bool {
        self.tables
            .values()
            .flat_map(|table| &table.columns)
            .any(|column| matches!(column.strategy, StrategyConfig::Hmac))
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
}
