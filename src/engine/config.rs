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

    #[serde(flatten)]
    pub strategy: StrategyConfig,
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
