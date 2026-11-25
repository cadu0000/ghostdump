use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;

use crate::engine::mask::{Guarantee, Masker};
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
///
/// Table and column names are matched without regard to case (see
/// [`fold_case`]), in every dialect: PostgreSQL folds unquoted names to lower
/// case, MySQL never distinguishes case in column names, and SQLite does not
/// distinguish it at all. Matching a name that the database would treat as
/// distinct can only mask one table more, which errs on the safe side.
#[derive(Debug)]
pub struct CompiledConfig {
    /// Rules by table, keyed by the folded name (see [`fold_case`]): `table`,
    /// or `schema.table` for a rule that names its schema.
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
        if self.column.is_empty() {
            write!(f, "table `{}`: {}", self.table, self.message)
        } else {
            write!(
                f,
                "table `{}`, column `{}`: {}",
                self.table, self.column, self.message
            )
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Validates every rule and builds its masker, before any data is read.
    pub fn compile(self) -> Result<CompiledConfig, ConfigError> {
        let mut tables = HashMap::with_capacity(self.tables.len());

        for (table, table_config) in self.tables {
            let key = fold_case(&table);
            let mut columns: Vec<CompiledColumn> = Vec::with_capacity(table_config.columns.len());
            for column in table_config.columns {
                if columns.iter().any(|c| c.matches(&column.name)) {
                    return Err(ConfigError {
                        table,
                        column: column.name,
                        message: "the column has more than one rule (names are compared \
                                  without case)"
                            .into(),
                    });
                }
                let mut masker = column.strategy.compile().map_err(|message| ConfigError {
                    table: table.clone(),
                    column: column.name.clone(),
                    message,
                })?;
                // Bound to the folded names, so consistent noise does not
                // depend on how a dump spells them.
                masker.bind(&key, &fold_case(&column.name));
                columns.push(CompiledColumn {
                    name: column.name,
                    null_policy: column.null_policy,
                    masker,
                });
            }
            if tables.insert(key, columns).is_some() {
                return Err(ConfigError {
                    table,
                    column: String::new(),
                    message: "the table has more than one set of rules (names are compared \
                              without case)"
                        .into(),
                });
            }
        }

        Ok(CompiledConfig { tables })
    }
}

/// What the rules of one table guarantee, for the summary printed at startup.
#[derive(Debug, PartialEq)]
pub struct TableGuarantees {
    pub table: String,
    /// Columns with ε-LDP and their ε.
    pub ldp: Vec<(String, f64)>,
    /// Columns whose output does not depend on the value (0-LDP).
    pub input_independent: Vec<String>,
    /// Columns pseudonymized with a keyed hash (no DP).
    pub pseudonymized: Vec<String>,
}

impl TableGuarantees {
    /// ε spent by one row: the sum over its ε-LDP columns.
    pub fn epsilon_per_row(&self) -> f64 {
        self.ldp.iter().map(|(_, epsilon)| epsilon).sum()
    }
}

impl fmt::Display for TableGuarantees {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table `{}`: ", self.table)?;
        if self.ldp.is_empty() {
            write!(f, "no ε-LDP column")?;
        } else {
            let columns: Vec<String> = self
                .ldp
                .iter()
                .map(|(name, epsilon)| format!("{name} ε = {epsilon:.4}"))
                .collect();
            write!(
                f,
                "ε = {:.4} per row ({})",
                self.epsilon_per_row(),
                columns.join(", ")
            )?;
        }
        if !self.input_independent.is_empty() {
            write!(f, "; 0-LDP: {}", self.input_independent.join(", "))?;
        }
        if !self.pseudonymized.is_empty() {
            write!(
                f,
                "; pseudonymized, no DP: {}",
                self.pseudonymized.join(", ")
            )?;
        }
        Ok(())
    }
}

/// A name in the form rules are compared in: each character mapped to upper
/// case and then to lower case. This ignores the case of ASCII letters, as
/// PostgreSQL does for unquoted names and SQLite for every name, and of other
/// letters too, as MySQL does for column names (`Ação` and `AÇÃO` are the
/// same column). Going through upper case also joins `σ` and the final `ς`.
pub fn fold_case(name: &str) -> String {
    folded(name).collect()
}

fn folded(name: &str) -> impl Iterator<Item = char> + '_ {
    name.chars()
        .flat_map(char::to_uppercase)
        .flat_map(char::to_lowercase)
}

impl CompiledColumn {
    /// Whether the rule is for the column `name`, compared without case.
    pub fn matches(&self, name: &str) -> bool {
        folded(&self.name).eq(folded(name))
    }
}

impl CompiledConfig {
    /// The rules of a table and the key they are stored under. A rule for
    /// `schema.table` takes precedence over a rule for `table`, which
    /// applies to the table in any schema.
    pub fn rules_for(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Option<(&str, &[CompiledColumn])> {
        let table = fold_case(table);
        let qualified = schema.map(|schema| format!("{}.{table}", fold_case(schema)));
        qualified
            .and_then(|key| self.tables.get_key_value(&key))
            .or_else(|| self.tables.get_key_value(&table))
            .map(|(key, columns)| (key.as_str(), columns.as_slice()))
    }

    /// The key of the rules that a name written in a statement may refer to.
    /// Without a schema, the name may also be the table of a rule for
    /// `schema.table`, since the statement may run with that schema in its
    /// search path.
    pub fn rules_named(&self, schema: Option<&str>, table: &str) -> Option<&str> {
        if let Some((key, _)) = self.rules_for(schema, table) {
            return Some(key);
        }
        if schema.is_some() {
            return None;
        }
        let table = fold_case(table);
        self.tables
            .keys()
            .filter(|key| key.rsplit_once('.').is_some_and(|(_, name)| name == table))
            .min()
            .map(String::as_str)
    }

    /// The guarantees of each table's rules, sorted by table name.
    pub fn guarantees(&self) -> Vec<TableGuarantees> {
        let mut tables: Vec<TableGuarantees> = self
            .tables
            .iter()
            .map(|(table, columns)| {
                let mut summary = TableGuarantees {
                    table: table.clone(),
                    ldp: Vec::new(),
                    input_independent: Vec::new(),
                    pseudonymized: Vec::new(),
                };
                for column in columns {
                    let name = column.name.clone();
                    match column.masker.guarantee() {
                        Guarantee::Ldp { epsilon } => summary.ldp.push((name, epsilon)),
                        Guarantee::InputIndependent => summary.input_independent.push(name),
                        Guarantee::Pseudonymization => summary.pseudonymized.push(name),
                    }
                }
                summary
            })
            .collect();
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        tables
    }

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

    /// Whether any column draws Laplace noise with the `f64` sampler, which
    /// the CLI accepts only for experiments.
    pub fn uses_f64_sampler(&self) -> bool {
        self.tables
            .values()
            .flatten()
            .any(|column| column.masker.uses_f64_sampler())
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

    #[test]
    fn test_guarantees_summary() {
        let config: Config = toml::from_str(
            r#"
            [tables.users]
            columns = [
                { name = "id", strategy = "hmac" },
                { name = "name", strategy = "faker_name" },
                { name = "salary", strategy = "dp_laplace", epsilon = 0.5, bounds = [0.0, 1000.0], scale = 2 },
                { name = "age", strategy = "dp_laplace", epsilon = 0.25, bounds = [0.0, 120.0], scale = 0, mechanism = "f64" },
            ]
            "#,
        )
        .unwrap();
        let compiled = config.compile().unwrap();
        assert!(compiled.uses_f64_sampler());
        let summary = compiled.guarantees();
        assert_eq!(summary.len(), 1);
        assert!((summary[0].epsilon_per_row() - 0.75).abs() < 1e-9);
        assert_eq!(
            summary[0].to_string(),
            "table `users`: ε = 0.7500 per row (salary ε = 0.5000, age ε = 0.2500); \
             0-LDP: name; pseudonymized, no DP: id"
        );
    }

    fn compile_rules(rules: &str) -> Result<CompiledConfig, ConfigError> {
        toml::from_str::<Config>(rules).unwrap().compile()
    }

    #[test]
    fn test_rules_that_differ_only_in_case_are_rejected() {
        let tables = r#"
            [tables.users]
            columns = [{ name = "a", strategy = "nullify" }]
            [tables.Users]
            columns = [{ name = "b", strategy = "nullify" }]
        "#;
        let err = compile_rules(tables).unwrap_err();
        assert!(
            err.to_string().contains("more than one set of rules"),
            "{err}"
        );

        let columns = r#"
            [tables.users]
            columns = [
                { name = "a", strategy = "nullify" },
                { name = "A", strategy = "hmac" },
            ]
        "#;
        let err = compile_rules(columns).unwrap_err();
        assert!(err.to_string().contains("more than one rule"), "{err}");
    }

    #[test]
    fn test_schema_rule_takes_precedence() {
        let config = compile_rules(
            r#"
            [tables.users]
            columns = [{ name = "a", strategy = "nullify" }]
            [tables."Audit.Users"]
            columns = [{ name = "b", strategy = "nullify" }]
        "#,
        )
        .unwrap();
        let key = |schema, table| config.rules_for(schema, table).map(|(key, _)| key);
        assert_eq!(key(Some("AUDIT"), "USERS"), Some("audit.users"));
        assert_eq!(key(Some("public"), "Users"), Some("users"));
        assert_eq!(key(None, "users"), Some("users"));
        assert_eq!(key(None, "orders"), None);
    }

    #[test]
    fn test_case_is_ignored_outside_ascii() {
        let config = compile_rules(
            r#"
            [tables."Ação"]
            columns = [{ name = "ΟΔΟΣ", strategy = "nullify" }]
        "#,
        )
        .unwrap();
        let (key, rules) = config.rules_for(None, "AÇÃO").unwrap();
        assert_eq!(key, "ação");
        assert!(rules[0].matches("οδος"));
        assert!(rules[0].matches("οδοσ"));
        assert!(!rules[0].matches("odos"));

        let err = compile_rules(
            r#"
            [tables.t]
            columns = [
                { name = "ação", strategy = "nullify" },
                { name = "AÇÃO", strategy = "nullify" },
            ]
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("more than one rule"), "{err}");
    }

    #[test]
    fn test_names_in_statements_also_find_rules_with_a_schema() {
        let config = compile_rules(
            r#"
            [tables."audit.users"]
            columns = [{ name = "a", strategy = "nullify" }]
        "#,
        )
        .unwrap();
        assert_eq!(config.rules_named(None, "USERS"), Some("audit.users"));
        assert_eq!(
            config.rules_named(Some("audit"), "users"),
            Some("audit.users")
        );
        assert_eq!(config.rules_named(Some("public"), "users"), None);
        assert_eq!(config.rules_named(None, "orders"), None);
    }
}
