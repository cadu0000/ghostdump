pub mod config;
pub mod strategy;

use log::{debug, warn};
use rand::{Rng, thread_rng};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use crate::codec::{self, FieldMasker, SqlValue};
use crate::engine::config::{ColumnConfig, Config, NullPolicy};
use crate::parser::state::SqlDialect;
use crate::parser::{HeaderMetadata, Remainder, SqlEvent, SqlParser};

/// Summary of a run, returned to the caller instead of printed.
#[derive(Debug, Default, PartialEq)]
pub struct RunReport {
    pub rows_processed: usize,
    /// The input ended inside a data block, before its terminator.
    pub unterminated_data_block: bool,
}

/// For each position in a data block's column list, the masked column there, if any.
type MaskingPlan<'a> = Vec<Option<&'a ColumnConfig>>;

pub struct AnonymizerEngine {
    config: Config,
    secret: String,
}

impl AnonymizerEngine {
    pub fn new(config: Config, secret: String) -> Self {
        Self { config, secret }
    }

    /// The columns of a data block that have rules, or `None` when no
    /// column has one (the rows are then copied as they are).
    fn masking_plan(&self, metadata: &HeaderMetadata) -> Option<MaskingPlan<'_>> {
        let table = self.config.tables.get(&metadata.table)?;

        let Some(columns) = &metadata.columns else {
            warn!(
                "table `{}` has rules, but its INSERT has no column list; its rows are \
                 written unmasked",
                metadata.table
            );
            return None;
        };

        let plan: MaskingPlan = columns
            .iter()
            .map(|name| table.columns.iter().find(|c| c.name == *name))
            .collect();
        plan.iter().any(Option::is_some).then_some(plan)
    }

    pub fn process_dump(
        &self,
        input: impl Read,
        output: impl Write,
        dialect: SqlDialect,
        dry_run: bool,
        limit: Option<usize>,
    ) -> io::Result<RunReport> {
        debug!("engine start: dialect {dialect:?}, dry run {dry_run}, limit {limit:?}");
        let mut reader = BufReader::new(input);
        let mut writer: Box<dyn Write> = if dry_run {
            Box::new(io::sink())
        } else {
            Box::new(BufWriter::new(output))
        };
        let mut parser = SqlParser::new(dialect);
        let mut rng = thread_rng();

        let mut buffer = [0u8; 64 * 1024];
        let mut rows_processed = 0;
        let mut reached_limit = false;
        let mut unterminated_data_block = false;

        let mut plan: Option<MaskingPlan> = None;
        let mut plan_metadata: Option<Arc<HeaderMetadata>> = None;
        let mut row_buffer = Vec::with_capacity(1024);

        'leitura: loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            for &byte in &buffer[..bytes_read] {
                let Some(event) = parser.handle_byte(byte) else {
                    continue;
                };
                match event {
                    SqlEvent::DefaultStatement(b) => writer.write_all(&[b])?,
                    SqlEvent::Passthrough(bytes) => writer.write_all(&bytes)?,

                    SqlEvent::Header {
                        raw_bytes,
                        metadata,
                    } => {
                        // pg_dump --inserts repeats the same header on every row.
                        let same = match (&metadata, &plan_metadata) {
                            (Some(new), Some(old)) => Arc::ptr_eq(new, old),
                            _ => false,
                        };
                        if !same {
                            plan = metadata.as_deref().and_then(|m| self.masking_plan(m));
                            if let Some(m) = metadata.as_deref() {
                                log_masking_plan(m, plan.as_deref());
                            }
                            plan_metadata = metadata;
                        }
                        writer.write_all(&raw_bytes)?;
                    }

                    SqlEvent::Tuple(row, format) => {
                        match &plan {
                            None => writer.write_all(&row)?,
                            Some(columns) => {
                                let mut masker = RowMasker {
                                    columns,
                                    rng: &mut rng,
                                    secret: &self.secret,
                                };
                                // Build the row in memory: one write instead of one per piece.
                                row_buffer.clear();
                                codec::rewrite_row(
                                    &row,
                                    format,
                                    dialect,
                                    &mut masker,
                                    &mut row_buffer,
                                )?;
                                writer.write_all(&row_buffer)?;
                            }
                        }

                        rows_processed += 1;

                        if let Some(l) = limit
                            && rows_processed >= l
                        {
                            reached_limit = true;
                            break 'leitura;
                        }
                    }

                    SqlEvent::Footer(footer_bytes) => writer.write_all(&footer_bytes)?,
                }
            }
        }

        // With `--limit` the output stops on purpose, in the middle of the input.
        if !reached_limit {
            match parser.finish() {
                Remainder::Bytes(rest) => writer.write_all(&rest)?,
                Remainder::UnterminatedData { partial_row: false } => {
                    warn!(
                        "the input ended inside a data block, before its terminator (`;` or `\\.`)"
                    );
                    unterminated_data_block = true;
                }
                Remainder::UnterminatedData { partial_row: true } => {
                    writer.flush()?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "the input ended in the middle of a data row (truncated dump?). \
                         The incomplete row was not written, because it could not be masked.",
                    ));
                }
            }
        }

        writer.flush()?;
        Ok(RunReport {
            rows_processed,
            unterminated_data_block,
        })
    }
}

struct RowMasker<'a, R> {
    columns: &'a [Option<&'a ColumnConfig>],
    rng: &'a mut R,
    secret: &'a str,
}

impl<R: Rng> FieldMasker for RowMasker<'_, R> {
    fn masks(&self, column: usize) -> bool {
        matches!(self.columns.get(column), Some(Some(_)))
    }

    fn mask(&mut self, column: usize, value: SqlValue) -> SqlValue {
        let Some(Some(config)) = self.columns.get(column) else {
            return value;
        };
        if value.is_null() && config.null_policy == NullPolicy::Keep {
            return SqlValue::Null;
        }
        config.strategy.apply(value, self.rng, self.secret)
    }
}

/// Logs which columns of a data block are masked. Only schema names are
/// logged, never values.
fn log_masking_plan(metadata: &HeaderMetadata, plan: Option<&[Option<&ColumnConfig>]>) {
    if !log::log_enabled!(log::Level::Debug) {
        return;
    }
    let masked: Vec<&str> = plan
        .unwrap_or_default()
        .iter()
        .flatten()
        .map(|column| column.name.as_str())
        .collect();
    if masked.is_empty() {
        debug!("table `{}`: no masked columns, rows are copied", metadata.table);
    } else {
        debug!("table `{}`: masking {}", metadata.table, masked.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn engine(rules: &str) -> AnonymizerEngine {
        let config = if rules.is_empty() {
            Config {
                tables: HashMap::new(),
            }
        } else {
            toml::from_str(rules).unwrap()
        };
        AnonymizerEngine::new(config, "test-secret-0123456789".into())
    }

    fn run_with(rules: &str, dialect: SqlDialect, input: &[u8]) -> io::Result<String> {
        let mut out = Vec::new();
        let report = engine(rules).process_dump(input, &mut out, dialect, false, None)?;
        assert!(!report.unterminated_data_block);
        Ok(String::from_utf8(out).unwrap())
    }

    fn run(input: &[u8]) -> io::Result<Vec<u8>> {
        run_with("", SqlDialect::Postgres, input).map(String::into_bytes)
    }

    const NAME_RULES: &str = r#"
        [tables.users]
        columns = [
            { name = "name", strategy = "fixed", value = "O'Conner" },
            { name = "age", strategy = "dp_laplace", epsilon = 1.0, sensitivity = 1.0 },
            { name = "secret", strategy = "nullify" },
        ]
    "#;

    #[test]
    fn test_statements_outside_data_blocks_round_trip() {
        let input = include_bytes!("../../tests/fixtures/passthrough.sql");
        assert_eq!(
            String::from_utf8_lossy(&run(input).unwrap()),
            String::from_utf8_lossy(input)
        );
    }

    #[test]
    fn test_data_blocks_round_trip_without_rules() {
        let input = b"INSERT INTO t (a, b) VALUES\n  (1, 'tem (parenteses)') , -- x\n  (2, NULL)\nON CONFLICT DO NOTHING;\nCOPY t (a) FROM stdin;\n\n1\t\\N\n\\.\n";
        assert_eq!(
            String::from_utf8_lossy(&run(input).unwrap()),
            String::from_utf8_lossy(input)
        );
    }

    #[test]
    fn test_insert_masking_keeps_types_and_layout() {
        let input = b"INSERT INTO users (id, name, age, secret, note) VALUES (1, 'Ana', 30, 'senha123', 'tem (parenteses)');\n";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap();

        assert!(out.starts_with("INSERT INTO users (id, name, age, secret, note) VALUES (1, 'O''Conner', "));
        assert!(out.ends_with(", NULL, 'tem (parenteses)');\n"), "{out}");
        assert!(!out.contains("senha123"));
        // The number stays unquoted.
        let age = out.split(", ").nth(6).unwrap();
        assert!(age.parse::<f64>().is_ok(), "{age}");
    }

    #[test]
    fn test_copy_masking() {
        let input = b"COPY public.users (id, name, secret) FROM stdin;\n1\tAna\tx\n2\t\\N\t\\N\n\\.\n";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap();
        assert_eq!(
            out,
            "COPY public.users (id, name, secret) FROM stdin;\n1\tO'Conner\t\\N\n2\t\\N\t\\N\n\\.\n"
        );
    }

    #[test]
    fn test_null_policy_generate() {
        let rules = r#"
            [tables.t]
            columns = [
                { name = "a", strategy = "fixed", value = "x" },
                { name = "b", strategy = "fixed", value = "y", null_policy = "generate" },
            ]
        "#;
        let out = run_with(rules, SqlDialect::Postgres, b"INSERT INTO t (a, b) VALUES (NULL, NULL);").unwrap();
        assert_eq!(out, "INSERT INTO t (a, b) VALUES (NULL, 'y');");
    }

    #[test]
    fn test_mysql_escaping() {
        let out = run_with(NAME_RULES, SqlDialect::Mysql, b"INSERT INTO users (name) VALUES ('Bob');").unwrap();
        assert_eq!(out, "INSERT INTO users (name) VALUES ('O\\'Conner');");
    }

    #[test]
    fn test_hmac_is_the_same_for_insert_and_copy() {
        let rules = r#"
            [tables.t]
            columns = [{ name = "a", strategy = "hmac" }]
        "#;
        let insert = run_with(rules, SqlDialect::Postgres, b"INSERT INTO t (a) VALUES ('O''Hara');").unwrap();
        let copy = run_with(rules, SqlDialect::Postgres, b"COPY t (a) FROM stdin;\nO'Hara\n\\.\n").unwrap();

        let from_insert = insert.split('\'').nth(1).unwrap().to_string();
        let from_copy = copy.lines().nth(1).unwrap().to_string();
        assert_eq!(from_insert.len(), 64);
        assert_eq!(from_insert, from_copy);
    }

    #[test]
    fn test_insert_without_column_list_is_copied() {
        let input = b"INSERT INTO users VALUES (1, 'Ana');";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap();
        assert_eq!(out.as_bytes(), input);
    }

    #[test]
    fn test_truncated_row_is_an_error() {
        let err = run(b"INSERT INTO t (a) VALUES (1), (2").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
