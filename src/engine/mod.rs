pub mod categorical;
pub mod config;
pub mod laplace;
pub mod mask;
pub mod sink;
pub mod strategy;

use log::{debug, warn};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::BTreeMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use crate::engine::config::{CompiledColumn, CompiledConfig};
use crate::engine::sink::{MaskingPlan, SequentialSink, Sink};
use crate::parser::state::{InsertFormat, SqlDialect};
use crate::parser::{DEFAULT_MAX_ROW_BYTES, HeaderMetadata, Remainder, SqlEvent, SqlParser};

/// How a dump is processed.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub dialect: SqlDialect,
    /// Process the input without writing anything.
    pub dry_run: bool,
    /// Stop after this many data rows, closing the statement they belong to.
    pub limit: Option<usize>,
    /// Longer rows stop the run with an error.
    pub max_row_bytes: usize,
    /// Seed for every random choice, for reproducible experiments. Anyone who
    /// knows it can recompute the generated values and the noise.
    pub seed: Option<u64>,
}

impl RunOptions {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            dry_run: false,
            limit: None,
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
            seed: None,
        }
    }
}

/// Summary of a run, returned to the caller instead of printed.
#[derive(Debug, Default, PartialEq)]
pub struct RunReport {
    pub rows_processed: usize,
    /// The input ended inside a data block, before its terminator.
    pub unterminated_data_block: bool,
    /// Values of `dp_laplace` columns that were not numbers. They were
    /// treated as the midpoint of their bounds, so the guarantee holds.
    pub non_numeric_dp_values: usize,
    /// Values of `randomized_response` columns that were not among their
    /// categories. They were replaced by a uniformly random category.
    pub values_outside_categories: usize,
    /// Bytes read from the input.
    pub bytes_read: u64,
    /// Data statements (`INSERT`, `REPLACE`, `COPY`) seen.
    pub data_blocks: usize,
    /// Rows rewritten because their table has rules.
    pub rows_masked: usize,
    /// Fields that went through a rule.
    pub fields_masked: usize,
    /// Data rows per table, by the name in the statement (`<unknown>` when
    /// it could not be read). Schema names only, never values.
    pub rows_by_table: BTreeMap<String, usize>,
}

pub struct AnonymizerEngine {
    config: CompiledConfig,
    secret: String,
}

impl AnonymizerEngine {
    pub fn new(config: CompiledConfig, secret: String) -> Self {
        for (table, columns) in &config.tables {
            for column in columns {
                debug!(
                    "rule `{table}.{}`: {}",
                    column.name,
                    column.masker.describe()
                );
            }
        }
        Self { config, secret }
    }

    /// The columns of a data block that have rules, or `None` when no
    /// column has one (the rows are then copied as they are).
    fn masking_plan(&self, metadata: &HeaderMetadata) -> Option<MaskingPlan<'_>> {
        let table = self.config.tables.get(&metadata.table)?;
        let columns = metadata.columns.as_ref()?;

        let plan: MaskingPlan = columns
            .iter()
            .map(|name| table.iter().find(|c| c.name == *name))
            .collect();
        plan.iter().any(Option::is_some).then_some(plan)
    }

    /// Decides how the rows of a data block are written, or refuses the block
    /// when rules exist that cannot be applied to it (fail closed).
    fn plan_block(
        &self,
        metadata: Option<&HeaderMetadata>,
        opaque: bool,
        header: &[u8],
    ) -> io::Result<Option<MaskingPlan<'_>>> {
        let Some(metadata) = metadata else {
            if self.config.tables.is_empty() {
                debug!("data block without table metadata, rows pass through");
                return Ok(None);
            }
            return Err(refuse(format!(
                "could not read the table name of a data block that starts with `{}`; \
                 stopping because rules are configured",
                preview(header)
            )));
        };
        let table = &metadata.table;

        if metadata.columns.is_none() && self.config.tables.contains_key(table) {
            return Err(refuse(format!(
                "table `{table}` has rules, but its INSERT has no column list, so they cannot \
                 be matched to values. Dump it with column names (mysqldump --complete-insert, \
                 pg_dump --column-inserts) or remove its rules"
            )));
        }

        let plan = self.masking_plan(metadata);
        if opaque && plan.is_some() {
            return Err(refuse(format!(
                "table `{table}` has rules, but its COPY uses options GhostDump cannot read \
                 (such as WITH (FORMAT csv)), so its rows cannot be masked"
            )));
        }

        log_masking_plan(metadata, plan.as_deref());
        Ok(plan)
    }

    pub fn process_dump(
        &self,
        input: impl Read,
        output: impl Write,
        options: &RunOptions,
    ) -> io::Result<RunReport> {
        debug!("engine start: {options:?}");
        let writer: Box<dyn Write> = if options.dry_run {
            Box::new(io::sink())
        } else {
            Box::new(BufWriter::new(output))
        };
        // ChaCha-based and seeded from the operating system unless a seed is given.
        let rng = match options.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };
        let sink = SequentialSink::new(writer, rng, options.dialect, &self.secret);
        self.run(input, sink, options)
    }

    /// Parses the input and sends every byte and row to `sink`.
    fn run<'a>(
        &'a self,
        input: impl Read,
        mut sink: impl Sink<'a>,
        options: &RunOptions,
    ) -> io::Result<RunReport> {
        let RunOptions {
            dialect,
            limit,
            max_row_bytes,
            ..
        } = *options;
        let mut reader = BufReader::new(input);
        let mut parser = SqlParser::new(dialect).with_max_row_bytes(max_row_bytes);

        let mut buffer = [0u8; 64 * 1024];
        let mut rows_processed = 0;
        let mut bytes_read_total = 0u64;
        let mut data_blocks = 0;
        let mut rows_masked = 0;
        let mut rows_by_table: BTreeMap<String, usize> = BTreeMap::new();
        // Rows of the current table, added to `rows_by_table` when the table
        // changes, so that rows are counted without a map lookup each.
        let mut current_table = String::new();
        let mut current_rows = 0usize;
        let mut reached_limit = false;
        let mut unterminated_data_block = false;

        let mut plan: Option<Arc<MaskingPlan>> = None;
        let mut plan_metadata: Option<Arc<HeaderMetadata>> = None;

        'leitura: loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            bytes_read_total += bytes_read as u64;

            for &byte in &buffer[..bytes_read] {
                let Some(event) = parser.handle_byte(byte) else {
                    continue;
                };
                match event {
                    SqlEvent::DefaultStatement(b) => sink.write(&[b])?,
                    SqlEvent::Passthrough(bytes) => sink.write(&bytes)?,

                    SqlEvent::Header {
                        raw_bytes,
                        metadata,
                        opaque,
                    } => {
                        // pg_dump --inserts repeats the same header on every row.
                        let same = match (&metadata, &plan_metadata) {
                            (Some(new), Some(old)) => Arc::ptr_eq(new, old),
                            _ => false,
                        };
                        data_blocks += 1;
                        if !same {
                            let table = metadata.as_deref().map_or("<unknown>", |m| &m.table);
                            if table != current_table {
                                count_rows(&mut rows_by_table, &current_table, current_rows);
                                current_table = table.to_string();
                                current_rows = 0;
                            }
                            plan = match self.plan_block(metadata.as_deref(), opaque, &raw_bytes) {
                                Ok(plan) => plan.map(Arc::new),
                                Err(e) => return stop(sink, e),
                            };
                            plan_metadata = metadata;
                        }
                        sink.write(&raw_bytes)?;
                    }

                    SqlEvent::Tuple(row, format) => {
                        match &plan {
                            None => sink.write(&row)?,
                            Some(columns) => {
                                sink.mask(row, format, columns)?;
                                rows_masked += 1;
                            }
                        }

                        rows_processed += 1;
                        current_rows += 1;

                        if let Some(l) = limit
                            && rows_processed >= l
                        {
                            // Close the statement, so the output still loads.
                            sink.write(match format {
                                InsertFormat::Values => b";\n",
                                InsertFormat::Copy => b"\\.\n",
                            })?;
                            reached_limit = true;
                            break 'leitura;
                        }
                    }

                    SqlEvent::Footer(footer_bytes) => sink.write(&footer_bytes)?,
                    SqlEvent::Error(e) => return stop(sink, refuse(e.to_string())),
                }
            }
        }

        // With `--limit` the output stops on purpose, in the middle of the input.
        if !reached_limit {
            match parser.finish() {
                Remainder::Bytes(rest) => sink.write(&rest)?,
                Remainder::UnterminatedData { partial_row: false } => {
                    warn!(
                        "the input ended inside a data block, before its terminator (`;` or `\\.`)"
                    );
                    unterminated_data_block = true;
                }
                Remainder::UnterminatedData { partial_row: true } => {
                    return stop(
                        sink,
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "the input ended in the middle of a data row (truncated dump?). \
                             The incomplete row was not written, because it could not be masked.",
                        ),
                    );
                }
            }
        }

        let stats = sink.finish()?;
        count_rows(&mut rows_by_table, &current_table, current_rows);
        if stats.non_numeric > 0 {
            // One warning per run, without the values.
            warn!(
                "{} value(s) of dp_laplace columns were not numbers; they were treated as \
                 the midpoint of their bounds",
                stats.non_numeric
            );
        }
        if stats.outside_categories > 0 {
            warn!(
                "{} value(s) of randomized_response columns were not among their categories; \
                 they were replaced by a uniformly random category",
                stats.outside_categories
            );
        }
        Ok(RunReport {
            rows_processed,
            unterminated_data_block,
            non_numeric_dp_values: stats.non_numeric,
            values_outside_categories: stats.outside_categories,
            bytes_read: bytes_read_total,
            data_blocks,
            rows_masked,
            fields_masked: stats.masked_fields,
            rows_by_table,
        })
    }
}

fn count_rows(rows_by_table: &mut BTreeMap<String, usize>, table: &str, rows: usize) {
    if rows > 0 {
        *rows_by_table.entry(table.to_string()).or_default() += rows;
    }
}

/// An error that stops the run because the data cannot be handled safely.
fn refuse(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// The start of a header for an error message: schema text, never row values.
fn preview(header: &[u8]) -> String {
    let text = String::from_utf8_lossy(header);
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match text.char_indices().nth(60) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text,
    }
}

/// Writes what came before an error that stops the run, then returns it.
fn stop<'a>(sink: impl Sink<'a>, error: io::Error) -> io::Result<RunReport> {
    sink.finish()?;
    Err(error)
}

/// Logs which columns of a data block are masked. Only schema names are
/// logged, never values.
fn log_masking_plan(metadata: &HeaderMetadata, plan: Option<&[Option<&CompiledColumn>]>) {
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
        debug!(
            "table `{}`: no masked columns, rows are copied",
            metadata.table
        );
    } else {
        debug!("table `{}`: masking {}", metadata.table, masked.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::config::Config;

    fn engine(rules: &str) -> AnonymizerEngine {
        let config = if rules.is_empty() {
            CompiledConfig::empty()
        } else {
            toml::from_str::<Config>(rules).unwrap().compile().unwrap()
        };
        AnonymizerEngine::new(config, "test-secret-0123456789".into())
    }

    fn run_with(rules: &str, dialect: SqlDialect, input: &[u8]) -> io::Result<String> {
        let mut out = Vec::new();
        let report = engine(rules).process_dump(input, &mut out, &RunOptions::new(dialect))?;
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
            { name = "age", strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 120.0], scale = 0 },
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

        assert!(out.starts_with(
            "INSERT INTO users (id, name, age, secret, note) VALUES (1, 'O''Conner', "
        ));
        assert!(out.ends_with(", NULL, 'tem (parenteses)');\n"), "{out}");
        assert!(!out.contains("senha123"));
        // The number stays unquoted.
        let age = out.split(", ").nth(6).unwrap();
        assert!(age.parse::<f64>().is_ok(), "{age}");
    }

    #[test]
    fn test_copy_masking() {
        let input =
            b"COPY public.users (id, name, secret) FROM stdin;\n1\tAna\tx\n2\t\\N\t\\N\n\\.\n";
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
        let out = run_with(
            rules,
            SqlDialect::Postgres,
            b"INSERT INTO t (a, b) VALUES (NULL, NULL);",
        )
        .unwrap();
        assert_eq!(out, "INSERT INTO t (a, b) VALUES (NULL, 'y');");
    }

    #[test]
    fn test_mysql_escaping() {
        let out = run_with(
            NAME_RULES,
            SqlDialect::Mysql,
            b"INSERT INTO users (name) VALUES ('Bob');",
        )
        .unwrap();
        assert_eq!(out, "INSERT INTO users (name) VALUES ('O\\'Conner');");
    }

    #[test]
    fn test_hmac_is_the_same_for_insert_and_copy() {
        let rules = r#"
            [tables.t]
            columns = [{ name = "a", strategy = "hmac" }]
        "#;
        let insert = run_with(
            rules,
            SqlDialect::Postgres,
            b"INSERT INTO t (a) VALUES ('O''Hara');",
        )
        .unwrap();
        let copy = run_with(
            rules,
            SqlDialect::Postgres,
            b"COPY t (a) FROM stdin;\nO'Hara\n\\.\n",
        )
        .unwrap();

        let from_insert = insert.split('\'').nth(1).unwrap().to_string();
        let from_copy = copy.lines().nth(1).unwrap().to_string();
        assert_eq!(from_insert.len(), 64);
        assert_eq!(from_insert, from_copy);
    }

    #[test]
    fn test_replace_and_insert_ignore_are_masked() {
        let input = b"REPLACE INTO `users` (`id`, `name`) VALUES (1,'Ana');\n\
                      INSERT IGNORE INTO `users` (`id`, `name`) VALUES (2,'Bia');\n";
        let out = run_with(NAME_RULES, SqlDialect::Mysql, input).unwrap();
        assert!(!out.contains("Ana") && !out.contains("Bia"), "{out}");
        assert_eq!(out.matches("'O\\'Conner'").count(), 2, "{out}");
    }

    #[test]
    fn test_rules_without_column_list_fail_closed() {
        let err = run_with(
            NAME_RULES,
            SqlDialect::Mysql,
            b"INSERT INTO `users` VALUES (1,'Ana');",
        )
        .unwrap_err();
        assert!(err.to_string().contains("no column list"), "{err}");

        // A table without rules is copied.
        let input = b"INSERT INTO `other` VALUES (1,'Ana');";
        let out = run_with(NAME_RULES, SqlDialect::Mysql, input).unwrap();
        assert_eq!(out.as_bytes(), input);
    }

    #[test]
    fn test_unreadable_table_name_fails_closed_with_rules() {
        let input = b"INSERT INTO \"my table\" (a) VALUES (1);";
        let err = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap_err();
        assert!(
            err.to_string().contains("could not read the table name"),
            "{err}"
        );
        assert!(run(input).is_ok());
    }

    #[test]
    fn test_copy_with_options() {
        let input = b"COPY public.users (id, name) FROM stdin WITH (FORMAT csv);\n1,Ana\n\\.\n";
        let err = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap_err();
        assert!(err.to_string().contains("COPY uses options"), "{err}");

        let other = b"COPY public.other (id) FROM stdin WITH (FORMAT csv);\n1\n\\.\n";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, other).unwrap();
        assert_eq!(out.as_bytes(), other);
    }

    #[test]
    fn test_limit_closes_the_statement() {
        let limited = |input: &[u8]| {
            let mut out = Vec::new();
            let options = RunOptions {
                limit: Some(1),
                ..RunOptions::new(SqlDialect::Postgres)
            };
            engine("").process_dump(input, &mut out, &options).unwrap();
            String::from_utf8(out).unwrap()
        };
        assert_eq!(
            limited(b"INSERT INTO t (a) VALUES (1), (2);\nCOMMIT;\n"),
            "INSERT INTO t (a) VALUES (1);\n"
        );
        assert_eq!(
            limited(b"COPY t (a) FROM stdin;\n1\n2\n\\.\n"),
            "COPY t (a) FROM stdin;\n1\n\\.\n"
        );
    }

    #[test]
    fn test_rows_longer_than_the_limit_stop_the_run() {
        let options = RunOptions {
            max_row_bytes: 8,
            ..RunOptions::new(SqlDialect::Postgres)
        };
        let input = b"COPY t (a) FROM stdin;\n0123456789\n\\.\n";
        let err = engine("")
            .process_dump(&input[..], io::sink(), &options)
            .unwrap_err();
        assert!(err.to_string().contains("longer than 8 bytes"), "{err}");
    }

    #[test]
    fn test_run_report_counts() {
        let input = b"INSERT INTO users (id, name) VALUES (1, 'a'), (2, NULL);\n\
            INSERT INTO users (id, name) VALUES (3, 'c');\n\
            COPY public.other (x) FROM stdin;\n1\n2\n\\.\n";
        let mut out = Vec::new();
        let report = engine(NAME_RULES)
            .process_dump(&input[..], &mut out, &RunOptions::new(SqlDialect::Postgres))
            .unwrap();
        assert_eq!(report.rows_processed, 5);
        assert_eq!(report.bytes_read, input.len() as u64);
        assert_eq!(report.data_blocks, 3);
        assert_eq!(report.rows_masked, 3);
        assert_eq!(report.fields_masked, 3);
        assert_eq!(
            report.rows_by_table,
            BTreeMap::from([("other".to_string(), 2), ("users".to_string(), 3)])
        );
    }

    #[test]
    fn test_truncated_row_is_an_error() {
        let err = run(b"INSERT INTO t (a) VALUES (1), (2").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
