pub mod categorical;
pub mod columns;
pub mod config;
pub mod laplace;
pub mod mask;
pub mod sink;
pub mod strategy;

use log::{debug, warn};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use std::thread;

use crate::engine::columns::{ColumnOrders, Columns};
use crate::engine::config::{CompiledColumn, CompiledConfig};
use crate::engine::sink::{MaskingPlan, ParallelSink, RowContext, SequentialSink, Sink};
use crate::parser::state::{InsertFormat, SqlDialect};
use crate::parser::{
    DEFAULT_MAX_ROW_BYTES, HeaderMetadata, Remainder, SessionSetting, SettingValue, SqlEvent,
    SqlParser, Statement, has_values,
};

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
    /// Threads that mask rows; the calling thread reads, parses and writes.
    /// With 1, rows are masked on the calling thread. The output is the same
    /// for any number, given a seed.
    pub threads: usize,
}

impl RunOptions {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            dry_run: false,
            limit: None,
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
            seed: None,
            threads: 1,
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
    /// Rules that matched no table or column of the input: a misspelt name
    /// leaves its column unmasked. Empty when `--limit` stopped the run.
    pub unused_rules: Vec<UnusedRule>,
}

/// A rule that matched nothing in the input.
#[derive(Debug, Clone, PartialEq)]
pub struct UnusedRule {
    pub table: String,
    /// `None` when no rule of the table matched: the table is absent from
    /// the input, or none of the columns with rules is in it.
    pub column: Option<String>,
}

impl fmt::Display for UnusedRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.column {
            Some(column) => write!(f, "column `{}.{column}` was not found", self.table),
            None => write!(f, "no rule of table `{}` matched the input", self.table),
        }
    }
}

/// A masking plan and the key of the rules it comes from.
type KeyedPlan<'a> = (&'a str, MaskingPlan<'a>);

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
    fn masking_plan(
        &self,
        metadata: &HeaderMetadata,
        schema: Option<&str>,
        columns: &[String],
    ) -> Option<KeyedPlan<'_>> {
        let (key, rules) = self.config.rules_for(schema, &metadata.table)?;

        let plan: MaskingPlan = columns
            .iter()
            .map(|name| rules.iter().find(|rule| rule.matches(name)))
            .collect();
        plan.iter().any(Option::is_some).then_some((key, plan))
    }

    /// Decides how the rows of a data block are written, or refuses the block
    /// when rules exist that cannot be applied to it (fail closed).
    fn plan_block(
        &self,
        metadata: Option<&HeaderMetadata>,
        opaque: bool,
        header: &[u8],
        orders: &ColumnOrders,
        database: Option<&str>,
    ) -> io::Result<Option<KeyedPlan<'_>>> {
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
        let schema = metadata.schema.as_deref().or(database);

        let has_rules = self.config.rules_for(schema, table).is_some();
        if schema.is_none()
            && !has_rules
            && let Some(key) = self.config.rules_named(None, table)
        {
            return Err(refuse(format!(
                "the rows of table `{table}` name no schema, and its rules name one (`{key}`), \
                 so GhostDump cannot tell whether they apply. Name the schema in the dump \
                 (for MySQL, a USE before the rows), or write the rules for `{table}`"
            )));
        }
        let columns: &[String] = match &metadata.columns {
            Some(columns) => columns,
            None if !has_rules => &[],
            None => match orders.columns(&metadata.key) {
                Columns::Known(columns) => {
                    debug!(
                        "table `{table}`: no column list, the columns of its CREATE TABLE apply"
                    );
                    columns
                }
                Columns::Unknown => {
                    return Err(refuse(format!(
                        "table `{table}` has rules, but its INSERT has no column list, and the \
                         dump does not show its columns with certainty: its CREATE TABLE does \
                         not list them all (AS, LIKE, INHERITS, PARTITION OF, OF, invisible \
                         columns), or a later statement may have changed them (ALTER TABLE, \
                         RENAME, DROP). Dump it with column names (mysqldump --complete-insert, \
                         pg_dump --column-inserts) or remove its rules"
                    )));
                }
                Columns::Missing => {
                    return Err(refuse(format!(
                        "table `{table}` has rules, but its INSERT has no column list, and no \
                         CREATE TABLE for it comes earlier in the dump, so the rules cannot be \
                         matched to values. Dump it with its schema, or with column names \
                         (mysqldump --complete-insert, pg_dump --column-inserts), or remove \
                         its rules"
                    )));
                }
            },
        };

        let plan = self.masking_plan(metadata, schema, columns);
        if opaque && plan.is_some() {
            return Err(refuse(format!(
                "table `{table}` has rules, but its COPY uses options GhostDump cannot read \
                 (such as WITH (FORMAT csv)), so its rows cannot be masked"
            )));
        }

        log_masking_plan(metadata, plan.as_ref().map(|(_, plan)| plan.as_slice()));
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
        let context = RowContext {
            dialect: options.dialect,
            secret: &self.secret,
        };
        // Without rules nothing is masked, so threads would only copy bytes.
        if options.threads <= 1 || self.config.tables.is_empty() {
            let sink = SequentialSink::new(writer, rng, context);
            return self.run(input, sink, options);
        }
        thread::scope(|scope| {
            let sink = ParallelSink::start(scope, options.threads, writer, rng, context);
            self.run(input, sink, options)
        })
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
        let mut parser = SqlParser::new(dialect)
            .with_max_row_bytes(max_row_bytes)
            .refusing_client_commands(!self.config.tables.is_empty());

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
        let mut orders = ColumnOrders::default();
        // MySQL: the database of the last `USE`, which table names without
        // one refer to.
        let mut database: Option<String> = None;
        // MySQL user variables that hold a saved setting.
        let mut saved = SavedSettings::default();
        // Rules applied at least once, as (table key, column name).
        let mut used: BTreeSet<(&str, &String)> = BTreeSet::new();

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
                    SqlEvent::Statement(bytes) => {
                        if !self.config.tables.is_empty() {
                            let statement = Statement::parse(&bytes, dialect);
                            if let Err(e) = check_settings(&statement, &mut saved)
                                .and_then(|()| self.check_statement(&statement))
                            {
                                return stop(sink, e);
                            }
                            if let Some(name) = statement.used_database() {
                                database = Some(name);
                                plan_metadata = None;
                            }
                            if let Some(change) = statement.table_change()
                                && orders.apply(change, &self.config, database.as_deref())
                            {
                                // The next header is planned again, even if
                                // it repeats the last one.
                                plan_metadata = None;
                            }
                        }
                        sink.write(&bytes)?;
                    }

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
                            plan = match self.plan_block(
                                metadata.as_deref(),
                                opaque,
                                &raw_bytes,
                                &orders,
                                database.as_deref(),
                            ) {
                                Ok(Some((key, columns))) => {
                                    used.extend(
                                        columns.iter().flatten().map(|rule| (key, &rule.name)),
                                    );
                                    Some(Arc::new(columns))
                                }
                                Ok(None) => None,
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

                    SqlEvent::Footer(footer_bytes) => {
                        if let Err(e) = self.check_footer(
                            plan_metadata.as_deref(),
                            &footer_bytes,
                            dialect,
                            database.as_deref(),
                        ) {
                            return stop(sink, e);
                        }
                        sink.write(&footer_bytes)?;
                    }
                    SqlEvent::Error(e) => return stop(sink, refuse(e.to_string())),
                }
            }
        }

        // With `--limit` the output stops on purpose, in the middle of the input.
        if !reached_limit {
            match parser.finish() {
                Remainder::Bytes(rest) => sink.write(&rest)?,
                Remainder::Statement(rest) => {
                    let statement = Statement::parse(&rest, dialect);
                    if !self.config.tables.is_empty()
                        && let Err(e) = check_settings(&statement, &mut saved)
                            .and_then(|()| self.check_statement(&statement))
                    {
                        return stop(sink, e);
                    }
                    sink.write(&rest)?;
                }
                Remainder::UnterminatedData {
                    partial_row: false,
                    footer,
                } => {
                    if let Err(e) = self.check_footer(
                        plan_metadata.as_deref(),
                        &footer,
                        dialect,
                        database.as_deref(),
                    ) {
                        return stop(sink, e);
                    }
                    sink.write(&footer)?;
                    warn!(
                        "the input ended inside a data block, before its terminator (`;` or `\\.`)"
                    );
                    unterminated_data_block = true;
                }
                Remainder::UnterminatedData {
                    partial_row: true, ..
                } => {
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
            // A run cut short by `--limit` has not seen the whole input.
            unused_rules: if reached_limit {
                Vec::new()
            } else {
                self.unused_rules(&used)
            },
        })
    }
}

impl AnonymizerEngine {
    /// Refuses a statement that may write data GhostDump cannot mask (see
    /// [`SqlEvent::Statement`]) when it refers to a table with rules. The
    /// check is by name, so a column or keyword spelt like such a table is
    /// refused too, which errs on the safe side.
    fn check_statement(&self, statement: &Statement) -> io::Result<()> {
        if !statement.may_write() {
            return Ok(());
        }
        let Some(key) = statement
            .names()
            .iter()
            .find_map(|parts| self.rules_named(parts))
        else {
            return Ok(());
        };
        Err(refuse(format!(
            "a {} statement refers to table `{key}`, which has rules, and may write data in \
             a form GhostDump cannot mask (only the rows of INSERT ... VALUES and \
             COPY ... FROM stdin are masked). Remove the statement from the dump, or the \
             rules of the table",
            statement.keyword()
        )))
    }

    /// The rules a name in a statement may refer to: each part alone, and
    /// each part with the one before it as its schema.
    fn rules_named(&self, parts: &[String]) -> Option<&str> {
        parts.iter().enumerate().find_map(|(i, part)| {
            let schema = i.checked_sub(1).map(|before| parts[before].as_str());
            schema
                .and_then(|schema| self.config.rules_named(Some(schema), part))
                .or_else(|| self.config.rules_named(None, part))
        })
    }

    /// Refuses a clause after the rows that writes values (`ON DUPLICATE KEY
    /// UPDATE name = '...'`) in a data block of a table with rules: the
    /// values may belong to a column with a rule, and cannot be masked.
    fn check_footer(
        &self,
        metadata: Option<&HeaderMetadata>,
        footer: &[u8],
        dialect: SqlDialect,
        database: Option<&str>,
    ) -> io::Result<()> {
        // Most footers are a bare `;`, with no byte that can start a value.
        let may_hold_values = footer
            .iter()
            .any(|&byte| byte.is_ascii_digit() || matches!(byte, b'\'' | b'"' | b'$'));
        let Some(metadata) = metadata.filter(|_| may_hold_values) else {
            return Ok(());
        };
        let table = &metadata.table;
        let schema = metadata.schema.as_deref().or(database);
        let has_rules = self.config.rules_for(schema, table).is_some();
        if has_rules && has_values(footer, dialect) {
            return Err(refuse(format!(
                "the INSERT into table `{table}`, which has rules, writes values after its rows \
                 (in a clause such as ON DUPLICATE KEY UPDATE or ON CONFLICT DO UPDATE), and \
                 GhostDump cannot mask them. Remove the clause, or the rules of the table"
            )));
        }
        Ok(())
    }

    /// The rules that are not in `used`, sorted by table and column.
    fn unused_rules(&self, used: &BTreeSet<(&str, &String)>) -> Vec<UnusedRule> {
        let mut unused = Vec::new();
        for (table, rules) in &self.config.tables {
            let missing: Vec<&String> = rules
                .iter()
                .map(|rule| &rule.name)
                .filter(|name| !used.contains(&(table.as_str(), *name)))
                .collect();
            if missing.len() == rules.len() {
                unused.push(UnusedRule {
                    table: table.clone(),
                    column: None,
                });
            } else {
                unused.extend(missing.into_iter().map(|column| UnusedRule {
                    table: table.clone(),
                    column: Some(column.clone()),
                }));
            }
        }
        unused.sort_by(|a, b| (&a.table, &a.column).cmp(&(&b.table, &b.column)));
        unused
    }
}

fn count_rows(rows_by_table: &mut BTreeMap<String, usize>, table: &str, rows: usize) {
    if rows > 0 {
        *rows_by_table.entry(table.to_string()).or_default() += rows;
    }
}

/// MySQL character sets in which a character can hold a byte below 0x80
/// (`\\` among them in all but euckr), so the server would end a string
/// where GhostDump does not. Found by converting, on MySQL 8.0, every
/// two-byte sequence with a lead byte from 0x80 and a second byte below it;
/// ucs2, utf16, utf16le and utf32 cannot be the character set of a client.
const ASCII_IN_CHARACTERS: &[&str] = &["big5", "cp932", "euckr", "gb18030", "gbk", "sjis"];

/// MySQL user variables that hold a saved setting (`SET @saved_sql_mode =
/// @@sql_mode`), so that restoring it (`SET sql_mode = @saved_sql_mode`)
/// gives back a value GhostDump has read, as mysqldump does around each
/// table, trigger and routine.
#[derive(Default)]
struct SavedSettings(Vec<(String, String)>);

impl SavedSettings {
    /// The variables kept; a setting restored from one no longer kept is
    /// refused. mysqldump uses four.
    const MAX: usize = 16;

    fn assign(&mut self, variable: String, saves: Option<String>) {
        self.0.retain(|(name, _)| *name != variable);
        let Some(setting) = saves
            .filter(|setting| ["sql_mode", "character_set_client"].contains(&setting.as_str()))
        else {
            return;
        };
        if self.0.len() == Self::MAX {
            self.0.remove(0);
        }
        self.0.push((variable, setting));
    }

    fn holds(&self, variable: &str, setting: &str) -> bool {
        self.0
            .iter()
            .any(|(name, saved)| name == variable && saved == setting)
    }
}

/// Refuses, when there are rules, a change after which GhostDump would
/// read the dump differently from the database: text in an encoding other
/// than UTF-8, strings whose backslashes do not work the way the lexer
/// takes them, or a value GhostDump cannot work out.
fn check_settings(statement: &Statement, saved: &mut SavedSettings) -> io::Result<()> {
    for setting in statement.session_settings() {
        match setting {
            SessionSetting::Encoding(name) if !name.as_deref().is_some_and(is_utf8) => {
                return Err(refuse(format!(
                    "the dump sets its encoding to {}; with rules, GhostDump reads only UTF-8 \
                     dumps: names and values in another encoding would not match the rules, and \
                     generated text would be written in UTF-8. Dump again in UTF-8 \
                     (pg_dump -E UTF8, mysqldump --default-character-set=utf8mb4)",
                    name.map_or("its default".to_string(), |name| format!("`{name}`"))
                )));
            }
            SessionSetting::StandardStrings(on) if on != Some(true) => {
                return Err(refuse(
                    "the dump sets standard_conforming_strings to off (or to a value GhostDump \
                     does not read): backslashes would then escape quotes in '...' strings, \
                     which GhostDump reads as standard strings, so a row could be misread. \
                     Dump with standard_conforming_strings on, as pg_dump does"
                        .into(),
                ));
            }
            SessionSetting::SqlMode(SettingValue::Literal(modes))
                if modes
                    .split(',')
                    .any(|mode| mode.trim().eq_ignore_ascii_case("no_backslash_escapes")) =>
            {
                return Err(refuse(
                    "the dump sets NO_BACKSLASH_ESCAPES in sql_mode: backslashes would then be \
                     plain characters in strings, which GhostDump reads as escapes, so a row \
                     could be misread. Dump with mysqldump, which escapes with backslashes and \
                     sets sql_mode itself"
                        .into(),
                ));
            }
            SessionSetting::ClientCharset(SettingValue::Literal(charset))
                if ASCII_IN_CHARACTERS.contains(&charset.to_ascii_lowercase().as_str()) =>
            {
                return Err(refuse(format!(
                    "the dump sets character_set_client to `{charset}`, in which a character \
                     can hold an ASCII byte such as a backslash: the server would end strings \
                     where GhostDump does not, so a row could be misread. Dump again in UTF-8 \
                     (mysqldump --default-character-set=utf8mb4)"
                )));
            }
            SessionSetting::SqlMode(value) => check_restored("sql_mode", value, saved)?,
            SessionSetting::ClientCharset(value) => {
                check_restored("character_set_client", value, saved)?;
            }
            SessionSetting::UserVariable { name, saves } => saved.assign(name, saves),
            SessionSetting::Unreadable(change) => {
                return Err(refuse(format!(
                    "the dump changes its settings with {change}, which GhostDump does not \
                     read: standard_conforming_strings or client_encoding could change how the \
                     rest is read, so a row could be misread. Use SET with a literal value"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Refuses a setting whose value GhostDump does not know: one restored
/// from a variable that does not hold a saved value of that setting, and
/// `DEFAULT`, a number or an expression.
fn check_restored(setting: &str, value: SettingValue, saved: &SavedSettings) -> io::Result<()> {
    let what = match value {
        SettingValue::Literal(_) => return Ok(()),
        SettingValue::Variable(variable) if saved.holds(&variable, setting) => return Ok(()),
        SettingValue::Variable(variable) => format!("the variable `@{variable}`"),
        SettingValue::Unknown => "DEFAULT, a number or an expression".to_string(),
    };
    Err(refuse(format!(
        "the dump sets {setting} from {what}, whose value GhostDump does not know, and \
         {setting} decides how the rest of the dump is read. Set it to a literal value, or \
         restore it from a variable saved with `SET @v = @@{setting}`"
    )))
}

/// Whether an encoding name means UTF-8, or ASCII, which is part of it.
/// PostgreSQL ignores case and every character but letters and digits in
/// encoding names, and has `UNICODE` as an alias of `UTF8`.
fn is_utf8(name: &str) -> bool {
    let name: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    matches!(
        name.as_str(),
        "utf8" | "utf8mb3" | "utf8mb4" | "unicode" | "ascii"
    )
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
    fn test_rows_without_column_list_take_the_columns_of_create_table() {
        let input = b"CREATE TABLE users (id int, name text, secret text);\n\
                      INSERT INTO users VALUES (1, 'Ana', 'pw'), (2, 'Bia', NULL);\n";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap();
        assert!(
            out.ends_with(
                "INSERT INTO users VALUES (1, 'O''Conner', NULL), (2, 'O''Conner', NULL);\n"
            ),
            "{out}"
        );

        // `"Users"` is another table in PostgreSQL: its CREATE TABLE does not apply.
        let input = b"CREATE TABLE \"Users\" (secret text, name text);\n\
                      INSERT INTO users VALUES ('Ana', 'pw');\n";
        let err = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap_err();
        assert!(err.to_string().contains("no CREATE TABLE"), "{err}");
    }

    #[test]
    fn test_a_column_change_after_create_table_is_refused() {
        // The second header repeats the first, but its columns changed.
        let input = b"CREATE TABLE users (id int, name text);\n\
                      INSERT INTO users VALUES (1, 'Ana');\n\
                      ALTER TABLE users ADD COLUMN secret text;\n\
                      INSERT INTO users VALUES (2, 'Bia', 'pw');\n";
        let mut out = Vec::new();
        let err = engine(NAME_RULES)
            .process_dump(&input[..], &mut out, &RunOptions::new(SqlDialect::Postgres))
            .unwrap_err();
        assert!(err.to_string().contains("may have changed"), "{err}");
        let out = String::from_utf8_lossy(&out);
        assert!(
            out.contains("(1, 'O''Conner')") && !out.contains("Bia"),
            "{out}"
        );

        // What pg_dump writes between CREATE TABLE and the rows keeps the columns.
        let input = b"CREATE TABLE public.users (id integer NOT NULL, name text);\n\
                      ALTER TABLE public.users OWNER TO postgres;\n\
                      ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (\n    SEQUENCE NAME public.users_id_seq\n);\n\
                      INSERT INTO public.users OVERRIDING SYSTEM VALUE VALUES (1, 'Ana');\n";
        let out = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap();
        assert!(out.contains("VALUES (1, 'O''Conner');"), "{out}");
    }

    #[test]
    fn test_unreadable_table_name_fails_closed_with_rules() {
        // A name with Unicode escapes, which GhostDump does not decode.
        let input = b"INSERT INTO U&\"d\\0061ta\" (a) VALUES (1);";
        let err = run_with(NAME_RULES, SqlDialect::Postgres, input).unwrap_err();
        assert!(
            err.to_string().contains("could not read the table name"),
            "{err}"
        );
        assert!(run(input).is_ok());
    }

    const STATEMENT_RULES: &str = r#"
        [tables.users]
        columns = [{ name = "name", strategy = "nullify" }]
        [tables."audit.events"]
        columns = [{ name = "payload", strategy = "nullify" }]
    "#;

    #[test]
    fn test_statements_that_write_to_tables_with_rules_are_refused() {
        let pg = SqlDialect::Postgres;
        let mysql = SqlDialect::Mysql;
        let refused = [
            (pg, "UPDATE users SET name = 'Ana' WHERE id = 1;"),
            (pg, "DELETE FROM public.users WHERE name = 'Ana';"),
            (pg, "INSERT INTO users (name) SELECT 'Ana';"),
            (
                pg,
                "WITH x AS (SELECT 'Ana' AS n) INSERT INTO users (name) SELECT n FROM x;",
            ),
            (
                pg,
                "MERGE INTO users u USING (VALUES ('Ana')) v(n) ON false \
                 WHEN NOT MATCHED THEN INSERT (name) VALUES (v.n);",
            ),
            (pg, "CREATE TABLE users AS SELECT 'Ana' AS name;"),
            (pg, "SELECT 'Ana' AS name INTO users;"),
            (pg, "COPY users FROM '/tmp/users.csv';"),
            // The rule for audit.events applies when audit is in the search path.
            (pg, "UPDATE events SET payload = 'Ana';"),
            (pg, "UPDATE users SET name = 'Ana'"),
            (mysql, "INSERT INTO `users` SET `name` = 'Ana';"),
            (mysql, "REPLACE INTO users (name) SELECT 'Ana';"),
            (mysql, "LOAD DATA INFILE 'Ana.txt' INTO TABLE users;"),
            (mysql, "CREATE TABLE users SELECT 'Ana' AS name;"),
            (
                mysql,
                "DELIMITER //\nINSERT INTO users (name) VALUES ('Ana')//\nDELIMITER ;",
            ),
            (SqlDialect::Sqlite, "UPDATE \"Users\" SET name = 'Ana';"),
        ];
        for (dialect, statement) in refused {
            let input = format!("SET x = 1;\n{statement}\n");
            let mut out = Vec::new();
            let err = engine(STATEMENT_RULES)
                .process_dump(input.as_bytes(), &mut out, &RunOptions::new(dialect))
                .unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains("which has rules"),
                "{statement}: {message}"
            );
            assert!(!message.contains("Ana"), "{message}");
            // What came before is written; the statement is not.
            let out = String::from_utf8_lossy(&out);
            assert!(!out.contains("Ana"), "{statement}: {out}");
            assert!(out.starts_with("SET x = 1;"), "{statement}: {out}");
        }
    }

    #[test]
    fn test_statements_without_tables_with_rules_are_copied() {
        let input = b"UPDATE orders SET note = 'x';\n\
                      SELECT pg_catalog.setval('public.users_id_seq', 2, true);\n\
                      COPY users TO STDOUT;\n\
                      CREATE TABLE users (name text) WITH (fillfactor = 70);\n\
                      CREATE INDEX ON users (name);\n\
                      INSERT INTO orders SELECT 1;\n";
        let out = run_with(STATEMENT_RULES, SqlDialect::Postgres, input).unwrap();
        assert_eq!(out.as_bytes(), input);
    }

    #[test]
    fn test_routine_bodies_are_copied_unchanged() {
        let cases: [(SqlDialect, &[u8]); 3] = [
            (
                SqlDialect::Postgres,
                b"CREATE FUNCTION f(n text) RETURNS void LANGUAGE sql\nBEGIN ATOMIC\n \
                  INSERT INTO users (name) VALUES (f.n);\n UPDATE users SET name = 'x';\nEND;\n",
            ),
            (
                SqlDialect::Mysql,
                b"DELIMITER ;;\nCREATE PROCEDURE p(IN n TEXT)\nBEGIN\n  DECLARE c INT;\n  \
                  INSERT INTO users (name) VALUES (n);\nEND ;;\nDELIMITER ;\n",
            ),
            (
                SqlDialect::Sqlite,
                b"CREATE TRIGGER t AFTER INSERT ON a BEGIN DELETE FROM b; \
                  INSERT INTO users (name) VALUES (NEW.x); END;\n",
            ),
        ];
        for (dialect, input) in cases {
            let out = run_with(STATEMENT_RULES, dialect, input).unwrap();
            assert_eq!(out.as_bytes(), input, "{dialect:?}");
        }
    }

    #[test]
    fn test_values_after_the_rows_of_a_table_with_rules_are_refused() {
        let refused = [
            (
                SqlDialect::Mysql,
                "INSERT INTO users (id) VALUES (1) ON DUPLICATE KEY UPDATE name = 'Ana';",
            ),
            (
                SqlDialect::Postgres,
                "INSERT INTO users (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET name = 'Ana';",
            ),
            // Without the final `;`.
            (
                SqlDialect::Postgres,
                "INSERT INTO users (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET name = 'Ana'",
            ),
        ];
        for (dialect, input) in refused {
            let mut out = Vec::new();
            let err = engine(STATEMENT_RULES)
                .process_dump(input.as_bytes(), &mut out, &RunOptions::new(dialect))
                .unwrap_err();
            assert!(err.to_string().contains("after its rows"), "{err}");
            assert!(!String::from_utf8_lossy(&out).contains("Ana"));
        }

        let copied = [
            (
                SqlDialect::Postgres,
                "INSERT INTO users (id, name) VALUES (1, 'Ana') ON CONFLICT DO NOTHING;\n",
            ),
            (
                SqlDialect::Mysql,
                "INSERT INTO users (id, name) VALUES (1, 'Ana') ON DUPLICATE KEY UPDATE name = VALUES(name);\n",
            ),
            (
                SqlDialect::Mysql,
                "INSERT INTO orders (id) VALUES (1) ON DUPLICATE KEY UPDATE note = 'x';\n",
            ),
        ];
        for (dialect, input) in copied {
            let out = run_with(STATEMENT_RULES, dialect, input.as_bytes()).unwrap();
            assert_eq!(out, input.replace("'Ana'", "NULL"));
        }
    }

    #[test]
    fn test_quoted_names_and_overriding_are_masked() {
        let rules = r#"
            [tables."my table"]
            columns = [{ name = "first name", strategy = "nullify" }]
            [tables.users]
            columns = [{ name = "name", strategy = "nullify" }]
        "#;
        let input = b"INSERT INTO public.\"my table\" (\"first name\") VALUES ('Ana');\n\
                      INSERT INTO public.users (id, name) OVERRIDING SYSTEM VALUE VALUES (1, 'Bia');\n";
        let out = run_with(rules, SqlDialect::Postgres, input).unwrap();
        assert_eq!(
            out,
            "INSERT INTO public.\"my table\" (\"first name\") VALUES (NULL);\n\
             INSERT INTO public.users (id, name) OVERRIDING SYSTEM VALUE VALUES (1, NULL);\n"
        );
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
    fn test_rows_at_the_limit_pass_and_longer_ones_stop() {
        let limit = 64;
        let options = RunOptions {
            max_row_bytes: limit,
            ..RunOptions::new(SqlDialect::Postgres)
        };
        let run = |rules: &str, input: &[u8]| {
            let mut out = Vec::new();
            engine(rules)
                .process_dump(input, &mut out, &options)
                .map(|_| out)
        };
        let x = |len: usize| vec![b'x'; len];
        // A COPY row is counted without its line ending, a VALUES row from
        // `(` to `)`, and a statement held for checking without its `;`.
        let copy = |len: usize| {
            [
                b"COPY users (name) FROM stdin;\n".as_slice(),
                &x(len),
                b"\n\\.\n",
            ]
            .concat()
        };
        let values = |len: usize| {
            [
                b"INSERT INTO users (name) VALUES ('".as_slice(),
                &x(len - 4),
                b"');\n",
            ]
            .concat()
        };
        let held = |len: usize| [b"UPDATE t SET a = '".as_slice(), &x(len - 19), b"';"].concat();

        for rules in ["", NAME_RULES] {
            for build in [&copy as &dyn Fn(usize) -> Vec<u8>, &values, &held] {
                let at_limit = build(limit);
                let out = run(rules, &at_limit).unwrap();
                if rules.is_empty() {
                    assert_eq!(out, at_limit);
                }
                let err = run(rules, &build(limit + 1)).unwrap_err();
                assert!(err.to_string().contains("longer than 64 bytes"), "{err}");
            }
        }
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

    #[test]
    fn test_names_match_without_case_and_with_schema() {
        let rules = r#"
            [tables.Users]
            columns = [{ name = "Name", strategy = "nullify" }]

            [tables."audit.users"]
            columns = [{ name = "id", strategy = "nullify" }]
        "#;
        let run = |input: &[u8]| run_with(rules, SqlDialect::Postgres, input).unwrap();
        assert_eq!(
            run(b"INSERT INTO USERS (ID, NAME) VALUES (1, 'Ana');"),
            "INSERT INTO USERS (ID, NAME) VALUES (1, NULL);"
        );
        assert_eq!(
            run(b"INSERT INTO public.\"users\" (id, \"name\") VALUES (1, 'Ana');"),
            "INSERT INTO public.\"users\" (id, \"name\") VALUES (1, NULL);"
        );
        // The rule that names the schema wins over the one that does not.
        assert_eq!(
            run(b"INSERT INTO Audit.Users (id, name) VALUES (1, 'Ana');"),
            "INSERT INTO Audit.Users (id, name) VALUES (NULL, 'Ana');"
        );
    }

    #[test]
    fn test_rules_that_match_nothing_are_reported() {
        let rules = r#"
            [tables.users]
            columns = [
                { name = "name", strategy = "nullify" },
                { name = "emial", strategy = "nullify" },
            ]

            [tables.orders]
            columns = [{ name = "card", strategy = "nullify" }]
        "#;
        let report = |limit| {
            let options = RunOptions {
                limit,
                ..RunOptions::new(SqlDialect::Postgres)
            };
            let input = b"INSERT INTO users (name, email) VALUES ('Ana', 'a@x.com');\n";
            engine(rules)
                .process_dump(&input[..], io::sink(), &options)
                .unwrap()
        };
        let unused: Vec<String> = report(None)
            .unused_rules
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            unused,
            [
                "no rule of table `orders` matched the input",
                "column `users.emial` was not found"
            ]
        );
        // A run cut short has not seen the whole input.
        assert!(report(Some(1)).unused_rules.is_empty());
    }

    const THREAD_RULES: &str = r#"
        [tables.users]
        columns = [
            { name = "name", strategy = "faker_name" },
            { name = "email", strategy = "hmac" },
            { name = "age", strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 120.0], scale = 0 },
            { name = "status", strategy = "randomized_response", epsilon = 1.0, categories = ["A", "B"] },
        ]
    "#;

    /// Several segments of each kind: COPY, multi-row INSERT, a table
    /// without rules and statements between them.
    fn many_rows() -> Vec<u8> {
        let mut dump = b"SET client_encoding = 'UTF8';\n".to_vec();
        dump.extend_from_slice(b"COPY users (id, name, email, age, status) FROM stdin;\n");
        for i in 0..3000 {
            let line = format!("{i}\tName {i}\tu{i}@x.com\t{}\tA\n", i % 90);
            dump.extend_from_slice(line.as_bytes());
        }
        dump.extend_from_slice(b"\\.\n\nINSERT INTO other (a) VALUES (1), (2);\n");
        dump.extend_from_slice(b"INSERT INTO users (id, name, email, age, status) VALUES\n");
        for i in 0..2500 {
            let end = if i == 2499 { ";\n" } else { ",\n" };
            let row = format!("({i}, 'N{i}', 'e{i}@x.com', {}, 'B'){end}", i % 70);
            dump.extend_from_slice(row.as_bytes());
        }
        dump.extend_from_slice(b"COMMIT;\n");
        dump
    }

    fn run_threads(
        rules: &str,
        input: &[u8],
        threads: usize,
        limit: Option<usize>,
    ) -> (io::Result<RunReport>, Vec<u8>) {
        let options = RunOptions {
            seed: Some(11),
            threads,
            limit,
            ..RunOptions::new(SqlDialect::Postgres)
        };
        let mut out = Vec::new();
        let report = engine(rules).process_dump(input, &mut out, &options);
        (report, out)
    }

    #[test]
    fn test_output_is_the_same_for_any_number_of_threads() {
        let input = many_rows();
        let (report, expected) = run_threads(THREAD_RULES, &input, 1, None);
        let report = report.unwrap();
        assert_eq!(report.rows_masked, 5500);
        assert_eq!(report.fields_masked, 4 * 5500);
        assert!(!String::from_utf8_lossy(&expected).contains("Name 7\t"));
        for threads in [2, 3, 8] {
            let (parallel, out) = run_threads(THREAD_RULES, &input, threads, None);
            assert_eq!(parallel.unwrap(), report, "{threads} threads");
            assert!(out == expected, "{threads} threads");
        }
        for limit in [1, 1500, 4000] {
            let (_, one) = run_threads(THREAD_RULES, &input, 1, Some(limit));
            let (_, four) = run_threads(THREAD_RULES, &input, 4, Some(limit));
            assert!(one == four, "limit {limit}");
        }
    }

    #[test]
    fn test_settings_are_checked_in_a_statement_that_ends_the_input() {
        let pg = SqlDialect::Postgres;
        let unended = b"SET client_encoding = 'LATIN1'";
        assert!(run_with(NAME_RULES, pg, unended).is_err());
        assert_eq!(run(unended).unwrap(), unended);
    }

    #[test]
    fn test_settings_that_change_how_the_dump_is_read_are_refused() {
        // Checks the statements in order, as one dump.
        let check = |statements: &[&str], dialect| {
            let mut saved = SavedSettings::default();
            statements.iter().try_for_each(|sql| {
                check_settings(&Statement::parse(sql.as_bytes(), dialect), &mut saved)
            })
        };
        let pg = SqlDialect::Postgres;
        for read in [
            &["SET client_encoding = 'UTF8';"][..],
            &["SET client_encoding = 'utf_8';"],
            &["SET NAMES 'Unicode';"],
            &["SET standard_conforming_strings = on;"],
            &["SET search_path = '';"],
            &["SELECT pg_catalog.set_config('search_path', '', false);"],
            &["SELECT set_config('standard_conforming_strings', 'on', false);"],
        ] {
            assert!(check(read, pg).is_ok(), "{read:?}");
        }
        for refused in [
            &["SET client_encoding = 'LATIN1';"][..],
            &["SET client_encoding = 'SQL_ASCII';"],
            &["SET client_encoding TO DEFAULT;"],
            &["SET standard_conforming_strings = off;"],
            &["SELECT set_config('standard_conforming_strings', 'off', false);"],
            &["SELECT set_config('client_encoding', 'SJIS', false);"],
            &["SELECT set_config(current_setting('x'), 'off', false);"],
            &["UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';"],
        ] {
            assert!(check(refused, pg).is_err(), "{refused:?}");
        }

        let my = SqlDialect::Mysql;
        for read in [
            &["SET NAMES utf8mb4 */;"][..],
            &["SET NAMES utf8 */;"],
            // mysqldump, at the start and the end of a dump.
            &[
                "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;",
                "SET SQL_MODE=@OLD_SQL_MODE */;",
            ],
            // mysqldump, around a trigger created from a latin1 client.
            &[
                "SET @saved_cs_client      = @@character_set_client */ ;",
                "SET character_set_client  = latin1 */ ;",
                "SET @saved_sql_mode       = @@sql_mode */ ;",
                "SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES' */ ;",
                "SET sql_mode              = @saved_sql_mode */ ;",
                "SET character_set_client  = @saved_cs_client */ ;",
            ],
            &["SET @S = @@session.sql_mode;", "SET sql_mode = @s;"],
            &["SET character_set_client = ujis;"],
        ] {
            assert!(check(read, my).is_ok(), "{read:?}");
        }
        for refused in [
            &["SET NAMES latin1 */;"][..],
            &["SET NAMES sjis;"],
            &["SET CHARACTER SET binary;"],
            &["SET sql_mode = 'ANSI,no_backslash_escapes';"],
            &["SET sql_mode = NO_BACKSLASH_ESCAPES;"],
            &["SET sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES');"],
            &["SET sql_mode = DEFAULT;"],
            &["SET sql_mode = @m;"],
            &["SET @m = 'NO_BACKSLASH_ESCAPES';", "SET sql_mode = @m;"],
            // Saved, then given another value.
            &[
                "SET @m = @@sql_mode;",
                "SET @m := 'x';",
                "SET sql_mode = @m;",
            ],
            // Saved from the global value, or from another setting.
            &["SET @m = @@GLOBAL.sql_mode;", "SET sql_mode = @m;"],
            &["SET @m = @@character_set_client;", "SET sql_mode = @m;"],
            &["SET character_set_client = sjis;"],
            &["SET @@session.character_set_client = 'GBK';"],
            &["SET character_set_client = 13;"],
            &["SET character_set_client = DEFAULT;"],
            &["SET @c = 'sjis';", "SET character_set_client = @c;"],
        ] {
            assert!(check(refused, my).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn test_only_the_latest_saved_settings_are_kept() {
        let mut saved = SavedSettings::default();
        for variable in 0..=SavedSettings::MAX {
            saved.assign(format!("v{variable}"), Some("sql_mode".into()));
        }
        assert!(!saved.holds("v0", "sql_mode"));
        assert!(saved.holds("v1", "sql_mode"));
        assert!(saved.holds(&format!("v{}", SavedSettings::MAX), "sql_mode"));
        assert!(!saved.holds("v1", "character_set_client"));
        saved.assign("v1".into(), Some("time_zone".into()));
        assert!(!saved.holds("v1", "sql_mode"));
    }

    #[test]
    fn test_threads_without_rules_copy_the_input() {
        let input = many_rows();
        let (_, out) = run_threads("", &input, 4, None);
        assert!(out == input);
    }

    #[test]
    fn test_threads_stop_on_errors_after_writing_what_came_before() {
        // The second data block has rules but no column list.
        let mut input = many_rows();
        input.extend_from_slice(b"INSERT INTO users VALUES (1, 'Ana');\n");
        let (one, one_out) = run_threads(THREAD_RULES, &input, 1, None);
        let (four, four_out) = run_threads(THREAD_RULES, &input, 4, None);
        let (one, four) = (one.unwrap_err(), four.unwrap_err());
        assert!(four.to_string().contains("no column list"), "{four}");
        assert_eq!(one.to_string(), four.to_string());
        assert!(one_out == four_out);
        // Everything up to the refused block was written.
        assert!(String::from_utf8_lossy(&four_out).contains("COMMIT"));
    }
}
