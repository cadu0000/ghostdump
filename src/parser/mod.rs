pub mod lexer;
pub mod schema;
pub mod state;

use std::fmt;
use std::sync::Arc;

pub use schema::{HeaderMetadata, extract_metadata};
pub use state::{InsertFormat, Remainder, SqlDialect};

use state::{
    InsertHeaderEvent, InsertHeaderState, NormalEvent, NormalState, State, ValueEvent, ValueState,
};

/// Largest row or data block header accepted by default: 256 MiB.
pub const DEFAULT_MAX_ROW_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// A row or a data block header grew past the size limit.
    TooLarge { limit: usize },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::TooLarge { limit } => write!(
                f,
                "a data row or statement header is longer than {limit} bytes \
                 (raise --max-row-size if the dump really has rows this large)"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// What the parser reports for each input byte. Writing every byte of
/// `DefaultStatement`, `Passthrough`, `Header`, `Tuple` and `Footer`, in
/// order, reproduces the input exactly.
#[derive(Debug, PartialEq)]
pub enum SqlEvent {
    /// A byte outside data rows, to be written unchanged.
    DefaultStatement(u8),
    /// Bytes outside data rows, to be written unchanged.
    Passthrough(Vec<u8>),
    Header {
        raw_bytes: Vec<u8>,
        /// Shared between identical headers (`pg_dump --inserts` repeats
        /// the same header on every row).
        metadata: Option<Arc<HeaderMetadata>>,
        /// A COPY with options the row codec cannot read.
        opaque: bool,
    },
    Tuple(Vec<u8>, InsertFormat),
    Footer(Vec<u8>),
    /// The input cannot be processed safely; the run must stop.
    Error(ParseError),
}

pub struct SqlParser {
    state: State,
    dialect: SqlDialect,
    /// The last header seen and its metadata, so repeated headers are not
    /// parsed again.
    last_header: Vec<u8>,
    last_metadata: Option<Arc<HeaderMetadata>>,
    max_row_bytes: usize,
}

impl SqlParser {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            state: State::Normal(NormalState::at_input_start(dialect)),
            dialect,
            last_header: Vec::new(),
            last_metadata: None,
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
        }
    }

    /// Rows and data block headers longer than `limit` bytes are reported
    /// as errors instead of growing the buffer without bound.
    pub fn with_max_row_bytes(mut self, limit: usize) -> Self {
        self.max_row_bytes = limit;
        self
    }

    pub fn handle_byte(&mut self, byte: u8) -> Option<SqlEvent> {
        match &mut self.state {
            State::Normal(normal_state) => match normal_state.process_byte(byte) {
                NormalEvent::StartInsertHeader {
                    initial_bytes,
                    keyword,
                    delimiter,
                } => {
                    self.state = State::InsertHeader(InsertHeaderState::new(
                        self.dialect,
                        keyword,
                        initial_bytes,
                        delimiter,
                        self.max_row_bytes,
                    ));
                    None
                }
                NormalEvent::Continue => None,
                NormalEvent::DefaultStatement(byte) => Some(SqlEvent::DefaultStatement(byte)),
            },
            State::InsertHeader(header_state) => match header_state.process_byte(byte) {
                InsertHeaderEvent::HeaderComplete {
                    format,
                    mut header_bytes,
                    carry,
                    opaque,
                } => {
                    let mut v_state = ValueState::new(self.dialect, format, self.max_row_bytes);
                    if let Some(carry) = carry
                        && !v_state.take_carry(carry)
                    {
                        header_bytes.push(carry);
                    }
                    self.state = State::ValueMode(v_state);

                    let metadata = self.metadata_for(&header_bytes);
                    Some(SqlEvent::Header {
                        raw_bytes: header_bytes,
                        metadata,
                        opaque,
                    })
                }
                InsertHeaderEvent::NotData(bytes) => {
                    self.state = State::Normal(NormalState::new(self.dialect));
                    Some(SqlEvent::Passthrough(bytes))
                }
                InsertHeaderEvent::TooLarge => Some(self.too_large()),
                InsertHeaderEvent::Continue => None,
            },
            State::ValueMode(v_state) => match v_state.process_byte(byte) {
                ValueEvent::TupleComplete(data) => Some(SqlEvent::Tuple(data, v_state.format)),
                ValueEvent::ExitValuesMode(footer_bytes) => {
                    self.state = State::Normal(NormalState::new(self.dialect));
                    Some(SqlEvent::Footer(footer_bytes))
                }
                ValueEvent::Passthrough(byte) => Some(SqlEvent::DefaultStatement(byte)),
                ValueEvent::RowTooLarge => Some(self.too_large()),
                ValueEvent::Continue => None,
            },
        }
    }

    fn too_large(&self) -> SqlEvent {
        SqlEvent::Error(ParseError::TooLarge {
            limit: self.max_row_bytes,
        })
    }

    fn metadata_for(&mut self, header: &[u8]) -> Option<Arc<HeaderMetadata>> {
        if header != self.last_header.as_slice() {
            self.last_metadata = extract_metadata(header, self.dialect).map(Arc::new);
            self.last_header.clear();
            self.last_header.extend_from_slice(header);
        }
        self.last_metadata.clone()
    }

    /// Ends the input and returns what the parser still holds.
    pub fn finish(&mut self) -> Remainder {
        match &mut self.state {
            State::Normal(normal_state) => Remainder::Bytes(normal_state.take_pending()),
            State::InsertHeader(header_state) => {
                Remainder::Bytes(std::mem::take(&mut header_state.keyword_buf))
            }
            State::ValueMode(v_state) => v_state.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feeds `sql` and returns every byte written outside data blocks, and
    /// whether a data block was opened.
    fn passthrough(dialect: SqlDialect, sql: &[u8]) -> (Vec<u8>, bool) {
        let mut parser = SqlParser::new(dialect);
        let mut out = Vec::new();
        let mut opened = false;
        for &byte in sql {
            match parser.handle_byte(byte) {
                Some(SqlEvent::DefaultStatement(b)) => out.push(b),
                Some(SqlEvent::Passthrough(bytes)) => out.extend_from_slice(&bytes),
                Some(SqlEvent::Header { .. }) => opened = true,
                _ => {}
            }
        }
        if let Remainder::Bytes(rest) = parser.finish() {
            out.extend_from_slice(&rest);
        }
        (out, opened)
    }

    fn assert_passes_through(dialect: SqlDialect, sql: &[u8]) {
        let (out, opened) = passthrough(dialect, sql);
        assert!(!opened, "a data block was opened in {:?}", String::from_utf8_lossy(sql));
        assert_eq!(String::from_utf8_lossy(&out), String::from_utf8_lossy(sql));
    }

    fn opens_data_block(dialect: SqlDialect, sql: &[u8]) -> bool {
        passthrough(dialect, sql).1
    }

    fn remainder(dialect: SqlDialect, sql: &[u8]) -> Remainder {
        let mut parser = SqlParser::new(dialect);
        for &byte in sql {
            parser.handle_byte(byte);
        }
        parser.finish()
    }

    /// Everything the parser reports for `sql`, with single bytes merged
    /// into runs, plus the remainder at the end of the input.
    #[derive(Debug, PartialEq)]
    enum Seen {
        Bytes(String),
        Header(String),
        Tuple(String),
        Footer(String),
    }

    fn parse(dialect: SqlDialect, sql: &[u8]) -> (Vec<Seen>, Remainder) {
        let mut parser = SqlParser::new(dialect);
        let mut seen: Vec<Seen> = Vec::new();
        let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
        let push_bytes = |seen: &mut Vec<Seen>, bytes: &[u8]| match seen.last_mut() {
            Some(Seen::Bytes(run)) => run.push_str(&text(bytes)),
            _ => seen.push(Seen::Bytes(text(bytes))),
        };
        for &byte in sql {
            match parser.handle_byte(byte) {
                Some(SqlEvent::DefaultStatement(b)) => push_bytes(&mut seen, &[b]),
                Some(SqlEvent::Passthrough(bytes)) => push_bytes(&mut seen, &bytes),
                Some(SqlEvent::Header { raw_bytes, .. }) => seen.push(Seen::Header(text(&raw_bytes))),
                Some(SqlEvent::Tuple(row, _)) => seen.push(Seen::Tuple(text(&row))),
                Some(SqlEvent::Footer(bytes)) => seen.push(Seen::Footer(text(&bytes))),
                Some(SqlEvent::Error(e)) => panic!("unexpected error: {e}"),
                None => {}
            }
        }
        (seen, parser.finish())
    }

    fn bytes(s: &str) -> Seen {
        Seen::Bytes(s.into())
    }
    fn header(s: &str) -> Seen {
        Seen::Header(s.into())
    }
    fn tuple(s: &str) -> Seen {
        Seen::Tuple(s.into())
    }
    fn footer(s: &str) -> Seen {
        Seen::Footer(s.into())
    }

    #[test]
    fn test_parser_with_traditional_insert() {
        let (seen, _) = parse(
            SqlDialect::Mysql,
            b"INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob, the Builder');",
        );
        assert_eq!(
            seen,
            [
                header("INSERT INTO users (id, name) VALUES "),
                tuple("(1, 'Alice')"),
                bytes(", "),
                tuple("(2, 'Bob, the Builder')"),
                footer(";"),
            ]
        );
    }

    #[test]
    fn test_parser_with_postgres_copy() {
        let (seen, _) = parse(
            SqlDialect::Postgres,
            b"COPY public.users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n",
        );
        assert_eq!(
            seen,
            [
                header("COPY public.users (id, name) FROM stdin;"),
                bytes("\n"),
                tuple("1\tAlice\n"),
                tuple("2\tBob\n"),
                footer("\\.\n"),
            ]
        );
    }

    #[test]
    fn test_every_byte_is_delivered_in_order() {
        let sql = b"SET x = 1;\nINSERT INTO t (a, b) VALUES\n  (1, 'x') , -- first\n  (2, ')')\nON CONFLICT (a) DO NOTHING;\nCOPY t (a) FROM stdin;\n\n1\n\\.\nSELECT 1;\n";
        let (seen, rest) = parse(SqlDialect::Postgres, sql);
        let mut out = String::new();
        for item in &seen {
            match item {
                Seen::Bytes(s) | Seen::Header(s) | Seen::Tuple(s) | Seen::Footer(s) => out.push_str(s),
            }
        }
        if let Remainder::Bytes(rest) = rest {
            out.push_str(&String::from_utf8_lossy(&rest));
        }
        assert_eq!(out, String::from_utf8_lossy(sql));
        assert!(seen.contains(&tuple("(2, ')')")));
        assert!(!seen.contains(&tuple("(a)")));
    }

    #[test]
    fn test_values_rows_are_framed_with_dialect_rules() {
        // PostgreSQL: 'C:\' ends at the second quote, so the row ends at `)`.
        let (seen, _) = parse(
            SqlDialect::Postgres,
            b"INSERT INTO t (a) VALUES ('C:\\'), ('x');",
        );
        assert_eq!(seen[1], tuple("('C:\\')"));
        assert_eq!(seen[3], tuple("('x')"));

        // MySQL: the backslash escapes the quote.
        let (seen, _) = parse(SqlDialect::Mysql, b"INSERT INTO t (a) VALUES ('it\\'s)'), ('x');");
        assert_eq!(seen[1], tuple("('it\\'s)')"));
    }

    #[test]
    fn test_replace_and_value_are_mysql_data_statements() {
        let (seen, _) = parse(SqlDialect::Mysql, b"REPLACE INTO t (a) VALUE (1);");
        assert_eq!(
            seen,
            [
                header("REPLACE INTO t (a) VALUE "),
                tuple("(1)"),
                footer(";")
            ]
        );

        // PostgreSQL has neither REPLACE nor VALUE.
        assert_passes_through(SqlDialect::Postgres, b"REPLACE INTO t (a) VALUES (1);");
        assert_passes_through(SqlDialect::Postgres, b"INSERT INTO t (a) VALUE (1);");
    }

    #[test]
    fn test_copy_with_options_is_opaque() {
        let opaque = |sql: &[u8]| -> Vec<bool> {
            let mut parser = SqlParser::new(SqlDialect::Postgres);
            sql.iter()
                .filter_map(|&byte| match parser.handle_byte(byte) {
                    Some(SqlEvent::Header { opaque, .. }) => Some(opaque),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            opaque(b"COPY t (a) FROM stdin WITH (FORMAT csv);\n\"x\"\n\\.\n"),
            [true]
        );
        assert_eq!(opaque(b"COPY t (a) FROM stdin ;\n1\n\\.\n"), [false]);
    }

    #[test]
    fn test_rows_longer_than_the_limit_are_reported() {
        let errors = |sql: &[u8]| -> Vec<SqlEvent> {
            let mut parser = SqlParser::new(SqlDialect::Postgres).with_max_row_bytes(16);
            sql.iter()
                .filter_map(|&byte| parser.handle_byte(byte))
                .filter(|event| matches!(event, SqlEvent::Error(_)))
                .collect()
        };
        let too_large = SqlEvent::Error(ParseError::TooLarge { limit: 16 });
        assert_eq!(
            errors(b"COPY t FROM stdin;\n0123456789abcdefghij\n").first(),
            Some(&too_large)
        );
        assert_eq!(
            errors(b"INSERT INTO t VALUES ('0123456789abcdef');").first(),
            Some(&too_large)
        );
        // The header counts too.
        assert_eq!(
            errors(b"INSERT INTO a_very_long_table_name (a) VALUES (1);").first(),
            Some(&too_large)
        );
    }

    #[test]
    fn test_repeated_headers_share_metadata() {
        let mut parser = SqlParser::new(SqlDialect::Postgres);
        // The header of a statement carries the line break before it, so
        // the second and third headers are identical.
        let sql = b"INSERT INTO t (a) VALUES (1);\nINSERT INTO t (a) VALUES (2);\nINSERT INTO t (a) VALUES (3);\n";
        let metadata: Vec<Arc<HeaderMetadata>> = sql
            .iter()
            .filter_map(|&byte| match parser.handle_byte(byte) {
                Some(SqlEvent::Header { metadata, .. }) => metadata,
                _ => None,
            })
            .collect();
        assert_eq!(metadata.len(), 3);
        assert!(Arc::ptr_eq(&metadata[1], &metadata[2]));
        assert_eq!(metadata[0].table, "t");
    }

    #[test]
    fn test_keywords_inside_literals_are_ignored() {
        assert_passes_through(
            SqlDialect::Postgres,
            b"SELECT 'INSERT INTO fake';\nSELECT 'a--b';\n",
        );
    }

    #[test]
    fn test_comments_pass_through_unchanged() {
        let sql = b"-- INSERT INTO fake VALUES (1);\n/* COPY fake FROM stdin; ' */\nSELECT 1;\n";
        assert_passes_through(SqlDialect::Postgres, sql);
        assert_passes_through(SqlDialect::Mysql, b"# INSERT INTO t VALUES (1);\nSELECT 1;\n");
    }

    #[test]
    fn test_keywords_inside_identifiers_are_ignored() {
        assert_passes_through(
            SqlDialect::Postgres,
            b"CREATE TABLE t (inserted_at date, copy_count int);\nALTER TABLE t ADD insert_id int;\n",
        );
    }

    #[test]
    fn test_function_bodies_pass_through() {
        let sql = b"CREATE FUNCTION f() RETURNS void AS $$\nBEGIN\n  INSERT INTO log VALUES (1);\nEND;\n$$ LANGUAGE plpgsql;\n";
        assert_passes_through(SqlDialect::Postgres, sql);
    }

    #[test]
    fn test_backslash_strings_follow_the_dialect() {
        // PostgreSQL: 'C:\' is complete and E'it\'s' honors the backslash.
        assert_passes_through(
            SqlDialect::Postgres,
            b"SELECT 'C:\\';\nSELECT E'it\\'s INSERT';\n",
        );
        assert!(opens_data_block(
            SqlDialect::Postgres,
            b"SELECT 'C:\\';\nINSERT INTO t (a) VALUES (1);\n"
        ));
    }

    #[test]
    fn test_psql_meta_commands_pass_through() {
        assert_passes_through(
            SqlDialect::Postgres,
            b"\\restrict abc'def\n\\connect db\nSELECT 1;\n",
        );
        assert!(opens_data_block(
            SqlDialect::Postgres,
            b"\\connect it's\nINSERT INTO t (a) VALUES (1);\n"
        ));
    }

    #[test]
    fn test_statements_that_are_not_data_pass_through() {
        assert_passes_through(SqlDialect::Postgres, b"INSERT INTO t SELECT * FROM u;\n");
        assert_passes_through(SqlDialect::Postgres, b"COPY t TO stdout;\n");
        assert_passes_through(SqlDialect::Postgres, b"INSERT INTO t DEFAULT VALUES;\n");
    }

    #[test]
    fn test_values_needs_a_word_boundary() {
        assert_passes_through(
            SqlDialect::Postgres,
            b"INSERT INTO t (values_count) SELECT 1;\n",
        );
    }

    #[test]
    fn test_insert_after_other_statements_is_detected() {
        assert!(opens_data_block(
            SqlDialect::Postgres,
            b"SET x = 1;\n-- comment\n  insert into t (a) values (1);\n"
        ));
    }

    #[test]
    fn test_insert_after_utf8_bom_is_detected() {
        let sql = b"\xEF\xBB\xBFINSERT INTO t (a) VALUES (1);\n";
        assert!(opens_data_block(SqlDialect::Postgres, sql));
        assert_passes_through(SqlDialect::Postgres, b"\xEF\xBB\xBFSELECT 1;\n");
    }

    #[test]
    fn test_copy_end_marker_is_a_whole_line() {
        let mut parser = SqlParser::new(SqlDialect::Postgres);
        let sql = b"COPY t (a) FROM stdin;\nC:\\\\.\n\\.\n";
        let tuples: Vec<Vec<u8>> = sql
            .iter()
            .filter_map(|&byte| match parser.handle_byte(byte) {
                Some(SqlEvent::Tuple(row, _)) => Some(row),
                _ => None,
            })
            .collect();
        assert_eq!(tuples, [b"C:\\\\.\n".to_vec()]);
    }

    #[test]
    fn test_empty_copy_lines_are_rows() {
        let mut parser = SqlParser::new(SqlDialect::Postgres);
        let sql = b"COPY t (a) FROM stdin;\n\nx\n\\.\n";
        let rows = sql
            .iter()
            .filter(|&&byte| matches!(parser.handle_byte(byte), Some(SqlEvent::Tuple(..))))
            .count();
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_buffered_bytes_are_returned_at_the_end() {
        assert_passes_through(SqlDialect::Postgres, b"SELECT 1");
        assert_passes_through(SqlDialect::Postgres, b"INSERT INTO t SELECT 1");
    }

    #[test]
    fn test_truncated_rows_are_reported() {
        assert_eq!(
            remainder(SqlDialect::Postgres, b"INSERT INTO t (a) VALUES (1), (2"),
            Remainder::UnterminatedData { partial_row: true }
        );
        assert_eq!(
            remainder(SqlDialect::Postgres, b"COPY t (a) FROM stdin;\n1\n2"),
            Remainder::UnterminatedData { partial_row: true }
        );
        assert_eq!(
            remainder(SqlDialect::Postgres, b"COPY t (a) FROM stdin;\n1\n"),
            Remainder::UnterminatedData { partial_row: false }
        );
    }

    #[test]
    fn test_copy_marker_without_final_newline() {
        let parsed = remainder(SqlDialect::Postgres, b"COPY t (a) FROM stdin;\n1\n\\.");
        assert_eq!(parsed, Remainder::Bytes(b"\\.".to_vec()));
    }
}
