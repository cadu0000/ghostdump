pub mod lexer;
pub mod schema;
pub mod state;
pub mod tokenizer;

pub use schema::extract_metadata;
pub use state::{InsertFormat, Remainder, SqlDialect};
pub use tokenizer::{join_tuple, split_tuple};

use state::{
    InsertHeaderEvent, InsertHeaderState, NormalEvent, NormalState, State, ValueEvent, ValueState,
};

pub enum SqlEvent {
    DefaultStatement(u8),
    /// Bytes outside data rows, to be written unchanged.
    Passthrough(Vec<u8>),
    Header {
        raw_bytes: Vec<u8>,
        table_name: Option<String>,
        columns: Option<Vec<String>>,
    },
    Tuple(Vec<u8>, InsertFormat),
    Footer(Vec<u8>),
}

pub struct SqlParser {
    state: State,
    dialect: SqlDialect,
}

impl SqlParser {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            state: State::Normal(NormalState::at_input_start(dialect)),
            dialect,
        }
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
                    ));
                    None
                }
                NormalEvent::Continue => None,
                NormalEvent::DefaultStatement(byte) => Some(SqlEvent::DefaultStatement(byte)),
            },
            State::InsertHeader(header_state) => match header_state.process_byte(byte) {
                InsertHeaderEvent::HeaderComplete {
                    format,
                    header_bytes,
                    carry,
                } => {
                    let mut v_state = ValueState::new(self.dialect, format);
                    if let Some(carry) = carry {
                        // The byte after VALUES never completes a row.
                        v_state.process_byte(carry);
                    }
                    self.state = State::ValueMode(v_state);

                    let (table_name, columns) = match extract_metadata(&header_bytes) {
                        Some((tbl, cols)) => (Some(tbl), cols),
                        None => (None, None),
                    };

                    Some(SqlEvent::Header {
                        raw_bytes: header_bytes,
                        table_name,
                        columns,
                    })
                }
                InsertHeaderEvent::NotData(bytes) => {
                    self.state = State::Normal(NormalState::new(self.dialect));
                    Some(SqlEvent::Passthrough(bytes))
                }
                InsertHeaderEvent::Continue => None,
            },
            State::ValueMode(v_state) => match v_state.process_byte(byte) {
                ValueEvent::TupleComplete(data) => Some(SqlEvent::Tuple(data, v_state.format)),
                ValueEvent::ExitValuesMode(footer_bytes) => {
                    self.state = State::Normal(NormalState::new(self.dialect));
                    Some(SqlEvent::Footer(footer_bytes))
                }
                ValueEvent::Continue => None,
            },
        }
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

    #[test]
    fn test_parser_with_traditional_insert() {
        let mut parser = SqlParser::new(SqlDialect::Mysql);
        let sql = b"INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob, the Builder');";
        let mut extracted_events = Vec::new();

        for &byte in sql {
            if let Some(event) = parser.handle_byte(byte) {
                let data = match event {
                    SqlEvent::Header { raw_bytes, .. } => raw_bytes,
                    SqlEvent::Tuple(bytes, _) => bytes,
                    SqlEvent::Footer(bytes) => bytes,
                    SqlEvent::DefaultStatement(_) | SqlEvent::Passthrough(_) => continue,
                };

                let event_string = String::from_utf8_lossy(&data).into_owned();
                extracted_events.push(event_string);
            }
        }

        assert_eq!(extracted_events.len(), 4);

        assert_eq!(extracted_events[0], "INSERT INTO users (id, name) VALUES");
        assert_eq!(extracted_events[1], "(1, 'Alice')");
        assert_eq!(extracted_events[2], "(2, 'Bob, the Builder')");
        assert_eq!(extracted_events[3], ";");
    }

    #[test]
    fn test_parser_with_postgres_copy() {
        let mut parser = SqlParser::new(SqlDialect::Postgres);
        let sql = b"COPY public.users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n";
        let mut extracted_events = Vec::new();

        for &byte in sql {
            if let Some(event) = parser.handle_byte(byte) {
                let data = match event {
                    SqlEvent::Header { raw_bytes, .. } => raw_bytes,
                    SqlEvent::Tuple(bytes, _) => bytes,
                    SqlEvent::Footer(bytes) => bytes,
                    SqlEvent::DefaultStatement(_) | SqlEvent::Passthrough(_) => continue,
                };

                let event_string = String::from_utf8_lossy(&data).into_owned();
                extracted_events.push(event_string);
            }
        }

        assert_eq!(extracted_events.len(), 4);

        assert_eq!(
            extracted_events[0],
            "COPY public.users (id, name) FROM stdin;"
        );
        assert_eq!(extracted_events[1], "1\tAlice\n");
        assert_eq!(extracted_events[2], "2\tBob\n");
        assert_eq!(extracted_events[3], "\\.\n");
    }

    #[test]
    fn test_tokenizer_split_and_join_copy() {
        let raw_tuple = b"100\t1234-5678-9012\tAlice\n";
        let format = InsertFormat::Copy;

        let columns = split_tuple(raw_tuple, format);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0], b"100");
        assert_eq!(columns[1], b"1234-5678-9012");
        assert_eq!(columns[2], b"Alice");

        let joined = join_tuple(&columns, format);
        assert_eq!(joined, raw_tuple);
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
