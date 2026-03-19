pub mod schema;
pub mod state;
pub mod tokenizer;

pub use schema::extract_metadata;
pub use state::{InsertFormat, SqlDialect};
pub use tokenizer::{join_tuple, split_tuple};

use state::{
    InsertHeaderEvent, InsertHeaderState, NormalEvent, NormalState, State, ValueEvent, ValueState,
};

pub enum SqlEvent {
    DefaultStatement(u8),
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
            state: State::Normal(NormalState::new()),
            dialect,
        }
    }

    pub fn handle_byte(&mut self, byte: u8) -> Option<SqlEvent> {
        match &mut self.state {
            State::Normal(normal_state) => match normal_state.process_byte(byte) {
                NormalEvent::StartInsertHeader(initial_bytes) => {
                    self.state =
                        State::InsertHeader(InsertHeaderState::new(self.dialect, initial_bytes));
                    None
                }
                NormalEvent::Continue => None,
                NormalEvent::DefaultStatement(byte) => Some(SqlEvent::DefaultStatement(byte)),
            },
            State::InsertHeader(header_state) => match header_state.process_byte(byte) {
                InsertHeaderEvent::HeaderComplete {
                    format,
                    header_bytes,
                } => {
                    let v_state = ValueState::new(self.dialect, format);
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
                InsertHeaderEvent::Continue => None,
            },
            State::ValueMode(v_state) => match v_state.process_byte(byte) {
                ValueEvent::TupleComplete(data) => Some(SqlEvent::Tuple(data, v_state.format)),
                ValueEvent::ExitValuesMode(footer_bytes) => {
                    self.state = State::Normal(NormalState::new());
                    Some(SqlEvent::Footer(footer_bytes))
                }
                ValueEvent::Continue => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    SqlEvent::DefaultStatement(_) => continue,
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
                    SqlEvent::DefaultStatement(_) => continue,
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
}