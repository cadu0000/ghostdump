pub mod schema;
pub mod state;
pub mod tokenizer;

pub use schema::extract_metadata;
pub use state::SqlDialect;
use state::{
    InsertHeaderEvent, InsertHeaderState, NormalEvent, NormalState, State, ValueEvent, ValueState,
};

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

    pub fn handle_byte(&mut self, byte: u8) -> Option<Vec<u8>> {
        match &mut self.state {
            State::Normal(normal_state) => match normal_state.process_byte(byte) {
                NormalEvent::StartInsertHeader(initial_bytes) => {
                    self.state =
                        State::InsertHeader(InsertHeaderState::new(self.dialect, initial_bytes));
                    None
                }
                NormalEvent::Continue => None,
            },
            State::InsertHeader(header_state) => match header_state.process_byte(byte) {
                InsertHeaderEvent::HeaderComplete {
                    format,
                    header_bytes,
                } => {
                    let v_state = ValueState::new(self.dialect, format);
                    self.state = State::ValueMode(v_state);

                    Some(header_bytes)
                }
                InsertHeaderEvent::Continue => None,
            },
            State::ValueMode(v_state) => match v_state.process_byte(byte) {
                ValueEvent::TupleComplete(data) => Some(data),
                ValueEvent::ExitValuesMode => {
                    self.state = State::Normal(NormalState::new());
                    None
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
            if let Some(data) = parser.handle_byte(byte) {
                let event_string = String::from_utf8_lossy(&data).into_owned();
                extracted_events.push(event_string);
            }
        }

        assert_eq!(extracted_events.len(), 3);
        assert_eq!(extracted_events[0], "INSERT INTO users (id, name) VALUES");

        assert_eq!(extracted_events[1], "(1, 'Alice')");
        assert_eq!(extracted_events[2], "(2, 'Bob, the Builder')");
    }

    #[test]
    fn test_parser_with_postgres_copy() {
        let mut parser = SqlParser::new(SqlDialect::Postgres);
        let sql = b"COPY public.users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n";
        let mut extracted_events = Vec::new();

        for &byte in sql {
            if let Some(data) = parser.handle_byte(byte) {
                let event_string = String::from_utf8_lossy(&data).into_owned();
                extracted_events.push(event_string);
            }
        }

        assert_eq!(extracted_events.len(), 3);

        assert_eq!(
            extracted_events[0],
            "COPY public.users (id, name) FROM stdin;"
        );

        assert_eq!(extracted_events[1], "1\tAlice\n");
        assert_eq!(extracted_events[2], "2\tBob\n");
    }
}
