pub mod state;

pub use state::SqlDialect; 
use state::{State, ValueEvent };

pub struct Parser {
    state: State,
    dialect: SqlDialect,
}

impl Parser {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            state: State::Normal,
            dialect,
        }
    }

    pub fn handle_byte(&mut self, byte: u8) -> Option<Vec<u8>> {
        match &mut self.state {
            State::Normal => {
                None
            }
            State::InsertHeader { keyword_buf } => {
                None
            }
            State::ValueMode(v_state) => {
                match v_state.process_byte(byte) {
                    ValueEvent::TupleComplete(data) => Some(data),
                    ValueEvent::ExitValuesMode => {
                        self.state = State::Normal;
                        None
                    }
                    ValueEvent::Continue => None,
                }
            }
        }
    }
}