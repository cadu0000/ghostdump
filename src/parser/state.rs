#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqlDialect {
    Mysql,
    Postgres,
    Sqlite,
}

pub enum State {
    Normal,
    InsertHeader {
        keyword_buf: Vec<u8>,
    },
    ValueMode(ValueState),
}

pub enum ValueEvent {
    Continue,
    TupleComplete(Vec<u8>),
    ExitValuesMode,
}

pub struct ValueState {
    pub paren_depth: usize,
    pub inside_string: bool,
    pub escape_next: bool,
    pub tuple_buffer: Vec<u8>,
    pub dialect: SqlDialect,
}

impl ValueState {
    pub fn new(dialect: SqlDialect) -> Self {
        ValueState {
            paren_depth: 0,
            inside_string: false,
            escape_next: false,
            tuple_buffer: Vec::with_capacity(1024),
            dialect,
        }
    }
    
    pub fn process_byte(&mut self, byte: u8) -> ValueEvent {
        todo!()
    }
}
