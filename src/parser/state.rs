#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqlDialect {
    Mysql,
    Postgres,
    Sqlite,
}

pub enum State {
    Normal(NormalState),
    InsertHeader(InsertHeaderState),
    ValueMode(ValueState),
}

pub enum NormalEvent {
    Continue,
    StartInsertHeader(Vec<u8>),
}

pub struct NormalState {
    pub buf: Vec<u8>,
}

impl NormalState {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32),
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> NormalEvent {
        self.buf.push(byte);

        let buf_len = self.buf.len();

        let is_insert = buf_len >= 6 && self.buf[buf_len - 6..].eq_ignore_ascii_case(b"INSERT");
        let is_copy = buf_len >= 4 && self.buf[buf_len - 4..].eq_ignore_ascii_case(b"COPY");

        if is_insert {
            NormalEvent::StartInsertHeader(b"INSERT".to_vec())
        } else if is_copy {
            NormalEvent::StartInsertHeader(b"COPY".to_vec())
        } else {
            NormalEvent::Continue
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InsertFormat {
    Values,
    Copy,
}

pub enum InsertHeaderEvent {
    Continue,
    HeaderComplete {
        format: InsertFormat,
        header_bytes: Vec<u8>,
    },
}

pub struct InsertHeaderState {
    pub keyword_buf: Vec<u8>,
    pub dialect: SqlDialect,
}

impl InsertHeaderState {
    pub fn new(dialect: SqlDialect, initial_bytes: Vec<u8>) -> Self {
        Self {
            keyword_buf: initial_bytes,
            dialect,
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> InsertHeaderEvent {
        self.keyword_buf.push(byte);

        let targets: &[&[u8]] = match self.dialect {
            SqlDialect::Mysql => &[b"VALUES"],
            SqlDialect::Sqlite => &[b"VALUES"],
            SqlDialect::Postgres => &[b"VALUES" as &[u8], b"STDIN;" as &[u8]],
        };

        let buf_len = self.keyword_buf.len();
        let mut found_format = None;

        for &target_word in targets {
            let target_len = target_word.len();

            if buf_len >= target_len {
                let tail = &self.keyword_buf[buf_len - target_len..];
                if tail.eq_ignore_ascii_case(target_word) {
                    if target_word.eq_ignore_ascii_case(b"STDIN;") {
                        found_format = Some(InsertFormat::Copy);
                    } else {
                        found_format = Some(InsertFormat::Values);
                    }
                    break;
                }
            }
        }

        if let Some(format) = found_format {
            let header_bytes = std::mem::take(&mut self.keyword_buf);

            InsertHeaderEvent::HeaderComplete {
                format,
                header_bytes,
            }
        } else {
            InsertHeaderEvent::Continue
        }
    }
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
    pub format: InsertFormat,
}

impl ValueState {
    pub fn new(dialect: SqlDialect, format: InsertFormat) -> Self {
        ValueState {
            paren_depth: 0,
            inside_string: false,
            escape_next: false,
            tuple_buffer: Vec::with_capacity(1024),
            dialect,
            format,
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> ValueEvent {
        match self.format {
            InsertFormat::Copy => self.process_copy_byte(byte),
            InsertFormat::Values => self.process_values_byte(byte),
        }
    }

    fn process_copy_byte(&mut self, byte: u8) -> ValueEvent {
        self.tuple_buffer.push(byte);

        if byte == b'\n' {
            let len = self.tuple_buffer.len();

            let is_end_marker = if len >= 3 && &self.tuple_buffer[len - 3..] == b"\\.\n" {
                true
            } else if len >= 4 && &self.tuple_buffer[len - 4..] == b"\\.\r\n" {
                true
            } else {
                false
            };

            if is_end_marker {
                return ValueEvent::ExitValuesMode;
            } else {
                let data = std::mem::take(&mut self.tuple_buffer);

                if data == b"\n" || data == b"\r\n" {
                    return ValueEvent::Continue;
                }

                return ValueEvent::TupleComplete(data);
            }
        }

        ValueEvent::Continue
    }

    fn process_values_byte(&mut self, byte: u8) -> ValueEvent {
        if self.escape_next {
            self.tuple_buffer.push(byte);
            self.escape_next = false;
            return ValueEvent::Continue;
        }

        match byte {
            b'\\' => {
                self.tuple_buffer.push(byte);
                self.escape_next = true;
            }
            b'\'' => {
                self.tuple_buffer.push(byte);
                self.inside_string = !self.inside_string;
            }
            b'(' if !self.inside_string => {
                self.paren_depth += 1;
                self.tuple_buffer.push(byte);
            }
            b')' if !self.inside_string => {
                self.tuple_buffer.push(byte);

                if self.paren_depth > 0 {
                    self.paren_depth -= 1;

                    if self.paren_depth == 0 {
                        let data = std::mem::take(&mut self.tuple_buffer);
                        return ValueEvent::TupleComplete(data);
                    }
                }
            }
            b';' if !self.inside_string && self.paren_depth == 0 => {
                return ValueEvent::ExitValuesMode;
            }
            _ => {
                if self.paren_depth > 0 {
                    self.tuple_buffer.push(byte);
                }
            }
        }

        ValueEvent::Continue
    }
}
