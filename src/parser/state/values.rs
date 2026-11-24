//! The rows of a data block, up to its end.

use super::{InsertFormat, Remainder, SqlDialect, is_separator};
use crate::parser::lexer::{ByteClass, Lexer};

pub enum ValueEvent {
    Continue,
    TupleComplete(Vec<u8>),
    ExitValuesMode(Vec<u8>),
    /// A byte outside the rows (separators, comments, the rest of the COPY
    /// command line), written unchanged.
    Passthrough(u8),
    /// A row grew past the size limit without ending.
    RowTooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RowPhase {
    /// VALUES: after `VALUES` or a comma, before the next row.
    BeforeRow,
    /// VALUES: inside a row, at the given parenthesis depth.
    InRow(usize),
    /// VALUES: after a row, before a comma or the end of the list.
    AfterRow,
    /// VALUES: after the rows, in a clause such as `ON CONFLICT ...`, held
    /// until the `;` so that the whole footer can be checked before it is
    /// written.
    Trailer,
    /// COPY: on the rest of the `COPY ... FROM stdin;` line, which holds no data.
    CopyCommandLine,
    /// COPY: data lines, until the `\.` line.
    CopyData,
}

pub struct ValueState {
    pub tuple_buffer: Vec<u8>,
    /// VALUES: the clause after the rows, up to the `;`.
    trailer: Vec<u8>,
    pub format: InsertFormat,
    phase: RowPhase,
    lexer: Lexer,
    max_row_bytes: usize,
}

impl ValueState {
    pub fn new(dialect: SqlDialect, format: InsertFormat, max_row_bytes: usize) -> Self {
        ValueState {
            max_row_bytes,
            tuple_buffer: Vec::with_capacity(1024),
            trailer: Vec::new(),
            format,
            phase: match format {
                InsertFormat::Values => RowPhase::BeforeRow,
                InsertFormat::Copy => RowPhase::CopyCommandLine,
            },
            lexer: Lexer::new(dialect),
        }
    }

    /// Takes the byte that followed `VALUES`. Returns true when it opened the
    /// first row; otherwise the byte belongs to the header and only updates
    /// the lexer (it may open a comment, for example).
    pub fn take_carry(&mut self, byte: u8) -> bool {
        let class = self.lexer.feed(byte);
        if class == ByteClass::Code && byte == b'(' {
            self.phase = RowPhase::InRow(1);
            self.tuple_buffer.push(byte);
            return true;
        }
        false
    }

    pub fn process_byte(&mut self, byte: u8) -> ValueEvent {
        match self.format {
            InsertFormat::Copy => self.process_copy_byte(byte),
            InsertFormat::Values => self.process_values_byte(byte),
        }
    }

    /// Called when the input ends inside this data block.
    pub fn finish(&mut self) -> Remainder {
        let rest = std::mem::take(&mut self.tuple_buffer);
        let footer = std::mem::take(&mut self.trailer);

        match self.phase {
            // End-of-data marker without its final newline.
            RowPhase::CopyData if rest == b"\\." || rest == b"\\.\r" => Remainder::Bytes(rest),
            RowPhase::CopyData => Remainder::UnterminatedData {
                partial_row: !rest.is_empty(),
                footer,
            },
            RowPhase::InRow(_) => Remainder::UnterminatedData {
                partial_row: true,
                footer,
            },
            _ => Remainder::UnterminatedData {
                partial_row: false,
                footer,
            },
        }
    }

    fn process_copy_byte(&mut self, byte: u8) -> ValueEvent {
        if self.phase == RowPhase::CopyCommandLine {
            if byte == b'\n' {
                self.phase = RowPhase::CopyData;
            }
            return ValueEvent::Passthrough(byte);
        }

        self.tuple_buffer.push(byte);

        if byte != b'\n' {
            return self.size_check();
        }

        let data = std::mem::take(&mut self.tuple_buffer);

        // The marker is a line of its own: a row such as `x\\.` (an escaped
        // backslash followed by a dot) also ends with `\.`.
        if data == b"\\.\n" || data == b"\\.\r\n" {
            return ValueEvent::ExitValuesMode(data);
        }

        // An empty line is a row too: one empty-string column.
        ValueEvent::TupleComplete(data)
    }

    fn process_values_byte(&mut self, byte: u8) -> ValueEvent {
        let class = self.lexer.feed(byte);
        let is_code = class == ByteClass::Code;

        match self.phase {
            RowPhase::InRow(depth) => {
                self.tuple_buffer.push(byte);
                if self.tuple_buffer.len() > self.max_row_bytes {
                    return ValueEvent::RowTooLarge;
                }
                if is_code && byte == b'(' {
                    self.phase = RowPhase::InRow(depth + 1);
                } else if is_code && byte == b')' {
                    if depth == 1 {
                        self.phase = RowPhase::AfterRow;
                        return ValueEvent::TupleComplete(std::mem::take(&mut self.tuple_buffer));
                    }
                    self.phase = RowPhase::InRow(depth - 1);
                }
                ValueEvent::Continue
            }
            _ if is_code && byte == b';' => {
                let mut footer = std::mem::take(&mut self.trailer);
                footer.push(byte);
                ValueEvent::ExitValuesMode(footer)
            }
            RowPhase::BeforeRow if is_code && byte == b'(' => {
                self.phase = RowPhase::InRow(1);
                self.tuple_buffer.push(byte);
                ValueEvent::Continue
            }
            RowPhase::AfterRow if is_code && byte == b',' => {
                self.phase = RowPhase::BeforeRow;
                ValueEvent::Passthrough(byte)
            }
            RowPhase::BeforeRow | RowPhase::AfterRow if is_separator(byte, class) => {
                ValueEvent::Passthrough(byte)
            }
            // Anything other than whitespace or a comment ends the list of
            // rows: `ON CONFLICT (id) DO NOTHING` must not be read as a row.
            RowPhase::BeforeRow | RowPhase::AfterRow | RowPhase::Trailer => {
                self.phase = RowPhase::Trailer;
                self.trailer.push(byte);
                if self.trailer.len() > self.max_row_bytes {
                    return ValueEvent::RowTooLarge;
                }
                ValueEvent::Continue
            }
            RowPhase::CopyCommandLine | RowPhase::CopyData => {
                unreachable!("COPY bytes are handled by process_copy_byte")
            }
        }
    }
}

impl ValueState {
    fn size_check(&self) -> ValueEvent {
        if self.tuple_buffer.len() > self.max_row_bytes {
            ValueEvent::RowTooLarge
        } else {
            ValueEvent::Continue
        }
    }
}
