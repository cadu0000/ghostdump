use std::collections::VecDeque;

use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};

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

/// Statement keywords that may open a data block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataKeyword {
    Insert,
    Copy,
}

/// What the parser still holds when the input ends.
#[derive(Debug, PartialEq)]
pub enum Remainder {
    /// Buffered bytes outside data rows, to be written unchanged.
    Bytes(Vec<u8>),
    /// The input ended inside a data block, before its terminator. When
    /// `partial_row` is true, an incomplete row was dropped: it cannot be
    /// masked, so it must not be written.
    UnterminatedData { partial_row: bool },
}

pub enum NormalEvent {
    Continue,
    StartInsertHeader {
        initial_bytes: Vec<u8>,
        keyword: DataKeyword,
        /// The byte after the keyword, already included in `initial_bytes`.
        delimiter: u8,
    },
    DefaultStatement(u8),
}

/// Bytes held back so that the first word of a statement (up to `INSERT`)
/// and the byte after it are still available when the keyword is recognized.
const LOOKBEHIND: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq)]
enum StatementPhase {
    /// Before the first token of a statement: only whitespace and comments so far.
    Start,
    /// A `-` or `/` at statement start that may open a comment.
    MaybeComment,
    /// Reading the first word of a statement; holds its length so far.
    FirstWord(usize),
    /// Inside a psql meta-command line, such as `\connect` or `\restrict`.
    MetaCommand,
    /// Anywhere else in a statement.
    Body,
}

/// Passes SQL through and watches for statements that start with `INSERT`
/// or `COPY`. Keywords inside literals, comments, identifiers or other
/// statements are ignored.
pub struct NormalState {
    dialect: SqlDialect,
    lexer: Lexer,
    pending: VecDeque<u8>,
    phase: StatementPhase,
    /// Bytes of a UTF-8 BOM matched so far; `None` once past the input start.
    bom_matched: Option<usize>,
}

impl NormalState {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            lexer: Lexer::new(dialect),
            pending: VecDeque::with_capacity(LOOKBEHIND + 1),
            phase: StatementPhase::Start,
            bom_matched: None,
        }
    }

    /// State for the very beginning of the input, where a UTF-8 BOM may appear.
    pub fn at_input_start(dialect: SqlDialect) -> Self {
        Self {
            bom_matched: Some(0),
            ..Self::new(dialect)
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> NormalEvent {
        self.pending.push_back(byte);

        if let Some(keyword) = self.advance(byte) {
            return NormalEvent::StartInsertHeader {
                initial_bytes: self.pending.drain(..).collect(),
                keyword,
                delimiter: byte,
            };
        }

        if self.pending.len() > LOOKBEHIND
            && let Some(oldest) = self.pending.pop_front()
        {
            return NormalEvent::DefaultStatement(oldest);
        }

        NormalEvent::Continue
    }

    /// Bytes still held back, to be written as they are when the input ends.
    pub fn take_pending(&mut self) -> Vec<u8> {
        self.pending.drain(..).collect()
    }

    fn advance(&mut self, byte: u8) -> Option<DataKeyword> {
        if self.skip_bom(byte) {
            return None;
        }

        if self.phase == StatementPhase::MetaCommand {
            // The line is not SQL: its quotes must not reach the lexer.
            if byte == b'\n' {
                self.phase = StatementPhase::Start;
            }
            return None;
        }

        let class = self.lexer.feed(byte);

        match self.phase {
            StatementPhase::Start => self.phase = self.after_start(byte, class),
            StatementPhase::MaybeComment => {
                if class == ByteClass::CommentStart {
                    self.phase = StatementPhase::Start;
                } else {
                    self.phase = StatementPhase::Body;
                    self.body_byte(byte, class);
                }
            }
            StatementPhase::FirstWord(len) => {
                if class == ByteClass::Code && is_ident_byte(byte) {
                    self.phase = StatementPhase::FirstWord(len + 1);
                } else {
                    let ends_statement = class == ByteClass::Code && byte == b';';
                    if let Some(keyword) = self.keyword_before_last(len)
                        && !ends_statement
                    {
                        return Some(keyword);
                    }
                    self.phase = StatementPhase::Body;
                    self.body_byte(byte, class);
                }
            }
            StatementPhase::Body => self.body_byte(byte, class),
            StatementPhase::MetaCommand => unreachable!("meta-command lines are handled above"),
        }

        None
    }

    fn after_start(&self, byte: u8, class: ByteClass) -> StatementPhase {
        match class {
            ByteClass::Comment | ByteClass::CommentStart => StatementPhase::Start,
            ByteClass::Literal => StatementPhase::Body,
            ByteClass::Code => match byte {
                b';' => StatementPhase::Start,
                b'-' | b'/' => StatementPhase::MaybeComment,
                b'\\' if self.dialect == SqlDialect::Postgres => StatementPhase::MetaCommand,
                b if b.is_ascii_whitespace() => StatementPhase::Start,
                b if b.is_ascii_alphabetic() => StatementPhase::FirstWord(1),
                _ => StatementPhase::Body,
            },
        }
    }

    fn body_byte(&mut self, byte: u8, class: ByteClass) {
        if class == ByteClass::Code && byte == b';' {
            self.phase = StatementPhase::Start;
        }
    }

    /// The data keyword formed by the `len` bytes before the last one, if any.
    fn keyword_before_last(&self, len: usize) -> Option<DataKeyword> {
        let (word, keyword): (&[u8], DataKeyword) = match len {
            6 => (b"INSERT", DataKeyword::Insert),
            4 if self.dialect == SqlDialect::Postgres => (b"COPY", DataKeyword::Copy),
            _ => return None,
        };

        let end = self.pending.len().checked_sub(1)?;
        let start = end.checked_sub(len)?;

        self.pending
            .range(start..end)
            .zip(word)
            .all(|(byte, expected)| byte.eq_ignore_ascii_case(expected))
            .then_some(keyword)
    }

    fn skip_bom(&mut self, byte: u8) -> bool {
        const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

        let Some(matched) = self.bom_matched else {
            return false;
        };

        if byte == BOM[matched] {
            self.bom_matched = (matched + 1 < BOM.len()).then_some(matched + 1);
            true
        } else {
            self.bom_matched = None;
            false
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
        /// The byte after `VALUES`, which belongs to the rows.
        carry: Option<u8>,
    },
    /// The statement ended without opening a data block (`INSERT ... SELECT`,
    /// `COPY ... TO stdout`); its bytes pass through unchanged.
    NotData(Vec<u8>),
}

pub struct InsertHeaderState {
    pub keyword_buf: Vec<u8>,
    pub dialect: SqlDialect,
    keyword: DataKeyword,
    lexer: Lexer,
    /// Length of the run of identifier bytes that ends at the last byte.
    word_len: usize,
}

impl InsertHeaderState {
    pub fn new(
        dialect: SqlDialect,
        keyword: DataKeyword,
        initial_bytes: Vec<u8>,
        delimiter: u8,
    ) -> Self {
        // The keyword itself is plain code; only the byte after it can
        // change the lexer state (for example, by opening a comment).
        let mut lexer = Lexer::new(dialect);
        lexer.feed(delimiter);

        Self {
            keyword_buf: initial_bytes,
            dialect,
            keyword,
            lexer,
            word_len: 0,
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> InsertHeaderEvent {
        self.keyword_buf.push(byte);

        if self.lexer.feed(byte) != ByteClass::Code {
            self.word_len = 0;
            return InsertHeaderEvent::Continue;
        }

        if is_ident_byte(byte) {
            self.word_len += 1;
            return InsertHeaderEvent::Continue;
        }

        let word_len = std::mem::take(&mut self.word_len);
        let ends_statement = byte == b';';

        match self.keyword {
            // `INSERT ... DEFAULT VALUES;` has no rows: it falls through to `NotData`.
            DataKeyword::Insert if !ends_statement && self.last_word_is(word_len, b"VALUES") => {
                let mut header_bytes = std::mem::take(&mut self.keyword_buf);
                header_bytes.pop();

                return InsertHeaderEvent::HeaderComplete {
                    format: InsertFormat::Values,
                    header_bytes,
                    carry: Some(byte),
                };
            }
            DataKeyword::Copy if ends_statement && self.last_word_is(word_len, b"STDIN") => {
                return InsertHeaderEvent::HeaderComplete {
                    format: InsertFormat::Copy,
                    header_bytes: std::mem::take(&mut self.keyword_buf),
                    carry: None,
                };
            }
            _ => {}
        }

        if ends_statement {
            return InsertHeaderEvent::NotData(std::mem::take(&mut self.keyword_buf));
        }

        InsertHeaderEvent::Continue
    }

    /// Whether the `len` bytes before the last one spell `word`.
    fn last_word_is(&self, len: usize, word: &[u8]) -> bool {
        let end = self.keyword_buf.len() - 1;
        len == word.len() && self.keyword_buf[end - len..end].eq_ignore_ascii_case(word)
    }
}

pub enum ValueEvent {
    Continue,
    TupleComplete(Vec<u8>),
    ExitValuesMode(Vec<u8>),
}

pub struct ValueState {
    pub paren_depth: usize,
    pub inside_string: bool,
    pub escape_next: bool,
    pub tuple_buffer: Vec<u8>,
    pub dialect: SqlDialect,
    pub format: InsertFormat,
    /// COPY only: still on the line of the `COPY ... FROM stdin;` command,
    /// which holds no data.
    in_command_line: bool,
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
            in_command_line: format == InsertFormat::Copy,
        }
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

        match self.format {
            // End-of-data marker without its final newline.
            InsertFormat::Copy if rest == b"\\." || rest == b"\\.\r" => Remainder::Bytes(rest),
            InsertFormat::Copy => Remainder::UnterminatedData {
                partial_row: !rest.is_empty(),
            },
            InsertFormat::Values => Remainder::UnterminatedData {
                partial_row: self.paren_depth > 0,
            },
        }
    }

    fn process_copy_byte(&mut self, byte: u8) -> ValueEvent {
        if self.in_command_line {
            if byte == b'\n' {
                self.in_command_line = false;
            }
            return ValueEvent::Continue;
        }

        self.tuple_buffer.push(byte);

        if byte != b'\n' {
            return ValueEvent::Continue;
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
                self.tuple_buffer.push(byte);
                let data = std::mem::take(&mut self.tuple_buffer);
                return ValueEvent::ExitValuesMode(data);
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
