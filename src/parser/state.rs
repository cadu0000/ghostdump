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
        /// Bytes before the keyword (the end of the statement before), to
        /// be written unchanged.
        before: Vec<u8>,
        /// The keyword and the byte after it, which start the header.
        initial_bytes: Vec<u8>,
        keyword: DataKeyword,
        /// The byte after the keyword, already included in `initial_bytes`.
        delimiter: u8,
    },
    DefaultStatement(u8),
}

/// Bytes held back so that the first word of a statement (up to `REPLACE`)
/// and the byte after it are still available when the keyword is recognized.
const LOOKBEHIND: usize = 8;

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

/// Passes SQL through and watches for statements that start with `INSERT`,
/// `REPLACE` (MySQL and SQLite) or `COPY` (PostgreSQL). Keywords inside literals, comments, identifiers or other
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

        if let Some((keyword, len)) = self.advance(byte) {
            let start = self.pending.len() - len - 1;
            return NormalEvent::StartInsertHeader {
                before: self.pending.drain(..start).collect(),
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

    /// The data keyword that ended at this byte, and its length.
    fn advance(&mut self, byte: u8) -> Option<(DataKeyword, usize)> {
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
                        return Some((keyword, len));
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
            7 if self.dialect != SqlDialect::Postgres => (b"REPLACE", DataKeyword::Insert),
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
        /// COPY with options GhostDump cannot read (`WITH (FORMAT csv)`): its
        /// rows can be copied but not masked.
        opaque: bool,
    },
    /// The statement ended without opening a data block (`INSERT ... SELECT`,
    /// `COPY ... TO stdout`); its bytes pass through unchanged.
    NotData(Vec<u8>),
    /// The header grew past the size limit without ending.
    TooLarge,
}

/// COPY: where the rows come from, as far as the words read so far tell.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CopySource {
    Unknown,
    /// Right after `FROM`.
    AfterFrom,
    /// `FROM STDIN`, or `FROM STDOUT`, which the server reads the same way:
    /// the rows follow the statement (psql, `psqlscan_is_copy_from_stdin`).
    Inline,
    /// A file, a program, or no `FROM` at all.
    Elsewhere,
}

pub struct InsertHeaderState {
    pub keyword_buf: Vec<u8>,
    pub dialect: SqlDialect,
    keyword: DataKeyword,
    lexer: Lexer,
    /// Length of the run of identifier bytes that ends at the last byte.
    word_len: usize,
    /// Parenthesis depth: `VALUES` and `FROM` count only outside parentheses.
    depth: u32,
    /// INSERT: a word showed that the rows come from a query or from
    /// assignments (`SELECT`, `TABLE`, `SET`, `DEFAULT VALUES`), so a later
    /// `VALUES` does not open a data block.
    not_values: bool,
    source: CopySource,
    /// COPY: something other than spaces followed `STDIN`.
    copy_options: bool,
    max_bytes: usize,
}

impl InsertHeaderState {
    pub fn new(
        dialect: SqlDialect,
        keyword: DataKeyword,
        initial_bytes: Vec<u8>,
        delimiter: u8,
        max_bytes: usize,
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
            depth: 0,
            not_values: false,
            source: CopySource::Unknown,
            copy_options: false,
            max_bytes,
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> InsertHeaderEvent {
        self.keyword_buf.push(byte);
        if self.keyword_buf.len() > self.max_bytes {
            return InsertHeaderEvent::TooLarge;
        }

        let class = self.lexer.feed(byte);
        if class == ByteClass::Code && is_ident_byte(byte) {
            self.word_len += 1;
            self.note_copy_option(byte, class);
            return InsertHeaderEvent::Continue;
        }

        // A word ends here, if one was being read.
        let word_len = std::mem::take(&mut self.word_len);
        let outside = self.depth == 0;
        if outside && word_len > 0 {
            self.take_word(word_len);
        } else if self.source == CopySource::AfterFrom && !is_separator(byte, class) {
            // `FROM 'file'`.
            self.source = CopySource::Elsewhere;
        }
        self.note_copy_option(byte, class);

        if class != ByteClass::Code {
            return InsertHeaderEvent::Continue;
        }
        // psql ends a statement only outside parentheses.
        let ends_statement = byte == b';' && (outside || self.dialect != SqlDialect::Postgres);

        match self.keyword {
            // `INSERT ... DEFAULT VALUES;` has no rows: it falls through to `NotData`.
            DataKeyword::Insert
                if !ends_statement
                    && outside
                    && !self.not_values
                    && self.is_values_keyword(word_len) =>
            {
                let mut header_bytes = std::mem::take(&mut self.keyword_buf);
                header_bytes.pop();

                return InsertHeaderEvent::HeaderComplete {
                    format: InsertFormat::Values,
                    header_bytes,
                    carry: Some(byte),
                    opaque: false,
                };
            }
            DataKeyword::Copy if ends_statement && self.source == CopySource::Inline => {
                return InsertHeaderEvent::HeaderComplete {
                    format: InsertFormat::Copy,
                    header_bytes: std::mem::take(&mut self.keyword_buf),
                    carry: None,
                    opaque: self.copy_options,
                };
            }
            _ => {}
        }

        match byte {
            b'(' => self.depth += 1,
            b')' => self.depth = self.depth.saturating_sub(1),
            _ => {}
        }

        if ends_statement {
            return InsertHeaderEvent::NotData(std::mem::take(&mut self.keyword_buf));
        }

        InsertHeaderEvent::Continue
    }

    /// Takes a word outside parentheses, made of the `len` bytes before the last one.
    fn take_word(&mut self, len: usize) {
        match self.keyword {
            DataKeyword::Insert => {
                let query = ["SELECT", "WITH", "TABLE", "DEFAULT"]
                    .iter()
                    .any(|word| self.last_word_is(len, word.as_bytes()));
                let assignments =
                    self.dialect == SqlDialect::Mysql && self.last_word_is(len, b"SET");
                if query || assignments {
                    self.not_values = true;
                }
            }
            DataKeyword::Copy => {
                self.source = match self.source {
                    CopySource::Unknown if self.last_word_is(len, b"FROM") => CopySource::AfterFrom,
                    CopySource::AfterFrom
                        if self.last_word_is(len, b"STDIN")
                            || self.last_word_is(len, b"STDOUT") =>
                    {
                        CopySource::Inline
                    }
                    CopySource::AfterFrom => CopySource::Elsewhere,
                    source => source,
                };
            }
        }
    }

    /// `VALUES`, or its MySQL synonym `VALUE`, made of the `len` bytes before the last one.
    fn is_values_keyword(&self, len: usize) -> bool {
        self.last_word_is(len, b"VALUES")
            || (self.dialect == SqlDialect::Mysql && self.last_word_is(len, b"VALUE"))
    }

    /// After `STDIN`, anything other than spaces and the final `;` is a COPY option.
    fn note_copy_option(&mut self, byte: u8, class: ByteClass) {
        let plain = class == ByteClass::Code && (byte.is_ascii_whitespace() || byte == b';');
        if self.source == CopySource::Inline && !plain {
            self.copy_options = true;
        }
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
    /// A byte outside the rows (separators, comments, trailing clauses such
    /// as `ON CONFLICT DO NOTHING`, the rest of the COPY command line),
    /// written unchanged.
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
    /// VALUES: after the rows, in a trailing clause, until the `;`.
    Trailer,
    /// COPY: on the rest of the `COPY ... FROM stdin;` line, which holds no data.
    CopyCommandLine,
    /// COPY: data lines, until the `\.` line.
    CopyData,
}

pub struct ValueState {
    pub tuple_buffer: Vec<u8>,
    pub dialect: SqlDialect,
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
            dialect,
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

        match self.phase {
            // End-of-data marker without its final newline.
            RowPhase::CopyData if rest == b"\\." || rest == b"\\.\r" => Remainder::Bytes(rest),
            RowPhase::CopyData => Remainder::UnterminatedData {
                partial_row: !rest.is_empty(),
            },
            RowPhase::InRow(_) => Remainder::UnterminatedData { partial_row: true },
            _ => Remainder::UnterminatedData { partial_row: false },
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
            _ if is_code && byte == b';' => ValueEvent::ExitValuesMode(vec![byte]),
            RowPhase::BeforeRow if is_code && byte == b'(' => {
                self.phase = RowPhase::InRow(1);
                self.tuple_buffer.push(byte);
                ValueEvent::Continue
            }
            RowPhase::AfterRow if is_code && byte == b',' => {
                self.phase = RowPhase::BeforeRow;
                ValueEvent::Passthrough(byte)
            }
            RowPhase::BeforeRow | RowPhase::AfterRow => {
                // Anything other than whitespace or a comment ends the list of
                // rows: `ON CONFLICT (id) DO NOTHING` must not be read as a row.
                if !is_separator(byte, class) {
                    self.phase = RowPhase::Trailer;
                }
                ValueEvent::Passthrough(byte)
            }
            RowPhase::Trailer => ValueEvent::Passthrough(byte),
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

/// Whitespace, comments, and a `-` or `/` that may open a comment.
fn is_separator(byte: u8, class: ByteClass) -> bool {
    match class {
        ByteClass::Comment | ByteClass::CommentStart => true,
        ByteClass::Literal => false,
        ByteClass::Code => byte.is_ascii_whitespace() || byte == b'-' || byte == b'/',
    }
}
