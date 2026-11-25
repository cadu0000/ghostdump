//! The header of a data block, from its keyword to its first row:
//! `INSERT ... VALUES` or `COPY ... FROM stdin;`.

use super::{DataKeyword, InsertFormat, SqlDialect, is_separator};
use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};

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
    /// `COPY ... FROM 'file'`, `COPY ... TO stdout`): it is reported whole,
    /// to be checked before it is written.
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
