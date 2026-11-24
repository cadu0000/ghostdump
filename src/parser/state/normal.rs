//! The state between data blocks: where each statement ends, which ones
//! open a data block, and which are held whole for checking.

use std::collections::VecDeque;

use super::framer::Framer;
use super::{DataKeyword, Remainder, SqlDialect, is_separator};
use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};

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
    /// Bytes held while the start of a statement was read, to be written
    /// unchanged.
    Released(Vec<u8>),
    /// A whole statement that may write data in a form that cannot be
    /// masked (`UPDATE`, `CREATE TABLE ... AS SELECT`, ...).
    Statement(Vec<u8>),
    /// A statement held whole grew past the size limit without ending.
    TooLarge,
    /// A client command that dump tools do not write (psql `\echo`, mysql
    /// `\g` or `source`, sqlite3 `.print`), found while such commands are
    /// refused.
    ClientCommand(Vec<u8>),
}

/// Longest client command name kept for messages.
const MAX_COMMAND: usize = 32;

/// mysql client commands that may start a statement, besides `DELIMITER`,
/// which is read, and `USE`, which is SQL too (`mysql --help` lists them).
const MYSQL_COMMANDS: &[&str] = &[
    "clear",
    "connect",
    "edit",
    "ego",
    "exit",
    "go",
    "help",
    "nopager",
    "notee",
    "pager",
    "print",
    "prompt",
    "quit",
    "rehash",
    "source",
    "status",
    "system",
    "tee",
    "charset",
    "warnings",
    "nowarning",
];

/// Bytes held back so that the first word of a statement (up to `REPLACE`)
/// and the byte after it are still available when the keyword is recognized.
const LOOKBEHIND: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
enum StatementPhase {
    /// Before the first token of a statement: only whitespace and comments so far.
    Start,
    /// A `-` or `/` at statement start that may open a comment, or in MySQL
    /// a `*` that may close a version comment.
    MaybeComment,
    /// Reading the first word of a statement.
    FirstWord,
    /// Inside a psql meta-command line, such as `\connect` or `\restrict`.
    MetaCommand,
    /// MySQL: the rest of a `DELIMITER` line, which holds the new delimiter.
    DelimiterLine,
    /// Anywhere else in a statement.
    Body,
}

/// How the bytes of the current statement are written.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Handling {
    /// As they come, after the short lookbehind.
    Stream,
    /// Held until the statement ends and then reported whole, so that it can
    /// be refused before any of it is written.
    Hold,
    /// After `CREATE`: held until the next words show whether the statement
    /// creates a table, which a query may fill (`CREATE TABLE ... AS SELECT`).
    AfterCreate,
}

/// What a statement's first word makes of it.
enum FirstWord {
    Data(DataKeyword),
    Delimiter,
    /// A mysql client command (`source`, `charset`, ...).
    Command,
    Handle(Handling),
}

/// The word being read: its first bytes in lower case, and its length.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Word {
    start: [u8; 12],
    len: usize,
}

impl Word {
    fn push(&mut self, byte: u8) {
        if let Some(slot) = self.start.get_mut(self.len) {
            *slot = byte.to_ascii_lowercase();
        }
        self.len += 1;
    }

    /// Whether the word is `keyword`, given in lower case.
    pub(super) fn is(&self, keyword: &str) -> bool {
        self.len == keyword.len() && self.start.get(..self.len) == Some(keyword.as_bytes())
    }

    pub(super) fn is_any(&self, keywords: &[&str]) -> bool {
        keywords.iter().any(|keyword| self.is(keyword))
    }

    /// A keyword or a name, as opposed to a number or a `$1` parameter.
    pub(super) fn is_name(&self) -> bool {
        let first = self.start[0];
        self.len > 0 && (first.is_ascii_alphabetic() || first == b'_' || first >= 0x80)
    }
}

/// Passes SQL through and finds where each statement starts and ends.
/// Statements that start with `INSERT`, `REPLACE` (MySQL and SQLite) or
/// `COPY` (PostgreSQL) open a data block. Statements that may write data in
/// another form are held whole, so that they can be checked before any byte
/// is written. Keywords inside literals, comments, identifiers or the body
/// of another statement are ignored.
pub struct NormalState {
    dialect: SqlDialect,
    lexer: Lexer,
    framer: Framer,
    pending: VecDeque<u8>,
    held: Vec<u8>,
    handling: Handling,
    phase: StatementPhase,
    word: Option<Word>,
    /// MySQL: the bytes of a `DELIMITER` line after the keyword.
    delimiter_line: Vec<u8>,
    /// Bytes of a UTF-8 BOM matched so far; `None` once past the input start.
    bom_matched: Option<usize>,
    max_bytes: usize,
    /// Whether the next byte starts a line.
    line_start: bool,
    /// A client command whose name is being read: its prefix (`\` or `.`)
    /// and name, up to the first space.
    command: Option<Vec<u8>>,
    /// Whether client commands that dump tools do not write stop the run.
    refuse_commands: bool,
    /// MySQL `USE`: the mysql client also ends it at the end of its line.
    ends_at_line_end: bool,
}

enum Outcome {
    /// A data keyword of the given length ended at this byte.
    Data(DataKeyword, usize),
    /// The first word, of the given length, ended at this byte, and the
    /// statement is held from it on.
    Hold(usize),
    End,
    /// A client command, given as written, to be refused.
    Refuse(Vec<u8>),
}

impl NormalState {
    /// `max_bytes` bounds a statement held whole.
    pub fn new(dialect: SqlDialect, max_bytes: usize) -> Self {
        Self {
            dialect,
            lexer: Lexer::new(dialect),
            framer: Framer::new(dialect),
            pending: VecDeque::with_capacity(LOOKBEHIND + 1),
            held: Vec::new(),
            handling: Handling::Stream,
            phase: StatementPhase::Start,
            word: None,
            delimiter_line: Vec::new(),
            bom_matched: None,
            max_bytes,
            line_start: true,
            command: None,
            refuse_commands: false,
            ends_at_line_end: false,
        }
    }

    /// Reports the client commands that dump tools do not write, instead of
    /// passing them through: psql meta-commands other than `\connect`,
    /// `\restrict` and `\unrestrict`, every mysql command but `DELIMITER`,
    /// and sqlite3 dot-commands. Their effect (variables, other files,
    /// another delimiter or encoding) is not modelled.
    pub fn refusing_commands(mut self, refuse: bool) -> Self {
        self.refuse_commands = refuse;
        self
    }

    /// State for the very beginning of the input, where a UTF-8 BOM may appear.
    pub fn at_input_start(dialect: SqlDialect, max_bytes: usize) -> Self {
        Self {
            bom_matched: Some(0),
            ..Self::new(dialect, max_bytes)
        }
    }

    pub fn process_byte(&mut self, byte: u8) -> NormalEvent {
        let streaming = self.handling == Handling::Stream;
        if streaming {
            self.pending.push_back(byte);
        } else {
            self.held.push(byte);
        }

        match self.advance(byte) {
            Some(Outcome::Refuse(command)) => return NormalEvent::ClientCommand(command),
            Some(Outcome::Data(keyword, len)) => {
                let start = self.pending.len() - len - 1;
                return NormalEvent::StartInsertHeader {
                    before: self.pending.drain(..start).collect(),
                    initial_bytes: self.pending.drain(..).collect(),
                    keyword,
                    delimiter: byte,
                };
            }
            Some(Outcome::Hold(len)) => {
                // The statement is held from its first word; what came
                // before it is written now.
                let start = self.pending.len() - len - 1;
                let before: Vec<u8> = self.pending.drain(..start).collect();
                self.held = self.pending.drain(..).collect();
                return if before.is_empty() {
                    NormalEvent::Continue
                } else {
                    NormalEvent::Released(before)
                };
            }
            Some(Outcome::End) => match std::mem::replace(&mut self.handling, Handling::Stream) {
                Handling::Hold => return NormalEvent::Statement(std::mem::take(&mut self.held)),
                Handling::AfterCreate => {
                    return NormalEvent::Released(std::mem::take(&mut self.held));
                }
                Handling::Stream => {}
            },
            None => {}
        }

        if !streaming && self.handling == Handling::Stream {
            // `CREATE` of something other than a table.
            return NormalEvent::Released(std::mem::take(&mut self.held));
        }

        if self.handling != Handling::Stream {
            return if self.held.len() > self.max_bytes {
                NormalEvent::TooLarge
            } else {
                NormalEvent::Continue
            };
        }
        if self.pending.len() > LOOKBEHIND
            && let Some(oldest) = self.pending.pop_front()
        {
            return NormalEvent::DefaultStatement(oldest);
        }
        NormalEvent::Continue
    }

    /// What is still held when the input ends: a statement held for
    /// checking, or bytes to be written as they are.
    pub fn finish(&mut self) -> Remainder {
        let mut rest: Vec<u8> = self.pending.drain(..).collect();
        rest.append(&mut self.held);
        if self.handling == Handling::Hold {
            Remainder::Statement(rest)
        } else {
            Remainder::Bytes(rest)
        }
    }

    fn advance(&mut self, byte: u8) -> Option<Outcome> {
        if self.skip_bom(byte) {
            return None;
        }
        let at_line_start = std::mem::replace(&mut self.line_start, byte == b'\n');

        // A client command's name ends at the first space or control byte.
        if let Some(command) = self.command.as_mut() {
            if byte > b' ' {
                if command.len() < MAX_COMMAND {
                    command.push(byte);
                }
            } else {
                let command = self.command.take().unwrap_or_default();
                if self.refuse_commands && !dump_tool_command(self.dialect, &command) {
                    return Some(Outcome::Refuse(command));
                }
            }
        }

        match self.phase {
            StatementPhase::MetaCommand => {
                // The line is not SQL: its quotes must not reach the lexer.
                // psql reads another meta-command, or SQL after `\\`, at an
                // unquoted backslash.
                if byte == b'\\' && self.dialect == SqlDialect::Postgres && self.command.is_none() {
                    self.command = Some(vec![byte]);
                }
                if byte == b'\n' {
                    self.phase = StatementPhase::Start;
                }
                return None;
            }
            StatementPhase::DelimiterLine => {
                if byte == b'\n' {
                    self.set_delimiter();
                    self.phase = StatementPhase::Start;
                } else if self.delimiter_line.len() < MAX_DELIMITER_LINE {
                    self.delimiter_line.push(byte);
                }
                return None;
            }
            _ => {}
        }

        let class = self.lexer.feed(byte);
        let in_word = class == ByteClass::Code && is_ident_byte(byte);
        let mut held_from = None;

        // psql and mysql read an unquoted backslash as a client command; a
        // psql one that starts a statement is read with its line, below.
        let statement_start = matches!(
            self.phase,
            StatementPhase::Start | StatementPhase::MaybeComment
        );
        if class == ByteClass::Code
            && byte == b'\\'
            && self.command.is_none()
            && match self.dialect {
                SqlDialect::Postgres => !statement_start,
                SqlDialect::Mysql => true,
                SqlDialect::Sqlite => false,
            }
        {
            self.command = Some(vec![byte]);
        }

        if !in_word && let Some(word) = self.word.take() {
            self.framer.word(&word);
            if self.phase == StatementPhase::FirstWord {
                self.phase = StatementPhase::Body;
                match self.first_word(&word, byte, class) {
                    FirstWord::Data(keyword) => return Some(Outcome::Data(keyword, word.len)),
                    FirstWord::Delimiter => {
                        self.phase = StatementPhase::DelimiterLine;
                        self.delimiter_line.clear();
                        return None;
                    }
                    FirstWord::Command if self.refuse_commands => {
                        let len = word.len.min(word.start.len());
                        return Some(Outcome::Refuse(word.start[..len].to_vec()));
                    }
                    FirstWord::Command => {}
                    FirstWord::Handle(Handling::Stream) => {}
                    FirstWord::Handle(handling) => {
                        self.handling = handling;
                        held_from = Some(word.len);
                        // mysql runs `use db` without a `;` as a client command.
                        self.ends_at_line_end = self.dialect == SqlDialect::Mysql && word.is("use");
                    }
                }
            } else if self.handling == Handling::AfterCreate {
                self.handling = after_create(&word);
            }
        }

        if in_word {
            self.word.get_or_insert_with(Word::default).push(byte);
        } else if self.handling == Handling::AfterCreate && !is_separator(byte, class) {
            // `CREATE (` or `CREATE "x"`: not a table.
            self.handling = Handling::Stream;
            held_from = None;
        }

        match self.phase {
            StatementPhase::Start => {
                self.phase = self.after_start(byte, class, at_line_start);
                if self.phase == StatementPhase::MetaCommand && byte != b'#' {
                    self.command = Some(vec![byte]);
                }
            }
            StatementPhase::MaybeComment => {
                self.phase = if matches!(class, ByteClass::CommentStart | ByteClass::VersionEnd) {
                    StatementPhase::Start
                } else if class == ByteClass::Code
                    && byte == b'-'
                    && self.dialect == SqlDialect::Mysql
                {
                    // MySQL: `--` opens a comment only if a space follows.
                    StatementPhase::MaybeComment
                } else {
                    StatementPhase::Body
                };
            }
            _ => {}
        }

        let line_end = self.ends_at_line_end && byte == b'\n' && class != ByteClass::Literal;
        if line_end || self.framer.ends_at(byte, class) {
            self.framer.reset();
            self.phase = StatementPhase::Start;
            self.ends_at_line_end = false;
            // A delimiter made of letters (`DELIMITER $$`) can end a statement
            // inside a word: what follows starts a new one.
            self.word = None;
            return Some(Outcome::End);
        }
        held_from.map(Outcome::Hold)
    }

    fn after_start(&self, byte: u8, class: ByteClass, at_line_start: bool) -> StatementPhase {
        match class {
            ByteClass::Comment | ByteClass::CommentStart | ByteClass::VersionEnd => {
                StatementPhase::Start
            }
            ByteClass::Literal => StatementPhase::Body,
            ByteClass::Code => match byte {
                b'-' | b'/' => StatementPhase::MaybeComment,
                // MySQL: the `*/` of a version comment that a delimiter
                // inside it split from its start (`/*!40000 SELECT 1; */`).
                b'*' if self.dialect == SqlDialect::Mysql => StatementPhase::MaybeComment,
                b'\\' if self.dialect == SqlDialect::Postgres => StatementPhase::MetaCommand,
                // sqlite3 reads a line that starts with `.` as a dot-command,
                // and one that starts with `#` as a comment.
                b'.' | b'#' if self.dialect == SqlDialect::Sqlite && at_line_start => {
                    StatementPhase::MetaCommand
                }
                b if b.is_ascii_whitespace() => StatementPhase::Start,
                b if b.is_ascii_alphabetic() => StatementPhase::FirstWord,
                _ => StatementPhase::Body,
            },
        }
    }

    /// What the first word of a statement makes of it; `byte` ended the word.
    fn first_word(&self, word: &Word, byte: u8, class: ByteClass) -> FirstWord {
        let ends_statement = class == ByteClass::Code && self.framer.ends_single_word(byte);
        if ends_statement {
            // A statement of one word writes nothing.
            return FirstWord::Handle(Handling::Stream);
        }
        let data =
            if word.is("insert") || (word.is("replace") && self.dialect != SqlDialect::Postgres) {
                Some(DataKeyword::Insert)
            } else if word.is("copy") && self.dialect == SqlDialect::Postgres {
                Some(DataKeyword::Copy)
            } else {
                None
            };

        match data {
            // The rows of a block end at `;`, so a custom delimiter is held instead.
            Some(_) if self.framer.custom_delimiter() => FirstWord::Handle(Handling::Hold),
            Some(keyword) => FirstWord::Data(keyword),
            _ if self.dialect == SqlDialect::Mysql
                && word.is("delimiter")
                && (byte == b' ' || byte == b'\t') =>
            {
                FirstWord::Delimiter
            }
            _ if self.dialect == SqlDialect::Mysql && word.is_any(MYSQL_COMMANDS) => {
                FirstWord::Command
            }
            // Statements that may write data, those that change tables,
            // whose column order an INSERT without a column list relies on,
            // and SET, which may change how the rest of the dump is read.
            _ if word.is_any(&[
                "update", "delete", "merge", "with", "select", "load", "alter", "drop", "rename",
                "set",
            ]) =>
            {
                FirstWord::Handle(Handling::Hold)
            }
            // MySQL `USE`, which sets the database of the table names after it.
            _ if self.dialect == SqlDialect::Mysql && word.is("use") => {
                FirstWord::Handle(Handling::Hold)
            }
            _ if word.is("create") => FirstWord::Handle(Handling::AfterCreate),
            _ => FirstWord::Handle(Handling::Stream),
        }
    }

    /// Reads the argument of a `DELIMITER` line, as the mysql client does: up
    /// to the first space, or between quotes. An empty delimiter, or one with
    /// a backslash, is refused by mysql and leaves the delimiter unchanged.
    fn set_delimiter(&mut self) {
        let line = std::mem::take(&mut self.delimiter_line);
        let argument = line.trim_ascii_start();
        let delimiter: Vec<u8> = match argument.split_first() {
            Some((&quote, rest)) if matches!(quote, b'\'' | b'"' | b'`') => {
                let mut out = Vec::new();
                let mut escaped = false;
                for &byte in rest {
                    if escaped {
                        out.push(byte);
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == quote {
                        break;
                    } else {
                        out.push(byte);
                    }
                }
                out
            }
            _ => argument
                .iter()
                .take_while(|byte| !byte.is_ascii_whitespace())
                .copied()
                .collect(),
        };
        if !delimiter.is_empty() && !delimiter.contains(&b'\\') {
            self.framer.set_delimiter(delimiter);
        }
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

/// The psql meta-commands that pg_dump and pg_dumpall write.
fn dump_tool_command(dialect: SqlDialect, command: &[u8]) -> bool {
    dialect == SqlDialect::Postgres
        && matches!(
            command,
            b"\\connect" | b"\\c" | b"\\restrict" | b"\\unrestrict"
        )
}

/// mysql reads at most this many bytes of a client command line.
const MAX_DELIMITER_LINE: usize = 256;

/// The handling of a statement after `CREATE` and another word: a table is
/// held, the words that may come before `TABLE` keep the decision open, and
/// anything else (`INDEX`, `FUNCTION`, `VIEW`, ...) is streamed.
fn after_create(word: &Word) -> Handling {
    if word.is("table") {
        Handling::Hold
    } else if word.is_any(&[
        "or",
        "replace",
        "global",
        "local",
        "temp",
        "temporary",
        "unlogged",
    ]) {
        Handling::AfterCreate
    } else {
        Handling::Stream
    }
}
