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
    /// A statement held for checking, without its end (see
    /// [`SqlEvent::Statement`](crate::parser::SqlEvent::Statement)).
    Statement(Vec<u8>),
    /// The input ended inside a data block, before its terminator. When
    /// `partial_row` is true, an incomplete row was dropped: it cannot be
    /// masked, so it must not be written. `footer` holds what came after the
    /// last row (a clause such as `ON CONFLICT ...`), to be checked like a
    /// footer before it is written.
    UnterminatedData { partial_row: bool, footer: Vec<u8> },
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
struct Word {
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
    fn is(&self, keyword: &str) -> bool {
        self.len == keyword.len() && self.start.get(..self.len) == Some(keyword.as_bytes())
    }

    fn is_any(&self, keywords: &[&str]) -> bool {
        keywords.iter().any(|keyword| self.is(keyword))
    }

    /// A keyword or a name, as opposed to a number or a `$1` parameter.
    fn is_name(&self) -> bool {
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

        if self.framer.ends_at(byte, class) {
            self.framer.reset();
            self.phase = StatementPhase::Start;
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

/// psql's record of the first words of a statement outside parentheses.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
enum Lead {
    #[default]
    Other,
    Create,
    Or,
    Replace,
    Routine,
}

impl Lead {
    fn of(word: &Word) -> Self {
        if word.is("create") {
            Lead::Create
        } else if word.is("or") {
            Lead::Or
        } else if word.is("replace") {
            Lead::Replace
        } else if word.is_any(&["function", "procedure"]) {
            Lead::Routine
        } else {
            Lead::Other
        }
    }
}

/// Tokens and states of `sqlite3_complete`, numbered as in SQLite's
/// `src/complete.c`. White space (token 1) never changes the state, so it is
/// not fed.
const SQLITE_SEMI: usize = 0;
const SQLITE_OTHER: usize = 2;
const SQLITE_EXPLAIN: usize = 3;
const SQLITE_CREATE: usize = 4;
const SQLITE_TEMP: usize = 5;
const SQLITE_TRIGGER: usize = 6;
const SQLITE_END: usize = 7;
const SQLITE_START: u8 = 1;
const SQLITE_TRANSITIONS: [[u8; 8]; 8] = [
    // SEMI WS OTHER EXPLAIN CREATE TEMP TRIGGER END
    [1, 0, 2, 3, 4, 2, 2, 2], // INVALID
    [1, 1, 2, 3, 4, 2, 2, 2], // START
    [1, 2, 2, 2, 2, 2, 2, 2], // NORMAL
    [1, 3, 3, 2, 4, 2, 2, 2], // EXPLAIN
    [1, 4, 2, 2, 2, 4, 5, 2], // CREATE
    [6, 5, 5, 5, 5, 5, 5, 5], // TRIGGER
    [6, 6, 5, 5, 5, 5, 5, 7], // SEMI
    [1, 7, 5, 5, 5, 5, 5, 5], // END
];

/// Finds where statements end, as the client that loads the dump does:
///
/// * psql ends a statement at a `;` outside parentheses and outside the
///   `BEGIN ... END` body of `CREATE [OR REPLACE] FUNCTION | PROCEDURE`, in
///   which it also counts `CASE ... END` (`psqlscan_track_identifier` in
///   PostgreSQL's `src/fe_utils/psqlscan.l`).
/// * mysql ends a statement at its delimiter, `;` until a `DELIMITER`
///   command changes it, anywhere outside quoted text (MySQL Reference
///   Manual, "mysql Client Commands").
/// * sqlite3 ends a statement at a `;`, except in `CREATE TRIGGER`, which
///   ends at `; END ;` (`sqlite3_complete` in SQLite's `src/complete.c`).
struct Framer {
    dialect: SqlDialect,
    paren_depth: u32,
    begin_depth: u32,
    /// psql: the first four words outside parentheses.
    lead: [Lead; 4],
    words: usize,
    /// sqlite3: the state of `sqlite3_complete`.
    sqlite: u8,
    /// sqlite3: a `-` or `/` not yet counted, since it may open a comment.
    sqlite_dash: bool,
    /// mysql: the statement delimiter set by `DELIMITER`; `None` for `;`.
    delimiter: Option<Vec<u8>>,
    /// mysql: the last bytes of code, compared with a longer delimiter.
    recent: VecDeque<u8>,
}

impl Framer {
    fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            paren_depth: 0,
            begin_depth: 0,
            lead: [Lead::Other; 4],
            words: 0,
            sqlite: SQLITE_START,
            sqlite_dash: false,
            delimiter: None,
            recent: VecDeque::new(),
        }
    }

    fn custom_delimiter(&self) -> bool {
        self.delimiter.is_some()
    }

    /// Whether `byte`, read in code right after the first word, ends the
    /// statement.
    fn ends_single_word(&self, byte: u8) -> bool {
        match &self.delimiter {
            None => byte == b';',
            Some(delimiter) => delimiter == &[byte],
        }
    }

    fn set_delimiter(&mut self, delimiter: Vec<u8>) {
        self.delimiter = (delimiter != b";").then_some(delimiter);
        self.recent.clear();
    }

    /// Takes a word that just ended, before the byte that ended it.
    fn word(&mut self, word: &Word) {
        match self.dialect {
            SqlDialect::Postgres if self.paren_depth == 0 && word.is_name() => {
                if let Some(slot) = self.lead.get_mut(self.words) {
                    *slot = Lead::of(word);
                }
                self.words += 1;
                if self.in_create_routine() {
                    // CASE also ends with END; it counts only inside BEGIN.
                    if word.is("begin") || (word.is("case") && self.begin_depth >= 1) {
                        self.begin_depth += 1;
                    } else if word.is("end") && self.begin_depth > 0 {
                        self.begin_depth -= 1;
                    }
                }
            }
            SqlDialect::Sqlite => {
                self.sqlite_flush_dash();
                let token = if word.is("create") {
                    SQLITE_CREATE
                } else if word.is_any(&["temp", "temporary"]) {
                    SQLITE_TEMP
                } else if word.is("trigger") {
                    SQLITE_TRIGGER
                } else if word.is("end") {
                    SQLITE_END
                } else if word.is("explain") {
                    SQLITE_EXPLAIN
                } else {
                    SQLITE_OTHER
                };
                self.sqlite_step(token);
            }
            _ => {}
        }
    }

    /// Whether `byte` ends the statement.
    fn ends_at(&mut self, byte: u8, class: ByteClass) -> bool {
        match self.dialect {
            SqlDialect::Postgres => {
                if class == ByteClass::Code {
                    match byte {
                        b'(' => self.paren_depth += 1,
                        b')' => self.paren_depth = self.paren_depth.saturating_sub(1),
                        b';' => return self.paren_depth == 0 && self.begin_depth == 0,
                        _ => {}
                    }
                }
                false
            }
            SqlDialect::Mysql => {
                if class != ByteClass::Code {
                    self.recent.clear();
                    return false;
                }
                let Some(delimiter) = &self.delimiter else {
                    return byte == b';';
                };
                self.recent.push_back(byte);
                if self.recent.len() > delimiter.len() {
                    self.recent.pop_front();
                }
                self.recent.iter().eq(delimiter.iter())
            }
            SqlDialect::Sqlite => self.sqlite_byte(byte, class),
        }
    }

    /// Forgets the statement that ended; the mysql delimiter stays.
    fn reset(&mut self) {
        self.paren_depth = 0;
        self.begin_depth = 0;
        self.lead = [Lead::Other; 4];
        self.words = 0;
        self.recent.clear();
    }

    /// psql's test for `CREATE [OR REPLACE] {FUNCTION | PROCEDURE}`.
    fn in_create_routine(&self) -> bool {
        matches!(
            self.lead,
            [Lead::Create, Lead::Routine, ..]
                | [Lead::Create, Lead::Or, Lead::Replace, Lead::Routine]
        )
    }

    fn sqlite_byte(&mut self, byte: u8, class: ByteClass) -> bool {
        if class == ByteClass::CommentStart {
            // The `-` or `/` before opened a comment, which counts as space.
            self.sqlite_dash = false;
            return false;
        }
        self.sqlite_flush_dash();
        let token = match class {
            ByteClass::Comment => return false,
            ByteClass::Literal => SQLITE_OTHER,
            _ => match byte {
                b';' => SQLITE_SEMI,
                b'-' | b'/' => {
                    self.sqlite_dash = true;
                    return false;
                }
                // Words are counted whole, when they end.
                b if b.is_ascii_whitespace() || is_ident_byte(b) => return false,
                _ => SQLITE_OTHER,
            },
        };
        self.sqlite_step(token);
        token == SQLITE_SEMI && self.sqlite == SQLITE_START
    }

    fn sqlite_flush_dash(&mut self) {
        if std::mem::take(&mut self.sqlite_dash) {
            self.sqlite_step(SQLITE_OTHER);
        }
    }

    fn sqlite_step(&mut self, token: usize) {
        self.sqlite = SQLITE_TRANSITIONS[usize::from(self.sqlite)][token];
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
            trailer: Vec::new(),
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

/// Whitespace, comments, and a `-` or `/` that may open a comment.
fn is_separator(byte: u8, class: ByteClass) -> bool {
    match class {
        ByteClass::Comment | ByteClass::CommentStart | ByteClass::VersionEnd => true,
        ByteClass::Literal => false,
        ByteClass::Code => byte.is_ascii_whitespace() || byte == b'-' || byte == b'/',
    }
}
