//! Where a statement ends in each dialect: at the delimiter outside
//! parentheses and routine bodies, or after `END` for a SQLite trigger.

use std::collections::VecDeque;

use super::SqlDialect;
use super::normal::Word;
use crate::parser::lexer::{ByteClass, is_ident_byte};

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
pub(super) struct Framer {
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
    pub(super) fn new(dialect: SqlDialect) -> Self {
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

    pub(super) fn custom_delimiter(&self) -> bool {
        self.delimiter.is_some()
    }

    /// Whether `byte`, read in code right after the first word, ends the
    /// statement.
    pub(super) fn ends_single_word(&self, byte: u8) -> bool {
        match &self.delimiter {
            None => byte == b';',
            Some(delimiter) => delimiter == &[byte],
        }
    }

    pub(super) fn set_delimiter(&mut self, delimiter: Vec<u8>) {
        self.delimiter = (delimiter != b";").then_some(delimiter);
        self.recent.clear();
    }

    /// Takes a word that just ended, before the byte that ended it.
    pub(super) fn word(&mut self, word: &Word) {
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
    pub(super) fn ends_at(&mut self, byte: u8, class: ByteClass) -> bool {
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
    pub(super) fn reset(&mut self) {
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
