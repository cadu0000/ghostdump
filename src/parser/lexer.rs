//! Byte-level SQL lexer for the parts of a dump outside data blocks.
//!
//! For each byte it answers one question: is this byte SQL code, or part of a
//! string literal, a quoted identifier or a comment? Keyword matching and
//! statement boundaries are left to the parser states that use it.

use crate::parser::state::SqlDialect;

/// What a byte is, as seen by the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteClass {
    /// SQL code, outside literals, quoted identifiers and comments.
    Code,
    /// Part of a string literal or quoted identifier, delimiters included.
    Literal,
    /// Part of a comment, delimiters included.
    Comment,
    /// Second byte of a `--` or `/*` comment opener. The first byte was
    /// reported as `Code`, before the lexer could know it opened a comment.
    CommentStart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LexState {
    Code,
    LineComment,
    BlockComment,
    /// Inside text delimited by the given quote byte: `'`, `"` or `` ` ``.
    Quoted(u8),
    /// After a `$` that may open a PostgreSQL dollar-quoted string.
    DollarTag,
    DollarQuoted,
}

/// Longest dollar-quote tag accepted (PostgreSQL's identifier length limit).
const MAX_DOLLAR_TAG: usize = 63;

pub struct Lexer {
    dialect: SqlDialect,
    state: LexState,
    /// The two previous bytes in code context, most recent first.
    prev: u8,
    prev2: u8,
    /// Whether a backslash escapes the next byte in the current quoted text.
    backslash_escapes: bool,
    escape_next: bool,
    comment_depth: u32,
    comment_prev: u8,
    /// Tag of the current dollar-quoted string, without the `$` signs.
    dollar_tag: Vec<u8>,
    /// How many bytes of the closing `$tag$` have been matched so far.
    dollar_matched: usize,
}

impl Lexer {
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            state: LexState::Code,
            prev: 0,
            prev2: 0,
            backslash_escapes: false,
            escape_next: false,
            comment_depth: 0,
            comment_prev: 0,
            dollar_tag: Vec::new(),
            dollar_matched: 0,
        }
    }

    pub fn feed(&mut self, byte: u8) -> ByteClass {
        match self.state {
            LexState::Code => self.code_byte(byte),
            LexState::LineComment => {
                if byte == b'\n' {
                    self.back_to_code(byte);
                }
                ByteClass::Comment
            }
            LexState::BlockComment => self.block_comment_byte(byte),
            LexState::Quoted(quote) => self.quoted_byte(byte, quote),
            LexState::DollarTag => self.dollar_tag_byte(byte),
            LexState::DollarQuoted => self.dollar_quoted_byte(byte),
        }
    }

    fn code_byte(&mut self, byte: u8) -> ByteClass {
        let class = match byte {
            b'-' if self.prev == b'-' => {
                self.state = LexState::LineComment;
                ByteClass::CommentStart
            }
            b'*' if self.prev == b'/' => {
                self.state = LexState::BlockComment;
                self.comment_depth = 1;
                self.comment_prev = 0;
                ByteClass::CommentStart
            }
            b'#' if self.dialect == SqlDialect::Mysql => {
                self.state = LexState::LineComment;
                ByteClass::Comment
            }
            b'\'' => {
                // `''` closes a literal and reopens it at once: keep its escape mode.
                if self.prev != b'\'' {
                    self.backslash_escapes = self.single_quote_escapes();
                }
                self.state = LexState::Quoted(b'\'');
                ByteClass::Literal
            }
            b'"' => {
                // MySQL reads "..." as a string with backslash escapes; the
                // other dialects read it as a quoted identifier.
                self.backslash_escapes = self.dialect == SqlDialect::Mysql;
                self.state = LexState::Quoted(b'"');
                ByteClass::Literal
            }
            b'`' if self.dialect != SqlDialect::Postgres => {
                self.backslash_escapes = false;
                self.state = LexState::Quoted(b'`');
                ByteClass::Literal
            }
            b'$' if self.dialect == SqlDialect::Postgres && !is_ident_byte(self.prev) => {
                self.dollar_tag.clear();
                self.state = LexState::DollarTag;
                ByteClass::Code
            }
            _ => ByteClass::Code,
        };
        self.prev2 = self.prev;
        self.prev = byte;
        class
    }

    fn single_quote_escapes(&self) -> bool {
        match self.dialect {
            SqlDialect::Mysql => true,
            SqlDialect::Sqlite => false,
            // With standard_conforming_strings (on by default since 9.1 and
            // set by pg_dump), only E'...' strings honor backslash escapes.
            SqlDialect::Postgres => {
                self.prev.eq_ignore_ascii_case(&b'e') && !is_ident_byte(self.prev2)
            }
        }
    }

    fn quoted_byte(&mut self, byte: u8, quote: u8) -> ByteClass {
        if self.escape_next {
            self.escape_next = false;
        } else if byte == b'\\' && self.backslash_escapes {
            self.escape_next = true;
        } else if byte == quote {
            self.back_to_code(byte);
        }
        ByteClass::Literal
    }

    fn block_comment_byte(&mut self, byte: u8) -> ByteClass {
        if self.comment_prev == b'*' && byte == b'/' {
            self.comment_depth -= 1;
            self.comment_prev = 0;
            if self.comment_depth == 0 {
                // The closing `/` must not pair with a following `*`.
                self.back_to_code(0);
            }
        } else if self.comment_prev == b'/' && byte == b'*' && self.dialect == SqlDialect::Postgres
        {
            // Only PostgreSQL nests block comments.
            self.comment_depth += 1;
            self.comment_prev = 0;
        } else {
            self.comment_prev = byte;
        }
        ByteClass::Comment
    }

    fn dollar_tag_byte(&mut self, byte: u8) -> ByteClass {
        if byte == b'$' {
            self.state = LexState::DollarQuoted;
            self.dollar_matched = 0;
            return ByteClass::Literal;
        }

        let allowed = if self.dollar_tag.is_empty() {
            byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
        } else {
            byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
        };

        if allowed && self.dollar_tag.len() < MAX_DOLLAR_TAG {
            self.dollar_tag.push(byte);
            self.prev2 = self.prev;
            self.prev = byte;
            return ByteClass::Code;
        }

        // Not a dollar quote after all (`$1`, `$ `, ...).
        self.state = LexState::Code;
        self.code_byte(byte)
    }

    fn dollar_quoted_byte(&mut self, byte: u8) -> ByteClass {
        // The closing delimiter is `$tag$`, and a tag never contains `$`.
        let tag_len = self.dollar_tag.len();
        let expected = match self.dollar_matched {
            0 => b'$',
            n if n <= tag_len => self.dollar_tag[n - 1],
            _ => b'$',
        };

        if byte == expected {
            self.dollar_matched += 1;
            if self.dollar_matched == tag_len + 2 {
                self.back_to_code(0);
            }
        } else {
            self.dollar_matched = usize::from(byte == b'$');
        }
        ByteClass::Literal
    }

    fn back_to_code(&mut self, prev: u8) {
        self.state = LexState::Code;
        self.prev = prev;
        self.prev2 = 0;
    }
}

/// Bytes that can continue an unquoted identifier or keyword.
pub fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One letter per input byte: `c` code, `l` literal, `m` comment and
    /// `s` for the second byte of a comment opener.
    fn classes(dialect: SqlDialect, input: &[u8]) -> String {
        let mut lexer = Lexer::new(dialect);
        input
            .iter()
            .map(|&byte| match lexer.feed(byte) {
                ByteClass::Code => 'c',
                ByteClass::Literal => 'l',
                ByteClass::Comment => 'm',
                ByteClass::CommentStart => 's',
            })
            .collect()
    }

    #[test]
    fn test_doubled_quote_stays_inside_literal() {
        assert_eq!(classes(SqlDialect::Postgres, b"'it''s' x"), "lllllllcc");
    }

    #[test]
    fn test_postgres_backslash_is_plain_in_standard_strings() {
        assert_eq!(classes(SqlDialect::Postgres, b"'C:\\' x"), "lllllcc");
    }

    #[test]
    fn test_postgres_escape_string_honors_backslash() {
        assert_eq!(classes(SqlDialect::Postgres, b"E'\\'' x"), "cllllcc");
        assert_eq!(classes(SqlDialect::Postgres, b"e'\\'' x"), "cllllcc");
    }

    #[test]
    fn test_escape_mode_survives_doubled_quote() {
        // E'a''\'' is one literal: `''` and `\'` are both escaped quotes.
        assert_eq!(classes(SqlDialect::Postgres, b"E'a''\\'' x"), "clllllllcc");
    }

    #[test]
    fn test_mysql_backslash_escapes_quote() {
        assert_eq!(classes(SqlDialect::Mysql, b"'O\\'B' x"), "llllllcc");
    }

    #[test]
    fn test_sqlite_backslash_is_plain() {
        assert_eq!(classes(SqlDialect::Sqlite, b"'a\\' x"), "llllcc");
    }

    #[test]
    fn test_line_comment() {
        assert_eq!(classes(SqlDialect::Postgres, b"a--b\nc"), "ccsmmc");
    }

    #[test]
    fn test_hash_comment_only_in_mysql() {
        assert_eq!(classes(SqlDialect::Mysql, b"#x\ny"), "mmmc");
        assert_eq!(classes(SqlDialect::Postgres, b"#x"), "cc");
    }

    #[test]
    fn test_block_comments_nest_only_in_postgres() {
        let input = b"/* a /* b */ c */d";
        let postgres = classes(SqlDialect::Postgres, input);
        let mysql = classes(SqlDialect::Mysql, input);
        assert_eq!(&postgres[13..], "mmmmc");
        assert_eq!(&mysql[13..], "ccccc");
    }

    #[test]
    fn test_block_comment_closer_does_not_reopen() {
        assert_eq!(classes(SqlDialect::Postgres, b"/**/*x"), "csmmcc");
    }

    #[test]
    fn test_dollar_quoted_string() {
        assert_eq!(classes(SqlDialect::Postgres, b"$f$ x $f$;"), "cclllllllc");
        assert_eq!(classes(SqlDialect::Postgres, b"$$'$$;"), "cllllc");
    }

    #[test]
    fn test_dollar_quote_ignores_other_tags() {
        let input = b"$a$ $b$ ; $a$;";
        assert_eq!(classes(SqlDialect::Postgres, input).pop(), Some('c'));
        assert_eq!(classes(SqlDialect::Postgres, input).as_bytes()[8], b'l');
    }

    #[test]
    fn test_dollar_sign_that_is_not_a_quote() {
        assert_eq!(classes(SqlDialect::Postgres, b"$1;"), "ccc");
        assert_eq!(classes(SqlDialect::Postgres, b"a$b$c"), "ccccc");
        assert_eq!(classes(SqlDialect::Mysql, b"$$'"), "ccl");
    }

    #[test]
    fn test_double_quoted_identifier_hides_quote() {
        assert_eq!(classes(SqlDialect::Postgres, b"\"it's\" x"), "llllllcc");
    }

    #[test]
    fn test_backticks_outside_postgres() {
        assert_eq!(classes(SqlDialect::Mysql, b"`a'` b"), "llllcc");
        assert_eq!(classes(SqlDialect::Postgres, b"`a` b"), "ccccc");
    }
}
