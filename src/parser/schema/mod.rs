//! The SQL text that GhostDump reads whole: the header of a data block,
//! a statement held for checking, and the settings it changes. They share
//! the tokens and names read here.

use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};
use crate::parser::state::SqlDialect;

mod header;
mod settings;
mod statement;

pub use header::{HeaderMetadata, extract_metadata};
pub use settings::{SessionSetting, SettingValue};
pub use statement::{Statement, TableChange};

/// A table as a statement names it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableName {
    /// The schema (or, in MySQL, the database) written before the table name.
    pub schema: Option<String>,
    pub table: String,
    /// The schema and table as the database tells them apart: case is
    /// folded where the dialect folds it (PostgreSQL unquoted names and every
    /// SQLite name, in ASCII), and kept where it does not (quoted PostgreSQL
    /// names, MySQL table names). Two spellings of one table that differ in
    /// a way the database does not ignore give two keys.
    pub key: String,
}

/// A token of SQL text from which comments were removed.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// An unquoted keyword or name.
    Word(String),
    /// Quoted text without its quotes: a quoted name, or a string. `quote`
    /// is the opening byte: `'`, `"`, `` ` ``, `[` or `$`.
    Quoted { quote: u8, text: String },
    /// A number, or a word that starts like one (`$1`).
    Number,
    /// Any other byte that is not white space.
    Symbol(u8),
}

impl Token {
    fn is_word(&self, keyword: &str) -> bool {
        matches!(self, Token::Word(word) if word.eq_ignore_ascii_case(keyword))
    }

    /// A name: an unquoted word, or a quoted name. A `"..."` is taken as a
    /// name in MySQL too (ANSI_QUOTES); SQLite also takes `'...'` as a
    /// name where a name is expected.
    fn name(&self, dialect: SqlDialect) -> Option<&str> {
        match self {
            Token::Word(word) => Some(word),
            Token::Quoted { quote, text } => match quote {
                b'"' | b'`' | b'[' => Some(text),
                b'\'' if dialect == SqlDialect::Sqlite => Some(text),
                _ => None,
            },
            _ => None,
        }
    }

    /// A string or a number, written as data.
    fn is_value(&self, dialect: SqlDialect) -> bool {
        match self {
            Token::Number => true,
            Token::Quoted { quote, .. } => {
                matches!(quote, b'\'' | b'$') || (*quote == b'"' && dialect == SqlDialect::Mysql)
            }
            _ => false,
        }
    }
}

/// Splits SQL into tokens, without comments.
fn tokenize(sql: &[u8], dialect: SqlDialect) -> Vec<Token> {
    let code = without_comments(sql, dialect);
    let mut lexer = Lexer::new(dialect);
    let mut tokens = Vec::new();
    let mut word: Vec<u8> = Vec::new();
    // Quoted text being read: its opening byte and content.
    let mut quoted: Option<(u8, Vec<u8>)> = None;
    // The quote that closed a token at the byte before: a doubled quote
    // (`'it''s'`) reopens that token.
    let mut just_closed: Option<u8> = None;

    for &byte in &code {
        let class = lexer.feed(byte);
        let closed = just_closed.take();

        if class != ByteClass::Literal {
            if is_ident_byte(byte) {
                word.push(byte);
                continue;
            }
            push_word(&mut word, &mut tokens);
            if !byte.is_ascii_whitespace() {
                tokens.push(Token::Symbol(byte));
            }
            continue;
        }

        if let Some((quote, text)) = quoted.as_mut() {
            if lexer.in_quotes() {
                text.push(byte);
            } else {
                let token = Token::Quoted {
                    quote: *quote,
                    text: String::from_utf8_lossy(text).into_owned(),
                };
                tokens.push(token);
                quoted = None;
                just_closed = Some(byte);
            }
            continue;
        }

        if word.first() == Some(&b'$') {
            // The tag of a dollar quote, read as a word, is not a name.
            word.clear();
            quoted = Some((b'$', Vec::new()));
            continue;
        }
        push_word(&mut word, &mut tokens);
        quoted = match (closed, tokens.last()) {
            (Some(quote), Some(Token::Quoted { quote: last, text }))
                if quote == byte && *last == byte =>
            {
                let mut text = text.clone().into_bytes();
                text.push(byte);
                tokens.pop();
                Some((byte, text))
            }
            _ => Some((byte, Vec::new())),
        };
    }

    push_word(&mut word, &mut tokens);
    if let Some((quote, text)) = quoted {
        tokens.push(Token::Quoted {
            quote,
            text: String::from_utf8_lossy(&text).into_owned(),
        });
    }
    tokens
}

/// Ends the word being read, if any.
fn push_word(word: &mut Vec<u8>, tokens: &mut Vec<Token>) {
    let Some(&first) = word.first() else {
        return;
    };
    tokens.push(if first.is_ascii_digit() || first == b'$' {
        Token::Number
    } else {
        Token::Word(String::from_utf8_lossy(word).into_owned())
    });
    word.clear();
}

/// A part of a qualified name, and whether it was quoted.
struct Part {
    text: String,
    quoted: bool,
}

impl Part {
    fn of(token: &Token, dialect: SqlDialect) -> Option<Self> {
        Some(Part {
            text: token.name(dialect)?.to_string(),
            quoted: matches!(token, Token::Quoted { .. }),
        })
    }
}

/// Reads a name made of parts separated by dots, from `tokens[*at]`.
fn qualified_parts(tokens: &[Token], at: &mut usize, dialect: SqlDialect) -> Option<Vec<Part>> {
    let mut parts = vec![Part::of(tokens.get(*at)?, dialect)?];
    *at += 1;
    while matches!(tokens.get(*at), Some(Token::Symbol(b'.'))) {
        parts.push(Part::of(tokens.get(*at + 1)?, dialect)?);
        *at += 2;
    }
    Some(parts)
}

fn qualified_name(tokens: &[Token], at: &mut usize, dialect: SqlDialect) -> Option<Vec<String>> {
    qualified_parts(tokens, at, dialect)
        .map(|parts| parts.into_iter().map(|part| part.text).collect())
}

/// Reads a table name from `tokens[*at]`.
fn table_name(tokens: &[Token], at: &mut usize, dialect: SqlDialect) -> Option<TableName> {
    TableName::from_parts(&qualified_parts(tokens, at, dialect)?, dialect)
}

impl TableName {
    fn from_parts(parts: &[Part], dialect: SqlDialect) -> Option<Self> {
        let (schema, table) = match parts {
            [table] => (None, table),
            // `database.schema.table`: the schema is the last qualifier.
            [.., schema, table] if parts.len() <= 3 => (Some(schema), table),
            _ => return None,
        };
        let fold = |part: &Part| match dialect {
            // PostgreSQL folds unquoted names, in ASCII for a UTF-8 database
            // (`downcase_identifier` in `src/backend/parser/scansup.c`).
            SqlDialect::Postgres if !part.quoted => part.text.to_ascii_lowercase(),
            SqlDialect::Postgres | SqlDialect::Mysql => part.text.clone(),
            SqlDialect::Sqlite => part.text.to_ascii_lowercase(),
        };
        // A name cannot hold a NUL byte in any of the three dialects.
        let key = match schema {
            Some(schema) => format!("{}\0{}", fold(schema), fold(table)),
            None => fold(table),
        };
        Some(TableName {
            schema: schema.map(|schema| schema.text.clone()),
            table: table.text.clone(),
            key,
        })
    }
}

/// Reads `(name, name, ...)` from `tokens[*at]`, which must be `(`.
fn name_list(tokens: &[Token], at: &mut usize, dialect: SqlDialect) -> Option<Vec<String>> {
    let mut names = Vec::new();
    *at += 1;
    loop {
        names.push(tokens.get(*at)?.name(dialect)?.to_string());
        *at += 1;
        match tokens.get(*at)? {
            Token::Symbol(b',') => *at += 1,
            Token::Symbol(b')') => {
                *at += 1;
                return Some(names);
            }
            _ => return None,
        }
    }
}

/// `tokens` split at the commas outside parentheses, without a final `;`.
fn split_outside_parentheses(tokens: &[Token]) -> Vec<&[Token]> {
    let tokens = match tokens.last() {
        Some(Token::Symbol(b';')) => &tokens[..tokens.len() - 1],
        _ => tokens,
    };
    let mut depth = 0u32;
    let mut start = 0;
    let mut parts = Vec::new();
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::Symbol(b'(') => depth += 1,
            Token::Symbol(b')') => depth = depth.saturating_sub(1),
            Token::Symbol(b',') if depth == 0 => {
                parts.push(&tokens[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&tokens[start..]);
    parts
}

/// The tokens outside parentheses.
fn outside_parentheses(tokens: &[Token]) -> Vec<&Token> {
    let mut depth = 0u32;
    let mut outside = Vec::new();
    for token in tokens {
        match token {
            Token::Symbol(b'(') => depth += 1,
            Token::Symbol(b')') => depth = depth.saturating_sub(1),
            _ if depth == 0 => outside.push(token),
            _ => {}
        }
    }
    outside
}

/// Whether SQL holds a string or a number, such as the values that an
/// `ON DUPLICATE KEY UPDATE` or `ON CONFLICT DO UPDATE` clause writes.
pub fn has_values(sql: &[u8], dialect: SqlDialect) -> bool {
    tokenize(sql, dialect)
        .iter()
        .any(|token| token.is_value(dialect))
}

/// Guesses the dialect from the first bytes of a dump, using the comments
/// and settings that pg_dump, mysqldump and SQLite's `.dump` write at the top.
pub fn detect_dialect(prefix: &[u8]) -> Option<SqlDialect> {
    let text = String::from_utf8_lossy(prefix);
    let has = |marker: &str| text.contains(marker);

    if has("PostgreSQL database dump") || has("SET standard_conforming_strings") {
        Some(SqlDialect::Postgres)
    } else if has("MySQL dump") || has("MariaDB dump") || has("/*!40101") {
        Some(SqlDialect::Mysql)
    } else if has("PRAGMA foreign_keys") || has("BEGIN TRANSACTION;") {
        Some(SqlDialect::Sqlite)
    } else {
        None
    }
}

/// The header with every comment replaced by spaces.
fn without_comments(header: &[u8], dialect: SqlDialect) -> Vec<u8> {
    let mut lexer = Lexer::new(dialect);
    let mut out = Vec::with_capacity(header.len());

    for &byte in header {
        match lexer.feed(byte) {
            ByteClass::Comment => out.push(b' '),
            ByteClass::CommentStart | ByteClass::VersionEnd => {
                // The bytes of the opener or closer before this one were
                // copied as code: both dashes of MySQL's `-- `, else one.
                let copied = if dialect == SqlDialect::Mysql && byte != b'*' && byte != b'/' {
                    2
                } else {
                    1
                };
                let start = out.len().saturating_sub(copied);
                out[start..].fill(b' ');
                out.push(b' ');
            }
            _ => out.push(byte),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_in_a_clause() {
        let (pg, mysql) = (SqlDialect::Postgres, SqlDialect::Mysql);
        assert!(has_values(b"ON DUPLICATE KEY UPDATE a = 'x';", mysql));
        assert!(has_values(b"ON DUPLICATE KEY UPDATE a = \"x\";", mysql));
        assert!(has_values(b"ON CONFLICT (a) DO UPDATE SET b = 2;", pg));
        assert!(has_values(b"ON CONFLICT DO UPDATE SET b = $$x$$;", pg));
        assert!(!has_values(
            b"ON CONFLICT (\"a\") DO UPDATE SET b = EXCLUDED.b2;",
            pg
        ));
        assert!(!has_values(
            b"ON DUPLICATE KEY UPDATE `a` = VALUES(`a`); -- 'x'",
            mysql
        ));
        assert!(!has_values(b"ON CONFLICT DO NOTHING;", pg));
        // MySQL reads `--1` as two minus signs and `-- 1` as a comment.
        assert!(has_values(b"--1;", mysql));
        assert!(!has_values(b"-- 1\n;", mysql));
        assert!(!has_values(b"--1;", pg));
    }

    #[test]
    fn test_detect_dialect() {
        assert_eq!(
            detect_dialect(b"--\n-- PostgreSQL database dump\n--\n"),
            Some(SqlDialect::Postgres)
        );
        assert_eq!(
            detect_dialect(b"-- MySQL dump 10.13  Distrib 8.0.36\n"),
            Some(SqlDialect::Mysql)
        );
        assert_eq!(
            detect_dialect(b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n"),
            Some(SqlDialect::Sqlite)
        );
        assert_eq!(detect_dialect(b"INSERT INTO t VALUES (1);"), None);
    }
}
