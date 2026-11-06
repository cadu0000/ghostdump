//! Rows of an `INSERT ... VALUES` statement: `(field, field, ...)`, with
//! string literals quoted and escaped according to the dialect.

use std::io::{self, Write};
use std::ops::Range;

use super::SqlValue;
use crate::parser::lexer::{ByteClass, Lexer};
use crate::parser::state::SqlDialect;

/// Field ranges of a VALUES row, between the outer parentheses. The ranges
/// include the spaces around each field.
pub fn split(row: &[u8], dialect: SqlDialect) -> Vec<Range<usize>> {
    let mut lexer = Lexer::new(dialect);
    let mut fields = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;

    for (i, &byte) in row.iter().enumerate() {
        if lexer.feed(byte) != ByteClass::Code {
            continue;
        }
        match byte {
            b'(' => {
                depth += 1;
                if depth == 1 {
                    start = i + 1;
                }
            }
            b')' => {
                if depth == 1 {
                    fields.push(start..i);
                    return fields;
                }
                depth = depth.saturating_sub(1);
            }
            b',' if depth == 1 => {
                fields.push(start..i);
                start = i + 1;
            }
            _ => {}
        }
    }
    fields
}

/// Decodes one trimmed field.
pub fn decode(field: &[u8], dialect: SqlDialect) -> SqlValue {
    if field.eq_ignore_ascii_case(b"NULL") {
        return SqlValue::Null;
    }
    string_literal(field, dialect)
        .unwrap_or_else(|| SqlValue::Raw(String::from_utf8_lossy(field).into_owned()))
}

/// The value of a string literal, or `None` if `field` is not one.
fn string_literal(field: &[u8], dialect: SqlDialect) -> Option<SqlValue> {
    let (prefix_len, backslash) = match dialect {
        SqlDialect::Postgres if field.len() > 1 && field[0].eq_ignore_ascii_case(&b'e') => {
            (1, true)
        }
        SqlDialect::Postgres | SqlDialect::Sqlite => (0, false),
        SqlDialect::Mysql => (mysql_prefix_len(field), true),
    };
    let body = &field[prefix_len..];

    let quote = *body.first()?;
    let quoted = quote == b'\'' || (quote == b'"' && dialect == SqlDialect::Mysql);
    if !quoted || body.len() < 2 || *body.last()? != quote {
        return None;
    }

    let inner = &body[1..body.len() - 1];
    let mut out = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        let byte = inner[i];
        if byte == quote {
            // A lone quote inside means `field` is not a single literal.
            if inner.get(i + 1) != Some(&quote) {
                return None;
            }
            out.push(quote);
            i += 2;
        } else if byte == b'\\' && backslash && i + 1 < inner.len() {
            out.extend_from_slice(unescape(inner[i + 1], dialect));
            i += 2;
        } else {
            out.push(byte);
            i += 1;
        }
    }
    Some(SqlValue::Text(String::from_utf8_lossy(&out).into_owned()))
}

/// MySQL string prefixes: `N'...'` and character set introducers (`_utf8mb4'...'`).
fn mysql_prefix_len(field: &[u8]) -> usize {
    match field.first() {
        Some(b'n' | b'N') if field.get(1) == Some(&b'\'') => 1,
        Some(b'_') => field
            .iter()
            .position(|&b| b == b'\'' || b == b'"')
            .filter(|&pos| field[1..pos].iter().all(|b| b.is_ascii_alphanumeric()))
            .unwrap_or(0),
        _ => 0,
    }
}

fn unescape(byte: u8, dialect: SqlDialect) -> &'static [u8] {
    match (byte, dialect) {
        (b'n', _) => b"\n",
        (b'r', _) => b"\r",
        (b't', _) => b"\t",
        (b'b', _) => b"\x08",
        (b'0', SqlDialect::Mysql) => b"\0",
        (b'Z', SqlDialect::Mysql) => b"\x1a",
        (b'f', SqlDialect::Postgres) => b"\x0c",
        // MySQL keeps the backslash before the LIKE wildcards.
        (b'%', SqlDialect::Mysql) => b"\\%",
        (b'_', SqlDialect::Mysql) => b"\\_",
        _ => std::slice::from_ref(ESCAPED.get(byte as usize).unwrap_or(&b'?')),
    }
}

/// Every byte value, so `unescape` can return `&'static [u8]` for any byte.
static ESCAPED: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = i as u8;
        i += 1;
    }
    table
};

pub fn encode(value: &SqlValue, dialect: SqlDialect, out: &mut impl Write) -> io::Result<()> {
    match value {
        SqlValue::Null => out.write_all(b"NULL"),
        SqlValue::Raw(text) => out.write_all(text.as_bytes()),
        SqlValue::Text(text) => quote(text, dialect, out),
    }
}

fn quote(text: &str, dialect: SqlDialect, out: &mut impl Write) -> io::Result<()> {
    let bytes = text.as_bytes();
    // PostgreSQL: an E'' string reads the same whatever standard_conforming_strings says.
    let postgres_escape = dialect == SqlDialect::Postgres && bytes.contains(&b'\\');
    out.write_all(if postgres_escape { b"E'" } else { b"'" })?;

    let mut start = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        let escaped: &[u8] = match (byte, dialect) {
            (b'\'', SqlDialect::Mysql) => b"\\'",
            (b'\'', _) => b"''",
            (b'\\', SqlDialect::Mysql) => b"\\\\",
            (b'\\', SqlDialect::Postgres) => b"\\\\",
            (0, SqlDialect::Mysql) => b"\\0",
            (b'\n', SqlDialect::Mysql) => b"\\n",
            (b'\r', SqlDialect::Mysql) => b"\\r",
            (0x1a, SqlDialect::Mysql) => b"\\Z",
            _ => continue,
        };
        out.write_all(&bytes[start..i])?;
        out.write_all(escaped)?;
        start = i + 1;
    }
    out.write_all(&bytes[start..])?;
    out.write_all(b"'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(row: &str, dialect: SqlDialect) -> Vec<&str> {
        split(row.as_bytes(), dialect)
            .into_iter()
            .map(|range| row[range].trim())
            .collect()
    }

    fn encoded(value: &SqlValue, dialect: SqlDialect) -> String {
        let mut out = Vec::new();
        encode(value, dialect, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_split_respects_quotes_nesting_and_comments() {
        assert_eq!(
            fields("(1, 'a,b', f(2, 3), /* x, y */ NULL)", SqlDialect::Postgres),
            ["1", "'a,b'", "f(2, 3)", "/* x, y */ NULL"]
        );
        assert_eq!(
            fields("(1, 'it\\'s, ok', 2)", SqlDialect::Mysql),
            ["1", "'it\\'s, ok'", "2"]
        );
        assert_eq!(fields("('C:\\', 2)", SqlDialect::Postgres), ["'C:\\'", "2"]);
    }

    #[test]
    fn test_decode_by_dialect() {
        let pg = SqlDialect::Postgres;
        assert_eq!(decode(b"NULL", pg), SqlValue::Null);
        assert_eq!(decode(b"42.5", pg), SqlValue::Raw("42.5".into()));
        assert_eq!(decode(b"'O''Hara'", pg), SqlValue::Text("O'Hara".into()));
        assert_eq!(decode(b"'C:\\'", pg), SqlValue::Text("C:\\".into()));
        assert_eq!(decode(b"E'a\\nb'", pg), SqlValue::Text("a\nb".into()));

        let my = SqlDialect::Mysql;
        assert_eq!(decode(b"'O\\'Hara'", my), SqlValue::Text("O'Hara".into()));
        assert_eq!(decode(b"_utf8mb4'x'", my), SqlValue::Text("x".into()));
        assert_eq!(decode(b"N'y'", my), SqlValue::Text("y".into()));

        let lite = SqlDialect::Sqlite;
        assert_eq!(decode(b"'a\\'", lite), SqlValue::Text("a\\".into()));
    }

    #[test]
    fn test_encode_by_dialect() {
        let text = SqlValue::Text("O'Hara \\ x".into());
        assert_eq!(encoded(&text, SqlDialect::Postgres), "E'O''Hara \\\\ x'");
        assert_eq!(encoded(&text, SqlDialect::Mysql), "'O\\'Hara \\\\ x'");
        assert_eq!(encoded(&text, SqlDialect::Sqlite), "'O''Hara \\ x'");
        assert_eq!(encoded(&SqlValue::Raw("7".into()), SqlDialect::Mysql), "7");
        assert_eq!(encoded(&SqlValue::Null, SqlDialect::Sqlite), "NULL");
    }

    #[test]
    fn test_encode_then_decode_round_trips() {
        let samples = [
            "plain",
            "O'Hara",
            "back\\slash",
            "new\nline",
            "\\'mix''",
            "",
        ];
        for dialect in [SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Sqlite] {
            for sample in samples {
                let value = SqlValue::Text(sample.into());
                let text = encoded(&value, dialect);
                assert_eq!(
                    decode(text.as_bytes(), dialect),
                    value,
                    "{dialect:?} {text}"
                );
            }
        }
    }
}
