use regex::Regex;
use std::sync::OnceLock;

use crate::parser::lexer::{ByteClass, Lexer};
use crate::parser::state::SqlDialect;

/// Names read from the header of a data block.
#[derive(Debug, Clone, PartialEq)]
pub struct HeaderMetadata {
    pub table: String,
    /// `None` when the statement has no column list.
    pub columns: Option<Vec<String>>,
}

/// Reads the table and column names of a data block header. Comments are
/// removed first, so `INSERT /* hint */ INTO t` still names `t`.
pub fn extract_metadata(header_bytes: &[u8], dialect: SqlDialect) -> Option<HeaderMetadata> {
    let header = without_comments(header_bytes, dialect);
    let header_str = String::from_utf8_lossy(&header);

    static INSERT_RE: OnceLock<Regex> = OnceLock::new();
    static COPY_RE: OnceLock<Regex> = OnceLock::new();

    // INSERT [OR action] [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO]
    // and REPLACE [INTO], with VALUES or MySQL's synonym VALUE.
    let insert_re = INSERT_RE.get_or_init(|| {
        Regex::new(
            r"(?i)(?:INSERT|REPLACE)\s+(?:OR\s+\w+\s+)?(?:(?:LOW_PRIORITY|DELAYED|HIGH_PRIORITY|IGNORE)\s+)*(?:INTO\s+)?(?P<table>[^\s\(]+)(?:\s*\((?P<cols>[^\)]+)\))?\s*VALUES?\b",
        )
        .unwrap()
    });

    let copy_re = COPY_RE.get_or_init(|| {
        Regex::new(r"(?i)COPY\s+(?P<table>[^\s\(]+)(?:\s*\((?P<cols>[^\)]+)\))?\s*FROM\s+STDIN;")
            .unwrap()
    });

    let process_match = |caps: regex::Captures| -> HeaderMetadata {
        let raw_table = caps.name("table").unwrap().as_str();

        let table = raw_table
            .rsplit('.')
            .next()
            .unwrap_or(raw_table)
            .trim_matches(|c| c == '"' || c == '`' || c == '\'')
            .to_string();

        let columns = caps.name("cols").map(|m| {
            m.as_str()
                .split(',')
                .map(|s| {
                    s.trim_matches(|c: char| c == '"' || c == '`' || c == '\'' || c.is_whitespace())
                        .to_string()
                })
                .collect()
        });

        HeaderMetadata { table, columns }
    };

    if let Some(caps) = insert_re.captures(&header_str) {
        return Some(process_match(caps));
    }

    if let Some(caps) = copy_re.captures(&header_str) {
        return Some(process_match(caps));
    }

    None
}

/// The header with every comment replaced by spaces.
fn without_comments(header: &[u8], dialect: SqlDialect) -> Vec<u8> {
    let mut lexer = Lexer::new(dialect);
    let mut out = Vec::with_capacity(header.len());

    for &byte in header {
        match lexer.feed(byte) {
            ByteClass::Comment => out.push(b' '),
            ByteClass::CommentStart => {
                // The first byte of the opener was copied as code.
                if let Some(last) = out.last_mut() {
                    *last = b' ';
                }
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

    type Names = Option<(String, Option<Vec<String>>)>;

    fn metadata(header: &str, dialect: SqlDialect) -> Names {
        extract_metadata(header.as_bytes(), dialect).map(|m| (m.table, m.columns))
    }

    fn named(table: &str, columns: &[&str]) -> Names {
        Some((
            table.to_string(),
            Some(columns.iter().map(|c| c.to_string()).collect()),
        ))
    }

    #[test]
    fn test_insert_variants() {
        let cases = [
            "INSERT INTO `t` (`a`, `b`) VALUES ",
            "INSERT IGNORE INTO `t` (`a`, `b`) VALUES ",
            "REPLACE INTO `t` (`a`, `b`) VALUES ",
            "INSERT LOW_PRIORITY IGNORE INTO t (a, b) VALUES",
            "INSERT t (a, b) VALUE ",
            "INSERT /* hint */ INTO t (a, -- first\n b) VALUES ",
        ];
        for header in cases {
            assert_eq!(
                metadata(header, SqlDialect::Mysql),
                named("t", &["a", "b"]),
                "{header}"
            );
        }

        assert_eq!(
            metadata(
                "INSERT OR REPLACE INTO \"t\" (a,\n\tb) VALUES",
                SqlDialect::Sqlite
            ),
            named("t", &["a", "b"])
        );
        assert_eq!(
            metadata("INSERT INTO public.t VALUES ", SqlDialect::Postgres),
            Some(("t".to_string(), None))
        );
    }
}
