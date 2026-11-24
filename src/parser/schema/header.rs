//! The table and columns that the header of a data block names.

use super::{TableName, Token, name_list, table_name, tokenize};
use crate::parser::state::SqlDialect;

/// Names read from the header of a data block.
#[derive(Debug, Clone, PartialEq)]
pub struct HeaderMetadata {
    /// The schema (or, in MySQL, the database) written before the table name.
    pub schema: Option<String>,
    pub table: String,
    /// The table as the database tells it apart (see [`TableName::key`]).
    pub key: String,
    /// `None` when the statement has no column list.
    pub columns: Option<Vec<String>>,
}

/// Reads the table and column names of a data block header:
///
/// * `INSERT [OR action] [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
///   [INTO] table [AS alias] [PARTITION (...)] [(columns)]
///   [OVERRIDING {SYSTEM | USER} VALUE] VALUES`, and `REPLACE` in its place;
/// * `COPY table [(columns)] FROM {STDIN | STDOUT} ...`.
///
/// Names may be quoted (`"my table"`, `` `my table` ``, `[my table]`) and
/// qualified (`schema.table`); comments are ignored. Anything else gives
/// `None`: the header cannot be read.
pub fn extract_metadata(header_bytes: &[u8], dialect: SqlDialect) -> Option<HeaderMetadata> {
    let tokens = tokenize(header_bytes, dialect);
    let mut at = 0;
    let copy = if tokens.first()?.is_word("copy") {
        true
    } else if tokens.first()?.is_word("insert") || tokens.first()?.is_word("replace") {
        false
    } else {
        return None;
    };
    at += 1;

    if !copy {
        if tokens.get(at)?.is_word("or") {
            at += 2;
        }
        while ["low_priority", "delayed", "high_priority", "ignore"]
            .iter()
            .any(|word| tokens.get(at).is_some_and(|token| token.is_word(word)))
        {
            at += 1;
        }
        if tokens.get(at)?.is_word("into") {
            at += 1;
        }
    }

    let TableName { schema, table, key } = table_name(&tokens, &mut at, dialect)?;

    if !copy && tokens.get(at)?.is_word("as") {
        tokens.get(at + 1)?.name(dialect)?;
        at += 2;
    }
    if !copy && tokens.get(at)?.is_word("partition") {
        at += 1;
        if tokens.get(at)? != &Token::Symbol(b'(') {
            return None;
        }
        name_list(&tokens, &mut at, dialect)?;
    }
    let columns = if tokens.get(at)? == &Token::Symbol(b'(') {
        Some(name_list(&tokens, &mut at, dialect)?)
    } else {
        None
    };

    let ends_header = if copy {
        tokens.get(at)?.is_word("from")
            && (tokens.get(at + 1)?.is_word("stdin") || tokens.get(at + 1)?.is_word("stdout"))
    } else {
        if tokens.get(at)?.is_word("overriding") {
            let kind = tokens.get(at + 1)?;
            if !(kind.is_word("system") || kind.is_word("user"))
                || !tokens.get(at + 2)?.is_word("value")
            {
                return None;
            }
            at += 3;
        }
        let values = tokens.get(at)?;
        values.is_word("values") || (dialect == SqlDialect::Mysql && values.is_word("value"))
    };

    ends_header.then_some(HeaderMetadata {
        schema,
        table,
        key,
        columns,
    })
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

    #[test]
    fn test_copy_header_with_and_without_options() {
        let pg = SqlDialect::Postgres;
        assert_eq!(
            metadata("COPY public.t (a, b) FROM stdin;", pg),
            named("t", &["a", "b"])
        );
        assert_eq!(
            metadata("COPY t (a) FROM stdin WITH (FORMAT csv);", pg),
            named("t", &["a"])
        );
    }

    #[test]
    fn test_quoted_names_with_spaces_and_doubled_quotes() {
        assert_eq!(
            metadata(
                "INSERT INTO \"my \"\"big\"\" table\" (\"first name\", \"a,b\", \"c)d\") VALUES ",
                SqlDialect::Postgres
            ),
            named("my \"big\" table", &["first name", "a,b", "c)d"])
        );
        assert_eq!(
            metadata(
                "INSERT INTO `my ``t``` (`first name`) VALUES ",
                SqlDialect::Mysql
            ),
            named("my `t`", &["first name"])
        );
        assert_eq!(
            metadata(
                "INSERT INTO [my table] ([first name]) VALUES ",
                SqlDialect::Sqlite
            ),
            named("my table", &["first name"])
        );
        assert_eq!(
            metadata(
                "COPY public.\"my table\" (\"first name\") FROM stdin;",
                SqlDialect::Postgres
            ),
            named("my table", &["first name"])
        );
    }

    #[test]
    fn test_clauses_between_the_table_and_values() {
        // pg_dump --column-inserts, for an identity column GENERATED ALWAYS.
        assert_eq!(
            metadata(
                "INSERT INTO public.t (id, a) OVERRIDING SYSTEM VALUE VALUES ",
                SqlDialect::Postgres
            ),
            named("t", &["id", "a"])
        );
        assert_eq!(
            metadata(
                "INSERT INTO t AS x (a) OVERRIDING USER VALUE VALUES ",
                SqlDialect::Postgres
            ),
            named("t", &["a"])
        );
        assert_eq!(
            metadata(
                "INSERT INTO t PARTITION (p0, p1) (a) VALUES ",
                SqlDialect::Mysql
            ),
            named("t", &["a"])
        );
    }

    #[test]
    fn test_schema_is_the_last_qualifier() {
        let m = extract_metadata(
            b"INSERT INTO db . audit . \"Users\" (a) VALUES ",
            SqlDialect::Postgres,
        )
        .unwrap();
        assert_eq!(m.schema.as_deref(), Some("audit"));
        assert_eq!(m.table, "Users");
    }

    #[test]
    fn test_headers_that_cannot_be_read() {
        let headers = [
            "INSERT INTO U&\"d\\0061ta\" (a) VALUES ",
            "INSERT INTO t SELECT 1 UNION VALUES ",
            "COPY BINARY t FROM stdin;",
            "INSERT INTO a.b.c.d (x) VALUES ",
        ];
        for header in headers {
            assert_eq!(
                extract_metadata(header.as_bytes(), SqlDialect::Postgres),
                None,
                "{header}"
            );
        }
        assert_eq!(
            metadata("COPY t FROM STDOUT;", SqlDialect::Postgres),
            Some(("t".to_string(), None))
        );
    }
}
