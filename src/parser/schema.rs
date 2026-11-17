use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};
use crate::parser::state::SqlDialect;

/// Names read from the header of a data block.
#[derive(Debug, Clone, PartialEq)]
pub struct HeaderMetadata {
    /// The schema (or, in MySQL, the database) written before the table name.
    pub schema: Option<String>,
    pub table: String,
    /// `None` when the statement has no column list.
    pub columns: Option<Vec<String>>,
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

/// Reads a name made of parts separated by dots, from `tokens[*at]`.
fn qualified_name(tokens: &[Token], at: &mut usize, dialect: SqlDialect) -> Option<Vec<String>> {
    let mut parts = vec![tokens.get(*at)?.name(dialect)?.to_string()];
    *at += 1;
    while matches!(tokens.get(*at), Some(Token::Symbol(b'.'))) {
        parts.push(tokens.get(*at + 1)?.name(dialect)?.to_string());
        *at += 2;
    }
    Some(parts)
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

    let parts = qualified_name(&tokens, &mut at, dialect)?;
    let (schema, table) = match parts.as_slice() {
        [table] => (None, table.clone()),
        // `database.schema.table`: the schema is the last qualifier.
        [.., schema, table] if parts.len() <= 3 => (Some(schema.clone()), table.clone()),
        _ => return None,
    };

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
        columns,
    })
}

/// Whether a statement held for checking may write data. Statements that
/// only read (`SELECT` without `INTO`, `COPY ... TO`) and `CREATE TABLE`
/// without a query cannot, so they need no check.
pub fn may_write(statement: &[u8], dialect: SqlDialect) -> bool {
    let tokens = tokenize(statement, dialect);
    let Some(first) = tokens.first() else {
        return false;
    };
    let outside = outside_parentheses(&tokens);
    let any_outside = |words: &[&str]| {
        outside
            .iter()
            .any(|token| words.iter().any(|word| token.is_word(word)))
    };

    if first.is_word("select") || first.is_word("with") {
        // `SELECT ... INTO table`, or a `WITH` that changes data.
        tokens.iter().any(|token| {
            ["into", "insert", "update", "delete", "merge"]
                .iter()
                .any(|word| token.is_word(word))
        })
    } else if first.is_word("copy") {
        any_outside(&["from"])
    } else if first.is_word("create") {
        // A query fills the table: `AS SELECT` (PostgreSQL, SQLite), or in
        // MySQL also `SELECT` with no `AS`, and `TABLE` or `VALUES` statements.
        let table = outside.iter().position(|token| token.is_word("table"));
        let after = table.map_or(&[][..], |table| &outside[table + 1..]);
        after.iter().any(|token| {
            token.is_word("as")
                || (dialect == SqlDialect::Mysql
                    && ["select", "with", "table", "values"]
                        .iter()
                        .any(|word| token.is_word(word)))
        })
    } else {
        true
    }
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

/// Every name a statement refers to, as its parts (`schema.table` gives two).
/// Keywords are included: the check that uses this errs on the safe side.
pub fn names_in(statement: &[u8], dialect: SqlDialect) -> Vec<Vec<String>> {
    let tokens = tokenize(statement, dialect);
    let mut names = Vec::new();
    let mut at = 0;
    while at < tokens.len() {
        match qualified_name(&tokens, &mut at, dialect) {
            Some(parts) => names.push(parts),
            None => at += 1,
        }
    }
    names
}

/// The first word of a statement, in upper case, to name it in messages.
pub fn statement_keyword(statement: &[u8], dialect: SqlDialect) -> String {
    match tokenize(statement, dialect).first() {
        Some(Token::Word(word)) => word.to_ascii_uppercase(),
        _ => "SQL".to_string(),
    }
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

    #[test]
    fn test_names_in_a_statement() {
        let names = names_in(
            b"UPDATE public.users u SET \"e-mail\" = 'x' -- logs\n FROM [o] WHERE u.id = 1",
            SqlDialect::Sqlite,
        );
        let has = |parts: &[&str]| names.iter().any(|name| name == parts);
        assert!(has(&["public", "users"]));
        assert!(has(&["e-mail"]));
        assert!(has(&["o"]));
        assert!(has(&["u", "id"]));
        assert!(!has(&["logs"]));
        // In SQLite a string may stand for a name; elsewhere it is a value.
        assert!(has(&["x"]));
        assert!(names_in(b"SELECT 'x'", SqlDialect::Postgres).len() == 1);
    }

    #[test]
    fn test_statements_that_may_write() {
        let pg = SqlDialect::Postgres;
        assert!(!may_write(
            b"SELECT pg_catalog.setval('public.users_id_seq', 2, true);",
            pg
        ));
        assert!(may_write(b"SELECT * INTO t2 FROM t;", pg));
        assert!(may_write(
            b"WITH x AS (DELETE FROM t RETURNING *) SELECT 1;",
            pg
        ));
        assert!(!may_write(b"COPY (SELECT * FROM t) TO STDOUT;", pg));
        assert!(may_write(b"COPY t FROM '/tmp/t.csv';", pg));
        assert!(!may_write(
            b"CREATE TABLE t (a int GENERATED ALWAYS AS (1) STORED) WITH (fillfactor = 70);",
            pg
        ));
        assert!(may_write(b"CREATE TABLE t AS SELECT 1;", pg));
        assert!(may_write(b"CREATE TABLE t SELECT 1;", SqlDialect::Mysql));
        assert!(may_write(b"UPDATE t SET a = 1;", pg));
        assert!(may_write(b"INSERT INTO t SELECT 1;", pg));
    }

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
