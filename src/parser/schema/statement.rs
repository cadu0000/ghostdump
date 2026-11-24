//! What a statement held whole for checking does: which names it
//! refers to, whether it may write data, and how it changes tables.

use super::{
    TableName, Token, outside_parentheses, qualified_name, split_outside_parentheses, table_name,
    tokenize,
};
use crate::parser::state::SqlDialect;

/// How a statement changes the columns of tables, as far as an
/// `INSERT ... VALUES` without a column list is concerned.
#[derive(Debug, Clone, PartialEq)]
pub enum TableChange {
    /// `CREATE TABLE`, with the columns that a row without a column list
    /// fills, in order: every column in PostgreSQL and MySQL, every column
    /// but the generated ones in SQLite. `None` when the statement does not
    /// show them all (`AS SELECT`, `LIKE`, `INHERITS`, `PARTITION OF`, `OF`,
    /// MySQL `INVISIBLE` columns).
    Created {
        table: TableName,
        columns: Option<Vec<String>>,
    },
    /// Tables that may have changed columns or names (`ALTER TABLE ... ADD
    /// COLUMN`, `RENAME`) or were dropped.
    Changed(Vec<TableName>),
    /// A change to tables whose names could not be read.
    Unknown,
}

/// A statement held whole for checking (see
/// [`SqlEvent::Statement`](crate::parser::SqlEvent::Statement)), read once.
pub struct Statement {
    pub(super) tokens: Vec<Token>,
    pub(super) dialect: SqlDialect,
    /// MySQL: the word `INVISIBLE` appears anywhere in the text, maybe in
    /// a version comment (`/*!80023 INVISIBLE */`).
    invisible: bool,
}

/// Words that start a table constraint, not a column, in every dialect.
const CONSTRAINTS: &[&str] = &["constraint", "primary", "unique", "check", "foreign"];

impl Statement {
    pub fn parse(sql: &[u8], dialect: SqlDialect) -> Self {
        let invisible = dialect == SqlDialect::Mysql
            && sql
                .windows(9)
                .any(|window| window.eq_ignore_ascii_case(b"invisible"));
        Self {
            tokens: tokenize(sql, dialect),
            dialect,
            invisible,
        }
    }

    /// The first word, in upper case, to name the statement in messages.
    pub fn keyword(&self) -> String {
        match self.tokens.first() {
            Some(Token::Word(word)) => word.to_ascii_uppercase(),
            _ => "SQL".to_string(),
        }
    }

    pub(super) fn starts_with(&self, words: &[&str]) -> bool {
        self.tokens
            .first()
            .is_some_and(|first| words.iter().any(|word| first.is_word(word)))
    }

    /// Whether the statement may write data. Statements that only read
    /// (`SELECT` without `INTO`, `COPY ... TO`), that change the schema
    /// (`ALTER`, `DROP`, `RENAME`) or that create a table without a query
    /// cannot.
    pub fn may_write(&self) -> bool {
        let outside = outside_parentheses(&self.tokens);
        if self.starts_with(&["update", "delete", "merge", "load", "insert", "replace"]) {
            true
        } else if self.starts_with(&["select", "with"]) {
            // `SELECT ... INTO table`, or a `WITH` that changes data.
            self.tokens.iter().any(|token| {
                ["into", "insert", "update", "delete", "merge"]
                    .iter()
                    .any(|word| token.is_word(word))
            })
        } else if self.starts_with(&["copy"]) {
            outside.iter().any(|token| token.is_word("from"))
        } else if self.starts_with(&["create"]) {
            let table = outside.iter().position(|token| token.is_word("table"));
            let after = table.map_or(&[][..], |table| &outside[table + 1..]);
            after.iter().any(|token| self.query_fills(token))
        } else {
            false
        }
    }

    /// A word after the table name of `CREATE TABLE` that shows that a
    /// query fills the table: `AS SELECT` (PostgreSQL, SQLite), or in MySQL
    /// also `SELECT` with no `AS`, and `TABLE` or `VALUES` statements.
    fn query_fills(&self, token: &Token) -> bool {
        token.is_word("as")
            || (self.dialect == SqlDialect::Mysql
                && ["select", "with", "table", "values"]
                    .iter()
                    .any(|word| token.is_word(word)))
    }

    /// Every name the statement refers to, as its parts (`schema.table`
    /// gives two). Keywords are included: the check that uses this errs on
    /// the safe side.
    pub fn names(&self) -> Vec<Vec<String>> {
        let mut names = Vec::new();
        let mut at = 0;
        while at < self.tokens.len() {
            match qualified_name(&self.tokens, &mut at, self.dialect) {
                Some(parts) => names.push(parts),
                None => at += 1,
            }
        }
        names
    }

    /// Whether the statement calls one of `functions`, by name, with or
    /// without a schema (`pg_catalog.lowrite(...)`).
    pub fn calls_any(&self, functions: &[&str]) -> bool {
        self.tokens.windows(2).any(|pair| {
            pair[1] == Token::Symbol(b'(') && functions.iter().any(|name| pair[0].is_word(name))
        })
    }

    /// MySQL `USE name`: the database that table names without one refer
    /// to after it.
    pub fn used_database(&self) -> Option<String> {
        if self.dialect != SqlDialect::Mysql || !self.starts_with(&["use"]) {
            return None;
        }
        self.tokens.get(1)?.name(self.dialect).map(str::to_string)
    }

    /// What the statement changes in the columns of tables, if it creates,
    /// alters, drops or renames one.
    pub fn table_change(&self) -> Option<TableChange> {
        if self.starts_with(&["create"]) {
            self.created()
        } else if self.starts_with(&["alter"]) {
            self.altered()
        } else if self.starts_with(&["drop"]) {
            self.dropped()
        } else if self.starts_with(&["rename"]) {
            self.renamed()
        } else {
            None
        }
    }

    /// `CREATE [OR REPLACE] [GLOBAL | LOCAL] [TEMP | TEMPORARY | UNLOGGED]
    /// TABLE [IF NOT EXISTS] name ...`.
    fn created(&self) -> Option<TableChange> {
        let tokens = &self.tokens;
        let mut at = 1;
        let modifiers = [
            "or",
            "replace",
            "global",
            "local",
            "temp",
            "temporary",
            "unlogged",
        ];
        while tokens
            .get(at)
            .is_some_and(|token| modifiers.iter().any(|word| token.is_word(word)))
        {
            at += 1;
        }
        if !tokens.get(at)?.is_word("table") {
            return None;
        }
        at += 1;
        if tokens.get(at).is_some_and(|token| token.is_word("if")) {
            at += 3;
        }
        let Some(table) = table_name(tokens, &mut at, self.dialect) else {
            return Some(TableChange::Unknown);
        };
        let columns = self.column_list(at);
        Some(TableChange::Created { table, columns })
    }

    /// The columns that a row without a column list fills, from the
    /// definition of a table that starts at `tokens[at]`.
    fn column_list(&self, at: usize) -> Option<Vec<String>> {
        let tokens = &self.tokens;
        if tokens.get(at)? != &Token::Symbol(b'(') || self.invisible {
            return None;
        }
        let (elements, end) = split_list(tokens, at)?;
        // A parent table or a query adds columns.
        let rest = outside_parentheses(&tokens[end + 1..]);
        if rest
            .iter()
            .any(|token| token.is_word("inherits") || self.query_fills(token))
        {
            return None;
        }

        // `CREATE TABLE t ()`: a table without columns.
        if let [element] = elements.as_slice()
            && element.is_empty()
        {
            return Some(Vec::new());
        }
        let mut columns = Vec::new();
        for element in elements {
            let first = element.first()?;
            if first.is_word("like") {
                return None;
            }
            if self.starts_constraint(element) {
                continue;
            }
            // SQLite leaves generated columns out of a row without a column
            // list; PostgreSQL and MySQL expect `DEFAULT` for them.
            if self.dialect == SqlDialect::Sqlite
                && outside_parentheses(element)
                    .iter()
                    .any(|token| token.is_word("as"))
            {
                continue;
            }
            columns.push(first.name(self.dialect)?.to_string());
        }
        Some(columns)
    }

    /// Whether an element of a table definition is a table constraint.
    fn starts_constraint(&self, element: &[Token]) -> bool {
        let Some(first) = element.first() else {
            return false;
        };
        let dialect_words: &[&str] = match self.dialect {
            SqlDialect::Mysql => &["key", "index", "fulltext", "spatial"],
            _ => &[],
        };
        CONSTRAINTS
            .iter()
            .chain(dialect_words)
            .any(|word| first.is_word(word))
            // PostgreSQL `EXCLUDE [USING method] (...)`; `exclude` alone may
            // name a column.
            || (self.dialect == SqlDialect::Postgres
                && first.is_word("exclude")
                && element
                    .get(1)
                    .is_some_and(|next| next == &Token::Symbol(b'(') || next.is_word("using")))
    }

    /// `ALTER TABLE [IF EXISTS] [ONLY] name [*] action [, action]*`: the
    /// table changes unless every action keeps its columns.
    fn altered(&self) -> Option<TableChange> {
        let tokens = &self.tokens;
        let mut at = 1;
        // MySQL `ALTER [ONLINE | OFFLINE] [IGNORE] TABLE`.
        while tokens.get(at).is_some_and(|token| {
            ["online", "offline", "ignore"]
                .iter()
                .any(|word| token.is_word(word))
        }) {
            at += 1;
        }
        if !tokens.get(at)?.is_word("table") {
            return None;
        }
        at += 1;
        if tokens.get(at).is_some_and(|token| token.is_word("if")) {
            at += 2;
        }
        if tokens.get(at).is_some_and(|token| token.is_word("only")) {
            at += 1;
        }
        let Some(table) = table_name(tokens, &mut at, self.dialect) else {
            return Some(TableChange::Unknown);
        };
        if tokens.get(at) == Some(&Token::Symbol(b'*')) {
            at += 1;
        }
        let actions = split_outside_parentheses(&tokens[at..]);
        let changes = actions.iter().any(|action| !keeps_columns(action));
        changes.then(|| TableChange::Changed(vec![table]))
    }

    /// `DROP [TEMPORARY] TABLE [IF EXISTS] name [, name]*`; a dropped schema
    /// or database takes tables with it.
    fn dropped(&self) -> Option<TableChange> {
        let tokens = &self.tokens;
        let mut at = 1;
        if tokens.get(at)?.is_word("temporary") {
            at += 1;
        }
        let object = tokens.get(at)?;
        if object.is_word("schema") || object.is_word("database") {
            return Some(TableChange::Unknown);
        }
        if !object.is_word("table") {
            return None;
        }
        at += 1;
        if tokens.get(at).is_some_and(|token| token.is_word("if")) {
            at += 2;
        }
        self.table_names(at)
    }

    /// MySQL `RENAME TABLE a TO b [, c TO d]*`.
    fn renamed(&self) -> Option<TableChange> {
        if !self.tokens.get(1)?.is_word("table") {
            return None;
        }
        let mut names = Vec::new();
        let mut at = 2;
        while at < self.tokens.len() {
            match &self.tokens[at] {
                Token::Symbol(b',' | b';') => at += 1,
                token if token.is_word("to") => at += 1,
                _ => match table_name(&self.tokens, &mut at, self.dialect) {
                    Some(name) => names.push(name),
                    None => return Some(TableChange::Unknown),
                },
            }
        }
        Some(TableChange::Changed(names))
    }

    /// Tables named from `tokens[at]`, separated by commas, up to the end or
    /// to `CASCADE` or `RESTRICT`.
    fn table_names(&self, mut at: usize) -> Option<TableChange> {
        let mut names = Vec::new();
        while let Some(token) = self.tokens.get(at) {
            if token.is_word("cascade") || token.is_word("restrict") {
                break;
            }
            match token {
                Token::Symbol(b',' | b';') => at += 1,
                _ => match table_name(&self.tokens, &mut at, self.dialect) {
                    Some(name) => names.push(name),
                    None => return Some(TableChange::Unknown),
                },
            }
        }
        Some(TableChange::Changed(names))
    }
}

/// The elements of the list in parentheses that starts at `tokens[open]`,
/// split at the commas outside nested parentheses, and the position of the
/// closing parenthesis.
fn split_list(tokens: &[Token], open: usize) -> Option<(Vec<&[Token]>, usize)> {
    let mut depth = 0u32;
    let mut start = open + 1;
    let mut elements = Vec::new();
    for (i, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::Symbol(b'(') => depth += 1,
            Token::Symbol(b')') => {
                depth -= 1;
                if depth == 0 {
                    elements.push(&tokens[start..i]);
                    return Some((elements, i));
                }
            }
            Token::Symbol(b',') if depth == 1 => {
                elements.push(&tokens[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    None
}

/// Whether an `ALTER TABLE` action keeps the columns of the table, their
/// order and their names. Only actions known to keep them do; any other
/// (`ADD COLUMN`, `DROP COLUMN`, `RENAME`, MySQL `CHANGE` and `MODIFY`,
/// `SET SCHEMA`, `INVISIBLE`) counts as a change, which errs on the safe
/// side.
fn keeps_columns(action: &[Token]) -> bool {
    let Some(first) = action.first() else {
        return true;
    };
    let next_is = |words: &[&str]| {
        action
            .get(1)
            .is_some_and(|next| words.iter().any(|word| next.is_word(word)))
    };
    if first.is_word("add") {
        next_is(&[
            "constraint",
            "primary",
            "unique",
            "check",
            "foreign",
            "exclude",
            "index",
            "key",
            "fulltext",
            "spatial",
        ])
    } else if first.is_word("drop") {
        next_is(&[
            "constraint",
            "index",
            "key",
            "primary",
            "foreign",
            "check",
            "partition",
        ])
    } else if first.is_word("alter") {
        !action
            .iter()
            .any(|token| token.is_word("invisible") || token.is_word("visible"))
    } else if first.is_word("set") {
        !next_is(&["schema"])
    } else {
        [
            "owner",
            "attach",
            "detach",
            "replica",
            "enable",
            "disable",
            "force",
            "no",
            "cluster",
            "reset",
            "inherit",
            "of",
            "not",
            "validate",
            "order",
            "algorithm",
            "lock",
            "convert",
            "engine",
            "auto_increment",
            "default",
            "character",
            "charset",
            "collate",
            "comment",
            "row_format",
            "tablespace",
        ]
        .iter()
        .any(|word| first.is_word(word))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::schema::extract_metadata;

    fn may_write(sql: &[u8], dialect: SqlDialect) -> bool {
        Statement::parse(sql, dialect).may_write()
    }

    fn names_in(sql: &[u8], dialect: SqlDialect) -> Vec<Vec<String>> {
        Statement::parse(sql, dialect).names()
    }

    fn change(sql: &str, dialect: SqlDialect) -> Option<TableChange> {
        Statement::parse(sql.as_bytes(), dialect).table_change()
    }

    /// The key and columns a `CREATE TABLE` gives.
    fn created(sql: &str, dialect: SqlDialect) -> (String, Option<Vec<String>>) {
        match change(sql, dialect) {
            Some(TableChange::Created { table, columns }) => (table.key, columns),
            other => panic!("not a created table: {other:?}"),
        }
    }

    fn columns(names: &[&str]) -> Option<Vec<String>> {
        Some(names.iter().map(|name| name.to_string()).collect())
    }

    /// The keys of the tables a statement changes.
    fn changed(sql: &str, dialect: SqlDialect) -> Vec<String> {
        match change(sql, dialect) {
            Some(TableChange::Changed(tables)) => tables.into_iter().map(|t| t.key).collect(),
            None => Vec::new(),
            other => panic!("not a change of named tables: {other:?}"),
        }
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
    fn test_column_order_of_created_tables() {
        let (pg, mysql, sqlite) = (SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Sqlite);
        // pg_dump: a named CHECK constraint and a generated column, which
        // PostgreSQL fills with DEFAULT in a row without a column list.
        assert_eq!(
            created(
                "CREATE TABLE public.users (\n    id integer NOT NULL,\n    name text,\n    \
                 tag text GENERATED ALWAYS AS (upper(name)) STORED,\n    \
                 CONSTRAINT users_check CHECK ((id > 0))\n);",
                pg
            ),
            ("public\0users".to_string(), columns(&["id", "name", "tag"]))
        );
        // mysqldump: quoted names, keys and a foreign key.
        assert_eq!(
            created(
                "CREATE TABLE `orders` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  \
                 `card` varchar(30) DEFAULT 'x,y',\n  `total` decimal(10,2),\n  \
                 PRIMARY KEY (`id`),\n  KEY `idx` (`card`),\n  \
                 CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `users` (`id`)\n) \
                 ENGINE=InnoDB /*!50100 PARTITION BY HASH (id) */;",
                mysql
            ),
            ("orders".to_string(), columns(&["id", "card", "total"]))
        );
        // SQLite leaves generated columns out of a row without a column list.
        assert_eq!(
            created(
                "CREATE TABLE IF NOT EXISTS \"Mixed\" (a INTEGER, b AS (a * 2), \"c d\" TEXT, \
                 e TEXT GENERATED ALWAYS AS (c) STORED, UNIQUE (a))",
                sqlite
            ),
            ("mixed".to_string(), columns(&["a", "c d"]))
        );
        // A PostgreSQL column may be called `key` or `exclude`.
        assert_eq!(
            created(
                "CREATE TABLE t (key text, exclude int, EXCLUDE USING gist (key WITH =))",
                pg
            )
            .1,
            columns(&["key", "exclude"])
        );
        assert_eq!(created("CREATE TABLE t ()", pg).1, columns(&[]));
    }

    #[test]
    fn test_tables_whose_columns_the_definition_does_not_show() {
        let (pg, mysql) = (SqlDialect::Postgres, SqlDialect::Mysql);
        for sql in [
            "CREATE TABLE t AS SELECT 1 AS a",
            "CREATE TABLE child (c text) INHERITS (parent)",
            "CREATE TABLE p PARTITION OF parent FOR VALUES IN (1)",
            "CREATE TABLE t OF some_type",
            "CREATE TABLE t (LIKE other INCLUDING ALL)",
        ] {
            assert_eq!(created(sql, pg).1, None, "{sql}");
        }
        for sql in [
            "CREATE TABLE t SELECT 1 AS a",
            "CREATE TABLE t LIKE other",
            "CREATE TABLE t (a int, h int /*!80023 INVISIBLE */)",
        ] {
            assert_eq!(created(sql, mysql).1, None, "{sql}");
        }
        assert_eq!(change("CREATE INDEX i ON t (a)", pg), None);
        assert_eq!(change("CREATE VIEW v AS SELECT 1", pg), None);
    }

    #[test]
    fn test_keys_follow_the_case_rules_of_each_dialect() {
        let key = |sql: &str, dialect| created(sql, dialect).0;
        let (pg, mysql, sqlite) = (SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Sqlite);
        assert_eq!(key("CREATE TABLE Users (a int)", pg), "users");
        assert_eq!(key("CREATE TABLE \"Users\" (a int)", pg), "Users");
        assert_eq!(key("CREATE TABLE `Users` (a int)", mysql), "Users");
        assert_eq!(key("CREATE TABLE \"Users\" (a int)", sqlite), "users");
        let header = |sql: &str, dialect| extract_metadata(sql.as_bytes(), dialect).unwrap().key;
        assert_eq!(header("INSERT INTO \"Users\" VALUES ", pg), "Users");
        assert_eq!(
            header("INSERT INTO public.users VALUES ", pg),
            "public\0users"
        );
    }

    #[test]
    fn test_alter_table_changes_only_with_column_actions() {
        let pg = SqlDialect::Postgres;
        // What pg_dump writes keeps the columns.
        for sql in [
            "ALTER TABLE public.users OWNER TO postgres;",
            "ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('s'::regclass);",
            "ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (\n    SEQUENCE NAME public.users_id_seq\n);",
            "ALTER TABLE ONLY public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id);",
            "ALTER TABLE ONLY public.events ATTACH PARTITION public.e1 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');",
            "ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;",
            "ALTER TABLE ONLY public.users REPLICA IDENTITY FULL;",
            "ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;",
        ] {
            assert_eq!(changed(sql, pg), Vec::<String>::new(), "{sql}");
        }
        for sql in [
            "ALTER TABLE users ADD COLUMN age int;",
            "ALTER TABLE users ADD age int, ADD CONSTRAINT c CHECK (age > 0);",
            "ALTER TABLE users DROP COLUMN name;",
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
            "ALTER TABLE users RENAME TO people;",
            "ALTER TABLE users SET SCHEMA archive;",
            "ALTER TABLE IF EXISTS ONLY users * ADD IF NOT EXISTS age int;",
        ] {
            assert_eq!(changed(sql, pg), ["users"], "{sql}");
        }
        let mysql = SqlDialect::Mysql;
        for sql in [
            "ALTER TABLE `users` MODIFY `name` varchar(9) FIRST;",
            "ALTER TABLE `users` CHANGE `name` `full_name` text;",
            "ALTER TABLE `users` ALTER COLUMN `name` SET INVISIBLE;",
        ] {
            assert_eq!(changed(sql, mysql), ["users"], "{sql}");
        }
        assert_eq!(
            changed(
                "ALTER TABLE `users` ADD INDEX i (`name`), ENGINE=InnoDB;",
                mysql
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_dropped_and_renamed_tables() {
        let (pg, mysql) = (SqlDialect::Postgres, SqlDialect::Mysql);
        assert_eq!(
            changed("DROP TABLE IF EXISTS public.users, logs CASCADE;", pg),
            ["public\0users", "logs"]
        );
        assert_eq!(changed("DROP VIEW v;", pg), Vec::<String>::new());
        assert_eq!(
            change("DROP SCHEMA archive CASCADE;", pg),
            Some(TableChange::Unknown)
        );
        assert_eq!(
            changed("RENAME TABLE `a` TO `b`, `c` TO `d`;", mysql),
            ["a", "b", "c", "d"]
        );
    }

    #[test]
    fn test_function_calls() {
        let pg = SqlDialect::Postgres;
        let calls = |sql: &str| Statement::parse(sql.as_bytes(), pg).calls_any(&["lowrite"]);
        assert!(calls("SELECT pg_catalog.lowrite(0, '\\x5345');"));
        assert!(calls("SELECT LOWRITE (0, 'x');"));
        assert!(!calls("SELECT pg_catalog.lo_open('49158', 131072);"));
        assert!(!calls("SELECT 'lowrite(0)', lowrite;"));
    }

    #[test]
    fn test_used_database() {
        let used = |sql: &str, dialect| Statement::parse(sql.as_bytes(), dialect).used_database();
        let my = SqlDialect::Mysql;
        assert_eq!(used("USE `shop`;", my).as_deref(), Some("shop"));
        assert_eq!(used("use shop\n", my).as_deref(), Some("shop"));
        assert_eq!(used("SELECT 1;", my), None);
        assert_eq!(used("USE shop;", SqlDialect::Postgres), None);
    }
}
