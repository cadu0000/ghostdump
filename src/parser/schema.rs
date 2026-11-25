use crate::parser::lexer::{ByteClass, Lexer, is_ident_byte};
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

/// A change to a setting that decides how the rest of a dump is read.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionSetting {
    /// The encoding of the text that follows: `SET client_encoding` and
    /// `SET NAMES` (PostgreSQL), `SET NAMES` and `SET CHARACTER SET`
    /// (MySQL). `None` when the value is not a name (`DEFAULT`).
    Encoding(Option<String>),
    /// MySQL `character_set_client`, the character set the server reads
    /// statements in. mysqldump sets it around each table, trigger and
    /// routine, and restores it from a variable.
    ClientCharset(SettingValue),
    /// PostgreSQL `standard_conforming_strings`: whether a backslash is a
    /// plain character in `'...'`. `None` for a value other than a word
    /// for on or off.
    StandardStrings(Option<bool>),
    /// MySQL `sql_mode`.
    SqlMode(SettingValue),
    /// MySQL: a user variable that the `SET` assigns. `saves` names the
    /// session variable it copies (`@saved_sql_mode = @@sql_mode`), in
    /// lower case, when its value is only that.
    UserVariable { name: String, saves: Option<String> },
    /// A change GhostDump cannot read, described for a message: a
    /// PostgreSQL `set_config` whose setting is not a string, or a write
    /// to `pg_settings`.
    Unreadable(String),
}

/// The value a statement gives a MySQL setting.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingValue {
    /// A word or a quoted string.
    Literal(String),
    /// A user variable, `@name`, its name in lower case.
    Variable(String),
    /// `DEFAULT`, a number or an expression, whose value GhostDump does not
    /// work out.
    Unknown,
}

impl SettingValue {
    fn of(value: &[Token]) -> Self {
        match value {
            [Token::Word(word)] if !word.eq_ignore_ascii_case("default") => {
                Self::Literal(word.clone())
            }
            [
                Token::Quoted {
                    quote: b'\'' | b'"',
                    text,
                },
            ] => Self::Literal(text.clone()),
            [Token::Symbol(b'@'), name] => {
                variable_name(name).map_or(Self::Unknown, Self::Variable)
            }
            _ => Self::Unknown,
        }
    }
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

/// A statement held whole for checking (see
/// [`SqlEvent::Statement`](crate::parser::SqlEvent::Statement)), read once.
pub struct Statement {
    tokens: Vec<Token>,
    dialect: SqlDialect,
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

    fn starts_with(&self, words: &[&str]) -> bool {
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

    /// The changes the statement makes to settings that decide how the
    /// rest of the dump is read: those of a `SET`, with the MySQL user
    /// variables it assigns, the user variables any other MySQL statement
    /// names, and in PostgreSQL the `set_config` calls and writes to
    /// `pg_settings` of any statement.
    pub fn session_settings(&self) -> Vec<SessionSetting> {
        let mut settings = Vec::new();
        if self.starts_with(&["set"]) {
            let mut tokens = &self.tokens[1..];
            if let [rest @ .., Token::Symbol(b';')] = tokens {
                tokens = rest;
            }
            // The end of the version comment that mysqldump writes its
            // settings in, `/*!40101 SET ... */;`, held from `SET`.
            if self.dialect == SqlDialect::Mysql
                && let [rest @ .., Token::Symbol(b'*'), Token::Symbol(b'/')] = tokens
            {
                tokens = rest;
            }
            settings.extend(
                split_outside_parentheses(tokens)
                    .into_iter()
                    .filter_map(session_setting),
            );
        } else if self.dialect == SqlDialect::Mysql {
            // Any other statement may assign the user variables it names
            // (`SELECT ... INTO @v`, `SELECT @v := ...`).
            let mentioned = self.tokens.windows(2).enumerate().filter_map(|(at, pair)| {
                let after_at = at > 0 && self.tokens[at - 1] == Token::Symbol(b'@');
                match pair {
                    [Token::Symbol(b'@'), name] if !after_at => variable_name(name),
                    _ => None,
                }
            });
            settings
                .extend(mentioned.map(|name| SessionSetting::UserVariable { name, saves: None }));
        }
        if self.dialect == SqlDialect::Postgres {
            settings.extend(set_config_calls(&self.tokens));
            if self.may_write() && self.tokens.iter().any(|token| token.is_word("pg_settings")) {
                settings.push(SessionSetting::Unreadable(format!(
                    "{} of pg_settings",
                    self.keyword()
                )));
            }
        }
        settings
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

/// One assignment of a `SET`: `[@@][scope.]name {= | TO} value`, `NAMES
/// value` or `CHARACTER SET value`. A user variable (`@name`) is not a
/// setting.
fn session_setting(tokens: &[Token]) -> Option<SessionSetting> {
    // A user variable: `@name = value`.
    if let [Token::Symbol(b'@'), name, rest @ ..] = tokens
        && *name != Token::Symbol(b'@')
    {
        return Some(SessionSetting::UserVariable {
            name: variable_name(name)?,
            saves: assigned(rest).and_then(session_variable),
        });
    }

    let mut at = 0;
    if tokens.first() == Some(&Token::Symbol(b'@')) {
        at = 2;
        // `@@SESSION.sql_mode`
        if tokens.get(at + 1) == Some(&Token::Symbol(b'.')) {
            at += 2;
        }
    }
    if tokens.get(at).is_some_and(|token| {
        ["session", "local", "global", "persist", "persist_only"]
            .iter()
            .any(|word| token.is_word(word))
    }) {
        at += 1;
    }

    let name = tokens.get(at)?;
    let value = |at: usize| tokens.get(at).and_then(setting_value);
    if name.is_word("names") || name.is_word("charset") {
        return Some(SessionSetting::Encoding(value(at + 1)));
    }
    if name.is_word("character") && tokens.get(at + 1)?.is_word("set") {
        return Some(SessionSetting::Encoding(value(at + 2)));
    }
    let assignment = assigned(&tokens[at + 1..])?;
    let first = assignment.first().and_then(setting_value);
    if name.is_word("client_encoding") {
        Some(SessionSetting::Encoding(first))
    } else if name.is_word("standard_conforming_strings") {
        Some(SessionSetting::StandardStrings(on_or_off(first)))
    } else if name.is_word("character_set_client") {
        Some(SessionSetting::ClientCharset(SettingValue::of(assignment)))
    } else if name.is_word("sql_mode") {
        Some(SessionSetting::SqlMode(SettingValue::of(assignment)))
    } else {
        None
    }
}

/// The value of an assignment: the tokens after `=`, `:=` or `TO`.
fn assigned(tokens: &[Token]) -> Option<&[Token]> {
    match tokens {
        [Token::Symbol(b'='), value @ ..]
        | [Token::Symbol(b':'), Token::Symbol(b'='), value @ ..] => Some(value),
        [to, value @ ..] if to.is_word("to") => Some(value),
        _ => None,
    }
}

/// The session variable that a value is, `@@name` or `@@SESSION.name`, in
/// lower case. `@@GLOBAL.name` is not the value of the session.
fn session_variable(value: &[Token]) -> Option<String> {
    let name = match value {
        [Token::Symbol(b'@'), Token::Symbol(b'@'), name] => name,
        [
            Token::Symbol(b'@'),
            Token::Symbol(b'@'),
            scope,
            Token::Symbol(b'.'),
            name,
        ] if scope.is_word("session") || scope.is_word("local") => name,
        _ => return None,
    };
    match name {
        Token::Word(word) => Some(word.to_ascii_lowercase()),
        _ => None,
    }
}

/// The name of a MySQL user variable after its `@`, in lower case: user
/// variables are compared without case.
fn variable_name(token: &Token) -> Option<String> {
    match token {
        Token::Word(name) | Token::Quoted { text: name, .. } => Some(name.to_ascii_lowercase()),
        _ => None,
    }
}

/// A setting written as a word or a quoted string; `DEFAULT` is not one.
fn setting_value(token: &Token) -> Option<String> {
    match token {
        Token::Word(word) if !word.eq_ignore_ascii_case("default") => Some(word.clone()),
        Token::Quoted { text, .. } => Some(text.clone()),
        _ => None,
    }
}

/// A boolean setting of PostgreSQL, in the words it accepts for on and off.
fn on_or_off(value: Option<String>) -> Option<bool> {
    match value?.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" => Some(true),
        "off" | "false" | "no" => Some(false),
        _ => None,
    }
}

/// The settings that PostgreSQL `set_config(setting, value, is_local)`
/// calls change, read as a `SET` would be. A setting that is not a string
/// cannot be read.
fn set_config_calls(tokens: &[Token]) -> Vec<SessionSetting> {
    let mut settings = Vec::new();
    for (at, token) in tokens.iter().enumerate() {
        if !token.is_word("set_config") || tokens.get(at + 1) != Some(&Token::Symbol(b'(')) {
            continue;
        }
        let rest = &tokens[at + 2..];
        let mut depth = 0u32;
        let end = rest
            .iter()
            .position(|token| match token {
                Token::Symbol(b'(') => {
                    depth += 1;
                    false
                }
                Token::Symbol(b')') if depth == 0 => true,
                Token::Symbol(b')') => {
                    depth -= 1;
                    false
                }
                _ => false,
            })
            .unwrap_or(rest.len());
        let arguments = split_outside_parentheses(&rest[..end]);
        let setting = match arguments.first() {
            Some([Token::Quoted { quote: b'\'', text }]) => text.to_ascii_lowercase(),
            _ => {
                settings.push(SessionSetting::Unreadable(
                    "set_config with a setting that is not a string".to_string(),
                ));
                continue;
            }
        };
        let value = match arguments.get(1) {
            Some([value]) => setting_value(value),
            _ => None,
        };
        match setting.as_str() {
            "client_encoding" => settings.push(SessionSetting::Encoding(value)),
            "standard_conforming_strings" => {
                settings.push(SessionSetting::StandardStrings(on_or_off(value)));
            }
            _ => {}
        }
    }
    settings
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

    type Names = Option<(String, Option<Vec<String>>)>;

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
        // MySQL reads `--1` as two minus signs and `-- 1` as a comment.
        assert!(has_values(b"--1;", mysql));
        assert!(!has_values(b"-- 1\n;", mysql));
        assert!(!has_values(b"--1;", pg));
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
    fn test_session_settings() {
        use SessionSetting::{
            ClientCharset, Encoding, SqlMode, StandardStrings, Unreadable, UserVariable,
        };
        use SettingValue::{Literal, Unknown, Variable};
        let settings =
            |sql: &str, dialect| Statement::parse(sql.as_bytes(), dialect).session_settings();
        let name = |name: &str| Encoding(Some(name.to_string()));
        let pg = SqlDialect::Postgres;
        assert_eq!(
            settings("SET client_encoding = 'LATIN1';", pg),
            [name("LATIN1")]
        );
        assert_eq!(
            settings("SET SESSION client_encoding TO utf8;", pg),
            [name("utf8")]
        );
        assert_eq!(settings("SET NAMES 'WIN1252';", pg), [name("WIN1252")]);
        assert_eq!(
            settings("SET client_encoding TO DEFAULT;", pg),
            [Encoding(None)]
        );
        assert_eq!(
            settings("SET standard_conforming_strings = on;", pg),
            [StandardStrings(Some(true))]
        );
        assert_eq!(
            settings("SET standard_conforming_strings TO 'off';", pg),
            [StandardStrings(Some(false))]
        );
        assert_eq!(
            settings("SET standard_conforming_strings = 0;", pg),
            [StandardStrings(None)]
        );
        assert!(settings("SET statement_timeout = 0;", pg).is_empty());

        // set_config, in any statement, and writes to pg_settings.
        assert!(
            settings(
                "SELECT pg_catalog.set_config('search_path', '', false);",
                pg
            )
            .is_empty()
        );
        assert_eq!(
            settings(
                "SELECT 1, set_config('Standard_Conforming_Strings', 'off', false);",
                pg
            ),
            [StandardStrings(Some(false))]
        );
        assert_eq!(
            settings("SELECT set_config('client_encoding', 'SJIS', false);", pg),
            [name("SJIS")]
        );
        assert_eq!(
            settings(
                "SELECT set_config('client_encoding', lower('X'), false);",
                pg
            ),
            [Encoding(None)]
        );
        assert!(matches!(
            settings("SELECT set_config(name, 'off', false) FROM t;", pg)[..],
            [Unreadable(_)]
        ));
        assert!(matches!(
            settings(
                "UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';",
                pg
            )[..],
            [Unreadable(_)]
        ));
        assert!(settings("SELECT setting FROM pg_settings;", pg).is_empty());
        assert!(settings("SELECT 'set_config(x)', $$set_config(y)$$;", pg).is_empty());

        // mysqldump's settings, held from their first word inside `/*!...*/`.
        let my = SqlDialect::Mysql;
        let saves = |name: &str, setting: Option<&str>| UserVariable {
            name: name.to_string(),
            saves: setting.map(str::to_string),
        };
        assert_eq!(settings("SET NAMES utf8mb4 */;", my), [name("utf8mb4")]);
        assert_eq!(
            settings("SET NAMES latin1 COLLATE latin1_swedish_ci;", my),
            [name("latin1")]
        );
        assert_eq!(settings("SET CHARACTER SET cp932;", my), [name("cp932")]);
        assert_eq!(
            settings(
                "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;",
                my
            ),
            [
                saves("old_sql_mode", Some("sql_mode")),
                SqlMode(Literal("NO_AUTO_VALUE_ON_ZERO".into()))
            ]
        );
        assert_eq!(
            settings("SET @@SESSION.sql_mode = 'ANSI,NO_BACKSLASH_ESCAPES';", my),
            [SqlMode(Literal("ANSI,NO_BACKSLASH_ESCAPES".into()))]
        );
        assert_eq!(
            settings("SET sql_mode = NO_BACKSLASH_ESCAPES;", my),
            [SqlMode(Literal("NO_BACKSLASH_ESCAPES".into()))]
        );
        assert_eq!(
            settings("SET SQL_MODE=@OLD_SQL_MODE */;", my),
            [SqlMode(Variable("old_sql_mode".into()))]
        );
        assert_eq!(
            settings("SET sql_mode = CONCAT(@@sql_mode, ',ANSI');", my),
            [SqlMode(Unknown)]
        );
        assert_eq!(
            settings("SET @saved_cs_client     = @@character_set_client */ ;", my),
            [saves("saved_cs_client", Some("character_set_client"))]
        );
        assert_eq!(
            settings("SET character_set_client  = latin1 */ ;", my),
            [ClientCharset(Literal("latin1".into()))]
        );
        assert_eq!(
            settings("SET character_set_client = @saved_cs_client */;", my),
            [ClientCharset(Variable("saved_cs_client".into()))]
        );
        for unknown in ["13", "DEFAULT", "_utf8mb4'sjis'", "@@character_set_server"] {
            assert_eq!(
                settings(&format!("SET character_set_client = {unknown};"), my),
                [ClientCharset(Unknown)],
                "{unknown}"
            );
        }
        assert_eq!(settings("SET @m := 'x';", my), [saves("m", None)]);
        assert_eq!(
            settings("SET @g = @@GLOBAL.sql_mode, @s = @@session.SQL_MODE;", my),
            [saves("g", None), saves("s", Some("sql_mode"))]
        );
        assert!(settings("SELECT 1, @@sql_mode;", my).is_empty());
        assert_eq!(
            settings("SELECT 'x', @@sql_mode INTO @a, @`B c`;", my),
            [saves("a", None), saves("b c", None)]
        );
        assert!(
            settings(
                "SELECT set_config('standard_conforming_strings', 'off');",
                my
            )
            .is_empty()
        );
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
