//! The settings a statement changes that decide how the rest of the dump
//! is read: `SET`, MySQL user variables, PostgreSQL `set_config`.

use super::statement::Statement;
use super::{Token, split_outside_parentheses};
use crate::parser::state::SqlDialect;

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

impl Statement {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
