//! The settings that decide how the rest of a dump is read. With rules,
//! GhostDump refuses those after which it would read the dump differently
//! from the database.

use std::io;

use crate::engine::refuse;
use crate::parser::{SessionSetting, SettingValue, Statement};

/// MySQL character sets in which a character can hold a byte below 0x80
/// (`\` among them in all but euckr), so the server would end a string
/// where GhostDump does not. Found by converting, on MySQL 8.0, every
/// two-byte sequence with a lead byte from 0x80 and a second byte below it;
/// ucs2, utf16, utf16le and utf32 cannot be the character set of a client.
const ASCII_IN_CHARACTERS: &[&str] = &["big5", "cp932", "euckr", "gb18030", "gbk", "sjis"];

/// How to dump a MySQL database whose triggers, routines or events were
/// created with a setting GhostDump refuses: mysqldump writes the setting
/// before each of them.
const DUMP_CODE_APART: &str = "Dump the data without them (mysqldump --skip-triggers, \
     without --routines and --events), and the triggers, routines and events apart: \
     they hold no rows (mysqldump --no-data --no-create-info --routines --events)";

/// MySQL user variables that hold a saved setting (`SET @saved_sql_mode =
/// @@sql_mode`), so that restoring it (`SET sql_mode = @saved_sql_mode`)
/// gives back a value GhostDump has read, as mysqldump does around each
/// table, trigger and routine.
#[derive(Default)]
pub(super) struct SavedSettings(Vec<(String, String)>);

impl SavedSettings {
    /// The variables kept; a setting restored from one no longer kept is
    /// refused. mysqldump uses four.
    const MAX: usize = 16;

    fn assign(&mut self, variable: String, saves: Option<String>) {
        self.0.retain(|(name, _)| *name != variable);
        let Some(setting) = saves
            .filter(|setting| ["sql_mode", "character_set_client"].contains(&setting.as_str()))
        else {
            return;
        };
        if self.0.len() == Self::MAX {
            self.0.remove(0);
        }
        self.0.push((variable, setting));
    }

    fn holds(&self, variable: &str, setting: &str) -> bool {
        self.0
            .iter()
            .any(|(name, saved)| name == variable && saved == setting)
    }
}

/// Refuses, when there are rules, a change after which GhostDump would
/// read the dump differently from the database: text in an encoding other
/// than UTF-8, strings whose backslashes do not work the way the lexer
/// takes them, or a value GhostDump cannot work out.
pub(super) fn check_settings(statement: &Statement, saved: &mut SavedSettings) -> io::Result<()> {
    for setting in statement.session_settings() {
        match setting {
            SessionSetting::Encoding(name) if !name.as_deref().is_some_and(is_utf8) => {
                return Err(refuse(format!(
                    "the dump sets its encoding to {}; with rules, GhostDump reads only UTF-8 \
                     dumps: names and values in another encoding would not match the rules, and \
                     generated text would be written in UTF-8. Dump again in UTF-8 \
                     (pg_dump -E UTF8, mysqldump --default-character-set=utf8mb4)",
                    name.map_or("its default".to_string(), |name| format!("`{name}`"))
                )));
            }
            SessionSetting::StandardStrings(on) if on != Some(true) => {
                return Err(refuse(
                    "the dump sets standard_conforming_strings to off (or to a value GhostDump \
                     does not read): backslashes would then escape quotes in '...' strings, \
                     which GhostDump reads as standard strings, so a row could be misread. \
                     Dump with standard_conforming_strings on, as pg_dump does"
                        .into(),
                ));
            }
            SessionSetting::SqlMode(SettingValue::Literal(modes))
                if modes
                    .split(',')
                    .any(|mode| mode.trim().eq_ignore_ascii_case("no_backslash_escapes")) =>
            {
                return Err(refuse(format!(
                    "the dump sets NO_BACKSLASH_ESCAPES in sql_mode, as mysqldump does before \
                     a trigger, routine or event created in that mode: backslashes would then \
                     be plain characters in strings, which GhostDump reads as escapes, so a \
                     later row could be misread. {DUMP_CODE_APART}"
                )));
            }
            SessionSetting::ClientCharset(SettingValue::Literal(charset))
                if ASCII_IN_CHARACTERS.contains(&charset.to_ascii_lowercase().as_str()) =>
            {
                return Err(refuse(format!(
                    "the dump sets character_set_client to `{charset}`, in which a character \
                     can hold an ASCII byte such as a backslash, as mysqldump does before a \
                     trigger, routine or event created in that character set: the server \
                     would end strings where GhostDump does not, so a later row could be \
                     misread. {DUMP_CODE_APART}"
                )));
            }
            SessionSetting::SqlMode(value) => check_restored("sql_mode", value, saved)?,
            SessionSetting::ClientCharset(value) => {
                check_restored("character_set_client", value, saved)?;
            }
            SessionSetting::UserVariable { name, saves } => saved.assign(name, saves),
            SessionSetting::Unreadable(change) => {
                return Err(refuse(format!(
                    "the dump changes its settings with {change}, which GhostDump does not \
                     read: standard_conforming_strings or client_encoding could change how the \
                     rest is read, so a row could be misread. Use SET with a literal value"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Refuses a setting whose value GhostDump does not know: one restored
/// from a variable that does not hold a saved value of that setting, and
/// `DEFAULT`, a number or an expression.
fn check_restored(setting: &str, value: SettingValue, saved: &SavedSettings) -> io::Result<()> {
    let what = match value {
        SettingValue::Literal(_) => return Ok(()),
        SettingValue::Variable(variable) if saved.holds(&variable, setting) => return Ok(()),
        SettingValue::Variable(variable) => format!("the variable `@{variable}`"),
        SettingValue::Unknown => "DEFAULT, a number or an expression".to_string(),
    };
    Err(refuse(format!(
        "the dump sets {setting} from {what}, whose value GhostDump does not know, and \
         {setting} decides how the rest of the dump is read. Set it to a literal value, or \
         restore it from a variable saved with `SET @v = @@{setting}`"
    )))
}

/// Whether an encoding name means UTF-8, or ASCII, which is part of it.
/// PostgreSQL ignores case and every character but letters and digits in
/// encoding names, and has `UNICODE` as an alias of `UTF8`.
fn is_utf8(name: &str) -> bool {
    let name: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    matches!(
        name.as_str(),
        "utf8" | "utf8mb3" | "utf8mb4" | "unicode" | "ascii"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::state::SqlDialect;

    #[test]
    fn test_settings_that_change_how_the_dump_is_read_are_refused() {
        // Checks the statements in order, as one dump.
        let check = |statements: &[&str], dialect| {
            let mut saved = SavedSettings::default();
            statements.iter().try_for_each(|sql| {
                check_settings(&Statement::parse(sql.as_bytes(), dialect), &mut saved)
            })
        };
        let pg = SqlDialect::Postgres;
        for read in [
            &["SET client_encoding = 'UTF8';"][..],
            &["SET client_encoding = 'utf_8';"],
            &["SET NAMES 'Unicode';"],
            &["SET standard_conforming_strings = on;"],
            &["SET search_path = '';"],
            &["SELECT pg_catalog.set_config('search_path', '', false);"],
            &["SELECT set_config('standard_conforming_strings', 'on', false);"],
        ] {
            assert!(check(read, pg).is_ok(), "{read:?}");
        }
        for refused in [
            &["SET client_encoding = 'LATIN1';"][..],
            &["SET client_encoding = 'SQL_ASCII';"],
            &["SET client_encoding TO DEFAULT;"],
            &["SET standard_conforming_strings = off;"],
            &["SELECT set_config('standard_conforming_strings', 'off', false);"],
            &["SELECT set_config('client_encoding', 'SJIS', false);"],
            &["SELECT set_config(current_setting('x'), 'off', false);"],
            &["UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';"],
        ] {
            assert!(check(refused, pg).is_err(), "{refused:?}");
        }

        let my = SqlDialect::Mysql;
        for read in [
            &["SET NAMES utf8mb4 */;"][..],
            &["SET NAMES utf8 */;"],
            // mysqldump, at the start and the end of a dump.
            &[
                "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;",
                "SET SQL_MODE=@OLD_SQL_MODE */;",
            ],
            // mysqldump, around a trigger created from a latin1 client.
            &[
                "SET @saved_cs_client      = @@character_set_client */ ;",
                "SET character_set_client  = latin1 */ ;",
                "SET @saved_sql_mode       = @@sql_mode */ ;",
                "SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES' */ ;",
                "SET sql_mode              = @saved_sql_mode */ ;",
                "SET character_set_client  = @saved_cs_client */ ;",
            ],
            &["SET @S = @@session.sql_mode;", "SET sql_mode = @s;"],
            &["SET character_set_client = ujis;"],
        ] {
            assert!(check(read, my).is_ok(), "{read:?}");
        }
        for refused in [
            &["SET NAMES latin1 */;"][..],
            &["SET NAMES sjis;"],
            &["SET CHARACTER SET binary;"],
            &["SET sql_mode = 'ANSI,no_backslash_escapes';"],
            &["SET sql_mode = NO_BACKSLASH_ESCAPES;"],
            &["SET sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES');"],
            &["SET sql_mode = DEFAULT;"],
            &["SET sql_mode = @m;"],
            &["SET @m = 'NO_BACKSLASH_ESCAPES';", "SET sql_mode = @m;"],
            // Saved, then given another value.
            &[
                "SET @m = @@sql_mode;",
                "SET @m := 'x';",
                "SET sql_mode = @m;",
            ],
            &[
                "SET @m = @@sql_mode;",
                "SELECT 'NO_BACKSLASH_ESCAPES' INTO @m;",
                "SET sql_mode = @m;",
            ],
            // Saved from the global value, or from another setting.
            &["SET @m = @@GLOBAL.sql_mode;", "SET sql_mode = @m;"],
            &["SET @m = @@character_set_client;", "SET sql_mode = @m;"],
            &["SET character_set_client = sjis;"],
            &["SET @@session.character_set_client = 'GBK';"],
            &["SET character_set_client = 13;"],
            &["SET character_set_client = DEFAULT;"],
            &["SET @c = 'sjis';", "SET character_set_client = @c;"],
        ] {
            assert!(check(refused, my).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn test_only_the_latest_saved_settings_are_kept() {
        let mut saved = SavedSettings::default();
        for variable in 0..=SavedSettings::MAX {
            saved.assign(format!("v{variable}"), Some("sql_mode".into()));
        }
        assert!(!saved.holds("v0", "sql_mode"));
        assert!(saved.holds("v1", "sql_mode"));
        assert!(saved.holds(&format!("v{}", SavedSettings::MAX), "sql_mode"));
        assert!(!saved.holds("v1", "character_set_client"));
        saved.assign("v1".into(), Some("time_zone".into()));
        assert!(!saved.holds("v1", "sql_mode"));
    }
}
