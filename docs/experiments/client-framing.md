# Unusual comments, quoting, client commands and settings, checked against the real clients

Stage 1.3 of the [roadmap](../roadmap.md). GhostDump decides where a statement ends, and whether a byte is code, a string or a comment, the way the client that loads the dump does. Where it decides differently, a value can leave in clear: a quote that GhostDump reads as the start of a string, and the client does not, hides the `INSERT` after it. A setting that changes how strings are read (an encoding, backslash escapes) does the same for the rest of the dump.

## Method

`scripts/client_framing.py` holds 73 small scripts (27 for psql, 34 for mysql, 12 for sqlite3), each with a table `users` and at least one value written to `users.name` in an unusual way. Each script:

1. runs in the real client on a scratch database: psql 16.15, mysql 8.0.46, sqlite3 3.45.1, with servers of the same versions;
2. the values the database stored in `users.name` are read back: these are the secrets;
3. GhostDump runs on the same script with a rule on `users.name` (`fixed`), and its output is searched for each secret.

A case **leaks** when a stored value appears in the output, is **masked** when none does, and is **refused** when GhostDump stops with an error, which is the fail-closed outcome. Run it with:

```bash
cargo build --release
PGHOST=/path/to/socket python3 scripts/client_framing.py
```

`--binary` runs another build, such as one from an earlier commit.

## Cases

* **PostgreSQL:** nested comments with a quote inside, dollar quotes, `E''` and `U&''` strings, string constants continued on the next line, psql meta-commands (`\echo it's`, `\set` with `:'v'`, `\g`, the `\\` separator, mid-line), comments between the tokens of an `INSERT`, quoted names, `COPY` escapes, a BOM with CRLF line ends.
* **MySQL:** `#` and `-- ` comments, `--` not followed by a space (`1--1` is arithmetic), version comments (`/*! ... */`, also split by a `;` inside), MariaDB's `/*M!`, the `\g`, `\G`, `\c`, `\d` and `\!` commands, `DELIMITER`, introducers (`_utf8mb4'...'`, `_binary '...'`, `N'...'`), hex literals, `ANSI_QUOTES`, `INSERT ... SET`.
* **SQLite:** `[...]` and backtick names, dot-commands (`.print it's`), lines starting with `#`, comments that do not nest, double-quoted strings, blobs.
* **Settings:** in mysql, `character_set_client` set to sjis (a character set in which byte 0x5C, the backslash, can end a character) by name, by its number (13) and from a variable, and `NO_BACKSLASH_ESCAPES` set from a variable, from an expression, and from a saved `sql_mode` assigned again by `SELECT ... INTO`; the way mysqldump saves and restores `sql_mode` and `character_set_client`. In psql, `standard_conforming_strings` and `client_encoding` changed with `set_config` and with `UPDATE pg_settings`.

## Results

| Build | Leaks | Masked | Refused |
| --- | --- | --- | --- |
| Before stage 1.3 (end of stage 1.2) | 22 | 44 | 7 |
| Stage 1.3 | 0 | 47 | 26 |

The 22 leaks, and what changed:

* **MySQL version comments** (1 case, now masked). mysql runs what `/*! ... */` holds and ends statements at a `;` inside it; an `INSERT` there passed in clear. GhostDump now reads the content as code, after the version number. `/*M!` and optimizer hints (`/*+`) stay comments, as they are for mysql 8.0.
* **MySQL `--`** (3 cases, now masked). MySQL opens a comment at `--` only before a space or a control character: after `DO 1--1;`, the `INSERT` on the next line passed in clear, because GhostDump read the `;` as part of a comment. It now follows the same rule.
* **sqlite3 `#` lines** (1 case, now masked). sqlite3 skips a line that starts with `#`; GhostDump read the quote in `# it's` as the start of a string. It now skips the line too.
* **Client commands** (8 cases, now refused). psql reads an unquoted backslash as a meta-command, anywhere in a line, until the end of the line or `\\`; mysql reads `\g`, `\d`, `\!`, ... anywhere, and `source`, `charset`, ... at the start of a statement; sqlite3 reads a line that starts with `.` as a dot-command. A quote inside such a command hid the next `INSERT`, a command could change the delimiter, and `\set v 'SECRET'` then `VALUES (:'v')` put the value in a psql variable. With rules, GhostDump now stops at any client command that pg_dump, mysqldump and sqlite3 do not write (they write only `\connect`, `\restrict` and `\unrestrict`, besides mysql's `DELIMITER` and `USE`).
* **Settings** (9 cases, now refused). After `SET character_set_client = sjis`, mysql reads `'<0x95>\'` as a complete string, GhostDump read `\'` as an escaped quote, and every `INSERT` after it passed in clear; the same with the number 13 or a variable holding `'sjis'`. A `NO_BACKSLASH_ESCAPES` that GhostDump did not see (from a variable or an expression) did the same with `'a\'`. In psql, `standard_conforming_strings` turned off with `set_config` or with an `UPDATE pg_settings` did it with `'x\''`, and `set_config('client_encoding', 'SJIS', false)` with an `E''` string. With rules, GhostDump now refuses a client character set in which a character can hold an ASCII byte (big5, cp932, euckr, gb18030, gbk, sjis, found by converting every two-byte sequence on MySQL 8.0), and a setting whose value it cannot work out: `DEFAULT`, a number, an expression, a variable that does not hold a value saved from the same setting with `SET @v = @@setting`, or a `set_config` whose setting is not a string. A variable named by another statement (`SELECT ... INTO @v`) no longer counts as saved.

The other changes: three `\echo` cases that were masked are now refused, as client commands, and `1--1`, which was refused, is now masked. The two save-and-restore cases written as mysqldump writes them stay masked.

The 26 refusals are 13 client commands (six `\echo` and one `\set` in psql, `.print` in sqlite3, `\g`, `\G`, `\c`, `\d` and `\!` in mysql), the 9 settings above, and four cases refused before as well: `\g` after the rows of an `INSERT` in psql and in mysql (values after the rows), `INSERT ... SET`, and `.print` inside a statement, whose table name cannot be read (sqlite3 itself stores nothing).

## Limits

* The cases are written by hand. pg_dump, mysqldump and sqlite3 write none of these forms but the version comments, introducers, comments and the save and restore of settings, so the [real dumps](../roadmap.md) are unchanged by the fixes: 30 of them give the same output before and after.
* Data, and settings, that reach the database through code are still not seen: a function or procedure body, a `DO` block, a prepared statement, a variable assigned by code. GhostDump follows only the `SET` statements and, in PostgreSQL, the `set_config` calls it reads.
* Statement ends, comments, client commands and settings follow psql 16, mysql 8.0 and sqlite3 3.45. Other clients, such as MariaDB's, were not checked.
