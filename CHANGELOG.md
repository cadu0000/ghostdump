# Changelog

Notable changes to GhostDump. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The V1 stabilization, stages 1.1 to 1.6 of the [roadmap](docs/roadmap.md), against the `dev` branch at commit `9d086cf`. What GhostDump still does not do is in [limitations](docs/limitations.md).

### Added

- `ghostdump check`: checks rules against a dump without writing it, and lists the rules applied, the tables copied as they are, the rules that match nothing, and what would stop a run.
- `ghostdump init`: writes a starting rules file from the tables and columns of a dump, with a strategy suggested from each column name.
- `--secret-file`, which keeps the HMAC secret out of the process list and the shell history. A warning names a secret file that other users can read, and a secret given with `-s`.
- `--strict`: a rule that matches no table or column of the input stops the run; without it, the rule only warns.
- Exit statuses from `sysexits.h`: 64 for the command line, 65 for an input that cannot be masked safely, 74 for reading or writing, 78 for the rules. Failures used to exit with 1, and command-line errors with 2.
- A refusal names the input line where the run stopped.
- The values a rule could not take as they were (not numbers for `dp_laplace`, outside the categories for `randomized_response`) are counted per rule, and the warning names its column.
- With rules, a warning counts the PostgreSQL large objects the dump writes (`lowrite`, `lo_put`, `lo_from_bytea`), which GhostDump copies as they are.
- Rules can name a schema (`[tables."shop.users"]`); rows that name none follow MySQL `USE`.
- Rows without a column list take their columns from the `CREATE TABLE` of the same dump: the only form of sqlite3 `.dump`, the default of mysqldump, and `pg_dump --inserts`. A table with rules used to be refused in that case.
- [Limitations](docs/limitations.md), each with its evidence.
- Rustdoc on every public item of the library.

### Changed

- The output file is written under a temporary name in its directory and renamed over the target only when the run succeeds. The replaced file keeps its permissions.
- Names in rules are matched without regard to case, in every dialect.
- The `f64` Laplace sampler needs the hidden option `--experimental-f64-sampler`.
- The help is in English, like the rest of the interface, and says what each `--dialect` value means.
- Rust 1.88 or later is required (`rust-version`).
- A key that the rules file does not know is an error (exit status 78): in a rule, in a table, or at the top. Unknown keys used to be ignored.

### Fixed

- The same file as input and output was emptied before it was read, with exit status 0.
- `--dry-run` with `-o` emptied the output file.
- A `.gz` input made of several gzip members (`cat a.gz b.gz`) was read only up to the end of the first member, and the rest of the dump was dropped without an error.
- Statements were split at every `;`, so the `INSERT ... VALUES` in the body of a MySQL procedure, a SQLite trigger, a PostgreSQL `BEGIN ATOMIC` function or a rule with several actions was read as data and rewritten.
- Data headers with a quoted name that holds a space, or with `OVERRIDING SYSTEM VALUE` (`pg_dump --column-inserts` on identity columns), were refused; `COPY ... FROM STDOUT` was not read as data.
- A MySQL delimiter made of letters (`DELIMITER T`) made the parser panic.
- The noise parameter, computed in `f64`, could exceed ε by about 10^-16, relative; the `f64` sampler wrote `inf` when its noise overflowed; with consistent noise, −0 and 0 got different noise.

### Security

- The temporary output file is created no more open than the file it replaces. Since the output was first written under a temporary name, it was created with the default permissions and given the target's only after, so a local user could open it in between and read the whole output of a run whose target was 0600 (19 of 2,000 runs, with a reader started on each new file).
- Values are masked from their bytes. They used to be decoded as UTF-8, so different values that are not valid UTF-8 got one `hmac` pseudonym (the `_binary` values of a mysqldump, LATIN1 text of a pg_dump); with rules, a dump in an encoding other than UTF-8 is now refused.
- With `mysqldump --databases`, rows name no schema and follow a `USE`; rules that named the schema did not apply, and the rows passed in clear.
- 22 of 73 scripts written by hand leaked a stored value through psql, mysql or sqlite3: version comments, `--` without a space, sqlite3's `#` lines, client commands, and settings that change how strings are read. None leaks now. With rules, these now stop the run: statements that write to a table with rules in a form that cannot be masked (`UPDATE`, `INSERT ... SELECT`, `CREATE TABLE ... AS SELECT`), values written after the rows (`ON DUPLICATE KEY UPDATE`), settings that change how strings are read, and client commands that dump tools do not write.
- A misspelt optional key of a rule, such as `null_polcy = "generate"`, was ignored without a word, so the rule protected less than it said: here, NULL values kept their pattern. Such keys are now errors.
- Names from the dump reach the terminal with their control characters escaped: a quoted name could hold escape sequences that recolor the terminal or move its cursor.
- The CI workflows run with a read-only token.
- `rand` 0.8.6, patched for RUSTSEC-2026-0097. GhostDump was not exposed: the advisory needs a custom logger that calls `rand::thread_rng()`.
- CI audits the dependencies with `cargo audit`.
