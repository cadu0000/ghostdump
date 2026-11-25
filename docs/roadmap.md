# Roadmap

How GhostDump evolves, in three phases. Every phase keeps the properties the tool is built on: a single pass, constant memory, no global state, fail closed (a column with a rule is never written unmasked), an exact copy of everything without a rule, and logs without column values.

A critical security or correctness problem found in any phase is fixed at once, outside the order below.

## Risks found in the review of November 2026

| # | Risk | Status |
| --- | --- | --- |
| R1 | A rule that matches nothing is ignored silently: a typo in a table or column name leaves the column in clear. | **fixed**: warning, and an error with `--strict` |
| R2 | Names are compared exactly, so `INSERT INTO Users (Name)` does not match the rule `users.name`. | **fixed**: names compare without case, also outside ASCII |
| R3 | Statements the parser does not treat as data pass through unmasked: `INSERT ... SELECT`, `INSERT ... SET`, `UPDATE`, `MERGE`, `WITH ... INSERT`, `CREATE TABLE AS SELECT`, and the literals of `ON DUPLICATE KEY UPDATE`. | **fixed**: refused when they name a table with rules |
| R4 | The schema is dropped from the table name, so `public.users` and `audit.users` share a rule. | **fixed**: `schema.table` rule keys |
| R5 | On an error in the middle of a run, the output file is left partial on disk. | **fixed**: the output is written next to its target and renamed over it when the run succeeds; on an error the target is untouched or absent |
| R6 | The secret can be passed with `-s`, where it shows in the process list. | **fixed**: `--secret-file`, a warning for `-s` and for a secret file others can read |
| R7 | The `f64` mechanism is still available, with a conditional guarantee. | **fixed**: it runs only with the hidden option `--experimental-f64-sampler`, which warns at startup |
| R8 | Numeric `as` conversions in the exact sampler have no recorded audit. | **fixed**: every conversion is listed with its reason in [decisions/0003](decisions/0003-numeric-conversions.md); the noise parameter is computed in integers |
| R9 | The CLI help is in Portuguese and everything else in English; the only exit code is 1. | **fixed**: exit codes from `sysexits.h` (stage 1.4), the help in English (stage 1.5) |
| R10 | The SQLite dialect has no end-to-end test. | **fixed**: a real dump of each dialect, in its default format, runs end to end (`cli::sqlite_dump_is_masked`, `cli::mysqldump_default_format_is_masked`, `cli::pg_dump_default_format_is_masked`) |
| R11 | CI has no dependency audit, no pinned minimum Rust version and no fuzzing; the mimalloc memory setting depends on `CFLAGS`. | open, stage 1.6 (the fuzz targets exist since stage 1.3) |
| R12 | The rows of an `INSERT` without a column list cannot be matched to rules, so a table with rules is refused. This is the only form `sqlite3`'s `.dump` writes, the default of mysqldump and the form of `pg_dump --inserts`. | **fixed**: the columns come from the `CREATE TABLE` of the same dump |
| R13 | PostgreSQL large objects, which pg_dump includes by default (`SELECT pg_catalog.lowrite(...)`), pass unmasked, and nothing tells the user. | **fixed**: with rules, a warning counts the `lowrite`, `lo_put` and `lo_from_bytea` calls and points to `pg_dump --no-large-objects` |
| R14 | mysqldump writes the `sql_mode` and the character set that each trigger, routine and event was created with before it. One created with `NO_BACKSLASH_ESCAPES`, or in a character set such as sjis, stops a run with rules there. | open: the error names the way to dump them apart; following these settings in the lexer is to be *evaluated* |

Found in November 2026, while fixing R3, and fixed at once as correctness problems: statements were split at every `;`, so the body of a MySQL procedure, a SQLite trigger, a PostgreSQL `BEGIN ATOMIC` function or a rule with several actions was read as data, and its `INSERT ... VALUES` was rewritten; quoted names with a space and `OVERRIDING SYSTEM VALUE` (pg_dump `--column-inserts` for identity columns) were refused; `COPY ... FROM STDOUT`, which psql loads like `FROM STDIN`, was not read as data.

Found in November 2026, in the audit of stage 1.2, and fixed at once: the noise parameter, computed in `f64`, could let Δ/t exceed ε by about 10^-16, relative, when Δ is near 2^54 (346 of 10,000 random cases); the `f64` sampler wrote `inf` when its noise overflowed, and with consistent noise it gave −0 and 0 different noise.

Found in November 2026, in stage 1.3, and fixed at once; each was reproduced with the build of the end of stage 1.2:

* values were decoded as UTF-8 text, replacing the bytes that are not, so different values got one `hmac` pseudonym: the binary values of two rows of a mysqldump (`_binary '...'`), and three different names in a LATIN1 pg_dump. Values are now masked from their bytes, and with rules a dump in another encoding is refused;
* with `mysqldump --databases`, rows name no schema and follow a `USE`, so the rule `[tables."shopq.users"]` did not apply and the rows passed in clear, with only the warning for an unused rule;
* 22 of 73 scripts written by hand leaked a stored value through psql, mysql or sqlite3: version comments, `--` without a space, sqlite3's `#` lines, client commands, and settings that change how strings are read, among them a client character set (sjis) in which a character can end in the byte of a backslash, and settings changed through variables, expressions, `set_config` or `pg_settings` ([client framing](experiments/client-framing.md)). None leaks now;
* a MySQL delimiter made of letters (`DELIMITER T`) that ended a statement inside a word made the parser panic (an index out of range in a release build). The `parser` fuzz target found it.

Found in November 2026, in stage 1.4, and fixed in it; each was reproduced with the build of the end of stage 1.3:

* `--dry-run` with `-o` emptied the output file, although it writes nothing;
* the same file as input and output was emptied before it was read, with exit status 0: a dump of 9.5 MB came out as its first 64 KiB, the bytes read to detect the dialect, and as nothing with `--dialect`. The output now replaces the file only at the end.

## V1: stabilization

Goal: a version that can be trusted. Branch `release/v1`; ends with the tag `v1.0.0` on `main`. Only what affects correctness, security or the ability to diagnose a run belongs here.

| Stage | Work | Priority | Done when |
| --- | --- | --- | --- |
| 1.0 Base | Merge the open work into `dev`; close the issues that are delivered, after checking their acceptance criteria. | critical | **done**: PR #17 merged; issues #3, #6, #7, #8, #10, #14, #15 and #16 closed |
| 1.1 Fail closed, completed | R1: a warning for each rule that matched nothing, and `--strict` to make it an error. R2: names compared without case in every dialect; rule tables that collide after folding are a configuration error. R3: refuse every statement that writes to a table with rules and cannot be masked, and values in clauses after `VALUES`; end statements where psql, mysql and sqlite3 end them. R4: accept `schema.table` as a rule key; a rule without a schema applies to any schema. | critical | **done**: one test per risk shows the column does not leave in clear; real dumps of the three dialects run end to end |
| 1.2 Numeric correctness | R8: audit every conversion in `laplace.rs`, use checked conversions where a loss is possible, test the extremes. R7: keep `f64` for comparison only, behind a hidden experiment option, with a warning at startup. Review `categorical.rs` at the limits of ε. | high | **done**: extreme-value tests pass; no lossy conversion without a written reason ([decisions/0003](decisions/0003-numeric-conversions.md)) |
| 1.3 Inputs and edge cases | Encodings other than UTF-8, rows at the size limit, unusual comments and quoting. R10: end-to-end tests with real dump formats of the three dialects (started with R3 and R12: pg_dump `--inserts` and `--column-inserts`, mysqldump in its default format and with `--complete-insert --routines`, sqlite3 `.dump`). R12, done: the column order comes from the `CREATE TABLE` of the same dump, kept for the tables with rules only; the run stops when it cannot be read with certainty (`AS`, `LIKE`, `INHERITS`, `PARTITION OF`, `OF`, MySQL `INVISIBLE`, a later `ALTER TABLE` that may change columns, a data-only dump). Two `cargo-fuzz` targets built from the property tests. | high | **done**: a real dump of each dialect passes end to end in its default format (R10); the `parser` and `engine` fuzz targets ran 15 minutes each without a failure (1,754,900 and 199,864 inputs); none of the 73 scripts checked against the real clients leaks ([client framing](experiments/client-framing.md)) |
| 1.4 Safe operation | R5: write to a temporary file and rename at the end when `-o` is a file. R6: accept the secret from a file and warn when it comes from `-s`. Distinct exit codes for usage, rules, input and I/O errors. | high | **done**: on an error the target file is absent or untouched, also with `--strict` (`cli::only_a_run_that_succeeds_writes_the_output_file`); exit codes 64, 65, 74 and 78 documented in the README and tested (`cli::exit_statuses_tell_what_failed`) |
| 1.5 CLI and diagnosis | R9: the interface in English. Error messages with table, column and position, never values. R13: a warning when the dump holds large objects and rules are configured. Subcommands, keeping `ghostdump -c ...` as the default form: `check` validates rules against a dump without writing; `init` reads the schema of a dump and writes a starting rules file. | medium | **done**: `check` lists the rules applied, the tables copied as they are and the unused rules, and exits 78 on one (`cli::check_lists_what_the_rules_do`); `init` writes rules that pass `check` on all 30 real dumps of the three dialects (`cli::init_writes_rules_that_pass_check`); every refusal names its input line; warnings about values name their column |
| 1.6 Maintenance | Done ahead, in stage 1.4: the modules that held several parts were split (the settings check of the engine, the reading of SQL text, the states of the parser), and no function is over the 100 lines of clippy's `too_many_lines` but the step of the statement state machine (118); the unused `dotenvy` dependency was removed. Rustdoc on every public item (`missing_docs`). `CHANGELOG`. R11: dependency audit in CI, pinned minimum Rust version, a check that the mimalloc setting is active. Two pipelines: a fast one on every push (format, lint, tests) and a full one on demand and before each release (statistical validation, long property tests, fuzzing, a smoke benchmark). | medium | CI green with the new steps; `cargo doc` without warnings |
| 1.7 Release | Security review of the whole diff, `dev` into `main`, tag `v1.0.0`. | critical | the criteria below hold |

V1 is complete when: no correctness or security issue is open; every invariant in [guarantees](guarantees.md) has a test that fails when it is broken; R1 to R3 are fixed with tests; the three dialects have end-to-end tests; CI audits the dependencies.

## V2: improvement and validation

Goal: measure what the tool delivers and improve where the evidence points. Items marked *evaluate* are analysed for relevance and feasibility first and may be declined.

| Stage | Work | Priority |
| --- | --- | --- |
| 2.1 Utility evaluation, second version | ε in {0.1, 0.5, 1, 2, 5, 10}; dispersion in every table (the raw CSVs already hold one row per repetition); the Adult dataset instead of ACS PUMS; error per column; randomized response with 2, 5, 9 and 16 categories. | high |
| 2.2 Measures over several columns | Counts over two attributes, dependence between columns (Cramér's V, correlation), one classification task. | medium |
| 2.3 Utility warnings at startup | Print the probability that randomized response keeps a value and warn when it is low; guidance on choosing ε per column. | medium |
| 2.4 Comparator with global access | A marginal-based synthesizer. The `pasteur` library (MARE, PrivBayes, AIM, MST) installs in the development environment but has not been run. *Evaluate* with a one-day trial. | medium |
| 2.5 Estimator shipped with the tool | A subcommand `estimate` that reads a masked dump and its rules and returns corrected histograms and shares, in one pass and constant memory. | medium |
| 2.6 Checking properties | Statistical tests of ε for `randomized_response` and consistent noise; a property test for the composition in the privacy summary. Formal verification of the sampler is out for now. | high |
| 2.7 Empirical risk | A linkage or inference experiment on a masked dump. *Evaluate* the scope. | medium |
| 2.8 Controlled benchmarks | A dedicated machine or declared noise, 10^7 rows, CPU profiles; decide the chunked parser with this data. | medium |
| 2.9 Guarantee per person | A two-pass helper that reports the largest number of rows per key. *Evaluate*: it is outside the single pass. | low |

V2 is complete when every claim in `docs/guarantees.md` and `docs/experiments/` can be reproduced by a documented command, with dispersion, and every recommendation of the reviews has a recorded outcome.

## V3: exploration

No closed scope. Candidate directions, each with the evidence that would justify it:

| Direction | Justified if |
| --- | --- |
| Joint randomized response over declared groups of columns | stage 2.2 shows a relevant loss of dependence between columns |
| Local mechanisms for numbers other than Laplace | stage 2.1 shows the numeric error rules out important uses |
| Shuffling in a window | the analysis in [future work](future-work.md) shows a gain for feasible windows |
| Chunked parser and parallel reading | stage 2.8 confirms the parser as the bottleneck on real loads |
| Other input formats and use as a library | there is demand |
| A simple graphical interface for building rules | `init` and `check` are not enough |
| An empirical study of local DP on real tabular data | stages 2.1 and 2.2 give solid results |

## Decisions behind this plan

* **Unused rules warn, unmaskable statements are refused.** An unused rule may come from a rules file shared by several dumps. A statement that writes to a table with rules and cannot be masked would write the sensitive value. Refusing costs nothing on the dumps the tools write: pg_dump, mysqldump and sqlite3 put data only in `INSERT ... VALUES` and `COPY ... FROM stdin`, which was checked on their real output (pg_dump 16 in five formats, mysqldump 8.0 in seven, sqlite3 3.45 in three). The check is by name, without a grammar per dialect: a statement that mentions a table with rules is refused, even when the name is a column or a keyword, so a gap in the parser cannot let a value through.
* **Statements end where the clients end them.** psql ends a statement at a `;` outside parentheses and outside the `BEGIN ... END` body of `CREATE [OR REPLACE] FUNCTION | PROCEDURE` (`psqlscan.l` in the PostgreSQL source, `psqlscan_track_identifier`); mysql at its delimiter, set by `DELIMITER` (MySQL Reference Manual, "mysql Client Commands"); sqlite3 at a `;`, except in `CREATE TRIGGER`, which ends at `; END ;` (`sqlite3_complete`, `src/complete.c` in the SQLite source). Ending statements anywhere else reads the body of a routine as data.
* **Identifiers compare without case, also outside ASCII.** PostgreSQL folds unquoted names to lower case and keeps quoted names as written (PostgreSQL documentation, "Lexical Structure"); in a UTF-8 database it folds only ASCII letters (`downcase_identifier` in `src/backend/parser/scansup.c`). MySQL never distinguishes case in column names, and in table names it depends on `lower_case_table_names` (MySQL Reference Manual, "Identifier Case Sensitivity"); MySQL 8.0 also treats `Ação` and `AÇÃO` as the same column. SQLite treats every identifier, quoted or not, without case ("Like other SQL identifiers, schema names are case-insensitive", SQLite documentation, "Schema Object Names"), but folds only ASCII letters (SQLite FAQ, question 18). Mapping each letter to upper and then lower case covers all of these; it can only apply a rule to one name more, which errs on the safe side. Each rule was also checked on PostgreSQL 16, MySQL 8.0 and SQLite 3.45.
* **The columns of a row without a column list come from the dump's `CREATE TABLE`.** PostgreSQL fills every column in declared order, or the first N (PostgreSQL documentation, "INSERT"); MySQL wants a value for every visible column (MySQL Reference Manual, "INSERT Statement" and "Invisible Columns"); SQLite wants values for the columns that are not generated (SQLite documentation, "Generated Columns", section "Quirks"). Each rule was checked on PostgreSQL 16, MySQL 8.0 and SQLite 3.45. Only the tables with rules are kept, so the memory grows with the rules, not with the rows, and the single pass stays. When the dump does not show the order with certainty, the run stops: a wrong order would mask one column and write another in clear.
* **`f64` stays for comparison only, hidden.** It measures the cost of exact sampling (about 5% end to end, see [benchmarks](benchmarks.md)). Its guarantee depends on rounding hiding the low-order bits of floating-point noise, so it runs only with the hidden option `--experimental-f64-sampler`, which warns at startup.
* **Comments and client commands are read as the clients read them.** mysql runs what `/*! ... */` holds and opens a comment at `--` only before a space or a control character; sqlite3 skips a line that starts with `#`; psql reads an unquoted backslash as a meta-command up to the end of the line or `\\`. Each was checked against psql 16, mysql 8.0 and sqlite3 3.45 ([client framing](experiments/client-framing.md)). With rules, the client commands that pg_dump, mysqldump and sqlite3 never write (all but `\connect`, `\restrict`, `\unrestrict`, `DELIMITER` and `USE`) are refused rather than followed: they can hold data (`\set`), run other files (`\i`, `source`) or change the delimiter (`\d`) or the character set (`charset`), as the clients' own help lists.
* **With rules, only settings GhostDump can read.** The rules are UTF-8, so names in another encoding would not match them, and generated values are written in UTF-8. GhostDump reads strings as pg_dump and mysqldump write them: `standard_conforming_strings` on, backslash escapes in MySQL. A setting that changes either is refused rather than followed, and so is a value GhostDump cannot work out; a variable restores a setting only when it holds a value saved from that setting, which is how mysqldump saves and restores its settings. The character sets refused for `character_set_client` are those in which a character can hold an ASCII byte, found by converting every two-byte sequence on MySQL 8.0. This costs nothing on the real dumps (30 give the same output), but stops a mysqldump of a database with a trigger, routine or event created with such settings (R14).
* **Exit statuses come from `sysexits.h`**: 64 for the command line, 65 for an input that cannot be masked safely, 74 for files that cannot be read or written, 78 for the rules. They are the values the system header defines for those cases, and they differ from 101, the status Rust gives a panic.
* **The output replaces its target only at the end.** It is written to a temporary file in the same directory, flushed to disk and renamed over the target, so a reader sees either the old file or the whole new one. A device or a pipe is written directly: renaming over it would replace it. A file that is replaced keeps its permissions, not its owner.
* **The secret comes from one place.** `--secret-file`, `-s` and `GHOSTDUMP_SECRET` together stop the run: with two of them, a variable left in the environment could silently take the place of the file, and the pseudonyms would change.
* **`init` suggests, `check` verifies.** `init` writes a rule only for columns whose names suggest personal data, with strategies that need no secret, and lists every other column in a comment; a table the dump has no rows of, or SQLite's own tables (`sqlite_`, a prefix sqlite3 reserves), stay commented out, so the file passes `check` on its dump. The names are a hint, not a classification: the file is a start to review. `check` runs the same engine as a run, without writing, so it refuses and warns where a run would; it needs no secret, since nothing it computes is written.
* **Errors name the input line.** The run counts line ends per buffer read, with memchr: about 1% more instructions on a run that copies a COPY block (callgrind, 9.7 MB). Wall-clock timings on this machine vary by 7% between two copies of one binary, too much to measure it otherwise.
* **Fuzzing runs on demand and in the full pipeline**, because `cargo-fuzz` needs the nightly compiler.
* **The interface is in English.**
