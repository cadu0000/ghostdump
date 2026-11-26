# Limitations

What GhostDump does not do, or does only under conditions, in one place. Each item says what happens, how it was established, and what to do about it. The list grows with each stage of the [roadmap](roadmap.md); what the strategies guarantee is in [guarantees.md](guarantees.md).

## Privacy model

* **One value is the unit of protection.** No strategy hides whether a row exists, how many rows a table has, their order, or the schema. A person in k rows spends k·Σε over the `dp_laplace` columns of a row, and T releases with fresh noise spend T·k·Σε; GhostDump sees neither k nor T, so it cannot account for them. Source: [guarantees](guarantees.md), P2 and the "Whole dump" row.
* **A column without a rule is copied as it is**, and can re-identify a row whatever the ε of its other columns. Source: the guarantees table.
* **`hmac` is not differentially private.** Equal values give equal pseudonyms, which keeps joins and value frequencies; anyone with the secret can brute-force small domains (sequential IDs, CPF). Source: the guarantees table.
* **Bounds must not come from the data.** Bounds computed from the data, as the "oracle" arm of the utility evaluation does, are not private. Source: guarantees, Assumptions; [utility evaluation](experiments/utility.md).
* **The noise is computationally random.** It comes from ChaCha12 seeded by the operating system; `--seed` makes it reproducible and removes the protection. Source: guarantees, Assumptions.
* **The exact Laplace sampler is our implementation**, checked against the exact probabilities, by goodness of fit and by negative controls, not audited. Source: [decisions/0001](decisions/0001-laplace-sampler.md), `laplace::validation`.
* **The `f64` sampler leaks when its output keeps low-order bits**, and its guarantee holds only for coarse `scale` values; it runs only with the hidden option `--experimental-f64-sampler`. Source: [floating-point experiment](experiments/floating-point.md).

## What GhostDump does not see

These pass as they are, with or without rules.

* **Data and settings that reach the database through code**: the bodies of functions, procedures, triggers and rules, `DO` blocks, prepared statements, procedures run with `CALL`, and SQL variables (MySQL `SET @v = ...`). GhostDump reads, and refuses when needed, only the statements it holds whole (`SET`, `SELECT`, `WITH`, `UPDATE`, `DELETE`, `MERGE`, `LOAD`, `CREATE TABLE`, `ALTER`, `DROP`, `RENAME`, MySQL `USE`) and the data blocks; a variable assigned by any other statement (`DO @v := ...`, `CALL p(@v)`) is not followed. Source: README, "What GhostDump does not see"; the list of held statements in `src/parser/state/normal.rs`; [client framing](experiments/client-framing.md), Limits.
* **PostgreSQL large objects** are copied: no rule applies to them. With rules, a warning counts the `lowrite`, `lo_put` and `lo_from_bytea` calls. Dump with `pg_dump --no-large-objects` (`-B`) when they may hold personal data. Source: `cli::large_objects_are_copied_with_a_warning`, on a real pg_dump 16.15 dump.
* **Settings the dump does not set** are taken to be the ones GhostDump reads: a UTF-8 (or ASCII) client encoding, `standard_conforming_strings` on, and an `sql_mode` without `NO_BACKSLASH_ESCAPES`. A server whose defaults differ reads the dump differently. Source: guarantees, Assumptions.
* **Clients other than psql, mysql and sqlite3 were not checked.** MariaDB's `/*M! ... */` comments are read as comments, as mysql 8.0 reads them; the mariadb client, which may run them, was not tested. Source: [client framing](experiments/client-framing.md).
* **The dialect is guessed from the first 64 KiB** (the markers pg_dump, mysqldump and sqlite3 write at the top); without one, GhostDump warns and assumes PostgreSQL. A data-only sqlite3 dump has no marker and is read as PostgreSQL: pass `--dialect`. Source: `detect_dialect`; seen on the data-only sqlite3 dump in stage 1.5.

## Dumps refused, to fail closed

With rules, these stop the run (exit status 65); the error names the input line where GhostDump stopped, unless the input ended first. Each refusal can stop a real dump; it is the price of never writing a value in clear.

* **Rows without a column list whose column order is not certain**: no `CREATE TABLE` before them (data-only dumps, `mysqldump --no-create-info`), a `CREATE TABLE` that does not list every column (`AS SELECT`, `LIKE`, `INHERITS`, `PARTITION OF`, `OF`, MySQL `INVISIBLE` columns), or a later statement that may change the columns. Source: README, fail closed; `engine::tests::test_rules_without_column_list_fail_closed`.
* **`COPY` with options** such as `WITH (FORMAT csv)`. Source: `engine::tests::test_copy_with_options`.
* **Statements that write to a table with rules in a form that cannot be masked** (`UPDATE`, `INSERT ... SELECT`, `CREATE TABLE ... AS SELECT`, ...). The check is by name, so a column or a keyword spelt like such a table stops the run too: a false refusal, on the safe side. Source: `engine::tests::test_statements_that_write_to_tables_with_rules_are_refused`.
* **Settings that change how strings are read**: an encoding other than UTF-8, `standard_conforming_strings` off, `NO_BACKSLASH_ESCAPES`, a MySQL client character set in which a character can hold an ASCII byte (big5, cp932, euckr, gb18030, gbk, sjis, found by converting every two-byte sequence on MySQL 8.0), and a setting whose value GhostDump cannot work out (`DEFAULT`, a number, an expression, a variable not saved from the same setting, `set_config` with a setting that is not a string, `UPDATE pg_settings`). Without the refusal, 9 scripts leaked a stored value through the real clients. Source: [client framing](experiments/client-framing.md); `engine::settings` tests.
* **Triggers, routines and events that mysqldump writes with such a setting (R14).** mysqldump writes the `sql_mode` and character set each of them was created with before it, and restores them after. One created with `NO_BACKSLASH_ESCAPES`, or in one of the character sets above, stops the run there, after the rows of its table. Following the setting would be possible but touches how every statement is framed; in November 2026 the refusal was kept. The way around it, checked with mysqldump 8.0.46: dump the data with `mysqldump --skip-triggers` (without `--routines` and `--events`), which has no such setting, and the triggers, routines and events apart, with no rows (`mysqldump --no-data --no-create-info --routines --events`). Source: `cli::triggers_without_backslash_escapes_stop_the_run`, on a real mysqldump; [roadmap](roadmap.md), R14.
* **Client commands that the dump tools do not write** (psql meta-commands other than `\connect`, `\restrict` and `\unrestrict`; mysql commands other than `DELIMITER` and `USE`; sqlite3 dot-commands). Source: `parser::tests::test_client_commands_are_refused_unless_dump_tools_write_them`.
* **Rows that name no schema**, when the only rules for their table name one and no `USE` says which schema the rows go to. Source: `cli::rules_with_a_schema_follow_mysql_use`.
* **Rows and held statements longer than `--max-row-size`** (256 MiB), and an input that ends inside a row. These stop the run without rules too. Source: `engine::tests::test_rows_at_the_limit_pass_and_longer_ones_stop`.

## Operation

* **The output file is written in its directory under a temporary name** and renamed over the target at the end: GhostDump needs write permission on that directory, and the replaced file keeps its permissions, not its owner. Devices and pipes are written directly, without this protection. Source: README; `cli::only_a_run_that_succeeds_writes_the_output_file`.
* **A new output file gets the permissions the umask gives**, 0644 with the usual 022, so other users can read the columns that no rule masks. Run with `umask 077`, or create the file with the permissions it needs first: a file that is replaced keeps its own, and its temporary file is never more open than it. Source: `io::tests::the_temporary_file_is_never_more_open_than_its_target`; checked in stage 1.7 with `stat`.
* **A run stopped by a signal leaves its temporary file.** After SIGINT, SIGTERM or SIGKILL the target is untouched, but `.<name>.<random>.tmp` stays next to it, holding the part of the output written so far; remove it by hand. Checked in stage 1.6 by signalling a run once the temporary file existed.
* **A `.gz` input must end with its last gzip member.** Every member is read, as gunzip reads them; bytes after the last one, even the zeros gunzip ignores, stop the run with exit status 74. Decompress such a file with `gunzip -c` and pipe it in. Until stage 1.6 only the first member was read, and the rest of the dump was dropped without an error. Source: `cli::every_member_of_a_gzip_input_is_read`.
* **Memory is constant in the number of rows, not in their size**: a row, or a statement held whole, is kept in memory up to `--max-row-size`; with `--threads`, at most twice as many segments as threads are out at once. `init` keeps the names of every table and column. Source: [decisions/0002](decisions/0002-parallel-masking.md).
* **Timings on the development machine vary by 5 to 7%** between runs of one binary, so smaller differences cannot be seen in wall-clock time; the cost of counting input lines was measured in instructions (callgrind) instead. Source: [benchmarks](benchmarks.md), reference machine; roadmap, stage 1.5 decisions.

## Utility

What a masked dump is good for, measured in the [utility evaluation](experiments/utility.md) on synthetic data and on UCI Adult.

* **SQL on a masked numeric column answers means, little else.** On Adult and on 10⁵ synthetic rows or more, at ε = 1, counts in a range are 75–99% off, the 90th percentile 145–270%, and the histogram distance 0.63–0.87, and more rows do not help; at ε = 10, still 11–93%, 5–61% and 0.10–0.42. Means come within about 2% from ε = 0.5 on. Source: utility evaluation, findings 1 and 4.
* **Correcting for the noise takes post-processing that GhostDump does not ship.** The deconvolution of the evaluation brings quantiles and smooth histograms back, but not a value that holds a large share of a column (40 hours, 47% of Adult's records: 92% off at ε = 1), and as configured it overfits on real data. Source: utility evaluation, findings 2 and 3; roadmap, stage 2.5.
* **Relations between columns need large ε, paid by every record.** With eight columns masked at ε each, a record spends 8ε. Read with SQL, the counts over two columns are 0.34–0.61 off at ε = 1, and Cramér's V and a correlation lose most of their value up to ε = 2. Inverting randomized response recovers a strong dependence from ε = 1 on, but on weak ones the leftover noise reads as dependence, and with 144 cells the inversion is worse than SQL at ε ≤ 0.5. A classifier trained on the masked rows predicts only the majority class at ε = 1, and is within 0.01 of AUC of one trained on real rows from ε = 5 on, where a record spends 40. Source: utility evaluation, Several columns.
* **Randomized response loses more with more categories.** Through the unbiased estimator, the distance to the true shares at ε = 1 and n = 48,842 is 0.002 with 2 categories and 0.073 with 16, as the variance of k-ary randomized response predicts; read straight with SQL, the shares are pulled toward uniform. Source: utility evaluation, finding 8.

## Tools

* **`init` suggests rules from column names.** It is a hint, not a classification: a column `name` of a table of products gets `faker_name`, and personal data in a column named `notes` gets no rule. Review every line. Source: `inventory::tests::test_suggestions_follow_the_column_name`.
* **`init` checks that its rules file does not exist when it starts**; a file created at that path while it reads the dump is replaced, because the rules are written under a temporary name and renamed at the end. Source: `run_init` and `PendingOutput::commit` in the code.
* **`check` runs `hmac` rules with a placeholder secret**: it shows what a run would apply and refuse, not the pseudonyms.

## Scope of the verification

* **Clients and dump tools**: psql and pg_dump 16.15, mysql and mysqldump 8.0.46, sqlite3 3.45.1.
* **Compilers**: Rust 1.88, the minimum, in CI, and 1.97 on the development machine.
* **Real dumps**: 30, of which 12 from pg_dump 16.15, 14 from mysqldump 8.0.46 and 4 from sqlite3, in their default formats and with the options that change how rows are written. Every change is checked to keep their output byte for byte.
* **Edge cases**: 73 scripts written by hand, run in the real clients ([client framing](experiments/client-framing.md)).
* **Fuzzing**: 15 minutes per target in stage 1.3 (1,754,900 inputs for the parser, 199,864 for the engine), and 5 minutes on the engine after each later change to it; no long campaign yet. Since stage 1.6 the full pipeline fuzzes each target for 15 minutes, by hand and before each release.
* **Property tests**: 256 cases per property in every run of the tests, 1,000,000 in the full pipeline.
* **Utility**: synthetic data, and one real dataset, UCI Adult (48,842 records from 1994): single columns, pairs of columns and one classification task; no relation between tables ([utility evaluation](experiments/utility.md), Limitations).
* **Performance**: one shared machine ([benchmarks](benchmarks.md)).
