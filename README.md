
#  GhostDump: High-Throughput SQL Stream Sanitizer

[![Rust](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/status-Active-success.svg)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-green.svg)](#license)

**GhostDump** anonymizes terabyte-scale SQL dumps in a single streaming pass, preserving relational integrity while protecting sensitive data.

It is a high-performance command-line interface (CLI) tool written in Rust. It acts as a stream processor to anonymize, pseudonymize, and apply Differential Privacy to massive SQL database dumps **on the fly**, without requiring active database connections or loading the dataset into memory.

---

##  Features

- **High-Throughput Streaming**: Processes massive files using Unix Pipes or direct file I/O.
- **Finite State Machine (FSM) Parser**: Reads SQL byte-by-byte, accurately identifying structural commands and comments without relying on fragile regular expressions.
- **Constant Memory Footprint**: Uses a sliding window architecture that keeps memory usage constant regardless of the dump size.
- **Hybrid Masking Strategy**: Supports deterministic HMAC (to preserve JOINs), Local Differential Privacy (Laplace mechanism for numerics), and Faker generation for PII.
- **Pass-through Proxy**: Any table or structural command (`CREATE TABLE`, `ALTER`, comments) not explicitly mapped in the rules is written to the output completely untouched.

---

##  Architecture & Performance Philosophy

GhostDump is designed around a **streaming-first architecture** inspired by the **Pipes and Filters** pattern. In this model, data flows through a sequence of independent processing stages, where each stage performs a specific transformation.

Instead of loading the entire SQL dump into memory, the tool processes the input as a continuous byte stream. 

```text
Compressed SQL Dump
       │
       ▼
  Stream Reader
       │
       ▼
 FSM SQL Parser
       │
       ▼
  Row Codec
       │
       ▼
 Masking Engine (HMAC / Faker / DP)
       │
       ▼
 Streaming Writer
       │
       ▼
Sanitized SQL Dump

```

This approach enables:

* **Single-pass processing**: The SQL dump is parsed only once.
* **High composability**: Easy integration with other Unix tools (`zcat`, `gzip`).
* **Lazy masking strategies**: Transformations are applied only to mapped columns.
* **Parallel masking** (`--threads N`): the reader groups rows into segments of at most 1,024 masked rows, N threads mask them, and the writer puts them back in input order. At most 2N segments are in flight, so memory stays bounded, and a seeded run writes the same output for any N.

---

##  Security Model & Deterministic Hashing

GhostDump aims to protect sensitive information while preserving the structural integrity of the dataset for analytics and development.

To maintain relational integrity without maintaining heavy in-memory mapping tables, GhostDump uses **deterministic HMAC-based pseudonymization** for primary and foreign keys. The same input always produces the same pseudonymized identifier, allowing `JOIN` operations to remain valid.

**Example:**

*Original dataset:*

```text
user_id | order_id
5       | 12
5       | 14

```

*After anonymization:*

```text
user_id | order_id
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | 114bd151f8fb0c58642d2170da4ae7d7c57977260ac2cc8905306cab6b2acabc
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | 114bd151f8fb0c58642d2170da4ae7d7c57977260ac2cc8905306cab6b2acabc

```

---

##  Supported SQL Dialects

GhostDump's FSM parser is designed to handle the most common SQL dump formats. The dialect is detected from the dump header (`--dialect auto`, the default) or chosen with `--dialect postgres|mysql|sqlite`. A statement ends where the client that loads the dump ends it: psql at a `;` outside parentheses and outside the `BEGIN ATOMIC ... END` body of a function, mysql at its delimiter (changed by `DELIMITER`, and also at the end of a `USE` line), sqlite3 at a `;`, or at `; END;` for a trigger. Comments are read as each client reads them: MySQL runs what a `/*! ... */` comment holds, so GhostDump reads it as code; MySQL opens a comment at `--` only before a space (`1--1` is arithmetic); sqlite3 skips a line that starts with `#`. The bodies of functions, procedures, triggers and rules are code: they are copied as they are.

* **PostgreSQL**: `COPY ... FROM stdin;` blocks in text format and `INSERT` statements (`pg_dump --inserts` and `--column-inserts`, also with `--rows-per-insert` and `--on-conflict-do-nothing`, and with `OVERRIDING SYSTEM VALUE` for identity columns).
* **MySQL**: multi-row `INSERT`, `INSERT IGNORE` and `REPLACE` statements, as mysqldump writes them with or without `--complete-insert`.
* **SQLite**: `INSERT` and `INSERT OR ...` statements, as the `.dump` command of `sqlite3` writes them.

An `INSERT` without a column list fills the columns of the table in order. GhostDump takes that order from the `CREATE TABLE` that comes before the rows in the same dump, as the database does: every column in PostgreSQL and MySQL (a generated one takes `DEFAULT`), every column but the generated ones in SQLite. It keeps the order of the tables with rules only, so its memory grows with the rules, not with the rows. A table is found by its name as the database tells it apart: PostgreSQL folds unquoted names to lower case and keeps quoted ones, MySQL keeps the case of table names, SQLite ignores it.

Quoted names are read with their spaces and escaped quotes: `"my table"`, `` `my table` ``, `[my table]`.

GhostDump fails closed: when its rules cannot be applied with certainty, it stops with an error instead of writing the values unmasked. This happens when:

* an `INSERT` has no column list and the dump does not show the columns with certainty: no `CREATE TABLE` for the table comes before (a data-only dump, such as `sqlite3`'s `.dump --data-only` or `mysqldump --no-create-info`), its `CREATE TABLE` does not list them all (`AS SELECT`, `LIKE`, `INHERITS`, `PARTITION OF`, `OF`, MySQL `INVISIBLE` columns), or a later statement may have changed them (`ALTER TABLE ... ADD COLUMN`, `DROP COLUMN`, `RENAME`, `DROP TABLE`);
* a `COPY ... FROM stdin` uses options such as `WITH (FORMAT csv)`, or the table name of a data block cannot be read;
* a statement that writes data in another form names a table with rules: `INSERT ... SELECT`, MySQL's `INSERT ... SET`, `UPDATE`, `DELETE`, `MERGE`, `LOAD DATA`, `COPY ... FROM 'file'`, `CREATE TABLE ... AS SELECT`, `SELECT ... INTO`, and `WITH` holding any of these. The check is by name, so a column or keyword spelt like such a table stops the run too. pg_dump, mysqldump and sqlite3 never write these statements;
* the clause after the rows of an `INSERT` into a table with rules holds a value, as `ON DUPLICATE KEY UPDATE name = 'x'` does (`ON CONFLICT DO NOTHING` holds none);
* there are rules and the dump changes how it must be read: it declares an encoding other than UTF-8 (`SET client_encoding`, `SET NAMES`, PostgreSQL's `set_config('client_encoding', ...)`), sets PostgreSQL's `standard_conforming_strings` to anything but `on`, sets MySQL's `NO_BACKSLASH_ESCAPES`, or sets MySQL's `character_set_client` to a character set in which a character can hold an ASCII byte (big5, cp932, euckr, gb18030, gbk, sjis). A value GhostDump cannot work out is refused too: `DEFAULT`, a number, an expression, a variable that does not hold a value saved from the same setting (`SET @v = @@sql_mode`, as mysqldump does), a `set_config` whose setting is not a string, an `UPDATE` of `pg_settings`. Dump in UTF-8: `pg_dump -E UTF8`, `mysqldump --default-character-set=utf8mb4` (the default of mysqldump 8.0). mysqldump also writes the `sql_mode` and character set a trigger, routine or event was created with before it: when those stop the run, dump the data without them (`mysqldump --skip-triggers`, without `--routines` and `--events`) and dump them apart, which gives no rows (`mysqldump --no-data --no-create-info --routines --events`);
* there are rules and the dump runs a client command that pg_dump, mysqldump and sqlite3 do not write: a psql meta-command other than `\connect` (`\c`), `\restrict` and `\unrestrict` (such as `\set`, `\copy` or `\g`), a mysql command (`\g`, `\d`, `source`, `charset`, ...) other than `DELIMITER` and `USE`, or a sqlite3 dot-command. These commands can carry data, read other files, or change the delimiter or the encoding;
* the rows of a table name no schema, the dump does not say which schema they go to (`mysqldump --databases` says it, with `USE`), and the only rules for the table name a schema, as `[tables."shop.users"]` does.

Rows, and the statements GhostDump holds to check them, also stop the run when they are larger than `--max-row-size` (256 MiB by default). Every error that stops a run names the line of the input where GhostDump stopped: the line that ends the statement, header or row it refuses.

Values are masked from their bytes, unescaped but not decoded: two values that differ in any byte, such as binary data, keep different `hmac` pseudonyms.

What GhostDump does not see: data that reaches a table through code (the body of a function, procedure, trigger or rule, a `DO` block, a prepared statement, an SQL variable such as MySQL's `SET @v = ...`), settings that code changes, and PostgreSQL large objects, which pg_dump writes as `SELECT pg_catalog.lowrite(...)`. Dump with `pg_dump --no-large-objects` (`-B`) when large objects hold personal data.

---

##  Roadmap & Current Status

GhostDump is currently under active development. The core parsing engine is functional, while some integration features are still in progress.

* [x] **Core Engine**: FSM byte-by-byte SQL parsing and structural bypassing.
* [x] **Zero-Memory Proxy**: Streaming architecture implementation.
* [x] **CLI Foundation**: Robust argument parsing using `clap`.
* [x] **Masking Strategies**: Deterministic HMAC and Faker-based PII anonymization.
* [x] **Differential Privacy**: Laplace mechanism with bounds, clamping and fixed precision.
* [x] **Rule Engine Integration**: Connect the `rules.toml` to the FSM.
* [x] **UX & Metrics**: Real-time progress metrics (`-p`, `indicatif`) and an end-of-run summary (rows, masked fields, MiB/s).
* [x] **Structured Logging**: Verbose debug mode (`--verbose`) and `GHOSTDUMP_LOG`.
* [x] **Edge Cases**: Advanced handling of `NULL` values (`null_policy`).
* [x] **Parallel Processing**: Masking on a pool of threads (`--threads`), with the output in input order.

---

##  Configuration as Code

GhostDump relies on two fundamental configuration components, separating **business rules** from **cryptographic secrets**.

### 1. The Rule File (`rules.toml`)

This file maps exactly how each column should be treated. It **should be versioned in Git** to ensure consistency across the engineering team. Tables/columns not declared here will be bypassed.

```toml
[tables.users]
columns = [
    # Deterministic Masking (Keeps Foreign Keys working)
    { name = "id", strategy = "hmac" },
    
    # Random Anonymization (PII)
    { name = "name", strategy = "faker_name" },
    
    # Fixed Value (Allows the dev team to log in with a known password)
    { name = "password_hash", strategy = "fixed", value = "$2a$12$R9h/cIPz0gi..." },
    
    # Local Differential Privacy (Laplace Mechanism for numerics)
    { name = "salary", strategy = "dp_laplace", epsilon = 0.5, bounds = [0.0, 15000.0], scale = 2 }
]

```

Only the declared columns are rewritten; every other byte of the dump, including the layout of `INSERT` statements and `COPY` blocks, is copied unchanged. Values keep their SQL type: numbers stay unquoted, generated text is escaped for the dialect, and `NULL` stays `NULL` by default. Add `null_policy = "generate"` to a column to mask `NULL` values too. A key that the rules file does not know, such as a misspelt `null_policy`, is an error, not ignored.

Table and column names are matched without regard to case, also for letters outside ASCII (MySQL compares column names this way), and quotes are ignored, so a rule for `users.name` also applies to `INSERT INTO "Users" ("Name")`. A rule for `[tables.users]` applies to the table in any schema; write `[tables."audit.users"]` to give one schema its own rules. Two rules whose names differ only in case are rejected.

A rule that matches no table or column of the input is reported in a warning at the end of the run, because a misspelt name leaves its column unmasked. `--strict` turns the warning into an error (the output has been written by then).

Rules are checked before any data is read: a rule with a missing or invalid parameter stops GhostDump with a message that names the table and column. Then GhostDump prints, on stderr, what the rules guarantee for each table: the ε a row spends over its `dp_laplace` columns, and which columns are 0-LDP or only pseudonymized.

#### `dp_laplace`

Each value is clamped to `bounds = [lo, hi]` and receives Laplace noise of scale `(hi - lo) / epsilon`, so every published value is ε-differentially private on its own (local differential privacy). The bounds must be chosen from knowledge of the domain, not from the data: values outside them lose accuracy, and bounds wider than needed add noise. `scale` fixes the number of decimal places written, so the output never reveals the precision of the input. `clamp_output = true` also keeps the noisy values inside the bounds (no negative salaries), at the cost of a bias near the edges. Values that are not numbers are treated as the midpoint of the bounds, never written as they are. The privacy budget adds up: a row with two `dp_laplace` columns spends the sum of their epsilons, and a person with several rows spends it once per row.

The noise is drawn by an exact sampler on integers (`mechanism = "exact"`, the default): values are handled in units of `scale` and no floating-point operation takes part, so the output carries no low-order artifacts. `mechanism = "f64"` selects the floating-point sampler, about 15 times cheaper per value but vulnerable to attacks on the low-order bits. It is kept only to measure the cost of exact sampling: it runs only with the hidden option `--experimental-f64-sampler`, which warns at startup, and it rejects a `scale` fine enough to expose the low-order bits of its noise. Bounds times 10^scale must stay below 2^53. See [docs/decisions/0001-laplace-sampler.md](docs/decisions/0001-laplace-sampler.md) for the reasons, including why OpenDP is not used.

`consistent = true` draws the noise of each value from a keyed PRF (HMAC-SHA256 of the secret, the column and the clamped value) instead of the run's random generator. The same value in the same column then gets the same noisy output in every row and in every run. Repeated dumps no longer let anyone average the noise away (T releases cost ε, not T·ε), and a person whose value repeats in several rows spends ε once. The price: equal values produce equal outputs, which reveals which rows share a value, as `hmac` does. The guarantee becomes computational: it rests on the secret staying secret. Consistent noise needs the secret.

#### `randomized_response`

For categorical columns, `{ name = "status", strategy = "randomized_response", epsilon = 1.0, categories = ["ACTIVE", "PENDING", "INACTIVE"] }` keeps the true category with probability e^ε/(e^ε + k − 1) and otherwise writes another declared category, so each value is ε-differentially private on its own. Values outside the categories are replaced by a uniformly random category and counted in a warning. Unlike `random_choice`, which erases the distribution, the share of each category can be estimated without bias from the masked dump with (observed − q)/(p − q), where q = 1/(e^ε + k − 1); see [docs/experiments/utility.md](docs/experiments/utility.md). Sampling uses integer arithmetic only.

### 2. The Cryptographic Secret

The secret key used to generate deterministic HMAC hashes. For security reasons, **never commit this to the repository**. Give it in a file, or in an environment variable:

```bash
openssl rand -hex 32 > ghostdump.secret && chmod 600 ghostdump.secret
ghostdump -c rules.toml --secret-file ghostdump.secret -i dump.sql -o masked.sql

export GHOSTDUMP_SECRET="$(openssl rand -hex 32)"
```

`--secret-file` reads the whole file, without one line ending at its end, and warns when its group or other users can read it. `-s` also works, with a warning: other users of the machine can read the command line in the process list, and it stays in the shell history. Give the secret one way only: two of them stop the run, so a variable left in the environment never takes the place of the file.

The secret is required only when the rules use `hmac`. Use 32 or more random bytes: an empty secret is rejected, and one shorter than 16 bytes triggers a warning, because HMAC pseudonyms of low-entropy values (sequential IDs, CPFs) can be brute-forced by anyone who guesses the secret.

All status messages and warnings are written to stderr, so stdout carries only the sanitized dump. Use `-v` for debug messages, or set `GHOSTDUMP_LOG` (for example `GHOSTDUMP_LOG=warn`) to choose the level. Logs never include column values: a warning about values names their column and counts them (`column users.salary: 2 value(s) were not numbers`). With rules, a PostgreSQL dump that holds large objects gets a warning too, since no rule applies to them.

---

##  Installation & Usage

### Building from Source

Clone the repository and build it with Cargo. It needs Rust 1.88 or later, and a C compiler for the allocator (mimalloc):

```bash
git clone https://github.com/cadu0000/ghostdump
cd ghostdump
cargo build --release

```

The optimized binary will be available at `target/release/ghostdump`.

### Basic Execution

```bash
ghostdump -c rules.toml -i production_db.sql -o dev_db.sql --secret-file ghostdump.secret
```

The output file is written next to its target, under a temporary name, and renamed over it only when the run succeeds: a run that stops leaves the target as it was (or absent), and removes the temporary file. A file that is replaced keeps its permissions. The input and the output may be the same file. Devices and pipes (such as `-o /dev/null`) are written directly.

### Starting and checking the rules

`init` reads the tables and columns of a dump (from its `CREATE TABLE` statements and the column lists of its rows) and writes a starting rules file. Columns whose names suggest personal data (`name`, `email`, `cpf`, `phone`, ...) get a rule that needs no secret; every other column is listed in a comment. A table the dump has no rows of, and SQLite's own tables, are commented out. It never replaces an existing file:

```bash
ghostdump init -i production_db.sql -o rules.toml
```

`check` runs the rules over a dump without writing anything, and lists the rules applied to each table, the tables copied as they are, and the rules that match nothing. It needs no secret, and fails like a run would: 78 when a rule matches nothing, 65 when the dump would be refused.

```bash
ghostdump check -c rules.toml -i production_db.sql
```

A rules file written by `init` passes `check` on its dump: checked on 30 real dumps of the three dialects.

### Unix Pipeline (Zero extra disk usage)

Ideal for processing compressed dumps without extracting them to disk first:

```bash
export GHOSTDUMP_SECRET="super_secret_key"
zcat production_db.sql.gz | ghostdump -c rules.toml | gzip > dev_db_anon.sql.gz

```

---

## 🛠️ CLI Arguments

| Flag | Long Argument | Description |
| --- | --- | --- |
| `-c` | `--config <FILE>` | Path to the TOML configuration file. |
| `-i` | `--input <FILE>` | Path to the input SQL dump file. |
| `-o` | `--output <FILE>` | Path to the output SQL dump file, replaced only when the run succeeds (stdout when absent). |
| | `--secret-file <FILE>` | File holding the HMAC secret, required when the rules use `hmac`. |
| `-s` | `--secret <STR>` | The HMAC secret on the command line, where other users can see it (warns). Can also be given in `GHOSTDUMP_SECRET`. |
| `-p` | `--progress` | Displays a progress bar on stderr: share read and ETA for a plain input file, bytes read and rate for stdin or `.gz`. Hidden when stderr is not a terminal. |
| `-v` | `--verbose` | Enables detailed debug logging for troubleshooting. |
| `-d` | `--dry-run` | Processes input without writing output (Validation mode); the output file is not even opened. |
| `-l` | `--limit <ROWS>` | Stop after N data rows, closing the statement they belong to. Statements after that point (such as a final `COMMIT`) are not written. |
| | `--dialect <NAME>` | `auto` (default), `postgres`, `mysql` or `sqlite`. |
| | `--max-row-size <MiB>` | Largest data row, or statement held for checking, accepted (default 256). |
| | `--strict` | Fail when a rule matches no table or column of the input. |
| `-j` | `--threads <N>` | Threads that mask rows (default 1). Reading, parsing and writing stay on one thread; the output is the same for any number of threads. See [benchmarks](docs/benchmarks.md) for the speedup. |

The exit status tells what failed, with the values of `sysexits.h`:

| Status | Meaning |
| --- | --- |
| 0 | The run succeeded. |
| 64 | The command line is wrong: an unknown option, or a missing or empty secret, or one given twice. |
| 65 | The input cannot be masked safely, or is not a dump GhostDump reads: every refusal listed under *Supported SQL Dialects*, a truncated row, a row over `--max-row-size`. |
| 74 | A file could not be read or written: the input, the rules, the secret file, the output (a full disk, a closed pipe). |
| 78 | The rules are invalid, or match nothing with `--strict`. |

---

##  Testing

To run the entire test suite and validate the parser:

```bash
cargo test

```

To run tests with standard output visible (useful for debugging state transitions):

```bash
cargo test -- --nocapture

```

Continuous integration has two pipelines. The fast one (`.github/workflows/ci.yml`) runs on every push and pull request, and weekly: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo doc` without warnings, `cargo test`, the validation of the Laplace samplers, the tests again on Rust 1.88, the minimum supported version, and `cargo audit` on the three lockfiles (the tool, its fuzz targets and the AST baseline of the benchmarks). The full one (`.github/workflows/full.yml`) runs by hand and on every pull request into `main`: the property tests with 1,000,000 cases each, 15 minutes of fuzzing per target, and a short run of the microbenchmarks. The integration tests in `tests/` run the binary on fixture dumps and compare its output with expected files.

`tests/properties.rs` holds property-based tests (proptest): on inputs built from SQL fragments and arbitrary bytes, the parser reports every byte, a run without rules is an exact copy or a clean error, and masking keeps the rows and the unmasked columns. `cargo test` runs 256 cases per property; for a deeper search, run `PROPTEST_CASES=100000 cargo test --release --test properties`.

The statistical validation of the Laplace samplers is ignored by default, because it is slow in a debug build (26 s, against under a second in release); run it with `cargo test --release -- --ignored`.

`fuzz/` holds two [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets built from the property tests: `parser` (no byte is lost) and `engine` (without rules, an exact copy or a clean error; with rules, no panic, and the same output on one and three threads). They need the nightly compiler:

```bash
cargo install cargo-fuzz
sh fuzz/seed.sh   # the dump fixtures, once per dialect
cargo +nightly fuzz run parser -- -max_total_time=900 -dict=fuzz/sql.dict
cargo +nightly fuzz run engine -- -max_total_time=900 -dict=fuzz/sql.dict
```

`scripts/client_framing.py` checks 73 unusual scripts (comments, quoting, client commands, settings) against psql, mysql and sqlite3: a value the database stores must not leave GhostDump in clear ([docs/experiments/client-framing.md](docs/experiments/client-framing.md)).

For reproducible experiments, the hidden `--seed <N>` option fixes every random choice (generated values and Laplace noise). Anyone who knows the seed can recompute them, so never use it on real data. A seeded run writes the same output for any `--threads`.

---

##  Documentation

| Page | Contents |
| --- | --- |
| [docs/guarantees.md](docs/guarantees.md) | What each strategy protects, under which conditions, and the propositions behind it |
| [docs/limitations.md](docs/limitations.md) | What GhostDump does not do, or does only under conditions, with the evidence for each |
| [docs/experiments/utility.md](docs/experiments/utility.md) | Utility of masked dumps, on synthetic data and on UCI Adult, by ε and with dispersion: single columns, pairs of columns and a classification task; local arms, SQL and estimator analysts, central-DP comparator (`scripts/utility_eval.py`, `scripts/utility_multi.py`, `scripts/utility_checks.py`) |
| [docs/experiments/floating-point.md](docs/experiments/floating-point.md) | Floating-point distinguishing experiment on the Laplace samplers (`examples/fp_distinguisher.rs`) |
| [docs/experiments/client-framing.md](docs/experiments/client-framing.md) | Unusual comments, quoting, client commands and settings, checked against psql, mysql and sqlite3 (`scripts/client_framing.py`) |
| [docs/benchmarks.md](docs/benchmarks.md) | Microbenchmarks, end-to-end throughput and memory, parallel masking, and the controlled runs of stage 2.8: the noise of the machine, instructions per row, profiles, 10^7 rows, real dumps and a parser that builds an AST (`benches/`, `scripts/bench_grid.py`, `scripts/bench_report.py`) |
| [docs/decisions/0001-laplace-sampler.md](docs/decisions/0001-laplace-sampler.md) | Why the exact integer sampler, and why not OpenDP |
| [docs/decisions/0002-parallel-masking.md](docs/decisions/0002-parallel-masking.md) | How `--threads` keeps the output ordered, reproducible and in bounded memory, and why mimalloc |
| [docs/decisions/0003-numeric-conversions.md](docs/decisions/0003-numeric-conversions.md) | Every numeric conversion in the samplers, and why it is exact or checked |
| [docs/dataset-generation.md](docs/dataset-generation.md) | The seeded dataset generator and its rule sets |
| [docs/future-work.md](docs/future-work.md) | Shuffling and other directions not taken |

---

##  License

This project is licensed under either of the following licenses, at your option:

* **MIT License** [LICENSE-MIT](https://www.google.com/search?q=LICENSE-MIT) 
* **Apache License, Version 2.0** [LICENSE-APACHE](https://www.google.com/search?q=LICENSE-APACHE)
