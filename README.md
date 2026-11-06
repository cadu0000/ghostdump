
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

GhostDump's FSM parser is designed to handle the most common SQL dump formats. The dialect is detected from the dump header (`--dialect auto`, the default) or chosen with `--dialect postgres|mysql|sqlite`.

* **PostgreSQL**: `COPY ... FROM stdin;` blocks in text format and `INSERT INTO` statements (including `pg_dump --inserts`, `--column-inserts` and `--on-conflict-do-nothing`).
* **MySQL**: multi-row `INSERT`, `INSERT IGNORE` and `REPLACE` statements. Rules need column names in the statements: dump with `mysqldump --complete-insert`.
* **SQLite**: `INSERT` and `INSERT OR ...` statements with a column list.

GhostDump fails closed: when a table has rules that cannot be applied, it stops with an error instead of writing the rows unmasked. This happens when an `INSERT` has no column list, when a `COPY ... FROM stdin` uses options such as `WITH (FORMAT csv)`, or when a data block's table name cannot be read. Rows larger than `--max-row-size` (256 MiB by default) also stop the run.

Not supported yet: MySQL `DELIMITER` blocks (stored routines), SQLite triggers with several statements, and inserts inside CTEs (`WITH ... INSERT`).

---

##  Roadmap & Current Status

GhostDump is currently under active development. The core parsing engine is functional, while some integration features are still in progress.

* [x] **Core Engine**: FSM byte-by-byte SQL parsing and structural bypassing.
* [x] **Zero-Memory Proxy**: Streaming architecture implementation.
* [x] **CLI Foundation**: Robust argument parsing using `clap`.
* [x] **Masking Strategies**: Deterministic HMAC and Faker-based PII anonymization.
* [x] **Differential Privacy**: Laplace mechanism with bounds, clamping and fixed precision.
* [x] **Rule Engine Integration**: Connect the `rules.toml` to the FSM.
* [ ] **UX & Metrics**: Real-time progress metrics (`indicatif`).
* [x] **Structured Logging**: Verbose debug mode (`--verbose`) and `GHOSTDUMP_LOG`.
* [x] **Edge Cases**: Advanced handling of `NULL` values (`null_policy`).
* [ ] **Parallel Processing**: Multi-threading for the masking step.

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

Only the declared columns are rewritten; every other byte of the dump, including the layout of `INSERT` statements and `COPY` blocks, is copied unchanged. Values keep their SQL type: numbers stay unquoted, generated text is escaped for the dialect, and `NULL` stays `NULL` by default. Add `null_policy = "generate"` to a column to mask `NULL` values too.

Rules are checked before any data is read: a rule with a missing or invalid parameter stops GhostDump with a message that names the table and column.

#### `dp_laplace`

Each value is clamped to `bounds = [lo, hi]` and receives Laplace noise of scale `(hi - lo) / epsilon`, so every published value is ε-differentially private on its own (local differential privacy). The bounds must be chosen from knowledge of the domain, not from the data: values outside them lose accuracy, and bounds wider than needed add noise. `scale` fixes the number of decimal places written, so the output never reveals the precision of the input. `clamp_output = true` also keeps the noisy values inside the bounds (no negative salaries), at the cost of a bias near the edges. Values that are not numbers are treated as the midpoint of the bounds, never written as they are. The privacy budget adds up: a row with two `dp_laplace` columns spends the sum of their epsilons, and a person with several rows spends it once per row.

The noise is drawn by an exact sampler on integers (`mechanism = "exact"`, the default): values are handled in units of `scale` and no floating-point operation takes part, so the output carries no low-order artifacts. `mechanism = "f64"` selects the floating-point sampler, about 15 times cheaper per value but vulnerable to attacks on the low-order bits; it is kept to measure the cost of exact sampling. Bounds times 10^scale must stay below 2^53. See [docs/decisions/0001-laplace-sampler.md](docs/decisions/0001-laplace-sampler.md) for the reasons, including why OpenDP is not used.

### 2. The Cryptographic Secret

The secret key used to generate deterministic HMAC hashes. For security reasons, **never commit this to the repository**. It can be provided via CLI argument or environment variable:

```bash
export GHOSTDUMP_SECRET="$(openssl rand -hex 32)"

```

The secret is required only when the rules use `hmac`. Use 32 or more random bytes: an empty secret is rejected, and one shorter than 16 bytes triggers a warning, because HMAC pseudonyms of low-entropy values (sequential IDs, CPFs) can be brute-forced by anyone who guesses the secret.

All status messages and warnings are written to stderr, so stdout carries only the sanitized dump. Use `-v` for debug messages, or set `GHOSTDUMP_LOG` (for example `GHOSTDUMP_LOG=warn`) to choose the level. Logs never include column values.

---

##  Installation & Usage

### Building from Source

Clone the repository and build using Cargo:

```bash
git clone [https://github.com/cadu0000/ghostdump](https://github.com/cadu0000/ghostdump)
cd ghostdump
cargo build --release

```

The optimized binary will be available at `target/release/ghostdump`.

### Basic Execution

```bash
ghostdump -c rules.toml -i production_db.sql -o dev_db.sql -s "super_secret_key"

```

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
| `-o` | `--output <FILE>` | Path to the output SQL dump file. |
| `-s` | `--secret <STR>` | Cryptographic secret for HMAC, required when the rules use `hmac` (Can also use `GHOSTDUMP_SECRET` env var). |
| `-p` | `--progress` | Displays real-time progress metrics (MB/s, ETA). |
| `-v` | `--verbose` | Enables detailed debug logging for troubleshooting. |
| `-d` | `--dry-run` | Processes input without writing output (Validation mode). |
| `-l` | `--limit <ROWS>` | Stop after N data rows, closing the statement they belong to. Statements after that point (such as a final `COMMIT`) are not written. |
| | `--dialect <NAME>` | `auto` (default), `postgres`, `mysql` or `sqlite`. |
| | `--max-row-size <MiB>` | Largest data row accepted (default 256). |

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

Continuous integration (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` on every push and pull request. The integration tests in `tests/` run the binary on fixture dumps and compare its output with expected files.

The statistical validation of the Laplace samplers is slow and ignored by default; run it with `cargo test --release -- --ignored`.

For reproducible experiments, the hidden `--seed <N>` option fixes every random choice (generated values and Laplace noise). Anyone who knows the seed can recompute them, so never use it on real data.

---

##  License

This project is licensed under either of the following licenses, at your option:

* **MIT License** [LICENSE-MIT](https://www.google.com/search?q=LICENSE-MIT) 
* **Apache License, Version 2.0** [LICENSE-APACHE](https://www.google.com/search?q=LICENSE-APACHE)
