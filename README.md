
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

GhostDump's FSM parser is designed to handle the most common SQL dump formats:

* **PostgreSQL**: Native support for `COPY ... FROM stdin;` formats (tab-separated values) and traditional `INSERT INTO` statements.
* **MySQL / SQLite**: Support for multi-value `INSERT INTO ... VALUES (...), (...);` dumps.

GhostDump fails closed: when a table has rules that cannot be applied, it stops with an error instead of writing the rows unmasked. This happens when an `INSERT` has no column list, when a `COPY ... FROM stdin` uses options such as `WITH (FORMAT csv)`, or when a data block's table name cannot be read. Rows larger than `--max-row-size` (256 MiB by default) also stop the run.

---

##  Roadmap & Current Status

GhostDump is currently under active development. The core parsing engine is functional, while some integration features are still in progress.

* [x] **Core Engine**: FSM byte-by-byte SQL parsing and structural bypassing.
* [x] **Zero-Memory Proxy**: Streaming architecture implementation.
* [x] **CLI Foundation**: Robust argument parsing using `clap`.
* [x] **Masking Strategies**: Deterministic HMAC and Faker-based PII anonymization.
* [ ] **Differential Privacy**: Laplace mechanism implementation.
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
    { name = "salary", strategy = "dp_laplace", epsilon = 0.5, sensitivity = 15000.0 }
]

```

Only the declared columns are rewritten; every other byte of the dump, including the layout of `INSERT` statements and `COPY` blocks, is copied unchanged. Values keep their SQL type: numbers stay unquoted, generated text is escaped for the dialect, and `NULL` stays `NULL` by default. Add `null_policy = "generate"` to a column to replace `NULL` values too (with strategies that do not depend on the original value).

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

---

##  License

This project is licensed under either of the following licenses, at your option:

* **MIT License** [LICENSE-MIT](https://www.google.com/search?q=LICENSE-MIT) 
* **Apache License, Version 2.0** [LICENSE-APACHE](https://www.google.com/search?q=LICENSE-APACHE)
