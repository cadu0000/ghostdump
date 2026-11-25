#  Dataset Generation Guide

GhostDump includes a synthetic SQL dump generator for testing, benchmarking and utility evaluation. Every value comes from one seeded generator, so the same seed produces the same dump, byte for byte.

---

##  Location

```bash
scripts/sql_generator.py
```

It uses only the Python standard library.

---

##  Usage

```bash
python scripts/sql_generator.py --rows 1000000 --mode copy --seed 42
```

---

##  Parameters

| Flag | Description |
| --- | --- |
| `--rows` | Number of users; orders are twice as many |
| `--mode` | Output format: `insert` or `copy` |
| `--seed` | Seed of every random choice (default 42). Same seed, same dump |
| `--null-rate COLUMN=RATE` | Share of NULL values in a column (repeatable). Defaults: `email=0.02`, `salary=0.05`, `amount=0.01` |
| `--output-dir` | Where to write the files (default `generated/` at the project root) |

---

##  Output

```bash
generated/
├── dump.sql.gz          # CREATE TABLE statements and the data
├── rules.toml           # rules with a priori bounds
├── rules_oracle.toml    # the same rules with the real range of the data as bounds
├── ground_truth.json    # exact statistics of the generated data
└── stats.txt            # number of data rows
```

The gzip header carries no timestamp, so `sha256sum generated/dump.sql.gz` is the same for the same seed and parameters.

---

##  Data

| Table | Column | Distribution |
| --- | --- | --- |
| `users` | `id` | `ID-USR-00000001`, ... |
| `users` | `company_id` | uniform over `rows / 50` companies |
| `users` | `status` | `ACTIVE` 70%, `PENDING` 20%, `INACTIVE` 10% |
| `users` | `salary` | lognormal (median about 4,000) |
| `orders` | `user_id` | bounded Zipf law (exponent 1.1): a few users place most orders |
| `orders` | `amount` | lognormal (median about 55) |

`ground_truth.json` records, per column, the number of rows and NULLs and, for numeric columns, the sum, mean, variance, minimum and maximum; for `status`, the frequency of each value. Utility experiments compare the statistics of a masked dump with these.

---

##  Rules

Both rule files pseudonymize keys and foreign keys with `hmac`, so joins between `users` and `orders` still work after masking, and apply `dp_laplace` to `salary`:

* `rules.toml` uses a priori bounds `[0, 10000]`, chosen without looking at the data. Salaries above 10,000 are clamped, which biases the mean.
* `rules_oracle.toml` uses the real minimum and maximum of the data. These bounds come from the data, so they are **not** differentially private: they exist only as the best case to compare the a priori bounds against.

`hmac` needs a secret (`-s` or `GHOSTDUMP_SECRET`).

---

##  Example workflow

```bash
# Generate dataset
python scripts/sql_generator.py --rows 1000000 --mode copy --seed 42

# Run GhostDump
cargo run --release -- \
  -c generated/rules.toml \
  -i generated/dump.sql.gz \
  -o output.sql \
  -s "$(openssl rand -hex 32)"
```

---

##  Notes

* COPY mode is recommended for performance benchmarks
* The dump loads into PostgreSQL in both formats (`zcat generated/dump.sql.gz | psql`)
* Data is synthetic: the distributions are realistic in shape, not in content

⚠️ This generator is intended for testing, benchmarking and evaluation only.
