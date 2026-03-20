
#  Dataset Generation Guide

GhostDump includes a synthetic SQL dump generator for testing and benchmarking.

---

##  Location

```bash
scripts/generate_dataset.py
````

---

##  Usage

```bash
python scripts/generate_dataset.py --rows 1000000 --mode copy
```

---

##  Parameters

| Flag     | Description                       |
| -------- | --------------------------------- |
| `--rows` | Base number of rows to generate   |
| `--mode` | Output format: `insert` or `copy` |

---

##  Output

The script creates a `generated/` directory at the project root:

```bash
generated/
├── dump.sql.gz
└── rules.toml
```

---

##  How it works

* Schema-driven generation
* Deterministic structure for reproducibility
* Designed for streaming workloads

---

##  Example workflow

```bash
# Generate dataset
python scripts/generate_dataset.py --rows 1000000 --mode copy

# Run GhostDump
cargo run --release -- \
  -c generated/rules.toml \
  -i generated/dump.sql.gz \
  -o output.sql.gz \
  -s my-secret
```

---

##  Notes

* COPY mode is recommended for performance benchmarks
* Output is gzip-compressed by default
* Data is synthetic and not meant to be realistic

⚠️ This generator is intended for testing and benchmarking only.

