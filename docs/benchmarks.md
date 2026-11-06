#  Benchmarks

Two kinds of measurement:

* **Microbenchmarks** (`benches/microbench.rs`, Criterion): throughput of the parser and of the whole engine on in-memory dumps, and the cost per value of each masker.
* **End-to-end runs** of the CLI over generated dumps (throughput and peak memory).

---

##  Microbenchmarks

```bash
cargo bench --bench microbench
# quicker, less precise:
cargo bench --bench microbench -- --warm-up-time 1 --measurement-time 3
```

The dumps are built in memory: 20,000 rows shaped like the generator's `users` table, as one COPY block (2.0 MB) or as INSERT statements of 1,000 rows (2.3 MB). "Rules" masks four of the seven columns: `hmac`, `faker_name`, `faker_email` and `dp_laplace` with the sampler named.

### Reference machine

Cloud container, 4 vCPUs (Intel Xeon @ 2.10 GHz), 15 GB of memory, Linux 6.18, rustc 1.97, release profile. The machine is shared, so differences under about 5% are noise. Numbers for a paper should come from a dedicated machine, described the same way.

### Results (November 2026)

Median of the Criterion run, with the 95% interval in brackets where it matters.

**Throughput**

| Benchmark | COPY | INSERT |
| --- | --- | --- |
| Parser alone (every event) | 126 MiB/s | 89 MiB/s |
| Engine, no rules | 122 MiB/s | 82 MiB/s |
| Engine, rules, `f64` sampler | 42 MiB/s | 34 MiB/s |
| Engine, rules, exact sampler | 38 MiB/s | 34 MiB/s |

**Cost per value**

| Masker | Time per value |
| --- | --- |
| `dp_laplace`, `f64` sampler | 171 ns [168, 174] |
| `dp_laplace`, exact sampler | 397 ns [387, 410] |
| `hmac` | 447 ns |
| `faker_name` | 193 ns |
| `cpf` | 191 ns |
| `codec::copy::split` (one row) | 82 ns |
| `codec::values::split` (one row) | 435 ns |

The masker times include parsing the input value and formatting the output, not only the noise. The exact sampler costs about 230 ns more per value than the `f64` one. End to end, with one `dp_laplace` column among four masked columns, that is about 10% on COPY and within noise on INSERT.

### Decision on the chunked parser (task CHK)

The criterion set in the plan: build the chunked parser if, with the full rule set and the `f64` sampler, the parser takes more than 40% of the time.

| | Parser alone | Engine with rules (`f64`) | Parser share |
| --- | --- | --- | --- |
| COPY | 14.0 ms | 42.0 ms | 33% |
| INSERT | 24.4 ms | 64.4 ms | 38% |

Both are below 40%, so the chunked parser is **not** built for now: with rules, masking dominates. Without rules the parser is almost the whole cost (122 of 126 MiB/s), so the chunked parser would mainly speed up pass-through copies. The question can be reopened if the evaluation needs faster runs without rules.

---

##  End-to-end runs

`scripts/bench_grid.py` runs the release binary over generated dumps (`sql_generator.py`, seed 42) and writes one CSV row per run. Each dump is decompressed once, so gzip is not measured, and the output goes to `/dev/null`. Peak memory is `VmHWM`, polled from `/proc` while the process runs.

```bash
cargo build --release
python3 scripts/bench_grid.py --sizes 100000,1000000 --runs 5 \
    --baseline-binary path/to/old/ghostdump --out results.csv
```

Configurations:

| Name | Rules |
| --- | --- |
| `no_rules` | none: a pass-through copy |
| `hmac_only` | `hmac` on the four keys (`users.id`, `users.company_id`, `orders.id`, `orders.user_id`) |
| `laplace_f64`, `laplace_exact` | `dp_laplace` on `users.salary`, with the sampler named |
| `full` | the generated `rules.toml`: every rule of the dataset guide |

`--baseline-binary` runs an older build with `no_rules` and with `full` written in its format (`sensitivity` instead of `bounds`).

### Results (November 2026, reference machine above)

Median of 5 runs after one warm-up; the raw runs are in [bench-results/2026-11-grid.csv](bench-results/2026-11-grid.csv). "Rows" counts data rows (users plus orders); "baseline" is the build of the first paper (`3849e9e`).

**3,000,000 rows (1,000,000 users)**

| Configuration | COPY rows/s | INSERT rows/s | Peak memory |
| --- | --- | --- | --- |
| `no_rules` | 1,687,000 | 868,000 | 5.7–5.8 MiB |
| `hmac_only` | 572,000 | 360,000 | 5.7–5.9 MiB |
| `laplace_f64` | 1,281,000 | 672,000 | 5.9–6.1 MiB |
| `laplace_exact` | 1,218,000 | 642,000 | 5.7–5.8 MiB |
| `full` | 418,000 | 290,000 | 5.7–5.8 MiB |
| baseline, `no_rules` | 1,029,000 | 470,000 | 5.4 MiB |
| baseline, `full` | 408,000 | 288,000 | 5.7 MiB |

Findings:

* **Memory stays flat.** Peak memory is the same, within 0.3 MiB, at 300,000 and at 3,000,000 rows, for every configuration: it does not grow with the dump.
* **Pass-through copies are 1.6–1.8 times faster** than the first paper's build, which re-quoted every value; that build also changed the output, so its numbers are an upper bound for what it did.
* **With every rule, throughput is the same as the first paper's build** (418,000 against 408,000 rows/s on COPY): the extra work (exact copy of unmasked bytes, typed values, fail-closed checks, compiled rules) costs nothing measurable.
* **Per column, HMAC is the most expensive masker.** On COPY, the four HMAC columns add 3.5 s to the pass-through time (about 0.9 s each), while one `dp_laplace` column adds 0.56 s (`f64`) or 0.69 s (exact).
* **The exact sampler costs about 5% end to end** (5.2% on COPY, 4.7% on INSERT) against the `f64` one, with one `dp_laplace` column; the cost of exact sampling is small next to the rest of the pipeline.
* The first paper reported about 307,000 rows/s and 4.7 MB with its rules on another machine; the numbers here are not directly comparable to it, only to each other.

Larger sizes (10^7 and 10^8 users) run with the same command; they were not measured on this machine because generating and running them takes hours.
