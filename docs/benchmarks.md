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

See task #8: `scripts/bench.sh` runs the CLI over generated dumps of growing size and records throughput and peak memory in a CSV.
