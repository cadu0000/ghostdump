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

**Reopened by parallel masking.** With `--threads`, masking runs beside the parser instead of after it, so the parser bounds every run, with or without rules (see [Parallel masking](#parallel-masking)).

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

---

##  Parallel masking

`--threads N` masks rows on N threads while the calling thread reads, parses and writes; the design is in [decisions/0002-parallel-masking.md](decisions/0002-parallel-masking.md). The grid takes a list of thread counts:

```bash
python3 scripts/bench_grid.py --sizes 100000,1000000 --runs 5 --threads 1,2,3,4 --out results.csv
```

Runs without rules use one thread: with nothing to mask, the CLI ignores the option.

### Results (November 2026, reference machine above)

Median of 5 runs after one warm-up, 3,000,000 rows, in rows per second; the raw runs are in [bench-results/2026-11-threads.csv](bench-results/2026-11-threads.csv). In brackets, the share of the speed of a copy without rules, which is 1,734,000 rows/s on COPY and 990,000 on INSERT.

**COPY**

| Configuration | 1 thread | 2 threads | 3 threads | 4 threads |
| --- | --- | --- | --- | --- |
| `hmac_only` | 597,000 (34%) | 1,522,000 (88%) | 1,716,000 (99%) | 1,588,000 (92%) |
| `laplace_f64` | 1,453,000 (84%) | 1,739,000 (100%) | 1,715,000 (99%) | 1,831,000 (106%) |
| `laplace_exact` | 1,311,000 (76%) | 1,676,000 (97%) | 1,801,000 (104%) | 1,773,000 (102%) |
| `full` | 446,000 (26%) | 1,151,000 (66%) | 1,612,000 (93%) | 1,468,000 (85%) |

**INSERT**

| Configuration | 1 thread | 2 threads | 3 threads | 4 threads |
| --- | --- | --- | --- | --- |
| `hmac_only` | 398,000 (40%) | 999,000 (101%) | 983,000 (99%) | 995,000 (101%) |
| `laplace_f64` | 769,000 (78%) | 1,002,000 (101%) | 1,022,000 (103%) | 1,019,000 (103%) |
| `laplace_exact` | 751,000 (76%) | 1,018,000 (103%) | 1,008,000 (102%) | 1,012,000 (102%) |
| `full` | 316,000 (32%) | 885,000 (89%) | 989,000 (100%) | 939,000 (95%) |

**Peak memory**, over both formats and every rule set:

| Threads | 1 | 2 | 3 | 4 |
| --- | --- | --- | --- | --- |
| 300,000 and 3,000,000 rows | 6.6–6.9 MiB | 9.5–11.2 MiB | 10.7–11.9 MiB | 10.7–13.6 MiB |

Findings:

* **Two or three threads hide the cost of masking.** With three threads every rule set runs at 93–104% of the speed of a copy without rules; on INSERT two are enough. The full rule set goes from 446,000 to 1,612,000 rows/s on COPY (3.6 times) and from 316,000 to 989,000 on INSERT (3.1 times). Shares above 100% are noise on a shared machine.
* **The parser is now the limit.** The reading thread parses every byte, so no number of threads goes past the copy without rules. Four threads are no better than three here: four maskers and the reading thread are more threads than the machine's 4 vCPUs.
* **Memory stays bounded.** Peak memory grows with the number of threads (segments in flight and one allocator heap per thread) but not with the dump: 300,000 and 3,000,000 rows differ by less than 2 MiB, in both directions.
* **One thread costs what it did before.** With `--threads 1`, the default, rows are masked on the reading thread as before. Against the table of the previous section, `full` on COPY runs at 446,000 rows/s instead of 418,000 and peaks at 6.6 MiB instead of 5.7–5.8 MiB; both differences come from the allocator change described next.

### Allocator

With glibc's allocator, threads do not help. Same runs, `full` rule set, 3,000,000 rows, median of 5 runs, in seconds; the glibc build is the same source without the `#[global_allocator]` lines in `src/main.rs`. Raw runs in [bench-results/2026-11-allocator.csv](bench-results/2026-11-allocator.csv).

| Allocator | Format | 1 thread | 2 threads | 3 threads | 4 threads |
| --- | --- | --- | --- | --- | --- |
| glibc | COPY | 7.20 | 6.64 | 7.63 | 5.66 |
| mimalloc | COPY | 6.71 | 2.54 | 1.82 | 2.22 |
| glibc | INSERT | 9.97 | 12.51 | 10.39 | 10.09 |
| mimalloc | INSERT | 9.55 | 3.44 | 3.09 | 3.16 |

The cause is glibc's per-thread cache (tcache), which keeps freed chunks of any arena. When a masking thread frees a chunk that the reading thread allocated (thread start-up does this), the chunk goes to the masking thread's cache and is handed out again. A later `realloc` of it locks the reading thread's arena and allocates the new chunk there, so such chunks keep circulating, and every masking thread ends up waiting on that one lock. The maskers call `realloc` often: the `fake` crate builds names and e-mails with `format!` and `replace`. On COPY with 4 threads, a run made about 250,000 context switches and used almost twice the CPU time of one thread (13.8 s against 7.1 s, user plus system); with `GLIBC_TUNABLES=glibc.malloc.tcache_count=0` the same binary took 2.5–2.7 s instead of 6.3–6.6 s.

GhostDump therefore uses mimalloc, whose heaps are per thread, as the global allocator of the binary (the library and the Criterion benchmarks keep the system allocator). By default mimalloc commits whole arenas on Linux, which peaked at 15.5 MiB with one thread and 38–66 MiB with threads; `.cargo/config.toml` builds it to commit on demand (`MI_DEFAULT_ARENA_EAGER_COMMIT=0`), which gives the peaks above at the same speed. Setting the option at run time comes too late, because the first arena is reserved before `main`. The cost is a C dependency built by `cc` and 157 KiB of binary.
