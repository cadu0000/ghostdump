#  Benchmarks

Two kinds of measurement:

* **Microbenchmarks** (`benches/microbench.rs`, Criterion): throughput of the parser and of the whole engine on in-memory dumps, and the cost per value of each masker.
* **End-to-end runs** of the CLI over generated dumps (throughput and peak memory).
* **Controlled runs** of stage 2.8: the noise of the machine, instructions per row, profiles per thread, dumps up to 10^7 rows, real dumps, and a parser that builds an AST.

---

##  Microbenchmarks

```bash
cargo bench --bench microbench
# quicker, less precise:
cargo bench --bench microbench -- --warm-up-time 1 --measurement-time 3
```

The dumps are built in memory: 20,000 rows shaped like the generator's `users` table, as one COPY block (2.0 MB) or as INSERT statements of 1,000 rows (2.3 MB). "Rules" masks four of the seven columns: `hmac`, `faker_name`, `faker_email` and `dp_laplace` with the sampler named.

### Reference machine

Cloud container, 4 vCPUs (Intel Xeon @ 2.10 GHz), 15 GB of memory, Linux 6.18, rustc 1.97, release profile. The machine is shared; its noise is measured in [controlled runs](#controlled-runs-stage-28): two copies of one binary differ by up to 4% in median time, and a single run varies by 4 to 8%. Criterion's intervals do not include that drift: `engine/insert/no_rules`, which hashes no value, came out 11% faster in a run minutes after the one before it, across the HMAC change of stage 2.8.

### Results (November 2026, commit `41fd2a1`)

Median of the Criterion run (`--warm-up-time 2 --measurement-time 8`), with the 95% interval in brackets where it matters.

**Throughput**

| Benchmark | COPY | INSERT |
| --- | --- | --- |
| Parser alone (every event) | 144 MiB/s | 108 MiB/s |
| Engine, no rules | 150 MiB/s | 109 MiB/s |
| Engine, rules, `f64` sampler | 51 MiB/s | 44 MiB/s |
| Engine, rules, exact sampler | 46 MiB/s | 40 MiB/s |

**Cost per value**

| Masker | Time per value |
| --- | --- |
| `dp_laplace`, `f64` sampler | 181 ns [178, 183] |
| `dp_laplace`, exact sampler | 372 ns [369, 374] |
| `hmac` | 179 ns |
| `faker_name` | 183 ns |
| `cpf` | 181 ns |
| `codec::copy::split` (one row) | 69 ns |
| `codec::values::split` (one row) | 399 ns |

The masker times include parsing the input value and formatting the output, not only the noise. The exact sampler costs about 190 ns more per value than the `f64` one; in the engine, with one `dp_laplace` column among four masked columns, that is 12% on COPY and 8% on INSERT. Before stage 2.8 `hmac` cost 447 ns per value; [the cost of HMAC](#the-cost-of-hmac) says what changed.

### Decision on the chunked parser (task CHK)

The criterion set in the plan: build the chunked parser if, with the full rule set and the `f64` sampler, the parser takes more than 40% of the time.

| | Parser alone | Engine with rules (`f64`) | Parser share |
| --- | --- | --- | --- |
| COPY | 14.0 ms | 42.0 ms | 33% |
| INSERT | 24.4 ms | 64.4 ms | 38% |

Both are below 40%, so the chunked parser is **not** built for now: with rules, masking dominates. Without rules the parser is almost the whole cost (122 of 126 MiB/s), so the chunked parser would mainly speed up pass-through copies. The question can be reopened if the evaluation needs faster runs without rules.

**Reopened by parallel masking.** With `--threads`, masking runs beside the parser instead of after it, so the parser bounds every run, with or without rules (see [Parallel masking](#parallel-masking)).

**Measured again in stage 2.8**, after `hmac` became cheaper: 12.3 ms of 34.4 ms on COPY (36%) and 20.2 ms of 49.9 ms on INSERT (41%), at the threshold and within the drift of the microbenchmarks. The decision is now taken with end-to-end runs and profiles: [decision on the chunked parser](#decision-on-the-chunked-parser).

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
| `laplace_f64`, `laplace_exact` | `dp_laplace` on `users.salary`, with the sampler named (the `f64` runs pass `--experimental-f64-sampler`) |
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

Larger sizes run with the same command; [size of the dump](#size-of-the-dump) measures up to 10^7 rows.

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

---

##  Controlled runs (stage 2.8)

Stage 2.8 of the [roadmap](roadmap.md) asked for a dedicated machine or a declared noise, 10^7 rows, CPU profiles, and a decision on the chunked parser taken with that data. There is no dedicated machine, so the noise of the shared one is measured and stated, builds are compared in alternation, run by run, and the comparisons that must not depend on the machine count instructions. Unless a table says otherwise, the runs are of commit `41fd2a1`, on the reference machine:

* Cloud container, 4 vCPUs, Intel Xeon @ 2.10 GHz (family 6, model 207, with the SHA and AVX-512 extensions), 15 GB of memory and no swap, Linux 6.18, rustc 1.97, release profile.
* The virtual machine exposes no hardware performance counters (`/sys/bus/event_source/devices` has no `cpu`), so instructions are counted with callgrind (valgrind 3.22) and profiles are sampled with perf 6.8 on the software event `cpu-clock`.
* Dumps from `sql_generator.py` with seed 42; "rows" counts data rows, users plus orders (three per user). Output to `/dev/null`.

`bench_grid.py` records, per run, the wall time, the peak resident memory (`VmHWM`), the CPU time, page faults and context switches (from `wait4`), and the CPU time of the reading thread. `bench_report.py` turns its CSVs into the tables below.

### Noise of the machine

Two copies of one binary, run in alternation 20 times each after one warm-up (an A/A run):

```bash
cp target/release/ghostdump /tmp/a && cp target/release/ghostdump /tmp/b
python3 scripts/bench_grid.py --sizes 1000000 --configs no_rules,full --threads 1,3 --runs 20 \
    --binary /tmp/a --compare-binary /tmp/b --out aa.csv
python3 scripts/bench_report.py pairs aa.csv
```

3,000,000 rows. The difference is that of the medians of the two copies, with a percentile bootstrap 95% interval (10,000 resamples); CV is the coefficient of variation of each copy's runs. Raw runs in [bench-results/2026-11-aa.csv](bench-results/2026-11-aa.csv).

| Format | Rules | Threads | Runs | copy A (s) | copy B (s) | Difference, wall [95%] | CV wall | Difference, CPU [95%] | CV CPU |
|---|---|---|---|---|---|---|---|---|---|
| COPY | `full` | 1 | 20 | 4.94 | 4.97 | -0.5% [-3.1%, +2.9%] | 5.2%, 4.5% | -0.8% [-3.4%, +2.8%] | 5.2%, 4.4% |
| COPY | `full` | 3 | 20 | 1.67 | 1.72 | -2.9% [-9.2%, +2.9%] | 8.2%, 6.8% | +0.4% [-4.6%, +2.1%] | 4.3%, 3.1% |
| COPY | `no_rules` | 1 | 20 | 1.58 | 1.64 | -3.7% [-7.2%, +3.3%] | 7.5%, 6.3% | -3.7% [-7.2%, +3.0%] | 7.5%, 6.0% |
| INSERT | `full` | 1 | 20 | 6.90 | 6.92 | -0.4% [-2.8%, +2.0%] | 3.8%, 4.6% | -0.6% [-2.7%, +2.0%] | 3.7%, 4.5% |
| INSERT | `full` | 3 | 20 | 2.84 | 2.84 | +0.0% [-5.6%, +3.8%] | 4.9%, 7.7% | -0.1% [-4.0%, +2.8%] | 4.1%, 4.8% |
| INSERT | `no_rules` | 1 | 20 | 2.66 | 2.65 | +0.6% [-2.2%, +4.0%] | 5.2%, 5.0% | +0.5% [-2.3%, +4.1%] | 5.2%, 4.7% |

* **Two copies of one binary differ by −3.7% to +0.6%, and every interval holds zero.** The intervals are 5 to 12 points wide: with 20 runs per build, a difference of a few percent is within the noise of this machine.
* **A single run varies by 4 to 8%** (CV of the wall time), so one run against one run says nothing about differences of that size.
* With one thread, CPU time varies as much as wall time; with three, less (CV 3–5% against 5–8%).

### Instructions per row

Callgrind counts the instructions a run executes; unlike time, the count does not depend on the load of the machine. 300,000 rows (100,000 users), every instruction of the process, divided by the rows, one run each:

```bash
python3 scripts/sql_generator.py --rows 100000 --mode copy --seed 42 --output-dir copy-100000
gunzip -k copy-100000/dump.sql.gz
GHOSTDUMP_SECRET=bench-secret-0123456789abcdef0123456789 valgrind --tool=callgrind \
    target/release/ghostdump -i copy-100000/dump.sql -o /dev/null -c copy-100000/rules.toml
```

The first paper's build (`3849e9e`) runs with its rules format (`sensitivity` instead of `bounds` and `scale`).

| Build | COPY, no rules | COPY, every rule | INSERT, no rules | INSERT, every rule |
| --- | --- | --- | --- | --- |
| first paper (`3849e9e`) | 13,705 | 50,517 | 26,010 | 63,763 |
| before the HMAC change (`a22cc90`) | 7,352 | 47,870 | 11,630 | 56,142 |
| this stage (`41fd2a1`) | 7,352 | 30,519 | 11,630 | 38,792 |

* **A copy without rules takes 46% fewer instructions per row than the first paper's build on COPY and 55% fewer on INSERT.** That build also rewrote every value, so it did more than copy.
* **With every rule, 40% and 39% fewer**, nearly all from the HMAC change below; before it, 5% and 12% fewer.
* The first paper's build read COPY only in part, as that paper says (on these dumps it warned that `\N` was not a number for `dp_laplace`), so its COPY counts are for what it did.
* **Valgrind's processor has no SHA extensions**, so the `sha2` crate runs its software path under callgrind: SHA-256 is 58% of the instructions of a COPY run with every rule before the change, while perf on the real processor, which runs the SHA instructions, puts it at 17–18% of the time. The counts compare builds; they are not times.

### The cost of HMAC

The first profiles, before this stage's change, put `hmac` first with one thread: a third of the samples of a COPY run with every rule (3,000,000 rows), of which 11 points went to turn the digest into hex one character at a time (`hex::encode` collects an iterator of `char`), and, for a value under 56 bytes, two of the four SHA-256 compressions to derive the key from the secret, again for every value. The key is now derived once per run and cloned for each value, and the hex digits are written from a table. The output is the same byte for byte (COPY and INSERT, every rule and consistent noise, with 1 and 3 threads, seeded), a test checks RFC 4231's vector, and the masker costs 168 ns per value instead of 437 (Criterion).

Before (`a22cc90`) against after (`41fd2a1`), in alternation, 10 runs each, 3,000,000 rows; a negative difference is a gain. Raw runs in [bench-results/2026-11-hmac.csv](bench-results/2026-11-hmac.csv).

| Format | Rules | Threads | Runs | after (s) | before (s) | Difference, wall [95%] | CV wall | Difference, CPU [95%] | CV CPU |
|---|---|---|---|---|---|---|---|---|---|
| COPY | `full` | 1 | 10 | 4.86 | 6.47 | -24.8% [-27.8%, -22.3%] | 3.6%, 2.7% | -24.8% [-27.8%, -22.2%] | 3.5%, 2.8% |
| COPY | `full` | 3 | 10 | 1.68 | 1.85 | -9.4% [-15.1%, +0.1%] | 8.0%, 5.1% | -20.8% [-25.7%, -16.4%] | 4.6%, 3.4% |
| COPY | `hmac_only` | 1 | 10 | 3.40 | 4.73 | -28.2% [-31.1%, -25.9%] | 3.7%, 4.7% | -28.3% [-31.2%, -25.4%] | 3.7%, 4.6% |
| COPY | `hmac_only` | 3 | 10 | 1.60 | 1.70 | -6.2% [-16.9%, +3.1%] | 11.4%, 8.9% | -31.0% [-35.6%, -27.0%] | 6.8%, 5.1% |
| INSERT | `full` | 1 | 10 | 7.07 | 8.19 | -13.7% [-17.0%, -10.9%] | 4.3%, 3.4% | -13.7% [-17.0%, -11.0%] | 4.4%, 3.2% |
| INSERT | `full` | 3 | 10 | 2.64 | 2.77 | -4.5% [-9.0%, +0.7%] | 3.1%, 6.1% | -19.3% [-23.2%, -16.8%] | 2.5%, 4.0% |
| INSERT | `hmac_only` | 1 | 10 | 5.19 | 6.69 | -22.4% [-25.8%, -20.5%] | 3.1%, 3.2% | -22.5% [-25.6%, -20.9%] | 2.9%, 3.0% |
| INSERT | `hmac_only` | 3 | 10 | 2.86 | 2.98 | -4.1% [-11.9%, +3.3%] | 6.2%, 8.6% | -25.5% [-28.2%, -22.0%] | 3.2%, 4.0% |

* **With one thread, a run with every rule takes 25% less time on COPY and 14% less on INSERT**, and one with `hmac` alone 28% and 22% less.
* **With three threads, CPU time falls by 19–31%, and wall time by 4–9%, within the noise** (every interval reaches zero). The masking threads do less work, but the reading thread bounds the run (next sections), so the gain is CPU left free.

### Where the time goes

Native profiles, sampled at 1,000 Hz with perf's `cpu-clock` event, 3,000,000 rows with every rule; the generated dumps and the [real ones](#real-dumps):

```bash
perf record -e cpu-clock -F 1000 -o run.data \
    target/release/ghostdump -i dump.sql -o /dev/null -c rules.toml --threads 3
perf script -i run.data -F pid,tid,ip,sym | c++filt > run.txt
python3 scripts/bench_report.py profile run.txt
```

Each sample counts for the part of the program its function belongs to, by the function's path without its generic arguments (`CATEGORIES` in `bench_report.py`): "parser" is `ghostdump::parser`, the state machine that reads the statements (and, on INSERT, the values of a row); "row codec" splits rows into values and writes them back; "text and numbers" is `core::fmt`, number parsing and string replacement; "engine, I/O" is the rest of GhostDump and `std::io`. Columns under 2% everywhere (fake values, kernel, other) are left out, so a row may add up to less than 100%.

With one thread, the share of the samples:

| Dump | Samples | HMAC | parser | row codec | Laplace | random numbers | text and numbers | allocation | memcpy, memchr | engine, I/O |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COPY | 5,241 | 16% | 25% | 16% | 3% | 4% | 9% | 15% | 6% | 5% |
| pg_dump | 5,158 | 17% | 25% | 15% | 3% | 4% | 9% | 15% | 6% | 5% |
| INSERT | 7,048 | 12% | 40% | 15% | 2% | 3% | 7% | 11% | 4% | 5% |
| mysqldump | 7,164 | 12% | 39% | 16% | 2% | 3% | 7% | 12% | 3% | 5% |

With three threads, the reading thread and the three masking threads together:

| Dump | Threads | Samples | HMAC | parser | row codec | Laplace | random numbers | text and numbers | allocation | memcpy, memchr | engine, I/O |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COPY | reading | 1,663 | 0% | 70% | 0% | 0% | 0% | 0% | 17% | 1% | 8% |
| | masking | 4,063 | 22% | 0% | 25% | 4% | 5% | 13% | 16% | 9% | 3% |
| pg_dump | reading | 1,626 | 0% | 68% | 0% | 0% | 0% | 0% | 19% | 1% | 8% |
| | masking | 3,691 | 23% | 0% | 24% | 5% | 5% | 13% | 14% | 9% | 4% |
| INSERT | reading | 2,851 | 0% | 78% | 0% | 0% | 0% | 0% | 12% | 1% | 7% |
| | masking | 5,162 | 18% | 13% | 23% | 3% | 4% | 11% | 14% | 6% | 3% |
| mysqldump | reading | 2,810 | 0% | 79% | 0% | 0% | 0% | 0% | 12% | 1% | 5% |
| | masking | 5,044 | 18% | 12% | 26% | 3% | 5% | 11% | 14% | 5% | 4% |

* **With one thread, the parser takes a quarter of the time on COPY and two fifths on INSERT**; no other part takes more than 17%. Real dumps profile like the generated ones of the same format.
* **With three threads, the reading thread spends 68–79% of its time in the parser** and 12–19% in the allocator. It reads, parses and writes; the masking threads split rows and mask them.
* On INSERT the masking threads also run the parser's value states (12–13%) to split rows into values.

### Real dumps

The generated data of 1,000,000 users (3,000,000 rows) loaded into PostgreSQL 16.15 and MySQL 8.0.46 and dumped back with each tool's defaults: `pg_dump` writes two COPY blocks (232 MB), `mysqldump` 254 INSERT statements on lines of up to 1,038,529 bytes (262 MB). `ghostdump check` applies every rule to both. 10 runs each after one warm-up:

```bash
python3 scripts/bench_grid.py --dump pg_dump.sql --rules rules.toml --data-rows 3000000 \
    --label pg_dump --configs no_rules,full --threads 1,3 --runs 10 --out real.csv
python3 scripts/bench_report.py scale real.csv
```

Raw runs in [bench-results/2026-11-real.csv](bench-results/2026-11-real.csv).

| Format | Rules | Threads | Rows | Runs | Median (s) | IQR (s) | Rows/s | Peak memory (MiB) | Minor faults | Major faults | Context switches | Reading thread on CPU |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MYSQLDUMP | `full` | 1 | 3,000,000 | 10 | 6.78 | 0.20 | 442,589 | 4.7–4.7 | 277 | 0 | 14 | 99% |
| MYSQLDUMP | `full` | 3 | 3,000,000 | 10 | 2.49 | 0.23 | 1,205,764 | 7.9–8.3 | 1,441 | 0 | 5,078 | 98% |
| MYSQLDUMP | `no_rules` | 1 | 3,000,000 | 10 | 2.44 | 0.17 | 1,229,886 | 4.5–4.7 | 264 | 0 | 6 | 99% |
| PG_DUMP | `full` | 1 | 3,000,000 | 10 | 5.13 | 0.28 | 585,212 | 4.7–4.7 | 277 | 0 | 12 | 99% |
| PG_DUMP | `full` | 3 | 3,000,000 | 10 | 1.75 | 0.19 | 1,715,609 | 8.1–8.8 | 1,342 | 0 | 3,686 | 98% |
| PG_DUMP | `no_rules` | 1 | 3,000,000 | 10 | 1.61 | 0.09 | 1,866,426 | 4.5–4.7 | 266 | 0 | 10 | 99% |

* **Real dumps run at the speed of the generated ones of their format**: with every rule and one thread, pg_dump's in 5.13 s against 4.94 s for the generated COPY, mysqldump's in 6.78 s against 6.90 s for the generated INSERT. Lines of a megabyte cost nothing measurable.
* **With three threads, every rule costs 2–9% over a copy without rules** (2.49 s against 2.44 s for mysqldump, 1.75 s against 1.61 s for pg_dump), and the reading thread is on the CPU 98% of the time.

### Against a parser that builds an AST

The first paper contrasted streaming with parsers that build the syntax tree (AST) of the whole dump, whose memory grows with the input. To measure it, `scripts/ast_roundtrip` parses a dump with sqlparser 0.64.0, a general SQL parser for Rust, and prints it back, without masking anything: the least an AST-based masker does. With `whole` it keeps the AST of every statement until the input ends, as a parser of the whole file does; with `statement` it prints each statement once parsed. One run each, peak memory from GNU time (`%M`):

```bash
cargo build --release --manifest-path scripts/ast_roundtrip/Cargo.toml
/usr/bin/time -f "%e s %M KiB" scripts/ast_roundtrip/target/release/ast-roundtrip postgres whole \
    < dump.sql > /dev/null
```

Raw results in [bench-results/2026-11-ast.csv](bench-results/2026-11-ast.csv); GhostDump's times are the medians of the runs above, on the same dumps.

| Dump | Rows | Size | AST, `whole` | AST, `statement` | GhostDump, no rules | GhostDump, every rule |
| --- | --- | --- | --- | --- | --- | --- |
| INSERT | 300,000 | 26 MiB | 1.79 s, 638 MiB | 2.01 s, 59 MiB | 0.26 s, 4.6 MiB | 0.69 s, 4.7 MiB |
| INSERT | 3,000,000 | 264 MiB | 19.50 s, 6,127 MiB | 16.97 s, 59 MiB | 2.66 s, 4.7 MiB | 6.90 s, 4.7 MiB |
| mysqldump | 3,000,000 | 250 MiB | 18.59 s, 6,118 MiB | 16.21 s, 56 MiB | 2.44 s, 4.7 MiB | 6.78 s, 4.7 MiB |
| COPY | 300,000 | 22 MiB | 0.15 s, 74 MiB | 0.15 s, 65 MiB | 0.16 s, 4.7 MiB | 0.44 s, 4.7 MiB |
| COPY | 3,000,000 | 221 MiB | 1.75 s, 559 MiB | 1.52 s, 450 MiB | 1.58 s, 4.7 MiB | 4.94 s, 4.7 MiB |
| pg_dump | 3,000,000 | 221 MiB | 1.60 s, 558 MiB | 1.66 s, 451 MiB | 1.61 s, 4.7 MiB | 5.13 s, 4.7 MiB |

* **The AST of a whole INSERT dump takes 23 to 25 times the dump's size** (6.0 GiB for 264 MiB) and grows with it, about ten times from 300,000 to 3,000,000 rows. GhostDump's peak is the same at both sizes.
* **One statement at a time bounds the AST by the largest statement, and a COPY block is one statement**: 59 MiB on INSERT at both sizes, but 450 MiB on COPY at 3,000,000 rows, where each block holds a whole table.
* **Parsing and printing alone, one statement at a time, take 6.4 to 6.6 times as long as GhostDump's copy of an INSERT dump**, and 2.4 to 2.5 times as long as GhostDump with every rule. On COPY, sqlparser keeps the fields of data rows as strings without parsing them as SQL, and is as fast as GhostDump's copy.
* **sqlparser rejects statements that the dump tools write**: the `\restrict` and `\unrestrict` meta-commands of pg_dump 16.15 and mysqldump 8.0.46's `/*!40000 ALTER TABLE ... DISABLE KEYS */`. An AST-based masker needs a way around them; this program copies them as they are.

This is one parser, not every AST-based tool; the sizes depend on how a parser represents values.

### Size of the dump

Generated dumps of 300,000 and 10,000,002 rows (100,000 and 3,333,334 users), 5 runs each after one warm-up, with the 3,000,000 rows of the A/A runs:

```bash
python3 scripts/bench_grid.py --sizes 100000,3333334 --configs no_rules,full --threads 1,3 --runs 5 \
    --out scale.csv
python3 scripts/bench_report.py scale scale.csv aa.csv
```

"Reading thread on CPU" is the CPU time of the reading thread over the wall time. Raw runs in [bench-results/2026-11-scale.csv](bench-results/2026-11-scale.csv).

| Format | Rules | Threads | Rows | Runs | Median (s) | IQR (s) | Rows/s | Peak memory (MiB) | Minor faults | Major faults | Context switches | Reading thread on CPU |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| COPY | `full` | 1 | 300,000 | 5 | 0.44 | 0.08 | 681,508 | 4.7–4.7 | 271 | 0 | 2 | 97% |
| COPY | `full` | 1 | 3,000,000 | 20 | 4.94 | 0.32 | 607,115 | 4.7–4.7 | 276 | 0 | 11 | 99% |
| COPY | `full` | 1 | 10,000,002 | 5 | 16.25 | 0.07 | 615,502 | 4.6–4.7 | 275 | 0 | 29 | 99% |
| COPY | `full` | 3 | 300,000 | 5 | 0.16 | 0.05 | 1,833,741 | 8.8–9.5 | 1,471 | 0 | 427 | 92% |
| COPY | `full` | 3 | 3,000,000 | 20 | 1.67 | 0.17 | 1,797,322 | 9.3–10.0 | 1,536 | 0 | 3,424 | 98% |
| COPY | `full` | 3 | 10,000,002 | 5 | 5.82 | 0.49 | 1,717,771 | 9.3–9.6 | 1,522 | 0 | 11,831 | 98% |
| COPY | `no_rules` | 1 | 300,000 | 5 | 0.16 | 0.03 | 1,897,533 | 4.6–4.7 | 264 | 0 | 1 | 96% |
| COPY | `no_rules` | 1 | 3,000,000 | 20 | 1.58 | 0.16 | 1,901,080 | 4.5–4.7 | 265 | 0 | 4 | 99% |
| COPY | `no_rules` | 1 | 10,000,002 | 5 | 5.12 | 0.56 | 1,951,334 | 4.5–4.7 | 267 | 0 | 11 | 99% |
| INSERT | `full` | 1 | 300,000 | 5 | 0.69 | 0.06 | 436,618 | 4.6–4.7 | 271 | 0 | 3 | 98% |
| INSERT | `full` | 1 | 3,000,000 | 20 | 6.90 | 0.43 | 434,997 | 4.7–4.7 | 275 | 0 | 14 | 99% |
| INSERT | `full` | 1 | 10,000,002 | 5 | 22.91 | 1.31 | 436,415 | 4.7–4.7 | 276 | 0 | 36 | 99% |
| INSERT | `full` | 3 | 300,000 | 5 | 0.27 | 0.02 | 1,128,668 | 8.6–9.7 | 1,524 | 0 | 592 | 95% |
| INSERT | `full` | 3 | 3,000,000 | 20 | 2.84 | 0.22 | 1,056,673 | 9.2–10.8 | 1,830 | 0 | 5,072 | 98% |
| INSERT | `full` | 3 | 10,000,002 | 5 | 9.93 | 0.85 | 1,006,857 | 9.4–10.0 | 1,699 | 0 | 17,244 | 98% |
| INSERT | `no_rules` | 1 | 300,000 | 5 | 0.26 | 0.04 | 1,141,118 | 4.5–4.6 | 264 | 0 | 2 | 95% |
| INSERT | `no_rules` | 1 | 3,000,000 | 20 | 2.66 | 0.16 | 1,127,184 | 4.5–4.7 | 266 | 0 | 6 | 99% |
| INSERT | `no_rules` | 1 | 10,000,002 | 5 | 8.89 | 0.54 | 1,124,404 | 4.6–4.7 | 267 | 0 | 15 | 99% |

* **Memory does not grow with the dump.** With one thread the peak is 4.5–4.7 MiB at every size, with three 8.6–10.8 MiB, and the minor page faults of a run are the same, 264–276 with one thread, for 300,000 rows as for 10^7: the process touches the same pages whatever the size. There are no major page faults, and the machine has no swap.
* **Throughput does not fall with the size**: with every rule and one thread, 607,000–682,000 rows per second on COPY and 435,000–437,000 on INSERT.
* **With three threads the reading thread is on the CPU 98% of the time** from 3,000,000 rows on (92–95% at 300,000 rows, in runs of 0.16–0.27 s): it bounds the run.
* The earlier tables of this document, measured on earlier builds, peaked at 5.7–6.9 MiB with one thread. Many changes came between those builds and this one, and the difference was not traced to one of them.

### Decision on the chunked parser

The [roadmap](roadmap.md) keeps the chunked parser and parallel reading for V3, if this stage confirmed the parser as the bottleneck on real loads. With threads, it does:

* With three threads, on the generated dumps and on pg_dump's and mysqldump's, the reading thread is on the CPU 98% of the time, and a run with every rule takes 2 to 9% longer than a copy without rules. Four threads were no faster than three in the runs of [parallel masking](#parallel-masking).
* The reading thread spends 68–79% of its time in the parser, which the engine calls once per byte of input (`SqlParser::handle_byte`).
* With one thread, the parser is a quarter (COPY) to two fifths (INSERT) of the time. The criterion of task CHK, measured again in the microbenchmarks, gives 36% and 41%, at its threshold of 40%.

What parallel reading can gain on the reference machine is bounded by its 4 vCPUs. With three threads and every rule (3,000,000 rows, medians of the A/A runs), a COPY run uses 5.56 s of CPU in 1.67 s, 3.3 cores, and an INSERT run 7.78 s in 2.84 s, 2.7 cores; the masking threads are busy 78% and 58% of the time. Spread over 4 cores, the same work takes at least 1.39 s and 1.94 s. Parsing in parallel does not reduce the work, so it can gain at most 17% on COPY and 31% on INSERT here; a faster sequential parser does reduce it, and is not bounded this way.

**Decision: the chunked parser is not built in V2, and stays in V3.** On this machine it can gain at most 17–31%. A chunk cannot start at any byte: whether a byte is inside a string, a comment, a COPY block or the body of a routine depends on every byte before it, so parallel reading needs a first pass to find the statements, which the single pass rules out, or a guess of the state at each chunk's start, checked when the chunk before it ends. On a machine with more cores the bound is higher, and the gain should be measured there.

What helps on every machine, and with one thread, is a faster sequential parser. In a COPY block only the end of a line changes its state, and inside quoted text only the closing quote or, where it escapes, a backslash (`Lexer::quoted_byte`), yet the parser takes one call per byte and copies each byte on its own into the row's buffer. Finding such bytes with `memchr` and copying the runs between them at once is a candidate for V3, to be measured with the instruction counts of this stage.
