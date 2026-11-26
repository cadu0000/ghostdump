"""Tables of the benchmark runs of `bench_grid.py`, in Markdown.

* `pairs CSV`: for runs with `--compare-binary`, the median time of each
  build, the difference between them with a bootstrap 95% interval, and the
  spread of each (coefficient of variation). Given two copies of one binary
  (an A/A run), the interval says how large a difference the machine makes
  on its own.
* `scale CSV [CSV ...]`: per dump size, the median time and throughput, the
  peak memory, the page faults and context switches, and the share of the
  wall time the reading thread spends on the CPU.
* `profile FILE`: the samples of `perf script -F pid,tid,ip,sym`, per thread
  and per part of the program, told apart by the names of the functions
  sampled (`CATEGORIES`). The reading thread is the one whose id is the
  process's.

Examples:

    python3 scripts/bench_report.py pairs docs/bench-results/2026-11-aa.csv
    python3 scripts/bench_report.py scale docs/bench-results/2026-11-scale*.csv
    perf record -e cpu-clock -F 1000 -o run.data ghostdump -i dump.sql -o /dev/null -c rules.toml
    perf script -i run.data -F pid,tid,ip,sym > run.txt
    python3 scripts/bench_report.py profile run.txt

Only the Python standard library is used.
"""

import argparse
import csv
import random
import re
import statistics
from collections import Counter, defaultdict

BOOTSTRAP = 10_000

# The first pattern found in a function's name, mangled or not, names its
# part of the program; samples in the kernel count apart.
CATEGORIES = [
    ("HMAC", r"sha2|hmac|hex|HexChars"),
    ("statement parser", r"ghostdump::parser|9ghostdump6parser"),
    ("row codec", r"ghostdump::codec|9ghostdump5codec"),
    ("Laplace", r"laplace"),
    ("random numbers", r"rand_chacha|rand_core|4rand4rngs|gen_range|3Rng"),
    ("fake values", r"4fake|fake::"),
    ("formatting", r"core3fmt|alloc3fmt|replace|7pattern|core::fmt|alloc::fmt"),
    ("allocation", r"^mi_|^_mi_|rust_alloc|rust_realloc|rust_dealloc|raw_vec|rust_no_alloc"),
    ("memcpy, memchr", r"memmove|memcpy|memchr|memset|memcmp"),
    ("engine", r"ghostdump::engine|9ghostdump6engine|9ghostdump2io"),
]


def read(paths):
    rows = []
    for path in paths:
        with open(path, newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


def cpu(row):
    return float(row["user_seconds"]) + float(row["system_seconds"])


def iqr(values):
    q1, _, q3 = statistics.quantiles(values, n=4)
    return q3 - q1


def bootstrap_ratio(a, b, seed):
    """The ratio of medians a/b with a percentile bootstrap 95% interval."""
    rng = random.Random(seed)
    ratios = sorted(
        statistics.median(rng.choices(a, k=len(a))) / statistics.median(rng.choices(b, k=len(b)))
        for _ in range(BOOTSTRAP)
    )
    return (
        statistics.median(a) / statistics.median(b),
        ratios[int(0.025 * BOOTSTRAP)],
        ratios[int(0.975 * BOOTSTRAP) - 1],
    )


def cv(values):
    return statistics.stdev(values) / statistics.fmean(values)


def pairs(args):
    groups = defaultdict(lambda: defaultdict(list))
    for row in read(args.csv):
        key = (row["format"], row["config"], int(row["threads"]), int(row["data_rows"]))
        groups[key][row["binary"]].append(row)
    print(
        "| Format | Rules | Threads | Runs | `--binary` (s) | `--compare-binary` (s) "
        "| Difference, wall [95%] | CV wall | Difference, CPU [95%] | CV CPU |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for seed, (key, by_binary) in enumerate(sorted(groups.items())):
        fmt, config, threads, _ = key
        a, b = by_binary["current"], by_binary["compare"]
        wall_a = [float(r["seconds"]) for r in a]
        wall_b = [float(r["seconds"]) for r in b]
        cpu_a, cpu_b = [cpu(r) for r in a], [cpu(r) for r in b]
        wall = bootstrap_ratio(wall_a, wall_b, seed)
        work = bootstrap_ratio(cpu_a, cpu_b, seed)
        print(
            f"| {fmt.upper()} | `{config}` | {threads} | {len(a)} "
            f"| {statistics.median(wall_a):.2f} | {statistics.median(wall_b):.2f} "
            f"| {percent(wall)} | {cv(wall_a):.1%}, {cv(wall_b):.1%} "
            f"| {percent(work)} | {cv(cpu_a):.1%}, {cv(cpu_b):.1%} |"
        )


def percent(ratio):
    point, lo, hi = ratio
    return f"{point - 1:+.1%} [{lo - 1:+.1%}, {hi - 1:+.1%}]"


def scale(args):
    groups = defaultdict(list)
    for row in read(args.csv):
        if row["binary"] != "current":
            continue
        key = (row["format"], row["config"], int(row["threads"]), int(row["data_rows"]))
        groups[key].append(row)
    print(
        "| Format | Rules | Threads | Rows | Runs | Median (s) | IQR (s) | Rows/s "
        "| Peak memory (MiB) | Minor faults | Major faults | Context switches "
        "| Reading thread on CPU |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(groups):
        fmt, config, threads, rows = key
        runs = groups[key]
        wall = [float(r["seconds"]) for r in runs]
        median = statistics.median(wall)
        spread = f"{iqr(wall):.2f}" if len(wall) > 2 else "–"
        peaks = [int(r["max_rss_kib"]) / 1024 for r in runs]
        minor = statistics.median(int(r["minor_faults"]) for r in runs)
        major = max(int(r["major_faults"]) for r in runs)
        switches = statistics.median(
            int(r["voluntary_switches"]) + int(r["involuntary_switches"]) for r in runs
        )
        busy = statistics.median(float(r["reader_cpu_seconds"]) / float(r["seconds"]) for r in runs)
        print(
            f"| {fmt.upper()} | `{config}` | {threads} | {rows:,} | {len(runs)} | {median:.2f} "
            f"| {spread} | {rows / median:,.0f} | {min(peaks):.1f}–{max(peaks):.1f} "
            f"| {minor:,.0f} | {major} | {switches:,.0f} | {busy:.0%} |"
        )


def category(symbol, address):
    if address.startswith("ffffffff"):
        return "kernel"
    for name, pattern in CATEGORIES:
        if re.search(pattern, symbol):
            return name
    return "other"


def profile(args):
    counts = defaultdict(Counter)
    with open(args.samples) as f:
        for line in f:
            # "pid/tid address symbol", where the symbol may be missing.
            fields = line.split(None, 2)
            if len(fields) < 2 or "/" not in fields[0]:
                continue
            pid, tid = fields[0].split("/")
            address = fields[1]
            symbol = fields[2].strip() if len(fields) > 2 else ""
            thread = "reading" if pid == tid else f"masking ({tid})"
            counts[thread][category(symbol, address)] += 1
    names = [name for name, _ in CATEGORIES] + ["kernel", "other"]
    print("| Thread | Samples | " + " | ".join(names) + " |")
    print("|---|---|" + "---|" * len(names))
    threads = sorted(counts, key=lambda t: (t != "reading", t))
    if len(threads) > 1:
        threads.append("all")
        counts["all"] = sum((counts[t] for t in threads[:-1]), Counter())
    for thread in threads:
        c = counts[thread]
        total = sum(c.values())
        cells = " | ".join(f"{c[name] / total:.0%}" for name in names)
        print(f"| {thread} | {total:,} | {cells} |")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="table", required=True)
    p = sub.add_parser("pairs", help="runs of two builds in alternation")
    p.add_argument("csv", nargs="+")
    p.set_defaults(run=pairs)
    s = sub.add_parser("scale", help="runs of one build over dump sizes")
    s.add_argument("csv", nargs="+")
    s.set_defaults(run=scale)
    f = sub.add_parser("profile", help="samples of perf script, by thread and part")
    f.add_argument("samples", help="output of `perf script -F pid,tid,ip,sym`")
    f.set_defaults(run=profile)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
