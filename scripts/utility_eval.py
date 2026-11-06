"""Offline utility evaluation of dp_laplace (task HAR).

Measures how much statistical utility a masked dump keeps, for the
questions of RQ-δ:

* three local-DP arms, all run through the GhostDump binary and differing
  only in the bounds written in the rules:
    - a_priori: bounds chosen without the data ([0, 10000]), the regime of a
      single pass;
    - dp_bounds: bounds from DP quantiles in a first pass over the column
      (a 10% share of ε, central DP), then GhostDump with the rest;
    - oracle: the true minimum and maximum (not private; the best case);
* two analysts on each masked dump:
    - naive: SQL straight on the dump (AVG, COUNT ... WHERE, percentiles);
    - estimator: post-processing that deconvolves the Laplace noise (EM on
      a histogram) before answering; it spends no privacy;
* a central-DP comparator that answers each query from the raw data with
  the same ε (the global-access upper bound).

Every combination of n, ε, arm and repetition writes one CSV row per query.
A summary of mean errors is printed as a Markdown table.

Example:

    cargo build --release
    python3 scripts/utility_eval.py --sizes 10000,100000 --epsilons 0.5,1,2 --reps 10

Only the Python standard library is used.
"""

import argparse
import bisect
import csv
import math
import os
import random
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from sql_generator import Generator  # noqa: E402  (same salary distribution)

A_PRIORI = (0.0, 10_000.0)
# Coarse public range in which the DP quantiles are searched.
QUANTILE_GRID = (0.0, 100_000.0, 100.0)
BOUNDS_SHARE = 0.1
HIST_EDGES = [i * 1000.0 for i in range(21)]  # 20 bins over [0, 20000)
EM_TRUE_BINS = 50
EM_OBS_BINS = 240
EM_ITERATIONS = 200

QUERIES = [
    "mean",
    "sum",
    "count",
    "range_50pct",
    "range_10pct",
    "range_2pct",
    "median",
    "p90",
    "histogram_tv",
]


# ==========================================
# DATA
# ==========================================
def make_salaries(n, seed):
    """n salaries with the generator's distribution (5% NULL)."""
    generator = Generator(seed, n, {"salary": 0.05})
    column = {"name": "salary", "type": "salary"}
    return [generator.value(column, i) for i in range(1, n + 1)]


def write_dump(path, salaries):
    with open(path, "w") as f:
        f.write("CREATE TABLE users (id integer, salary numeric(12,2));\n")
        f.write("COPY users (id, salary) FROM stdin;\n")
        for i, value in enumerate(salaries, 1):
            field = "\\N" if value is None else f"{value:.2f}"
            f.write(f"{i}\t{field}\n")
        f.write("\\.\n")


def read_masked(path):
    values = []
    with open(path) as f:
        in_copy = False
        for line in f:
            if line.startswith("COPY users"):
                in_copy = True
            elif line.startswith("\\."):
                in_copy = False
            elif in_copy:
                field = line.rstrip("\n").split("\t")[1]
                values.append(None if field == "\\N" else float(field))
    return values


def run_ghostdump(binary, dump, epsilon, bounds, seed, workdir):
    rules = workdir / "rules.toml"
    lo, hi = bounds
    rules.write_text(
        "[tables.users]\ncolumns = [\n"
        f'    {{ name = "salary", strategy = "dp_laplace", epsilon = {epsilon!r}, '
        f"bounds = [{lo!r}, {hi!r}], scale = 2 }},\n]\n"
    )
    out = workdir / "masked.sql"
    subprocess.run(
        [str(binary), "-c", str(rules), "-i", str(dump), "-o", str(out), "--seed", str(seed)],
        check=True,
        env=dict(os.environ, GHOSTDUMP_LOG="error"),
    )
    return read_masked(out)


# ==========================================
# QUERIES
# ==========================================
def quantile(sorted_values, q):
    """Linear-interpolated quantile, as PostgreSQL's percentile_cont."""
    if not sorted_values:
        return float("nan")
    pos = q * (len(sorted_values) - 1)
    low = math.floor(pos)
    high = min(low + 1, len(sorted_values) - 1)
    return sorted_values[low] + (pos - low) * (sorted_values[high] - sorted_values[low])


def histogram(values, edges):
    """Shares of the values in each bin, with an underflow and an overflow bin."""
    counts = [0] * (len(edges) + 1)
    for v in values:
        counts[bisect.bisect_right(edges, v)] += 1
    total = len(values) or 1
    return [c / total for c in counts]


def total_variation(p, q):
    return 0.5 * sum(abs(a - b) for a, b in zip(p, q))


def range_queries(true_sorted):
    """Ranges centred on the median holding 50%, 10% and 2% of the values."""
    ranges = {}
    for name, share in (("range_50pct", 0.5), ("range_10pct", 0.1), ("range_2pct", 0.02)):
        ranges[name] = (quantile(true_sorted, 0.5 - share / 2), quantile(true_sorted, 0.5 + share / 2))
    return ranges


def answer_naive(values, ranges):
    present = sorted(v for v in values if v is not None)
    answers = {
        "mean": statistics.fmean(present),
        "sum": math.fsum(present),
        "count": len(present),
        "median": quantile(present, 0.5),
        "p90": quantile(present, 0.9),
        "histogram": histogram(present, HIST_EDGES),
    }
    for name, (a, b) in ranges.items():
        answers[name] = bisect.bisect_left(present, b) - bisect.bisect_left(present, a)
    return answers


# ==========================================
# ESTIMATOR: deconvolution by EM
# ==========================================
def laplace_cdf(x, b):
    return 0.5 * math.exp(x / b) if x < 0 else 1.0 - 0.5 * math.exp(-x / b)


def deconvolve(noisy, bounds, b):
    """Distribution of the clamped values over EM_TRUE_BINS bins of the
    bounds, estimated from the noisy values (Laplace noise of scale b)."""
    lo, hi = bounds
    width = (hi - lo) / EM_TRUE_BINS
    centres = [lo + (j + 0.5) * width for j in range(EM_TRUE_BINS)]
    obs_lo, obs_hi = lo - 6 * b, hi + 6 * b
    obs_width = (obs_hi - obs_lo) / EM_OBS_BINS
    edges = [obs_lo + m * obs_width for m in range(EM_OBS_BINS + 1)]

    counts = [0] * EM_OBS_BINS
    for y in noisy:
        m = min(EM_OBS_BINS - 1, max(0, int((y - obs_lo) // obs_width)))
        counts[m] += 1

    # kernel[m][j] = P(noisy value in bin m | true value at centre j); the
    # outer bins absorb the tails.
    kernel = []
    for m in range(EM_OBS_BINS):
        row = []
        for c in centres:
            low = 0.0 if m == 0 else laplace_cdf(edges[m] - c, b)
            high = 1.0 if m == EM_OBS_BINS - 1 else laplace_cdf(edges[m + 1] - c, b)
            row.append(high - low)
        kernel.append(row)

    observed = [(counts[m], kernel[m]) for m in range(EM_OBS_BINS) if counts[m]]
    total = len(noisy)
    p = [1.0 / EM_TRUE_BINS] * EM_TRUE_BINS
    for _ in range(EM_ITERATIONS):
        acc = [0.0] * EM_TRUE_BINS
        for count, row in observed:
            denom = sum(pj * k for pj, k in zip(p, row))
            if denom <= 0:
                continue
            factor = count / denom
            for j in range(EM_TRUE_BINS):
                acc[j] += row[j] * factor
        p = [pj * a / total for pj, a in zip(p, acc)]
    return centres, width, p


def answer_estimator(noisy, bounds, b, ranges):
    present = [v for v in noisy if v is not None]
    centres, width, p = deconvolve(present, bounds, b)
    n = len(present)

    def share_between(a, c):
        """Estimated share of values in [a, c), uniform inside each bin."""
        total = 0.0
        for centre, pj in zip(centres, p):
            left, right = centre - width / 2, centre + width / 2
            overlap = max(0.0, min(right, c) - max(left, a))
            total += pj * overlap / width
        return total

    def inverse_cdf(q):
        acc = 0.0
        for centre, pj in zip(centres, p):
            if acc + pj >= q:
                left = centre - width / 2
                return left + width * ((q - acc) / pj if pj else 0.0)
            acc += pj
        return centres[-1] + width / 2

    mean = sum(c * pj for c, pj in zip(centres, p))
    hist_inner = [share_between(HIST_EDGES[i], HIST_EDGES[i + 1]) for i in range(len(HIST_EDGES) - 1)]
    under = share_between(-math.inf, HIST_EDGES[0])
    over = share_between(HIST_EDGES[-1], math.inf)
    answers = {
        "mean": mean,
        "sum": mean * n,
        "count": n,
        "median": inverse_cdf(0.5),
        "p90": inverse_cdf(0.9),
        "histogram": [under] + hist_inner + [over],
    }
    for name, (a, c) in ranges.items():
        answers[name] = share_between(a, c) * n
    return answers


# ==========================================
# CENTRAL DP COMPARATOR
# ==========================================
def laplace(rng, b):
    magnitude = -b * math.log(1.0 - rng.random())
    return magnitude if rng.random() < 0.5 else -magnitude


def dp_quantile(rng, sorted_values, q, epsilon, grid):
    """Exponential mechanism over a fixed grid of candidates; utility is
    minus the distance between the candidate's rank and q·n (sensitivity 1)."""
    lo, hi, step = grid
    candidates = [lo + i * step for i in range(int((hi - lo) / step) + 1)]
    target = q * len(sorted_values)
    scores = [-abs(bisect.bisect_left(sorted_values, c) - target) for c in candidates]
    best = max(scores)
    weights = [math.exp(epsilon * (s - best) / 2.0) for s in scores]
    return rng.choices(candidates, weights=weights)[0]


def answer_central(rng, values, bounds, epsilon, ranges):
    """Each query answered independently from the raw data with ε-DP under
    replacement of one value. The NULL pattern, and so the count, is public,
    as in the local arms (null_policy = "keep")."""
    lo, hi = bounds
    clamped = sorted(min(hi, max(lo, v)) for v in values if v is not None)
    n = len(clamped)
    delta = hi - lo
    total = math.fsum(clamped)
    answers = {
        "count": n,
        "sum": total + laplace(rng, delta / epsilon),
        "mean": (total + laplace(rng, delta / epsilon)) / n,
        "median": dp_quantile(rng, clamped, 0.5, epsilon, (lo, hi, delta / 1000)),
        "p90": dp_quantile(rng, clamped, 0.9, epsilon, (lo, hi, delta / 1000)),
    }
    for name, (a, b) in ranges.items():
        count = bisect.bisect_left(clamped, b) - bisect.bisect_left(clamped, a)
        answers[name] = count + laplace(rng, 1.0 / epsilon)
    # Replacing one value moves one unit between two bins: L1 sensitivity 2.
    hist = histogram(clamped, HIST_EDGES)
    noisy = [max(0.0, h * n + laplace(rng, 2.0 / epsilon)) for h in hist]
    s = sum(noisy) or 1.0
    answers["histogram"] = [v / s for v in noisy]
    return answers


# ==========================================
# ERRORS
# ==========================================
def errors(answers, truth):
    """Relative error per query (signed for mean, to show bias), and total
    variation distance for the histogram."""
    rows = {}
    for query in QUERIES:
        if query == "histogram_tv":
            rows[query] = (total_variation(answers["histogram"], truth["histogram"]), None)
            continue
        estimate, true = answers[query], truth[query]
        rows[query] = ((estimate - true) / true if true else float("nan"), estimate)
    return rows


def dp_bounds(rng, values, epsilon_share):
    present = sorted(v for v in values if v is not None)
    lo = dp_quantile(rng, present, 0.01, epsilon_share / 2, QUANTILE_GRID)
    hi = dp_quantile(rng, present, 0.99, epsilon_share / 2, QUANTILE_GRID)
    if hi <= lo:
        hi = lo + QUANTILE_GRID[2]
    return lo, hi


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", default="10000,100000")
    parser.add_argument("--epsilons", default="0.5,1,2")
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42, help="seed of the dataset")
    parser.add_argument(
        "--binary", default=str(PROJECT_ROOT / "target" / "release" / "ghostdump")
    )
    parser.add_argument("--out", default="utility_results.csv")
    args = parser.parse_args()

    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for n in (int(x) for x in args.sizes.split(",")):
            salaries = make_salaries(n, args.seed)
            dump = workdir / "dump.sql"
            write_dump(dump, salaries)
            true_sorted = sorted(v for v in salaries if v is not None)
            ranges = range_queries(true_sorted)
            truth = answer_naive(salaries, ranges)
            oracle = (math.floor(true_sorted[0]), math.ceil(true_sorted[-1]))

            for epsilon in (float(e) for e in args.epsilons.split(",")):
                for rep in range(args.reps):
                    rng = random.Random(f"{args.seed}-{n}-{epsilon}-{rep}")
                    arms = {
                        "a_priori": (A_PRIORI, epsilon),
                        "dp_bounds": (
                            dp_bounds(rng, salaries, BOUNDS_SHARE * epsilon),
                            (1 - BOUNDS_SHARE) * epsilon,
                        ),
                        "oracle": (oracle, epsilon),
                    }
                    for arm, (bounds, value_epsilon) in arms.items():
                        noisy = run_ghostdump(
                            args.binary, dump, value_epsilon, bounds, rep + 1, workdir
                        )
                        b = (bounds[1] - bounds[0]) / value_epsilon
                        for analyst, answers in (
                            ("naive", answer_naive(noisy, ranges)),
                            ("estimator", answer_estimator(noisy, bounds, b, ranges)),
                        ):
                            rows += result_rows(n, epsilon, arm, analyst, rep, bounds, answers, truth)
                    for arm, bounds in (("central_a_priori", A_PRIORI), ("central_oracle", oracle)):
                        answers = answer_central(rng, salaries, bounds, epsilon, ranges)
                        rows += result_rows(n, epsilon, arm, "central", rep, bounds, answers, truth)
                print(f"n = {n}, ε = {epsilon}: done", file=sys.stderr)

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print_summary(rows)
    print(f"\n{len(rows)} rows written to {args.out}", file=sys.stderr)


def result_rows(n, epsilon, arm, analyst, rep, bounds, answers, truth):
    out = []
    for query, (error, estimate) in errors(answers, truth).items():
        out.append(
            {
                "n": n,
                "epsilon": epsilon,
                "arm": arm,
                "analyst": analyst,
                "rep": rep,
                "lo": round(bounds[0], 2),
                "hi": round(bounds[1], 2),
                "query": query,
                "estimate": "" if estimate is None else round(estimate, 4),
                "truth": "" if query == "histogram_tv" else round(truth[query], 4),
                "error": round(error, 6),
            }
        )
    return out


def print_summary(rows):
    """Mean absolute error per configuration and query (signed mean error for
    `mean`, which shows the bias)."""
    groups = {}
    for r in rows:
        key = (r["n"], r["epsilon"], r["arm"], r["analyst"])
        groups.setdefault(key, {}).setdefault(r["query"], []).append(r["error"])
    header = ["n", "ε", "arm", "analyst", "mean (signed)"] + [q for q in QUERIES if q != "mean"]
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for (n, eps, arm, analyst), by_query in sorted(groups.items()):
        cells = [str(n), str(eps), arm, analyst, f"{statistics.fmean(by_query['mean']):+.4f}"]
        for q in QUERIES:
            if q == "mean":
                continue
            cells.append(f"{statistics.fmean(abs(e) for e in by_query[q]):.4f}")
        print("| " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
