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

With `--task categorical`, the same is done for a categorical column
(`status`, 70/20/10): `random_choice` against `randomized_response` read
straight (SQL shares) and through the unbiased frequency estimator, and
central DP (Laplace on the counts).

With `--task adult`, both are done on the UCI Adult dataset (48,842 rows,
https://archive.ics.uci.edu/dataset/2/adult), one column at a time: the
numeric columns `age` and `hours_per_week`, and the categorical columns
`sex`, `race`, `workclass` and `education` (2, 5, 9 and 16 categories).
`--adult-dir` names a directory holding `adult.data` and `adult.test`; the
files are checked against their SHA-256 before use.

The summary gives, for each column and configuration, the mean error over
the repetitions and its standard deviation. `--summarize FILE` prints it
again from a CSV written by an earlier run.

Examples:

    cargo build --release
    python3 scripts/utility_eval.py --sizes 10000,100000 --epsilons 0.5,1,2 --reps 10
    python3 scripts/utility_eval.py --task adult --adult-dir adult/ --epsilons 0.1,1,10
    python3 scripts/utility_eval.py --summarize docs/bench-results/2026-11-utility.csv

Only the Python standard library is used.
"""

import argparse
import bisect
import csv
import hashlib
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
# EM stops when the estimated distribution moves less than EM_TOLERANCE (L1)
# in one iteration, or after EM_MAX_ITERATIONS. With noise much wider than
# the bounds, EM converges slowly: a fixed small number of iterations leaves
# the estimate near its uniform start.
EM_TOLERANCE = 1e-5
EM_MAX_ITERATIONS = 5000

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


def read_masked(path, convert=float):
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
                values.append(None if field == "\\N" else convert(field))
    return values


def run_ghostdump(binary, dump, epsilon, bounds, seed, workdir):
    rule = laplace_rule("salary", epsilon, bounds, scale=2)
    return read_masked(mask(binary, dump, rule, seed, workdir))


def laplace_rule(column, epsilon, bounds, scale):
    lo, hi = bounds
    return (
        f'{{ name = "{column}", strategy = "dp_laplace", epsilon = {epsilon!r}, '
        f"bounds = [{lo!r}, {hi!r}], scale = {scale} }}"
    )


def mask(binary, dump, rules, seed, workdir, table="users"):
    """Runs GhostDump with the rules (one, or a list) on one table; returns
    the output path."""
    if isinstance(rules, str):
        rules = [rules]
    listed = "".join(f"    {rule},\n" for rule in rules)
    rules_file = workdir / "rules.toml"
    rules_file.write_text(f"[tables.{table}]\ncolumns = [\n{listed}]\n")
    out = workdir / "masked.sql"
    subprocess.run(
        [str(binary), "-c", str(rules_file), "-i", str(dump), "-o", str(out), "--seed", str(seed)],
        check=True,
        env=dict(os.environ, GHOSTDUMP_LOG="error"),
    )
    return out


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


def whole_ranges(true_sorted):
    """Ranges for whole numbers, where the quantiles around the median often
    coincide (40 hours is both the 45% and the 55% quantile of
    hours_per_week): [⌊a⌋, ⌈b⌉] with both ends, written as the half-open
    [⌊a⌋ − 0.5, ⌈b⌉ + 0.5) that the queries count. A range then holds at least
    the share in its name."""
    return {
        name: (math.floor(a) - 0.5, math.ceil(b) + 0.5)
        for name, (a, b) in range_queries(true_sorted).items()
    }


def answer_naive(values, ranges, edges=HIST_EDGES):
    present = sorted(v for v in values if v is not None)
    answers = {
        "mean": statistics.fmean(present),
        "sum": math.fsum(present),
        "count": len(present),
        "median": quantile(present, 0.5),
        "p90": quantile(present, 0.9),
        "histogram": histogram(present, edges),
    }
    for name, (a, b) in ranges.items():
        answers[name] = bisect.bisect_left(present, b) - bisect.bisect_left(present, a)
    return answers


# ==========================================
# ESTIMATOR: deconvolution by EM
# ==========================================
def laplace_cdf(x, b):
    return 0.5 * math.exp(x / b) if x < 0 else 1.0 - 0.5 * math.exp(-x / b)


def deconvolve(noisy, bounds, b, unit=None):
    """Distribution of the clamped values over bins of the bounds, estimated
    from the noisy values (Laplace noise of scale b). Returns the bins, as
    (left, right, centre, width), and their estimated shares.

    Without `unit`, the bins are EM_TRUE_BINS equal parts of the bounds, and
    the noisy values are counted in EM_OBS_BINS equal cells. With `unit`, the
    step of the values (1 for whole numbers, where the noise is a whole
    number too), both follow that grid: one bin per value the bounds allow,
    and cells of whole units centred on the values. Cells that do not follow
    the grid fall between the values the noise can take, and stay empty
    where the model expects mass."""
    lo, hi = bounds
    if unit is None:
        width = (hi - lo) / EM_TRUE_BINS
        centres = [lo + (j + 0.5) * width for j in range(EM_TRUE_BINS)]
        bins = [(c - width / 2, c + width / 2, c, width) for c in centres]
        obs_lo, obs_hi = lo - 6 * b, hi + 6 * b
        obs_cells = EM_OBS_BINS
        obs_width = (obs_hi - obs_lo) / obs_cells
    else:
        centres = [lo + j * unit for j in range(round((hi - lo) / unit) + 1)]
        bins = [(c - unit / 2, c + unit / 2, c, unit) for c in centres]
        margin = math.ceil(6 * b / unit)
        values = round((hi - lo) / unit) + 1 + 2 * margin
        per_cell = math.ceil(values / EM_OBS_BINS)
        obs_cells = math.ceil(values / per_cell)
        obs_width = per_cell * unit
        obs_lo = lo - (margin + 0.5) * unit
    edges = [obs_lo + m * obs_width for m in range(obs_cells + 1)]

    counts = [0] * obs_cells
    for y in noisy:
        m = min(obs_cells - 1, max(0, int((y - obs_lo) // obs_width)))
        counts[m] += 1

    # kernel[m][j] = P(noisy value in bin m | true value at centre j); the
    # outer bins absorb the tails.
    kernel = []
    for m in range(obs_cells):
        row = []
        for c in centres:
            low = 0.0 if m == 0 else laplace_cdf(edges[m] - c, b)
            high = 1.0 if m == obs_cells - 1 else laplace_cdf(edges[m + 1] - c, b)
            row.append(high - low)
        kernel.append(row)

    observed = [(counts[m], kernel[m]) for m in range(obs_cells) if counts[m]]
    total = len(noisy)
    p = [1.0 / len(centres)] * len(centres)
    for _ in range(EM_MAX_ITERATIONS):
        acc = [0.0] * len(centres)
        for count, row in observed:
            denom = sum(pj * k for pj, k in zip(p, row))
            if denom <= 0:
                continue
            factor = count / denom
            for j in range(len(centres)):
                acc[j] += row[j] * factor
        new = [pj * a / total for pj, a in zip(p, acc)]
        moved = sum(abs(a - b) for a, b in zip(new, p))
        p = new
        if moved < EM_TOLERANCE:
            break
    return bins, p


def answer_estimator(noisy, bounds, b, ranges, edges=HIST_EDGES, unit=None):
    present = [v for v in noisy if v is not None]
    bins, p = deconvolve(present, bounds, b, unit)
    n = len(present)

    def share_between(a, c):
        """Estimated share of values in [a, c), uniform inside each bin."""
        total = 0.0
        for (left, right, _, width), pj in zip(bins, p):
            overlap = max(0.0, min(right, c) - max(left, a))
            total += pj * overlap / width
        return total

    def inverse_cdf(q):
        acc = 0.0
        for (left, _, _, width), pj in zip(bins, p):
            if acc + pj >= q:
                return left + width * ((q - acc) / pj if pj else 0.0)
            acc += pj
        return bins[-1][2] + bins[-1][3] / 2

    mean = sum(centre * pj for (_, _, centre, _), pj in zip(bins, p))
    hist_inner = [share_between(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]
    under = share_between(-math.inf, edges[0])
    over = share_between(edges[-1], math.inf)
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


def answer_central(rng, values, bounds, epsilon, ranges, edges=HIST_EDGES):
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
    hist = histogram(clamped, edges)
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


def dp_bounds(rng, values, epsilon_share, grid=QUANTILE_GRID):
    present = sorted(v for v in values if v is not None)
    lo = dp_quantile(rng, present, 0.01, epsilon_share / 2, grid)
    hi = dp_quantile(rng, present, 0.99, epsilon_share / 2, grid)
    if hi <= lo:
        hi = lo + grid[2]
    return lo, hi


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--task", choices=["numeric", "categorical", "adult"], default="numeric")
    parser.add_argument("--adult-dir", help="directory with adult.data and adult.test (--task adult)")
    parser.add_argument("--summarize", metavar="CSV", help="print the summary of an earlier run and exit")
    parser.add_argument("--query", help="with --summarize: one table of this query, arms by n or by ε")
    parser.add_argument("--column", help="with --query: the column (for CSVs with several)")
    parser.add_argument("--n", type=int, help="with --query: fix n, one table column per ε")
    parser.add_argument("--epsilon", type=float, help="with --query: fix ε, one table column per n")
    parser.add_argument("--sizes", default="10000,100000")
    parser.add_argument("--epsilons", default="0.5,1,2")
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42, help="seed of the dataset")
    parser.add_argument(
        "--binary", default=str(PROJECT_ROOT / "target" / "release" / "ghostdump")
    )
    parser.add_argument("--out", default="utility_results.csv")
    args = parser.parse_args()

    if args.summarize:
        with open(args.summarize, newline="") as f:
            rows = list(csv.DictReader(f))
        if args.query:
            if (args.n is None) == (args.epsilon is None):
                parser.error("--query needs one of --n and --epsilon")
            print_query(rows, args.query, args.column, args.n, args.epsilon)
        else:
            print_summary(rows)
        return
    if args.task == "adult" and not args.adult_dir:
        parser.error("--task adult needs --adult-dir")

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        if args.task == "numeric":
            rows = run_numeric(args, workdir)
        elif args.task == "categorical":
            rows = run_categorical(args, workdir)
        else:
            rows = run_adult(args, workdir)

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print_summary(rows)
    print(f"\n{len(rows)} rows written to {args.out}", file=sys.stderr)


def run_numeric(args, workdir):
    rows = []
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
                        rows += result_rows("salary", n, epsilon, arm, analyst, rep, bounds, answers, truth)
                for arm, bounds in (("central_a_priori", A_PRIORI), ("central_oracle", oracle)):
                    answers = answer_central(rng, salaries, bounds, epsilon, ranges)
                    rows += result_rows("salary", n, epsilon, arm, "central", rep, bounds, answers, truth)
            print(f"n = {n}, ε = {epsilon}: done", file=sys.stderr)
    return rows


# ==========================================
# CATEGORICAL COLUMN
# ==========================================
CATEGORIES = ["ACTIVE", "PENDING", "INACTIVE"]
CATEGORICAL_QUERIES = [f"share_{c}" for c in CATEGORIES] + ["tv"]


def make_statuses(n, seed):
    generator = Generator(seed, n, {})
    column = {"name": "status", "type": "status"}
    return [generator.value(column, i) for i in range(1, n + 1)]


def write_category_dump(path, values):
    with open(path, "w") as f:
        f.write("CREATE TABLE users (id integer, status text);\n")
        f.write("COPY users (id, status) FROM stdin;\n")
        for i, value in enumerate(values, 1):
            f.write(f"{i}\t{value}\n")
        f.write("\\.\n")


def shares(values, categories=CATEGORIES):
    counts = {c: 0 for c in categories}
    for v in values:
        if v in counts:
            counts[v] += 1
    total = sum(counts.values()) or 1
    return [counts[c] / total for c in categories]


def project(estimates):
    """Clips negative shares to 0 and renormalizes (post-processing)."""
    clipped = [max(0.0, e) for e in estimates]
    total = sum(clipped) or 1.0
    return [c / total for c in clipped]


def rr_estimator(observed, epsilon):
    """Unbiased frequency estimate for k-ary randomized response:
    (observed − q) / (p − q), with p = e^ε/(e^ε+k−1) and q = 1/(e^ε+k−1)."""
    k = len(observed)
    e = math.exp(epsilon)
    p, q = e / (e + k - 1), 1 / (e + k - 1)
    return project([(o - q) / (p - q) for o in observed])


def central_shares(rng, values, epsilon, categories=CATEGORIES):
    """Laplace noise on each count; replacing one value changes two counts
    by one, so the L1 sensitivity is 2."""
    n = len(values)
    return project([s * n + laplace(rng, 2.0 / epsilon) for s in shares(values, categories)])


def categorical_rows(n, epsilon, arm, analyst, rep, estimate, truth, column="status", categories=CATEGORIES):
    out = []
    errors = {f"share_{c}": e - t for c, e, t in zip(categories, estimate, truth)}
    errors["max_share"] = max(abs(e - t) for e, t in zip(estimate, truth))
    errors["tv"] = total_variation(estimate, truth)
    for query, error in errors.items():
        out.append(
            {
                "column": column,
                "n": n,
                "epsilon": epsilon,
                "arm": arm,
                "analyst": analyst,
                "rep": rep,
                "lo": "",
                "hi": "",
                "query": query,
                "estimate": "",
                "truth": "",
                "error": round(error, 6),
            }
        )
    return out


def run_categorical(args, workdir):
    rows = []
    options = ", ".join(f'"{c}"' for c in CATEGORIES)
    for n in (int(x) for x in args.sizes.split(",")):
        statuses = make_statuses(n, args.seed)
        dump = workdir / "dump.sql"
        write_category_dump(dump, statuses)
        truth = shares(statuses)
        for epsilon in (float(e) for e in args.epsilons.split(",")):
            for rep in range(args.reps):
                rng = random.Random(f"{args.seed}-{n}-{epsilon}-{rep}-categorical")
                choice = mask(
                    args.binary,
                    dump,
                    f'{{ name = "status", strategy = "random_choice", options = [{options}] }}',
                    rep + 1,
                    workdir,
                )
                observed = shares(read_masked(choice, str))
                rows += categorical_rows(n, epsilon, "random_choice", "naive", rep, observed, truth)

                rr = mask(
                    args.binary,
                    dump,
                    f'{{ name = "status", strategy = "randomized_response", '
                    f"epsilon = {epsilon!r}, categories = [{options}] }}",
                    rep + 1,
                    workdir,
                )
                observed = shares(read_masked(rr, str))
                rows += categorical_rows(n, epsilon, "randomized_response", "naive", rep, observed, truth)
                rows += categorical_rows(
                    n, epsilon, "randomized_response", "estimator", rep, rr_estimator(observed, epsilon), truth
                )
                rows += categorical_rows(
                    n, epsilon, "central", "central", rep, central_shares(rng, statuses, epsilon), truth
                )
            print(f"n = {n}, ε = {epsilon}: done", file=sys.stderr)
    return rows


# ==========================================
# ADULT DATASET
# ==========================================
# The files of https://archive.ics.uci.edu/static/public/2/adult.zip
# (Becker and Kohavi, 1996, https://doi.org/10.24432/C5XW20).
ADULT_SHA256 = {
    "adult.data": "5b00264637dbfec36bdeaab5676b0b309ff9eb788d63554ca0a249491c86603d",
    "adult.test": "a2a9044bc167a35b2361efbabec64e89d69ce82d9790d2980119aac5fd7e9c05",
}
# The fields of a record, in the order of adult.names.
ADULT_FIELDS = [
    "age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
    "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
    "hours_per_week", "native_country", "income",
]
# Bounds a data owner can set from what the column means, without reading
# it: an age in years, and hours in a working week. The DP quantiles of the
# dp_bounds arm are searched on a grid of whole units over the same range.
ADULT_NUMERIC = {
    "age": (0.0, 100.0),
    "hours_per_week": (0.0, 100.0),
}
# The categories as adult.names lists them; adult.names also says that
# unknown values were converted to "?", which workclass has.
ADULT_CATEGORICAL = {
    "sex": ["Female", "Male"],
    "race": ["White", "Asian-Pac-Islander", "Amer-Indian-Eskimo", "Other", "Black"],
    "workclass": [
        "Private", "Self-emp-not-inc", "Self-emp-inc", "Federal-gov", "Local-gov",
        "State-gov", "Without-pay", "Never-worked", "?",
    ],
    "education": [
        "Bachelors", "Some-college", "11th", "HS-grad", "Prof-school", "Assoc-acdm",
        "Assoc-voc", "9th", "7th-8th", "12th", "Masters", "1st-4th", "10th",
        "Doctorate", "5th-6th", "Preschool",
    ],
}
ADULT_COLUMNS = list(ADULT_NUMERIC) + list(ADULT_CATEGORICAL)


def load_adult(directory):
    """The records of adult.data and adult.test; stops when a file is not
    the published one."""
    records = []
    for name, digest in ADULT_SHA256.items():
        path = Path(directory) / name
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            sys.exit(f"{path}: SHA-256 differs from the published file")
        for line in data.decode("ascii").splitlines():
            fields = [field.strip() for field in line.split(",")]
            # Skips the blank last lines and the first line of adult.test.
            if len(fields) == len(ADULT_FIELDS):
                records.append(dict(zip(ADULT_FIELDS, fields)))
    return records


def write_adult_dump(path, records):
    types = {c: "integer" for c in ADULT_NUMERIC} | {c: "text" for c in ADULT_CATEGORICAL}
    declared = ", ".join(f"{c} {types[c]}" for c in ADULT_COLUMNS)
    with open(path, "w") as f:
        f.write(f"CREATE TABLE adult (id integer, {declared});\n")
        f.write(f"COPY adult (id, {', '.join(ADULT_COLUMNS)}) FROM stdin;\n")
        for i, record in enumerate(records, 1):
            f.write("\t".join([str(i)] + [record[c] for c in ADULT_COLUMNS]) + "\n")
        f.write("\\.\n")


def read_masked_columns(path, table, columns):
    """The values of each column of a masked COPY block, as text (None for NULL)."""
    values = {c: [] for c in columns}
    with open(path) as f:
        in_copy = False
        for line in f:
            if line.startswith(f"COPY {table} "):
                in_copy = True
            elif line.startswith("\\."):
                in_copy = False
            elif in_copy:
                fields = line.rstrip("\n").split("\t")[1:]
                for column, field in zip(columns, fields):
                    values[column].append(None if field == "\\N" else field)
    return values


def run_adult(args, workdir):
    records = load_adult(args.adult_dir)
    n = len(records)
    dump = workdir / "adult.sql"
    write_adult_dump(dump, records)

    def masked(rules, seed):
        return read_masked_columns(mask(args.binary, dump, rules, seed, workdir, "adult"), "adult", ADULT_COLUMNS)

    numeric = {c: [float(r[c]) for r in records] for c in ADULT_NUMERIC}
    setup = {}
    for column, values in numeric.items():
        lo, hi = ADULT_NUMERIC[column]
        # Edges between whole numbers, so that each bin holds the same whole
        # numbers as with edges on them, and the estimator's bins fit in it.
        edges = [lo - 0.5 + i * (hi - lo) / 20 for i in range(21)]
        true_sorted = sorted(values)
        ranges = whole_ranges(true_sorted)
        oracle = (true_sorted[0], true_sorted[-1])
        setup[column] = (edges, ranges, answer_naive(values, ranges, edges), oracle)
    categorical = {c: [r[c] for r in records] for c in ADULT_CATEGORICAL}
    options = {c: ", ".join(f'"{v}"' for v in cats) for c, cats in ADULT_CATEGORICAL.items()}

    rows = []
    for epsilon in (float(e) for e in args.epsilons.split(",")):
        for rep in range(args.reps):
            rng = random.Random(f"adult-{epsilon}-{rep}")
            arms = {
                "a_priori": ({c: ADULT_NUMERIC[c] for c in numeric}, epsilon),
                "dp_bounds": (
                    {
                        c: dp_bounds(rng, numeric[c], BOUNDS_SHARE * epsilon, (*ADULT_NUMERIC[c], 1.0))
                        for c in numeric
                    },
                    (1 - BOUNDS_SHARE) * epsilon,
                ),
                "oracle": ({c: setup[c][3] for c in numeric}, epsilon),
            }
            for arm, (bounds, value_epsilon) in arms.items():
                out = masked([laplace_rule(c, value_epsilon, bounds[c], scale=0) for c in numeric], rep + 1)
                for column in numeric:
                    edges, ranges, truth, _ = setup[column]
                    noisy = [None if v is None else float(v) for v in out[column]]
                    b = (bounds[column][1] - bounds[column][0]) / value_epsilon
                    for analyst, answers in (
                        ("naive", answer_naive(noisy, ranges, edges)),
                        ("estimator", answer_estimator(noisy, bounds[column], b, ranges, edges, unit=1.0)),
                    ):
                        rows += result_rows(column, n, epsilon, arm, analyst, rep, bounds[column], answers, truth)
            for column, values in numeric.items():
                edges, ranges, truth, oracle = setup[column]
                for arm, bounds in (("central_a_priori", ADULT_NUMERIC[column]), ("central_oracle", oracle)):
                    answers = answer_central(rng, values, bounds, epsilon, ranges, edges)
                    rows += result_rows(column, n, epsilon, arm, "central", rep, bounds, answers, truth)

            choice = masked(
                [f'{{ name = "{c}", strategy = "random_choice", options = [{options[c]}] }}' for c in categorical],
                rep + 1,
            )
            rr = masked(
                [
                    f'{{ name = "{c}", strategy = "randomized_response", epsilon = {epsilon!r}, '
                    f"categories = [{options[c]}] }}"
                    for c in categorical
                ],
                rep + 1,
            )
            for column, values in categorical.items():
                cats = ADULT_CATEGORICAL[column]
                truth = shares(values, cats)
                tagged = dict(column=column, categories=cats)
                observed = shares(choice[column], cats)
                rows += categorical_rows(n, epsilon, "random_choice", "naive", rep, observed, truth, **tagged)
                observed = shares(rr[column], cats)
                rows += categorical_rows(n, epsilon, "randomized_response", "naive", rep, observed, truth, **tagged)
                estimate = rr_estimator(observed, epsilon)
                rows += categorical_rows(n, epsilon, "randomized_response", "estimator", rep, estimate, truth, **tagged)
                estimate = central_shares(rng, values, epsilon, cats)
                rows += categorical_rows(n, epsilon, "central", "central", rep, estimate, truth, **tagged)
            print(f"adult, ε = {epsilon}, repetition {rep + 1}: done", file=sys.stderr)
    return rows


def result_rows(column, n, epsilon, arm, analyst, rep, bounds, answers, truth):
    out = []
    for query, (error, estimate) in errors(answers, truth).items():
        out.append(
            {
                "column": column,
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
    """Mean error over the repetitions, ± its standard deviation, per column,
    configuration and query: absolute, but signed for a query named `mean`,
    so that bias shows. Rows may come from a CSV, with every field a string;
    CSVs written before the `column` field read as one unnamed column. The
    per-category shares are left out: `max_share` and `tv` sum them up."""
    groups = {}
    queries_of = {}
    for r in rows:
        query = r["query"]
        if query.startswith("share_"):
            continue
        column = r.get("column", "")
        queries = queries_of.setdefault(column, [])
        if query not in queries:
            queries.append(query)
        key = (column, int(r["n"]), float(r["epsilon"]), r["arm"], r["analyst"])
        error = float(r["error"])
        if not math.isnan(error):
            groups.setdefault(key, {}).setdefault(query, []).append(error)
    # One table per set of queries: numeric and categorical columns apart.
    tables = {}
    for column, queries in queries_of.items():
        tables.setdefault(tuple(queries), []).append(column)
    for queries, columns in tables.items():
        header = ["column", "n", "ε", "arm", "analyst"] + [
            "mean (signed)" if q == "mean" else q for q in queries
        ]
        print("\n| " + " | ".join(header) + " |")
        print("|" + "---|" * len(header))
        for (column, n, eps, arm, analyst), by_query in sorted(groups.items()):
            if column not in columns:
                continue
            cells = [column, str(n), f"{eps:g}", arm, analyst]
            for q in queries:
                errors = by_query.get(q, [])
                if q != "mean":
                    errors = [abs(e) for e in errors]
                cells.append(dispersion(errors, signed=q == "mean"))
            print("| " + " | ".join(cells) + " |")


ARM_ORDER = [
    "a_priori", "dp_bounds", "oracle", "central_a_priori", "central_oracle",
    "random_choice", "randomized_response", "central",
]
ANALYST_NAMES = {"naive": "SQL", "estimator": "estimator", "central": "central DP"}
# Queries whose error is a distance, not a relative error.
DISTANCES = {"histogram_tv", "tv", "max_share"}


def print_query(rows, query, column, n, epsilon):
    """One query as a Markdown table: a row per arm and analyst, a column
    per ε (n fixed) or per n (ε fixed); each cell the mean error over the
    repetitions ± its standard deviation, in percent for relative errors."""
    by_cell = {}
    for r in rows:
        if r["query"] != query or (column and r.get("column", "") != column):
            continue
        if n is not None and int(r["n"]) != n:
            continue
        if epsilon is not None and float(r["epsilon"]) != epsilon:
            continue
        across = float(r["epsilon"]) if n is not None else int(r["n"])
        error = float(r["error"])
        if not math.isnan(error):
            by_cell.setdefault((r["arm"], r["analyst"]), {}).setdefault(across, []).append(error)
    if not by_cell:
        sys.exit(f"no rows for query {query}")
    acrosses = sorted({a for cells in by_cell.values() for a in cells})
    label = (lambda a: f"ε = {a:g}") if n is not None else (lambda a: f"n = {a:,}")
    print("| Arm | Analyst | " + " | ".join(label(a) for a in acrosses) + " |")
    print("|---|---|" + "---|" * len(acrosses))
    order = {arm: i for i, arm in enumerate(ARM_ORDER)}
    for (arm, analyst), cells in sorted(by_cell.items(), key=lambda kv: (order.get(kv[0][0], 99), kv[0][1] != "naive", kv[0][1])):
        out = []
        for a in acrosses:
            errors = cells.get(a, [])
            if not errors:
                out.append("n/a")
                continue
            signed = query == "mean"
            values = errors if signed else [abs(e) for e in errors]
            mean = statistics.fmean(values)
            sd = statistics.stdev(values) if len(values) > 1 else 0.0
            if query in DISTANCES:
                out.append(f"{mean:.3f} ± {sd:.3f}")
            elif signed:
                out.append(f"{100 * mean:+.1f}% ± {100 * sd:.1f}%")
            else:
                out.append(f"{100 * mean:.1f}% ± {100 * sd:.1f}%")
        print(f"| `{arm}` | {ANALYST_NAMES.get(analyst, analyst)} | " + " | ".join(out) + " |")


def dispersion(errors, signed=False):
    """`mean ± sd` of the errors of the repetitions (sample standard deviation)."""
    if not errors:
        return "n/a"
    mean = statistics.fmean(errors)
    sd = statistics.stdev(errors) if len(errors) > 1 else 0.0
    return f"{mean:+.4f} ± {sd:.4f}" if signed else f"{mean:.4f} ± {sd:.4f}"


if __name__ == "__main__":
    main()
