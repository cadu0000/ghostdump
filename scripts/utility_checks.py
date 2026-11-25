"""Two checks of the utility evaluation that need more than its CSVs.

* `rr-variance`: the distance of randomized response read through the
  estimator on Adult, against the distance that the variance of k-ary
  randomized response predicts (Wang et al., "Optimizing Locally
  Differentially Private Protocols", arXiv:1705.04421, Section 3, Eq. 2),
  taking each estimated share as normal with that variance.
* `em-iterations`: on one masked run of Adult's `age` (a priori bounds),
  the histogram distance of the EM estimate after a fixed number of
  iterations, from 20 to 5,000.

Examples:

    python3 scripts/utility_checks.py rr-variance --adult-dir adult/ \\
        --csv docs/bench-results/2026-11-adult.csv
    python3 scripts/utility_checks.py em-iterations --adult-dir adult/

Only the Python standard library is used.
"""

import argparse
import csv
import math
import statistics
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import utility_eval as u  # noqa: E402

EPSILONS = (0.1, 0.5, 1.0, 2.0, 5.0, 10.0)


def rr_variance(args):
    """A row per categorical column of Adult, a column per ε: the observed
    distance (mean ± standard deviation over the repetitions) and, in
    brackets, the predicted one."""
    records = u.load_adult(args.adult_dir)
    n = len(records)
    with open(args.csv, newline="") as f:
        rows = [
            r for r in csv.DictReader(f)
            if r["query"] == "tv" and r["arm"] == "randomized_response" and r["analyst"] == "estimator"
        ]
    print("| Column | k | " + " | ".join(f"ε = {e:g}" for e in EPSILONS) + " |")
    print("|---|---|" + "---|" * len(EPSILONS))
    for column, categories in u.ADULT_CATEGORICAL.items():
        k = len(categories)
        shares = u.shares([r[column] for r in records], categories)
        cells = []
        for epsilon in EPSILONS:
            e = math.exp(epsilon)
            p, q = e / (e + k - 1), 1 / (e + k - 1)
            # Eq. 2 is the variance of the estimated count; divided by n²,
            # that of the estimated share.
            sds = [
                math.sqrt((q * (1 - q) / (p - q) ** 2 + f * (1 - p - q) / (p - q)) / n)
                for f in shares
            ]
            predicted = 0.5 * sum(math.sqrt(2 / math.pi) * sd for sd in sds)
            observed = [float(r["error"]) for r in rows if r["column"] == column and float(r["epsilon"]) == epsilon]
            mean = statistics.fmean(observed)
            sd = statistics.stdev(observed)
            cells.append(f"{mean:.4f} ± {sd:.4f} ({predicted:.4f})")
        print(f"| `{column}` | {k} | " + " | ".join(cells) + " |")


def em_iterations(args):
    """A row per ε, a column per number of EM iterations: the histogram
    distance of the estimate of `age`, masked once with the a priori bounds."""
    records = u.load_adult(args.adult_dir)
    lo, hi = u.ADULT_NUMERIC["age"]
    edges = [lo - 0.5 + i * (hi - lo) / 20 for i in range(21)]
    truth = u.histogram(sorted(float(r["age"]) for r in records), edges)
    iterations = (20, 100, 300, 1000, 5000)
    print("| ε | " + " | ".join(f"{k:,} iterations" for k in iterations) + " |")
    print("|---|" + "---|" * len(iterations))
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        dump = workdir / "adult.sql"
        u.write_adult_dump(dump, records)
        for epsilon in (1.0, 5.0):
            rule = u.laplace_rule("age", epsilon, (lo, hi), scale=0)
            masked = u.mask(args.binary, dump, rule, args.seed, workdir, "adult")
            noisy = [float(v) for v in u.read_masked_columns(masked, "adult", u.ADULT_COLUMNS)["age"]]
            cells = []
            for k in iterations:
                # Exactly k iterations: no stopping on a small change.
                u.EM_MAX_ITERATIONS, u.EM_TOLERANCE = k, 0.0
                bins, p = u.deconvolve(noisy, (lo, hi), (hi - lo) / epsilon, unit=1.0)
                estimate = [0.0] * (len(edges) + 1)
                for (_, _, centre, _), share in zip(bins, p):
                    estimate[u.bisect.bisect_right(edges, centre)] += share
                cells.append(f"{u.total_variation(estimate, truth):.4f}")
            print(f"| {epsilon:g} | " + " | ".join(cells) + " |")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=["rr-variance", "em-iterations"])
    parser.add_argument("--adult-dir", required=True, help="directory with adult.data and adult.test")
    parser.add_argument("--csv", help="rr-variance: the CSV of a run of `utility_eval.py --task adult`")
    parser.add_argument(
        "--binary", default=str(u.PROJECT_ROOT / "target" / "release" / "ghostdump")
    )
    parser.add_argument("--seed", type=int, default=1, help="em-iterations: the seed of the masked run")
    args = parser.parse_args()
    if args.check == "rr-variance":
        if not args.csv:
            parser.error("rr-variance needs --csv")
        rr_variance(args)
    else:
        em_iterations(args)


if __name__ == "__main__":
    main()
