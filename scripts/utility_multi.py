"""Utility over several columns of a masked dump (roadmap stage 2.2), on the
UCI Adult dataset.

Every column of the evaluation is masked in one GhostDump run per ε and
repetition: `age` and `hours_per_week` with `dp_laplace` and the a priori
bounds [0, 100], and `sex`, `race`, `workclass`, `education`,
`relationship` and `income` with `randomized_response`, each with the same
ε, so that a record spends 8ε. On the masked dump:

* `joint_tv`: the distance between the true and the estimated shares of the
  cells of a pair of categorical columns (10 to 144 cells; `sex` and
  `relationship` are the strongly dependent pair). The SQL analyst
  reads the shares straight; the estimator inverts the randomized response
  of both columns (post-processing); central DP adds Laplace noise to each
  cell count, with the 2ε that the pair spends in the local model.
* `cramers_v`: Cramér's V of each pair, from the same three tables, against
  its true value (a signed difference).
* `correlation`: the Pearson correlation of `age` and `hours_per_week`. The
  noise, independent of the values, leaves the covariance and adds its
  variance to each column's, which lowers the correlation; the estimator
  subtracts that variance, known from the rules.
* `accuracy` and `auc`: a logistic regression trained on the masked rows of
  `adult.data` (label `income` masked too), tested on the real rows of
  `adult.test`, against the same model trained on the real rows.

The CSV has the columns of `utility_eval.py`, so its `--summarize` prints
the tables:

    python3 scripts/utility_multi.py --adult-dir adult/ --out multi.csv
    python3 scripts/utility_eval.py --summarize multi.csv --query joint_tv \\
        --column sex,race --n 48842

Only the Python standard library is used.
"""

import argparse
import csv
import math
import random
import statistics
import sys
import tempfile
from fractions import Fraction
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import utility_eval as u  # noqa: E402

INCOME = ["<=50K", ">50K"]
# As adult.names lists them; no record has "?".
RELATIONSHIP = ["Wife", "Own-child", "Husband", "Not-in-family", "Other-relative", "Unmarried"]
CATEGORIES = dict(u.ADULT_CATEGORICAL, relationship=RELATIONSHIP, income=INCOME)
# The classifier's inputs: every masked column but the label.
FEATURE_COLUMNS = [c for c in CATEGORIES if c != "income"]
NUMERIC = ["age", "hours_per_week"]
COLUMNS = NUMERIC + list(CATEGORIES)
# Pairs of categorical columns, from 10 to 144 cells.
PAIRS = [
    ("sex", "race"), ("sex", "relationship"), ("sex", "education"),
    ("race", "workclass"), ("workclass", "education"),
]
# Logistic regression: features scaled to [0, 1] by the bounds, plain SGD.
EPOCHS = 10
LEARNING_RATE = 0.1


# ==========================================
# DATA AND MASKING
# ==========================================
def load(directory):
    """The training and test records; the test labels lose their final dot."""
    parts = u.load_adult_files(directory)
    for record in parts["adult.test"]:
        record["income"] = record["income"].rstrip(".")
    return parts["adult.data"], parts["adult.test"]


def write_dump(path, records):
    types = {c: "integer" if c in NUMERIC else "text" for c in COLUMNS}
    with open(path, "w") as f:
        f.write(f"CREATE TABLE adult (id integer, {', '.join(f'{c} {types[c]}' for c in COLUMNS)});\n")
        f.write(f"COPY adult (id, {', '.join(COLUMNS)}) FROM stdin;\n")
        for i, record in enumerate(records, 1):
            f.write("\t".join([str(i)] + [record[c] for c in COLUMNS]) + "\n")
        f.write("\\.\n")


def rules(epsilon):
    out = [u.laplace_rule(c, epsilon, u.ADULT_NUMERIC[c], scale=0) for c in NUMERIC]
    for column, categories in CATEGORIES.items():
        listed = ", ".join(f'"{c}"' for c in categories)
        out.append(
            f'{{ name = "{column}", strategy = "randomized_response", '
            f"epsilon = {epsilon!r}, categories = [{listed}] }}"
        )
    return out


def masked_records(binary, dump, epsilon, seed, workdir):
    out = u.mask(binary, dump, rules(epsilon), seed, workdir, "adult")
    columns = u.read_masked_columns(out, "adult", COLUMNS)
    return [dict(zip(COLUMNS, values)) for values in zip(*(columns[c] for c in COLUMNS))]


# ==========================================
# PAIRS OF CATEGORICAL COLUMNS
# ==========================================
def joint(records, a, b):
    """Shares of the cells of columns a and b, row-major over their categories."""
    index_a = {c: i for i, c in enumerate(CATEGORIES[a])}
    index_b = {c: i for i, c in enumerate(CATEGORIES[b])}
    kb = len(index_b)
    counts = [0] * (len(index_a) * kb)
    for r in records:
        counts[index_a[r[a]] * kb + index_b[r[b]]] += 1
    total = sum(counts) or 1
    return [c / total for c in counts]


def rr_parameters(epsilon, k):
    e = math.exp(epsilon)
    return e / (e + k - 1), 1 / (e + k - 1)


def invert_joint(observed, epsilon, ka, kb):
    """Inverts the randomized response of both columns: with M = (p − q)I + qJ
    and p + (k − 1)q = 1, M⁻¹x = (x − q·Σx)/(p − q), applied along each axis.
    Negative shares are then clipped and the rest renormalized."""
    pa, qa = rr_parameters(epsilon, ka)
    pb, qb = rr_parameters(epsilon, kb)
    rows = [observed[i * kb:(i + 1) * kb] for i in range(ka)]
    column_sums = [sum(rows[i][j] for i in range(ka)) for j in range(kb)]
    rows = [[(rows[i][j] - qa * column_sums[j]) / (pa - qa) for j in range(kb)] for i in range(ka)]
    rows = [[(row[j] - qb * sum(row)) / (pb - qb) for j in range(kb)] for row in rows]
    return u.project([x for row in rows for x in row])


def central_joint(rng, truth, n, epsilon):
    """Laplace noise on each cell count, with the 2ε that the pair spends
    locally; replacing one record changes two counts by one (sensitivity 2)."""
    return u.project([s * n + u.laplace(rng, 2.0 / (2 * epsilon)) for s in truth])


def cramers_v(shares, ka, kb):
    rows = [sum(shares[i * kb:(i + 1) * kb]) for i in range(ka)]
    cols = [sum(shares[i * kb + j] for i in range(ka)) for j in range(kb)]
    phi2 = 0.0
    for i in range(ka):
        for j in range(kb):
            expected = rows[i] * cols[j]
            if expected > 0:
                phi2 += (shares[i * kb + j] - expected) ** 2 / expected
    return math.sqrt(phi2 / (min(ka, kb) - 1))


# ==========================================
# CORRELATION OF THE NUMERIC COLUMNS
# ==========================================
def noise_variance(epsilon, bounds):
    """Variance of GhostDump's noise on a whole-number column: discrete
    Laplace with P(z) ∝ exp(−|z|/t), t = ⌈Δ/ε⌉ computed exactly as the
    sampler does, so 2q/(1 − q)² with q = e^(−1/t)."""
    t = math.ceil(Fraction(bounds[1] - bounds[0]) / Fraction(epsilon))
    q = math.exp(-1 / t)
    return 2 * q / (1 - q) ** 2


def correlation(xs, ys, subtract=(0.0, 0.0)):
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    cov = math.fsum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (len(xs) - 1)
    vx = statistics.variance(xs) - subtract[0]
    vy = statistics.variance(ys) - subtract[1]
    if vx <= 0 or vy <= 0:
        return float("nan")
    return max(-1.0, min(1.0, cov / math.sqrt(vx * vy)))


# ==========================================
# CLASSIFICATION
# ==========================================
def feature_index():
    index, position = {}, 3  # 0: bias, 1: age, 2: hours_per_week
    for column in FEATURE_COLUMNS:
        for category in CATEGORIES[column]:
            index[(column, category)] = position
            position += 1
    return index, position


FEATURES, WIDTH = feature_index()


def features(record):
    """The non-zero features of a record, as (index, value)."""
    out = [(0, 1.0)]
    for i, column in enumerate(NUMERIC, 1):
        lo, hi = u.ADULT_NUMERIC[column]
        out.append((i, (float(record[column]) - lo) / (hi - lo)))
    out += [(FEATURES[(c, record[c])], 1.0) for c in FEATURE_COLUMNS]
    return out


def sigmoid(z):
    return 1 / (1 + math.exp(-z)) if z >= 0 else math.exp(z) / (1 + math.exp(z))


def train(records, seed):
    rng = random.Random(seed)
    data = [(features(r), 1.0 if r["income"] == ">50K" else 0.0) for r in records]
    w = [0.0] * WIDTH
    for epoch in range(EPOCHS):
        rng.shuffle(data)
        rate = LEARNING_RATE / (1 + epoch)
        for x, y in data:
            gradient = sigmoid(sum(w[i] * v for i, v in x)) - y
            for i, v in x:
                w[i] -= rate * gradient * v
    return w


def evaluate(w, records):
    """Accuracy at 0.5 and the area under the ROC curve (by ranks, ties
    sharing their average rank)."""
    scored = [(sum(w[i] * v for i, v in features(r)), r["income"] == ">50K") for r in records]
    accuracy = sum((s >= 0) == positive for s, positive in scored) / len(scored)
    scored.sort(key=lambda sp: sp[0])
    rank_sum, i = 0.0, 0
    while i < len(scored):
        j = i
        while j < len(scored) and scored[j][0] == scored[i][0]:
            j += 1
        rank_sum += sum(1 for _, positive in scored[i:j] if positive) * (i + j + 1) / 2
        i = j
    positives = sum(1 for _, positive in scored if positive)
    negatives = len(scored) - positives
    auc = (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)
    return accuracy, auc


# ==========================================
# RUN
# ==========================================
def row(column, n, epsilon, arm, analyst, rep, query, estimate, truth):
    return {
        "column": column, "n": n, "epsilon": epsilon, "arm": arm, "analyst": analyst,
        "rep": rep, "lo": "", "hi": "", "query": query,
        "estimate": round(estimate, 6), "truth": round(truth, 6), "error": round(estimate - truth, 6),
    }


def run(args, workdir):
    train_records, test_records = load(args.adult_dir)
    records = train_records + test_records
    n = len(records)
    dump = workdir / "adult.sql"
    write_dump(dump, records)

    true_joint = {(a, b): joint(records, a, b) for a, b in PAIRS}
    true_v = {
        (a, b): cramers_v(true_joint[(a, b)], len(CATEGORIES[a]), len(CATEGORIES[b])) for a, b in PAIRS
    }
    ages = [float(r["age"]) for r in records]
    hours = [float(r["hours_per_week"]) for r in records]
    true_r = correlation(ages, hours)
    real_accuracy, real_auc = evaluate(train(train_records, "real"), test_records)
    majority = sum(1 for r in test_records if r["income"] == "<=50K") / len(test_records)
    print(
        f"true correlation {true_r:.4f}; trained on the real rows: accuracy {real_accuracy:.4f}, "
        f"AUC {real_auc:.4f}; majority class {majority:.4f}",
        file=sys.stderr,
    )

    rows = []
    for epsilon in (float(e) for e in args.epsilons.split(",")):
        variances = [noise_variance(epsilon, u.ADULT_NUMERIC[c]) for c in NUMERIC]
        for rep in range(args.reps):
            rng = random.Random(f"multi-{epsilon}-{rep}")
            masked = masked_records(args.binary, dump, epsilon, rep + 1, workdir)
            for a, b in PAIRS:
                ka, kb = len(CATEGORIES[a]), len(CATEGORIES[b])
                name, truth = f"{a},{b}", true_joint[(a, b)]
                observed = joint(masked, a, b)
                for arm, analyst, estimate in (
                    ("local", "naive", observed),
                    ("local", "estimator", invert_joint(observed, epsilon, ka, kb)),
                    ("central", "central", central_joint(rng, truth, n, epsilon)),
                ):
                    rows.append(row(name, n, epsilon, arm, analyst, rep, "joint_tv", u.total_variation(estimate, truth), 0.0))
                    rows.append(row(name, n, epsilon, arm, analyst, rep, "cramers_v", cramers_v(estimate, ka, kb), true_v[(a, b)]))
            noisy_ages = [float(r["age"]) for r in masked]
            noisy_hours = [float(r["hours_per_week"]) for r in masked]
            for analyst, estimate in (
                ("naive", correlation(noisy_ages, noisy_hours)),
                ("estimator", correlation(noisy_ages, noisy_hours, variances)),
            ):
                rows.append(row("age,hours_per_week", n, epsilon, "local", analyst, rep, "correlation", estimate, true_r))
            model = train(masked[: len(train_records)], f"{epsilon}-{rep}")
            accuracy, auc = evaluate(model, test_records)
            rows.append(row("income", len(train_records), epsilon, "local", "naive", rep, "accuracy", accuracy, real_accuracy))
            rows.append(row("income", len(train_records), epsilon, "local", "naive", rep, "auc", auc, real_auc))
            print(f"ε = {epsilon}, repetition {rep + 1}: done", file=sys.stderr)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--adult-dir", required=True, help="directory with adult.data and adult.test")
    parser.add_argument("--epsilons", default="0.1,0.5,1,2,5,10")
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--binary", default=str(u.PROJECT_ROOT / "target" / "release" / "ghostdump"))
    parser.add_argument("--out", default="multi_results.csv")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        rows = run(args, Path(tmp))
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} rows written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
