# Utility evaluation

**Question.** How much statistical utility does a masked dump keep, and where does the loss come from? The evaluation separates the two costs of RQ-δ:

* **δ2, the local model:** local DP against central DP at the same ε. Any mechanism that sees one row at a time pays it.
* **δ3, the state restriction:** given local DP, bounds chosen without the data against bounds informed by the data.

This is the second version (roadmap stage 2.1). Against the first, it adds ε ∈ {0.1, 0.5, 1, 2, 5, 10}, the dispersion over the repetitions in every table, a real dataset (UCI Adult), the error of each column, and randomized response with 2, 5, 9 and 16 categories.

**Run it:**

```bash
cargo build --release
python3 scripts/utility_eval.py --task numeric --sizes 10000,100000,1000000 \
    --epsilons 0.1,0.5,1,2,5,10 --reps 10 --out utility.csv
python3 scripts/utility_eval.py --task categorical --sizes 10000,100000,1000000 \
    --epsilons 0.1,0.5,1,2,5,10 --reps 10 --out categorical.csv
python3 scripts/utility_eval.py --task adult --adult-dir adult/ \
    --epsilons 0.1,0.5,1,2,5,10 --reps 10 --out adult.csv
```

The harness is `scripts/utility_eval.py` (standard library only). Every masked dump goes through the release binary. On the reference machine of [benchmarks.md](../benchmarks.md), the three runs took 46, 1 and 46 minutes, in parallel. The raw results, one row per query and repetition, are in [bench-results](../bench-results/): `2026-11-utility-v2.csv` (12,960 rows), `2026-11-categorical-v2.csv` (3,600 rows) and `2026-11-adult.csv` (18,240 rows).

Every table below is printed by one command, which reads a CSV:

```bash
python3 scripts/utility_eval.py --summarize docs/bench-results/2026-11-utility-v2.csv \
    --query mean --epsilon 1                       # one column per n
python3 scripts/utility_eval.py --summarize docs/bench-results/2026-11-adult.csv \
    --query tv --column education --n 48842        # one column per ε
python3 scripts/utility_eval.py --summarize docs/bench-results/2026-11-adult.csv   # everything
```

A cell is the mean error over the 10 repetitions ± its standard deviation. The first version's CSVs (`2026-11-utility.csv`, `2026-11-categorical.csv`) stay in the same directory, and `--summarize` gives their dispersion too. They ran with earlier builds of GhostDump, which drew other noise for the same seed, so their single numbers differ from these.

## Setup

**Synthetic data.** One `salary` column with the generator's distribution: lognormal with a median of about 4,000, mean 4,566, 5% NULL; and one `status` column, `ACTIVE` 70%, `PENDING` 20%, `INACTIVE` 10%. n ∈ {10⁴, 10⁵, 10⁶} rows, seed 42; the datasets are nested (the first 10⁵ rows of the 10⁶ dataset are the 10⁵ dataset).

**Adult.** The 48,842 records of `adult.data` and `adult.test` (Becker and Kohavi, 1996), from the 1994 US census, licensed CC BY 4.0. Download `https://archive.ics.uci.edu/static/public/2/adult.zip` and unzip it into the directory given to `--adult-dir`; the harness checks the SHA-256 of both files (`adult.data`: `5b0026…603d`, `adult.test`: `a2a904…7e9c05`, in full in the script) and stops if they differ. Each column is masked and measured on its own:

| Column | Kind | Strategy | Values in the data |
|---|---|---|---|
| `age` | whole number | `dp_laplace`, `scale = 0` | 17–90 |
| `hours_per_week` | whole number | `dp_laplace`, `scale = 0` | 1–99; 40 holds 47% of the records |
| `sex` | 2 categories | `randomized_response` | |
| `race` | 5 categories | `randomized_response` | |
| `workclass` | 9 categories | `randomized_response` | the 8 that `adult.names` lists, and `?`, its mark for an unknown value (2,799 records) |
| `education` | 16 categories | `randomized_response` | |

The categories are those that `adult.names` lists, not those read from the data.

**Mechanisms.** `dp_laplace` with the exact sampler (`scale = 2` for `salary`, `0` for Adult), and `randomized_response`, which keeps the true category with probability e^ε/(e^ε + k − 1). 10 repetitions per point, with different noise each time.

**Local arms for numbers** (same binary, different bounds in the rules):

| Arm | Bounds | Private? | Passes over the data |
|---|---|---|---|
| `a_priori` | chosen from what the column means: [0, 10000] for `salary`, which clamps its top 3.4%; [0, 100] for an age and for hours in a working week | yes | 1 |
| `dp_bounds` | 1% and 99% quantiles, estimated with central DP (exponential mechanism on a grid: steps of 100 over [0, 100000] for `salary`, whole units over [0, 100] for Adult) with 10% of ε; the values get the other 90% | yes (ε in total) | 2 |
| `oracle` | the true minimum and maximum | **no**: used only as a reference | 2 |

`dp_bounds` keeps memory constant (a fixed histogram) but needs two passes, so it does not work with `zcat dump.gz | ghostdump`.

**Categorical arms.** `random_choice` replaces each value with a uniform category (0-LDP); `randomized_response` (ε-LDP).

**Analysts** on each masked dump:

* **SQL**: what a developer gets by querying the dump directly (`AVG`, `COUNT ... WHERE`, `percentile_cont`, `width_bucket`, shares with `GROUP BY`).
* **estimator**: post-processing, which spends no privacy. For numbers, EM deconvolution of the Laplace noise on a histogram of the clamped values: 50 bins of the bounds for `salary`; for whole numbers, one bin per value the bounds allow and observation cells of whole units, because the noise is a whole number too. For categories, the unbiased correction (observed − q)/(p − q), with negative shares clipped to 0 and the rest renormalized.

**Central comparator.** Each query answered from the raw data with ε-DP under replacement of one value: Laplace on sums and counts (scale 2/ε on each category count), the exponential mechanism for quantiles. It is generous to the central model: every query gets the full ε, where a real release would split it. `central_a_priori` uses the a priori bounds, `central_oracle` the true range.

**Queries.** For numbers: mean, sum, count, counts in ranges centred on the median holding 50%, 10% and 2% of the values, median, 90th percentile, and a histogram of 20 bins (of 1,000 for `salary`, of 5 for Adult). The error is the relative error, signed for the mean (so that bias shows) and absolute elsewhere; the histogram uses the total variation distance. On whole numbers the quantiles around the median often coincide (40 hours is both the 45% and the 55% quantile), so a range there includes both ends and holds at least the share in its name. For categories: the total variation distance between the estimated and the true shares (`tv`), and the largest error on one share (`max_share`).

## Synthetic numeric column (`salary`)

**Mean: signed relative error (bias plus noise), ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | -1.0% ± 2.5% | -1.4% ± 0.8% | -1.8% ± 0.3% |
| `a_priori` | estimator | -1.7% ± 1.7% | -1.5% ± 0.5% | -1.9% ± 0.2% |
| `dp_bounds` | SQL | -7.7% ± 15.4% | +0.0% ± 1.0% | -0.4% ± 0.3% |
| `dp_bounds` | estimator | +2.8% ± 10.5% | +0.2% ± 1.0% | -0.6% ± 0.4% |
| `oracle` | SQL | -0.1% ± 3.7% | +0.5% ± 3.3% | -0.1% ± 1.1% |
| `oracle` | estimator | +0.1% ± 4.0% | +0.3% ± 3.8% | -0.3% ± 0.7% |
| `central_a_priori` | central DP | -1.7% ± 0.0% | -1.7% ± 0.0% | -1.8% ± 0.0% |
| `central_oracle` | central DP | -0.0% ± 0.1% | +0.0% ± 0.0% | -0.0% ± 0.0% |

**Count in a range holding 10% of the values, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 77.6% ± 1.0% | 78.7% ± 0.3% | 78.4% ± 0.1% |
| `a_priori` | estimator | 28.1% ± 33.5% | 13.9% ± 11.8% | 10.6% ± 5.8% |
| `dp_bounds` | SQL | 94.1% ± 5.0% | 82.6% ± 0.5% | 82.6% ± 0.1% |
| `dp_bounds` | estimator | 41.2% ± 14.2% | 11.0% ± 8.8% | 4.5% ± 2.3% |
| `oracle` | SQL | 89.1% ± 1.0% | 94.4% ± 0.3% | 94.4% ± 0.1% |
| `oracle` | estimator | 22.0% ± 18.4% | 36.6% ± 7.9% | 38.3% ± 2.6% |
| `central_a_priori` | central DP | 0.1% ± 0.1% | 0.0% ± 0.0% | 0.0% ± 0.0% |
| `central_oracle` | central DP | 0.2% ± 0.2% | 0.0% ± 0.0% | 0.0% ± 0.0% |

**90th percentile, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 173.2% ± 4.0% | 172.8% ± 1.0% | 172.6% ± 0.4% |
| `a_priori` | estimator | 11.6% ± 9.9% | 5.2% ± 5.0% | 3.1% ± 1.8% |
| `dp_bounds` | SQL | 1228.8% ± 689.4% | 230.8% ± 2.6% | 232.0% ± 0.7% |
| `dp_bounds` | estimator | 21.9% ± 20.6% | 3.8% ± 2.1% | 1.0% ± 0.8% |
| `oracle` | SQL | 441.9% ± 7.4% | 863.7% ± 6.3% | 863.0% ± 2.2% |
| `oracle` | estimator | 10.5% ± 7.8% | 13.8% ± 6.4% | 15.2% ± 1.7% |
| `central_a_priori` | central DP | 0.1% ± 0.1% | 0.0% ± 0.0% | 0.0% ± 0.0% |
| `central_oracle` | central DP | 0.0% ± 0.0% | 0.2% ± 0.0% | 0.2% ± 0.0% |

**Histogram: total variation distance, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 0.6338 ± 0.0061 | 0.6330 ± 0.0014 | 0.6326 ± 0.0004 |
| `a_priori` | estimator | 0.2777 ± 0.0816 | 0.1330 ± 0.0415 | 0.0952 ± 0.0126 |
| `dp_bounds` | SQL | 0.8691 ± 0.0770 | 0.6849 ± 0.0020 | 0.6865 ± 0.0006 |
| `dp_bounds` | estimator | 0.2926 ± 0.1043 | 0.0906 ± 0.0401 | 0.0591 ± 0.0107 |
| `oracle` | SQL | 0.7887 ± 0.0040 | 0.8677 ± 0.0014 | 0.8669 ± 0.0004 |
| `oracle` | estimator | 0.2019 ± 0.1327 | 0.2181 ± 0.0454 | 0.2232 ± 0.0135 |
| `central_a_priori` | central DP | 0.0227 ± 0.0005 | 0.0214 ± 0.0000 | 0.0221 ± 0.0000 |
| `central_oracle` | central DP | 0.0022 ± 0.0005 | 0.0002 ± 0.0001 | 0.0000 ± 0.0000 |

**Varying ε at n = 1,000,000.** Mean:

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | -1.7% ± 3.4% | -1.7% ± 0.8% | -1.8% ± 0.3% | -1.8% ± 0.2% | -1.7% ± 0.1% | -1.8% ± 0.0% |
| `a_priori` | estimator | -1.5% ± 2.8% | -2.1% ± 0.5% | -1.9% ± 0.2% | -1.8% ± 0.2% | -1.8% ± 0.1% | -1.8% ± 0.0% |
| `dp_bounds` | SQL | +1.7% ± 3.1% | -0.5% ± 0.7% | -0.4% ± 0.3% | -0.4% ± 0.2% | -0.5% ± 0.1% | -0.5% ± 0.0% |
| `dp_bounds` | estimator | +3.1% ± 1.6% | -0.8% ± 0.5% | -0.6% ± 0.4% | -0.4% ± 0.1% | -0.5% ± 0.1% | -0.5% ± 0.0% |
| `oracle` | SQL | +7.3% ± 11.7% | +1.6% ± 2.1% | -0.1% ± 1.1% | +0.3% ± 0.6% | +0.1% ± 0.3% | -0.0% ± 0.1% |
| `oracle` | estimator | +50.6% ± 4.9% | +0.6% ± 2.2% | -0.3% ± 0.7% | -0.2% ± 0.5% | +0.0% ± 0.2% | -0.0% ± 0.1% |
| `central_a_priori` | central DP | -1.8% ± 0.0% | -1.8% ± 0.0% | -1.8% ± 0.0% | -1.8% ± 0.0% | -1.8% ± 0.0% | -1.8% ± 0.0% |
| `central_oracle` | central DP | -0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% | +0.0% ± 0.0% | +0.0% ± 0.0% | +0.0% ± 0.0% |

Count in a range holding 10% of the values:

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 97.5% ± 0.1% | 88.4% ± 0.1% | 78.4% ± 0.1% | 62.6% ± 0.1% | 34.0% ± 0.2% | 14.5% ± 0.3% |
| `a_priori` | estimator | 46.8% ± 0.6% | 10.9% ± 3.9% | 10.6% ± 5.8% | 5.4% ± 3.4% | 6.8% ± 4.3% | 4.0% ± 3.5% |
| `dp_bounds` | SQL | 98.1% ± 0.0% | 90.8% ± 0.1% | 82.6% ± 0.1% | 68.9% ± 0.1% | 42.2% ± 0.2% | 20.5% ± 0.3% |
| `dp_bounds` | estimator | 34.1% ± 1.4% | 19.8% ± 2.6% | 4.5% ± 2.3% | 6.5% ± 4.0% | 7.5% ± 4.9% | 5.4% ± 3.1% |
| `oracle` | SQL | 99.4% ± 0.0% | 97.1% ± 0.0% | 94.4% ± 0.1% | 89.1% ± 0.1% | 75.6% ± 0.1% | 58.3% ± 0.2% |
| `oracle` | estimator | 57.2% ± 0.6% | 50.0% ± 1.2% | 38.3% ± 2.6% | 13.0% ± 3.3% | 5.3% ± 3.5% | 6.1% ± 3.4% |
| `central_a_priori` | central DP | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% |
| `central_oracle` | central DP | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% |

90th percentile:

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 2063.7% ± 3.8% | 381.7% ± 0.9% | 172.6% ± 0.4% | 70.7% ± 0.2% | 18.3% ± 0.1% | 5.6% ± 0.0% |
| `a_priori` | estimator | 13.2% ± 1.3% | 2.4% ± 1.1% | 3.1% ± 1.8% | 1.5% ± 0.7% | 0.8% ± 0.4% | 0.4% ± 0.3% |
| `dp_bounds` | SQL | 2668.4% ± 47.8% | 499.8% ± 2.4% | 232.0% ± 0.7% | 100.4% ± 0.2% | 29.1% ± 0.1% | 9.5% ± 0.1% |
| `dp_bounds` | estimator | 19.1% ± 2.3% | 4.6% ± 1.9% | 1.0% ± 0.8% | 0.8% ± 0.6% | 0.6% ± 0.4% | 0.3% ± 0.3% |
| `oracle` | SQL | 8979.6% ± 17.5% | 1765.6% ± 2.6% | 863.0% ± 2.2% | 413.0% ± 0.6% | 145.5% ± 0.3% | 60.6% ± 0.1% |
| `oracle` | estimator | 101.5% ± 6.8% | 29.4% ± 3.3% | 15.2% ± 1.7% | 1.8% ± 1.7% | 0.9% ± 0.8% | 1.0% ± 1.0% |
| `central_a_priori` | central DP | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% |
| `central_oracle` | central DP | 0.2% ± 0.0% | 0.2% ± 0.0% | 0.2% ± 0.0% | 0.2% ± 0.0% | 0.2% ± 0.0% | 0.2% ± 0.0% |

Histogram distance:

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 0.9301 ± 0.0002 | 0.7673 ± 0.0004 | 0.6326 ± 0.0004 | 0.4579 ± 0.0005 | 0.2260 ± 0.0003 | 0.1149 ± 0.0004 |
| `a_priori` | estimator | 0.3177 ± 0.0052 | 0.1441 ± 0.0095 | 0.0952 ± 0.0126 | 0.0729 ± 0.0098 | 0.0578 ± 0.0083 | 0.0467 ± 0.0039 |
| `dp_bounds` | SQL | 0.9429 ± 0.0009 | 0.8056 ± 0.0005 | 0.6865 ± 0.0006 | 0.5233 ± 0.0004 | 0.2798 ± 0.0005 | 0.1424 ± 0.0005 |
| `dp_bounds` | estimator | 0.1782 ± 0.0123 | 0.1010 ± 0.0102 | 0.0591 ± 0.0107 | 0.0476 ± 0.0098 | 0.0383 ± 0.0067 | 0.0274 ± 0.0050 |
| `oracle` | SQL | 0.9787 ± 0.0001 | 0.9215 ± 0.0002 | 0.8669 ± 0.0004 | 0.7791 ± 0.0005 | 0.5985 ± 0.0004 | 0.4183 ± 0.0005 |
| `oracle` | estimator | 0.3713 ± 0.0061 | 0.3048 ± 0.0109 | 0.2232 ± 0.0135 | 0.1166 ± 0.0091 | 0.0525 ± 0.0105 | 0.0361 ± 0.0066 |
| `central_a_priori` | central DP | 0.0221 ± 0.0000 | 0.0221 ± 0.0000 | 0.0221 ± 0.0000 | 0.0221 ± 0.0000 | 0.0221 ± 0.0000 | 0.0221 ± 0.0000 |
| `central_oracle` | central DP | 0.0002 ± 0.0001 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 |

## Adult: numeric columns

n = 48,842 in every table. The a priori bounds [0, 100] hold every value of both columns, so no arm clamps, and the mean has no bias floor.

**`age`, mean: signed relative error**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | +2.3% ± 17.9% | +1.0% ± 3.3% | +0.7% ± 1.9% | +0.3% ± 0.9% | -0.0% ± 0.4% | +0.0% ± 0.2% |
| `a_priori` | estimator | +2.0% ± 10.7% | +0.1% ± 2.7% | +0.1% ± 1.1% | +0.2% ± 0.7% | -0.1% ± 0.4% | -0.0% ± 0.2% |
| `dp_bounds` | SQL | +1.3% ± 13.9% | +0.8% ± 2.0% | -0.1% ± 1.3% | +0.3% ± 0.4% | -0.1% ± 0.2% | -0.0% ± 0.1% |
| `dp_bounds` | estimator | +2.0% ± 8.2% | +0.5% ± 1.3% | +0.3% ± 0.7% | +0.3% ± 0.3% | +0.1% ± 0.2% | +0.1% ± 0.1% |
| `oracle` | SQL | +7.9% ± 18.0% | +0.3% ± 2.6% | +0.1% ± 1.2% | +0.2% ± 0.5% | +0.0% ± 0.2% | -0.0% ± 0.2% |
| `oracle` | estimator | +4.5% ± 8.3% | +0.6% ± 2.1% | -0.0% ± 0.8% | +0.4% ± 0.7% | +0.1% ± 0.2% | +0.1% ± 0.2% |
| `central_a_priori` | central DP | +0.0% ± 0.0% | +0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% |
| `central_oracle` | central DP | +0.0% ± 0.1% | -0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% | -0.0% ± 0.0% | +0.0% ± 0.0% |

**`age`, 90th percentile**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 2735.5% ± 19.8% | 523.1% ± 4.5% | 245.9% ± 3.0% | 108.6% ± 1.1% | 31.0% ± 0.0% | 10.3% ± 0.0% |
| `a_priori` | estimator | 39.8% ± 8.1% | 6.9% ± 5.7% | 4.1% ± 2.4% | 3.4% ± 2.4% | 2.6% ± 1.5% | 1.6% ± 1.2% |
| `dp_bounds` | SQL | 2083.6% ± 239.2% | 326.2% ± 25.2% | 144.5% ± 1.8% | 60.9% ± 0.8% | 15.5% ± 0.0% | 5.2% ± 0.0% |
| `dp_bounds` | estimator | 16.6% ± 8.8% | 5.3% ± 3.1% | 4.5% ± 3.4% | 2.8% ± 1.8% | 1.1% ± 0.9% | 1.2% ± 1.0% |
| `oracle` | SQL | 1994.4% ± 21.3% | 373.1% ± 2.3% | 171.7% ± 1.5% | 74.5% ± 1.1% | 20.3% ± 0.7% | 6.9% ± 0.0% |
| `oracle` | estimator | 19.3% ± 9.4% | 4.9% ± 2.3% | 4.7% ± 3.1% | 3.0% ± 1.4% | 2.5% ± 1.3% | 1.4% ± 0.8% |
| `central_a_priori` | central DP | 0.7% ± 0.5% | 0.7% ± 0.5% | 0.9% ± 0.5% | 0.6% ± 0.4% | 0.7% ± 0.4% | 1.0% ± 0.6% |
| `central_oracle` | central DP | 0.8% ± 0.5% | 0.6% ± 0.5% | 0.9% ± 0.5% | 1.0% ± 0.6% | 0.7% ± 0.5% | 0.8% ± 0.4% |

**`age`, histogram distance**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 0.9641 ± 0.0007 | 0.8535 ± 0.0015 | 0.7466 ± 0.0013 | 0.5863 ± 0.0019 | 0.3202 ± 0.0020 | 0.1558 ± 0.0013 |
| `a_priori` | estimator | 0.4347 ± 0.0105 | 0.1466 ± 0.0533 | 0.1259 ± 0.0273 | 0.1434 ± 0.0450 | 0.1605 ± 0.0416 | 0.1313 ± 0.0360 |
| `dp_bounds` | SQL | 0.9546 ± 0.0046 | 0.7909 ± 0.0094 | 0.6454 ± 0.0029 | 0.4572 ± 0.0026 | 0.2108 ± 0.0025 | 0.0965 ± 0.0019 |
| `dp_bounds` | estimator | 0.2375 ± 0.0806 | 0.1101 ± 0.0364 | 0.1346 ± 0.0456 | 0.1760 ± 0.0417 | 0.1336 ± 0.0493 | 0.1005 ± 0.0205 |
| `oracle` | SQL | 0.9529 ± 0.0007 | 0.8111 ± 0.0014 | 0.6788 ± 0.0023 | 0.5010 ± 0.0024 | 0.2454 ± 0.0018 | 0.1175 ± 0.0020 |
| `oracle` | estimator | 0.1823 ± 0.0124 | 0.0895 ± 0.0316 | 0.1256 ± 0.0482 | 0.1424 ± 0.0291 | 0.1434 ± 0.0294 | 0.0861 ± 0.0241 |
| `central_a_priori` | central DP | 0.0036 ± 0.0011 | 0.0009 ± 0.0004 | 0.0005 ± 0.0001 | 0.0002 ± 0.0000 | 0.0001 ± 0.0000 | 0.0000 ± 0.0000 |
| `central_oracle` | central DP | 0.0043 ± 0.0010 | 0.0007 ± 0.0001 | 0.0004 ± 0.0001 | 0.0002 ± 0.0001 | 0.0001 ± 0.0000 | 0.0000 ± 0.0000 |

**`hours_per_week`, count in the range around the median.** On this column the range is the single value 40, which holds 47% of the records.

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 99.9% ± 0.0% | 99.5% ± 0.1% | 99.0% ± 0.1% | 98.1% ± 0.1% | 96.0% ± 0.1% | 93.0% ± 0.2% |
| `a_priori` | estimator | 97.7% ± 0.1% | 94.5% ± 0.4% | 91.7% ± 1.1% | 88.5% ± 2.9% | 80.2% ± 5.2% | 43.6% ± 10.9% |
| `dp_bounds` | SQL | 99.9% ± 0.0% | 99.4% ± 0.1% | 98.8% ± 0.1% | 97.7% ± 0.1% | 95.2% ± 0.1% | 91.7% ± 0.1% |
| `dp_bounds` | estimator | 97.1% ± 0.3% | 94.7% ± 0.8% | 89.8% ± 2.5% | 86.5% ± 4.7% | 68.7% ± 6.3% | 30.0% ± 6.6% |
| `oracle` | SQL | 99.9% ± 0.0% | 99.4% ± 0.1% | 99.0% ± 0.1% | 98.1% ± 0.1% | 95.9% ± 0.2% | 93.0% ± 0.2% |
| `oracle` | estimator | 97.7% ± 0.1% | 94.4% ± 0.7% | 91.3% ± 1.2% | 88.9% ± 2.9% | 76.5% ± 4.0% | 45.9% ± 10.1% |
| `central_a_priori` | central DP | 0.1% ± 0.1% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% |
| `central_oracle` | central DP | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% | 0.0% ± 0.0% |

**`hours_per_week`, histogram distance**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `a_priori` | SQL | 0.9542 ± 0.0011 | 0.8265 ± 0.0014 | 0.7217 ± 0.0027 | 0.5928 ± 0.0020 | 0.4215 ± 0.0021 | 0.3535 ± 0.0011 |
| `a_priori` | estimator | 0.5220 ± 0.0129 | 0.3721 ± 0.0105 | 0.3596 ± 0.0151 | 0.3218 ± 0.0349 | 0.3038 ± 0.0347 | 0.1565 ± 0.0504 |
| `dp_bounds` | SQL | 0.9459 ± 0.0058 | 0.8020 ± 0.0125 | 0.6779 ± 0.0038 | 0.5424 ± 0.0050 | 0.3983 ± 0.0016 | 0.3365 ± 0.0019 |
| `dp_bounds` | estimator | 0.4714 ± 0.0239 | 0.3875 ± 0.0209 | 0.3460 ± 0.0240 | 0.3373 ± 0.0590 | 0.2634 ± 0.0390 | 0.1433 ± 0.0323 |
| `oracle` | SQL | 0.9537 ± 0.0008 | 0.8235 ± 0.0013 | 0.7173 ± 0.0018 | 0.5878 ± 0.0018 | 0.4218 ± 0.0017 | 0.3539 ± 0.0018 |
| `oracle` | estimator | 0.5245 ± 0.0170 | 0.3756 ± 0.0168 | 0.3627 ± 0.0262 | 0.3252 ± 0.0355 | 0.2987 ± 0.0514 | 0.1806 ± 0.0503 |
| `central_a_priori` | central DP | 0.0041 ± 0.0006 | 0.0008 ± 0.0003 | 0.0005 ± 0.0001 | 0.0002 ± 0.0000 | 0.0001 ± 0.0000 | 0.0000 ± 0.0000 |
| `central_oracle` | central DP | 0.0044 ± 0.0010 | 0.0009 ± 0.0002 | 0.0005 ± 0.0001 | 0.0002 ± 0.0000 | 0.0001 ± 0.0000 | 0.0000 ± 0.0000 |

**The EM estimate of `age` overfits.** From ε = 0.5 on, the estimator's histogram distance for `age` stays between 0.09 and 0.18 instead of falling with ε. Stopping EM after a fixed number of iterations, on one masked run, shows why: the estimate improves, then gets worse as EM fits the noise, and the stopping rule (a change below 10⁻⁵, or 5,000 iterations), which suited the smooth `salary`, runs past the best point.

```bash
python3 scripts/utility_checks.py em-iterations --adult-dir adult/
```

| ε | 20 iterations | 100 iterations | 300 iterations | 1,000 iterations | 5,000 iterations |
|---|---|---|---|---|---|
| 1 | 0.4161 | 0.3298 | 0.2048 | 0.0885 | 0.1702 |
| 5 | 0.0825 | 0.0655 | 0.0613 | 0.0964 | 0.1997 |

The best number of iterations differs with ε and is found here with the truth, so it cannot be used as the estimator's rule; the tables above keep the rule of the first version.

## Categorical columns

**Synthetic `status`, total variation distance, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `random_choice` | SQL | 0.3707 ± 0.0030 | 0.3676 ± 0.0012 | 0.3668 ± 0.0004 |
| `randomized_response` | SQL | 0.2359 ± 0.0046 | 0.2334 ± 0.0009 | 0.2335 ± 0.0004 |
| `randomized_response` | estimator | 0.0175 ± 0.0088 | 0.0043 ± 0.0023 | 0.0016 ± 0.0011 |
| `central` | central DP | 0.0002 ± 0.0001 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 |

**Synthetic `status`, total variation distance, n = 1,000,000**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `random_choice` | SQL | 0.3668 ± 0.0004 | 0.3668 ± 0.0004 | 0.3668 ± 0.0004 | 0.3668 ± 0.0004 | 0.3668 ± 0.0004 | 0.3668 ± 0.0004 |
| `randomized_response` | SQL | 0.3546 ± 0.0005 | 0.3016 ± 0.0005 | 0.2335 ± 0.0004 | 0.1173 ± 0.0004 | 0.0073 ± 0.0001 | 0.0000 ± 0.0000 |
| `randomized_response` | estimator | 0.0169 ± 0.0091 | 0.0029 ± 0.0013 | 0.0016 ± 0.0011 | 0.0006 ± 0.0003 | 0.0001 ± 0.0001 | 0.0000 ± 0.0000 |
| `central` | central DP | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 |

**Adult, randomized response read through the estimator: total variation distance by number of categories, with the distance its variance predicts in brackets** (n = 48,842; the other arms of each column are in the CSV)

```bash
python3 scripts/utility_checks.py rr-variance --adult-dir adult/ \
    --csv docs/bench-results/2026-11-adult.csv
```

| Column | k | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `sex` | 2 | 0.0241 ± 0.0212 (0.0361) | 0.0060 ± 0.0043 (0.0071) | 0.0024 ± 0.0019 (0.0035) | 0.0022 ± 0.0014 (0.0015) | 0.0003 ± 0.0003 (0.0003) | 0.0000 ± 0.0000 (0.0000) |
| `race` | 5 | 0.1338 ± 0.0389 (0.1752) | 0.0311 ± 0.0085 (0.0312) | 0.0127 ± 0.0052 (0.0136) | 0.0050 ± 0.0019 (0.0052) | 0.0010 ± 0.0004 (0.0009) | 0.0001 ± 0.0000 (0.0001) |
| `workclass` | 9 | 0.3352 ± 0.0716 (0.4420) | 0.0660 ± 0.0222 (0.0757) | 0.0245 ± 0.0090 (0.0313) | 0.0095 ± 0.0026 (0.0110) | 0.0015 ± 0.0004 (0.0017) | 0.0001 ± 0.0000 (0.0001) |
| `education` | 16 | 0.4878 ± 0.1122 (1.0706) | 0.1439 ± 0.0249 (0.1792) | 0.0733 ± 0.0168 (0.0716) | 0.0193 ± 0.0022 (0.0233) | 0.0033 ± 0.0008 (0.0032) | 0.0002 ± 0.0001 (0.0003) |

**Adult `education` (16 categories), all arms**

| Arm | Analyst | ε = 0.1 | ε = 0.5 | ε = 1 | ε = 2 | ε = 5 | ε = 10 |
|---|---|---|---|---|---|---|---|
| `random_choice` | SQL | 0.5229 ± 0.0015 | 0.5229 ± 0.0015 | 0.5229 ± 0.0015 | 0.5229 ± 0.0015 | 0.5229 ± 0.0015 | 0.5229 ± 0.0015 |
| `randomized_response` | SQL | 0.5198 ± 0.0018 | 0.5034 ± 0.0019 | 0.4736 ± 0.0018 | 0.3732 ± 0.0018 | 0.0512 ± 0.0009 | 0.0004 ± 0.0001 |
| `randomized_response` | estimator | 0.4878 ± 0.1122 | 0.1439 ± 0.0249 | 0.0733 ± 0.0168 | 0.0193 ± 0.0022 | 0.0033 ± 0.0008 | 0.0002 ± 0.0001 |
| `central` | central DP | 0.0035 ± 0.0006 | 0.0006 ± 0.0001 | 0.0004 ± 0.0001 | 0.0002 ± 0.0000 | 0.0001 ± 0.0000 | 0.0000 ± 0.0000 |

**The error follows the variance of k-ary randomized response.** Wang et al. (2017, Section 3, Eq. 2) give the variance of the estimated count of a category with true share f: n·q(1 − q)/(p − q)² + n·f(1 − p − q)/(p − q), with p = e^ε/(e^ε + k − 1) and q = 1/(e^ε + k − 1). Taking each estimated share as normal with that variance gives an expected distance of ½ Σ √(2/π)·σᵢ. That is the number in brackets in the table above. From ε = 0.5 on, the prediction lies within two standard deviations of the observed mean in every column. At ε = 0.1 it overshoots everywhere, up to an impossible 1.07 for `education`: clipping the negative shares bounds the error, and the normal approximation no longer holds.

## Findings

1. **Means survive; other queries do not, for the SQL analyst.** Straight SQL on the masked dump gives the mean of `salary` within 2.1% at 10⁵ rows or more and ε ≥ 0.5 with a priori or DP bounds, and the means of Adult's columns within 1% from ε = 2 on. Everything else is noise until ε is large. On Adult and on `salary` from 10⁵ rows, at ε = 1, range counts are 75–99% off, the 90th percentile 145–270% (860% with the true range of `salary`), and the histogram distance 0.63–0.87; at ε = 10, still 11–93%, 5–61% and 0.10–0.42. That bias does not shrink with n.
2. **Post-processing recovers quantiles and smooth histograms.** Deconvolving the noise, which costs no privacy, brings the 90th percentile of `salary` to 3.1% and its histogram distance to 0.095 (`a_priori`, ε = 1, n = 10⁶), and the 90th percentile of `age` and `hours_per_week` to 3–5% at ε = 1. On `salary` its error falls with ε, from a histogram distance of 0.32 at ε = 0.1 to 0.047 at ε = 10, and with n.
3. **It does not recover a spike, and as configured it overfits.** On `hours_per_week`, where 40 holds 47% of the records, the count of that value stays 92% off at ε = 1 and 44% at ε = 10 with the a priori bounds, and the histogram distance is 0.36 at ε = 1. On `age`, its range and histogram errors stop falling from ε = 0.5 on, because EM runs past its best number of iterations. Smooth synthetic data hid both.
4. **δ3: with a priori bounds that clamp, the bias sets a floor.** The bounds [0, 10000] clamp 3.4% of the salaries, which biases the mean by about −1.8% at n = 10⁶ (−1.5% to −2.1% across ε and analysts, from ε = 0.1 to 10), in the local and the central model alike. More data or more ε do not remove it; data-informed bounds leave −0.4% to −0.8% from ε = 0.5 on. On Adult, the bounds [0, 100] clamp nothing, and the mean error of every arm stays within 1.5% from ε = 0.5 on.
5. **DP bounds need data.** At 10⁴ rows the 10% share of ε does not find the quantiles: the upper bound of `salary` landed anywhere from 13,200 to 92,600 at ε = 1, and the SQL mean is −7.7% ± 15.4%. From 10⁵ rows on, at ε = 1, `dp_bounds` read through the estimator gives the lowest errors of the local arms on the range count, the 90th percentile and the histogram of `salary`; at other ε the a priori bounds sometimes do better (the 10% range at ε = 0.5 and n = 10⁶: 10.9% against 19.8%).
6. **The true range is not the best bound.** `oracle` has no clamping bias, but the true maximum of `salary` is far out (43,161 against a 99th percentile near 12,800), so its noise is wider. At ε = 1 and n = 10⁶ its estimator misses the 10% range by 38% where `dp_bounds` misses it by 4.5%, and at ε = 0.1 EM cannot undo noise of scale 428,000 at all (mean +50.6%). Choosing bounds is a bias–variance trade-off (Amin et al., 2019), and quantiles beat the extremes; the gap closes at ε ≥ 5.
7. **δ2: the local model costs orders of magnitude on everything but the mean.** On Adult and on `salary` from 10⁵ rows, at every ε, central DP with the true bounds stays within 1% on every query and within 0.005 in distance, its quantiles aside (up to 1.1%). The best local answers at n = 10⁶ and ε = 1 are 4.5% off for the 10% range count of `salary`, 1.0% for its 90th percentile and 0.059 in histogram distance. In theory the local error falls as 1/√n and the central one as 1/n (Duchi et al., 2018).
8. **Randomized response pays for its categories.** `random_choice` erases the distribution (distance 0.17–0.66 on Adult, whatever ε). Read with SQL, randomized response pulls the shares toward uniform by a factor that depends only on ε and k. Through the estimator it is unbiased, and its error grows with k as the variance of k-ary randomized response predicts: at ε = 1 and n = 48,842, a distance of 0.002 with 2 categories, 0.013 with 5, 0.025 with 9 and 0.073 with 16. With 16 categories it is 0.14 at ε = 0.5 and 0.49 at ε = 0.1.
9. **One run is not a result.** Where the dispersion is as large as the mean (the SQL mean at 10⁴ rows or at ε = 0.1, the estimator's ranges on `age`), a single masked dump can land far from the averages above.

## Limitations

* Each column is masked and measured alone. What a dump keeps of the relations between columns is stage 2.2.
* Adult is one real dataset: 48,842 records from 1994, with two numeric columns. Its numbers are whole and its `hours_per_week` has a spike at 40, which the synthetic `salary` has not; other data will behave otherwise.
* The numeric estimator is a plain EM deconvolution without regularization, stopped when the estimate moves by less than 10⁻⁵ or after 5,000 iterations. On Adult that rule overfits (see above); a stopping rule chosen without the truth, or a regularized estimator, is left to stage 2.5.
* The central comparator gives each query the full ε; a fair comparison of a whole release would split ε across queries, which would raise the central errors.
* No synthetic-data comparator (MST, AIM) was run (stage 2.4).

## References

* Amin, K.; Kulesza, A.; Muñoz Medina, A.; Vassilvitskii, S. (2019). Bounding user contributions: a bias-variance trade-off in differential privacy. *ICML*.
* Becker, B.; Kohavi, R. (1996). Adult [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5XW20.
* Duchi, J.; Jordan, M.; Wainwright, M. (2018). Minimax optimal procedures for locally private estimation. *JASA*.
* Hay, M. et al. (2016). Principled evaluation of differentially private algorithms using DPBench. *SIGMOD*.
* Wang, T.; Blocki, J.; Li, N.; Jha, S. (2017). Optimizing locally differentially private protocols. arXiv:1705.04421.
