# Utility evaluation

**Question.** How much statistical utility does a dump masked with `dp_laplace` keep, and where does the loss come from? The evaluation separates the two costs of RQ-δ:

* **δ2, the local model:** local DP against central DP at the same ε. Any mechanism that sees one row at a time pays it.
* **δ3, the state restriction:** given local DP, bounds chosen without the data against bounds informed by the data.

**Run it:**

```bash
cargo build --release
python3 scripts/utility_eval.py --sizes 10000,100000,1000000 --epsilons 0.5,1,2 --reps 10
```

The harness is `scripts/utility_eval.py` (standard library only). Every masked dump goes through the release binary; the run below took about 40 minutes on the reference machine of [benchmarks.md](../benchmarks.md). The raw results (6,480 rows) are in [bench-results/2026-11-utility.csv](../bench-results/2026-11-utility.csv).

## Setup

**Data.** One `salary` column with the generator's distribution: lognormal with a median of about 4,000, mean 4,566, 5% NULL. n ∈ {10⁴, 10⁵, 10⁶} rows, seed 42; the datasets are nested (the first 10⁵ rows of the 10⁶ dataset are the 10⁵ dataset).

**Mechanism.** `dp_laplace` with the exact sampler, `scale = 2`, ε ∈ {0.5, 1, 2}, 10 repetitions per point (different noise each time).

**Local arms** (same binary, different bounds in the rules):

| Arm | Bounds | Private? | Passes over the data |
| --- | --- | --- | --- |
| `a_priori` | [0, 10000], chosen from domain knowledge; clamps the top 3.4% of salaries | yes | 1 |
| `dp_bounds` | 1% and 99% quantiles, estimated with central DP (exponential mechanism on a grid of 100) with 10% of ε; the values get the other 90% | yes (ε in total) | 2 |
| `oracle` | the true minimum and maximum | **no**: used only as a reference | 2 |

`dp_bounds` keeps memory constant (a fixed histogram) but needs two passes, so it does not work with `zcat dump.gz | ghostdump`.

**Analysts** on each masked dump:

* **SQL**: what a developer gets by querying the dump directly (`AVG`, `COUNT ... WHERE`, `percentile_cont`, `width_bucket`).
* **estimator**: post-processing that deconvolves the Laplace noise (EM on a 50-bin histogram of the clamped values) before answering. It spends no privacy.

**Central comparator.** Each query answered from the raw data with ε-DP under replacement of one value: Laplace on sums and counts, the exponential mechanism for quantiles. It is generous to the central model: every query gets the full ε, where a real release would split it. `central_a_priori` uses the a priori bounds, `central_oracle` the true range.

**Queries.** Mean, sum, count, counts in ranges centred on the median holding 50%, 10% and 2% of the values, median, 90th percentile, and a histogram of 20 bins of 1,000. The error is the relative error, signed for the mean (so that bias shows) and absolute elsewhere, averaged over the repetitions; the histogram uses the total variation distance.

## Results

**Mean salary: signed relative error (bias plus noise), ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | -0.6% | -1.7% | -1.8% |
| `a_priori` | estimator | -1.2% | -1.9% | -2.0% |
| `dp_bounds` | SQL | +9.8% | -1.0% | -0.6% |
| `dp_bounds` | estimator | +6.8% | -0.8% | -0.7% |
| `oracle` | SQL | +2.2% | +0.9% | +0.9% |
| `oracle` | estimator | +0.9% | +0.3% | +0.2% |
| `central_a_priori` | central DP | -1.7% | -1.7% | -1.8% |
| `central_oracle` | central DP | -0.0% | +0.0% | -0.0% |

**Mean salary: mean absolute relative error, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 2.8% | 1.8% | 1.8% |
| `a_priori` | estimator | 2.1% | 2.0% | 2.0% |
| `dp_bounds` | SQL | 17.0% | 1.6% | 0.6% |
| `dp_bounds` | estimator | 13.1% | 1.3% | 0.7% |
| `oracle` | SQL | 4.8% | 3.8% | 1.2% |
| `oracle` | estimator | 5.5% | 2.3% | 0.8% |
| `central_a_priori` | central DP | 1.7% | 1.7% | 1.8% |
| `central_oracle` | central DP | 0.0% | 0.0% | 0.0% |

**Count in a range holding 10% of the values: mean absolute relative error, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 76.8% | 78.4% | 78.4% |
| `a_priori` | estimator | 70.7% | 19.5% | 8.5% |
| `dp_bounds` | SQL | 93.9% | 82.7% | 82.7% |
| `dp_bounds` | estimator | 32.6% | 7.3% | 5.0% |
| `oracle` | SQL | 88.8% | 94.3% | 94.3% |
| `oracle` | estimator | 22.6% | 37.4% | 38.8% |
| `central_a_priori` | central DP | 0.1% | 0.0% | 0.0% |
| `central_oracle` | central DP | 0.2% | 0.0% | 0.0% |

**90th percentile: mean absolute relative error, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 174.1% | 172.4% | 172.5% |
| `a_priori` | estimator | 11.8% | 5.4% | 2.9% |
| `dp_bounds` | SQL | 1247.3% | 230.7% | 231.6% |
| `dp_bounds` | estimator | 12.9% | 4.5% | 0.8% |
| `oracle` | SQL | 446.7% | 861.0% | 863.7% |
| `oracle` | estimator | 13.9% | 14.2% | 16.4% |
| `central_a_priori` | central DP | 0.1% | 0.0% | 0.0% |
| `central_oracle` | central DP | 0.0% | 0.2% | 0.2% |

**Histogram (20 bins of 1,000): total variation distance, ε = 1**

| Arm | Analyst | n = 10,000 | n = 100,000 | n = 1,000,000 |
|---|---|---|---|---|
| `a_priori` | SQL | 0.635 | 0.633 | 0.632 |
| `a_priori` | estimator | 0.310 | 0.138 | 0.091 |
| `dp_bounds` | SQL | 0.867 | 0.686 | 0.686 |
| `dp_bounds` | estimator | 0.232 | 0.090 | 0.059 |
| `oracle` | SQL | 0.791 | 0.867 | 0.867 |
| `oracle` | estimator | 0.212 | 0.221 | 0.227 |
| `central_a_priori` | central DP | 0.023 | 0.021 | 0.022 |
| `central_oracle` | central DP | 0.002 | 0.000 | 0.000 |

**Varying ε at n = 1,000,000:**

**Mean salary: mean absolute relative error, n = 1,000,000**

| Arm | Analyst | ε = 0.5 | ε = 1 | ε = 2 |
|---|---|---|---|---|
| `a_priori` | SQL | 1.7% | 1.8% | 1.8% |
| `a_priori` | estimator | 2.2% | 2.0% | 1.9% |
| `dp_bounds` | SQL | 0.3% | 0.6% | 0.5% |
| `dp_bounds` | estimator | 0.4% | 0.7% | 0.5% |
| `oracle` | SQL | 2.1% | 1.2% | 0.6% |
| `oracle` | estimator | 2.0% | 0.8% | 0.5% |
| `central_a_priori` | central DP | 1.8% | 1.8% | 1.8% |
| `central_oracle` | central DP | 0.0% | 0.0% | 0.0% |

**Count in a range holding 10% of the values, n = 1,000,000**

| Arm | Analyst | ε = 0.5 | ε = 1 | ε = 2 |
|---|---|---|---|---|
| `a_priori` | SQL | 88.3% | 78.4% | 62.6% |
| `a_priori` | estimator | 11.8% | 8.5% | 4.0% |
| `dp_bounds` | SQL | 90.8% | 82.7% | 68.9% |
| `dp_bounds` | estimator | 20.8% | 5.0% | 6.6% |
| `oracle` | SQL | 97.1% | 94.3% | 89.2% |
| `oracle` | estimator | 50.6% | 38.8% | 13.4% |
| `central_a_priori` | central DP | 0.0% | 0.0% | 0.0% |
| `central_oracle` | central DP | 0.0% | 0.0% | 0.0% |

**Histogram total variation distance, n = 1,000,000**

| Arm | Analyst | ε = 0.5 | ε = 1 | ε = 2 |
|---|---|---|---|---|
| `a_priori` | SQL | 0.767 | 0.632 | 0.458 |
| `a_priori` | estimator | 0.148 | 0.091 | 0.065 |
| `dp_bounds` | SQL | 0.806 | 0.686 | 0.523 |
| `dp_bounds` | estimator | 0.105 | 0.059 | 0.048 |
| `oracle` | SQL | 0.922 | 0.867 | 0.779 |
| `oracle` | estimator | 0.310 | 0.227 | 0.126 |
| `central_a_priori` | central DP | 0.022 | 0.022 | 0.022 |
| `central_oracle` | central DP | 0.000 | 0.000 | 0.000 |

The full table, with the 50% and 2% ranges, the median and the sum, is printed by the harness and can be rebuilt from the CSV.

## Findings

1. **Means and sums survive, other queries do not, for the SQL analyst.** Straight SQL on the masked dump gives the mean within 2% at 10⁵ rows or more with a priori or DP bounds (3.8% with the true range at 10⁵, 1.2% at 10⁶). Range counts are wrong by 63–97%, the 90th percentile by 170% or more, and the histogram distance is 0.46–0.92, at every n: the noisy values spread far beyond the data, and that bias does not shrink with n. A developer who runs these queries on the dump gets noise.
2. **Post-processing recovers much of it.** Deconvolving the noise, which costs no privacy, brings the 10% range count from 78% to 8.5% error, the 90th percentile from 172% to 2.9% and the histogram distance from 0.63 to 0.09 (`a_priori`, ε = 1, n = 10⁶). Its error falls with n, as it should. Shipping such an estimator next to masked dumps would change what they are good for.
3. **δ3: with a priori bounds, the clamping bias sets a floor.** The a priori bounds clamp 3.4% of the salaries, which biases the mean by about −1.8% from 10⁵ rows on, at every ε, in the local and the central model alike (at 10⁴ rows the noise still hides it). More data or more ε do not remove it. Data-informed bounds do: at ε = 1 and n ≥ 10⁵, `dp_bounds` gives the mean within 0.6–1.6% and the lowest errors of all local arms on ranges, percentiles and histograms (at ε = 2 and n = 10⁶, `a_priori` is slightly better on the range count, 4.0% against 6.6%). Below 10⁵ rows, the 10% share of ε is not enough for the quantiles: the upper bound landed anywhere from 11,600 to 98,600 at n = 10⁴, and `dp_bounds` gives the worst mean of all arms (17% error at ε = 1).
4. **The true range is not the best bound.** `oracle` has no clamping bias, but the true maximum of a heavy-tailed column is far out (43,161 against a 99th percentile near 12,800), so the noise is 3.3 times wider than with `dp_bounds`. Its mean error falls with n (1.2% at 10⁶), but its range, percentile and histogram errors are 3–20 times those of `dp_bounds`. Choosing bounds is a bias–variance trade-off (Amin et al., 2019), and quantiles beat the extremes.
5. **δ2: the local model costs orders of magnitude on everything but the mean.** With the true bounds, central DP answers every query within 1% at n = 10⁴ and within 0.2% at n = 10⁶. At n = 10⁶ and ε = 1, the best local answers are 5–9% off for the 10% range count, 0.8–3% for the 90th percentile, and 0.06–0.09 in histogram distance. In theory the local error falls as 1/√n and the central one as 1/n (Duchi et al., 2018). For the mean with a priori bounds, the gap disappears behind the shared clamping bias (−1.8% in both).

## Limitations

* One numeric column of synthetic data; real data (ACS PUMS) was planned but not reachable from the development environment.
* The estimator is a plain EM deconvolution without regularization, stopped when the estimate changes by less than 10⁻⁵; better estimators exist. With noise much wider than the bounds it converges slowly, and an early version that stopped after 200 iterations reported errors that were too high for the `oracle` arm.
* The central comparator gives each query the full ε; a fair comparison of a whole release would split ε across queries, which would raise the central errors.
* No synthetic-data comparator (MST, AIM) was run.

## References

* Amin, K.; Kulesza, A.; Muñoz Medina, A.; Vassilvitskii, S. (2019). Bounding user contributions: a bias-variance trade-off in differential privacy. *ICML*.
* Duchi, J.; Jordan, M.; Wainwright, M. (2018). Minimax optimal procedures for locally private estimation. *JASA*.
* Hay, M. et al. (2016). Principled evaluation of differentially private algorithms using DPBench. *SIGMOD*.
