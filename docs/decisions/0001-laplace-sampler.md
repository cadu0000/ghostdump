# 0001: Exact Laplace sampler on integers, without OpenDP

- **Status:** accepted
- **Date:** November 2026
- **Scope:** `dp_laplace` columns (`src/engine/laplace.rs`)

## Context

`dp_laplace` adds Laplace noise to each value so that every published value is ε-LDP. The first sampler drew the noise in `f64`. Floating-point samplers are vulnerable to attacks on the low-order bits of the output (Mironov, 2012): the set of reachable outputs depends on the input, so an adversary can sometimes tell inputs apart with certainty. With that sampler the guarantee only holds under an "exact arithmetic" assumption.

The evaluation of the paper (research question RQ-δ) needs a sampler whose guarantee does not rest on that assumption, and needs to measure what it costs.

## Options evaluated

All numbers come from one machine, 10^6 samples, bounds `[0, 100000]`, ε = 1.

| Option | Cost per value | Floating point in sampling | New dependencies | Notes |
|---|---|---|---|---|
| `f64` sampler (existing) | 15–20 ns | yes | none | vulnerable to Mironov's attack |
| OpenDP 0.16, default granularity | 21–26 µs | no | `opendp`, `dashu` | `contrib` only, see below |
| OpenDP 0.16, granularity from `scale` | 6.0–6.7 µs | no | `opendp`, `dashu` | ε slightly above the configured value |
| **Own exact sampler on integers** | **258–284 ns** | **no** | **none** | chosen |

## Why not OpenDP

OpenDP was the first candidate. A feasibility test (OpenDP 0.16.0, `default-features = false`) found four problems:

1. **Its Laplace mechanism is not reviewed.** `make_laplace` only exists under the `contrib` feature, which OpenDP uses for code whose proofs have not finished its review process. Replacing a known weakness with unreviewed third-party code would not give a stronger guarantee to state in the paper.
2. **Cost.** 6–26 µs per value, depending on the noise granularity: one million rows with one `dp_laplace` column would spend 6–26 s on sampling alone, while the whole pipeline processes 900 thousand rows with rules in about 2.5 s. It also adds an arbitrary-precision rational library (`dashu`) and about 40 s of build time.
3. **It cannot use GhostDump's random generator.** OpenDP draws random bits from OpenSSL (`use-openssl`) or from `rand::thread_rng`, so it cannot honor `--seed`.
4. **Its privacy map exceeds ε with a coarse granularity.** With the granularity derived from `scale` (needed for the 6 µs figure), the map rounds the sensitivity up to the grid: ε = 1 became 1.000000078, and ε = 0.3 with bounds `[18, 99]` became 0.3037. The scale would have to be inflated and the map checked again.

**What is lost:** an independent implementation to cross-check our sampler. The statistical validation below compensates by comparing the sampler with the exact distribution.

## Decision

`dp_laplace` uses, by default (`mechanism = "exact"`), the discrete Laplace sampler of Canonne, Kamath and Steinke (2020, algorithms 1 and 2), implemented with integer arithmetic only:

1. the value is clamped to the bounds and rounded to an integer number of units of the published precision (with `scale = 2`, one unit is 0.01);
2. the sensitivity in units is Δ = units(hi) − units(lo), since rounding is monotone;
3. the noise parameter is t = ⌈Δ / ε⌉, so the effective ε, Δ/t, never exceeds the configured one;
4. the output is the integer (value + noise), written with the decimal point in place.

No floating-point operation takes part in sampling or in the output. Inputs are parsed as `f64` before clamping, which does not affect the guarantee: the result is mapped into the bounded integer domain either way.

The `f64` sampler stays available (`mechanism = "f64"`) to measure the cost of exact sampling.

## Random source

Both samplers draw from the run's `StdRng` from the `rand` crate (ChaCha12, seeded from the operating system, or from `--seed` for experiments). The Rust standard library has no random generator, and `StdRng` is the generator `rand` recommends for general use.

`rand` does not promise to keep the same algorithm behind `StdRng` across versions. `Cargo.lock` pins the version, and the golden test (`tests/fixtures/pg_data_masked.sql`) fails if the random sequence changes, so an update of `rand` is an explicit decision.

## Consequences

- The exact sampler costs about 15 times the `f64` one per value, which is small next to the cost of a row with rules.
- Its guarantee no longer assumes exact arithmetic. It is still our implementation and has not been audited; it is checked by tests:
  - `discrete_laplace` against the exact probability of each integer (small t);
  - goodness of fit (chi-square over 100 equiprobable bins) for both samplers;
  - a periodogram of the low-order residues, to catch periodic artifacts;
  - a negative control for each check, which must fail on a broken sampler.

  Run them with `cargo test --release -- --ignored` (CI runs them too).
- Bounds, `scale` and ε must fit the integer domain: bounds times 10^scale below 2^53 and t at most 2^50. Rules outside these limits are rejected at startup.

## References

- Canonne, C.; Kamath, G.; Steinke, T. (2020). The discrete Gaussian for differential privacy. *NeurIPS*.
- Mironov, I. (2012). On significance of the least significant bits for differential privacy. *CCS*.
- OpenDP 0.16.0 source: `measurements/noise/distribution/laplace/mod.rs`, `traits/samplers/mod.rs`.
