# 0003: Audit of the numeric conversions in the samplers

- **Status:** accepted
- **Date:** November 2026
- **Scope:** `src/engine/laplace.rs`, `src/engine/categorical.rs` and the checks of `dp_laplace` in `src/engine/strategy.rs` (roadmap stage 1.2, risks R7 and R8)

## Context

The guarantee of the exact sampler (P1 in [guarantees](../guarantees.md)) holds when three things are true: every input is mapped into the integer domain [L, H], in units of `scale`; the noise parameter t satisfies Δ/t ≤ ε exactly, with Δ = H − L; and the noise follows the discrete Laplace distribution with parameter t. A conversion that silently loses information, or an overflow, can break any of the three, and the review of November 2026 found that the `as` conversions of the sampler had no recorded reason (R8).

This record lists every numeric conversion and every operation that could overflow in the code that draws noise, with the reason it is exact, checked or harmless, and the changes the audit made.

Conversions that only decide which integer an input or a bound becomes do not touch the guarantee. P1 holds for any map of the inputs into [L, H], because the inputs are clamped to the integer bounds actually used and Δ is computed from them. A rounding at a tie moves a value by one unit, which only affects utility.

## Exact sampler (`laplace.rs`)

| Where | Operation | Why it is exact or safe |
| --- | --- | --- |
| `exact_fraction` | `to_bits`, the exponent through `i32::try_from`, the mantissa through `u128::from` | bit reinterpretation and widening |
| `exact_fraction` | halving `num` while it is even | removes factors of two only; the denominator is 2^shift with shift ≤ 100 |
| `exact_fraction` | `num · 2^s` for values of 2^52 and above | `checked_shl` and `checked_mul`: `None` past u128 |
| `exact_fraction` | negative, NaN or infinite input | `None` |
| `ExactDomain::new` | `scale` to `u32`, then 10^scale | `u32::try_from` and `checked_pow` |
| `ExactDomain::new` | `unit as f64` | exact: 10^d = 2^d·5^d, and 5^d < 2^53 for d ≤ 22 (here d ≤ 18) |
| `ExactDomain::new` | `(bound · 10^scale).round()` | rounds the bound to the grid of units (see Context) |
| `ExactDomain::new` | `units as i64` for the bounds | NaN and \|units\| > 2^53 are rejected first; an integer-valued `f64` within ±2^53 converts exactly |
| `ExactDomain::new` | Δ = `hi.abs_diff(lo)`, widened to u128 | exact; Δ ≤ 2^54 |
| `ExactDomain::new` | t = ⌈Δ·den/num⌉, with ε = num/den | integers only: `checked_mul`, `div_ceil`, `u64::try_from`, and t ≤ 2^50. ε is capped at 2^54 first: every ε ≥ Δ gives t = 1 either way |
| `ExactDomain::effective_epsilon` | `Δ as f64 / t as f64` | display only (startup summary, debug log). For Δ ≤ 2^53 both convert exactly and the quotient is correctly rounded, so it never shows more than ε; above 2^53 it can show at most about 2^-52 more, relative. The bound itself is checked in integers |
| `ExactDomain::to_units` | input to `f64`, `round(v · 10^scale)`, clamp, `i64` | parsing is correctly rounded (`str::parse`) and non-finite input counts as not a number; v · 10^scale is finite or ±∞, never NaN; after clamping to the bounds (exact in `f64`) the value is an integer within ±2^53, so the cast is exact |
| `ExactDomain::to_units` | midpoint `lo + (hi − lo) / 2` | i64 arithmetic within ±2^54 |
| `ExactDomain::noisy` | value plus noise | `saturating_add`; noise beyond 2^62 has probability at most 2e^-4096 |
| `ExactDomain::format` | `unsigned_abs`, `/`, `%` | exact, also for `i64::MIN` |
| `discrete_laplace` | `u128::from(t)`, `u + t · v` | widening; t < 2^64, and v < 2^64 except with probability e^-(2^64), so the product fits |
| `discrete_laplace` | magnitude to `i64` | `i64::try_from`; with t ≤ 2^50 it fails only if v ≥ 2^13 (probability e^-8192), and then saturates |
| `bernoulli` | `gen_range(0..den)` compared with `num` | exactly uniform (see below); den ≥ 1 in every call |
| `bernoulli_exp_le1` | `den · k` and `k + 1` | `checked_mul` and `k < u128::MAX`: the loop stops before either overflows. For den ≤ 2^100 that happens at step 2^28 at the earliest, which changes the distribution by less than 1/(2^28 − 1)! |
| `bernoulli_exp` | `num / den`, `num % den` | integer division, den ≥ 1 |
| `discrete_laplace` | sign of the noise, `gen::<bool>()` | exactly 1/2 (see below) |

## Randomized response (`categorical.rs`)

| Where | Operation | Why it is exact or safe |
| --- | --- | --- |
| `RandomizedResponse::new` | ε to num/den | ε is checked to be in (0, 64], so den ≤ 2^100 and num ≤ 2^106; `bernoulli_exp` then loops at most 64 times, and every product in `bernoulli_exp_le1` is checked |
| `apply` | `gen_range(0..k)` | exactly uniform |
| `keep_probability` | `len() as f64`, `exp` | display only |

## The `f64` sampler, kept for experiments

| Where | Operation | Why it is exact or safe |
| --- | --- | --- |
| `max_f64_scale` | `log2().floor()` to `i32` | the exponent is clamped to [−1074, 1024] before the cast, so it is exact; the magnitude is positive because lo < hi |
| `max_f64_scale` | `−log10(grid)` to `u32` | a non-negative integer or +∞, and `as u32` saturates. +∞ (a grid below the smallest double) gives `u32::MAX`, for magnitudes where any `scale` up to 12 is safe |
| `laplace_noise` | U to −ln(1 − U) · b | U = (u64 >> 11) · 2^-53, so 1 − U ≥ 2^-53 and −ln(1 − U) ≤ 53 · ln 2 < 37. `compile_laplace` rejects bounds and ε for which \|bound\| + 37 · b is not finite, so no output is ±∞ or NaN |
| `apply`, consistent noise | clamped value to the bytes of the key | −0.0 is mapped to 0.0 first, so equal values get one key |
| `format` | `{:.*}` | Rust writes the exact decimal value of the double, rounded to `scale` places; "-0.00" is written "0.00" |

What makes this sampler unsafe is not a conversion: floating-point noise leaks through its low-order bits (Mironov, 2012; [floating-point experiment](../experiments/floating-point.md)). That is why it runs only with `--experimental-f64-sampler`.

`compile_laplace` also converts `scale` (u32, at most 12) to `usize`, which is lossless.

## Uniform values from `rand` 0.8.5

The exact samplers need integers that are exactly uniform and fair bits. `Cargo.lock` pins `rand` 0.8.5, and its source shows:

* `gen_range` on an integer range calls `UniformInt::sample_single_inclusive` (`src/distributions/uniform.rs`). For a range of size n it draws v uniform in [0, 2^N), writes v · n = hi · 2^N + lo, and accepts when lo < n · 2^z, where z is the number of leading zeros of n. For each result hi, the accepted v are those with v · n in an interval of length n · 2^z, a multiple of n, so each result has exactly 2^z of them. The accepted output is exactly uniform, given uniform bits.
* `gen::<bool>()` is the sign bit of `next_u32()` (`src/distributions/other.rs`).
* `gen::<f64>()` is `(next_u64() >> 11) · 2^-53` (`src/distributions/float.rs`).

## What the audit changed

1. **The noise parameter is computed in integers.** It was ⌈Δ/ε⌉ in `f64`, raised until Δ/t ≤ ε held in `f64`. Above 2^53, Δ does not convert exactly, and the check could pass while Δ/t > ε, by at most about 2.2 · 10^-16 relative (one rounding of Δ and one of the quotient). On the second half of `test_noise_parameter_is_exact_for_random_parameters` (10,000 parameters with Δ near 2^54 and ε in [16, 65)), 346 exceeded ε; on the first half (Δ up to 2^31), none did. Now t = ⌈Δ·den/num⌉ from the exact value of ε, and both exactness tests fail when the old computation is put back.
2. **`exact_fraction` has a checked domain.** It moved from `categorical.rs`, where ε ≤ 64, to the Laplace path, where ε has no upper limit. It now returns `None` for negative, non-finite and too large values, instead of dropping the sign or shifting bits out of the u128, and ε is capped at 2^54 before the conversion.
3. **`ExactDomain::new` checks its own inputs.** It is public; it rejects a non-positive or non-finite ε and NaN bounds, which converted to 0 before.
4. **No overflow in the exact sampler.** The noisy sum saturates, and `bernoulli_exp_le1` cannot overflow its product or its count.
5. **`f64` sampler.** Rules whose noise could overflow are rejected: with ε = 1e-320, or bounds of ±1e308, it wrote `inf` and `-inf`, which are not SQL numbers. With `consistent = true`, −0 and 0 now get the same noise, as with the exact sampler. And `mechanism = "f64"` runs only with the hidden option `--experimental-f64-sampler`, which warns at startup (R7).

## Tests at the extremes

* `laplace::tests::test_exact_fraction`: zero, 2^127, the largest double below 2^128, values from 2^128 up, NaN, infinity, negative values.
* `test_noise_parameter_is_exact_at_the_limits`: bounds of ±2^53 (Δ = 2^54), an odd Δ above 2^53, ε from 10^-9 to `f64::MAX`, t = 2^50 exactly, and the first values past each limit; invalid ε and bounds.
* `test_noise_parameter_is_exact_for_random_parameters`: t is the smallest integer with Δ/t ≤ ε, compared in integers, on 10,000 random parameters with Δ up to 2^31 and 10,000 with Δ near 2^54.
* `test_noisy_sum_saturates`, `test_bernoulli_ends_whatever_the_generator_returns`, `test_f64_scale_limit_at_extreme_parameters`, and the signed zeros in `test_consistent_noise_depends_only_on_value_column_and_secret`.
* `strategy::tests::test_f64_sampler_rejects_noise_that_overflows`.
* `categorical::tests::test_epsilon_at_its_limits`: ε = 64 and 10^-9 accepted, just above 64 rejected, and the probability of keeping the category at both ends.
* `cli::f64_sampler_needs_the_hidden_experiment_option`.

## Not covered

The parsing of SQL values (`codec`), the generators of synthetic values, and ChaCha12 itself, which is `rand`'s.

## References

- Canonne, C.; Kamath, G.; Steinke, T. (2020). The discrete Gaussian for differential privacy. *NeurIPS*.
- Mironov, I. (2012). On significance of the least significant bits for differential privacy. *CCS*.
- `rand` 0.8.5 source: `src/distributions/uniform.rs`, `src/distributions/other.rs`, `src/distributions/float.rs`.
