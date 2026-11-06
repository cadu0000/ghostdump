//! The Laplace mechanism of `dp_laplace` columns.

use rand::Rng;

use crate::codec::SqlValue;
use crate::engine::mask::MaskStats;

/// The Laplace mechanism for one column, with its parameters validated.
///
/// Each value is clamped to `[lo, hi]` (values that are not numbers, and NULL
/// with `null_policy = "generate"`, take the midpoint), receives noise of
/// scale `(hi - lo) / ε`, and is written with a fixed number of decimals.
/// Every output is then ε-LDP whatever the input.
///
/// Two samplers are available. `Exact` works on integers in units of the
/// published precision and involves no floating-point step. `F64` samples
/// in floating point, which is vulnerable to attacks on the low-order bits
/// (Mironov, 2012): its guarantee assumes exact arithmetic. It is kept to
/// measure the cost of exact sampling.
#[derive(Debug, Clone)]
pub struct LaplaceMechanism {
    epsilon: f64,
    lo: f64,
    hi: f64,
    decimals: usize,
    clamp_output: bool,
    sampler: Sampler,
}

#[derive(Debug, Clone)]
enum Sampler {
    F64 { noise_scale: f64 },
    Exact(ExactDomain),
}

impl LaplaceMechanism {
    /// The `f64` sampler.
    pub fn new(epsilon: f64, lo: f64, hi: f64, decimals: usize, clamp_output: bool) -> Self {
        Self {
            epsilon,
            lo,
            hi,
            decimals,
            clamp_output,
            sampler: Sampler::F64 {
                noise_scale: (hi - lo) / epsilon,
            },
        }
    }

    /// The exact integer sampler; fails when the bounds, `scale` and ε do
    /// not fit its integer domain.
    pub fn exact(
        epsilon: f64,
        lo: f64,
        hi: f64,
        decimals: usize,
        clamp_output: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            sampler: Sampler::Exact(ExactDomain::new(epsilon, lo, hi, decimals)?),
            ..Self::new(epsilon, lo, hi, decimals, clamp_output)
        })
    }

    pub(crate) fn apply(
        &self,
        value: SqlValue,
        rng: &mut impl Rng,
        stats: &mut MaskStats,
    ) -> SqlValue {
        let parsed = value
            .as_str()
            .and_then(|text| text.trim().parse::<f64>().ok())
            .filter(|number| number.is_finite());
        if parsed.is_none() && !value.is_null() {
            stats.non_numeric += 1;
        }

        let text = match &self.sampler {
            Sampler::F64 { noise_scale } => self.format(self.noisy(parsed, *noise_scale, rng)),
            Sampler::Exact(domain) => {
                domain.format(domain.noisy(domain.to_units(parsed), self.clamp_output, rng))
            }
        };
        // A number stays unquoted when it came unquoted.
        match value {
            SqlValue::Text(_) => SqlValue::Text(text),
            SqlValue::Raw(_) | SqlValue::Null => SqlValue::Raw(text),
        }
    }

    /// The ε spent per value: the configured one for `F64`, and Δ/t (never
    /// above it) for `Exact`.
    pub fn epsilon(&self) -> f64 {
        match &self.sampler {
            Sampler::F64 { .. } => self.epsilon,
            Sampler::Exact(domain) => domain.effective_epsilon(),
        }
    }

    /// One line for the debug log and the guarantees table.
    pub fn describe(&self) -> String {
        let sampler = match &self.sampler {
            Sampler::F64 { .. } => "f64 sampler: assumes exact arithmetic".to_string(),
            Sampler::Exact(_) => format!("exact integer sampler, configured ε = {}", self.epsilon),
        };
        format!(
            "Laplace noise, ε-LDP per value with ε = {}, bounds [{}, {}], scale {} ({sampler})",
            self.epsilon(),
            self.lo,
            self.hi,
            self.decimals
        )
    }

    /// `None` stands for a value that is not a number.
    fn noisy(&self, value: Option<f64>, noise_scale: f64, rng: &mut impl Rng) -> f64 {
        let clamped = value.map_or((self.lo + self.hi) / 2.0, |v| v.clamp(self.lo, self.hi));
        let noisy = clamped + laplace_noise(noise_scale, rng);
        if self.clamp_output {
            noisy.clamp(self.lo, self.hi)
        } else {
            noisy
        }
    }

    fn format(&self, value: f64) -> String {
        let text = format!("{:.*}", self.decimals, value);
        // Rounding can produce "-0.00"; write it as "0.00".
        match text.strip_prefix('-') {
            Some(rest) if rest.bytes().all(|b| b == b'0' || b == b'.') => rest.to_string(),
            _ => text,
        }
    }
}

/// A Laplace sample with the given scale: an exponential magnitude with a
/// random sign. `1 - U` lies in (0, 1], so the logarithm is always finite.
///
/// This is the `f64` sampler. It is public so that the floating-point
/// experiment (`examples/fp_distinguisher.rs`) can attack it directly.
pub fn laplace_noise(scale: f64, rng: &mut impl Rng) -> f64 {
    let magnitude = -(1.0 - rng.r#gen::<f64>()).ln() * scale;
    if rng.r#gen::<bool>() {
        magnitude
    } else {
        -magnitude
    }
}

/// Bernoulli(num / den), exactly: one uniform integer compared with `num`.
fn bernoulli(num: u128, den: u128, rng: &mut impl Rng) -> bool {
    rng.gen_range(0..den) < num
}

/// Bernoulli(exp(-num / den)) for 0 <= num / den <= 1 (Canonne, Kamath and
/// Steinke, 2020, algorithm 1). Only integer comparisons are involved.
fn bernoulli_exp_le1(num: u128, den: u128, rng: &mut impl Rng) -> bool {
    let mut k: u128 = 1;
    while bernoulli(num, den * k, rng) {
        k += 1;
    }
    k % 2 == 1
}

/// A sample of the discrete Laplace distribution with parameter `t`:
/// P(z) is proportional to exp(-|z| / t) for every integer z (Canonne,
/// Kamath and Steinke, 2020, algorithm 2 with s = 1).
///
/// No floating-point operation is involved, so the output carries none of
/// the low-order artifacts of `f64` samplers (Mironov, 2012).
pub fn discrete_laplace(t: u64, rng: &mut impl Rng) -> i64 {
    assert!(t > 0, "the discrete Laplace parameter must be positive");
    let t = u128::from(t);
    loop {
        let u = rng.gen_range(0..t);
        if !bernoulli_exp_le1(u, t, rng) {
            continue;
        }
        let mut v: u128 = 0;
        while bernoulli_exp_le1(1, 1, rng) {
            v += 1;
        }
        let magnitude = u + t * v;
        let negative = rng.r#gen::<bool>();
        if negative && magnitude == 0 {
            continue;
        }
        // The magnitude has a geometric tail: it fits in an i64 for any t
        // accepted by the rules (see `ExactDomain`).
        let magnitude = i64::try_from(magnitude).unwrap_or(i64::MAX);
        return if negative { -magnitude } else { magnitude };
    }
}

/// Largest noise parameter accepted, so that values plus noise stay far
/// from the limits of an i64.
const MAX_NOISE_PARAMETER: u64 = 1 << 50;

/// The integer domain of an exact `dp_laplace` column: values in units of
/// the published precision (with `scale = 2`, one unit is 0.01).
#[derive(Debug, Clone, PartialEq)]
pub struct ExactDomain {
    /// 10^scale.
    unit: i64,
    lo: i64,
    hi: i64,
    /// Discrete Laplace parameter: t = ceil((hi - lo) / ε).
    t: u64,
}

impl ExactDomain {
    /// The domain for `bounds = [lo, hi]` written with `decimals` places,
    /// and the noise parameter that makes it ε-LDP.
    pub fn new(epsilon: f64, lo: f64, hi: f64, decimals: usize) -> Result<Self, String> {
        let unit = 10i64
            .checked_pow(decimals as u32)
            .ok_or("`scale` is too large for the exact sampler")?;
        let to_units = |value: f64| {
            let units = (value * unit as f64).round();
            // 2^53: beyond it, f64 no longer holds every integer.
            if units.abs() > 9_007_199_254_740_992.0 {
                Err("`bounds` times 10^scale must stay below 2^53 for the exact sampler")
            } else {
                Ok(units as i64)
            }
        };
        let (lo, hi) = (to_units(lo)?, to_units(hi)?);
        if lo >= hi {
            return Err("`bounds` must differ by at least one unit of `scale`".into());
        }

        let delta = (hi - lo) as u64;
        let needed = (delta as f64 / epsilon).ceil();
        if needed.is_nan() || needed > MAX_NOISE_PARAMETER as f64 {
            return Err(format!(
                "bounds, scale and epsilon need a noise parameter of {needed} units, above \
                 the exact sampler's limit of 2^50; use a larger epsilon or a smaller scale"
            ));
        }
        let mut t = (needed as u64).max(1);
        // ceil in f64 may land one below the exact value: make sure Δ/t <= ε.
        while (delta as f64) / (t as f64) > epsilon {
            t += 1;
        }
        Ok(Self { unit, lo, hi, t })
    }

    /// The ε this domain really spends: Δ / t, never above the configured ε.
    pub fn effective_epsilon(&self) -> f64 {
        (self.hi - self.lo) as f64 / self.t as f64
    }

    /// Clamps `value` to the bounds and converts it to units; values that
    /// are not numbers take the midpoint.
    pub fn to_units(&self, value: Option<f64>) -> i64 {
        match value {
            Some(v) => {
                let units = (v * self.unit as f64).round();
                (units.max(self.lo as f64).min(self.hi as f64)) as i64
            }
            None => self.lo + (self.hi - self.lo) / 2,
        }
    }

    pub fn noisy(&self, units: i64, clamp_output: bool, rng: &mut impl Rng) -> i64 {
        let noisy = units + discrete_laplace(self.t, rng);
        if clamp_output {
            noisy.clamp(self.lo, self.hi)
        } else {
            noisy
        }
    }

    /// Writes a number of units with the decimal point in place, without
    /// any floating-point step.
    pub fn format(&self, units: i64) -> String {
        let decimals = self.unit.ilog10() as usize;
        let sign = if units < 0 { "-" } else { "" };
        let magnitude = units.unsigned_abs();
        let unit = self.unit as u64;
        if decimals == 0 {
            format!("{sign}{magnitude}")
        } else {
            format!("{sign}{}.{:0decimals$}", magnitude / unit, magnitude % unit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::rngs::mock::StepRng;

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    fn mechanism(clamp_output: bool) -> LaplaceMechanism {
        LaplaceMechanism::new(1.0, 0.0, 100.0, 2, clamp_output)
    }

    fn apply(m: &LaplaceMechanism, value: SqlValue, rng: &mut StdRng) -> (SqlValue, MaskStats) {
        let mut stats = MaskStats::default();
        let out = m.apply(value, rng, &mut stats);
        (out, stats)
    }

    #[test]
    fn test_output_has_fixed_decimals() {
        let m = mechanism(false);
        let mut rng = rng();
        for input in ["5", "5.123456", "70.1"] {
            let (out, _) = apply(&m, SqlValue::Raw(input.into()), &mut rng);
            let text = out.as_str().unwrap().to_string();
            assert_eq!(text.split('.').nth(1).map(str::len), Some(2), "{text}");
        }
    }

    #[test]
    fn test_inputs_are_clamped_before_noise() {
        let m = mechanism(false);
        let (far, _) = apply(&m, SqlValue::Raw("1000000".into()), &mut rng());
        let (edge, _) = apply(&m, SqlValue::Raw("100".into()), &mut rng());
        assert_eq!(far, edge);
    }

    #[test]
    fn test_text_and_null_take_the_midpoint() {
        let m = mechanism(false);
        let (text, stats) = apply(&m, SqlValue::Text("Batman".into()), &mut rng());
        let (mid, _) = apply(&m, SqlValue::Text("50".into()), &mut rng());
        assert_eq!(text, mid);
        assert_eq!(stats.non_numeric, 1);

        let (null, stats) = apply(&m, SqlValue::Null, &mut rng());
        assert_eq!(null.as_str(), mid.as_str());
        assert_eq!(stats.non_numeric, 0);
    }

    #[test]
    fn test_literal_type_is_preserved() {
        let m = mechanism(false);
        let mut rng = rng();
        assert!(matches!(
            apply(&m, SqlValue::Raw("1".into()), &mut rng).0,
            SqlValue::Raw(_)
        ));
        assert!(matches!(
            apply(&m, SqlValue::Text("1".into()), &mut rng).0,
            SqlValue::Text(_)
        ));
    }

    #[test]
    fn test_clamp_output_keeps_values_inside_bounds() {
        let m = mechanism(true);
        let mut rng = rng();
        for _ in 0..2000 {
            let (out, _) = apply(&m, SqlValue::Raw("50".into()), &mut rng);
            let value: f64 = out.as_str().unwrap().parse().unwrap();
            assert!((0.0..=100.0).contains(&value), "{value}");
        }
    }

    #[test]
    fn test_noise_has_the_laplace_mean_and_variance() {
        let scale = 10.0;
        let mut rng = rng();
        let n = 20_000;
        let samples: Vec<f64> = (0..n).map(|_| laplace_noise(scale, &mut rng)).collect();
        let mean = samples.iter().sum::<f64>() / n as f64;
        let var = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        assert!(mean.abs() < 0.5, "mean {mean}");
        let expected = 2.0 * scale * scale;
        assert!((var - expected).abs() / expected < 0.1, "variance {var}");
    }

    #[test]
    fn test_noise_is_finite_at_the_ends_of_the_generator() {
        for step in [u64::MAX, 0] {
            let mut rng = StepRng::new(step, 0);
            assert!(laplace_noise(1.0, &mut rng).is_finite());
        }
    }

    #[test]
    fn test_negative_zero_is_written_as_zero() {
        assert_eq!(mechanism(false).format(-0.001), "0.00");
    }

    #[test]
    fn test_discrete_laplace_matches_its_distribution() {
        // P(z) = (1 - q) / (1 + q) * q^|z| with q = exp(-1/t).
        let t = 3;
        let q = (-1.0 / t as f64).exp();
        let n = 200_000;
        let mut rng = rng();
        let mut counts = std::collections::HashMap::new();
        for _ in 0..n {
            *counts
                .entry(discrete_laplace(t, &mut rng))
                .or_insert(0usize) += 1;
        }
        for z in -6i64..=6 {
            let expected = (1.0 - q) / (1.0 + q) * q.powi(z.unsigned_abs() as i32);
            let observed = counts.get(&z).copied().unwrap_or(0) as f64 / n as f64;
            let sd = (expected * (1.0 - expected) / n as f64).sqrt();
            assert!(
                (observed - expected).abs() < 5.0 * sd,
                "z = {z}: {observed} vs {expected}"
            );
        }
    }

    #[test]
    fn test_exact_domain_spends_at_most_epsilon() {
        for (epsilon, lo, hi, decimals) in [
            (1.0, 0.0, 100_000.0, 2),
            (0.5, 0.0, 10_000.0, 2),
            (0.3, 18.0, 99.0, 0),
            (2.0, 0.0, 1.0, 4),
            (0.7, -5.5, 5.5, 1),
        ] {
            let domain = ExactDomain::new(epsilon, lo, hi, decimals).unwrap();
            assert!(domain.effective_epsilon() <= epsilon, "{domain:?}");
            assert!(domain.effective_epsilon() > 0.99 * epsilon, "{domain:?}");
        }
    }

    #[test]
    fn test_exact_domain_rejects_what_it_cannot_represent() {
        assert!(ExactDomain::new(1.0, 0.0, 1e15, 4).is_err());
        assert!(ExactDomain::new(1e-12, 0.0, 1e6, 2).is_err());
        assert!(ExactDomain::new(1.0, 0.0, 0.001, 2).is_err());
    }

    #[test]
    fn test_exact_units_and_format() {
        let domain = ExactDomain::new(1.0, 0.0, 100.0, 2).unwrap();
        assert_eq!(domain.to_units(Some(12.345)), 1235);
        assert_eq!(domain.to_units(Some(1e9)), 10_000);
        assert_eq!(domain.to_units(Some(-3.0)), 0);
        assert_eq!(domain.to_units(None), 5_000);
        assert_eq!(domain.format(1235), "12.35");
        assert_eq!(domain.format(-5), "-0.05");
        assert_eq!(domain.format(0), "0.00");

        let integers = ExactDomain::new(1.0, 18.0, 99.0, 0).unwrap();
        assert_eq!(integers.format(-42), "-42");
    }

    #[test]
    fn test_exact_clamp_output() {
        let domain = ExactDomain::new(0.1, 0.0, 1.0, 2).unwrap();
        let mut rng = rng();
        for _ in 0..1000 {
            let out = domain.noisy(50, true, &mut rng);
            assert!((0..=100).contains(&out), "{out}");
        }
    }

    #[test]
    fn test_exact_mechanism_writes_integer_units() {
        let m = LaplaceMechanism::exact(1.0, 0.0, 100.0, 2, false).unwrap();
        let mut rng = rng();
        for input in ["5", "5.123456", "Batman"] {
            let (out, _) = apply(&m, SqlValue::Raw(input.into()), &mut rng);
            let text = out.as_str().unwrap().to_string();
            assert_eq!(text.split('.').nth(1).map(str::len), Some(2), "{text}");
        }
        assert!(m.epsilon() <= 1.0);
        assert!(m.describe().contains("exact integer sampler"));
    }

    #[test]
    fn test_exact_mechanism_clamps_before_noise() {
        let m = LaplaceMechanism::exact(1.0, 0.0, 100.0, 2, false).unwrap();
        let (far, _) = apply(&m, SqlValue::Raw("1000000".into()), &mut rng());
        let (edge, _) = apply(&m, SqlValue::Raw("100".into()), &mut rng());
        assert_eq!(far, edge);
    }
}

/// Statistical validation of both samplers. Slow (millions of samples), so
/// ignored by default: `cargo test --release -- --ignored`.
///
/// Each check runs on a correct sampler and on a deliberately broken one
/// (the negative control), so a check that cannot detect anything fails.
#[cfg(test)]
mod validation {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const SAMPLES: usize = 1_000_000;
    /// Standard normal quantile for a false alarm probability of 1e-4.
    const Z_1E4: f64 = 3.719;

    /// Upper 1e-4 quantile of a chi-square with `df` degrees of freedom
    /// (Wilson–Hilferty approximation).
    fn chi2_critical(df: usize) -> f64 {
        let df = df as f64;
        let h = 2.0 / (9.0 * df);
        df * (1.0 - h + Z_1E4 * h.sqrt()).powi(3)
    }

    fn chi2(observed: &[usize], expected: &[f64]) -> f64 {
        observed
            .iter()
            .zip(expected)
            .map(|(&o, &e)| (o as f64 - e).powi(2) / e)
            .sum()
    }

    /// Inverse CDF of Laplace(0, b).
    fn laplace_quantile(p: f64, b: f64) -> f64 {
        if p < 0.5 {
            b * (2.0 * p).ln()
        } else {
            -b * (2.0 * (1.0 - p)).ln()
        }
    }

    /// Chi-square over 100 equiprobable bins of Laplace(0, b). True when the
    /// samples fit.
    fn fits_laplace(samples: &[f64], b: f64) -> bool {
        let bins = 100;
        let edges: Vec<f64> = (1..bins)
            .map(|i| laplace_quantile(i as f64 / bins as f64, b))
            .collect();
        let mut observed = vec![0usize; bins];
        for &x in samples {
            observed[edges.partition_point(|&edge| edge <= x)] += 1;
        }
        let expected = vec![samples.len() as f64 / bins as f64; bins];
        chi2(&observed, &expected) < chi2_critical(bins - 1)
    }

    /// Largest normalized periodogram value of the residues modulo 4096.
    /// Under uniform residues each value is about Exp(1); a periodic
    /// artifact shows as a peak.
    fn periodogram_peak(values: impl Iterator<Item = i64>) -> f64 {
        const N: usize = 4096;
        let mut counts = vec![0f64; N];
        let mut n = 0usize;
        for value in values {
            counts[value.rem_euclid(N as i64) as usize] += 1.0;
            n += 1;
        }
        let mean = n as f64 / N as f64;
        let mut peak = 0f64;
        for k in 1..N / 2 {
            let (mut re, mut im) = (0f64, 0f64);
            for (j, &c) in counts.iter().enumerate() {
                let angle = -2.0 * std::f64::consts::PI * (j * k % N) as f64 / N as f64;
                re += (c - mean) * angle.cos();
                im += (c - mean) * angle.sin();
            }
            peak = peak.max((re * re + im * im) / (N as f64 * mean));
        }
        peak
    }

    /// The periodogram peak above which residues are not uniform: the
    /// largest of 2047 Exp(1) values passes it with probability 1e-4.
    fn periodogram_threshold() -> f64 {
        (2047.0f64 / 1e-4).ln()
    }

    fn rng() -> StdRng {
        StdRng::seed_from_u64(20261106)
    }

    #[test]
    #[ignore = "slow: run with cargo test --release -- --ignored"]
    fn exact_sampler_matches_the_discrete_laplace_pmf() {
        let t = 3u64;
        let q = (-1.0 / t as f64).exp();
        let pmf = |z: i64| (1.0 - q) / (1.0 + q) * q.powi(z.unsigned_abs() as i32);
        let mut rng = rng();

        // Bins -15..=15, plus one bin for each tail.
        let check = |sample: &mut dyn FnMut() -> i64| -> bool {
            let mut observed = vec![0usize; 33];
            for _ in 0..SAMPLES {
                let z = sample();
                observed[(z.clamp(-16, 16) + 16) as usize] += 1;
            }
            let tail: f64 = q.powi(16) / (1.0 + q);
            let expected: Vec<f64> = (-16i64..=16)
                .map(|z| {
                    let p = if z.abs() == 16 { tail } else { pmf(z) };
                    p * SAMPLES as f64
                })
                .collect();
            chi2(&observed, &expected) < chi2_critical(32)
        };

        assert!(check(&mut || discrete_laplace(t, &mut rng)));
        // Negative control: parameter 4 instead of 3.
        assert!(!check(&mut || discrete_laplace(t + 1, &mut rng)));
    }

    #[test]
    #[ignore = "slow: run with cargo test --release -- --ignored"]
    fn both_samplers_fit_the_laplace_distribution() {
        let b = 10_000.0;
        let mut rng = rng();

        let exact: Vec<f64> = (0..SAMPLES)
            .map(|_| discrete_laplace(b as u64, &mut rng) as f64)
            .collect();
        assert!(fits_laplace(&exact, b), "exact sampler");

        let float: Vec<f64> = (0..SAMPLES).map(|_| laplace_noise(b, &mut rng)).collect();
        assert!(fits_laplace(&float, b), "f64 sampler");

        // Negative control: noise 5% wider than declared.
        let wide: Vec<f64> = (0..SAMPLES)
            .map(|_| laplace_noise(b * 1.05, &mut rng))
            .collect();
        assert!(!fits_laplace(&wide, b), "wider noise was not detected");
    }

    #[test]
    #[ignore = "slow: run with cargo test --release -- --ignored"]
    fn no_sampler_shows_periodic_artifacts() {
        let t = 10_000_000u64;
        let threshold = periodogram_threshold();
        let mut rng = rng();

        let exact = periodogram_peak((0..SAMPLES).map(|_| discrete_laplace(t, &mut rng)));
        assert!(exact < threshold, "exact sampler peak {exact}");

        let float = periodogram_peak(
            (0..SAMPLES).map(|_| laplace_noise(t as f64, &mut rng).round() as i64),
        );
        assert!(float < threshold, "f64 sampler peak {float}");

        // Negative control: values forced to multiples of 8.
        let broken = periodogram_peak((0..SAMPLES).map(|_| discrete_laplace(t, &mut rng) & !7));
        assert!(broken > threshold, "period 8 was not detected ({broken})");
    }
}
