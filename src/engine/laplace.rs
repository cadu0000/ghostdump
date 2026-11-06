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
/// The sampler uses `f64` arithmetic, which is vulnerable to floating-point
/// attacks (Mironov, 2012); the guarantee assumes exact arithmetic.
#[derive(Debug, Clone)]
pub struct LaplaceMechanism {
    epsilon: f64,
    lo: f64,
    hi: f64,
    noise_scale: f64,
    decimals: usize,
    clamp_output: bool,
}

impl LaplaceMechanism {
    pub fn new(epsilon: f64, lo: f64, hi: f64, decimals: usize, clamp_output: bool) -> Self {
        Self {
            epsilon,
            lo,
            hi,
            noise_scale: (hi - lo) / epsilon,
            decimals,
            clamp_output,
        }
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

        let text = self.format(self.noisy(parsed, rng));
        // A number stays unquoted when it came unquoted.
        match value {
            SqlValue::Text(_) => SqlValue::Text(text),
            SqlValue::Raw(_) | SqlValue::Null => SqlValue::Raw(text),
        }
    }

    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }

    /// One line for the debug log and the guarantees table.
    pub fn describe(&self) -> String {
        format!(
            "Laplace noise, ε-LDP per value with ε = {}, bounds [{}, {}], scale {} \
             (f64 sampler: assumes exact arithmetic)",
            self.epsilon, self.lo, self.hi, self.decimals
        )
    }

    /// `None` stands for a value that is not a number.
    fn noisy(&self, value: Option<f64>, rng: &mut impl Rng) -> f64 {
        let clamped = value.map_or((self.lo + self.hi) / 2.0, |v| v.clamp(self.lo, self.hi));
        let noisy = clamped + laplace_noise(self.noise_scale, rng);
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
fn laplace_noise(scale: f64, rng: &mut impl Rng) -> f64 {
    let magnitude = -(1.0 - rng.r#gen::<f64>()).ln() * scale;
    if rng.r#gen::<bool>() {
        magnitude
    } else {
        -magnitude
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
}
