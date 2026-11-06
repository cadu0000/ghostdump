//! Maskers compiled from the rules file: validated, with their parameters
//! precomputed, and able to say which guarantee they offer.

use cpf_cnpj::{cnpj, cpf};
use fake::Fake;
use fake::faker::internet::en::{IPv4, SafeEmail};
use fake::faker::name::en::Name;
use hmac::{Hmac, Mac};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use sha2::Sha256;
use uuid::Uuid;

use crate::codec::SqlValue;

type HmacSha256 = Hmac<Sha256>;

/// What a masker protects, in the terms of the guarantees table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Guarantee {
    /// The output does not depend on the input (0-LDP).
    InputIndependent,
    /// Deterministic keyed hash: linkable and not differentially private.
    Pseudonymization,
    /// ε-LDP per value, assuming exact arithmetic (f64 sampler).
    Ldp { epsilon: f64 },
}

/// Counters that a run reports instead of logging one line per value.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct MaskStats {
    /// Values of `dp_laplace` columns that were not numbers.
    pub non_numeric: usize,
}

#[derive(Debug, Clone)]
pub enum Masker {
    Synthetic(Synthetic),
    Hmac,
    Nullify,
    Laplace(LaplaceMechanism),
}

/// Generators whose output does not depend on the original value.
#[derive(Debug, Clone)]
pub enum Synthetic {
    FakerName,
    FakerEmail,
    Ipv4,
    Uuid,
    CreditCard,
    PhoneBr,
    Cpf,
    Cnpj,
    Fixed(String),
    Choice(Vec<String>),
    Alphanumeric(usize),
}

impl Masker {
    /// Masks one value. NULL inputs reach this function only when the
    /// column's `null_policy` is `generate`.
    pub fn apply(
        &self,
        value: SqlValue,
        rng: &mut impl Rng,
        secret: &str,
        stats: &mut MaskStats,
    ) -> SqlValue {
        match self {
            Masker::Synthetic(kind) => SqlValue::Text(kind.generate(rng)),
            Masker::Nullify => SqlValue::Null,
            // A pseudonym of NULL would link every NULL to each other.
            Masker::Hmac => match value.as_str() {
                Some(text) => SqlValue::Text(hmac_hash(text, secret)),
                None => SqlValue::Null,
            },
            Masker::Laplace(mechanism) => mechanism.apply(value, rng, stats),
        }
    }

    pub fn guarantee(&self) -> Guarantee {
        match self {
            Masker::Synthetic(_) | Masker::Nullify => Guarantee::InputIndependent,
            Masker::Hmac => Guarantee::Pseudonymization,
            Masker::Laplace(m) => Guarantee::Ldp { epsilon: m.epsilon },
        }
    }

    pub fn uses_secret(&self) -> bool {
        matches!(self, Masker::Hmac)
    }

    /// One line for the debug log and the guarantees table.
    pub fn describe(&self) -> String {
        match self {
            Masker::Synthetic(_) => "replaced by values independent of the original (0-LDP)".into(),
            Masker::Nullify => "replaced by NULL (0-LDP)".into(),
            Masker::Hmac => {
                "pseudonymized with HMAC-SHA256 (deterministic, not differentially private)".into()
            }
            Masker::Laplace(m) => format!(
                "Laplace noise, ε-LDP per value with ε = {}, bounds [{}, {}], scale {} \
                 (f64 sampler: assumes exact arithmetic)",
                m.epsilon, m.lo, m.hi, m.decimals
            ),
        }
    }
}

impl Synthetic {
    fn generate(&self, rng: &mut impl Rng) -> String {
        match self {
            Synthetic::FakerName => Name().fake::<String>(),
            Synthetic::FakerEmail => SafeEmail().fake::<String>(),
            Synthetic::Ipv4 => IPv4().fake::<String>(),
            Synthetic::Uuid => Uuid::new_v4().to_string(),
            Synthetic::CreditCard => generate_fake_credit_card(rng),
            Synthetic::PhoneBr => generate_fake_phone_br(rng),
            Synthetic::Cpf => cpf::generate(),
            Synthetic::Cnpj => cnpj::generate(),
            Synthetic::Fixed(value) => value.clone(),
            Synthetic::Choice(options) => options.choose(rng).cloned().unwrap_or_default(),
            Synthetic::Alphanumeric(length) => rng
                .sample_iter(&Alphanumeric)
                .take(*length)
                .map(char::from)
                .collect(),
        }
    }
}

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

    fn apply(&self, value: SqlValue, rng: &mut impl Rng, stats: &mut MaskStats) -> SqlValue {
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

pub fn generate_fake_credit_card(rng: &mut impl Rng) -> String {
    format!(
        "{:04}-{:04}-{:04}-{:04}",
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999)
    )
}

pub fn generate_fake_phone_br(rng: &mut impl Rng) -> String {
    format!(
        "({:02}) 9{:04}-{:04}",
        rng.gen_range(11..=99),
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999)
    )
}

pub fn hmac_hash(val: &str, secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(val.as_bytes());
    hex::encode(mac.finalize().into_bytes())
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

    fn generate(kind: Synthetic) -> String {
        kind.generate(&mut rng())
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
    fn test_guarantees() {
        assert_eq!(Masker::Nullify.guarantee(), Guarantee::InputIndependent);
        assert_eq!(Masker::Hmac.guarantee(), Guarantee::Pseudonymization);
        assert_eq!(
            Masker::Laplace(mechanism(false)).guarantee(),
            Guarantee::Ldp { epsilon: 1.0 }
        );
    }

    #[test]
    fn test_synthetic_generators() {
        assert!(Uuid::parse_str(&generate(Synthetic::Uuid)).is_ok());
        assert_eq!(generate(Synthetic::Cpf).len(), 11);
        assert_eq!(generate(Synthetic::Cnpj).len(), 14);
        assert_eq!(generate(Synthetic::CreditCard).matches('-').count(), 3);
        assert!(generate(Synthetic::PhoneBr).starts_with('('));
        assert_eq!(generate(Synthetic::Alphanumeric(12)).len(), 12);
        assert_eq!(generate(Synthetic::Fixed("X".into())), "X");
        let choice = generate(Synthetic::Choice(vec!["A".into(), "B".into()]));
        assert!(choice == "A" || choice == "B");
    }

    #[test]
    fn test_hmac_is_deterministic_and_keyed() {
        assert_eq!(hmac_hash("123", "k"), hmac_hash("123", "k"));
        assert_ne!(hmac_hash("123", "k"), hmac_hash("123", "other"));
        assert_eq!(hmac_hash("123", "k").len(), 64);
    }
}
