//! Maskers compiled from the rules file: validated, with their parameters
//! precomputed, and able to say which guarantee they offer.

use fake::Fake;
use fake::faker::internet::en::{IPv4, SafeEmail};
use fake::faker::name::en::Name;
use hmac::{Hmac, Mac};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use sha2::Sha256;
use uuid::Builder;

use crate::codec::SqlValue;
use crate::engine::laplace::LaplaceMechanism;

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
            Masker::Laplace(m) => Guarantee::Ldp {
                epsilon: m.epsilon(),
            },
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
            Masker::Laplace(m) => m.describe(),
        }
    }
}

impl Synthetic {
    /// Every generator draws from `rng`, so a seeded run is reproducible.
    fn generate(&self, rng: &mut impl Rng) -> String {
        match self {
            Synthetic::FakerName => Name().fake_with_rng::<String, _>(rng),
            Synthetic::FakerEmail => SafeEmail().fake_with_rng::<String, _>(rng),
            Synthetic::Ipv4 => IPv4().fake_with_rng::<String, _>(rng),
            // Version 4 and the RFC 4122 variant are set over 16 random bytes.
            Synthetic::Uuid => Builder::from_random_bytes(rng.r#gen())
                .into_uuid()
                .to_string(),
            Synthetic::CreditCard => generate_fake_credit_card(rng),
            Synthetic::PhoneBr => generate_fake_phone_br(rng),
            Synthetic::Cpf => generate_cpf(rng),
            Synthetic::Cnpj => generate_cnpj(rng),
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

/// A random valid CPF: nine digits and two check digits (mod 11).
pub fn generate_cpf(rng: &mut impl Rng) -> String {
    loop {
        let mut digits: Vec<u32> = (0..9).map(|_| rng.gen_range(0..10)).collect();
        // Nine equal digits pass the check digits but are not valid CPFs.
        if digits.iter().all(|&digit| digit == digits[0]) {
            continue;
        }
        for len in [9, 10] {
            // Weights 10..2 for the first check digit, 11..2 for the second.
            let sum: u32 = digits
                .iter()
                .zip((2..=len + 1).rev())
                .map(|(digit, weight)| digit * weight)
                .sum();
            digits.push(check_digit(sum));
        }
        return to_text(&digits);
    }
}

/// A random valid CNPJ: twelve digits and two check digits (mod 11).
pub fn generate_cnpj(rng: &mut impl Rng) -> String {
    loop {
        let mut digits: Vec<u32> = (0..12).map(|_| rng.gen_range(0..10)).collect();
        if digits.iter().all(|&digit| digit == digits[0]) {
            continue;
        }
        for _ in 0..2 {
            // Weights 2..9, repeated, from the rightmost digit.
            let sum: u32 = digits
                .iter()
                .rev()
                .zip((2..=9).cycle())
                .map(|(digit, weight)| digit * weight)
                .sum();
            digits.push(check_digit(sum));
        }
        return to_text(&digits);
    }
}

fn check_digit(sum: u32) -> u32 {
    match sum % 11 {
        0 | 1 => 0,
        rest => 11 - rest,
    }
}

fn to_text(digits: &[u32]) -> String {
    digits
        .iter()
        .map(|&digit| char::from_digit(digit, 10).expect("a decimal digit"))
        .collect()
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

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    fn generate(kind: Synthetic) -> String {
        kind.generate(&mut rng())
    }

    #[test]
    fn test_guarantees() {
        assert_eq!(Masker::Nullify.guarantee(), Guarantee::InputIndependent);
        assert_eq!(Masker::Hmac.guarantee(), Guarantee::Pseudonymization);
        assert_eq!(
            Masker::Laplace(LaplaceMechanism::new(1.0, 0.0, 100.0, 2, false)).guarantee(),
            Guarantee::Ldp { epsilon: 1.0 }
        );
    }

    #[test]
    fn test_synthetic_generators() {
        let uuid = uuid::Uuid::parse_str(&generate(Synthetic::Uuid)).unwrap();
        assert_eq!(uuid.get_version_num(), 4);
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
    fn test_generators_are_reproducible_with_a_seed() {
        for kind in [
            Synthetic::FakerName,
            Synthetic::FakerEmail,
            Synthetic::Ipv4,
            Synthetic::Uuid,
            Synthetic::Cpf,
            Synthetic::Cnpj,
        ] {
            assert_eq!(generate(kind.clone()), generate(kind));
        }
    }

    #[test]
    fn test_generated_documents_are_valid() {
        let mut rng = rng();
        for _ in 0..200 {
            let cpf = generate_cpf(&mut rng);
            assert!(cpf_cnpj::cpf::validate(&cpf), "{cpf}");
            let cnpj = generate_cnpj(&mut rng);
            assert!(cpf_cnpj::cnpj::validate(&cnpj), "{cnpj}");
        }
    }

    #[test]
    fn test_hmac_is_deterministic_and_keyed() {
        assert_eq!(hmac_hash("123", "k"), hmac_hash("123", "k"));
        assert_ne!(hmac_hash("123", "k"), hmac_hash("123", "other"));
        assert_eq!(hmac_hash("123", "k").len(), 64);
    }
}
