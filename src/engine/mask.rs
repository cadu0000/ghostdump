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
use std::collections::BTreeMap;
use uuid::Builder;

use crate::codec::SqlValue;
use crate::engine::categorical::RandomizedResponse;
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
    Ldp {
        /// The ε of one value.
        epsilon: f64,
    },
}

/// Counters that a run reports instead of logging one line per value.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct MaskStats {
    /// Values of `dp_laplace` columns that were not numbers.
    pub non_numeric: usize,
    /// Fields that went through a rule (NULLs kept by `null_policy` included).
    pub masked_fields: usize,
    /// Values of `randomized_response` columns outside their categories.
    pub outside_categories: usize,
    /// `non_numeric` and `outside_categories` by rule (see
    /// [`CompiledColumn::id`](crate::engine::config::CompiledColumn::id)).
    pub by_rule: BTreeMap<usize, RuleStats>,
}

/// The values of one rule that its strategy could not take as they were.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RuleStats {
    /// Values that `dp_laplace` could not read as numbers.
    pub non_numeric: usize,
    /// Values outside the categories of `randomized_response`.
    pub outside_categories: usize,
}

impl MaskStats {
    /// Adds the counters of another part of the run.
    pub fn add(&mut self, other: &MaskStats) {
        self.non_numeric += other.non_numeric;
        self.masked_fields += other.masked_fields;
        self.outside_categories += other.outside_categories;
        for (rule, stats) in &other.by_rule {
            let sum = self.by_rule.entry(*rule).or_default();
            sum.non_numeric += stats.non_numeric;
            sum.outside_categories += stats.outside_categories;
        }
    }

    /// A value of a `dp_laplace` rule that is not a number.
    pub fn count_non_numeric(&mut self, rule: usize) {
        self.non_numeric += 1;
        self.by_rule.entry(rule).or_default().non_numeric += 1;
    }

    /// A value of a `randomized_response` rule outside its categories.
    pub fn count_outside_category(&mut self, rule: usize) {
        self.outside_categories += 1;
        self.by_rule.entry(rule).or_default().outside_categories += 1;
    }
}

/// How a rule masks the values of its column.
#[derive(Debug, Clone)]
pub enum Masker {
    /// Replaces the value with a generated one.
    Synthetic(Synthetic),
    /// Replaces the value with a pseudonym, its HMAC-SHA256 under the secret
    /// (`hmac`).
    Hmac,
    /// Replaces the value with NULL (`nullify`).
    Nullify,
    /// Adds Laplace noise to a number (`dp_laplace`).
    Laplace(LaplaceMechanism),
    /// Keeps the category, or replaces it with another one at random
    /// (`randomized_response`).
    RandomizedResponse(RandomizedResponse),
}

/// Generators whose output does not depend on the original value.
#[derive(Debug, Clone)]
pub enum Synthetic {
    /// A person's name (`faker_name`).
    FakerName,
    /// An e-mail address (`faker_email`).
    FakerEmail,
    /// An IPv4 address (`faker_ip`).
    Ipv4,
    /// A version 4 UUID (`random_uuid`).
    Uuid,
    /// Sixteen random digits in groups of four (`fake_credit_card`).
    CreditCard,
    /// A Brazilian mobile number (`faker_phone_br`).
    PhoneBr,
    /// A CPF with valid check digits (`cpf`).
    Cpf,
    /// A CNPJ with valid check digits (`cnpj`).
    Cnpj,
    /// The same text for every value (`fixed`).
    Fixed(String),
    /// One of the given options, at random (`random_choice`).
    Choice(Vec<String>),
    /// Random ASCII letters and digits, as many as given (`random_string`).
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
            Masker::Synthetic(kind) => SqlValue::Text(kind.generate(rng).into_bytes()),
            Masker::Nullify => SqlValue::Null,
            // A pseudonym of NULL would link every NULL to each other.
            Masker::Hmac => match value.as_bytes() {
                Some(bytes) => SqlValue::Text(hmac_hash(bytes, secret).into_bytes()),
                None => SqlValue::Null,
            },
            Masker::Laplace(mechanism) => mechanism.apply(value, rng, secret, stats),
            Masker::RandomizedResponse(rr) => rr.apply(value, rng, stats),
        }
    }

    /// What the masker protects, for the summary printed at startup.
    pub fn guarantee(&self) -> Guarantee {
        match self {
            Masker::Synthetic(_) | Masker::Nullify => Guarantee::InputIndependent,
            Masker::Hmac => Guarantee::Pseudonymization,
            Masker::Laplace(m) => Guarantee::Ldp {
                epsilon: m.epsilon(),
            },
            Masker::RandomizedResponse(rr) => Guarantee::Ldp {
                epsilon: rr.epsilon(),
            },
        }
    }

    /// Whether the output depends on the secret: `hmac`, and `dp_laplace`
    /// with consistent noise.
    pub fn uses_secret(&self) -> bool {
        match self {
            Masker::Hmac => true,
            Masker::Laplace(m) => m.is_consistent(),
            _ => false,
        }
    }

    /// Whether the masker draws Laplace noise with the `f64` sampler.
    pub fn uses_f64_sampler(&self) -> bool {
        matches!(self, Masker::Laplace(m) if m.is_f64())
    }

    /// Names the column the masker belongs to (used by consistent noise).
    pub fn bind(&mut self, table: &str, column: &str) {
        if let Masker::Laplace(m) = self {
            m.bind(table, column);
        }
    }

    /// Names the rule whose values the masker counts in `MaskStats`.
    pub fn set_rule(&mut self, rule: usize) {
        match self {
            Masker::Laplace(m) => m.set_rule(rule),
            Masker::RandomizedResponse(rr) => rr.set_rule(rule),
            Masker::Synthetic(_) | Masker::Nullify | Masker::Hmac => {}
        }
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
            Masker::RandomizedResponse(rr) => rr.describe(),
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

/// Sixteen random digits in four groups joined by hyphens; the last digit
/// is not computed as a check digit.
pub fn generate_fake_credit_card(rng: &mut impl Rng) -> String {
    format!(
        "{:04}-{:04}-{:04}-{:04}",
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999),
        rng.gen_range(0..=9999)
    )
}

/// A Brazilian mobile number, `(NN) 9NNNN-NNNN`, with an area code drawn
/// from 11 to 99.
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

/// HMAC-SHA256 of the bytes of a value, in hex. The bytes are hashed as the
/// dump has them, so values that differ in any byte get different pseudonyms.
pub fn hmac_hash(value: &[u8], secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(value);
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
        assert_eq!(hmac_hash(b"123", "k"), hmac_hash(b"123", "k"));
        assert_ne!(hmac_hash(b"123", "k"), hmac_hash(b"123", "other"));
        assert_eq!(hmac_hash(b"123", "k").len(), 64);
    }

    #[test]
    fn test_hmac_hashes_the_bytes_of_the_value() {
        let pseudonym = |bytes: &[u8]| {
            let mut stats = MaskStats::default();
            Masker::Hmac.apply(SqlValue::Text(bytes.to_vec()), &mut rng(), "k", &mut stats)
        };
        // Latin-1 values that differ in one byte outside ASCII, and binary
        // values, keep different pseudonyms.
        assert_ne!(pseudonym(b"Jos\xe9"), pseudonym(b"Jos\xe8"));
        assert_ne!(
            pseudonym(&[0xe9, 0xff, 0, 1]),
            pseudonym(&[0xe8, 0xff, 0, 1])
        );
        assert_eq!(
            pseudonym("José".as_bytes()),
            SqlValue::Text(hmac_hash("José".as_bytes(), "k").into_bytes())
        );
    }
}
