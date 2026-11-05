use cpf_cnpj::{cnpj, cpf};
use fake::Fake;
use fake::faker::internet::en::{IPv4, SafeEmail};
use fake::faker::name::en::Name;
use hmac::{Hmac, Mac};
use log::warn;
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use serde::Deserialize;
use sha2::Sha256;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

use crate::codec::SqlValue;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum StrategyConfig {
    Hmac,
    DpLaplace { epsilon: f64, sensitivity: f64 },
    FakerName,
    FakerEmail,
    FakeCreditCard,
    FakerPhoneBr,
    FakerIp,
    RandomUuid,
    Cpf,
    Cnpj,
    Nullify,
    Fixed { value: String },
    RandomChoice { options: Vec<String> },
    RandomString { length: usize },
}

impl StrategyConfig {
    /// Masks one value. NULL inputs reach this function only when the
    /// column's `null_policy` is `generate`; strategies that derive their
    /// output from the input keep NULL as NULL.
    pub fn apply(&self, value: SqlValue, rng: &mut impl Rng, secret: &str) -> SqlValue {
        match self {
            StrategyConfig::FakerName => SqlValue::Text(Name().fake::<String>()),
            StrategyConfig::FakerEmail => SqlValue::Text(SafeEmail().fake::<String>()),
            StrategyConfig::FakerIp => SqlValue::Text(IPv4().fake::<String>()),
            StrategyConfig::RandomUuid => SqlValue::Text(Uuid::new_v4().to_string()),
            StrategyConfig::FakeCreditCard => SqlValue::Text(generate_fake_credit_card(rng)),
            StrategyConfig::FakerPhoneBr => SqlValue::Text(generate_fake_phone_br(rng)),
            StrategyConfig::Cpf => SqlValue::Text(cpf::generate()),
            StrategyConfig::Cnpj => SqlValue::Text(cnpj::generate()),
            StrategyConfig::Fixed { value } => SqlValue::Text(value.clone()),
            StrategyConfig::Nullify => SqlValue::Null,
            StrategyConfig::RandomChoice { options } => {
                SqlValue::Text(options.choose(rng).cloned().unwrap_or_default())
            }
            // A pseudonym of NULL would link every NULL to each other.
            StrategyConfig::Hmac => match value.as_str() {
                Some(text) => SqlValue::Text(hmac_hash(text, secret)),
                None => SqlValue::Null,
            },
            // A number stays unquoted when it came unquoted.
            StrategyConfig::DpLaplace {
                epsilon,
                sensitivity,
            } => match value {
                SqlValue::Null => SqlValue::Null,
                SqlValue::Raw(text) => {
                    SqlValue::Raw(dp_laplace(&text, *epsilon, *sensitivity, rng))
                }
                SqlValue::Text(text) => {
                    SqlValue::Text(dp_laplace(&text, *epsilon, *sensitivity, rng))
                }
            },
            StrategyConfig::RandomString { length } => SqlValue::Text(
                rng.sample_iter(&Alphanumeric)
                    .take(*length)
                    .map(char::from)
                    .collect(),
            ),
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

pub fn hmac_hash(val: &str, secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC aceita qualquer tamanho");

    mac.update(val.as_bytes());
    let result = mac.finalize();

    hex::encode(result.into_bytes())
}

static INVALID_PARAMETERS: AtomicBool = AtomicBool::new(false);
static NOT_A_NUMBER: AtomicBool = AtomicBool::new(false);

/// Runs `log` the first time only, so a warning appears once per run
/// instead of once per value.
fn warn_once(flag: &AtomicBool, log: impl FnOnce()) {
    if !flag.swap(true, Ordering::Relaxed) {
        log();
    }
}

/// Applies the Laplace Mechanism for Differential Privacy.
///
/// WARNING (SECURITY DEBT):
/// This is a naive implementation based on Inverse Transform Sampling using `f64`.
/// For production environments requiring absolute cryptographic rigor, the standard
/// use of `f64` is vulnerable to floating-point attacks (Mironov, 2012).
///
/// FUTURE ARCHITECTURE NOTES:
/// We explicitly avoid using Google's DP C++ library bindings here to prevent
/// heavy FFI overhead and maintain strict single-pass streaming performance.
/// In future versions (1.0), a native Rust implementation of the Snapping
/// Mechanism should be developed to replace this function.
pub fn dp_laplace(value: &str, epsilon: f64, sensitivity: f64, rng: &mut impl Rng) -> String {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("NULL") || trimmed.is_empty() {
        return value.to_string();
    }

    if epsilon <= 0.0 || sensitivity <= 0.0 {
        warn_once(&INVALID_PARAMETERS, || {
            warn!(
                "dp_laplace: invalid parameters (epsilon: {epsilon}, sensitivity: {sensitivity}); \
                 values of this column are written unchanged"
            )
        });
        return value.to_string();
    }
    
    let decimal_places = trimmed
        .split_once('.')
        .map(|(_, frac)| frac.len())
        .unwrap_or(0);

    let parsed_value: f64 = match trimmed.parse() {
        Ok(v) => v,
        Err(_) => {
            // Never log the value itself: logs are an output channel too.
            warn_once(&NOT_A_NUMBER, || {
                warn!("dp_laplace: a value is not a number and was written unchanged")
            });
            return value.to_string();
        }
    };

    let b = sensitivity / epsilon;
    let u: f64 = rng.gen_range(-0.5..0.5);

    let sign = u.signum();
    let noise = -b * sign * (1.0 - 2.0 * u.abs()).ln();

    let noised_value = parsed_value + noise;

    format!("{:.*}", decimal_places, noised_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    fn seeded_rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    fn text(value: &str) -> SqlValue {
        SqlValue::Text(value.into())
    }

    #[test]
    fn test_null_is_kept_by_value_derived_strategies() {
        let mut rng = seeded_rng();
        let dp = StrategyConfig::DpLaplace {
            epsilon: 1.0,
            sensitivity: 1.0,
        };
        assert_eq!(StrategyConfig::Hmac.apply(SqlValue::Null, &mut rng, "s"), SqlValue::Null);
        assert_eq!(dp.apply(SqlValue::Null, &mut rng, "s"), SqlValue::Null);
    }

    #[test]
    fn test_literal_type_is_preserved() {
        let mut rng = seeded_rng();
        let dp = StrategyConfig::DpLaplace {
            epsilon: 1.0,
            sensitivity: 1.0,
        };
        assert!(matches!(dp.apply(SqlValue::Raw("10".into()), &mut rng, "s"), SqlValue::Raw(_)));
        assert!(matches!(dp.apply(text("10"), &mut rng, "s"), SqlValue::Text(_)));
    }

    #[test]
    fn test_hmac_ignores_the_literal_form() {
        let mut rng = seeded_rng();
        let raw = StrategyConfig::Hmac.apply(SqlValue::Raw("42".into()), &mut rng, "s");
        let quoted = StrategyConfig::Hmac.apply(text("42"), &mut rng, "s");
        assert_eq!(raw, quoted);
    }

    #[test]
    fn test_all_strategies_do_not_panic() {
        let mut rng = seeded_rng();

        let strategies = vec![
            StrategyConfig::FakerName,
            StrategyConfig::FakerEmail,
            StrategyConfig::FakerIp,
            StrategyConfig::RandomUuid,
            StrategyConfig::FakeCreditCard,
            StrategyConfig::FakerPhoneBr,
            StrategyConfig::Cpf,
            StrategyConfig::Cnpj,
            StrategyConfig::Nullify,
            StrategyConfig::Fixed {
                value: "STABLE".into(),
            },
            StrategyConfig::RandomChoice {
                options: vec!["A".into(), "B".into()],
            },
            StrategyConfig::RandomString { length: 8 },
            StrategyConfig::Hmac,
            StrategyConfig::DpLaplace {
                epsilon: 1.0,
                sensitivity: 100.0,
            },
        ];

        for strategy in strategies {
            let _ = strategy.apply(text("12345"), &mut rng, "secret");
        }
    }

    #[test]
    fn test_nullify_strategy() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Nullify.apply(text("123"), &mut rng, "secret");
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_fixed_strategy() {
        let mut rng = seeded_rng();
        let strategy = StrategyConfig::Fixed {
            value: "CONST".into(),
        };
        let result = strategy.apply(text("123"), &mut rng, "secret");
        assert_eq!(result, text("CONST"));
    }

    #[test]
    fn test_random_string_length() {
        let mut rng = seeded_rng();
        let strategy = StrategyConfig::RandomString { length: 12 };
        let result = strategy.apply(text("abc"), &mut rng, "secret");
        assert_eq!(result.as_str().unwrap().len(), 12);
    }

    #[test]
    fn test_random_choice_returns_valid_option() {
        let mut rng = seeded_rng();
        let options = vec!["A".into(), "B".into()];
        let strategy = StrategyConfig::RandomChoice {
            options: options.clone(),
        };

        let result = strategy.apply(text("abc"), &mut rng, "secret");
        assert!(options.iter().any(|o| Some(o.as_str()) == result.as_str()));
    }

    #[test]
    fn test_uuid_strategy_valid_format() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::RandomUuid
            .apply(text("abc"), &mut rng, "secret");

        assert!(uuid::Uuid::parse_str(result.as_str().unwrap()).is_ok());
    }

    #[test]
    fn test_cpf_strategy_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Cpf
            .apply(text("abc"), &mut rng, "secret");

        let result = result.as_str().unwrap();
        assert_eq!(result.len(), 11);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_cnpj_strategy_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Cnpj
            .apply(text("abc"), &mut rng, "secret");

        let result = result.as_str().unwrap();
        assert_eq!(result.len(), 14);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_credit_card_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::FakeCreditCard
            .apply(text("abc"), &mut rng, "secret");

        let result = result.as_str().unwrap();
        assert_eq!(result.len(), 19);
        assert_eq!(result.matches('-').count(), 3);
    }

    #[test]
    fn test_phone_br_format() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::FakerPhoneBr
            .apply(text("abc"), &mut rng, "secret");

        assert!(result.as_str().unwrap().starts_with('('));
    }

    #[test]
    fn test_hmac_is_deterministic_for_same_input() {
        let mut rng1 = seeded_rng();
        let mut rng2 = seeded_rng();

        let strategy = StrategyConfig::Hmac;

        let r1 = strategy.apply(text("123"), &mut rng1, "secret");
        let r2 = strategy.apply(text("123"), &mut rng2, "secret");

        assert_eq!(r1, r2);
    }

    #[test]
    fn test_dp_laplace_returns_value() {
        let mut rng = seeded_rng();

        let strategy = StrategyConfig::DpLaplace {
            epsilon: 1.0,
            sensitivity: 100.0,
        };

        let result = strategy.apply(SqlValue::Raw("5000".into()), &mut rng, "secret");

        assert!(!result.as_str().unwrap().is_empty());
    }
    #[test]
    fn test_dp_laplace_valid_number() {
        let mut rng = StdRng::seed_from_u64(42);

        let original = "1000.00";
        let epsilon = 1.0;
        let sensitivity = 10.0;

        let result = dp_laplace(original, epsilon, sensitivity, &mut rng);

        assert_ne!(result, "1000.00");
        assert!(result.contains('.'));
    }

    #[test]
    fn test_dp_laplace_null_handling() {
        let mut rng = StdRng::seed_from_u64(42);

        assert_eq!(dp_laplace("NULL", 1.0, 10.0, &mut rng), "NULL");
        assert_eq!(dp_laplace("null", 1.0, 10.0, &mut rng), "null");
        assert_eq!(dp_laplace("  NULL  ", 1.0, 10.0, &mut rng), "  NULL  ");
    }

    #[test]
    fn test_dp_laplace_invalid_string() {
        let mut rng = StdRng::seed_from_u64(42);

        assert_eq!(dp_laplace("Batman", 1.0, 10.0, &mut rng), "Batman");
    }

    #[test]
    fn test_dp_laplace_zero_epsilon() {
        let mut rng = StdRng::seed_from_u64(42);

        let result = dp_laplace("1000", 0.0, 10.0, &mut rng);

        assert_eq!(result, "1000");
    }

    #[test]
    fn test_dp_laplace_zero_sensitivity() {
        let mut rng = StdRng::seed_from_u64(42);

        let result = dp_laplace("1000", 1.0, 0.0, &mut rng);

        assert_eq!(result, "1000");
    }

    #[test]
    fn test_dp_laplace_negative_number() {
        let mut rng = StdRng::seed_from_u64(42);

        let result = dp_laplace("-1000", 1.0, 10.0, &mut rng);

        assert_ne!(result, "-1000");
    }

    #[test]
    fn test_dp_laplace_integer() {
        let mut rng = StdRng::seed_from_u64(42);

        let result = dp_laplace("42", 1.0, 10.0, &mut rng);

        assert_ne!(result, "42");
    }

    #[test]
    fn test_dp_laplace_distribution_changes() {
        let mut rng = StdRng::seed_from_u64(42);

        let mut values = Vec::new();

        for _ in 0..1000 {
            let v = dp_laplace("1000", 1.0, 10.0, &mut rng);
            values.push(v.parse::<f64>().unwrap());
        }

        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;

        assert!(mean > 990.0 && mean < 1010.0);
    }
}
