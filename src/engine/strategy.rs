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
    pub fn apply(
        &self,
        original_value: Option<&str>,
        rng: &mut impl Rng,
        secret: &str,
    ) -> Option<String> {
        match self {
            StrategyConfig::FakerName => Some(Name().fake::<String>()),
            StrategyConfig::FakerEmail => Some(SafeEmail().fake::<String>()),
            StrategyConfig::FakerIp => Some(IPv4().fake::<String>()),
            StrategyConfig::RandomUuid => Some(Uuid::new_v4().to_string()),
            StrategyConfig::FakeCreditCard => Some(generate_fake_credit_card(rng)),
            StrategyConfig::FakerPhoneBr => Some(generate_fake_phone_br(rng)),
            StrategyConfig::Cpf => Some(cpf::generate()),
            StrategyConfig::Cnpj => Some(cnpj::generate()),
            StrategyConfig::Fixed { value } => Some(value.clone()),
            StrategyConfig::Nullify => None,
            StrategyConfig::RandomChoice { options } => {
                Some(options.choose(rng).cloned().unwrap_or_default())
            }
            StrategyConfig::Hmac => {
                let val = original_value.unwrap_or("");
                let hashed = hmac_hash(val, secret);
                Some(hashed)
            }
            StrategyConfig::DpLaplace {
                epsilon,
                sensitivity,
            } => {
                let val = original_value.unwrap_or("0");
                let noisy_value = dp_laplace(val, *epsilon, *sensitivity, rng);
                Some(noisy_value.to_string())
            }
            StrategyConfig::RandomString { length } => {
                let random_str: String = rng
                    .sample_iter(&Alphanumeric)
                    .take(*length)
                    .map(char::from)
                    .collect();
                Some(random_str)
            }
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
            let _ = strategy.apply(Some("12345"), &mut rng, "secret");
        }
    }

    #[test]
    fn test_nullify_strategy() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Nullify.apply(Some("123"), &mut rng, "secret");
        assert!(result.is_none());
    }

    #[test]
    fn test_fixed_strategy() {
        let mut rng = seeded_rng();
        let strategy = StrategyConfig::Fixed {
            value: "CONST".into(),
        };
        let result = strategy.apply(Some("123"), &mut rng, "secret");
        assert_eq!(result.unwrap(), "CONST");
    }

    #[test]
    fn test_random_string_length() {
        let mut rng = seeded_rng();
        let strategy = StrategyConfig::RandomString { length: 12 };
        let result = strategy.apply(Some("abc"), &mut rng, "secret").unwrap();
        assert_eq!(result.len(), 12);
    }

    #[test]
    fn test_random_choice_returns_valid_option() {
        let mut rng = seeded_rng();
        let options = vec!["A".into(), "B".into()];
        let strategy = StrategyConfig::RandomChoice {
            options: options.clone(),
        };

        let result = strategy.apply(Some("abc"), &mut rng, "secret").unwrap();
        assert!(options.contains(&result));
    }

    #[test]
    fn test_uuid_strategy_valid_format() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::RandomUuid
            .apply(Some("abc"), &mut rng, "secret")
            .unwrap();

        assert!(uuid::Uuid::parse_str(&result).is_ok());
    }

    #[test]
    fn test_cpf_strategy_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Cpf
            .apply(Some("abc"), &mut rng, "secret")
            .unwrap();

        assert_eq!(result.len(), 11);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_cnpj_strategy_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::Cnpj
            .apply(Some("abc"), &mut rng, "secret")
            .unwrap();

        assert_eq!(result.len(), 14);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_credit_card_structure() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::FakeCreditCard
            .apply(Some("abc"), &mut rng, "secret")
            .unwrap();

        assert_eq!(result.len(), 19);
        assert_eq!(result.matches('-').count(), 3);
    }

    #[test]
    fn test_phone_br_format() {
        let mut rng = seeded_rng();
        let result = StrategyConfig::FakerPhoneBr
            .apply(Some("abc"), &mut rng, "secret")
            .unwrap();

        assert!(result.starts_with('('));
    }

    #[test]
    fn test_hmac_is_deterministic_for_same_input() {
        let mut rng1 = seeded_rng();
        let mut rng2 = seeded_rng();

        let strategy = StrategyConfig::Hmac;

        let r1 = strategy.apply(Some("123"), &mut rng1, "secret");
        let r2 = strategy.apply(Some("123"), &mut rng2, "secret");

        assert_eq!(r1, r2);
    }

    #[test]
    fn test_dp_laplace_returns_value() {
        let mut rng = seeded_rng();

        let strategy = StrategyConfig::DpLaplace {
            epsilon: 1.0,
            sensitivity: 100.0,
        };

        let result = strategy.apply(Some("5000"), &mut rng, "secret");

        assert!(result.is_some());
        assert!(!result.unwrap().is_empty());
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
