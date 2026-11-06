//! Strategies as written in the rules file. They are validated and compiled
//! into maskers (`engine::mask`) once, before any data is read.

use serde::Deserialize;

use crate::engine::laplace::LaplaceMechanism;
use crate::engine::mask::{Masker, Synthetic};

/// Most decimal places `dp_laplace` accepts in `scale`.
const MAX_SCALE: u32 = 12;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum StrategyConfig {
    Hmac,
    DpLaplace {
        epsilon: f64,
        /// `[lo, hi]`: values are clamped to it before noise is added.
        bounds: Option<[f64; 2]>,
        /// No longer accepted; kept so the error can explain the change.
        sensitivity: Option<f64>,
        /// Decimal places written.
        scale: Option<u32>,
        /// Also clamp the noisy output to the bounds.
        #[serde(default)]
        clamp_output: bool,
        /// Which sampler draws the noise.
        #[serde(default)]
        mechanism: LaplaceSampler,
        /// Draw the noise from a keyed PRF of the value: equal values get
        /// equal outputs in every row and run. Needs the secret.
        #[serde(default)]
        consistent: bool,
    },
    FakerName,
    FakerEmail,
    FakeCreditCard,
    FakerPhoneBr,
    FakerIp,
    RandomUuid,
    Cpf,
    Cnpj,
    Nullify,
    Fixed {
        value: String,
    },
    RandomChoice {
        options: Vec<String>,
    },
    RandomString {
        length: usize,
    },
}

/// `exact` (the default) samples on integers with no floating-point step;
/// `f64` is the floating-point sampler, kept for comparison.
#[derive(Debug, Default, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaplaceSampler {
    #[default]
    Exact,
    F64,
}

impl StrategyConfig {
    /// Validates the parameters and builds the masker.
    pub fn compile(&self) -> Result<Masker, String> {
        let synthetic = |kind| Ok(Masker::Synthetic(kind));
        match self {
            StrategyConfig::Hmac => Ok(Masker::Hmac),
            StrategyConfig::Nullify => Ok(Masker::Nullify),
            StrategyConfig::FakerName => synthetic(Synthetic::FakerName),
            StrategyConfig::FakerEmail => synthetic(Synthetic::FakerEmail),
            StrategyConfig::FakerIp => synthetic(Synthetic::Ipv4),
            StrategyConfig::RandomUuid => synthetic(Synthetic::Uuid),
            StrategyConfig::FakeCreditCard => synthetic(Synthetic::CreditCard),
            StrategyConfig::FakerPhoneBr => synthetic(Synthetic::PhoneBr),
            StrategyConfig::Cpf => synthetic(Synthetic::Cpf),
            StrategyConfig::Cnpj => synthetic(Synthetic::Cnpj),
            StrategyConfig::Fixed { value } => synthetic(Synthetic::Fixed(value.clone())),
            StrategyConfig::RandomChoice { options } => {
                if options.is_empty() {
                    return Err("random_choice needs at least one option".into());
                }
                synthetic(Synthetic::Choice(options.clone()))
            }
            StrategyConfig::RandomString { length } => {
                if *length == 0 {
                    return Err("random_string needs a length of at least 1".into());
                }
                synthetic(Synthetic::Alphanumeric(*length))
            }
            StrategyConfig::DpLaplace {
                epsilon,
                bounds,
                sensitivity,
                scale,
                clamp_output,
                mechanism,
                consistent,
            } => compile_laplace(
                *epsilon,
                *bounds,
                *sensitivity,
                *scale,
                *clamp_output,
                *mechanism,
                *consistent,
            ),
        }
    }
}

fn compile_laplace(
    epsilon: f64,
    bounds: Option<[f64; 2]>,
    sensitivity: Option<f64>,
    scale: Option<u32>,
    clamp_output: bool,
    sampler: LaplaceSampler,
    consistent: bool,
) -> Result<Masker, String> {
    if sensitivity.is_some() {
        return Err(
            "`sensitivity` is no longer accepted: in local differential privacy the \
             sensitivity is the width of the input domain. Declare `bounds = [lo, hi]` \
             instead; values are clamped to it and Δ = hi - lo"
                .into(),
        );
    }
    let Some([lo, hi]) = bounds else {
        return Err(
            "dp_laplace needs `bounds = [lo, hi]`, the range values are clamped to \
             before noise is added"
                .into(),
        );
    };
    if !(epsilon.is_finite() && epsilon > 0.0) {
        return Err("`epsilon` must be a positive number".into());
    }
    if !(lo.is_finite() && hi.is_finite() && lo < hi) {
        return Err("`bounds` must be two finite numbers with lo < hi".into());
    }
    let Some(scale) = scale else {
        return Err(
            "dp_laplace needs `scale`, the number of decimal places written \
             (0 for integers, 2 for money): copying the precision of each input would \
             reveal it"
                .into(),
        );
    };
    if scale > MAX_SCALE {
        return Err(format!("`scale` must be at most {MAX_SCALE}"));
    }

    let decimals = scale as usize;
    let mechanism = match sampler {
        LaplaceSampler::Exact => LaplaceMechanism::exact(epsilon, lo, hi, decimals, clamp_output)?,
        LaplaceSampler::F64 => LaplaceMechanism::new(epsilon, lo, hi, decimals, clamp_output),
    };
    let mechanism = if consistent {
        mechanism.with_consistent_noise()
    } else {
        mechanism
    };
    Ok(Masker::Laplace(mechanism))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(toml_column: &str) -> Result<Masker, String> {
        #[derive(Deserialize)]
        struct Wrapper {
            column: StrategyConfig,
        }
        let parsed: Wrapper = toml::from_str(&format!("column = {{ {toml_column} }}")).unwrap();
        parsed.column.compile()
    }

    #[test]
    fn test_every_strategy_compiles() {
        for column in [
            r#"strategy = "hmac""#,
            r#"strategy = "nullify""#,
            r#"strategy = "faker_name""#,
            r#"strategy = "faker_email""#,
            r#"strategy = "faker_ip""#,
            r#"strategy = "random_uuid""#,
            r#"strategy = "fake_credit_card""#,
            r#"strategy = "faker_phone_br""#,
            r#"strategy = "cpf""#,
            r#"strategy = "cnpj""#,
            r#"strategy = "fixed", value = "x""#,
            r#"strategy = "random_choice", options = ["a"]"#,
            r#"strategy = "random_string", length = 4"#,
            r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2"#,
            r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2, mechanism = "f64""#,
        ] {
            assert!(compile(column).is_ok(), "{column}");
        }
    }

    #[test]
    fn test_dp_laplace_validation() {
        let cases = [
            (
                r#"strategy = "dp_laplace", epsilon = 1.0, sensitivity = 5.0"#,
                "no longer accepted",
            ),
            (
                r#"strategy = "dp_laplace", epsilon = 1.0, scale = 2"#,
                "needs `bounds",
            ),
            (
                r#"strategy = "dp_laplace", epsilon = 0.0, bounds = [0.0, 1.0], scale = 2"#,
                "positive",
            ),
            (
                r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [5.0, 1.0], scale = 2"#,
                "lo < hi",
            ),
            (
                r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 1.0]"#,
                "needs `scale`",
            ),
            (
                r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 1.0], scale = 13"#,
                "at most 12",
            ),
        ];
        for (column, message) in cases {
            let err = compile(column).unwrap_err();
            assert!(err.contains(message), "{column}: {err}");
        }
    }

    #[test]
    fn test_exact_sampler_is_the_default() {
        let column = r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2"#;
        let Ok(Masker::Laplace(m)) = compile(column) else {
            panic!("dp_laplace compiles");
        };
        assert!(m.describe().contains("exact integer sampler"));
        let too_fine = r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 1e12], scale = 6"#;
        assert!(compile(too_fine).unwrap_err().contains("2^53"));
    }

    #[test]
    fn test_consistent_noise_needs_the_secret() {
        let plain = r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2"#;
        assert!(!compile(plain).unwrap().uses_secret());
        let consistent = r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2, consistent = true"#;
        assert!(compile(consistent).unwrap().uses_secret());
    }

    #[test]
    fn test_other_validation() {
        assert!(compile(r#"strategy = "random_choice", options = []"#).is_err());
        assert!(compile(r#"strategy = "random_string", length = 0"#).is_err());
    }
}
