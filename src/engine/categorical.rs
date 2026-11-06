//! k-ary randomized response for categorical columns.

use rand::Rng;

use crate::codec::SqlValue;
use crate::engine::laplace::bernoulli_exp;
use crate::engine::mask::MaskStats;

/// k-ary randomized response (Warner, 1965; Kairouz et al., 2016) over a
/// declared set of k categories: the true category is written with
/// probability e^ε / (e^ε + k − 1), and each other one with probability
/// 1 / (e^ε + k − 1). Every output is then ε-LDP.
///
/// Values outside the categories (and NULL with `null_policy = "generate"`)
/// are replaced by a uniformly random category. That keeps the guarantee:
/// the uniform distribution is within a factor e^ε of every row of the
/// mechanism.
///
/// Sampling is exact: a uniform candidate is accepted at once if it is the
/// true category and with probability e^-ε otherwise, where e^-ε is drawn
/// with integer arithmetic from the exact rational value of ε.
#[derive(Debug, Clone)]
pub struct RandomizedResponse {
    epsilon: f64,
    categories: Vec<String>,
    /// ε as an exact fraction num / den.
    num: u128,
    den: u128,
}

impl RandomizedResponse {
    pub fn new(epsilon: f64, categories: Vec<String>) -> Result<Self, String> {
        if !(epsilon.is_finite() && epsilon > 0.0) {
            return Err("`epsilon` must be a positive number".into());
        }
        if epsilon > 64.0 {
            return Err("`epsilon` above 64 is not supported".into());
        }
        if categories.len() < 2 {
            return Err("randomized_response needs at least two categories".into());
        }
        let mut sorted = categories.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != categories.len() {
            return Err("the categories of randomized_response must be distinct".into());
        }
        let (num, den) = exact_fraction(epsilon)
            .ok_or("`epsilon` is too small to be represented exactly; use at least 1e-9")?;
        Ok(Self {
            epsilon,
            categories,
            num,
            den,
        })
    }

    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }

    /// Probability of writing the true category: e^ε / (e^ε + k − 1).
    pub fn keep_probability(&self) -> f64 {
        let e = self.epsilon.exp();
        e / (e + self.categories.len() as f64 - 1.0)
    }

    pub fn describe(&self) -> String {
        format!(
            "k-ary randomized response over {} categories, ε-LDP per value with ε = {} \
             (exact sampler; true category kept with probability {:.4})",
            self.categories.len(),
            self.epsilon,
            self.keep_probability()
        )
    }

    pub(crate) fn apply(
        &self,
        value: SqlValue,
        rng: &mut impl Rng,
        stats: &mut MaskStats,
    ) -> SqlValue {
        let truth = value.as_str().map(str::trim).and_then(|text| {
            let found = self.categories.iter().position(|c| c == text);
            if found.is_none() {
                stats.outside_categories += 1;
            }
            found
        });
        let k = self.categories.len();
        let index = loop {
            let candidate = rng.gen_range(0..k);
            match truth {
                // Unknown values and NULL: a uniform category.
                None => break candidate,
                Some(t) if candidate == t => break candidate,
                Some(_) if bernoulli_exp(self.num, self.den, rng) => break candidate,
                Some(_) => {}
            }
        };
        SqlValue::Text(self.categories[index].clone())
    }
}

/// `value` as an exact fraction of integers. Every finite f64 is a dyadic
/// rational m·2^e; the denominator is kept below 2^100 so that the Bernoulli
/// sampler's products stay within u128.
fn exact_fraction(value: f64) -> Option<(u128, u128)> {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = if exponent == 0 {
        (bits & ((1 << 52) - 1)) << 1
    } else {
        (bits & ((1 << 52) - 1)) | (1 << 52)
    };
    // value = mantissa · 2^(exponent − 1075)
    let mut num = u128::from(mantissa);
    let mut shift = 1075 - exponent;
    while shift > 0 && num % 2 == 0 {
        num /= 2;
        shift -= 1;
    }
    if shift <= 0 {
        return Some((num << (-shift), 1));
    }
    if shift > 100 {
        return None;
    }
    Some((num, 1u128 << shift))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn categories() -> Vec<String> {
        ["ACTIVE", "PENDING", "INACTIVE"]
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    fn shares(rr: &RandomizedResponse, input: SqlValue, n: usize) -> (Vec<f64>, MaskStats) {
        let mut rng = StdRng::seed_from_u64(9);
        let mut stats = MaskStats::default();
        let mut counts = vec![0usize; rr.categories.len()];
        for _ in 0..n {
            let out = rr.apply(input.clone(), &mut rng, &mut stats);
            let index = rr
                .categories
                .iter()
                .position(|c| Some(c.as_str()) == out.as_str())
                .unwrap();
            counts[index] += 1;
        }
        (counts.iter().map(|&c| c as f64 / n as f64).collect(), stats)
    }

    #[test]
    fn test_exact_fraction() {
        assert_eq!(exact_fraction(0.5), Some((1, 2)));
        assert_eq!(exact_fraction(2.0), Some((2, 1)));
        assert_eq!(exact_fraction(1.25), Some((5, 4)));
        let (num, den) = exact_fraction(0.1).unwrap();
        assert_eq!(num as f64 / den as f64, 0.1);
        assert_eq!(exact_fraction(1e-40), None);
    }

    #[test]
    fn test_true_category_is_kept_with_the_right_probability() {
        let n = 60_000;
        for epsilon in [0.5, 1.0, 2.5] {
            let rr = RandomizedResponse::new(epsilon, categories()).unwrap();
            let (shares, _) = shares(&rr, SqlValue::Text("PENDING".into()), n);
            let keep = rr.keep_probability();
            let other = (1.0 - keep) / 2.0;
            let sd = |p: f64| (p * (1.0 - p) / n as f64).sqrt();
            assert!(
                (shares[1] - keep).abs() < 5.0 * sd(keep),
                "{epsilon}: {shares:?}"
            );
            assert!(
                (shares[0] - other).abs() < 5.0 * sd(other),
                "{epsilon}: {shares:?}"
            );
            assert!(
                (shares[2] - other).abs() < 5.0 * sd(other),
                "{epsilon}: {shares:?}"
            );
        }
    }

    #[test]
    fn test_unknown_values_and_null_become_uniform() {
        let rr = RandomizedResponse::new(1.0, categories()).unwrap();
        let n = 30_000;
        let sd = (1.0 / 3.0 * 2.0 / 3.0 / n as f64).sqrt();
        let (unknown, stats) = shares(&rr, SqlValue::Text("BANNED".into()), n);
        assert_eq!(stats.outside_categories, n);
        let (null, stats) = shares(&rr, SqlValue::Null, n);
        assert_eq!(stats.outside_categories, 0);
        for share in unknown.iter().chain(&null) {
            assert!((share - 1.0 / 3.0).abs() < 5.0 * sd, "{share}");
        }
    }

    #[test]
    fn test_validation() {
        assert!(RandomizedResponse::new(0.0, categories()).is_err());
        assert!(RandomizedResponse::new(1.0, vec!["A".into()]).is_err());
        assert!(RandomizedResponse::new(1.0, vec!["A".into(), "A".into()]).is_err());
        assert!(RandomizedResponse::new(1e-30, categories()).is_err());
    }
}
