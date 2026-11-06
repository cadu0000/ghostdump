//! Floating-point distinguishing experiment (task ATK).
//!
//! Two inputs x0 and x1 go through each Laplace sampler many times. For each
//! output, a distinguisher guesses which input produced it. The empirical
//! advantage, 2·P(correct) − 1, is compared with the most that ε-DP allows
//! for that pair of inputs.
//!
//! Two distinguishers are used:
//!
//! * **Likelihood ratio (LR)**: picks the input closer to the output. It is
//!   the optimal test for ideal Laplace noise, so its advantage is the one DP
//!   allows.
//! * **Reachability (Mironov, 2012)**: the `f64` sampler can only produce a
//!   sparse set of noise values, `-ln(1 - k·2^-53)·b` for integers k. An
//!   output that only one input can reach gives that input away; otherwise
//!   the test falls back to LR.
//!
//! Run with `cargo run --release --example fp_distinguisher`.

use rand::SeedableRng;
use rand::rngs::StdRng;

use ghostdump::codec::SqlValue;
use ghostdump::engine::laplace::{LaplaceMechanism, laplace_noise};
use ghostdump::engine::mask::{MaskStats, Masker};

const SAMPLES: usize = 200_000;
const EPSILON: f64 = 1.0;
const LO: f64 = 0.0;
const HI: f64 = 100_000.0;

/// 2^53: the f64 sampler draws U = k / 2^53.
const TWO_53: f64 = 9_007_199_254_740_992.0;

/// Whether `output` can be produced from `input` by the f64 sampler with
/// noise scale `b`, searching the integers k around the inverse.
fn reachable(output: f64, input: f64, b: f64) -> bool {
    let distance = (output - input).abs();
    let u = -(-distance / b).exp_m1(); // 1 - exp(-d/b), accurate near 0
    let k0 = (u * TWO_53).round() as i64;
    (k0 - 3..=k0 + 3)
        .filter(|&k| (0..1i64 << 53).contains(&k))
        .any(|k| {
            let noise = -(1.0 - k as f64 / TWO_53).ln() * b;
            input + noise == output || input - noise == output
        })
}

/// Guess for the likelihood-ratio test: true means x1.
fn lr_guess(output: f64, x0: f64, x1: f64, rng: &mut StdRng) -> bool {
    let (d0, d1) = ((output - x0).abs(), (output - x1).abs());
    if d0 == d1 {
        rand::Rng::r#gen(rng)
    } else {
        d1 < d0
    }
}

/// Guess for the reachability test: true means x1.
fn reachability_guess(output: f64, x0: f64, x1: f64, b: f64, rng: &mut StdRng) -> bool {
    match (reachable(output, x0, b), reachable(output, x1, b)) {
        (true, false) => false,
        (false, true) => true,
        _ => lr_guess(output, x0, x1, rng),
    }
}

/// How a sampler's output is produced for an input.
enum Arm {
    /// The f64 sampler without rounding: what the noise looks like inside
    /// the program, before it is written.
    RawF64,
    /// GhostDump's output with `mechanism = "f64"` and this `scale`.
    F64(usize),
    /// GhostDump's output with `mechanism = "exact"` and this `scale`.
    Exact(usize),
}

impl Arm {
    fn name(&self) -> String {
        match self {
            Arm::RawF64 => "f64, unrounded noise".into(),
            Arm::F64(decimals) => format!("f64, GhostDump output, scale {decimals}"),
            Arm::Exact(decimals) => format!("exact, GhostDump output, scale {decimals}"),
        }
    }
}

struct Outcome {
    lr: f64,
    reachability: f64,
    /// Share of outputs that only one input can reach.
    decisive: f64,
}

fn run(arm: &Arm, x0: f64, x1: f64, seed: u64) -> Outcome {
    let b = (HI - LO) / EPSILON;
    let masker = match arm {
        Arm::RawF64 => None,
        Arm::F64(decimals) => Some(Masker::Laplace(LaplaceMechanism::new(
            EPSILON, LO, HI, *decimals, false,
        ))),
        Arm::Exact(decimals) => Some(Masker::Laplace(
            LaplaceMechanism::exact(EPSILON, LO, HI, *decimals, false).unwrap(),
        )),
    };
    let mut rng = StdRng::seed_from_u64(seed);
    let mut guesser = StdRng::seed_from_u64(seed ^ 0x5eed);
    let mut stats = MaskStats::default();
    let (mut lr_ok, mut reach_ok, mut decisive) = (0usize, 0usize, 0usize);

    for i in 0..SAMPLES {
        let truth = i % 2 == 1;
        let input = if truth { x1 } else { x0 };
        let output = match &masker {
            None => input + laplace_noise(b, &mut rng),
            Some(m) => {
                let text = format!("{input:.2}");
                let out = m.apply(SqlValue::Raw(text), &mut rng, "", &mut stats);
                out.as_str().unwrap().parse::<f64>().unwrap()
            }
        };
        if reachable(output, x0, b) != reachable(output, x1, b) {
            decisive += 1;
        }
        lr_ok += usize::from(lr_guess(output, x0, x1, &mut guesser) == truth);
        reach_ok += usize::from(reachability_guess(output, x0, x1, b, &mut guesser) == truth);
    }

    let advantage = |ok: usize| 2.0 * ok as f64 / SAMPLES as f64 - 1.0;
    Outcome {
        lr: advantage(lr_ok),
        reachability: advantage(reach_ok),
        decisive: decisive as f64 / SAMPLES as f64,
    }
}

/// The largest advantage any test can have between two ε'-DP output
/// distributions: (e^ε' − 1) / (e^ε' + 1).
fn dp_bound(x0: f64, x1: f64) -> f64 {
    let local_epsilon = EPSILON * (x1 - x0).abs() / (HI - LO);
    local_epsilon.exp_m1() / (local_epsilon.exp() + 1.0)
}

fn main() {
    // 95% half-width of an advantage estimated from SAMPLES guesses.
    let margin = 2.0 * 1.96 * (0.25 / SAMPLES as f64).sqrt();
    println!(
        "ε = {EPSILON}, bounds [{LO}, {HI}], {SAMPLES} outputs per row \
         (95% margin ±{margin:.4})\n"
    );
    println!(
        "| x0 | x1 | Sampler and output | DP bound | LR advantage | Reachability advantage | Decisive outputs |"
    );
    println!("|---|---|---|---|---|---|---|");

    let pairs = [
        (50_000.0, 50_000.01),
        (50_000.0, 50_001.0),
        (25_000.0, 75_000.0),
    ];
    for (n, (x0, x1)) in pairs.into_iter().enumerate() {
        let arms = [
            Arm::RawF64,
            Arm::F64(2),
            Arm::F64(6),
            Arm::F64(12),
            Arm::Exact(2),
            Arm::Exact(6),
        ];
        for (m, arm) in arms.iter().enumerate() {
            let outcome = run(arm, x0, x1, 1000 + (n * 10 + m) as u64);
            println!(
                "| {x0:.2} | {x1:.2} | {} | {:.1e} | {:.4} | {:.4} | {:.1}% |",
                arm.name(),
                dp_bound(x0, x1),
                outcome.lr,
                outcome.reachability,
                100.0 * outcome.decisive
            );
        }
    }
}
