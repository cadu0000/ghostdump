# Floating-point distinguishing experiment

**Question.** Does the `f64` Laplace sampler leak more than ε-DP allows, as Mironov (2012) showed for floating-point samplers, and do GhostDump's outputs (rounded to `scale`) or the exact sampler close the leak?

**Run it:**

```bash
cargo run --release --example fp_distinguisher
```

The program is `examples/fp_distinguisher.rs`. It uses a fixed seed per row, so the table below is reproducible.

## Method

Two inputs x0 and x1 go through each sampler 200,000 times (half each). For every output, a distinguisher guesses which input produced it. The empirical advantage is 2·P(correct) − 1: 0 means guessing, 1 means certainty.

The parameters are those of a typical salary column: ε = 1, bounds [0, 100000], so the noise scale is b = 100000.

Two distinguishers:

* **Likelihood ratio (LR):** picks the input closer to the output. It is the optimal test for ideal Laplace noise, so it reaches the advantage DP allows and no more.
* **Reachability (Mironov's idea):** the `f64` sampler draws U = k/2^53 and outputs x ± (−ln(1 − U)·b), so for a given input only a sparse set of outputs is possible. The test checks, for each input, whether some integer k reproduces the output exactly. An output that only one input can reach gives that input away; otherwise the test falls back to LR.

The **DP bound** is the largest advantage any test can have between two ε'-DP distributions, (e^ε' − 1)/(e^ε' + 1), with ε' = ε·|x1 − x0|/(hi − lo). "Decisive outputs" is the share of outputs that only one input can reach.

Arms:

* **f64, unrounded noise:** x + noise as the program holds it before writing.
* **f64, GhostDump output, scale s:** what GhostDump writes with `mechanism = "f64"`.
* **exact, GhostDump output, scale s:** what GhostDump writes with `mechanism = "exact"` (the default).

## Results (November 2026)

95% margin of each advantage: ±0.0044.

| x0 | x1 | Sampler and output | DP bound | LR advantage | Reachability advantage | Decisive outputs |
|---|---|---|---|---|---|---|
| 50000.00 | 50000.01 | f64, unrounded noise | 5.0e-8 | 0.0004 | 0.5015 | 50.5% |
| 50000.00 | 50000.01 | f64, GhostDump output, scale 2 | 5.0e-8 | 0.0000 | 0.0006 | 33.7% |
| 50000.00 | 50000.01 | f64, GhostDump output, scale 6 | 5.0e-8 | -0.0009 | 0.0003 | 33.8% |
| 50000.00 | 50000.01 | f64, GhostDump output, scale 12 | 5.0e-8 | -0.0052 | 0.4897 | 49.5% |
| 50000.00 | 50000.01 | exact, GhostDump output, scale 2 | 5.0e-8 | -0.0006 | -0.0016 | 33.8% |
| 50000.00 | 50000.01 | exact, GhostDump output, scale 6 | 5.0e-8 | -0.0006 | 0.0002 | 33.5% |
| 50000.00 | 50001.00 | f64, unrounded noise | 5.0e-6 | 0.0046 | 0.4425 | 43.9% |
| 50000.00 | 50001.00 | f64, GhostDump output, scale 2 | 5.0e-6 | -0.0024 | -0.0021 | 31.0% |
| 50000.00 | 50001.00 | f64, GhostDump output, scale 6 | 5.0e-6 | -0.0000 | -0.0028 | 31.0% |
| 50000.00 | 50001.00 | f64, GhostDump output, scale 12 | 5.0e-6 | 0.0009 | 0.4263 | 42.8% |
| 50000.00 | 50001.00 | exact, GhostDump output, scale 2 | 5.0e-6 | -0.0017 | -0.0007 | 30.8% |
| 50000.00 | 50001.00 | exact, GhostDump output, scale 6 | 5.0e-6 | 0.0040 | 0.0011 | 30.9% |
| 25000.00 | 75000.00 | f64, unrounded noise | 2.4e-1 | 0.2209 | 0.5453 | 52.1% |
| 25000.00 | 75000.00 | f64, GhostDump output, scale 2 | 2.4e-1 | 0.2204 | 0.1766 | 35.5% |
| 25000.00 | 75000.00 | f64, GhostDump output, scale 6 | 2.4e-1 | 0.2222 | 0.1804 | 35.5% |
| 25000.00 | 75000.00 | f64, GhostDump output, scale 12 | 2.4e-1 | 0.2217 | 0.5335 | 50.5% |
| 25000.00 | 75000.00 | exact, GhostDump output, scale 2 | 2.4e-1 | 0.2219 | 0.1783 | 35.5% |
| 25000.00 | 75000.00 | exact, GhostDump output, scale 6 | 2.4e-1 | 0.2227 | 0.1818 | 35.5% |

## Findings

1. **The unrounded `f64` noise leaks far beyond the guarantee.** For inputs one cent apart, DP allows an advantage of 5·10^-8; the reachability test reaches 0.50, because half of the outputs can only come from one of the inputs. The same holds for inputs 1 and 50,000 apart. This is the attack of Mironov (2012), reproduced on GhostDump's own sampler.
2. **Rounding to `scale` closes the leak only when the grid is coarser than the floating-point spacing.** With `scale` 2 or 6, both distinguishers stay at the DP bound (within the margin), and the "decisive" outputs are coincidences: the reachability test does worse than LR on them. With `scale = 12`, finer than the spacing of doubles near 50,000 (about 7·10^-12), the output keeps the low-order bits and the leak is back (advantage 0.49).
3. **The exact sampler does not leak at any scale tested.** Its outputs are integers in units of `scale`, with no floating-point step, so there is no sparse structure to exploit. Its LR advantage matches the bound, as it should for Laplace noise.

## Consequences

* The `f64` arm keeps its guarantee only if the output is rounded to a grid much coarser than the spacing of doubles at the output's magnitude. GhostDump does not check this condition: it accepts `scale` up to 12. The exact sampler, the default, does not depend on it.
* These results support the decision recorded in [decisions/0001-laplace-sampler.md](../decisions/0001-laplace-sampler.md) and feed the conditions of the guarantees table.
* Limitation: the reachability test assumes the attacker knows the sampler and its parameters, which is the standard assumption (Kerckhoffs). It does not need the seed.

## Reference

Mironov, I. (2012). On significance of the least significant bits for differential privacy. *CCS*.
