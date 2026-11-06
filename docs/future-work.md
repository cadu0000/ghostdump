# Future work

Directions that the evaluation points to but that are not implemented. Each one keeps, or states how it would break, the constraints of the tool: a single pass, constant memory, no global state.

## Shuffling inside a bounded buffer

**Idea.** In the shuffle model, local reports are randomly permuted before release. An observer who cannot tell which report came from which row gets much less from each one: n reports that are each ε₀-LDP become (ε, δ)-DP for the dataset with ε far below ε₀ (Erlingsson et al., 2019; Balle et al., 2019). This "amplification by shuffling" sits between the local and the central models, which is exactly the gap the [utility evaluation](experiments/utility.md) measures.

**How it would fit GhostDump.** A buffer of B rows, filled from the stream and emitted in a random order, keeps memory constant (B rows) and the single pass. The amplification would apply within each window of B rows, so its strength depends on B, not on the size of the dump.

**Why it is not done.**

* **The row is the unit that moves.** For the amplification to hold, the noisy value must not be linkable to its row. GhostDump writes whole rows: an `hmac` key, any column without a rule, or the position of the row next to the rows of other tables would link the shuffled value back. Shuffling would only help for columns released apart from any identifier, which is not how dumps are used.
* **Shuffling breaks the exact-copy invariant** for everything outside the masked columns (row order is part of the dump) and the correspondence between tables that share an order.
* **The guarantee changes kind.** Amplification gives (ε, δ)-DP, with δ > 0 depending on n and B, instead of the pure ε-LDP of each value. The guarantees table would need a second notion.

A useful first step would be analytical: compute the amplified ε for windows of B rows (for example with the bounds of Feldman, McMillan and Talwar, 2021) and show for which B and ε₀ the gain matters. That needs no code.

## Other directions

* **Parallel masking (issue #10).** Masking dominates the cost with rules (see [benchmarks](benchmarks.md)): with HMAC on four columns, throughput drops from 1.7 to 0.57 million rows per second. A bounded queue of row batches between the parser and a pool of maskers, with ordered output, keeps memory constant. Each worker would need its own random generator, which the masker API already allows (the generator is passed per call).
* **Chunked parser (task CHK).** Not needed while masking dominates (the parser takes 33–38% of the time with rules); it would mainly speed up copies without rules. See the decision in [benchmarks](benchmarks.md).
* **Real data (ACS PUMS).** The utility evaluation uses synthetic data with known distributions. A converter from ACS PUMS (folktables) to SQL dumps was planned but needs access to census.gov, which the development environment did not have.

## References

* Balle, B.; Bell, J.; Gascón, A.; Nissim, K. (2019). The privacy blanket of the shuffle model. *CRYPTO*.
* Erlingsson, Ú.; Feldman, V.; Mironov, I.; Raghunathan, A.; Talwar, K.; Thakurta, A. (2019). Amplification by shuffling: from local to central differential privacy via anonymity. *SODA*.
* Feldman, V.; McMillan, A.; Talwar, K. (2021). Hiding among the clones: a simple and nearly optimal analysis of privacy amplification by shuffling. *FOCS*.
