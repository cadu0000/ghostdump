# Roadmap

How GhostDump evolves, in three phases. Every phase keeps the properties the tool is built on: a single pass, constant memory, no global state, fail closed (a column with a rule is never written unmasked), an exact copy of everything without a rule, and logs without column values.

A critical security or correctness problem found in any phase is fixed at once, outside the order below.

## Risks found in the review of November 2026

| # | Risk | Status |
| --- | --- | --- |
| R1 | A rule that matches nothing is ignored silently: a typo in a table or column name leaves the column in clear. | open, stage 1.1 |
| R2 | Names are compared exactly, so `INSERT INTO Users (Name)` does not match the rule `users.name`. | open, stage 1.1 |
| R3 | Statements the parser does not treat as data pass through unmasked: `INSERT ... SELECT`, `INSERT ... SET`, `UPDATE`, `MERGE`, `WITH ... INSERT`, `CREATE TABLE AS SELECT`, and the literals of `ON DUPLICATE KEY UPDATE`. | open, stage 1.1 |
| R4 | The schema is dropped from the table name, so `public.users` and `audit.users` share a rule. | open, stage 1.1 |
| R5 | On an error in the middle of a run, the output file is left partial on disk. | open, stage 1.4 |
| R6 | The secret can be passed with `-s`, where it shows in the process list. | open, stage 1.4 |
| R7 | The `f64` mechanism is still available, with a conditional guarantee. | open, stage 1.2 |
| R8 | Numeric `as` conversions in the exact sampler have no recorded audit. | open, stage 1.2 |
| R9 | The CLI help is in Portuguese and everything else in English; the only exit code is 1. | open, stages 1.4 and 1.5 |
| R10 | The SQLite dialect has no end-to-end test. | open, stage 1.3 |
| R11 | CI has no dependency audit, no pinned minimum Rust version and no fuzzing; the mimalloc memory setting depends on `CFLAGS`. | open, stage 1.6 |

## V1: stabilization

Goal: a version that can be trusted. Branch `release/v1`; ends with the tag `v1.0.0` on `main`. Only what affects correctness, security or the ability to diagnose a run belongs here.

| Stage | Work | Priority | Done when |
| --- | --- | --- | --- |
| 1.0 Base | Merge the open work into `dev`; close the issues that are delivered, after checking their acceptance criteria. | critical | **done**: PR #17 merged; issues #3, #6, #7, #8, #10, #14, #15 and #16 closed |
| 1.1 Fail closed, completed | R1: a warning for each rule that matched nothing, and `--strict` to make it an error. R2: names compared without case (ASCII) in every dialect; rule tables that collide after folding are a configuration error. R3: refuse every statement that writes to a table with rules and cannot be masked, and literals in clauses after `VALUES`; accept `WITH ... INSERT ... VALUES`. R4: accept `schema.table` as a rule key; a rule without a schema applies to any schema. | critical | one test per risk shows the column does not leave in clear |
| 1.2 Numeric correctness | R8: audit every conversion in `laplace.rs`, use checked conversions where a loss is possible, test the extremes. R7: keep `f64` for comparison only, behind a hidden experiment option, with a warning at startup. Review `categorical.rs` at the limits of ε. | high | extreme-value tests pass; no lossy conversion without a written reason |
| 1.3 Inputs and edge cases | Encodings other than UTF-8, rows at the size limit, unusual comments and quoting. R10: end-to-end tests with real dump formats of the three dialects, which also confirm that standard dumps do not use the forms of R3. A `cargo-fuzz` target built from the property tests. | high | a real dump of each dialect passes end to end; fuzzing runs for a fixed time without a failure |
| 1.4 Safe operation | R5: write to a temporary file and rename at the end when `-o` is a file. R6: accept the secret from a file and warn when it comes from `-s`. Distinct exit codes for usage, rules, input and I/O errors. | high | on an error the target file is absent or untouched; exit codes documented and tested |
| 1.5 CLI and diagnosis | R9: the interface in English. Error messages with table, column and position, never values. Subcommands, keeping `ghostdump -c ...` as the default form: `check` validates rules against a dump without writing; `init` reads the schema of a dump and writes a starting rules file. | medium | `check` lists unused rules; `init` writes rules that pass `check` |
| 1.6 Maintenance | Rustdoc on every public item (`missing_docs`). `CHANGELOG`. R11: dependency audit in CI, pinned minimum Rust version, a check that the mimalloc setting is active. Two pipelines: a fast one on every push (format, lint, tests) and a full one on demand and before each release (statistical validation, long property tests, fuzzing, a smoke benchmark). | medium | CI green with the new steps; `cargo doc` without warnings |
| 1.7 Release | Security review of the whole diff, `dev` into `main`, tag `v1.0.0`. | critical | the criteria below hold |

V1 is complete when: no correctness or security issue is open; every invariant in [guarantees](guarantees.md) has a test that fails when it is broken; R1 to R3 are fixed with tests; the three dialects have end-to-end tests; CI audits the dependencies.

## V2: improvement and validation

Goal: measure what the tool delivers and improve where the evidence points. Items marked *evaluate* are analysed for relevance and feasibility first and may be declined.

| Stage | Work | Priority |
| --- | --- | --- |
| 2.1 Utility evaluation, second version | ε in {0.1, 0.5, 1, 2, 5, 10}; dispersion in every table (the raw CSVs already hold one row per repetition); the Adult dataset instead of ACS PUMS; error per column; randomized response with 2, 5, 9 and 16 categories. | high |
| 2.2 Measures over several columns | Counts over two attributes, dependence between columns (Cramér's V, correlation), one classification task. | medium |
| 2.3 Utility warnings at startup | Print the probability that randomized response keeps a value and warn when it is low; guidance on choosing ε per column. | medium |
| 2.4 Comparator with global access | A marginal-based synthesizer. The `pasteur` library (MARE, PrivBayes, AIM, MST) installs in the development environment but has not been run. *Evaluate* with a one-day trial. | medium |
| 2.5 Estimator shipped with the tool | A subcommand `estimate` that reads a masked dump and its rules and returns corrected histograms and shares, in one pass and constant memory. | medium |
| 2.6 Checking properties | Statistical tests of ε for `randomized_response` and consistent noise; a property test for the composition in the privacy summary. Formal verification of the sampler is out for now. | high |
| 2.7 Empirical risk | A linkage or inference experiment on a masked dump. *Evaluate* the scope. | medium |
| 2.8 Controlled benchmarks | A dedicated machine or declared noise, 10^7 rows, CPU profiles; decide the chunked parser with this data. | medium |
| 2.9 Guarantee per person | A two-pass helper that reports the largest number of rows per key. *Evaluate*: it is outside the single pass. | low |

V2 is complete when every claim in `docs/guarantees.md` and `docs/experiments/` can be reproduced by a documented command, with dispersion, and every recommendation of the reviews has a recorded outcome.

## V3: exploration

No closed scope. Candidate directions, each with the evidence that would justify it:

| Direction | Justified if |
| --- | --- |
| Joint randomized response over declared groups of columns | stage 2.2 shows a relevant loss of dependence between columns |
| Local mechanisms for numbers other than Laplace | stage 2.1 shows the numeric error rules out important uses |
| Shuffling in a window | the analysis in [future work](future-work.md) shows a gain for feasible windows |
| Chunked parser and parallel reading | stage 2.8 confirms the parser as the bottleneck on real loads |
| Other input formats and use as a library | there is demand |
| A simple graphical interface for building rules | `init` and `check` are not enough |
| An empirical study of local DP on real tabular data | stages 2.1 and 2.2 give solid results |

## Decisions behind this plan

* **Unused rules warn, unmaskable statements are refused.** An unused rule may come from a rules file shared by several dumps. A statement that writes to a table with rules and cannot be masked would write the sensitive value.
* **Identifiers compare without case.** PostgreSQL folds unquoted names to lower case and keeps quoted names as written; MySQL never distinguishes case in column names, and in table names it depends on the operating system; SQLite does not distinguish case. Comparing without case can only apply a rule to one table more, which errs on the safe side. The SQLite rule is still to be confirmed in its official documentation.
* **`f64` stays for comparison only, hidden.**
* **Fuzzing runs on demand and in the full pipeline**, because `cargo-fuzz` needs the nightly compiler.
* **The interface is in English.**
