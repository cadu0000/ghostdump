# Guarantees

What each strategy protects, under which conditions, and what it leaves exposed. Every condition points to the test or experiment that checks it. This page answers the first part of the research question (δ1: which formal guarantees hold under a single pass with constant memory and no global state, and which become conditional).

## Scope and model

* **Unit of protection: one value of one column.** Neighbouring inputs differ in the value of a single field. No strategy hides whether a row exists.
* **Local model.** Each value is masked on its own, with no state shared across rows. This follows from the constant-memory, single-pass design, not from distrust of the curator: GhostDump sees the raw dump.
* **ε-LDP implies ε-DP for the released column**, with neighbours that differ in one value. So ε here is directly comparable with central DP at the same ε; the difference is in utility (see the [utility evaluation](experiments/utility.md)).

## The table

| Strategy | Guarantee | Conditions | Residual leakage | Checked by |
| --- | --- | --- | --- | --- |
| `nullify` | **0-LDP**: constant output (`NULL`) | none | number of rows | `engine::tests::test_insert_masking_keeps_types_and_layout` |
| `fixed`, `faker_*`, `random_*`, `random_uuid`, `cpf`, `cnpj`, `fake_credit_card` | **0-LDP**: output independent of the value | none (the generators never read the value) | number of rows; which rows are NULL with `null_policy = "keep"` | `mask::tests::test_synthetic_generators`, `test_generated_documents_are_valid` |
| `hmac` | **no DP**: deterministic keyed pseudonym | secret of 32+ random bytes, never disclosed | equal values give equal pseudonyms: joins across tables and dumps, value frequencies; with the secret, brute force over small domains (IDs, CPF) | `mask::tests::test_hmac_is_deterministic_and_keyed`, `cli::hmac_rules_require_a_secret`, `cli::blank_secret_is_rejected`, `cli::short_secret_only_warns` |
| `dp_laplace`, exact sampler (default) | **ε-LDP** per value (P1) | the sampler is implemented correctly; the random generator is unpredictable | NULL pattern with `null_policy = "keep"` | `laplace::tests::test_exact_domain_spends_at_most_epsilon`, `test_noise_parameter_is_exact_at_the_limits`, `test_noise_parameter_is_exact_for_random_parameters`, `test_exact_mechanism_clamps_before_noise`, `test_text_and_null_take_the_midpoint`; `laplace::validation::*` |
| `dp_laplace`, `mechanism = "f64"` | **ε-LDP** per value, conditional | as above, **and** `scale` coarse enough that rounding hides the spacing of doubles at the output's magnitude (enforced at startup; rare tail outputs beyond 10·b are not covered). Runs only with the hidden option `--experimental-f64-sampler` | as above | `strategy::tests::test_f64_sampler_rejects_scales_that_expose_float_bits`, `test_f64_sampler_rejects_noise_that_overflows`, `laplace::tests::test_inputs_are_clamped_before_noise`, `cli::f64_sampler_needs_the_hidden_experiment_option`; [floating-point experiment](experiments/floating-point.md) |
| `dp_laplace`, `consistent = true` | **ε-LDP** per distinct value, computational (P4) | as above, and the secret is never disclosed | equal values give equal outputs (which rows share a value); NULL pattern as above | `laplace::tests::test_consistent_noise_depends_only_on_value_column_and_secret`, `cli::consistent_noise_repeats_across_runs_and_rows` |
| `randomized_response` | **ε-LDP** per value (P5) | the categories are declared in the rule; the random generator is unpredictable | NULL pattern with `null_policy = "keep"`; the set of categories | `categorical::tests::test_true_category_is_kept_with_the_right_probability`, `test_unknown_values_and_null_become_uniform`, `cli::randomized_response_writes_only_declared_categories` |
| Column without a rule | **none** | — | the value, and what can be inferred from it about protected columns (P3) | — |
| Whole dump | **Σε per row** over the `dp_laplace` columns; k·Σε for a person in k rows; T·k·Σε over T releases with fresh noise (P2) | k and T known to the publisher: GhostDump cannot see them without state | presence and order of rows, table sizes, schema; columns without rules and `hmac` columns can re-identify a row, without breaking the ε of its `dp_laplace` columns | — |

`null_policy = "generate"` changes the NULL row of the table. With an input-independent strategy, NULLs become ordinary generated values. With `dp_laplace`, a NULL is treated like a non-numeric value: it takes the midpoint of the bounds before the noise. Its output is then distributed like any other value's, so the ε-LDP of P1 covers whether the value was NULL. `hmac` keeps NULL as NULL under either policy.

## Propositions

**P1 (exact sampler).** Let f map any input of the column to an integer in [L, H], where L and H are the bounds in units of `scale`: numbers are clamped and rounded, and anything else (text, NULL with `generate`) maps to the midpoint. Let Z follow the discrete Laplace distribution with parameter t, P(Z = z) = ((1 − q)/(1 + q))·q^|z| with q = e^(−1/t), and t chosen so that Δ/t ≤ ε, with Δ = H − L. Then M(x) = f(x) + Z is ε-LDP.

*Proof.* For any inputs x, x' and any integer output y,
P(M(x) = y) / P(M(x') = y) = q^(|y − f(x)| − |y − f(x')|) ≤ q^(−|f(x) − f(x')|) ≤ e^(Δ/t) ≤ e^ε,
by the triangle inequality and because f(x) and f(x') lie in [L, H]. Writing y with the decimal point in place is a bijection, and clamping the output to [L, H] (`clamp_output`) is a function of y alone; both are post-processing and keep ε. ∎

GhostDump computes t in integers from the exact value of ε, t = ⌈Δ·den/num⌉ for ε = num/den (every `f64` is a fraction with a power of two as denominator), so Δ/t ≤ ε holds exactly, not up to rounding.

The same argument, with densities, shows that x ↦ clamp(x) + Lap((hi − lo)/ε) is ε-LDP over the reals. The `f64` sampler implements it only approximately: the [floating-point experiment](experiments/floating-point.md) shows that its unrounded output leaks far beyond ε (distinguishing advantage 0.50 where DP allows 5·10^-8), and that rounding to `scale` removes the leak only when the grid is coarser than the floating-point spacing. Rounding to the precision of each *input* would not be post-processing, because it depends on x; GhostDump always uses the fixed `scale` of the rule.

**P2 (composition).** A row with `dp_laplace` columns of budgets ε₁, …, ε_m spends Σεⱼ (sequential composition; Dwork and Roth, 2014, theorem 3.16). A person who appears in k rows spends k·Σεⱼ, and T releases of the same data with fresh noise multiply this by T. Without state, GhostDump knows neither k nor T, so the guarantee per person is conditional on the publisher knowing them.

**P3 (attribute scope).** The guarantees are per attribute. ε bounds what the noisy column reveals about its own value; it does not bound what can be inferred about that value from correlated columns written without protection (Kifer and Machanavajjhala, 2011). A rule set protects a value only if every column that predicts it is masked too.

**P4 (consistent noise).** With `consistent = true`, the noise of a value is drawn from a generator seeded by HMAC-SHA256(secret, column, clamped value). For an adversary who does not know the secret, and assuming HMAC-SHA256 is a pseudorandom function, the output for one value is computationally indistinguishable from P1's, so it is ε-LDP in the computational sense. Repeating a release, or a value repeated in several rows of one person, adds no privacy loss for that value: the output is the same each time. The price is that the output is a deterministic function of the value: rows with equal values get equal outputs, and no DP guarantee covers that equality pattern (as with `hmac`).

**P5 (randomized response).** Let the rule declare k categories. A value in the categories is written unchanged with probability p = e^ε/(e^ε + k − 1) and as each other category with probability q = 1/(e^ε + k − 1); any other input (an unknown value, or NULL with `generate`) is written as a uniform category, with probability 1/k each. Then the mechanism is ε-LDP: for any two inputs and any output, the ratio of probabilities is at most p/q = e^ε between two categories, and between a category and the uniform case it lies between k/(e^ε + k − 1) ≥ e^−ε and k·e^ε/(e^ε + k − 1) ≤ e^ε. Sampling is exact: a uniform candidate is accepted if it is the true category, and otherwise with probability e^−ε, drawn with integer arithmetic from the exact rational value of ε (Canonne, Kamath and Steinke, 2020). Accepted candidates then follow p and q exactly. The unbiased frequency estimate (observed − q)/(p − q) is post-processing.

## Structural invariants

These are properties of the pipeline, not of a mechanism. Each is enforced by the code and checked by tests rather than proved.

| Invariant | Checked by |
| --- | --- |
| Without rules, the output is the input, byte for byte (or the run stops with an error on a truncated row). | `properties::no_rules_means_an_exact_copy`, `properties::parser_reports_every_byte`, `cli::data_blocks_are_copied_exactly_without_rules` |
| A column with a rule is never written unmasked by a statement GhostDump reads: when a rule cannot be applied (no column list and no certain column order in the dump, COPY options, unreadable table name, truncated or oversized row), or when a statement writes to a table with rules in a form that cannot be masked (`UPDATE`, `INSERT ... SELECT`, `CREATE TABLE ... AS SELECT`, ...) or with values after its rows (`ON DUPLICATE KEY UPDATE name = '...'`), the run stops before writing that statement. | `engine::tests::test_rules_without_column_list_fail_closed`, `test_a_column_change_after_create_table_is_refused`, `test_copy_with_options`, `test_unreadable_table_name_fails_closed_with_rules`, `test_truncated_row_is_an_error`, `test_rows_longer_than_the_limit_stop_the_run`, `test_statements_that_write_to_tables_with_rules_are_refused`, `test_values_after_the_rows_of_a_table_with_rules_are_refused`, `cli::rows_without_column_names_or_create_table_are_refused` |
| A row without a column list fills the columns of the table's `CREATE TABLE` earlier in the dump, in the order the database uses: every column in PostgreSQL and MySQL, every column but the generated ones in SQLite. | `columns::tests::test_orders_follow_create_alter_and_drop`, `schema::tests::test_column_order_of_created_tables`, `test_tables_whose_columns_the_definition_does_not_show`, `test_keys_follow_the_case_rules_of_each_dialect`, `test_alter_table_changes_only_with_column_actions`, `engine::tests::test_rows_without_column_list_take_the_columns_of_create_table`, `cli::mysqldump_default_format_is_masked`, `cli::sqlite_dump_is_masked`, `cli::pg_dump_with_inserts_is_masked` |
| Statements end where the client that loads the dump ends them (psql, mysql, sqlite3), so the bodies of functions, procedures, triggers and rules are copied unchanged, never read as data. | `parser::tests::test_postgres_sql_function_bodies_are_one_statement`, `test_postgres_statements_end_outside_parentheses`, `test_mysql_delimiter_keeps_routine_bodies_whole`, `test_sqlite_triggers_end_at_end`, `engine::tests::test_routine_bodies_are_copied_unchanged`, `cli::pg_dump_with_column_inserts_is_masked`, `cli::mysqldump_routines_are_copied_and_rows_masked`, `cli::sqlite_trigger_bodies_are_copied` |
| Masking changes only the masked fields, and keeps the number of rows. | `codec::tests::test_unmasked_fields_and_layout_are_copied`, `properties::masking_keeps_rows_and_other_columns` |
| Logs never contain column values. | `cli::logs_never_contain_column_values` |
| Invalid rules are rejected before any data is read. | `strategy::tests::test_dp_laplace_validation`, `cli::invalid_rules_stop_before_reading_data` |

## Assumptions

* **Randomness.** Noise and generated values come from `rand`'s `StdRng` (ChaCha12). The run's generator is seeded by the operating system; each segment of rows (at most 1,024 masked rows) draws from its own generator, seeded with 256 bits from the run's one, so the output does not depend on `--threads`. Its unpredictability is computational. `--seed` makes the run reproducible and removes the protection of the noise: it is meant for experiments only, and GhostDump warns when it is used.
* **Implementation.** The exact sampler follows Canonne, Kamath and Steinke (2020). It is our implementation, not an audited library: its distribution is checked against the exact probabilities, by goodness of fit, by a periodicity test and by negative controls (`laplace::validation`, run in CI). The decision and the alternatives are in [decisions/0001-laplace-sampler.md](decisions/0001-laplace-sampler.md). Every numeric conversion of the samplers is listed, with the reason it is exact or checked, in [decisions/0003-numeric-conversions.md](decisions/0003-numeric-conversions.md), which also shows from `rand`'s source that `gen_range` on integers is exactly uniform, given uniform bits.
* **Input.** Data reaches a table only through `INSERT ... VALUES` and `COPY ... FROM stdin`, which are masked, and through the statements GhostDump refuses. The columns of an `INSERT` without a column list are those of the `CREATE TABLE` before it, changed only by the statements GhostDump reads (`ALTER TABLE`, `RENAME TABLE`, `DROP TABLE`); a statement it does not see, in code or in a MySQL version comment, could change them unnoticed. It does not see data that reaches a table through code (the body of a function, procedure, trigger or rule, a `DO` block, a prepared statement, a variable), statements inside MySQL version comments (`/*! ... */`), or PostgreSQL large objects, which pg_dump includes by default when it dumps a whole database (`SELECT pg_catalog.lowrite(...)`; leave them out with `--no-large-objects`). These are copied as they are. The check of the refused statements is by name: a statement that mentions a table with rules, even as a column or a keyword spelt the same, is refused, which errs on the safe side.
* **Bounds.** The bounds must be chosen without looking at the data. Bounds computed from the data (the "oracle" arm of the utility evaluation) are not private.

## References

* Canonne, C.; Kamath, G.; Steinke, T. (2020). The discrete Gaussian for differential privacy. *NeurIPS*.
* Desfontaines, D.; Pejó, B. (2020). SoK: Differential privacies. *PoPETs*.
* Dwork, C.; Roth, A. (2014). *The Algorithmic Foundations of Differential Privacy*.
* Kairouz, P.; Bonawitz, K.; Ramage, D. (2016). Discrete distribution estimation under local privacy. *ICML*.
* Kifer, D.; Machanavajjhala, A. (2011). No free lunch in data privacy. *SIGMOD*.
* Mironov, I. (2012). On significance of the least significant bits for differential privacy. *CCS*.
* Warner, S. L. (1965). Randomized response: a survey technique for eliminating evasive answer bias. *JASA*.
