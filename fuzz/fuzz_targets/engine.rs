//! Without rules, a run copies the input exactly or stops with an
//! `InvalidData` error; with rules it never panics, and three threads write
//! what one does (`no_rules_means_an_exact_copy`, `rules_never_panic` and
//! `threads_do_not_change_the_output` in tests/properties.rs). The first
//! byte chooses the dialect.

#![no_main]

use std::io;
use std::sync::LazyLock;
use std::{fs, thread};

use ghostdump::engine::config::{CompiledConfig, Config};
use ghostdump::engine::{AnonymizerEngine, RunOptions};
use ghostdump::parser::SqlDialect;
use libfuzzer_sys::fuzz_target;

const DIALECTS: [SqlDialect; 3] = [SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Sqlite];
const SECRET: &str = "fuzz-secret-0123456789abcdef";
const RULES: &str = r#"
    [tables.t]
    columns = [
        { name = "a", strategy = "hmac" },
        { name = "b", strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2, null_policy = "generate" },
    ]

    [tables.users]
    columns = [
        { name = "name", strategy = "fixed", value = "x" },
        { name = "status", strategy = "randomized_response", epsilon = 1.0, categories = ["A", "B"] },
    ]
"#;

static WITHOUT_RULES: LazyLock<AnonymizerEngine> =
    LazyLock::new(|| AnonymizerEngine::new(CompiledConfig::empty(), SECRET.into()));
static WITH_RULES: LazyLock<AnonymizerEngine> = LazyLock::new(|| {
    let config = toml::from_str::<Config>(RULES).unwrap().compile().unwrap();
    AnonymizerEngine::new(config, SECRET.into())
});

/// The number of threads of this process, or 0 where `/proc` is missing.
fn threads() -> usize {
    fs::read_dir("/proc/self/task").map_or(0, Iterator::count)
}

fuzz_target!(|data: &[u8]| {
    let Some((&first, input)) = data.split_first() else {
        return;
    };
    let dialect = DIALECTS[usize::from(first % 3)];

    let mut out = Vec::new();
    match WITHOUT_RULES.process_dump(input, &mut out, &RunOptions::new(dialect)) {
        Ok(_) => assert_eq!(out, input),
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
    }

    let run = |threads| {
        let options = RunOptions {
            seed: Some(1),
            threads,
            ..RunOptions::new(dialect)
        };
        let mut out = Vec::new();
        let result = WITH_RULES.process_dump(input, &mut out, &options);
        (result.map_err(|e| e.to_string()), out)
    };
    let before = threads();
    assert_eq!(run(1), run(3));
    // `thread::scope` returns when the closures of the workers end, before
    // their thread-local destructors free what they hold. libFuzzer counts
    // the frees of an input when it returns, and checks for leaks when they
    // fall short, so wait until the workers are gone. Without the wait,
    // LeakSanitizer reported the channel context of a worker in 4 runs of
    // 4, within 20,000 inputs; with it, in none of 4 runs of 5 minutes.
    while threads() > before {
        thread::yield_now();
    }
});
