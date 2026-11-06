//! Property-based tests of the parser and the engine (task FUZ).
//!
//! Inputs are built from SQL fragments that reach the parser's hard states
//! (quotes, escapes, comments, dollar quotes, COPY markers, statement
//! starts) mixed with arbitrary bytes. Run more cases with
//! `PROPTEST_CASES=10000 cargo test --release --test properties`.

use std::io;

use proptest::prelude::*;

use ghostdump::engine::config::{CompiledConfig, Config};
use ghostdump::engine::{AnonymizerEngine, RunOptions};
use ghostdump::parser::{Remainder, SqlDialect, SqlEvent, SqlParser};

const FRAGMENTS: &[&str] = &[
    "INSERT INTO t (a, b) VALUES ",
    "INSERT INTO t VALUES ",
    "REPLACE INTO `t` (`a`) VALUE ",
    "COPY t (a, b) FROM stdin;\n",
    "COPY t (a) FROM stdin WITH (FORMAT csv);\n",
    "SELECT 1;\n",
    "inserted_at ",
    "(",
    ")",
    "(1, 'x')",
    ", ",
    ";",
    "'",
    "''",
    "\"",
    "`",
    "\\",
    "\\'",
    "E'",
    "$$",
    "$f$",
    "--",
    "/*",
    "*/",
    "#",
    "\n",
    "\t",
    "\\.\n",
    "\\N",
    "NULL",
    "ON CONFLICT DO NOTHING",
    "\\connect db\n",
    "\u{feff}",
    "é",
];

/// A byte string made of SQL fragments and arbitrary bytes.
fn sql_like() -> impl Strategy<Value = Vec<u8>> {
    let piece = prop_oneof![
        3 => prop::sample::select(FRAGMENTS).prop_map(|f| f.as_bytes().to_vec()),
        1 => prop::collection::vec(any::<u8>(), 1..4),
    ];
    prop::collection::vec(piece, 0..40).prop_map(|pieces| pieces.concat())
}

fn dialect() -> impl Strategy<Value = SqlDialect> {
    prop_oneof![
        Just(SqlDialect::Postgres),
        Just(SqlDialect::Mysql),
        Just(SqlDialect::Sqlite),
    ]
}

/// Every byte the parser reports, in order, and its remainder.
fn reassemble(dialect: SqlDialect, input: &[u8]) -> (Vec<u8>, Remainder) {
    let mut parser = SqlParser::new(dialect);
    let mut out = Vec::with_capacity(input.len());
    for &byte in input {
        match parser.handle_byte(byte) {
            Some(SqlEvent::DefaultStatement(b)) => out.push(b),
            Some(SqlEvent::Passthrough(bytes))
            | Some(SqlEvent::Header {
                raw_bytes: bytes, ..
            })
            | Some(SqlEvent::Tuple(bytes, _))
            | Some(SqlEvent::Footer(bytes)) => out.extend_from_slice(&bytes),
            Some(SqlEvent::Error(e)) => panic!("unexpected parser error: {e}"),
            None => {}
        }
    }
    (out, parser.finish())
}

fn engine(rules: &str) -> AnonymizerEngine {
    let config = if rules.is_empty() {
        CompiledConfig::empty()
    } else {
        toml::from_str::<Config>(rules).unwrap().compile().unwrap()
    };
    AnonymizerEngine::new(config, "property-test-secret-0123456789".into())
}

proptest! {
    /// The parser loses no byte: what it reports, plus its remainder, is
    /// the input. A dump that ends inside a row may only drop that row.
    #[test]
    fn parser_reports_every_byte(dialect in dialect(), input in sql_like()) {
        let (out, remainder) = reassemble(dialect, &input);
        match remainder {
            Remainder::Bytes(rest) => {
                let mut all = out;
                all.extend_from_slice(&rest);
                prop_assert_eq!(all, input);
            }
            Remainder::UnterminatedData { .. } => {
                prop_assert!(input.starts_with(&out));
            }
        }
    }

    /// Without rules, a run either copies the input exactly or stops with
    /// an `InvalidData` error (a truncated row); it never panics.
    #[test]
    fn no_rules_means_an_exact_copy(dialect in dialect(), input in sql_like()) {
        let mut out = Vec::new();
        match engine("").process_dump(&input[..], &mut out, &RunOptions::new(dialect)) {
            Ok(_) => prop_assert_eq!(out, input),
            Err(e) => prop_assert_eq!(e.kind(), io::ErrorKind::InvalidData),
        }
    }

    /// With rules, a run never panics, whatever the input.
    #[test]
    fn rules_never_panic(dialect in dialect(), input in sql_like()) {
        let rules = r#"
            [tables.t]
            columns = [
                { name = "a", strategy = "hmac" },
                { name = "b", strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 10.0], scale = 2, null_policy = "generate" },
            ]
        "#;
        let options = RunOptions { seed: Some(1), ..RunOptions::new(dialect) };
        let _ = engine(rules).process_dump(&input[..], io::sink(), &options);
    }

    /// Masking one column keeps the other columns byte for byte and the
    /// number of rows, for values with quotes, backslashes and NULLs.
    #[test]
    fn masking_keeps_rows_and_other_columns(
        dialect in dialect(),
        rows in prop::collection::vec(
            (prop::option::of("[a-z' \\\\,()]{0,8}"), -1000i64..1000),
            1..20,
        ),
    ) {
        let quote = |text: &str| -> String {
            match dialect {
                SqlDialect::Mysql => format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'")),
                _ => format!("'{}'", text.replace('\'', "''")),
            }
        };
        let values: Vec<String> = rows
            .iter()
            .map(|(text, number)| {
                let a = text.as_deref().map_or("NULL".to_string(), quote);
                format!("({a}, {number})")
            })
            .collect();
        let input = format!("INSERT INTO t (a, b) VALUES {};\n", values.join(", "));
        let rules = r#"
            [tables.t]
            columns = [{ name = "b", strategy = "fixed", value = "x" }]
        "#;
        let mut out = Vec::new();
        engine(rules)
            .process_dump(input.as_bytes(), &mut out, &RunOptions::new(dialect))
            .unwrap();

        let (_, remainder) = reassemble(dialect, &out);
        prop_assert!(matches!(remainder, Remainder::Bytes(_)));
        let tuples = |bytes: &[u8]| -> Vec<Vec<u8>> {
            let mut parser = SqlParser::new(dialect);
            bytes
                .iter()
                .filter_map(|&b| match parser.handle_byte(b) {
                    Some(SqlEvent::Tuple(row, _)) => Some(row),
                    _ => None,
                })
                .collect()
        };
        let before = tuples(input.as_bytes());
        let after = tuples(&out);
        prop_assert_eq!(before.len(), rows.len());
        prop_assert_eq!(after.len(), rows.len());
        for (old, new) in before.iter().zip(&after) {
            // Column a is everything up to the last comma: unchanged.
            let cut = |row: &[u8]| row[..row.iter().rposition(|&c| c == b',').unwrap()].to_vec();
            prop_assert_eq!(cut(old), cut(new));
            prop_assert!(new.ends_with(b", 'x')"));
        }
    }
}
