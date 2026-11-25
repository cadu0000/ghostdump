//! Microbenchmarks: parser and codec throughput, and the cost per value of
//! each masker. Run with `cargo bench`; see docs/benchmarks.md.

use std::hint::black_box;
use std::io;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;

use ghostdump::codec::{SqlValue, copy, values};
use ghostdump::engine::config::{CompiledConfig, Config};
use ghostdump::engine::mask::{MaskStats, Masker};
use ghostdump::engine::{AnonymizerEngine, RunOptions};
use ghostdump::parser::{SqlDialect, SqlParser};

const ROWS: usize = 20_000;

/// A COPY block shaped like the generator's `users` table.
fn copy_dump() -> Vec<u8> {
    let mut dump =
        b"COPY users (id, company_id, name, email, password, status, salary) FROM stdin;\n"
            .to_vec();
    for i in 0..ROWS {
        dump.extend_from_slice(
            format!(
                "ID-USR-{i:08}\tID-CMP-{:08}\tCustomer {i}\tuser_{i}@test.com\t$2a$12$Hash\tACTIVE\t{}.{:02}\n",
                i % 400,
                3000 + i % 5000,
                i % 100
            )
            .as_bytes(),
        );
    }
    dump.extend_from_slice(b"\\.\n");
    dump
}

/// The same rows as multi-row INSERT statements.
fn insert_dump() -> Vec<u8> {
    let mut dump = Vec::new();
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1000) {
        dump.extend_from_slice(
            b"INSERT INTO users (id, company_id, name, email, password, status, salary) VALUES\n",
        );
        for (n, i) in chunk.iter().enumerate() {
            let end = if n + 1 == chunk.len() { ";\n" } else { ",\n" };
            dump.extend_from_slice(
                format!(
                    "('ID-USR-{i:08}', 'ID-CMP-{:08}', 'Customer {i}', 'user_{i}@test.com', '$2a$12$Hash', 'ACTIVE', {}.{:02}){end}",
                    i % 400,
                    3000 + i % 5000,
                    i % 100
                )
                .as_bytes(),
            );
        }
    }
    dump
}

fn rules(sampler: &str) -> CompiledConfig {
    let toml = format!(
        r#"
        [tables.users]
        columns = [
            {{ name = "id", strategy = "hmac" }},
            {{ name = "name", strategy = "faker_name" }},
            {{ name = "email", strategy = "faker_email" }},
            {{ name = "salary", strategy = "dp_laplace", epsilon = 0.5, bounds = [0.0, 10000.0], scale = 2, mechanism = "{sampler}" }},
        ]
        "#
    );
    toml::from_str::<Config>(&toml).unwrap().compile().unwrap()
}

fn parser(c: &mut Criterion) {
    for (name, dump) in [("copy", copy_dump()), ("insert", insert_dump())] {
        let mut group = c.benchmark_group(format!("parser/{name}"));
        group.throughput(Throughput::Bytes(dump.len() as u64));
        group.bench_function("events", |b| {
            b.iter(|| {
                let mut parser = SqlParser::new(SqlDialect::Postgres);
                let mut events = 0usize;
                for &byte in &dump {
                    if parser.handle_byte(byte).is_some() {
                        events += 1;
                    }
                }
                black_box(events)
            })
        });
        group.finish();
    }
}

fn engine(c: &mut Criterion) {
    for (name, dump) in [("copy", copy_dump()), ("insert", insert_dump())] {
        let mut group = c.benchmark_group(format!("engine/{name}"));
        group.throughput(Throughput::Bytes(dump.len() as u64));
        let configs = [
            ("no_rules", CompiledConfig::empty()),
            ("rules_f64", rules("f64")),
            ("rules_exact", rules("exact")),
        ];
        for (label, config) in configs {
            let engine = AnonymizerEngine::new(config, "bench-secret-0123456789abcdef".into());
            let options = RunOptions {
                seed: Some(1),
                ..RunOptions::new(SqlDialect::Postgres)
            };
            group.bench_function(label, |b| {
                b.iter(|| {
                    engine
                        .process_dump(&dump[..], io::sink(), &options)
                        .unwrap()
                })
            });
        }
        group.finish();
    }
}

fn codec(c: &mut Criterion) {
    let copy_row = b"ID-USR-00000042\tID-CMP-00000042\tCustomer 42\tuser_42@test.com\t$2a$12$Hash\tACTIVE\t3042.42\n";
    let values_row = b"('ID-USR-00000042', 'ID-CMP-00000042', 'Customer 42', 'user_42@test.com', '$2a$12$Hash', 'ACTIVE', 3042.42)";
    let mut group = c.benchmark_group("codec/split");
    group.bench_function("copy", |b| b.iter(|| copy::split(black_box(copy_row))));
    group.bench_function("values", |b| {
        b.iter(|| values::split(black_box(values_row), SqlDialect::Postgres))
    });
    group.finish();
}

fn maskers(c: &mut Criterion) {
    let compile = |column: &str| -> Masker {
        let config: Config = toml::from_str(&format!(
            "[tables.t]\ncolumns = [{{ name = \"c\", {column} }}]"
        ))
        .unwrap();
        let mut compiled = config.compile().unwrap();
        compiled.tables.remove("t").unwrap().remove(0).masker
    };
    let cases = [
        (
            "laplace_f64",
            r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 100000.0], scale = 2, mechanism = "f64""#,
        ),
        (
            "laplace_exact",
            r#"strategy = "dp_laplace", epsilon = 1.0, bounds = [0.0, 100000.0], scale = 2"#,
        ),
        ("hmac", r#"strategy = "hmac""#),
        ("faker_name", r#"strategy = "faker_name""#),
        ("cpf", r#"strategy = "cpf""#),
    ];

    let mut group = c.benchmark_group("masker");
    for (name, column) in cases {
        let masker = compile(column);
        let mut rng = StdRng::seed_from_u64(7);
        let mut stats = MaskStats::default();
        group.bench_function(name, |b| {
            b.iter_batched(
                || SqlValue::Raw("54321.09".into()),
                |value| masker.apply(value, &mut rng, "bench-secret-0123456789abcdef", &mut stats),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, parser, engine, codec, maskers);
criterion_main!(benches);
