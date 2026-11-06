use ghostdump::cli::{Args, DialectArg, MIN_SECRET_BYTES, SecretError, SecretLength, check_secret};
use ghostdump::engine::config::{CompiledConfig, Config};
use ghostdump::engine::{AnonymizerEngine, RunOptions};
use ghostdump::io::{InputSource, OutputSource};
use ghostdump::parser::detect_dialect;
use ghostdump::parser::state::SqlDialect;

use clap::Parser as ClapParser;
use log::{Level, LevelFilter, debug, error, info, warn};
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

// Every message goes to stderr: stdout may carry the sanitized dump.
fn main() {
    let args = Args::parse();
    init_logger(args.verbose);

    info!("Starting GhostDump...");

    let config = match args.config {
        Some(ref path) => {
            let config = Config::load_from_file(path).unwrap_or_else(|e| {
                fatal(&format!("could not load the rules file: {}", e));
            });
            // Every rule is checked before any data is read.
            let config = config
                .compile()
                .unwrap_or_else(|e| fatal(&format!("invalid rule in {}: {}", path.display(), e)));
            debug!(
                "loaded rules for {} table(s) from {}",
                config.tables.len(),
                path.display()
            );
            config
        }
        None => {
            info!(
                "No rules (-c) provided. GhostDump will perform an exact copy of the original data."
            );
            CompiledConfig::empty()
        }
    };

    let secret = if config.uses_secret() {
        resolve_secret(args.secret.as_deref())
    } else {
        String::new()
    };

    let input = InputSource::new(args.input.clone())
        .unwrap_or_else(|e| fatal(&format!("could not open the input: {}", e)));
    let mut input = BufReader::with_capacity(64 * 1024, input);
    let dialect = resolve_dialect(args.dialect, &mut input);
    let output = OutputSource::new(args.output.clone())
        .unwrap_or_else(|e| fatal(&format!("could not open the output: {}", e)));
    let engine = AnonymizerEngine::new(config, secret);
    let options = RunOptions {
        dry_run: args.dry_run,
        limit: args.limit,
        max_row_bytes: args.max_row_size.saturating_mul(1024 * 1024),
        seed: args.seed,
        ..RunOptions::new(dialect)
    };

    if args.seed.is_some() {
        warn!(
            "--seed makes every random choice reproducible: anyone with the seed can recompute \
             the generated values and the noise. Use it for experiments, never on real data."
        );
    }

    if args.dry_run {
        info!("Dry-Run mode enabled. Data will be processed, but nothing will be written to disk.");
    }

    let start_time = Instant::now();
    let report = engine
        .process_dump(input, output, &options)
        .unwrap_or_else(|e| fatal(&e.to_string()));
    let duration = start_time.elapsed();

    info!("Processing completed successfully!");
    info!("Rows processed: {}", report.rows_processed);
    info!(
        "Masked: {} field(s) in {} row(s), in {} data statement(s)",
        report.fields_masked, report.rows_masked, report.data_blocks
    );
    for (table, rows) in &report.rows_by_table {
        debug!("table `{table}`: {rows} row(s)");
    }
    info!("Total time: {:.2?}", duration);

    let seconds = duration.as_secs_f64();
    if seconds > 0.0 {
        let rows_per_sec = (report.rows_processed as f64 / seconds) as u64;
        let mib = report.bytes_read as f64 / (1024.0 * 1024.0);
        info!(
            "Speed: {} rows/second, {:.1} MiB/s ({:.1} MiB read)",
            rows_per_sec,
            mib / seconds,
            mib
        );
    }
}

/// `-v` turns on debug messages; `GHOSTDUMP_LOG` (for example `warn`)
/// replaces the level filter altogether.
fn init_logger(verbose: bool) {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    let mut builder = env_logger::Builder::new();
    match std::env::var("GHOSTDUMP_LOG") {
        Ok(filters) => builder.parse_filters(&filters),
        Err(_) => builder
            .filter_level(LevelFilter::Warn)
            .filter_module("ghostdump", level),
    };

    builder
        .target(env_logger::Target::Stderr)
        .format(|buf, record| {
            let prefix = match record.level() {
                Level::Info => return writeln!(buf, "{}", record.args()),
                Level::Error => "error",
                Level::Warn => "warning",
                Level::Debug => "debug",
                Level::Trace => "trace",
            };
            writeln!(buf, "{}: {}", prefix, record.args())
        })
        .init();
}

fn resolve_dialect(choice: DialectArg, input: &mut impl BufRead) -> SqlDialect {
    let dialect = match choice {
        DialectArg::Postgres => SqlDialect::Postgres,
        DialectArg::Mysql => SqlDialect::Mysql,
        DialectArg::Sqlite => SqlDialect::Sqlite,
        DialectArg::Auto => {
            // Peek without consuming: the engine still reads these bytes.
            let prefix = input
                .fill_buf()
                .unwrap_or_else(|e| fatal(&format!("could not read the input: {}", e)));
            detect_dialect(prefix).unwrap_or_else(|| {
                warn!("could not detect the SQL dialect; assuming PostgreSQL (use --dialect)");
                SqlDialect::Postgres
            })
        }
    };
    debug!("dialect: {dialect:?}");
    dialect
}

fn resolve_secret(secret: Option<&str>) -> String {
    match check_secret(secret) {
        Ok(SecretLength::Recommended) => {}
        Ok(SecretLength::Short) => warn!(
            "the secret is shorter than {} bytes. HMAC pseudonyms of low-entropy \
             values (IDs, CPFs) can be brute-forced if the secret is guessed. \
             Use 32 or more random bytes.",
            MIN_SECRET_BYTES
        ),
        Err(SecretError::Missing) => {
            fatal("the rules use `hmac`, but no secret was given. Pass -s or set GHOSTDUMP_SECRET.")
        }
        Err(SecretError::Empty) => fatal(
            "the secret is empty. Pass -s or set GHOSTDUMP_SECRET to 32 or more random bytes.",
        ),
    }

    secret.unwrap_or_default().to_string()
}

fn fatal(message: &str) -> ! {
    error!("{}", message);
    std::process::exit(1);
}
