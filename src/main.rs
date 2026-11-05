use ghostdump::cli::{Args, MIN_SECRET_BYTES, SecretError, SecretLength, check_secret};
use ghostdump::engine::AnonymizerEngine;
use ghostdump::engine::config::Config;
use ghostdump::io::{InputSource, OutputSource};
use ghostdump::parser::state::SqlDialect;

use clap::Parser as ClapParser;
use std::collections::HashMap;
use std::time::Instant;

// Status messages go to stderr: stdout may carry the sanitized dump.
fn main() {
    let args = Args::parse();

    eprintln!("Starting GhostDump...");

    let config = match args.config {
        Some(ref path) => Config::load_from_file(path).unwrap_or_else(|e| {
            fatal(&format!("could not load the rules file: {}", e));
        }),
        None => {
            eprintln!(
                "No rules (-c) provided. GhostDump will perform an exact copy of the original data."
            );
            Config {
                tables: HashMap::new(),
            }
        }
    };

    let secret = if config.uses_secret() {
        resolve_secret(args.secret.as_deref())
    } else {
        String::new()
    };

    let input = InputSource::new(args.input.clone())
        .unwrap_or_else(|e| fatal(&format!("could not open the input: {}", e)));
    let output = OutputSource::new(args.output.clone())
        .unwrap_or_else(|e| fatal(&format!("could not open the output: {}", e)));
    let dialect = SqlDialect::Postgres;
    let engine = AnonymizerEngine::new(config, secret);

    if args.dry_run {
        eprintln!(
            "Dry-Run mode enabled. Data will be processed, but nothing will be written to disk."
        );
    }

    let start_time = Instant::now();
    let rows_processed = engine
        .process_dump(input, output, dialect, args.dry_run, args.limit)
        .unwrap_or_else(|e| fatal(&e.to_string()));
    let duration = start_time.elapsed();

    eprintln!("Processing completed successfully!");
    eprintln!("Rows processed: {}", rows_processed);
    eprintln!("Total time: {:.2?}", duration);

    if duration.as_secs_f64() > 0.0 {
        let rows_per_sec = (rows_processed as f64 / duration.as_secs_f64()) as u64;
        eprintln!("Speed: {} rows/second", rows_per_sec);
    }
}

fn resolve_secret(secret: Option<&str>) -> String {
    match check_secret(secret) {
        Ok(SecretLength::Recommended) => {}
        Ok(SecretLength::Short) => eprintln!(
            "Warning: the secret is shorter than {} bytes. HMAC pseudonyms of low-entropy \
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
    eprintln!("Fatal error: {}", message);
    std::process::exit(1);
}
