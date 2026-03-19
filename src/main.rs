use ghostdump::cli::Args;
use ghostdump::engine::AnonymizerEngine;
use ghostdump::engine::config::Config;
use ghostdump::io::{InputSource, OutputSource};
use ghostdump::parser::state::SqlDialect;

use clap::Parser as ClapParser;
use std::collections::HashMap;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    println!("Starting GhostDump...");

    let config = match args.config {
        Some(ref path) => Config::load_from_file(path).unwrap_or_else(|e| {
            eprintln!("Fatal error loading rules file: {}", e);
            std::process::exit(1);
        }),
        None => {
            println!(
                "No rules (-c) provided. GhostDump will perform an exact copy of the original data."
            );
            Config {
                tables: HashMap::new(),
            }
        }
    };

    let input = InputSource::new(args.input.clone())?;
    let output = OutputSource::new(args.output.clone())?;
    let dialect = SqlDialect::Postgres;
    let engine = AnonymizerEngine::new(config, args.secret.clone());

    if args.dry_run {
        println!(
            "Dry-Run mode enabled. Data will be processed, but nothing will be written to disk."
        );
    }

    let start_time = Instant::now();
    let rows_processed = engine.process_dump(input, output, dialect, args.dry_run, args.limit)?;
    let duration = start_time.elapsed();

    println!("Processing completed successfully!");
    println!("Rows processed: {}", rows_processed);
    println!("Total time: {:.2?}", duration);

    if duration.as_secs_f64() > 0.0 {
        let rows_per_sec = (rows_processed as f64 / duration.as_secs_f64()) as u64;
        println!("Speed: {} rows/second", rows_per_sec);
    }

    Ok(())
}
