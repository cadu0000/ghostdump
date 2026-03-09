pub mod cli;
pub mod engine;
pub mod io;
pub mod parser;

use clap::Parser as ClapParser;
use cli::Args;
use engine::AnonymizerEngine;
use io::{InputSource, OutputSource};
use parser::state::SqlDialect;

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let input = InputSource::new(args.input.clone())?;
    let output = OutputSource::new(args.output.clone())?;

    let dialect = SqlDialect::Postgres;
    let engine = AnonymizerEngine::new(args.secret.clone());

    engine.process_dump(input, output, dialect, args.dry_run, args.limit)?;

    Ok(())
}