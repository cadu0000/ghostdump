pub mod cli;
pub mod engine;
pub mod io;
pub mod parser;

use clap::Parser as ClapParser;
use cli::Args;
use io::{InputSource, OutputSource};
use parser::state::{InsertFormat, SqlDialect};
use parser::tokenizer::{join_tuple, split_tuple};
use parser::{SqlEvent, SqlParser};

use std::io::{Read, Write};

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let input = InputSource::new(args.input.clone())?;
    let output = OutputSource::new(args.output.clone())?;
    let dialect = SqlDialect::Postgres;

    process_dump(&args, input, output, dialect)?;
    Ok(())
}

fn process_dump(
    args: &Args,
    input: InputSource,
    output: OutputSource,
    dialect: SqlDialect,
) -> std::io::Result<()> {
    let mut reader = input.into_buffered();
    let mut writer = output.into_buffered();
    let mut parser = SqlParser::new(dialect);

    let mut buffer = [0u8; 64 * 1024];
    let mut rows_processed = 0;
    let mut is_first_tuple = true;

    'read: loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        for &byte in &buffer[..bytes_read] {
            if let Some(event) = parser.handle_byte(byte) {
                match event {
                    SqlEvent::Header(header_bytes) => {
                        is_first_tuple = true;

                        if !args.dry_run {
                            writer.write_all(&header_bytes)?;
                            writer.write_all(b"\n")?;
                        }
                    }
                    SqlEvent::Tuple(tuple_bytes, format) => {
                        let mut columns = split_tuple(&tuple_bytes, format);
                        let num_cols = columns.len(); 
                        for (i, col) in columns.iter_mut().enumerate() {
                            let raw_string = String::from_utf8_lossy(col);

                            let clean_string = raw_string
                                .trim()
                                .trim_matches(|c| c == '\'' || c == '(' || c == ')');

                            let hashed_string = engine::hmac_hash(clean_string, &args.secret);

                            if format == InsertFormat::Values {
                                let prefix = if i == 0 { "(" } else { " " };
                                let suffix = if i == num_cols - 1 { ")" } else { "" };

                                *col =
                                    format!("{}'{}'{}", prefix, hashed_string, suffix).into_bytes();
                            } else {
                                *col = hashed_string.into_bytes();
                            }
                        }

                        let modified_tuple = join_tuple(&columns, format);

                        if !args.dry_run {
                            if format == InsertFormat::Values && !is_first_tuple {
                                writer.write_all(b",\n")?;
                            }

                            writer.write_all(&modified_tuple)?;
                        }

                        is_first_tuple = false;
                        rows_processed += 1;

                        if let Some(limit) = args.limit {
                            if rows_processed >= limit {
                                break 'read;
                            }
                        }
                    }
                    SqlEvent::Footer(footer_bytes) => {
                        if !args.dry_run {
                            writer.write_all(&footer_bytes)?;
                            writer.write_all(b"\n\n")?; 
                        }
                    }
                }
            }
        }
    }

    if !args.dry_run {
        writer.flush()?;
    }

    if args.verbose {
        println!(" VERBOSE MODE\n Processed lines: {rows_processed}");
    }

    Ok(())
}
