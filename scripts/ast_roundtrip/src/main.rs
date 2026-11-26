//! Parses a dump into sqlparser ASTs and prints them back: the least an
//! AST-based masker does, measured in `docs/benchmarks.md`.
//!
//! `ast-roundtrip DIALECT MODE < dump.sql > out.sql`, with DIALECT `mysql`
//! or `postgres` and MODE `whole`, which keeps the AST of every statement
//! until the input ends, as a parser of the whole file does, or
//! `statement`, which prints each statement once it is parsed.
//!
//! A statement ends with a line that ends with `;`, and a line that starts
//! with `COPY` and ends with `stdin;` is followed by data up to `\.`: this
//! holds for the dumps measured. Statements the parser rejects are named
//! on standard error and written as they were.

use std::io::{self, BufRead, BufWriter, Write};

use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

enum Parsed {
    Ast(Vec<Statement>),
    Rejected(String),
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: ast-roundtrip mysql|postgres whole|statement";
    let dialect: Box<dyn Dialect> = match args.get(1).map(String::as_str) {
        Some("mysql") => Box::new(MySqlDialect {}),
        Some("postgres") => Box::new(PostgreSqlDialect {}),
        _ => panic!("{usage}"),
    };
    let whole = match args.get(2).map(String::as_str) {
        Some("whole") => true,
        Some("statement") => false,
        _ => panic!("{usage}"),
    };

    let mut out = BufWriter::new(io::stdout().lock());
    let mut kept = Vec::new();
    let (mut statements, mut rejected) = (0u64, 0u64);
    let mut text = String::new();
    let mut in_copy = false;
    let mut lines = io::stdin().lock().lines();
    loop {
        // What follows the last statement is parsed as one more.
        let ends = match lines.next().transpose()? {
            Some(line) => {
                text.push_str(&line);
                text.push('\n');
                if in_copy {
                    line == "\\."
                } else if opens_copy_data(&line) {
                    in_copy = true;
                    false
                } else {
                    line.trim_end().ends_with(';')
                }
            }
            None if text.trim().is_empty() => break,
            None => true,
        };
        if !ends {
            continue;
        }
        in_copy = false;
        statements += 1;
        let parsed = match Parser::parse_sql(&*dialect, &text) {
            Ok(ast) => Parsed::Ast(ast),
            Err(error) => {
                rejected += 1;
                let first = text.lines().find(|l| !l.is_empty() && !l.starts_with("--"));
                eprintln!("rejected: {}: {error}", first.unwrap_or_default());
                Parsed::Rejected(text.clone())
            }
        };
        text.clear();
        if whole {
            kept.push(parsed);
        } else {
            write(&mut out, &parsed)?;
        }
    }
    for parsed in &kept {
        write(&mut out, parsed)?;
    }
    out.flush()?;
    eprintln!("{statements} statements, {rejected} rejected by the parser");
    Ok(())
}

fn opens_copy_data(line: &str) -> bool {
    let line = line.trim().as_bytes();
    line.len() >= 6
        && line[..4].eq_ignore_ascii_case(b"COPY")
        && line[line.len() - 6..].eq_ignore_ascii_case(b"stdin;")
}

fn write(out: &mut impl Write, parsed: &Parsed) -> io::Result<()> {
    match parsed {
        Parsed::Ast(ast) => ast.iter().try_for_each(|s| writeln!(out, "{s};")),
        Parsed::Rejected(text) => out.write_all(text.as_bytes()),
    }
}
