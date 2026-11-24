//! The parser loses no byte: what it reports, plus its remainder, is the
//! input, and a dump that ends inside a row may only drop that row
//! (`parser_reports_every_byte` in tests/properties.rs). The first byte
//! chooses the dialect.

#![no_main]

use ghostdump::parser::{Remainder, SqlDialect, SqlEvent, SqlParser};
use libfuzzer_sys::fuzz_target;

const DIALECTS: [SqlDialect; 3] = [SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Sqlite];

fuzz_target!(|data: &[u8]| {
    let Some((&first, input)) = data.split_first() else {
        return;
    };
    let mut parser = SqlParser::new(DIALECTS[usize::from(first % 3)]);
    let mut out = Vec::with_capacity(input.len());
    for &byte in input {
        match parser.handle_byte(byte) {
            Some(SqlEvent::DefaultStatement(b)) => out.push(b),
            Some(
                SqlEvent::Passthrough(bytes)
                | SqlEvent::Statement(bytes)
                | SqlEvent::Header {
                    raw_bytes: bytes, ..
                }
                | SqlEvent::Tuple(bytes, _)
                | SqlEvent::Footer(bytes),
            ) => out.extend_from_slice(&bytes),
            Some(SqlEvent::Error(e)) => panic!("unexpected parser error: {e}"),
            None => {}
        }
    }
    match parser.finish() {
        Remainder::Bytes(rest) | Remainder::Statement(rest) => {
            out.extend_from_slice(&rest);
            assert_eq!(out, input);
        }
        Remainder::UnterminatedData {
            partial_row,
            footer,
        } => {
            out.extend_from_slice(&footer);
            assert!(input.starts_with(&out));
            if !partial_row {
                assert_eq!(out, input);
            }
        }
    }
});
