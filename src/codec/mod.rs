//! Field-level reading and writing of data rows.
//!
//! The parser delivers each row as raw bytes. This module splits a row into
//! fields without copying it, decodes only the fields that will be masked,
//! and writes every other byte back unchanged.

pub mod copy;
pub mod values;

use std::io::{self, Write};
use std::ops::Range;

use crate::parser::state::{InsertFormat, SqlDialect};

/// A column value, with the type information the dump carries.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    /// Text, written as a string literal in VALUES.
    Text(String),
    /// A literal written as it is: a number, a boolean, a function call.
    Raw(String),
}

impl SqlValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqlValue::Null => None,
            SqlValue::Text(text) | SqlValue::Raw(text) => Some(text),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }
}

/// Decides which fields of a row are masked, and masks them.
pub trait FieldMasker {
    fn masks(&self, column: usize) -> bool;
    fn mask(&mut self, column: usize, value: SqlValue) -> SqlValue;
}

/// Writes `row` to `out`, replacing the fields that `masker` masks. Every
/// other byte (separators, untouched fields, the line ending) is copied as it is.
pub fn rewrite_row(
    row: &[u8],
    format: InsertFormat,
    dialect: SqlDialect,
    masker: &mut impl FieldMasker,
    out: &mut impl Write,
) -> io::Result<()> {
    let fields = match format {
        InsertFormat::Copy => copy::split(row),
        InsertFormat::Values => values::split(row, dialect),
    };

    let mut written = 0;
    for (column, field) in fields.into_iter().enumerate() {
        if !masker.masks(column) {
            continue;
        }

        // In VALUES the spaces around a field are layout, not data.
        let field = match format {
            InsertFormat::Copy => field,
            InsertFormat::Values => trim(row, field),
        };

        out.write_all(&row[written..field.start])?;
        let value = match format {
            InsertFormat::Copy => copy::decode(&row[field.clone()]),
            InsertFormat::Values => values::decode(&row[field.clone()], dialect),
        };
        let masked = masker.mask(column, value);
        match format {
            InsertFormat::Copy => copy::encode(&masked, out)?,
            InsertFormat::Values => values::encode(&masked, dialect, out)?,
        }
        written = field.end;
    }

    out.write_all(&row[written..])
}

fn trim(row: &[u8], field: Range<usize>) -> Range<usize> {
    let bytes = &row[field.clone()];
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |last| last + 1);
    field.start + start..field.start + end
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Masks the listed columns with a fixed value.
    struct Fixed(Vec<usize>, SqlValue);

    impl FieldMasker for Fixed {
        fn masks(&self, column: usize) -> bool {
            self.0.contains(&column)
        }

        fn mask(&mut self, _: usize, _: SqlValue) -> SqlValue {
            self.1.clone()
        }
    }

    fn rewrite(row: &[u8], format: InsertFormat, dialect: SqlDialect, masker: Fixed) -> String {
        let mut masker = masker;
        let mut out = Vec::new();
        rewrite_row(row, format, dialect, &mut masker, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_unmasked_fields_and_layout_are_copied() {
        let row = b"(1,  'Ana' , NULL)";
        let masked = rewrite(
            row,
            InsertFormat::Values,
            SqlDialect::Postgres,
            Fixed(vec![1], SqlValue::Text("O'Conner".into())),
        );
        assert_eq!(masked, "(1,  'O''Conner' , NULL)");
    }

    #[test]
    fn test_copy_row_keeps_line_ending() {
        let row = b"1\tAna\t\\N\r\n";
        let masked = rewrite(
            row,
            InsertFormat::Copy,
            SqlDialect::Postgres,
            Fixed(vec![1, 2], SqlValue::Null),
        );
        assert_eq!(masked, "1\t\\N\t\\N\r\n");
    }

    #[test]
    fn test_nothing_masked_copies_the_row() {
        let row = b"(1, 'a,b', f(2, 3))";
        let masked = rewrite(
            row,
            InsertFormat::Values,
            SqlDialect::Mysql,
            Fixed(vec![], SqlValue::Null),
        );
        assert_eq!(masked.as_bytes(), row);
    }
}
