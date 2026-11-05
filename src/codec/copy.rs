//! PostgreSQL `COPY ... FROM stdin` text format: tab-separated fields,
//! `\N` for NULL and backslash escapes.

use std::io::{self, Write};
use std::ops::Range;

use super::SqlValue;

/// Field ranges of a COPY line, without its line ending.
pub fn split(row: &[u8]) -> Vec<Range<usize>> {
    let end = content_end(row);
    let mut fields = Vec::new();
    let mut start = 0;

    for tab in memchr::memchr_iter(b'\t', &row[..end]) {
        fields.push(start..tab);
        start = tab + 1;
    }
    fields.push(start..end);
    fields
}

fn content_end(row: &[u8]) -> usize {
    if row.ends_with(b"\r\n") {
        row.len() - 2
    } else if row.ends_with(b"\n") {
        row.len() - 1
    } else {
        row.len()
    }
}

pub fn decode(field: &[u8]) -> SqlValue {
    if field == b"\\N" {
        return SqlValue::Null;
    }
    if !field.contains(&b'\\') {
        return SqlValue::Text(String::from_utf8_lossy(field).into_owned());
    }

    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        let byte = field[i];
        if byte != b'\\' || i + 1 == field.len() {
            out.push(byte);
            i += 1;
            continue;
        }
        let next = field[i + 1];
        i += 2;
        match next {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'0'..=b'7' => {
                // Up to three octal digits.
                let mut value = u32::from(next - b'0');
                let mut digits = 1;
                while digits < 3 && i < field.len() && (b'0'..=b'7').contains(&field[i]) {
                    value = value * 8 + u32::from(field[i] - b'0');
                    i += 1;
                    digits += 1;
                }
                out.push(value as u8);
            }
            b'x' if i < field.len() && field[i].is_ascii_hexdigit() => {
                // One or two hex digits.
                let mut value = hex_value(field[i]);
                i += 1;
                if i < field.len() && field[i].is_ascii_hexdigit() {
                    value = value * 16 + hex_value(field[i]);
                    i += 1;
                }
                out.push(value);
            }
            other => out.push(other),
        }
    }
    SqlValue::Text(String::from_utf8_lossy(&out).into_owned())
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => byte - b'A' + 10,
    }
}

pub fn encode(value: &SqlValue, out: &mut impl Write) -> io::Result<()> {
    let Some(text) = value.as_str() else {
        return out.write_all(b"\\N");
    };
    let bytes = text.as_bytes();
    let mut start = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        let escaped: &[u8] = match byte {
            b'\\' => b"\\\\",
            b'\t' => b"\\t",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            _ => continue,
        };
        out.write_all(&bytes[start..i])?;
        out.write_all(escaped)?;
        start = i + 1;
    }
    out.write_all(&bytes[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(value: &SqlValue) -> String {
        let mut out = Vec::new();
        encode(value, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_split_keeps_empty_fields() {
        let row = b"1\t\tx\n";
        let fields: Vec<&[u8]> = split(row).into_iter().map(|r| &row[r]).collect();
        assert_eq!(fields, [&b"1"[..], b"", b"x"]);
    }

    #[test]
    fn test_decode_escapes_and_null() {
        assert_eq!(decode(b"\\N"), SqlValue::Null);
        assert_eq!(decode(b"a\\tb\\\\c\\n"), SqlValue::Text("a\tb\\c\n".into()));
        assert_eq!(decode(b"\\101\\x42"), SqlValue::Text("AB".into()));
    }

    #[test]
    fn test_encode_round_trip() {
        let value = SqlValue::Text("tab\there\\ and\nnewline".into());
        assert_eq!(decode(encoded(&value).as_bytes()), value);
        assert_eq!(encoded(&SqlValue::Null), "\\N");
    }
}
