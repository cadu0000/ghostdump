use regex::Regex;
use std::sync::OnceLock;

pub fn extract_metadata(header_bytes: &[u8]) -> Option<(String, Option<Vec<String>>)> {
    let header_str = String::from_utf8_lossy(header_bytes);

    static INSERT_RE: OnceLock<Regex> = OnceLock::new();
    static COPY_RE: OnceLock<Regex> = OnceLock::new();

    let insert_re = INSERT_RE.get_or_init(|| {
        Regex::new(r"(?i)INSERT\s+INTO\s+(?P<table>[^\s\(]+)(?:\s*\((?P<cols>[^\)]+)\))?\s*VALUES")
            .unwrap()
    });

    let copy_re = COPY_RE.get_or_init(|| {
        Regex::new(r"(?i)COPY\s+(?P<table>[^\s\(]+)(?:\s*\((?P<cols>[^\)]+)\))?\s*FROM\s+STDIN;")
            .unwrap()
    });

    let process_match = |caps: regex::Captures| -> (String, Option<Vec<String>>) {
        let raw_table = caps.name("table").unwrap().as_str();

        let table = raw_table
            .split('.')
            .last()
            .unwrap_or(raw_table)
            .trim_matches(|c| c == '"' || c == '`' || c == '\'')
            .to_string();

        let cols = caps.name("cols").map(|m| {
            m.as_str()
                .split(',')
                .map(|s| {
                    s.trim_matches(|c| c == '"' || c == '`' || c == '\'' || c == ' ' || c == '\n')
                        .to_string()
                })
                .collect()
        });

        (table, cols)
    };

    if let Some(caps) = insert_re.captures(&header_str) {
        return Some(process_match(caps));
    }

    if let Some(caps) = copy_re.captures(&header_str) {
        return Some(process_match(caps));
    }

    None
}
