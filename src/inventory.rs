//! The tables and columns a dump shows, and a starting rules file written
//! from them (`ghostdump init`).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, Read};

use crate::parser::state::SqlDialect;
use crate::parser::{HeaderMetadata, SqlEvent, SqlParser, Statement, TableChange};

/// A table of the dump, with its columns in the order they first appear.
#[derive(Debug, Clone, PartialEq)]
pub struct TableColumns {
    /// The schema (or, in MySQL, the database) written before the table
    /// name, if any.
    pub schema: Option<String>,
    /// The name of the table.
    pub table: String,
    /// Empty when the dump shows none: no `CREATE TABLE` that lists them,
    /// and rows without a column list.
    pub columns: Vec<String>,
    /// Whether the dump has rows of the table.
    pub rows: bool,
    /// SQLite's own tables (`sqlite_sequence`), whose names SQLite keeps
    /// for itself.
    pub internal: bool,
}

/// Reads the tables of a dump from its `CREATE TABLE` statements and the
/// column lists of its `INSERT` and `COPY` statements, which also show the
/// columns that a later `ALTER TABLE` added. Rows are skipped, so memory
/// grows with the schema only.
pub fn read_tables(
    input: impl Read,
    dialect: SqlDialect,
    max_row_bytes: usize,
) -> io::Result<Vec<TableColumns>> {
    let mut parser = SqlParser::new(dialect).with_max_row_bytes(max_row_bytes);
    // By the key of the table, in the order tables first appear.
    let mut tables: Vec<(String, TableColumns)> = Vec::new();
    let mut add = |key: &str, schema: &Option<String>, table: &str, columns: &[String], rows| {
        let at = match tables.iter().position(|(known, _)| known == key) {
            Some(at) => at,
            None => {
                tables.push((
                    key.to_string(),
                    TableColumns {
                        schema: schema.clone(),
                        table: table.to_string(),
                        columns: Vec::new(),
                        rows: false,
                        internal: dialect == SqlDialect::Sqlite
                            && table.to_ascii_lowercase().starts_with("sqlite_"),
                    },
                ));
                tables.len() - 1
            }
        };
        tables[at].1.rows |= rows;
        let known = &mut tables[at].1.columns;
        for column in columns {
            if !known.iter().any(|name| name.eq_ignore_ascii_case(column)) {
                known.push(column.clone());
            }
        }
    };

    let mut buffer = [0u8; 64 * 1024];
    let mut input = input;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        for &byte in &buffer[..read] {
            match parser.handle_byte(byte) {
                Some(SqlEvent::Statement(bytes)) => {
                    if let Some(TableChange::Created { table, columns }) =
                        Statement::parse(&bytes, dialect).table_change()
                    {
                        add(
                            &table.key,
                            &table.schema,
                            &table.table,
                            columns.as_deref().unwrap_or_default(),
                            false,
                        );
                    }
                }
                Some(SqlEvent::Header {
                    metadata: Some(metadata),
                    ..
                }) => {
                    let HeaderMetadata {
                        schema,
                        table,
                        key,
                        columns,
                    } = metadata.as_ref();
                    add(
                        key,
                        schema,
                        table,
                        columns.as_deref().unwrap_or_default(),
                        true,
                    );
                }
                Some(SqlEvent::Error(e)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
                }
                _ => {}
            }
        }
    }
    Ok(tables.into_iter().map(|(_, table)| table).collect())
}

/// A strategy for a column whose name suggests personal data, picked from
/// those that need no secret and keep the format of the value. `None`
/// for any other column.
pub fn suggested_strategy(column: &str) -> Option<&'static str> {
    let name = column.to_ascii_lowercase();
    let words: Vec<&str> = name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect();
    let has = |wanted: &[&str]| words.iter().any(|word| wanted.contains(word));
    let names = [
        "name",
        "nome",
        "firstname",
        "lastname",
        "fullname",
        "surname",
        "sobrenome",
    ];
    if name.contains("email") || name.contains("e_mail") {
        Some("faker_email")
    } else if has(&["cpf"]) {
        Some("cpf")
    } else if has(&["cnpj"]) {
        Some("cnpj")
    } else if has(&["phone", "telefone", "celular", "fone", "mobile"]) {
        Some("faker_phone_br")
    } else if has(&["ip", "ipaddr"]) || name.contains("ip_address") {
        Some("faker_ip")
    } else if name.contains("credit_card") || name.contains("card_number") || has(&["cartao"]) {
        Some("fake_credit_card")
    } else if words.len() <= 2 && words.last().is_some_and(|word| names.contains(word)) {
        // `name`, `first_name`, `nome`; not `user_name` or `table_name`.
        match words.first() {
            Some(&("user" | "username" | "table" | "file" | "product" | "company")) => None,
            _ => Some("faker_name"),
        }
    } else {
        None
    }
}

/// A rules file with every table and column of `tables`. Columns whose
/// names suggest personal data have a rule; the others are commented out,
/// and so are the tables left without a rule, those the dump has no rows
/// of and SQLite's own tables, so that every rule matches the dump and the
/// file passes `ghostdump check`.
pub fn starting_rules(tables: &[TableColumns], source: &str) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Rules for GhostDump, written by `ghostdump init` from {source}.\n\
         # Every table and column the dump shows is listed. Columns whose names\n\
         # suggest personal data have a rule; the others are commented out, and\n\
         # a column without a rule is copied as it is: review every line.\n\
         # Strategies: hmac, dp_laplace, randomized_response, faker_name,\n\
         # faker_email, faker_phone_br, faker_ip, fake_credit_card, random_uuid,\n\
         # cpf, cnpj, fixed, random_choice, random_string, nullify (see the README).\n\
         \n\
         [tables]"
    );
    // A table name found in two schemas gets a rule per schema.
    let mut schemas: BTreeMap<&str, usize> = BTreeMap::new();
    for table in tables {
        *schemas.entry(table.table.as_str()).or_default() += 1;
    }
    for table in tables {
        let name = match &table.schema {
            Some(schema) if schemas[table.table.as_str()] > 1 => {
                format!("{schema}.{}", table.table)
            }
            _ => table.table.clone(),
        };
        let lines: Vec<(bool, String)> = table
            .columns
            .iter()
            .map(|column| match suggested_strategy(column) {
                Some(strategy) => (
                    true,
                    format!(
                        "{{ name = {}, strategy = \"{strategy}\" }},",
                        quoted(column)
                    ),
                ),
                None => (
                    false,
                    format!("{{ name = {}, strategy = \"nullify\" }},", quoted(column)),
                ),
            })
            .collect();
        let active = table.rows && !table.internal && lines.iter().any(|(active, _)| *active);
        let comment = if active { "" } else { "# " };
        out.push('\n');
        let _ = writeln!(out, "{comment}[tables.{}]", key(&name));
        if table.internal {
            let _ = writeln!(out, "# A table of SQLite itself.");
        } else if !table.rows {
            let _ = writeln!(out, "# The dump has no rows of this table.");
        }
        if table.columns.is_empty() {
            let _ = writeln!(
                out,
                "# The dump shows no column of this table: name them in the rules."
            );
            let _ = writeln!(out, "{comment}columns = []");
            continue;
        }
        let _ = writeln!(out, "{comment}columns = [");
        for (rule, line) in &lines {
            let indent = if *rule { "    " } else { "    # " };
            let _ = writeln!(out, "{comment}{indent}{line}");
        }
        let _ = writeln!(out, "{comment}]");
    }
    out
}

/// A TOML key: bare when it can be, quoted otherwise.
fn key(name: &str) -> String {
    let bare = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if bare { name.to_string() } else { quoted(name) }
}

/// A TOML basic string.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::config::Config;

    fn tables(sql: &str, dialect: SqlDialect) -> Vec<TableColumns> {
        read_tables(sql.as_bytes(), dialect, 1 << 20).unwrap()
    }

    #[test]
    fn test_tables_come_from_create_table_and_column_lists() {
        let found = tables(
            "CREATE TABLE public.users (id int, name text, email text);\n\
             COPY public.users (id, name, email, phone) FROM stdin;\n1\tAna\ta@x\t9\n\\.\n\
             INSERT INTO orders (id, total) VALUES (1, 2);\n\
             INSERT INTO logs VALUES ('x');\n\
             CREATE TABLE empty (name text);\n",
            SqlDialect::Postgres,
        );
        let names: Vec<_> = found
            .iter()
            .map(|t| (t.table.as_str(), t.columns.join(","), t.rows))
            .collect();
        assert_eq!(
            names,
            [
                ("users", "id,name,email,phone".to_string(), true),
                ("orders", "id,total".to_string(), true),
                ("logs", String::new(), true),
                ("empty", "name".to_string(), false),
            ]
        );
        assert_eq!(found[0].schema.as_deref(), Some("public"));
    }

    #[test]
    fn test_sqlite_tables_are_internal() {
        let found = tables(
            "CREATE TABLE users (id INTEGER, name TEXT);\n\
             INSERT INTO users VALUES(1,'Ana');\n\
             DELETE FROM sqlite_sequence;\n\
             INSERT INTO sqlite_sequence(rowid,name,seq) VALUES(1,'users',1);\n",
            SqlDialect::Sqlite,
        );
        let internal: Vec<_> = found
            .iter()
            .map(|t| (t.table.as_str(), t.internal))
            .collect();
        assert_eq!(internal, [("users", false), ("sqlite_sequence", true)]);
        let rules = starting_rules(&found, "dump.sql");
        assert!(rules.contains("\n[tables.users]\n"), "{rules}");
        assert!(rules.contains("# [tables.sqlite_sequence]\n# A table of SQLite itself."));
    }

    #[test]
    fn test_suggestions_follow_the_column_name() {
        for (column, strategy) in [
            ("email", Some("faker_email")),
            ("contact_email", Some("faker_email")),
            ("CPF", Some("cpf")),
            ("cnpj_empresa", Some("cnpj")),
            ("phone", Some("faker_phone_br")),
            ("telefone", Some("faker_phone_br")),
            ("ip", Some("faker_ip")),
            ("credit_card_number", Some("fake_credit_card")),
            ("name", Some("faker_name")),
            ("first_name", Some("faker_name")),
            ("nome", Some("faker_name")),
            ("user_name", None),
            ("table_name", None),
            ("zip", None),
            ("id", None),
            ("salary", None),
        ] {
            assert_eq!(suggested_strategy(column), strategy, "{column}");
        }
    }

    #[test]
    fn test_starting_rules_compile_and_name_every_column() {
        let table = |schema: Option<&str>, name: &str, columns: &[&str], rows| TableColumns {
            schema: schema.map(str::to_string),
            table: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows,
            internal: false,
        };
        let found = vec![
            table(
                Some("public"),
                "users",
                &["id", "name", "e-mail \"x\""],
                true,
            ),
            table(Some("audit"), "users", &["note"], true),
            table(None, "my table", &[], true),
            table(None, "people", &["name"], false),
        ];
        let rules = starting_rules(&found, "dump.sql");
        let config = toml::from_str::<Config>(&rules).unwrap().compile().unwrap();
        assert_eq!(config.tables.len(), 1, "{rules}");
        let (key, columns) = config.rules_for(Some("public"), "users").unwrap();
        assert_eq!(key, "public.users");
        let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["name"]);
        assert!(rules.contains("    # { name = \"id\", strategy = \"nullify\" },"));
        assert!(rules.contains("# [tables.\"audit.users\"]"));
        assert!(rules.contains("# [tables.\"my table\"]"));
        assert!(rules.contains("\"e-mail \\\"x\\\"\""), "{rules}");
        assert!(rules.contains("# [tables.people]\n# The dump has no rows of this table."));

        let none = starting_rules(&[table(None, "logs", &[], true)], "dump.sql");
        let config = toml::from_str::<Config>(&none).unwrap().compile().unwrap();
        assert!(config.tables.is_empty());
    }
}
