pub mod config;
pub mod strategy;

use rand::{Rng, thread_rng};
use std::io::{Read, Write};

use crate::engine::config::Config;
use crate::engine::strategy::StrategyConfig;
use crate::io::{InputSource, OutputSource};
use crate::parser::state::{InsertFormat, SqlDialect};
use crate::parser::tokenizer::{join_tuple, split_tuple};
use crate::parser::{SqlEvent, SqlParser};

pub struct AnonymizerEngine {
    config: Config,
    secret: String,
}

impl AnonymizerEngine {
    pub fn new(config: Config, secret: String) -> Self {
        Self { config, secret }
    }

    pub fn build_strategy_map<'a>(
        &'a self,
        table_name: &str,
        parsed_columns: &[String],
    ) -> Vec<Option<&'a StrategyConfig>> {
        let mut strategy_map = vec![None; parsed_columns.len()];

        if let Some(table_config) = self.config.tables.get(table_name) {
            for (i, col_name) in parsed_columns.iter().enumerate() {
                if let Some(col_config) = table_config.columns.iter().find(|c| c.name == *col_name)
                {
                    strategy_map[i] = Some(&col_config.strategy);
                }
            }
        }

        strategy_map
    }

    /// O(1)
    pub fn process_row(
        &self,
        row_values: Vec<String>,
        strategy_map: &[Option<&StrategyConfig>],
        rng: &mut impl Rng,
    ) -> Vec<String> {
        row_values
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                if let Some(strategy) = strategy_map.get(i).copied().flatten() {
                    strategy
                        .apply(Some(&value), rng, &self.secret)
                        .unwrap_or(value)
                } else {
                    value
                }
            })
            .collect()
    }

    pub fn process_dump(
        &self,
        input: InputSource,
        output: OutputSource,
        dialect: SqlDialect,
        dry_run: bool,
        limit: Option<usize>,
    ) -> std::io::Result<usize> {
        let mut reader = input.into_buffered();
        let mut writer = output.into_buffered();
        let mut parser = SqlParser::new(dialect);
        let mut rng = thread_rng(); 

        let mut buffer = [0u8; 64 * 1024];
        let mut rows_processed = 0;
        let mut is_first_tuple = true;

        let mut active_strategy_map: Vec<Option<&StrategyConfig>> = Vec::new();

        'leitura: loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            for &byte in &buffer[..bytes_read] {
                if let Some(event) = parser.handle_byte(byte) {
                    match event {
                        SqlEvent::DefaultStatement(b) => {
                            if !dry_run {
                                writer.write_all(&[b])?;
                            }
                        }
                        
                        SqlEvent::Header {
                            raw_bytes,
                            table_name,
                            columns,
                        } => {
                            is_first_tuple = true;

                            if let (Some(tbl), Some(cols)) = (table_name, columns) {
                                active_strategy_map = self.build_strategy_map(&tbl, &cols);
                            } else {
                                active_strategy_map = Vec::new();
                            }

                            if !dry_run {
                                writer.write_all(&raw_bytes)?;
                                writer.write_all(b"\n")?;
                            }
                        }

                        SqlEvent::Tuple(tuple_bytes, format) => {
                            let mut columns = split_tuple(&tuple_bytes, format);
                            let num_cols = columns.len();

                            let mut string_cols = Vec::with_capacity(num_cols);
                            for col in &columns {
                                let raw_string = String::from_utf8_lossy(col);
                                let clean_string = raw_string
                                    .trim()
                                    .trim_matches(|c| c == '\'' || c == '(' || c == ')')
                                    .to_string();
                                string_cols.push(clean_string);
                            }

                            let anonymized_cols =
                                self.process_row(string_cols, &active_strategy_map, &mut rng);

                            for (i, (col, anon_val)) in
                                columns.iter_mut().zip(anonymized_cols).enumerate()
                            {
                                if format == InsertFormat::Values {
                                    let prefix = if i == 0 { "(" } else { " " };
                                    let suffix = if i == num_cols - 1 { ")" } else { "" };
                                    *col =
                                        format!("{}'{}'{}", prefix, anon_val, suffix).into_bytes();
                                } else {
                                    *col = anon_val.into_bytes();
                                }
                            }

                            let modified_tuple = join_tuple(&columns, format);

                            if !dry_run {
                                if format == InsertFormat::Values && !is_first_tuple {
                                    writer.write_all(b",\n")?;
                                }
                                writer.write_all(&modified_tuple)?;
                            }

                            is_first_tuple = false;
                            rows_processed += 1;

                            if let Some(l) = limit {
                                if rows_processed >= l {
                                    break 'leitura;
                                }
                            }
                        }

                        SqlEvent::Footer(footer_bytes) => {
                            if !dry_run {
                                writer.write_all(&footer_bytes)?;
                                writer.write_all(b"\n\n")?;
                            }
                        }
                    }
                }
            }
        }

        if !dry_run {
            writer.flush()?;
        }
        Ok(rows_processed)
    }
}
