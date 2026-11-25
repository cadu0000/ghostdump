//! The command line: its arguments, the exit statuses and the checks on
//! the HMAC secret.

use clap::{Parser, Subcommand, ValueEnum};
use std::num::NonZeroUsize;
use std::path::PathBuf;

/// The command line: masks a dump, unless a subcommand is given.
#[derive(Parser, Debug)]
#[command(
    name = "ghostdump",
    version,
    about = "High-Throughput SQL Stream Sanitizer",
    long_about = "Masks the values of a SQL dump in one streaming pass, with rules per column: deterministic pseudonyms, generated values and local differential privacy. Everything without a rule is copied byte for byte.",
    args_conflicts_with_subcommands = true
)]
pub struct Args {
    /// Without one, the dump is masked: `ghostdump -c rules.toml -i dump.sql`.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// The rules; without them the dump is copied as it is.
    #[arg(
        short = 'c',
        long,
        value_name = "FILE",
        help = "Rules file (TOML); without it the dump is copied as it is"
    )]
    pub config: Option<PathBuf>,

    /// The dump to read; stdin when absent.
    #[arg(
        short = 'i',
        long,
        value_name = "INPUT_FILE",
        help = "Input dump, decompressed when it ends in .gz (stdin when absent)"
    )]
    pub input: Option<PathBuf>,

    /// Where the masked dump is written; stdout when absent.
    #[arg(
        short = 'o',
        long,
        value_name = "OUTPUT_FILE",
        help = "Output dump, replaced only when the run succeeds (stdout when absent)"
    )]
    pub output: Option<PathBuf>,

    /// The HMAC secret, from the command line or `GHOSTDUMP_SECRET`.
    #[arg(
        short = 's',
        long,
        env = "GHOSTDUMP_SECRET",
        hide_env_values = true,
        help = "HMAC secret (32 or more random bytes), needed when the rules use hmac. Other users can read it in the process list: prefer --secret-file"
    )]
    pub secret: Option<String>,

    /// Kept out of the process list and the shell history, unlike `-s`.
    #[arg(
        long,
        value_name = "FILE",
        help = "Read the HMAC secret from a file (one line ending at its end is ignored)"
    )]
    pub secret_file: Option<PathBuf>,

    /// Logs debug messages.
    #[arg(short = 'v', long, action = clap::ArgAction::SetTrue, help = "Show debug messages")]
    pub verbose: bool,

    /// Reads and checks the input without writing the output.
    #[arg(
        short = 'd',
        long,
        help = "Read and check the input without writing the output"
    )]
    pub dry_run: bool,

    /// Stops after this many data rows.
    #[arg(
        short = 'l',
        long,
        value_name = "ROWS",
        help = "Stop after N data rows, closing the statement they belong to"
    )]
    pub limit: Option<usize>,

    /// Shows a progress bar on stderr.
    #[arg(short = 'p', long, help = "Show a progress bar on stderr")]
    pub progress: bool,

    /// The dialect of the dump, detected when `Auto`.
    #[arg(
        long,
        value_enum,
        default_value_t = DialectArg::Auto,
        help = "SQL dialect of the dump; auto detects it from its first lines"
    )]
    pub dialect: DialectArg,

    /// The largest data row, or statement held for checking, in MiB.
    #[arg(
        long,
        value_name = "MiB",
        default_value_t = 256,
        help = "Largest data row, or statement held for checking, in MiB"
    )]
    pub max_row_size: usize,

    /// Threads that mask rows.
    #[arg(
        short = 'j',
        long,
        value_name = "N",
        default_value = "1",
        help = "Threads that mask rows; reading and writing stay on the main thread"
    )]
    pub threads: NonZeroUsize,

    /// Without it, a rule that matches no table or column only warns.
    #[arg(
        long,
        help = "Fail when a rule matches no table or column of the input"
    )]
    pub strict: bool,

    /// Fixes every random choice, for reproducible experiments. Hidden: a
    /// known seed lets anyone recompute the generated values and the noise.
    #[arg(long, hide = true, value_name = "N")]
    pub seed: Option<u64>,

    /// Accepts `mechanism = "f64"`, kept to measure the cost of the exact
    /// sampler. Hidden: its guarantee holds only while rounding hides the
    /// low-order bits of floating-point noise.
    #[arg(long, hide = true)]
    pub experimental_f64_sampler: bool,
}

/// The subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Check rules against a dump without writing it: the rules applied,
    /// the tables copied as they are, the rules that match nothing, and
    /// what would stop a run
    Check(CheckArgs),
    /// Write a starting rules file from the tables and columns of a dump
    Init(InitArgs),
}

/// The arguments of `ghostdump check`.
#[derive(clap::Args, Debug)]
pub struct CheckArgs {
    /// The rules to check.
    #[arg(short = 'c', long, value_name = "FILE", help = "Rules file (TOML)")]
    pub config: PathBuf,

    /// The dump to check the rules against; stdin when absent.
    #[arg(
        short = 'i',
        long,
        value_name = "INPUT_FILE",
        help = "Input dump, decompressed when it ends in .gz (stdin when absent)"
    )]
    pub input: Option<PathBuf>,

    /// The dialect of the dump, detected when `Auto`.
    #[arg(
        long,
        value_enum,
        default_value_t = DialectArg::Auto,
        help = "SQL dialect of the dump; auto detects it from its first lines"
    )]
    pub dialect: DialectArg,

    /// The largest data row, or statement held for checking, in MiB.
    #[arg(
        long,
        value_name = "MiB",
        default_value_t = 256,
        help = "Largest data row, or statement held for checking, in MiB"
    )]
    pub max_row_size: usize,
}

/// The arguments of `ghostdump init`.
#[derive(clap::Args, Debug)]
pub struct InitArgs {
    /// The dump whose tables and columns are listed; stdin when absent.
    #[arg(
        short = 'i',
        long,
        value_name = "INPUT_FILE",
        help = "Input dump, decompressed when it ends in .gz (stdin when absent)"
    )]
    pub input: Option<PathBuf>,

    /// The rules file to create; stdout when absent.
    #[arg(
        short = 'o',
        long,
        value_name = "FILE",
        help = "Rules file to write, which must not exist yet (stdout when absent)"
    )]
    pub output: Option<PathBuf>,

    /// The dialect of the dump, detected when `Auto`.
    #[arg(
        long,
        value_enum,
        default_value_t = DialectArg::Auto,
        help = "SQL dialect of the dump; auto detects it from its first lines"
    )]
    pub dialect: DialectArg,

    /// The largest data row, or statement held for checking, in MiB.
    #[arg(
        long,
        value_name = "MiB",
        default_value_t = 256,
        help = "Largest data row, or statement held for checking, in MiB"
    )]
    pub max_row_size: usize,
}

/// The dialect named on the command line.
#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum DialectArg {
    /// Detect it from the first lines of the dump
    Auto,
    /// PostgreSQL, as pg_dump writes it
    Postgres,
    /// MySQL, as mysqldump writes it
    Mysql,
    /// SQLite, as the sqlite3 .dump command writes it
    Sqlite,
}

/// Secrets shorter than this, in bytes, trigger a warning.
pub const MIN_SECRET_BYTES: usize = 16;

/// Why no HMAC secret can be used.
#[derive(Debug, PartialEq)]
pub enum SecretError {
    /// None was given.
    Missing,
    /// It is empty or only whitespace.
    Empty,
}

/// Exit statuses of a run that fails, from sysexits.h. A run that succeeds
/// exits with 0.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitStatus {
    /// The command line is wrong (`EX_USAGE`).
    Usage = 64,
    /// The input cannot be masked safely, or is not a dump GhostDump reads
    /// (`EX_DATAERR`).
    Input = 65,
    /// Reading or writing a file failed (`EX_IOERR`).
    Io = 74,
    /// The rules are invalid, or match nothing with `--strict` (`EX_CONFIG`).
    Rules = 78,
}

/// Whether a secret that can be used is long enough.
#[derive(Debug, PartialEq)]
pub enum SecretLength {
    /// At least [`MIN_SECRET_BYTES`] bytes.
    Recommended,
    /// Shorter than [`MIN_SECRET_BYTES`] bytes: used, with a warning.
    Short,
}

/// Validates the HMAC secret. A blank secret is rejected, because it makes
/// pseudonyms of low-entropy values (IDs, CPFs) trivial to brute-force.
pub fn check_secret(secret: Option<&str>) -> Result<SecretLength, SecretError> {
    let secret = secret.ok_or(SecretError::Missing)?;

    if secret.trim().is_empty() {
        Err(SecretError::Empty)
    } else if secret.len() < MIN_SECRET_BYTES {
        Ok(SecretLength::Short)
    } else {
        Ok(SecretLength::Recommended)
    }
}

/// The secret held in a file: its text, without one line ending at its
/// end, which editors and `echo` add. `None` when it is not UTF-8.
pub fn secret_from_file(contents: Vec<u8>) -> Option<String> {
    let text = String::from_utf8(contents).ok()?;
    let secret = text
        .strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .unwrap_or(&text);
    Some(secret.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_and_blank_secrets_are_rejected() {
        assert_eq!(check_secret(None), Err(SecretError::Missing));
        assert_eq!(check_secret(Some("")), Err(SecretError::Empty));
        assert_eq!(check_secret(Some("   ")), Err(SecretError::Empty));
    }

    #[test]
    fn test_threads() {
        let parse = |args: &[&str]| Args::try_parse_from([&["ghostdump"], args].concat());
        assert_eq!(parse(&[]).unwrap().threads.get(), 1);
        assert_eq!(parse(&["-j", "4"]).unwrap().threads.get(), 4);
        assert!(parse(&["--threads", "0"]).is_err());
    }

    #[test]
    fn test_subcommands_keep_the_default_form() {
        let parse = |args: &[&str]| Args::try_parse_from([&["ghostdump"], args].concat());
        let masked = parse(&["-c", "rules.toml", "-i", "dump.sql"]).unwrap();
        assert!(masked.command.is_none());
        let check = parse(&["check", "-c", "rules.toml", "-i", "dump.sql"]).unwrap();
        assert!(matches!(check.command, Some(Command::Check(_))));
        assert!(matches!(
            parse(&["init", "-i", "dump.sql"]).unwrap().command,
            Some(Command::Init(_))
        ));
        assert!(parse(&["-c", "rules.toml", "check", "-c", "rules.toml"]).is_err());
        assert!(parse(&["check", "-i", "dump.sql"]).is_err());
    }

    #[test]
    fn test_secret_files_lose_one_line_ending() {
        let read = |contents: &[u8]| secret_from_file(contents.to_vec());
        assert_eq!(read(b"s3cret\n").as_deref(), Some("s3cret"));
        assert_eq!(read(b"s3cret\r\n").as_deref(), Some("s3cret"));
        assert_eq!(read(b"s3cret").as_deref(), Some("s3cret"));
        assert_eq!(read(b"s3cret\n\n").as_deref(), Some("s3cret\n"));
        assert_eq!(read(b" s3cret ").as_deref(), Some(" s3cret "));
        assert_eq!(read(&[0xff, b'a']), None);
    }

    #[test]
    fn test_secret_length() {
        assert_eq!(check_secret(Some("short")), Ok(SecretLength::Short));
        assert_eq!(
            check_secret(Some("0123456789abcdef")),
            Ok(SecretLength::Recommended)
        );
    }
}
