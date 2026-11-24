use clap::{Parser, ValueEnum};
use std::num::NonZeroUsize;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "ghostdump",
    version,
    about = "High-Throughput SQL Stream Sanitizer",
    long_about = "Ferramenta CLI para anonimização e pseudonimização determinística de dumps SQL em alta performance."
)]
pub struct Args {
    #[arg(
        short = 'c',
        long,
        value_name = "FILE",
        help = "Define o caminho do arquivo de configuração TOML"
    )]
    pub config: Option<PathBuf>,

    #[arg(
        short = 'i',
        long,
        value_name = "INPUT_FILE",
        help = "Define o caminho do arquivo de entrada"
    )]
    pub input: Option<PathBuf>,

    #[arg(
        short = 'o',
        long,
        value_name = "OUTPUT_FILE",
        help = "Define o caminho do arquivo de saída"
    )]
    pub output: Option<PathBuf>,

    #[arg(
        short = 's',
        long,
        env = "GHOSTDUMP_SECRET",
        hide_env_values = true,
        help = "Chave secreta do HMAC; obrigatória quando as regras usam hmac (use 32 ou mais bytes aleatórios)"
    )]
    pub secret: Option<String>,

    /// Kept out of the process list and the shell history, unlike `-s`.
    #[arg(
        long,
        value_name = "FILE",
        help = "Read the HMAC secret from a file (one line ending at its end is ignored)"
    )]
    pub secret_file: Option<PathBuf>,

    #[arg(short = 'v', long, action = clap::ArgAction::SetTrue, help = "Ativa logs detalhados (Debug)")]
    pub verbose: bool,

    #[arg(
        short = 'd',
        long,
        help = "Processa o arquivo sem escrever a saída, apenas validando erros e logs"
    )]
    pub dry_run: bool,

    #[arg(
        short = 'l',
        long,
        value_name = "ROWS",
        help = "Para depois de N linhas de dados, fechando o comando em que estão"
    )]
    pub limit: Option<usize>,

    #[arg(short = 'p', long, help = "Exibe uma barra de progresso no terminal")]
    pub progress: bool,

    #[arg(
        long,
        value_enum,
        default_value_t = DialectArg::Auto,
        help = "Dialeto SQL do dump; auto detecta pelo cabeçalho"
    )]
    pub dialect: DialectArg,

    #[arg(
        long,
        value_name = "MiB",
        default_value_t = 256,
        help = "Tamanho máximo de uma linha de dados, em MiB"
    )]
    pub max_row_size: usize,

    #[arg(
        short = 'j',
        long,
        value_name = "N",
        default_value = "1",
        help = "Threads que mascaram as linhas; a leitura e a escrita ficam na thread principal"
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

#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum DialectArg {
    Auto,
    Postgres,
    Mysql,
    Sqlite,
}

/// Secrets shorter than this, in bytes, trigger a warning.
pub const MIN_SECRET_BYTES: usize = 16;

#[derive(Debug, PartialEq)]
pub enum SecretError {
    Missing,
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

#[derive(Debug, PartialEq)]
pub enum SecretLength {
    Recommended,
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
