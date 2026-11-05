use clap::Parser;
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
        help = "Limita o processamento a N linhas de INSERT"
    )]
    pub limit: Option<usize>,

    #[arg(short = 'p', long, help = "Exibe uma barra de progresso no terminal")]
    pub progress: bool,
}

/// Secrets shorter than this, in bytes, trigger a warning.
pub const MIN_SECRET_BYTES: usize = 16;

#[derive(Debug, PartialEq)]
pub enum SecretError {
    Missing,
    Empty,
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
    fn test_secret_length() {
        assert_eq!(check_secret(Some("short")), Ok(SecretLength::Short));
        assert_eq!(
            check_secret(Some("0123456789abcdef")),
            Ok(SecretLength::Recommended)
        );
    }
}
