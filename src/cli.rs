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
    #[arg(short = 'c', long, value_name = "FILE", help = "Define o caminho do arquivo de configuração TOML")]
    pub config: Option<PathBuf>,

    #[arg(short = 'i', long, value_name = "INPUT_FILE", help = "Define o caminho do arquivo de entrada")]
    pub input: Option<PathBuf>,

    #[arg(short = 'o', long, value_name = "OUTPUT_FILE", help = "Define o caminho do arquivo de saída")]
    pub output: Option<PathBuf>,

    #[arg(short = 's', long, env = "GHOSTDUMP_SECRET", hide_env_values = true, help = "Define uma nova secret_key")]
    pub secret: String, 

    #[arg(short = 'v', long, action = clap::ArgAction::SetTrue, help = "Ativa logs detalhados (Debug)")]
    pub verbose: bool,
    
    #[arg(short = 'd', long, help = "Processa o arquivo sem escrever a saída, apenas validando erros e logs")]
    pub dry_run: bool,
    
    #[arg(short = 'l', long, value_name = "ROWS", help = "Limita o processamento a N linhas de INSERT")]
    pub limit: Option<usize>,
    
    #[arg(short = 'p', long, help = "Exibe uma barra de progresso no terminal")]
    pub progress: bool,
}