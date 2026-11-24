use ghostdump::cli::{
    Args, DialectArg, ExitStatus, MIN_SECRET_BYTES, SecretError, SecretLength, check_secret,
    secret_from_file,
};
use ghostdump::engine::config::{CompiledConfig, Config};
use ghostdump::engine::{AnonymizerEngine, RunOptions, RunReport};
use ghostdump::io::{InputSource, OutputSource};
use ghostdump::parser::detect_dialect;
use ghostdump::parser::state::SqlDialect;

use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches};
use indicatif::{ProgressBar, ProgressStyle};
use log::{Level, LevelFilter, debug, error, info, warn};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant};

// With glibc's allocator, the threads of --threads end up sharing the main
// thread's arena and wait on its lock (see docs/benchmarks.md).
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Why a run stopped: its exit status, and the message for the user.
struct Failure(ExitStatus, String);

impl Failure {
    fn io(context: &str, error: io::Error) -> Self {
        Failure(ExitStatus::Io, format!("{context}: {error}"))
    }
}

// Every message goes to stderr: stdout may carry the sanitized dump.
fn main() -> ExitCode {
    let matches = match Args::command().try_get_matches() {
        Ok(matches) => matches,
        Err(e) if e.use_stderr() => {
            let _ = e.print();
            return ExitCode::from(ExitStatus::Usage as u8);
        }
        // --help and --version.
        Err(e) => e.exit(),
    };
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    init_logger(args.verbose);

    let secret_on_command_line = matches.value_source("secret") == Some(ValueSource::CommandLine);
    match run(&args, secret_on_command_line) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure(status, message)) => {
            error!("{message}");
            ExitCode::from(status as u8)
        }
    }
}

fn run(args: &Args, secret_on_command_line: bool) -> Result<(), Failure> {
    info!("Starting GhostDump...");
    if secret_on_command_line {
        warn!(
            "the secret given with -s can be read by other users of this machine in the \
             process list, and stays in the shell history. Use --secret-file or \
             GHOSTDUMP_SECRET."
        );
    }

    let config = match &args.config {
        Some(path) => load_rules(path, args.experimental_f64_sampler)?,
        None => {
            info!(
                "No rules (-c) provided. GhostDump will perform an exact copy of the original data."
            );
            CompiledConfig::empty()
        }
    };

    let secret = if config.uses_secret() {
        resolve_secret(args)?
    } else {
        String::new()
    };

    let input = InputSource::new(args.input.clone())
        .map_err(|e| Failure::io("could not open the input", e))?;
    let progress = args.progress.then(|| progress_bar(args.input.as_deref()));
    let input: Box<dyn Read> = match &progress {
        Some(bar) => Box::new(bar.wrap_read(input)),
        None => Box::new(input),
    };
    let mut input = BufReader::with_capacity(64 * 1024, input);
    let dialect = resolve_dialect(args.dialect, &mut input)?;
    // --dry-run writes nothing, so it does not even open the output file.
    let output_path = if args.dry_run {
        None
    } else {
        args.output.as_deref()
    };
    let (output, pending) =
        OutputSource::open(output_path).map_err(|e| Failure::io("could not open the output", e))?;
    // What an error means for an output file: it is written only at the end.
    let not_written = match &pending {
        Some(pending) => format!(" {} was not written.", pending.target().display()),
        None => String::new(),
    };
    let engine = AnonymizerEngine::new(config, secret);
    let options = RunOptions {
        dry_run: args.dry_run,
        limit: args.limit,
        max_row_bytes: args.max_row_size.saturating_mul(1024 * 1024),
        seed: args.seed,
        threads: args.threads.get(),
        ..RunOptions::new(dialect)
    };
    debug!("masking threads: {}", options.threads);

    if args.seed.is_some() {
        warn!(
            "--seed makes every random choice reproducible: anyone with the seed can recompute \
             the generated values and the noise. Use it for experiments, never on real data."
        );
    }

    if args.dry_run {
        info!("Dry-Run mode enabled. Data will be processed, but nothing will be written to disk.");
    }

    let start_time = Instant::now();
    let report = engine.process_dump(input, output, &options).map_err(|e| {
        let status = match e.kind() {
            io::ErrorKind::InvalidData => ExitStatus::Input,
            _ => ExitStatus::Io,
        };
        Failure(status, format!("{e}{not_written}"))
    })?;
    let duration = start_time.elapsed();
    if let Some(bar) = &progress {
        bar.finish_and_clear();
    }

    for rule in &report.unused_rules {
        warn!("unused rule: {rule}. Check the name: a misspelt rule leaves its column unmasked.");
    }
    if args.strict && !report.unused_rules.is_empty() {
        let written = match pending {
            Some(_) => not_written,
            None => " The output was written, but columns may be unmasked.".to_string(),
        };
        return Err(Failure(
            ExitStatus::Rules,
            format!(
                "{} rule(s) matched nothing in the input (--strict).{written}",
                report.unused_rules.len()
            ),
        ));
    }
    if let Some(pending) = pending {
        pending
            .commit()
            .map_err(|e| Failure::io("could not replace the output file", e))?;
    }

    log_report(&report, duration);
    Ok(())
}

/// The summary of a run that succeeded: counts and speed, never values.
fn log_report(report: &RunReport, duration: Duration) {
    info!("Processing completed successfully!");
    info!("Rows processed: {}", report.rows_processed);
    info!(
        "Masked: {} field(s) in {} row(s), in {} data statement(s)",
        report.fields_masked, report.rows_masked, report.data_blocks
    );
    for (table, rows) in &report.rows_by_table {
        debug!("table `{table}`: {rows} row(s)");
    }
    info!("Total time: {:.2?}", duration);

    let seconds = duration.as_secs_f64();
    if seconds > 0.0 {
        let rows_per_sec = (report.rows_processed as f64 / seconds) as u64;
        let mib = report.bytes_read as f64 / (1024.0 * 1024.0);
        info!(
            "Speed: {} rows/second, {:.1} MiB/s ({:.1} MiB read)",
            rows_per_sec,
            mib / seconds,
            mib
        );
    }
}

/// Loads and compiles the rules: every rule is checked before any data is read.
fn load_rules(path: &Path, f64_allowed: bool) -> Result<CompiledConfig, Failure> {
    let config = Config::load_from_file(path).map_err(|e| {
        let status = if e.downcast_ref::<io::Error>().is_some() {
            ExitStatus::Io
        } else {
            ExitStatus::Rules
        };
        Failure(status, format!("could not load the rules file: {e}"))
    })?;
    let config = config.compile().map_err(|e| {
        Failure(
            ExitStatus::Rules,
            format!("invalid rule in {}: {}", path.display(), e),
        )
    })?;
    debug!(
        "loaded rules for {} table(s) from {}",
        config.tables.len(),
        path.display()
    );
    check_f64_sampler(&config, f64_allowed)?;
    log_guarantees(&config);
    Ok(config)
}

/// What the rules protect, one line per table. ε adds up over the rows of
/// a person and over repeated releases, which GhostDump cannot see.
/// `mechanism = "f64"` runs only with `--experimental-f64-sampler`, and
/// then with a warning.
fn check_f64_sampler(config: &CompiledConfig, allowed: bool) -> Result<(), Failure> {
    if !config.uses_f64_sampler() {
        return Ok(());
    }
    if !allowed {
        return Err(Failure(
            ExitStatus::Rules,
            "mechanism = \"f64\" is kept for experiments: its guarantee holds only while \
             rounding hides the low-order bits of floating-point noise. Use the exact sampler \
             (the default), or pass --experimental-f64-sampler to measure it"
                .to_string(),
        ));
    }
    warn!(
        "--experimental-f64-sampler: the f64 sampler's guarantee holds only while rounding \
         hides the low-order bits of floating-point noise (see \
         docs/experiments/floating-point.md). Use it for experiments, never on real data."
    );
    Ok(())
}

fn log_guarantees(config: &CompiledConfig) {
    for table in config.guarantees() {
        info!("privacy: {table}");
    }
    if config.guarantees().iter().any(|t| !t.ldp.is_empty()) {
        info!(
            "privacy: ε adds up over the rows of one person and over repeated releases \
             (see docs/guarantees.md)"
        );
    }
}

/// `-v` turns on debug messages; `GHOSTDUMP_LOG` (for example `warn`)
/// replaces the level filter altogether.
fn init_logger(verbose: bool) {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    let mut builder = env_logger::Builder::new();
    match std::env::var("GHOSTDUMP_LOG") {
        Ok(filters) => builder.parse_filters(&filters),
        Err(_) => builder
            .filter_level(LevelFilter::Warn)
            .filter_module("ghostdump", level),
    };

    builder
        .target(env_logger::Target::Stderr)
        .format(|buf, record| {
            let prefix = match record.level() {
                Level::Info => return writeln!(buf, "{}", record.args()),
                Level::Error => "error",
                Level::Warn => "warning",
                Level::Debug => "debug",
                Level::Trace => "trace",
            };
            writeln!(buf, "{}: {}", prefix, record.args())
        })
        .init();
}

/// A progress bar on stderr, updated as the input is read. With a plain
/// input file it shows the share read and an ETA; for stdin or a `.gz` file,
/// whose decompressed size is unknown, it shows bytes read and the rate.
/// indicatif draws nothing when stderr is not a terminal.
fn progress_bar(input: Option<&Path>) -> ProgressBar {
    let total = input
        .filter(|path| !path.to_string_lossy().to_lowercase().ends_with(".gz"))
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len());
    let (bar, template) = match total {
        Some(total) => (
            ProgressBar::new(total),
            "{spinner} [{elapsed_precise}] {wide_bar} {binary_bytes}/{binary_total_bytes} \
             {binary_bytes_per_sec} ETA {eta}",
        ),
        None => (
            ProgressBar::new_spinner(),
            "{spinner} [{elapsed_precise}] {binary_bytes} read, {binary_bytes_per_sec}",
        ),
    };
    bar.set_style(ProgressStyle::with_template(template).expect("a valid template"));
    bar
}

fn resolve_dialect(choice: DialectArg, input: &mut impl BufRead) -> Result<SqlDialect, Failure> {
    let dialect = match choice {
        DialectArg::Postgres => SqlDialect::Postgres,
        DialectArg::Mysql => SqlDialect::Mysql,
        DialectArg::Sqlite => SqlDialect::Sqlite,
        DialectArg::Auto => {
            // Peek without consuming: the engine still reads these bytes.
            let prefix = input
                .fill_buf()
                .map_err(|e| Failure::io("could not read the input", e))?;
            detect_dialect(prefix).unwrap_or_else(|| {
                warn!("could not detect the SQL dialect; assuming PostgreSQL (use --dialect)");
                SqlDialect::Postgres
            })
        }
    };
    debug!("dialect: {dialect:?}");
    Ok(dialect)
}

/// The HMAC secret, from `--secret-file`, `-s` or `GHOSTDUMP_SECRET`: one
/// of them, so that a forgotten variable never takes the place of the file.
fn resolve_secret(args: &Args) -> Result<String, Failure> {
    let secret = match (&args.secret, &args.secret_file) {
        (Some(_), Some(_)) => {
            return Err(Failure(
                ExitStatus::Usage,
                "the secret is given twice, with --secret-file and with -s or \
                 GHOSTDUMP_SECRET. Give it once."
                    .to_string(),
            ));
        }
        (_, Some(path)) => Some(read_secret_file(path)?),
        (secret, None) => secret.clone(),
    };
    match check_secret(secret.as_deref()) {
        Ok(SecretLength::Recommended) => {}
        Ok(SecretLength::Short) => warn!(
            "the secret is shorter than {} bytes. HMAC pseudonyms of low-entropy \
             values (IDs, CPFs) can be brute-forced if the secret is guessed. \
             Use 32 or more random bytes.",
            MIN_SECRET_BYTES
        ),
        Err(SecretError::Missing) => {
            return Err(Failure(
                ExitStatus::Usage,
                "the rules use `hmac`, but no secret was given. Pass --secret-file, -s or \
                 set GHOSTDUMP_SECRET."
                    .to_string(),
            ));
        }
        Err(SecretError::Empty) => {
            return Err(Failure(
                ExitStatus::Usage,
                "the secret is empty. Give 32 or more random bytes with --secret-file, -s or \
                 GHOSTDUMP_SECRET."
                    .to_string(),
            ));
        }
    }
    Ok(secret.unwrap_or_default())
}

fn read_secret_file(path: &Path) -> Result<String, Failure> {
    let contents = std::fs::read(path).map_err(|e| {
        Failure::io(
            &format!("could not read the secret file {}", path.display()),
            e,
        )
    })?;
    warn_if_others_can_read(path);
    secret_from_file(contents).ok_or_else(|| {
        Failure(
            ExitStatus::Usage,
            format!(
                "the secret file {} does not hold UTF-8 text",
                path.display()
            ),
        )
    })
}

/// A secret file that its group or other users can read exposes the secret.
#[cfg(unix)]
fn warn_if_others_can_read(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = std::fs::metadata(path) {
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o044 != 0 {
            warn!(
                "the secret file {} can be read by its group or by other users (mode {mode:o}). \
                 Restrict it with chmod 600.",
                path.display()
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_others_can_read(_: &Path) {}
