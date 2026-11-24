//! End-to-end tests: run the binary on fixture dumps.

use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const PASSTHROUGH: &[u8] = include_bytes!("fixtures/passthrough.sql");
const PG_DATA: &[u8] = include_bytes!("fixtures/pg_data.sql");
const HMAC_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/hmac_rules.toml"
);
const MYSQL_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/mysql_rules.toml"
);
const INVALID_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/invalid_rules.toml"
);
const PG_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/pg_data_rules.toml"
);
const GOLDEN_SECRET: &str = "golden-test-secret-0123456789";
const CONSISTENT_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/consistent_rules.toml"
);
const RR_RULES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/rr_rules.toml");
const DP_RULES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dp_rules.toml");
const F64_RULES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/f64_rules.toml");
const MYSQL_BINARY: &[u8] = include_bytes!("fixtures/mysqldump_binary.sql");
const MYSQL_DATABASES: &[u8] = include_bytes!("fixtures/mysqldump_databases.sql");
const QUALIFIED_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/qualified_rules.toml"
);
const PG_LATIN1: &[u8] = include_bytes!("fixtures/pg_dump_latin1.sql");
const PG_LARGE_OBJECT: &[u8] = include_bytes!("fixtures/pg_dump_large_object.sql");
const MYSQL_NBE_TRIGGER: &[u8] = include_bytes!("fixtures/mysqldump_nbe_trigger.sql");
const TRIGGER_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/trigger_rules.toml"
);
const LATIN1_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/latin1_rules.toml"
);
const BINARY_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/binary_rules.toml"
);
const TYPO_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/typo_rules.toml"
);
const DUMP_FORMS_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/dump_forms_rules.toml"
);
const AUDIT_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/audit_rules.toml"
);

/// Runs ghostdump with `args`, the secret (if any) in the environment, and
/// `stdin` as input.
fn run(args: &[&str], secret: Option<&str>, stdin: &[u8]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ghostdump"));
    command
        .args(args)
        .env_remove("GHOSTDUMP_SECRET")
        .env_remove("GHOSTDUMP_LOG")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(secret) = secret {
        command.env("GHOSTDUMP_SECRET", secret);
    }

    let mut child = command.spawn().expect("the binary runs");
    let mut input = child.stdin.take().expect("stdin is piped");
    // The process may exit before reading its input (a rejected secret).
    if let Err(e) = input.write_all(stdin)
        && e.kind() != ErrorKind::BrokenPipe
    {
        panic!("could not write the input: {e}");
    }
    drop(input);
    child.wait_with_output().expect("the binary finishes")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A new, empty directory for one test.
fn scratch(name: &str) -> PathBuf {
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("the directory is created");
    directory
}

fn path(path: &Path) -> &str {
    path.to_str().expect("a UTF-8 path")
}

/// Exit statuses follow sysexits.h: 64 for the command line, 65 for an
/// input that cannot be masked safely, 74 for files that cannot be read or
/// written, 78 for the rules.
#[test]
fn exit_statuses_tell_what_failed() {
    let dump = b"INSERT INTO users (name) VALUES ('Ana');\n";
    let status = |args: &[&str], stdin: &[u8]| run(args, None, stdin).status.code();
    assert_eq!(status(&[], dump), Some(0));
    assert_eq!(status(&["--threads", "0"], dump), Some(64));
    assert_eq!(status(&["-c", HMAC_RULES], dump), Some(64));
    assert_eq!(
        status(&["-c", TYPO_RULES], b"INSERT INTO users VALUES ('Ana');\n"),
        Some(65)
    );
    assert_eq!(status(&["-i", "/nonexistent/dump.sql"], b""), Some(74));
    assert_eq!(status(&["-c", "/nonexistent/rules.toml"], dump), Some(74));
    assert_eq!(status(&["-o", "/dev/full"], dump), Some(74));
    assert_eq!(status(&["-c", INVALID_RULES], dump), Some(78));
    assert_eq!(status(&["-c", TYPO_RULES, "--strict"], dump), Some(78));
}

/// The output file is written next to its target and renamed over it at
/// the end: a run that fails leaves the target as it was, or absent, and
/// no temporary file behind.
#[test]
fn only_a_run_that_succeeds_writes_the_output_file() {
    let directory = scratch("output_file");
    let target = directory.join("out.sql");
    let new = directory.join("new.sql");
    fs::write(&target, "previous\n").unwrap();
    let masked = b"INSERT INTO users (name) VALUES ('Ana');\n";
    let refused = b"INSERT INTO users (name) VALUES ('Ana');\nINSERT INTO users VALUES ('Bea');\n";

    for args in [
        &["-c", TYPO_RULES, "-o", path(&target)][..],
        &["-c", TYPO_RULES, "-o", path(&new)],
    ] {
        let output = run(args, None, refused);
        assert_eq!(output.status.code(), Some(65), "{}", stderr(&output));
        assert!(
            stderr(&output).contains("was not written"),
            "{}",
            stderr(&output)
        );
    }
    let strict = run(
        &["-c", TYPO_RULES, "--strict", "-o", path(&target)],
        None,
        masked,
    );
    assert_eq!(strict.status.code(), Some(78));
    let dry_run = run(
        &["-c", TYPO_RULES, "--dry-run", "-o", path(&target)],
        None,
        masked,
    );
    assert!(dry_run.status.success(), "{}", stderr(&dry_run));
    assert_eq!(fs::read_to_string(&target).unwrap(), "previous\n");
    assert!(!new.exists());

    let written = run(&["-c", TYPO_RULES, "-o", path(&target)], None, masked);
    assert!(written.status.success(), "{}", stderr(&written));
    assert_eq!(
        fs::read_to_string(&target).unwrap(),
        "INSERT INTO users (name) VALUES (NULL);\n"
    );
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
}

/// `check` runs the rules over a dump without writing it, and lists what
/// they do; a rule that matches nothing makes it fail.
/// `.cargo/config.toml` builds mimalloc to commit memory on demand, which
/// keeps the peak memory of a run at about half (docs/benchmarks.md). A
/// `CFLAGS` variable in the environment of the build replaces that setting.
#[test]
fn the_allocator_commits_memory_on_demand() {
    let output = Command::new(env!("CARGO_BIN_EXE_ghostdump"))
        .arg("--version")
        .env("MIMALLOC_VERBOSE", "1")
        .env_remove("MIMALLOC_ARENA_EAGER_COMMIT")
        .output()
        .expect("the binary runs");
    assert!(
        stderr(&output).contains("option 'arena_eager_commit': 0"),
        "mimalloc was built without MI_DEFAULT_ARENA_EAGER_COMMIT=0 (is CFLAGS set?):\n{}",
        stderr(&output)
    );
}

#[test]
fn every_member_of_a_gzip_input_is_read() {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let gzip = |bytes: &[u8]| {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    };
    // `cat a.gz b.gz` gives one file of two members, which gunzip reads
    // whole.
    let split = PG_DATA.len() / 2;
    let split = split + PG_DATA[split..].iter().position(|&b| b == b'\n').unwrap() + 1;
    let mut members = gzip(&PG_DATA[..split]);
    members.extend(gzip(&PG_DATA[split..]));

    let directory = scratch("gzip_members");
    for (name, bytes) in [("one.sql.gz", gzip(PG_DATA)), ("two.sql.gz", members)] {
        let input = directory.join(name);
        fs::write(&input, bytes).unwrap();
        let output = run(&["-i", path(&input)], None, b"");
        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        assert!(output.stdout == PG_DATA, "{name} was not read whole");
    }

    // gunzip ignores zeros after the last member; GhostDump stops.
    let padded = directory.join("padded.sql.gz");
    let mut bytes = gzip(PG_DATA);
    bytes.extend([0; 512]);
    fs::write(&padded, bytes).unwrap();
    let output = run(&["-i", path(&padded)], None, b"");
    assert_eq!(output.status.code(), Some(74), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("could not decompress the input"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn check_lists_what_the_rules_do() {
    let dump = include_bytes!("fixtures/pg_dump_default.sql");
    let output = run(&["check", "-c", TRIGGER_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(78), "{}", stderr(&output));
    assert!(
        stdout.contains("masked: table `users`, columns name"),
        "{stdout}"
    );
    assert!(
        stdout.contains("copied as it is: table `public.log`"),
        "{stdout}"
    );
    assert!(
        stdout.contains("unused: no rule of table `visits` matched the input"),
        "{stdout}"
    );
    assert!(!stdout.contains("Alice"), "{stdout}");

    let refused = run(
        &["check", "-c", TRIGGER_RULES],
        None,
        b"INSERT INTO users VALUES ('Ana');\n",
    );
    assert_eq!(refused.status.code(), Some(65));
    assert!(
        stderr(&refused).contains("(line 1 of the input)"),
        "{}",
        stderr(&refused)
    );
}

/// `init` writes every table and column of a dump, with rules for the
/// columns whose names suggest personal data, and the rules pass `check`.
/// It never replaces a file.
#[test]
fn init_writes_rules_that_pass_check() {
    let directory = scratch("init");
    let rules = directory.join("rules.toml");
    let dump = include_bytes!("fixtures/pg_dump_default.sql");
    let init = run(&["init", "-o", path(&rules)], None, dump);
    assert!(init.status.success(), "{}", stderr(&init));
    let text = fs::read_to_string(&rules).unwrap();
    assert!(text.contains("[tables.users]"), "{text}");
    assert!(
        text.contains("    { name = \"name\", strategy = \"faker_name\" },"),
        "{text}"
    );
    assert!(
        text.contains("    # { name = \"id\", strategy = \"nullify\" },"),
        "{text}"
    );

    let check = run(&["check", "-c", path(&rules)], None, dump);
    assert!(check.status.success(), "{}", stderr(&check));
    let masked = run(&["-c", path(&rules)], None, dump);
    assert!(masked.status.success(), "{}", stderr(&masked));
    assert!(!String::from_utf8_lossy(&masked.stdout).contains("Alice Example"));

    let again = run(&["init", "-o", path(&rules)], None, dump);
    assert_eq!(again.status.code(), Some(64));
    assert_eq!(fs::read_to_string(&rules).unwrap(), text);
}

/// `--secret-file` gives the same secret as GHOSTDUMP_SECRET, without the
/// line ending at its end. The secret is given once; `-s` warns that it
/// shows in the process list.
#[test]
fn secret_files_hold_the_secret() {
    let directory = scratch("secret_file");
    let file = directory.join("secret");
    fs::write(&file, format!("{GOLDEN_SECRET}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let from_environment = run(&["-c", HMAC_RULES], Some(GOLDEN_SECRET), PG_DATA);
    let from_file = run(
        &["-c", HMAC_RULES, "--secret-file", path(&file)],
        None,
        PG_DATA,
    );
    assert!(from_file.status.success(), "{}", stderr(&from_file));
    assert_eq!(from_file.stdout, from_environment.stdout);
    assert!(
        !stderr(&from_file).contains("secret"),
        "{}",
        stderr(&from_file)
    );

    let twice = run(
        &["-c", HMAC_RULES, "--secret-file", path(&file)],
        Some(GOLDEN_SECRET),
        PG_DATA,
    );
    assert_eq!(twice.status.code(), Some(64));

    let on_command_line = run(&["-c", HMAC_RULES, "-s", GOLDEN_SECRET], None, PG_DATA);
    assert!(on_command_line.status.success());
    assert!(
        stderr(&on_command_line).contains("process list"),
        "{}",
        stderr(&on_command_line)
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        let readable = run(
            &["-c", HMAC_RULES, "--secret-file", path(&file)],
            None,
            PG_DATA,
        );
        assert!(readable.status.success());
        assert!(
            stderr(&readable).contains("chmod 600"),
            "{}",
            stderr(&readable)
        );
    }
}

#[test]
fn stdout_carries_only_the_dump() {
    let output = run(&[], None, PASSTHROUGH);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(PASSTHROUGH)
    );
    assert!(stderr(&output).contains("Rows processed"));
}

#[test]
fn hmac_rules_require_a_secret() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    let output = run(&["-c", HMAC_RULES], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("no secret was given"));
    assert!(output.stdout.is_empty());
}

#[test]
fn blank_secret_is_rejected() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    for secret in ["", "   "] {
        let output = run(&["-c", HMAC_RULES], Some(secret), dump);

        assert!(!output.status.success(), "secret {secret:?} was accepted");
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn short_secret_only_warns() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    let output = run(&["-c", HMAC_RULES], Some("short"), dump);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stderr(&output).contains("shorter than 16 bytes"));
}

#[test]
fn truncated_data_block_fails_without_writing_the_partial_row() {
    let dump = b"INSERT INTO t (a) VALUES ('first'), ('sec";
    let output = run(&[], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("middle of a data row"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("sec"));
}

#[test]
fn verbose_logging_does_not_touch_stdout() {
    let quiet = run(&[], None, PASSTHROUGH);
    let verbose = run(&["-v"], None, PASSTHROUGH);

    assert!(verbose.status.success());
    assert_eq!(quiet.stdout, verbose.stdout);
    assert!(stderr(&verbose).contains("debug:"));
}

#[test]
fn log_level_can_be_overridden() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ghostdump"));
    let output = command
        .env("GHOSTDUMP_LOG", "warn")
        .stdin(Stdio::null())
        .output()
        .expect("the binary runs");

    assert!(output.status.success());
    assert!(!stderr(&output).contains("Starting GhostDump"));
}

#[test]
fn logs_never_contain_column_values() {
    let dump = b"INSERT INTO users (salary) VALUES ('Batman-Secret-123');\n";
    for flags in [&[][..], &["-v"][..]] {
        let args: Vec<&str> = ["-c", DP_RULES].iter().chain(flags).copied().collect();
        let output = run(&args, None, dump);

        assert!(output.status.success(), "stderr: {}", stderr(&output));
        assert!(!stderr(&output).contains("Batman-Secret-123"));
    }
}

#[test]
fn data_blocks_are_copied_exactly_without_rules() {
    let output = run(&[], None, PG_DATA);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(PG_DATA)
    );
}

#[test]
fn mysqldump_with_column_names_is_masked() {
    let dump = include_bytes!("fixtures/mysql_complete_insert.sql");
    let output = run(&["-c", MYSQL_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains("(1,'Masked O\\'Name',NULL),(2,'Masked O\\'Name',NULL);"),
        "{stdout}"
    );
    assert!(!stdout.contains("ana@example.com"));
    assert!(stdout.contains("UNLOCK TABLES;"));
}

#[test]
fn mysqldump_default_format_is_masked() {
    // No column list: the columns come from the CREATE TABLE before the rows.
    let dump = include_bytes!("fixtures/mysql_default.sql");
    let output = run(&["-c", MYSQL_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains(
            "INSERT INTO `customers` VALUES (1,'Masked O\\'Name',NULL),(2,'Masked O\\'Name',NULL);"
        ),
        "{stdout}"
    );
    assert!(!stdout.contains("example.com"));
}

#[test]
fn rows_without_column_names_or_create_table_are_refused() {
    // A data-only dump: nothing tells which value belongs to which column.
    let dump = b"INSERT INTO `customers` VALUES (1,'Ana O\\'Neil','ana@example.com');\n";
    let output = run(&["-c", MYSQL_RULES, "--dialect", "mysql"], None, dump);

    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(message.contains("no CREATE TABLE"), "{message}");
    assert!(message.contains("--complete-insert"), "{message}");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("Ana"));
}

#[test]
fn invalid_rules_stop_before_reading_data() {
    let dump = b"INSERT INTO users (salary) VALUES (10);\n";
    let output = run(&["-c", INVALID_RULES], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("table `users`, column `salary`"));
    assert!(output.stdout.is_empty());
}

#[test]
fn f64_sampler_needs_the_hidden_experiment_option() {
    let dump = b"INSERT INTO users (salary) VALUES (10);\n";

    let refused = run(&["-c", F64_RULES], None, dump);
    assert!(!refused.status.success());
    assert!(refused.stdout.is_empty());
    assert!(
        stderr(&refused).contains("--experimental-f64-sampler"),
        "{}",
        stderr(&refused)
    );

    let allowed = run(&["-c", F64_RULES, "--experimental-f64-sampler"], None, dump);
    assert!(allowed.status.success(), "stderr: {}", stderr(&allowed));
    assert!(
        stderr(&allowed).contains("never on real data"),
        "{}",
        stderr(&allowed)
    );

    let help = run(&["--help"], None, b"");
    assert!(!String::from_utf8_lossy(&help.stdout).contains("f64"));
}

#[test]
fn non_numeric_dp_values_are_counted_once() {
    let dump = b"INSERT INTO users (salary) VALUES ('a'), ('b'), (30);\n";
    let output = run(&["-c", DP_RULES], None, dump);
    let stderr = stderr(&output);

    assert!(output.status.success(), "stderr: {stderr}");
    assert_eq!(stderr.matches("were not numbers").count(), 1, "{stderr}");
    assert!(
        stderr.contains("column `users.salary`: 2 value(s)"),
        "{stderr}"
    );
}

/// Regenerate after an intended change (a new `fake` version, for example):
///
/// ```sh
/// GHOSTDUMP_SECRET=golden-test-secret-0123456789 ghostdump --seed 42 \
///     -c tests/fixtures/pg_data_rules.toml -i tests/fixtures/pg_data.sql \
///     > tests/fixtures/pg_data_masked.sql
/// ```
#[test]
fn seeded_run_matches_the_golden_output() {
    let expected = include_bytes!("fixtures/pg_data_masked.sql");
    let output = run(
        &["-c", PG_RULES, "--seed", "42"],
        Some(GOLDEN_SECRET),
        PG_DATA,
    );

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stderr(&output).contains("--seed makes every random choice reproducible"));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(expected)
    );
}

#[test]
fn threads_give_the_golden_output() {
    let expected = include_bytes!("fixtures/pg_data_masked.sql");
    for threads in ["2", "4"] {
        let output = run(
            &["-c", PG_RULES, "--seed", "42", "--threads", threads],
            Some(GOLDEN_SECRET),
            PG_DATA,
        );
        assert!(output.status.success(), "stderr: {}", stderr(&output));
        assert!(output.stdout == expected, "{threads} threads");
    }
}

#[test]
fn zero_threads_are_rejected() {
    let output = run(&["--threads", "0"], None, b"");
    assert!(!output.status.success());
}

#[test]
fn noise_is_reproducible_only_with_a_seed() {
    let dump = b"INSERT INTO users (salary) VALUES (10), (20), (30);\n";
    let seeded = |seed: &str| run(&["-c", DP_RULES, "--seed", seed], None, dump).stdout;
    let unseeded = || run(&["-c", DP_RULES], None, dump).stdout;

    assert_eq!(seeded("7"), seeded("7"));
    assert_ne!(seeded("7"), seeded("8"));
    assert_ne!(unseeded(), unseeded());
}

#[test]
fn progress_flag_keeps_stdout_clean() {
    let output = run(&["-p"], None, PG_DATA);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(output.stdout, PG_DATA);
}

#[test]
fn consistent_noise_repeats_across_runs_and_rows() {
    let dump = b"INSERT INTO users (salary) VALUES (10), (20), (10);\n";
    let first = run(&["-c", CONSISTENT_RULES], Some(GOLDEN_SECRET), dump);
    let second = run(&["-c", CONSISTENT_RULES], Some(GOLDEN_SECRET), dump);

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert_eq!(first.stdout, second.stdout);
    let text = String::from_utf8_lossy(&first.stdout);
    let values: Vec<&str> = text
        .trim_end()
        .trim_end_matches(");")
        .split("VALUES (")
        .nth(1)
        .unwrap()
        .split("), (")
        .collect();
    assert_eq!(values[0], values[2], "{text}");
    assert_ne!(values[0], values[1], "{text}");

    let without_secret = run(&["-c", CONSISTENT_RULES], None, dump);
    assert!(!without_secret.status.success());
}

#[test]
fn startup_summary_states_the_privacy_budget() {
    let output = run(&["-c", PG_RULES], Some(GOLDEN_SECRET), PG_DATA);
    let stderr = stderr(&output);

    assert!(output.status.success(), "stderr: {stderr}");
    assert!(
        stderr.contains("privacy: table `users`: ε = 1.0000 per row (salary ε = 1.0000)"),
        "{stderr}"
    );
    assert!(stderr.contains("pseudonymized, no DP: email"), "{stderr}");
}

#[test]
fn randomized_response_writes_only_declared_categories() {
    let dump = b"INSERT INTO users (id, status) VALUES (1, 'ACTIVE'), (2, 'BANNED'), (3, NULL);\n";
    let output = run(&["-c", RR_RULES, "--seed", "3"], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(!stdout.contains("BANNED"), "{stdout}");
    assert!(stdout.contains("(3, NULL)"), "{stdout}");
    let categories = ["'ACTIVE'", "'PENDING'", "'INACTIVE'"];
    for row in ["(1, ", "(2, "] {
        let start = stdout.find(row).unwrap() + row.len();
        let value = &stdout[start..start + stdout[start..].find(')').unwrap()];
        assert!(categories.contains(&value), "{value}");
    }
    assert!(
        stderr(&output).contains("column `users.status`: 1 value(s) were not among the categories"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn unused_rules_warn_and_fail_with_strict() {
    let dump = b"INSERT INTO Users (Name, email) VALUES ('Ana', 'a@x.com');\n";

    let output = run(&["-c", TYPO_RULES], None, dump);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    // The name is masked whatever its case; the misspelt rule is reported.
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "INSERT INTO Users (Name, email) VALUES (NULL, 'a@x.com');\n"
    );
    let warnings = stderr(&output);
    assert!(
        warnings.contains("column `users.emial` was not found"),
        "{warnings}"
    );

    let strict = run(&["-c", TYPO_RULES, "--strict"], None, dump);
    assert!(!strict.status.success());
    assert!(stderr(&strict).contains("--strict"), "{}", stderr(&strict));
}

/// The text from the first `start` to the `end` after it, both included.
fn section<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    let from = text
        .find(start)
        .unwrap_or_else(|| panic!("`{start}` not found"));
    let to = from + text[from..].find(end).expect("the end follows") + end.len();
    &text[from..to]
}

#[test]
fn pg_dump_with_column_inserts_is_masked() {
    // pg_dump 16 --column-inserts: an identity column GENERATED ALWAYS, a
    // quoted name with a space, a SQL-standard function body and a rule
    // with two actions.
    let dump = include_bytes!("fixtures/pg_dump_column_inserts.sql");
    let output = run(&["-c", DUMP_FORMS_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let input = String::from_utf8_lossy(dump);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    for value in [
        "Alice Example",
        "alice@example.com",
        "O''Brien",
        "Carol Example",
    ] {
        assert!(!stdout.contains(value), "{value} in {stdout}");
    }
    assert!(stdout.contains(
        "INSERT INTO public.users (id, name, email) OVERRIDING SYSTEM VALUE VALUES (1, 'Masked', NULL);"
    ));
    assert!(stdout.contains("INSERT INTO public.\"my table\" (\"first name\") VALUES (NULL);"));
    // The function body and the rule are code: they are copied as they are.
    for (start, end) in [("CREATE FUNCTION", "\nEND;"), ("CREATE RULE", "\n);\n")] {
        assert_eq!(section(&stdout, start, end), section(&input, start, end));
    }
}

#[test]
fn mysqldump_routines_are_copied_and_rows_masked() {
    // mysqldump 8.0 --complete-insert --routines: a trigger and a procedure
    // whose bodies write to tables with rules.
    let dump = include_bytes!("fixtures/mysqldump_routines.sql");
    let output = run(&["-c", DUMP_FORMS_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let input = String::from_utf8_lossy(dump);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    for value in ["Alice Example", "alice@example.com", "O\\'Brien"] {
        assert!(!stdout.contains(value), "{value} in {stdout}");
    }
    assert!(stdout.contains("VALUES (1,'Masked',NULL),(2,'Masked',NULL);"));
    assert!(stdout.contains("VALUES (1,NULL),(2,NULL);"));
    for (start, end) in [("/*!50003 CREATE*/", "*/;;"), ("CREATE DEFINER", "END ;;")] {
        assert_eq!(section(&stdout, start, end), section(&input, start, end));
    }
}

#[test]
fn sqlite_trigger_bodies_are_copied() {
    // The trigger inserts into `audit`, which has a rule: its body is code,
    // not a data block.
    let dump = include_bytes!("fixtures/sqlite_dump.sql");
    let output = run(&["-c", AUDIT_RULES], None, dump);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(dump)
    );
}

#[test]
fn sqlite_dump_is_masked() {
    // sqlite3's .dump never writes column names: they come from its CREATE TABLE.
    let dump = include_bytes!("fixtures/sqlite_dump.sql");
    let output = run(&["-c", DUMP_FORMS_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains("INSERT INTO users VALUES(1,'Masked',NULL);"),
        "{stdout}"
    );
    assert!(stdout.contains("INSERT INTO users VALUES(2,'Masked',NULL);"));
    assert!(!stdout.contains("Alice") && !stdout.contains("example.com"));
}

#[test]
fn pg_dump_with_inserts_is_masked() {
    // pg_dump 16 --inserts: no column list, and OVERRIDING SYSTEM VALUE for
    // the identity column.
    let dump = include_bytes!("fixtures/pg_dump_inserts.sql");
    let output = run(&["-c", DUMP_FORMS_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains(
            "INSERT INTO public.users OVERRIDING SYSTEM VALUE VALUES (1, 'Masked', NULL);"
        )
    );
    assert!(stdout.contains("INSERT INTO public.\"my table\" VALUES (NULL);"));
    for value in [
        "Alice Example",
        "alice@example.com",
        "O''Brien",
        "Carol Example",
    ] {
        assert!(!stdout.contains(value), "{value} in {stdout}");
    }
}

#[test]
fn pg_dump_default_format_is_masked() {
    // pg_dump 16 with no options: COPY blocks, and psql's \\restrict.
    let dump = include_bytes!("fixtures/pg_dump_default.sql");
    let output = run(&["-c", DUMP_FORMS_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stdout.contains(
        "COPY public.users (id, name, email) FROM stdin;\n1\tMasked\t\\N\n2\tMasked\t\\N\n\\.\n"
    ));
    assert!(stdout.contains("COPY public.\"my table\" (\"first name\") FROM stdin;\n\\N\n\\.\n"));
    assert!(stdout.contains("COPY public.log (x) FROM stdin;\n\\N\n\\N\n\\.\n"));
    for value in [
        "Alice Example",
        "alice@example.com",
        "O'Brien",
        "Carol Example",
    ] {
        assert!(!stdout.contains(value), "{value} in {stdout}");
    }
    // The rule on "my table" is schema, copied as it is, with its literal.
    assert!(stdout.contains("  VALUES ('second action'::text);\n);"));
}

/// mysqldump writes binary columns as raw bytes (`_binary '...'`), most of
/// them not UTF-8. Each value is pseudonymized from its own bytes, so values
/// that differ only there keep different pseudonyms.
#[test]
fn binary_values_keep_distinct_pseudonyms() {
    let output = run(&["-c", BINARY_RULES], Some(GOLDEN_SECRET), MYSQL_BINARY);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let pseudonym = |bytes: &[u8]| ghostdump::engine::mask::hmac_hash(bytes, GOLDEN_SECRET);
    let tokens: [&[u8]; 3] = [
        &[0xe9, 0xff, 0, 1],
        &[0xe8, 0xff, 0, 1],
        &[b'A', 0xff, 0, 1],
    ];
    let names = ["José", "Josè", "Ana"];
    for (id, (name, token)) in names.iter().zip(tokens).enumerate() {
        let row = format!(
            "({},'{}','{}')",
            id + 1,
            pseudonym(name.as_bytes()),
            pseudonym(token)
        );
        assert!(stdout.contains(&row), "{row} not in {stdout}");
    }
    assert_ne!(pseudonym(tokens[0]), pseudonym(tokens[1]));
}

/// pg_dump writes a LATIN1 database in LATIN1 unless told otherwise. With
/// rules the run stops at that setting, before any row; without rules the
/// dump is copied as it is.
#[test]
fn dumps_in_another_encoding_are_refused_with_rules() {
    let refused = run(&["-c", LATIN1_RULES], Some(GOLDEN_SECRET), PG_LATIN1);
    let message = stderr(&refused);
    assert!(!refused.status.success());
    assert!(message.contains("`LATIN1`"), "{message}");
    assert!(message.contains("pg_dump -E UTF8"), "{message}");
    assert!(PG_LATIN1.starts_with(&refused.stdout));
    assert!(!String::from_utf8_lossy(&refused.stdout).contains("COPY"));

    let copied = run(&[], None, PG_LATIN1);
    assert!(copied.status.success(), "stderr: {}", stderr(&copied));
    assert_eq!(copied.stdout, PG_LATIN1);
}

/// mysql runs what `/*! ... */` holds, and ends statements at a `;` inside
/// it, so rows there are masked like any other.
#[test]
fn rows_inside_mysql_version_comments_are_masked() {
    let dump = b"/*!40101 SET NAMES utf8mb4 */;\n/*!40000 INSERT INTO customers (name, email) VALUES ('Ana', 'ana@example.com') */;\n";
    let output = run(&["-c", MYSQL_RULES, "--dialect", "mysql"], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains(
            "/*!40000 INSERT INTO customers (name, email) VALUES ('Masked O\\'Name', NULL) */;"
        ),
        "{stdout}"
    );
    assert!(!stdout.contains("ana@example.com"));
}

/// psql reads `\\echo it's` as a command, not SQL; a quote there would
/// hide the INSERT after it. With rules, a client command that dump tools
/// do not write stops the run; without rules the dump is copied.
#[test]
fn client_commands_stop_the_run_with_rules() {
    let dump = b"\\echo it's\nINSERT INTO users (name) VALUES ('Ana');\n";
    let refused = run(&["-c", PG_RULES], Some(GOLDEN_SECRET), dump);
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("`\\echo`"),
        "{}",
        stderr(&refused)
    );
    assert!(!String::from_utf8_lossy(&refused.stdout).contains("Ana"));

    let copied = run(&[], None, dump);
    assert!(copied.status.success(), "stderr: {}", stderr(&copied));
    assert_eq!(copied.stdout, dump);
}

/// pg_dump writes the bytes of a large object with `lowrite`. No rule
/// applies to them, so with rules the run warns, and copies them.
#[test]
fn large_objects_are_copied_with_a_warning() {
    let rules = run(&["-c", TRIGGER_RULES], None, PG_LARGE_OBJECT);
    assert!(rules.status.success(), "{}", stderr(&rules));
    assert!(
        stderr(&rules).contains("--no-large-objects"),
        "{}",
        stderr(&rules)
    );
    let stdout = String::from_utf8_lossy(&rules.stdout);
    assert!(stdout.contains("SELECT pg_catalog.lowrite(0, '\\x534543524554204c4f204259544553');"));
    assert!(stdout.contains("1\tMASKED\t49158"), "{stdout}");

    let copied = run(&[], None, PG_LARGE_OBJECT);
    assert!(
        !stderr(&copied).contains("large object"),
        "{}",
        stderr(&copied)
    );
    assert_eq!(copied.stdout, PG_LARGE_OBJECT);
}

/// mysqldump writes the sql_mode a trigger was created with before it. A
/// trigger created with NO_BACKSLASH_ESCAPES stops the run there: after the
/// rows of its table, before the rows of the next one, with the way to dump
/// the trigger apart.
#[test]
fn triggers_without_backslash_escapes_stop_the_run() {
    let output = run(&["-c", TRIGGER_RULES], None, MYSQL_NBE_TRIGGER);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = stderr(&output);
    assert!(!output.status.success());
    assert!(
        stderr.contains("NO_BACKSLASH_ESCAPES") && stderr.contains("--skip-triggers"),
        "{stderr}"
    );
    assert!(
        stdout.contains("INSERT INTO `users` VALUES (1,'MASKED');"),
        "{stdout}"
    );
    assert!(!stdout.contains("Secret"), "{stdout}");
}

/// `mysqldump --databases` names the database once, in a `USE`, and the
/// tables without it. A rule for `shopq.users` applies to them; without the
/// `USE` the rows cannot be told apart, and the run stops.
#[test]
fn rules_with_a_schema_follow_mysql_use() {
    let output = run(&["-c", QUALIFIED_RULES], None, MYSQL_DATABASES);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains("INSERT INTO `users` VALUES (1,'MASKED');"),
        "{stdout}"
    );
    assert!(!stdout.contains("Ana Secret"));

    let without_use: Vec<u8> = String::from_utf8_lossy(MYSQL_DATABASES)
        .lines()
        .filter(|line| !line.starts_with("USE "))
        .flat_map(|line| [line.as_bytes(), b"\n"].concat())
        .collect();
    let refused = run(&["-c", QUALIFIED_RULES], None, &without_use);
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("name no schema"),
        "{}",
        stderr(&refused)
    );
    assert!(!String::from_utf8_lossy(&refused.stdout).contains("Ana Secret"));
}
