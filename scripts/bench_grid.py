"""End-to-end benchmark grid for the GhostDump CLI.

For each dump size and format, generates a dump with sql_generator.py (fixed
seed), decompresses it once, and runs the CLI several times per rule
configuration. Each run records wall time and peak resident memory (VmHWM,
polled from /proc while the process runs, so no external tool is needed). Results go to a CSV with one row
per run, and a summary of medians is printed.

Example:

    cargo build --release
    python3 scripts/bench_grid.py --sizes 100000,1000000 --runs 10

To compare with an older build, pass --baseline-binary; it is run with the
configurations it understands (no rules, and the full rule set written with
`sensitivity`, the parameter older versions used).
"""

import argparse
import csv
import gzip
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SECRET = "bench-secret-0123456789abcdef0123456789"

SALARY_RULE = (
    '{{ name = "salary", strategy = "dp_laplace", epsilon = 0.5, '
    'bounds = [0.0, 10000.0], scale = 2, mechanism = "{sampler}" }}'
)
HMAC_KEYS = """
[tables.users]
columns = [{{ name = "id", strategy = "hmac" }}, {extra}]

[tables.orders]
columns = [
    {{ name = "id", strategy = "hmac" }},
    {{ name = "user_id", strategy = "hmac" }},
]
"""


def write_configs(workdir, generated_rules):
    """Rule files per configuration; `None` means no rules."""
    configs = {"no_rules": None}

    def write(name, text):
        path = workdir / f"{name}.toml"
        path.write_text(text)
        configs[name] = path

    write("hmac_only", HMAC_KEYS.format(extra='{ name = "company_id", strategy = "hmac" }'))
    for sampler in ("f64", "exact"):
        rule = SALARY_RULE.format(sampler=sampler)
        write(f"laplace_{sampler}", f"[tables.users]\ncolumns = [{rule}]\n")
    shutil.copy(generated_rules, workdir / "full.toml")
    configs["full"] = workdir / "full.toml"
    return configs


def write_baseline_configs(workdir, generated_rules):
    """The full rule set in the format of older versions (`sensitivity`)."""
    text = generated_rules.read_text()
    text = text.replace("  bounds = [0.0, 10000.0]\n  scale = 2\n", "  sensitivity = 10000.0\n")
    path = workdir / "full_baseline.toml"
    path.write_text(text)
    return {"no_rules": None, "full": path}


def read_hwm(pid):
    """VmHWM of a running process, in KiB, or None once it has exited."""
    try:
        with open(f"/proc/{pid}/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def run_once(binary, config, dump):
    """Runs the CLI once; returns (seconds, peak RSS in KiB).

    The peak is VmHWM, which belongs to the program after exec. The
    rusage of os.wait4 would also count the memory the child inherited
    from this Python process before exec.
    """
    command = [str(binary), "-i", str(dump), "-o", os.devnull]
    if config is not None:
        command += ["-c", str(config)]
    env = dict(os.environ, GHOSTDUMP_SECRET=SECRET, GHOSTDUMP_LOG="error")
    # A file, not a pipe: a build that logs one line per value would fill a
    # pipe and block before exiting.
    errors = tempfile.TemporaryFile()
    start = time.perf_counter()
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.DEVNULL, stderr=errors
    )
    peak = 0
    while True:
        hwm = read_hwm(process.pid)
        if hwm is not None:
            peak = max(peak, hwm)
        pid, status = os.waitpid(process.pid, os.WNOHANG)
        if pid:
            break
        time.sleep(0.002)
    seconds = time.perf_counter() - start
    errors.seek(0)
    stderr = errors.read()[-4000:].decode(errors="replace")
    errors.close()
    if os.waitstatus_to_exitcode(status) != 0:
        sys.exit(f"run failed: {' '.join(command)}\n{stderr}")
    return seconds, peak


def prepare_dump(workdir, rows, fmt, seed):
    """Generates and decompresses one dump; returns (dump, rules, data rows)."""
    target = workdir / f"{fmt}-{rows}"
    dump = target / "dump.sql"
    if not dump.exists():
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "sql_generator.py"),
                "--rows",
                str(rows),
                "--mode",
                fmt,
                "--seed",
                str(seed),
                "--output-dir",
                str(target),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        with gzip.open(target / "dump.sql.gz", "rb") as src, open(dump, "wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
    data_rows = int((target / "stats.txt").read_text())
    return dump, target / "rules.toml", data_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", default="100000,1000000", help="users per dump")
    parser.add_argument("--formats", default="copy,insert")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--configs",
        default="no_rules,hmac_only,laplace_f64,laplace_exact,full",
        help="comma-separated subset of the configurations",
    )
    parser.add_argument(
        "--binary", default=str(PROJECT_ROOT / "target" / "release" / "ghostdump")
    )
    parser.add_argument("--baseline-binary", help="an older build to compare with")
    parser.add_argument("--workdir", default=str(PROJECT_ROOT / "generated" / "bench"))
    parser.add_argument("--out", default="bench_results.csv")
    args = parser.parse_args()

    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    binaries = [("current", Path(args.binary))]
    if args.baseline_binary:
        binaries.append(("baseline", Path(args.baseline_binary)))
    wanted = args.configs.split(",")

    rows_out = []
    for rows in (int(n) for n in args.sizes.split(",")):
        for fmt in args.formats.split(","):
            dump, generated_rules, data_rows = prepare_dump(workdir, rows, fmt, args.seed)
            mib = dump.stat().st_size / (1 << 20)
            for label, binary in binaries:
                if label == "current":
                    configs = write_configs(dump.parent, generated_rules)
                else:
                    configs = write_baseline_configs(dump.parent, generated_rules)
                for name, config in configs.items():
                    if name not in wanted:
                        continue
                    for _ in range(args.warmup):
                        run_once(binary, config, dump)
                    for run in range(1, args.runs + 1):
                        seconds, rss = run_once(binary, config, dump)
                        rows_out.append(
                            {
                                "binary": label,
                                "config": name,
                                "users": rows,
                                "data_rows": data_rows,
                                "format": fmt,
                                "run": run,
                                "seconds": f"{seconds:.4f}",
                                "rows_per_second": round(data_rows / seconds),
                                "mib_per_second": f"{mib / seconds:.1f}",
                                "max_rss_kib": rss,
                            }
                        )
                    print_summary(rows_out[-args.runs :])

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_out[0]))
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"\n{len(rows_out)} runs written to {args.out}")


def print_summary(runs):
    first = runs[0]
    seconds = sorted(float(r["seconds"]) for r in runs)
    q1, median, q3 = statistics.quantiles(seconds, n=4) if len(seconds) > 1 else seconds * 3
    rss = max(r["max_rss_kib"] for r in runs)
    print(
        f"{first['binary']:8} {first['format']:6} {first['data_rows']:>10} rows "
        f"{first['config']:14} median {median:8.3f} s (IQR {q3 - q1:.3f}) "
        f"{first['data_rows'] / median:>12,.0f} rows/s  peak {rss / 1024:6.1f} MiB"
    )


if __name__ == "__main__":
    main()
