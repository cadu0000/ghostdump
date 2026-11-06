import argparse
import gzip
import json
import math
import random
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
CHUNK_SIZE = 10_000
OUTPUT_DIR_NAME = "generated"

# Salaries and order amounts follow lognormal distributions; the parameters
# are those of the underlying normal (median = exp(mu)).
SALARY_MU = 8.3
SALARY_SIGMA = 0.5
AMOUNT_MU = 4.0
AMOUNT_SIGMA = 0.9

# Orders are assigned to users with a bounded Zipf law of this exponent:
# a few users place most orders.
ORDERS_ZIPF_S = 1.1

# Categorical column with skewed frequencies.
STATUS_WEIGHTS = {"ACTIVE": 0.7, "PENDING": 0.2, "INACTIVE": 0.1}

# SQL types of the generated columns, for the CREATE TABLE statements.
SQL_TYPES = {"salary": "numeric(12,2)", "amount": "numeric(12,2)"}

SCHEMA = {
    "users": {
        "multiplier": 1.0,
        "columns": [
            {"name": "id", "type": "user_id", "anon": {"strategy": "random_uuid"}},
            {
                "name": "company_id",
                "type": "company_id",
                "anon": {"strategy": "random_uuid"},
            },
            {"name": "name", "type": "name", "anon": {"strategy": "faker_name"}},
            {"name": "email", "type": "email", "anon": {"strategy": "faker_email"}},
            {
                "name": "password",
                "type": "hash",
                "anon": {"strategy": "fixed", "value": "$2a$12$FixedHash12345"},
            },
            {
                "name": "status",
                "type": "status",
                "anon": {
                    "strategy": "random_choice",
                    "options": list(STATUS_WEIGHTS),
                },
            },
            {
                "name": "salary",
                "type": "salary",
                "anon": {
                    "strategy": "dp_laplace",
                    "epsilon": 0.5,
                    "bounds": [0.0, 10000.0],
                    "scale": 2,
                },
            },
        ],
    },
    "orders": {
        "multiplier": 2.0,
        "columns": [
            {"name": "id", "type": "order_id", "anon": {"strategy": "random_uuid"}},
            {
                "name": "user_id",
                "type": "order_user",
                "anon": {"strategy": "random_uuid"},
            },
            {
                "name": "credit_card",
                "type": "cc",
                "anon": {"strategy": "fake_credit_card"},
            },
            {"name": "amount", "type": "amount", "anon": None},
        ],
    },
}


# ==========================================
# PATH HANDLING
# ==========================================
def get_output_dir(path=None):
    if path is None:
        script_dir = Path(__file__).resolve().parent
        path = script_dir.parent / OUTPUT_DIR_NAME
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# ==========================================
# HELPERS
# ==========================================
def deterministic_id(prefix, i):
    return f"ID-{prefix}-{i:08d}"


def zipf_rank(rng, n, s):
    """A rank in 1..n with probability roughly proportional to 1/rank^s
    (inverse transform of the continuous bounded Zipf law, O(1) memory)."""
    u = rng.random()
    if abs(s - 1.0) < 1e-9:
        rank = math.exp(u * math.log(n + 1))
    else:
        a = 1.0 - s
        rank = ((((n + 1) ** a) - 1.0) * u + 1.0) ** (1.0 / a)
    return min(n, max(1, int(rank)))


class Generator:
    """Draws every value from one seeded generator, so the same seed gives
    the same dump."""

    def __init__(self, seed, base_rows, null_rates):
        self.rng = random.Random(seed)
        self.base_rows = base_rows
        self.null_rates = null_rates
        self.companies = max(1, base_rows // 50)
        self.status_names = list(STATUS_WEIGHTS)
        self.status_cum = []
        total = 0.0
        for weight in STATUS_WEIGHTS.values():
            total += weight
            self.status_cum.append(total)

    def value(self, column, row_index):
        """The value of `column` in row `row_index`, or None for NULL."""
        rate = self.null_rates.get(column["name"], 0.0)
        if rate and self.rng.random() < rate:
            return None

        col_type = column["type"]
        rng = self.rng
        if col_type == "user_id":
            return deterministic_id("USR", row_index)
        if col_type == "order_id":
            return deterministic_id("ORD", row_index)
        if col_type == "order_user":
            rank = zipf_rank(rng, self.base_rows, ORDERS_ZIPF_S)
            return deterministic_id("USR", rank)
        if col_type == "company_id":
            return deterministic_id("CMP", rng.randint(1, self.companies))
        if col_type == "name":
            return f"Customer {row_index}"
        if col_type == "email":
            return f"user_{row_index}@test.com"
        if col_type == "hash":
            return "$2a$12$OriginalHashNeverSeen..."
        if col_type == "status":
            return rng.choices(self.status_names, cum_weights=self.status_cum)[0]
        if col_type == "cc":
            return f"4532-{rng.randint(1000, 9999)}-{rng.randint(0, 9999):04d}-1234"
        if col_type == "salary":
            return round(rng.lognormvariate(SALARY_MU, SALARY_SIGMA), 2)
        if col_type == "amount":
            return round(rng.lognormvariate(AMOUNT_MU, AMOUNT_SIGMA), 2)
        return "UNKNOWN"


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:.2f}"
    return "'" + value.replace("'", "''") + "'"


def copy_field(value):
    if value is None:
        return "\\N"
    if isinstance(value, float):
        return f"{value:.2f}"
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


class GroundTruth:
    """Exact statistics of the generated data, for the utility evaluation."""

    def __init__(self):
        self.tables = {}

    def add(self, table, column, value):
        stats = self.tables.setdefault(table, {}).setdefault(
            column, {"count": 0, "nulls": 0}
        )
        stats["count"] += 1
        if value is None:
            stats["nulls"] += 1
        elif isinstance(value, float):
            stats["sum"] = stats.get("sum", 0.0) + value
            stats["sum_squares"] = stats.get("sum_squares", 0.0) + value * value
            stats["min"] = min(stats.get("min", value), value)
            stats["max"] = max(stats.get("max", value), value)
        elif column == "status":
            freq = stats.setdefault("frequencies", {})
            freq[value] = freq.get(value, 0) + 1

    def finish(self):
        for columns in self.tables.values():
            for stats in columns.values():
                present = stats["count"] - stats["nulls"]
                if "sum" in stats and present:
                    mean = stats["sum"] / present
                    stats["mean"] = mean
                    stats["variance"] = stats["sum_squares"] / present - mean * mean
        return self.tables


class TextWriter:
    def __init__(self, binary):
        self.binary = binary

    def write(self, text):
        self.binary.write(text.encode("utf-8"))


# ==========================================
# TOML GENERATOR
# ==========================================
def generate_toml(filepath):
    print(f"[*] Generating TOML config: {filepath}")

    with open(filepath, "w") as f:
        for table_name, table_data in SCHEMA.items():
            f.write(f"# --- TABLE {table_name.upper()} ---\n")
            f.write(f"[tables.{table_name}]\n\n")

            for col in table_data["columns"]:
                anon = col.get("anon")
                if not anon:
                    continue

                f.write(f"  [[tables.{table_name}.columns]]\n")
                f.write(f'  name = "{col["name"]}"\n')

                for key, value in anon.items():
                    if isinstance(value, str):
                        f.write(f'  {key} = "{value}"\n')
                    elif isinstance(value, list):
                        options = ", ".join(
                            f'"{v}"' if isinstance(v, str) else str(v) for v in value
                        )
                        f.write(f"  {key} = [{options}]\n")
                    else:
                        f.write(f"  {key} = {value}\n")

                f.write("\n")


# ==========================================
# SQL GENERATOR + STATS
# ==========================================
def generate_sql(filepath, base_rows, mode, stats_path, generator, truth):
    print(f"[*] Generating SQL ({mode.upper()}): {filepath}")

    total_lines = 0

    # mtime=0 keeps the gzip header, and so the file hash, reproducible.
    with open(filepath, "wb") as raw, gzip.GzipFile(
        fileobj=raw, mode="wb", compresslevel=3, mtime=0
    ) as gz:
        f = TextWriter(gz)
        for table_name, table_data in SCHEMA.items():
            target_rows = int(base_rows * table_data["multiplier"])
            print(f"    -> {table_name}: {target_rows} rows")

            columns = table_data["columns"]
            columns_str = ", ".join(c["name"] for c in columns)

            definitions = ", ".join(
                f"{c['name']} {SQL_TYPES.get(c['type'], 'text')}" for c in columns
            )
            f.write(f"CREATE TABLE {table_name} ({definitions});\n\n")

            def row(i):
                values = [generator.value(col, i) for col in columns]
                for col, value in zip(columns, values):
                    truth.add(table_name, col["name"], value)
                return values

            if mode == "copy":
                f.write(f"COPY {table_name} ({columns_str}) FROM STDIN;\n")

                for i in range(1, target_rows + 1):
                    f.write("\t".join(copy_field(v) for v in row(i)) + "\n")
                    total_lines += 1

                f.write("\\.\n\n")

            else:
                for chunk_start in range(1, target_rows + 1, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE - 1, target_rows)

                    f.write(f"INSERT INTO {table_name} ({columns_str}) VALUES\n")

                    for i in range(chunk_start, chunk_end + 1):
                        line = f"({', '.join(sql_literal(v) for v in row(i))})"
                        f.write(line + (",\n" if i < chunk_end else ";\n"))
                        total_lines += 1

    # Save stats
    with open(stats_path, "w") as s:
        s.write(str(total_lines))


def parse_null_rates(items):
    rates = {}
    for item in items:
        name, _, rate = item.partition("=")
        rates[name] = float(rate)
    return rates


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GhostDump Data Generator")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--mode", choices=["insert", "copy"], default="insert")
    parser.add_argument(
        "--seed", type=int, default=42, help="same seed, same dump (default 42)"
    )
    parser.add_argument(
        "--null-rate",
        action="append",
        default=["email=0.02", "salary=0.05", "amount=0.01"],
        metavar="COLUMN=RATE",
        help="share of NULL values in a column (repeatable)",
    )
    parser.add_argument(
        "--output-dir", help="where to write the files (default: generated/)"
    )

    args = parser.parse_args()

    output_dir = get_output_dir(args.output_dir)

    sql_path = output_dir / "dump.sql.gz"
    toml_path = output_dir / "rules.toml"
    stats_path = output_dir / "stats.txt"
    truth_path = output_dir / "ground_truth.json"

    generator = Generator(args.seed, args.rows, parse_null_rates(args.null_rate))
    truth = GroundTruth()

    generate_toml(toml_path)
    generate_sql(sql_path, args.rows, args.mode, stats_path, generator, truth)

    with open(truth_path, "w") as t:
        summary = {
            "seed": args.seed,
            "rows": args.rows,
            "mode": args.mode,
            "tables": truth.finish(),
        }
        json.dump(summary, t, indent=2, sort_keys=True)

    print("\n[✓] Generation completed successfully")
    print(f"    Output directory: {output_dir}")
