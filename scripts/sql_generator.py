import argparse
import gzip
import random
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
CHUNK_SIZE = 10_000
OUTPUT_DIR_NAME = "generated"

SCHEMA = {
    "users": {
        "multiplier": 1.0,
        "columns": [
            {"name": "id", "type": "uuid", "anon": {"strategy": "random_uuid"}},
            {"name": "company_id", "type": "uuid", "anon": {"strategy": "random_uuid"}},
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
                    "options": ["ACTIVE", "PENDING", "INACTIVE"],
                },
            },
            {
                "name": "salary",
                "type": "float",
                "anon": {
                    "strategy": "dp_laplace",
                    "epsilon": 0.5,
                    "sensitivity": 15000.0,
                },
            },
        ],
    },
    "orders": {
        "multiplier": 2.0,
        "columns": [
            {"name": "id", "type": "uuid", "anon": {"strategy": "random_uuid"}},
            {
                "name": "user_id",
                "type": "uuid_ref",
                "anon": {"strategy": "random_uuid"},
            },
            {
                "name": "credit_card",
                "type": "cc",
                "anon": {"strategy": "fake_credit_card"},
            },
            {"name": "amount", "type": "float", "anon": None},
        ],
    },
}


# ==========================================
# PATH HANDLING
# ==========================================
def get_output_dir():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    output_dir = project_root / OUTPUT_DIR_NAME
    output_dir.mkdir(exist_ok=True)
    return output_dir


# ==========================================
# HELPERS
# ==========================================
def deterministic_id(prefix, i):
    return f"ID-{prefix}-{i:08d}"


def generate_value(col_type, row_index, base_rows, for_copy=False):
    if col_type == "uuid":
        val = deterministic_id("GEN", row_index)

    elif col_type == "uuid_ref":
        parent_index = (row_index % base_rows) + 1
        val = deterministic_id("GEN", parent_index)

    elif col_type == "name":
        val = f"Customer {row_index}"

    elif col_type == "email":
        val = f"user_{row_index}@test.com"

    elif col_type == "hash":
        val = "$2a$12$OriginalHashNeverSeen..."

    elif col_type == "status":
        val = random.choice(("ACTIVE", "PENDING", "INACTIVE"))

    elif col_type == "cc":
        val = f"4532-{(row_index % 9000) + 1000}-0000-1234"

    elif col_type == "float":
        val = f"{100 + (row_index % 9900):.2f}"

    else:
        val = "UNKNOWN"

    if for_copy:
        return val
    else:
        return f"'{val}'" if not val.replace(".", "", 1).isdigit() else val


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
                        options = ", ".join(f'"{v}"' for v in value)
                        f.write(f"  {key} = [{options}]\n")
                    else:
                        f.write(f"  {key} = {value}\n")

                f.write("\n")


# ==========================================
# SQL GENERATOR + STATS
# ==========================================
def generate_sql(filepath, base_rows, mode, stats_path):
    print(f"[*] Generating SQL ({mode.upper()}): {filepath}")

    total_lines = 0

    with gzip.open(filepath, "wt", compresslevel=3, encoding="utf-8") as f:
        for table_name, table_data in SCHEMA.items():
            target_rows = int(base_rows * table_data["multiplier"])
            print(f"    -> {table_name}: {target_rows} rows")

            columns = table_data["columns"]
            columns_str = ", ".join(c["name"] for c in columns)

            if mode == "copy":
                f.write(f"COPY {table_name} ({columns_str}) FROM STDIN;\n")

                for i in range(1, target_rows + 1):
                    values = [
                        generate_value(col["type"], i, base_rows, for_copy=True)
                        for col in columns
                    ]
                    f.write("\t".join(values) + "\n")
                    total_lines += 1

                f.write("\\.\n\n")

            else:
                for chunk_start in range(1, target_rows + 1, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE - 1, target_rows)

                    f.write(f"INSERT INTO {table_name} ({columns_str}) VALUES\n")

                    for i in range(chunk_start, chunk_end + 1):
                        values = [
                            generate_value(col["type"], i, base_rows) for col in columns
                        ]

                        line = f"({', '.join(values)})"
                        f.write(line + (",\n" if i < chunk_end else ";\n"))
                        total_lines += 1

    # Save stats
    with open(stats_path, "w") as s:
        s.write(str(total_lines))


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GhostDump Data Generator")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--mode", choices=["insert", "copy"], default="insert")

    args = parser.parse_args()

    output_dir = get_output_dir()

    sql_path = output_dir / "dump.sql.gz"
    toml_path = output_dir / "rules.toml"
    stats_path = output_dir / "stats.txt"

    generate_toml(toml_path)
    generate_sql(sql_path, args.rows, args.mode, stats_path)

    print("\n[✓] Generation completed successfully")
    print(f"    Output directory: {output_dir}")
