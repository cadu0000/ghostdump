import random
import argparse
import gzip

CHUNK_SIZE = 10_000

SCHEMA = {
    "users": {
        "multiplier": 1.0,
        "columns": [
            {"name": "id", "type": "uuid", "rule": '{ strategy = "hmac" }'},
            {"name": "company_id", "type": "uuid", "rule": '{ strategy = "hmac" }'},
            {"name": "name", "type": "name", "rule": '{ strategy = "faker_name" }'},
            {"name": "email", "type": "email", "rule": '{ strategy = "faker_email" }'},
            {"name": "password", "type": "hash", "rule": '{ strategy = "fixed", value = "$2a$12$FixedHash12345" }'},
            {"name": "status", "type": "status", "rule": None},
            {"name": "salary", "type": "float", "rule": '{ strategy = "dp_laplace", epsilon = 0.5, sensitivity = 15000.0 }'},
        ]
    },
    "orders": {
        "multiplier": 2.0,
        "columns": [
            {"name": "id", "type": "uuid", "rule": '{ strategy = "hmac" }'},
            {"name": "user_id", "type": "uuid_ref", "rule": '{ strategy = "hmac" }'},
            {"name": "credit_card", "type": "cc", "rule": '{ strategy = "fake_credit_card" }'},
            {"name": "amount", "type": "float", "rule": None},
        ]
    }
}

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
        return f"'{val}'" if not val.replace('.', '', 1).isdigit() else val


# ==========================================
# SQL GENERATOR
# ==========================================
def generate_sql(filename, base_rows, mode):
    print(f"[*] Generate SQL ({mode.upper()}): {filename}")

    with gzip.open(filename, "wt", compresslevel=3, encoding="utf-8") as f:

        for table_name, table_data in SCHEMA.items():
            target_rows = int(base_rows * table_data["multiplier"])
            print(f"    -> {table_name}: {target_rows} rows")

            columns = table_data["columns"]
            columns_str = ", ".join(c["name"] for c in columns)

            # ==========================================
            # COPY MODE
            # ==========================================
            if mode == "copy":
                f.write(f"COPY {table_name} ({columns_str}) FROM STDIN;\n")

                for i in range(1, target_rows + 1):
                    values = [
                        generate_value(col["type"], i, base_rows, for_copy=True)
                        for col in columns
                    ]
                    f.write("\t".join(values) + "\n")

                    if i % 1_000_000 == 0:
                        print(f"       {i//1_000_000}M rows...")

                f.write("\\.\n\n")

            # ==========================================
            # INSERT MODE
            # ==========================================
            else:
                for chunk_start in range(1, target_rows + 1, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE - 1, target_rows)

                    f.write(f"INSERT INTO {table_name} ({columns_str}) VALUES\n")

                    for i in range(chunk_start, chunk_end + 1):
                        values = [
                            generate_value(col["type"], i, base_rows)
                            for col in columns
                        ]

                        line = f"({', '.join(values)})"
                        if i < chunk_end:
                            f.write(line + ",\n")
                        else:
                            f.write(line + ";\n")

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GhostDump Generator (INSERT | COPY)")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--sql", type=str, default="test_dump.sql.gz")
    parser.add_argument("--mode", type=str, choices=["insert", "copy"], default="insert")

    args = parser.parse_args()

    generate_sql(args.sql, args.rows, args.mode)

    print("\n[✓]")