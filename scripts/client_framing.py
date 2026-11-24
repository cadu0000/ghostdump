"""Edge cases of comments, quoting and client commands, checked against the
real clients.

Each case is a script for psql, mysql or sqlite3 that writes values to
`users.name`. The script runs in the real client on a scratch database, and
the values the database stored are the secrets. GhostDump then runs on the
same script with a rule on `users.name`; a case leaks when a stored value
appears in its output. A refusal (GhostDump stops with an error) is not a
leak: it is the fail-closed outcome.

Needs psql, createdb and dropdb (connection from the usual PG* variables),
mysql (from its option files) and sqlite3. Example:

    cargo build --release
    PGHOST=/var/run/postgresql python3 scripts/client_framing.py

The exit status is 1 when a case leaks.
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRATCH = "ghostdump_client_case"
RULES = '[tables.users]\ncolumns = [{ name = "name", strategy = "fixed", value = "MASKED" }]\n'
SCHEMA = {
    "postgres": "CREATE TABLE users (id int, name text);\n",
    "mysql": "CREATE TABLE users (id int, name varchar(200));\n",
    "sqlite": "CREATE TABLE users (id int, name text);\n",
}

# (name, dialect, statements after the CREATE TABLE of `users`)
CASES = [
    ('pg nested comment with quote', 'postgres', "/* outer /* inner */ it's still a comment */\nINSERT INTO users (id, name) VALUES (1, 'SECRET_P1');\n-- '\n"),
    ('pg dollar quotes with quotes', 'postgres', "SELECT $$ it's $$, $tag$ a ' b $tag$;\nINSERT INTO users (id, name) VALUES (2, 'SECRET_P2');\n"),
    ('pg E string escaped quote', 'postgres', "SELECT E'it\\'s';\nINSERT INTO users (id, name) VALUES (3, 'SECRET_P3');\n"),
    ('pg U& string', 'postgres', "SELECT U&'d\\0061t\\+000061';\nINSERT INTO users (id, name) VALUES (4, 'SECRET_P4');\n"),
    ('pg adjacent literals across newline', 'postgres', "INSERT INTO users (id, name) VALUES (5, 'SECRET'\n'_P5');\n"),
    ('pg meta-command with quote', 'postgres', "\\echo it's a test\nINSERT INTO users (id, name) VALUES (6, 'SECRET_P6');\n"),
    ('pg comments between tokens', 'postgres', "INSERT /* c */ INTO /* c ' */ users -- c '\n (id, name) VALUES (7, 'SECRET_P7');\n"),
    ('pg quoted identifiers', 'postgres', 'INSERT INTO "users" ("id", "name") VALUES (8, \'SECRET_P8\');\n'),
    ('pg COPY with escapes', 'postgres', "COPY users (id, name) FROM stdin;\n9\tSECRET_P9\\t'x\n10\tSECRET_P10\n\\.\n"),
    ('pg comment openers inside string', 'postgres', "INSERT INTO users (id, name) VALUES (11, 'SECRET_P11 -- not /* a comment');\n"),
    ('pg dollar-quoted value', 'postgres', 'INSERT INTO users (id, name) VALUES (12, $$SECRET_P12$$);\n'),
    ('pg psql variable', 'postgres', "\\set v 'SECRET_P13'\nINSERT INTO users (id, name) VALUES (13, :'v');\n"),
    ('pg E string value', 'postgres', "INSERT INTO users (id, name) VALUES (14, E'SECRET_P14\\'s');\n"),
    ('pg subquery value', 'postgres', "INSERT INTO users (id, name) VALUES (16, (SELECT 'SECRET_P16'));\n"),
    ('pg positional insert', 'postgres', "INSERT INTO users VALUES (18, 'SECRET_P18');\n"),
    ('pg unterminated dollar tag lookalike', 'postgres', "SELECT 1 AS a$b;\nINSERT INTO users (id, name) VALUES (19, 'SECRET_P19');\n"),
    ('my hash comment with quote', 'mysql', "# it's a comment\nINSERT INTO users (id, name) VALUES (1, 'SECRET_M1');\n"),
    ('my double dash without space', 'mysql', "INSERT INTO users (id, name) VALUES (1--1, 'SECRET_M2');\n"),
    ('my dash dash space comment quote', 'mysql', "-- it's\nINSERT INTO users (id, name) VALUES (3, 'SECRET_M3');\n"),
    ('my \\g terminator', 'mysql', "INSERT INTO users (id, name) VALUES (4, 'SECRET_M4')\\g\nINSERT INTO users (id, name) VALUES (40, 'SECRET_M40');\n"),
    ('my double-quoted string', 'mysql', 'INSERT INTO users (id, name) VALUES (5, "SECRET_M5 \\"x\\"");\n'),
    ('my backslash before quote end', 'mysql', "INSERT INTO users (id, name) VALUES (6, 'SECRET_M6\\\\');\n"),
    ('my version comment insert', 'mysql', "/*!40000 INSERT INTO users (id, name) VALUES (7, 'SECRET_M7') */;\n"),
    ('my split version comment', 'mysql', "/*!40000 SELECT 1; */ INSERT INTO users (id, name) VALUES (8, 'SECRET_M8');\n"),
    ('my delimiter routine then insert', 'mysql', "DELIMITER //\nCREATE PROCEDURE p() BEGIN SELECT 'it''s'; END//\nDELIMITER ;\nINSERT INTO users (id, name) VALUES (9, 'SECRET_M9');\n"),
    ('my mariadb comment with quote', 'mysql', "/*M!100000 it's */ INSERT INTO users (id, name) VALUES (10, 'SECRET_M10');\n"),
    ('my introducers', 'mysql', "INSERT INTO users (id, name) VALUES (11, _utf8mb4'SECRET_M11'), (12, N'SECRET_M12'), (13, _binary 'SECRET_M13');\n"),
    ('my hex literal', 'mysql', 'INSERT INTO users (id, name) VALUES (14, 0x5345435245545F4D3134);\n'),
    ('my ansi quotes', 'mysql', 'SET sql_mode=\'ANSI_QUOTES\';\nINSERT INTO "users" ("id", "name") VALUES (15, \'SECRET_M15\');\n'),
    ('my \\G terminator', 'mysql', "INSERT INTO users (id, name) VALUES (16, 'SECRET_M16')\\G\n"),
    ('my comment with semicolon in insert', 'mysql', "INSERT INTO users (id, name) /* ; */ VALUES (17, 'SECRET_M17');\n"),
    ('my dash dash tab comment', 'mysql', "--\tit's\nINSERT INTO users (id, name) VALUES (18, 'SECRET_M18');\n"),
    ('my insert set', 'mysql', "INSERT INTO users SET id = 19, name = 'SECRET_M19';\n"),
    ('my positional insert', 'mysql', "INSERT INTO users VALUES (20, 'SECRET_M20');\n"),
    ('sq bracket identifiers', 'sqlite', "INSERT INTO [users] ([id], [name]) VALUES (1, 'SECRET_S1');\n"),
    ('sq backtick identifiers', 'sqlite', "INSERT INTO `users` (`id`, `name`) VALUES (2, 'SECRET_S2');\n"),
    ('sq dot command with quote', 'sqlite', ".print it's\nINSERT INTO users (id, name) VALUES (3, 'SECRET_S3');\n"),
    ('sq double-quoted string fallback', 'sqlite', 'INSERT INTO users (id, name) VALUES (4, "SECRET_S4");\n'),
    ('sq comment does not nest', 'sqlite', "/* a /* b */ INSERT INTO users (id, name) VALUES (5, 'SECRET_S5'); -- */\n"),
    ('sq blob value', 'sqlite', "INSERT INTO users (id, name) VALUES (6, X'5345435245545F5336');\n"),
    ('sq semicolon in identifier', 'sqlite', 'CREATE TABLE "a;b" (x);\nINSERT INTO users (id, name) VALUES (7, \'SECRET_S7\');\n'),
    ('sq positional insert', 'sqlite', "INSERT INTO users VALUES (8, 'SECRET_S8');\n"),
    ('sq backslash plain', 'sqlite', "INSERT INTO users (id, name) VALUES (9, 'SECRET_S9\\'); SELECT 'x';\n"),
    ('my DO with double dash', 'mysql', "DO 1--1;\nINSERT INTO users (id, name) VALUES (21, 'SECRET_M21');\n"),
    ('my SET with double dash', 'mysql', "SET @x = 1--1;\nINSERT INTO users (id, name) VALUES (22, 'SECRET_M22');\n"),
    ('my double dash then quote', 'mysql', "DO 1--1, 'it''s';\nINSERT INTO users (id, name) VALUES (23, 'SECRET_M23');\n"),
    ('sq hash line', 'sqlite', "# it's\nINSERT INTO users (id, name) VALUES (10, 'SECRET_S10');\n"),
    ('sq indented dot command', 'sqlite', "  .print it's\nINSERT INTO users (id, name) VALUES (12, 'SECRET_S12');\n"),
    ('sq dot command mid-statement', 'sqlite', "INSERT INTO users (id, name)\n.print x\nVALUES (11, 'SECRET_S11');\n"),
    ('pg \\g terminator', 'postgres', "INSERT INTO users (id, name) VALUES (20, 'SECRET_P20') \\g\nINSERT INTO users (id, name) VALUES (21, 'SECRET_P21');\n"),
    ('pg meta-command mid-line', 'postgres', "SELECT 1 \\echo it's\n;\nINSERT INTO users (id, name) VALUES (22, 'SECRET_P22');\n"),
    ('pg indented meta-command', 'postgres', "   \\echo it's\nINSERT INTO users (id, name) VALUES (23, 'SECRET_P23');\n"),
    ('pg separator after meta at start', 'postgres', "\\echo x \\\\ INSERT INTO users (id, name) VALUES (24, 'SECRET_P24');\n"),
    ('pg separator after meta mid-line', 'postgres', "SELECT 1 \\echo x \\\\ INSERT INTO users (id, name) VALUES (25, 'SECRET_P25');\n"),
    ('pg meta-command quoted arg', 'postgres', "\\echo 'it''s \\\\ x' it's\nINSERT INTO users (id, name) VALUES (26, 'SECRET_P26');\n"),
    ('pg backslash in identifier-ish', 'postgres', 'SELECT 1 AS "a\\b";\nINSERT INTO users (id, name) VALUES (27, \'SECRET_P27\');\n'),
    ('my \\g after DO', 'mysql', "DO 1 \\g\nINSERT INTO users (id, name) VALUES (24, 'SECRET_M24');\n"),
    ('my \\G after DO quote', 'mysql', "DO 'it''s' \\G\nINSERT INTO users (id, name) VALUES (25, 'SECRET_M25');\n"),
    ('my \\c clears', 'mysql', "SELECT 'x' \\c\nINSERT INTO users (id, name) VALUES (26, 'SECRET_M26');\n"),
    ('my \\d delimiter', 'mysql', "\\d //\nINSERT INTO users (id, name) VALUES (27, 'SECRET_M27')//\n\\d ;\nINSERT INTO users (id, name) VALUES (28, 'SECRET_M28');\n"),
    ('my backslash command with quote', 'mysql', "DO 1 \\! echo it's\n;\nINSERT INTO users (id, name) VALUES (29, 'SECRET_M29');\n"),
]

# Scripts that write the CREATE TABLE themselves.
FULL_SCRIPTS = [
    (
        "pg BOM and CRLF",
        "postgres",
        "\ufeffCREATE TABLE users (id int, name text);\r\n"
        "INSERT INTO users (id, name) VALUES (17, 'SECRET_P17');\r\n",
    ),
]


def run(command, stdin=None):
    return subprocess.run(command, input=stdin, capture_output=True, check=False)


def lines(output):
    return {line for line in output.decode(errors="replace").splitlines() if line}


def stored_postgres(script):
    run(["dropdb", "--if-exists", SCRATCH])
    run(["createdb", SCRATCH])
    run(["psql", "-X", "-q", "-d", SCRATCH], stdin=script)
    names = lines(run(["psql", "-X", "-At", "-d", SCRATCH, "-c", "SELECT name FROM users"]).stdout)
    run(["dropdb", "--if-exists", SCRATCH])
    return names


def stored_mysql(script):
    run(["mysql", "-e", f"DROP DATABASE IF EXISTS {SCRATCH}; CREATE DATABASE {SCRATCH}"])
    run(["mysql", "--force", SCRATCH], stdin=script)
    names = lines(run(["mysql", "-N", "-B", SCRATCH, "-e", "SELECT name FROM users"]).stdout)
    run(["mysql", "-e", f"DROP DATABASE IF EXISTS {SCRATCH}"])
    return names


def stored_sqlite(script):
    with tempfile.TemporaryDirectory() as scratch:
        database = os.path.join(scratch, "case.db")
        run(["sqlite3", database], stdin=script)
        return lines(run(["sqlite3", database, "SELECT name FROM users"]).stdout)


STORED = {"postgres": stored_postgres, "mysql": stored_mysql, "sqlite": stored_sqlite}


def check(binary, rules, dialect, script):
    """(stored secrets, outcome, leaked secrets, first error line)."""
    script = script.encode()
    secrets = STORED[dialect](script) - {"MASKED"}
    result = run([binary, "--dialect", dialect, "-c", rules], stdin=script)
    output = result.stdout.decode(errors="replace")
    leaked = sorted(secret for secret in secrets if secret in output)
    errors = [line for line in result.stderr.decode(errors="replace").splitlines() if "error" in line]
    if leaked:
        outcome = "LEAK"
    elif result.returncode:
        outcome = "refused"
    else:
        outcome = "masked"
    return secrets, outcome, leaked, errors[0] if errors else ""


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--binary", default=str(PROJECT_ROOT / "target" / "release" / "ghostdump")
    )
    parser.add_argument("--dialects", default="postgres,mysql,sqlite")
    args = parser.parse_args()
    wanted = args.dialects.split(",")

    cases = [(name, dialect, SCHEMA[dialect] + body) for name, dialect, body in CASES]
    cases += FULL_SCRIPTS
    counts = {}
    with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as rules:
        rules.write(RULES)
    try:
        for name, dialect, script in cases:
            if dialect not in wanted:
                continue
            secrets, outcome, leaked, error = check(args.binary, rules.name, dialect, script)
            counts[outcome] = counts.get(outcome, 0) + 1
            stored = ", ".join(sorted(secrets)) or "-"
            detail = f" {leaked}" if leaked else (f"  {error[:80]}" if error else "")
            print(f"{outcome:8} {dialect:8} {name:40} stored: {stored}{detail}")
    finally:
        os.unlink(rules.name)
    print(", ".join(f"{count} {outcome}" for outcome, count in sorted(counts.items())))
    sys.exit(1 if counts.get("LEAK") else 0)


if __name__ == "__main__":
    main()
