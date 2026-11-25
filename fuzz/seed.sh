#!/bin/sh
# Seeds the corpus of both targets with the dump fixtures, once per dialect:
# the first byte of an input chooses it (0 PostgreSQL, 1 MySQL, 2 SQLite).
set -e
cd "$(dirname "$0")"
for target in parser engine; do
    mkdir -p "corpus/$target"
    for fixture in ../tests/fixtures/*.sql; do
        for dialect in 0 1 2; do
            { printf "\\00$dialect"; cat "$fixture"; } > "corpus/$target/$dialect-$(basename "$fixture")"
        done
    done
done
