"""
01_create_schema.py
-------------------
Creates the SQLite database schema
Does NOT insert any data.

Usage:
    python 01_create_schema.py
    python 01_create_schema.py --db path/to/university.db
"""

import argparse
import sqlite3
from pathlib import Path
import os

DEFAULT_DB = "university.db"

# ---------------------------------------------------------------------------
# DDL — base tables
# ---------------------------------------------------------------------------

DDL_PERSONALE = """
CREATE TABLE IF NOT EXISTS personale (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Raw full name as found in the source (e.g. "Rossi Mario")
    nome                 TEXT NOT NULL,
    role                 TEXT,
    department           TEXT,
    department_staff_url TEXT,
    phone                TEXT,
    email                TEXT,
    last_updated         TEXT
);
"""

DDL_insegnamento = """
CREATE TABLE IF NOT EXISTS insegnamento (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    course_name          TEXT NOT NULL,
    course_code          TEXT,
    teams_code           TEXT,
    degree_program       TEXT,
    degree_program_eng   TEXT,
    degree_program_code  TEXT,
    academic_year        TEXT,
    -- Raw professors field — may contain one or more names separated by
    -- "#" or "," and may have IDs in parentheses, e.g.:
    --   "VERCESI PAOLO (014095)#CAMPAGNA DARIO"
    professors_raw       TEXT,
    professor_id         TEXT,
    period               TEXT,
    af_id                TEXT,
    url_o365             TEXT,
    last_update          TEXT
);
"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def create_schema(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=OFF;")  # no FK constraints by design

    with con:
        con.execute(DDL_PERSONALE)
        con.execute(DDL_insegnamento)

    con.close()
    print(f"Schema created: {db_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create university DB schema")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite file path")
    args = parser.parse_args()
    if Path(args.db).exists():
        print(f"Database file overwrite warning: {args.db} already exists. Deleting it to create a fresh schema.")
        os.remove(Path(args.db))
    create_schema(Path(args.db))