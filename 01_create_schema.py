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
    nome_and_surname     TEXT NOT NULL,
    role                 TEXT,
    department           TEXT,
    department_url       TEXT,
    phone                TEXT,
    email                TEXT,
    last_update          TEXT
);
"""

# course is RETI 1
# degree is INGEGNERIA
DDL_INSEGNAMENTO = """
CREATE TABLE IF NOT EXISTS insegnamento (
    subject_code                       INTEGER PRIMARY KEY AUTOINCREMENT,

    degree_program_name      TEXT,
    degree_program_name_eng  TEXT,
    degree_program_code      TEXT,

    subject_name             TEXT NOT NULL,
    
    study_code               TEXT,
    academic_year            TEXT,

    teams_code               TEXT,
    professors               TEXT,
    main_professor_id        TEXT,
    period                   TEXT,
    last_update              TEXT
);
"""

DDL_LEZIONE = """
CREATE TABLE IF NOT EXISTS lezione (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    subject_code        TEXT NOT NULL,

    degree_program_name TEXT NOT NULL,
    degree_program_code TEXT NOT NULL,

    subject_name        TEXT NOT NULL,

    study_year_code     TEXT NOT NULL,
    curriculum          TEXT,

    date                TEXT NOT NULL,
    start_time          TEXT NOT NULL,
    end_time            TEXT NOT NULL,
    full_location       TEXT,
    department          TEXT NOT NULL,

    professors          TEXT,
    cancelled           TEXT DEFAULT 'no', 
    url                 TEXT
);
"""

DDL_EVENTO_AULA = """
CREATE TABLE IF NOT EXISTS evento (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    subject_code        TEXT NOT NULL,

    degree_program_name TEXT NOT NULL,
    degree_program_code TEXT NOT NULL,

    subject_name        TEXT NOT NULL,

    study_year_code     TEXT NOT NULL,
    curriculum          TEXT,

    date                TEXT NOT NULL,
    start_time          TEXT NOT NULL,
    end_time            TEXT NOT NULL,
    full_location       TEXT,
    department          TEXT NOT NULL,

    professors          TEXT,
    cancelled           TEXT DEFAULT 'no', 
    url                 TEXT
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
        con.execute(DDL_INSEGNAMENTO)
        con.execute(DDL_LEZIONE)


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