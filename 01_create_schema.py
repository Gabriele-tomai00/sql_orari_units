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

DEFAULT_DB = "2025-2026_data/university.db"

# ---------------------------------------------------------------------------
# DDL — base tables
# ---------------------------------------------------------------------------

DDL_STAFF = """
CREATE TABLE IF NOT EXISTS staff (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Raw full name as found in the source (e.g. "Rossi Mario")
    name_and_surname     TEXT NOT NULL,
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
DDL_SUBJECT = """
CREATE TABLE IF NOT EXISTS subject (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,

    subject_code             TEXT,

    degree_program_name      TEXT,
    degree_program_name_eng  TEXT,
    degree_program_code      TEXT,

    subject_name             TEXT,
    
    study_code               TEXT,
    academic_year            TEXT,

    teams_code               TEXT,
    professors               TEXT,
    main_professor_id        TEXT,
    period                   TEXT,
    last_update              TEXT
);
"""

DDL_DEGREE_PROGRAM = """
CREATE TABLE IF NOT EXISTS degree_program (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    name            TEXT,
    url             TEXT,
    department      TEXT,
    type            TEXT,
    duration        TEXT,
    location        TEXT,
    language        TEXT

);
"""

DDL_CALENDAR_LESSON = """
CREATE TABLE IF NOT EXISTS calendar_lesson (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    subject_code        TEXT,

    degree_program_name TEXT,
    degree_program_code TEXT,

    subject_name        TEXT,

    study_year_code     TEXT,
    curriculum          TEXT,

    date                TEXT,
    start_time          TEXT,
    end_time            TEXT,

    department          TEXT,

    room_code           TEXT,
    room_name           TEXT,
    site_code           TEXT,
    site_name           TEXT,
    address             TEXT,

    professors          TEXT,
    cancelled           TEXT DEFAULT 'no',
    url                 TEXT
);
"""

DDL_ROOM_EVENT = """
CREATE TABLE IF NOT EXISTS room_event (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    site_code           TEXT,
    room_code           TEXT,
    date                TEXT,
    last_update         TEXT,
    site_name           TEXT,
    room_name           TEXT,
    start_time          TEXT,
    end_time            TEXT,
    name_event          TEXT,
    professors          TEXT,
    cancelled           TEXT DEFAULT 'no',
    event_type          TEXT
);
"""

DDL_ROOM_INFO = """
CREATE TABLE IF NOT EXISTS room_info (
    room_code               TEXT PRIMARY KEY,

    room_name               TEXT,
    site_name               TEXT,
    site_code               TEXT,
    address                 TEXT,
    floor                   TEXT,
    room_type               TEXT,
    capacity                TEXT,
    accessible              BOOL,
    maps_url                TEXT,
    equipment               TEXT,
    url                     TEXT
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
        con.execute(DDL_STAFF)
        con.execute(DDL_SUBJECT)
        con.execute(DDL_DEGREE_PROGRAM)
        con.execute(DDL_CALENDAR_LESSON)
        con.execute(DDL_ROOM_EVENT)
        con.execute(DDL_ROOM_INFO)

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