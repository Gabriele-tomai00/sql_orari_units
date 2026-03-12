"""
02_insert_data.py
-----------------
Loads personale.json, insegnamenti.json, and all JSON files from the
'lezioni/' folder, then inserts all rows into the SQLite database
created by 01_create_db.py.

Usage:
    python 02_insert_data.py
"""

import argparse
import json
import sqlite3
from pathlib import Path

DEFAULT_PERSONALE    = "personale.json"
DEFAULT_INSEGNAMENTO = "insegnamenti.json"
DEFAULT_LEZIONI_DIR   = "schedule_lezioni/"
DEFAULT_DB           = "university.db"

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_personale(path: Path) -> list[dict]:
    """Parse personale.json — top-level structure: {"entries": [...]}"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for entry in entries:
        meta = entry.get("metadata", {})
        rows.append({
            "nome_and_surname":     meta.get("nome"),
            "role":                 meta.get("role"),
            "department":           meta.get("department"),
            "department_url":       meta.get("department_url"),
            "phone":                meta.get("phone"),
            "email":                meta.get("email"),
            "last_update":         meta.get("last_updated"),
        })
    return rows


def load_insegnamento(path: Path) -> list[dict]:
    """Parse insegnamenti.json — top-level structure: [...]"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for entry in entries:
        meta = entry.get("metadata", {})
        rows.append({
            "subject_code":      meta.get("AF_ID"),          # 535588

            "degree_program_name":       meta.get("degree_program"),        # "COMPUTER ENGINEERING",
            "degree_program_name_eng":   meta.get("degree_program_eng"),    # "COMPUTER ENGINEERING"
            "degree_program_code":       meta.get("degree_program_code"),    # IN23

            "subject_name":              meta.get("course_name"),    # COMPLEXITY AND CRYPTOGRAPHY
            
            "study_code":                meta.get("course_code"),    # 504MI
            "academic_year":             meta.get("academic_year"),

            "teams_code":                meta.get("teams_code"),
            "professors":                meta.get("teacher_name"),
            "main_professor_id":         meta.get("teacher_id"),
            "period":                    meta.get("period"),
            "last_update":               meta.get("last_update"),
        })
    return rows


def load_lezioni(lezioni_dir: Path) -> list[dict]:
    """
    Parse all JSON files inside lezioni_dir.
    Each file is a list of event objects with a 'metadata' key.
    """
    if not lezioni_dir.exists() or not lezioni_dir.is_dir():
        raise FileNotFoundError(f"Events directory not found: {lezioni_dir}")

    json_files = sorted(lezioni_dir.glob("*.json"))
    if not json_files:
        print(f"[lezione]       no JSON files found in {lezioni_dir}")
        return []

    rows = []
    for json_file in json_files:
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)

        # Each file is a list of event entries
        entries = data if isinstance(data, list) else data.get("entries", [])

        for entry in entries:
            meta = entry.get("metadata", {})
            rows.append({
                "subject_code":            meta.get("subject_code"),    # EC535583

                "degree_program_name":     meta.get("study_course"),    # "COMPUTER ENGINEERING",
                "degree_program_code":     meta.get("course_code"),     # IN23

                "subject_name":            meta.get("subject_name"),    # ADVANCED INTERNET TECHNOLOGIES

                "study_year_code":         meta.get("study_year_code"), # IN23+1+|1
                "curriculum":              meta.get("curriculum"),

                "date":                    meta.get("date_iso"),
                "start_time":              meta.get("start_time"),
                "end_time":                meta.get("end_time"),
                "full_location":           meta.get("full_location"),
                "department":              meta.get("department"),

                "professors":              meta.get("professor"),
                "cancelled":               meta.get("cancelled", "no"),
                "url":                     meta.get("url"),
            })

    print(f"[lezione]       loaded {len(rows):>6} rows from {len(json_files)} file(s)")
    return rows


# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def insert_data(
    db_path: Path,
    personale_path: Path,
    insegnamento_path: Path,
    lezioni_dir: Path,
) -> None:
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}. Run 01_create_db.py first."
        )

    con = sqlite3.connect(db_path)

    with con:
        # -- personale --
        personale_rows = load_personale(personale_path)
        con.executemany(
            """INSERT INTO personale
               (nome_and_surname, role, department, department_url, phone, email, last_update)
               VALUES (:nome_and_surname, :role, :department, :department_url,
                       :phone, :email, :last_update)""",
            personale_rows,
        )
        print(f"[personale]    inserted {len(personale_rows):>6} rows")

        # -- insegnamento --
        insegnamento_rows = load_insegnamento(insegnamento_path)
        con.executemany(
            """INSERT INTO insegnamento
               (subject_code, degree_program_name, degree_program_name_eng, degree_program_code,
                subject_name, study_code, academic_year, teams_code, professors,
                main_professor_id, period, last_update)
               VALUES (:subject_code, :degree_program_name, :degree_program_name_eng, :degree_program_code,
                       :subject_name, :study_code, :academic_year, :teams_code, :professors,
                       :main_professor_id, :period, :last_update)""",
            insegnamento_rows,
        )
        print(f"[insegnamento] inserted {len(insegnamento_rows):>6} rows")

        # -- lezione --
        lezione_rows = load_lezioni(lezioni_dir)
        if lezione_rows:
            con.executemany(
                """INSERT INTO lezione
                   (subject_code, degree_program_name, degree_program_code, subject_name,
                    study_year_code, curriculum, date, start_time, end_time, full_location,
                    department, professors, cancelled, url)
                   VALUES (:subject_code, :degree_program_name, :degree_program_code, :subject_name,
                           :study_year_code, :curriculum, :date, :start_time, :end_time, :full_location,
                           :department, :professors, :cancelled, :url)""",
                lezione_rows,
            )
            print(f"[lezione]       inserted {len(lezione_rows):>6} rows")

    con.close()
    print(f"\nData inserted into: {db_path.resolve()}")


if __name__ == "__main__":
    insert_data(
        db_path=Path(DEFAULT_DB),
        personale_path=Path(DEFAULT_PERSONALE),
        insegnamento_path=Path(DEFAULT_INSEGNAMENTO),
        lezioni_dir=Path(DEFAULT_LEZIONI_DIR),
    )