"""
02_insert_data.py
-----------------
Loads personali.json and insegnamento.json and inserts all rows
into the SQLite database created by 01_create_db.py.

Usage:
    python 02_insert_data.py
    python 02_insert_data.py --personale path/to/personale.json \
                              --insegnamento path/to/insegnamento.json \
                              --db path/to/university.db
"""

import argparse
import json
import sqlite3
from pathlib import Path

DEFAULT_PERSONALE    = "personale.json"
DEFAULT_insegnamento = "insegnamenti.json"
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
            "nome":                 meta.get("nome"),
            "role":                 meta.get("role"),
            "department":           meta.get("department"),
            "department_staff_url": meta.get("department_staff_url"),
            "phone":                meta.get("phone"),
            "email":                meta.get("email"),
            "last_updated":         meta.get("last_updated"),
        })
    return rows


def load_insegnamento(path: Path) -> list[dict]:
    """Parse insegnamento.json — top-level structure: [...]"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for entry in entries:
        meta = entry.get("metadata", {})
        rows.append({
            "course_name":         meta.get("course_name"),
            "course_code":         meta.get("course_code"),
            "teams_code":          meta.get("teams_code"),
            "degree_program":      meta.get("degree_program"),
            "degree_program_eng":  meta.get("degree_program_eng"),
            "degree_program_code": meta.get("degree_program_code"),
            "academic_year":       meta.get("academic_year"),
            "professors_raw":      meta.get("teacher_name"),  # kept raw, no normalization
            "professor_id":        meta.get("teacher_id"),
            "period":              meta.get("period"),
            "af_id":               meta.get("AF_ID"),
            "url_o365":            meta.get("URL_O365"),
            "last_update":         meta.get("last_update"),
        })
    return rows

# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def insert_data(db_path: Path, personale_path: Path, insegnamento_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}. Run 01_create_db.py first."
        )

    con = sqlite3.connect(db_path)

    with con:
        personale_rows = load_personale(personale_path)
        con.executemany(
            """INSERT INTO personale
               (nome, role, department, department_staff_url, phone, email, last_updated)
               VALUES (:nome, :role, :department, :department_staff_url,
                       :phone, :email, :last_updated)""",
            personale_rows,
        )
        print(f"[personale]    inserted {len(personale_rows):>6} rows")

        insegnamento_rows = load_insegnamento(insegnamento_path)
        con.executemany(
            """INSERT INTO insegnamento
               (course_name, course_code, teams_code, degree_program, degree_program_eng,
                degree_program_code, academic_year, professors_raw, professor_id,
                period, af_id, url_o365, last_update)
               VALUES (:course_name, :course_code, :teams_code, :degree_program,
                       :degree_program_eng, :degree_program_code, :academic_year,
                       :professors_raw, :professor_id, :period, :af_id,
                       :url_o365, :last_update)""",
            insegnamento_rows,
        )
        print(f"[insegnamento] inserted {len(insegnamento_rows):>6} rows")

    con.close()
    print(f"\nData inserted into: {db_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert data into university.db")
    parser.add_argument("--personale",    default=DEFAULT_PERSONALE)
    parser.add_argument("--insegnamento", default=DEFAULT_insegnamento)
    parser.add_argument("--db",           default=DEFAULT_DB)
    args = parser.parse_args()

    insert_data(
        db_path=Path(args.db),
        personale_path=Path(args.personale),
        insegnamento_path=Path(args.insegnamento),
    )