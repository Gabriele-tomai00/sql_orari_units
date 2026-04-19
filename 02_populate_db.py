"""
02_insert_data.py
-----------------

Usage:
    python 02_insert_data.py
"""

import json
import sqlite3
from pathlib import Path
from utils import normalize_text

DEFAULT_STAFF                   =    "2025-2026_data/address_book.json"
DEFAULT_SUBJECT                 =    "2025-2026_data/courses_with_teams_code.json"
DEFAULT_DEGREE_PROGRAM          =    "2025-2026_data/full_degree_programs.json"
DEFAULT_CALENDAR_LESSONS_DIR    =    "2025-2026_data/lessons_calendar/"
DEFAULT_ROOM_CALENDAR_DIR       =    "2025-2026_data/rooms_calendar/"
DEFAULT_ROOM_INFO               =    "2025-2026_data/info_rooms.json"

DEFAULT_DB                      =    "2025-2026_data/university.db"

# ---------------------------------------------------------------------------
# Loaders

def load_staff(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for entry in entries:
        meta = entry.get("metadata", {})
        rows.append({
            "name_and_surname":     normalize_text(meta.get("nome")),
            "role":                 normalize_text(meta.get("role")),
            "department":           normalize_text(meta.get("department")),
            "department_url":         meta.get("department_url"),   # URL — do not normalize
            "phone":                  meta.get("phone"),            # phone number — do not normalize
            "email":                  meta.get("email"),            # email — do not normalize
            "last_update":            meta.get("last_updated"),
        })
    return rows


def load_subject(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for entry in entries:
        meta = entry.get("metadata", {})
        rows.append({
            "subject_code":              meta.get("AF_ID"),                          # 535588

            "degree_program_name":       normalize_text(meta.get("degree_program")),     # "COMPUTER ENGINEERING"
            "degree_program_name_eng":   normalize_text(meta.get("degree_program_eng")), # "COMPUTER ENGINEERING"
            "degree_program_code":         meta.get("degree_program_code"), # IN23 — code, do not normalize

            "subject_name":              normalize_text(meta.get("course_name")),        # COMPLEXITY AND CRYPTOGRAPHY

            "study_code":                  meta.get("course_code"),         # 504MI — code, do not normalize
            "academic_year":               meta.get("academic_year"),

            "teams_code":                  meta.get("teams_code"),          # Teams code — do not normalize
            "professors":                normalize_text(meta.get("teacher_name")),
            "main_professor_id":           meta.get("teacher_id"),
            "period":                      meta.get("period"),              # e.g. "S1" — code, do not normalize
            "last_update":                 meta.get("last_update"),
        })
    return rows


def load_degree_program(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for meta in entries:
        # Normalize equipment string values but keep the JSON structure intact
        equipment = meta.get("equipment")
        if isinstance(equipment, dict):
            equipment = {k: normalize_text(v) if isinstance(v, str) else v for k, v in equipment.items()}

        rows.append({
            "name":            normalize_text(meta.get("name")),
            "category":        normalize_text(meta.get("category")),
            "department":      normalize_text(meta.get("department")),
            "type":            normalize_text(meta.get("type")),
            "duration":        normalize_text(meta.get("duration")),
            "location":        normalize_text(meta.get("location")),
            "language":        normalize_text(meta.get("language")),
            "url":             meta.get("link"),               # URL — do not normalize
        })
    return rows


def load_lessons(calendar_lessons_dir: Path) -> list[dict]:
    """
    Parse all JSON files inside lezioni_dir.
    Each file contains a list of lesson objects.
    """

    if not calendar_lessons_dir.exists() or not calendar_lessons_dir.is_dir():
        raise FileNotFoundError(f"Events directory not found: {calendar_lessons_dir}")

    json_files = sorted(calendar_lessons_dir.glob("*.json"))
    if not json_files:
        print(f"[lesson]       no JSON files found in {calendar_lessons_dir}")
        return []

    rows = []

    for json_file in json_files:
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)

        entries = data if isinstance(data, list) else data.get("entries", [])

        for meta in entries:

            rows.append({
                "subject_code":          meta.get("subject_code"),          # EC535583 — code

                "degree_program_name":   normalize_text(meta.get("degree_program_name")),
                "degree_program_code":     meta.get("degree_program_code"), # IN23 — code

                "subject_name":          normalize_text(meta.get("subject_name")),

                "study_year_code":         meta.get("study_year_code"),     # IN23+1+|1 — code
                "curriculum":            normalize_text(meta.get("curriculum")),

                "date":                    meta.get("date"),                # ISO date — do not normalize
                "start_time":              meta.get("start_time"),          # HH:MM — do not normalize
                "end_time":                meta.get("end_time"),
                "department":            normalize_text(meta.get("department")),

                "room_code":               meta.get("room_code"),           # 035_2 — code
                "room_name":             normalize_text(meta.get("room_name")),
                "site_code":               meta.get("site_code"),           # AA01 — code
                "site_name":             normalize_text(meta.get("site_name")),
                "address":               normalize_text(meta.get("address")),

                "professors":            normalize_text(meta.get("professors")),
                "cancelled":               meta.get("cancelled", "no"),
                "url":                     meta.get("url"),                 # URL — do not normalize
            })

    print(f"[lesson]       loaded {len(rows):>6} rows from {len(json_files)} file(s)")
    return rows

def load_room_calendar(room_calendar_dir: Path) -> list[dict]:
    """
    Parse all JSON files inside room_calendar_dir.
    Each file is a list of event objects with a 'metadata' key.
    """
    if not room_calendar_dir.exists() or not room_calendar_dir.is_dir():
        raise FileNotFoundError(f"Events directory not found: {room_calendar_dir}")

    json_files = sorted(room_calendar_dir.glob("*.json"))
    if not json_files:
        print(f"[lezione]       no JSON files found in {room_calendar_dir}")
        return []

    rows = []
    for json_file in json_files:
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)

        entries = data if isinstance(data, list) else data.get("entries", [])

        for entry in entries:
            rows.append({
                "site_code":     entry.get("site_code"),            # code
                "room_code":     entry.get("room_code"),            # code
                "site_name":     normalize_text(entry.get("site_name")),
                "room_name":     normalize_text(entry.get("room_name")),
                "date":          entry.get("date"),                 # ISO date
                "last_update":   entry.get("last_update"),
                "start_time":    entry.get("start_time"),           # HH:MM
                "end_time":      entry.get("end_time"),
                "name_event":    normalize_text(entry.get("name_event")),
                "professors":    normalize_text(entry.get("professors")),
                "cancelled":    normalize_text(entry.get("cancelled")),
                "event_type":    normalize_text(entry.get("event_type")),
            })

    print(f"[calendario aule]       loaded {len(rows):>6} rows from {len(json_files)} file(s)")
    return rows


def load_info_aule(path: Path) -> list[dict]:

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", data) if isinstance(data, dict) else data

    rows = []
    for meta in entries:
        # Normalize equipment string values but keep the JSON structure intact
        equipment = meta.get("equipment")
        if isinstance(equipment, dict):
            equipment = {k: normalize_text(v) if isinstance(v, str) else v for k, v in equipment.items()}

        rows.append({
            "room_code":        meta.get("room_code"),         # 001_3 — code
            "room_name":        normalize_text(meta.get("room_name")),
            "site_name":        normalize_text(meta.get("site_name")),
            "site_code":        meta.get("site_code"),         # AH03 — code
            "address":          normalize_text(meta.get("address")),
            "floor":            normalize_text(meta.get("floor")),
            "room_type":        normalize_text(meta.get("room_type")),
            "capacity":         meta.get("capacity"),          # integer — do not normalize
            "accessible":       meta.get("accessible"),        # bool — do not normalize
            "maps_url":         meta.get("maps_url"),          # URL — do not normalize
            "equipment":        json.dumps(equipment, ensure_ascii=False),
            "url":              meta.get("url"),               # URL — do not normalize
        })
    return rows


# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def insert_data(
    db_path: Path,
    staff_path: Path,
    subject_path: Path,
    info_corsi_di_laurea: Path,
    calendar_lessons_dir: Path,
    room_calendar_dir: Path,
    info_aule: Path,
) -> None:
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}. Run 01_create_db.py first."
        )

    con = sqlite3.connect(db_path)

    with con:
        # -- staff --
        staff_rows = load_staff(staff_path)
        con.executemany(
            """INSERT INTO staff
               (name_and_surname, role, department, department_url, phone, email, last_update)
               VALUES (:name_and_surname, :role, :department, :department_url,
                       :phone, :email, :last_update)""",
            staff_rows,
        )
        print(f"[staff]    inserted {len(staff_rows):>6} rows")

        # -- subject --
        subject_rows = load_subject(subject_path)
        con.executemany(
            """INSERT INTO subject
               (subject_code, degree_program_name, degree_program_name_eng, degree_program_code,
                subject_name, study_code, academic_year, teams_code, professors,
                main_professor_id, period, last_update)
               VALUES (:subject_code, :degree_program_name, :degree_program_name_eng, :degree_program_code,
                       :subject_name, :study_code, :academic_year, :teams_code, :professors,
                       :main_professor_id, :period, :last_update)""",
            subject_rows,
        )
        print(f"[subject] inserted {len(subject_rows):>6} rows")

      # -- corsi di laurea --
        degree_program_rows = load_degree_program(info_corsi_di_laurea)
        con.executemany(
            """INSERT INTO degree_program
               (name, url, department, type, duration, location, language)
               VALUES (:name, :url, :department,
                       :type, :duration, :location, :language)""",
            degree_program_rows,
        )
        print(f"[subject] inserted {len(degree_program_rows):>6} rows")
        
        # -- lesson --
        calendar_lesson_rows = load_lessons(calendar_lessons_dir)
        if calendar_lesson_rows:
            con.executemany(
                """INSERT INTO calendar_lesson
                (subject_code, degree_program_name, degree_program_code, subject_name,
                    study_year_code, curriculum, date, start_time, end_time, department,
                    room_code, room_name, site_code, site_name, address,
                    professors, cancelled, url)
                VALUES (:subject_code, :degree_program_name, :degree_program_code, :subject_name,
                        :study_year_code, :curriculum, :date, :start_time, :end_time, :department,
                        :room_code, :room_name, :site_code, :site_name, :address,
                        :professors, :cancelled, :url)""",
                calendar_lesson_rows,
            )
            print(f"[lesson]       inserted {len(calendar_lesson_rows):>6} rows")


        # -- room_event --
        room_calendar_rows = load_room_calendar(room_calendar_dir)
        if room_calendar_rows:
            con.executemany(
                """INSERT INTO room_event
                   (site_code, room_code, site_name, room_name, date, last_update, start_time, end_time, name_event,
                    professors, cancelled, event_type)
                   VALUES (:site_code, :room_code, :site_name, :room_name, :date, :last_update, :start_time, :end_time, :name_event,
                           :professors, :cancelled, :event_type)""",
                room_calendar_rows,
            )
            print(f"[room event]       inserted {len(room_calendar_rows):>6} rows")

        # -- room_info --
        room_info_rows = load_info_aule(info_aule)
        if room_info_rows:
            con.executemany(
                """INSERT INTO room_info
                   (room_code, room_name, site_name, site_code, address, floor, room_type, capacity, accessible,
                    maps_url, equipment)
                   VALUES (:room_code, :room_name, :site_name, :site_code, :address, :floor, :room_type, :capacity, :accessible,
                           :maps_url, :equipment)""",
                room_info_rows,
            )
            print(f"[room info]       inserted {len(room_info_rows):>6} rows")

    con.close()
    print(f"\nData inserted into: {db_path.resolve()}")


if __name__ == "__main__":
    insert_data(
        db_path=Path(DEFAULT_DB),
        staff_path=Path(DEFAULT_STAFF),
        subject_path=Path(DEFAULT_SUBJECT),
        info_corsi_di_laurea=Path(DEFAULT_DEGREE_PROGRAM),
        calendar_lessons_dir=Path(DEFAULT_CALENDAR_LESSONS_DIR),
        room_calendar_dir=Path(DEFAULT_ROOM_CALENDAR_DIR),
        info_aule=Path(DEFAULT_ROOM_INFO),
    )