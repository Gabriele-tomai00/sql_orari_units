"""
03_build_index.py
-----------------
Builds and persists ChromaDB embedding indexes for columns used as
search filters in natural language queries.

Indexed columns:
  - subject.course_name            (e.g. "sw dev" → "SOFTWARE DEVELOPMENT METHODS")
  - subject.professors             (e.g. "Paolo Vercesi" → "VERCESI PAOLO (014095)")
  - subject.degree_program_name    (e.g. "informatica" → "COMPUTER ENGINEERING")
  - subject.academic_year          (e.g. "2025-2026" → "2025/2026")
  - subject.period                 (e.g. "primo semestre" → "S1")
  - staff.nome                      (fuzzy professor name matching)

Usage:
    python 03_build_index.py
    python 03_build_index.py
"""

import sqlite3
from pathlib import Path

import chromadb
from llama_index.core import Settings

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import TextNode
from llama_index.vector_stores.chroma import ChromaVectorStore

DEFAULT_DB = "2025-2026_data/university.db"
DEFAULT_CHROMA_DIR = "2025-2026_data/chroma_store"

from utils import *


def build_column_index(
    values: list[str],
    collection_name: str,
    chroma_client: chromadb.PersistentClient,
) -> VectorStoreIndex:
    """
    Creates (or recreates) a ChromaDB collection and builds a VectorStoreIndex
    from a list of string values. Deletes existing collection first to avoid
    stale embeddings after a DB update.
    """
    # Delete existing collection to ensure fresh embeddings on rebuild
    try:
        chroma_client.delete_collection(collection_name)
    except Exception:
        pass  # Collection did not exist yet — fine

    chroma_collection = chroma_client.get_or_create_collection(collection_name)
    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # One TextNode per distinct value
    nodes = [TextNode(text=v) for v in values if v and v.strip()]

    index = VectorStoreIndex(
        nodes,
        storage_context=storage_context,
        show_progress=True,
    )
    print(f"  [{collection_name}] indexed {len(nodes)} values")
    return index


def build_all_indexes(db_path: Path, chroma_dir: Path) -> None:
    chroma_dir.mkdir(parents=True, exist_ok=True)
    chroma_client = chromadb.PersistentClient(path=str(chroma_dir))
    con = sqlite3.connect(db_path)



# ------------ STAFF ---------------

    print("\nBuilding index: staff.name_and_surname")
    rows = con.execute(
        "SELECT DISTINCT name_and_surname FROM staff WHERE name_and_surname IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "staff__name_and_surname", chroma_client)

    print("\nBuilding index: staff.role")
    rows = con.execute(
        "SELECT DISTINCT role FROM staff WHERE role IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "staff__role", chroma_client)

    print("\nBuilding index: staff.department")
    rows = con.execute(
        "SELECT DISTINCT department FROM staff WHERE department IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "staff__department", chroma_client)

# ----------- SUBJECT ---------------

    print("\nBuilding index: subject.degree_program_name")
    rows = con.execute(
        "SELECT DISTINCT degree_program_name FROM subject WHERE degree_program_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "subject__degree_program_name", chroma_client)

    print("\nBuilding index: subject.degree_program_name_eng")
    rows = con.execute(
        "SELECT DISTINCT degree_program_name_eng FROM subject WHERE degree_program_name_eng IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "subject__degree_program_name_eng", chroma_client)

    print("\nBuilding index: subject.subject_name")
    rows = con.execute(
        "SELECT DISTINCT subject_name FROM calendar_lesson WHERE subject_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "subject__subject_name", chroma_client)

    print("\nBuilding index: subject.professors")
    rows = con.execute(
        "SELECT DISTINCT professors FROM subject WHERE professors IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "subject__professors", chroma_client)

    print("\nBuilding index: subject.period")
    rows = con.execute(
        "SELECT DISTINCT period FROM subject WHERE period IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "subject__period", chroma_client)

# ------------ DEGREE PROGRAM ---------------

    print("\nBuilding index: degree_program.name")
    rows = con.execute(
        "SELECT DISTINCT name FROM degree_program WHERE name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__name", chroma_client)

    print("\nBuilding index: degree_program.url")
    rows = con.execute(
        "SELECT DISTINCT url FROM degree_program WHERE url IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__url", chroma_client)

    print("\nBuilding index: degree_program.department")
    rows = con.execute(
        "SELECT DISTINCT department FROM degree_program WHERE department NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__department", chroma_client)

    print("\nBuilding index: degree_program.type")
    rows = con.execute(
        "SELECT DISTINCT type FROM degree_program WHERE type IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__type", chroma_client)

    print("\nBuilding index: degree_program.duration")
    rows = con.execute(
        "SELECT DISTINCT duration FROM degree_program WHERE duration NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__duration", chroma_client)

    print("\nBuilding index: degree_program.location")
    rows = con.execute(
        "SELECT DISTINCT location FROM degree_program WHERE location NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__location", chroma_client)

    print("\nBuilding index: degree_program.language")
    rows = con.execute(
        "SELECT DISTINCT language FROM degree_program WHERE language IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "degree_program__language", chroma_client)

# ------------ calendar_lesson ---------------

    print("\nBuilding index: calendar_lesson.degree_program_name")
    rows = con.execute(
        "SELECT DISTINCT degree_program_name FROM calendar_lesson WHERE degree_program_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__degree_program_name", chroma_client)

    print("\nBuilding index: calendar_lesson.subject_name")
    rows = con.execute(
        "SELECT DISTINCT subject_name FROM calendar_lesson WHERE subject_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__subject_name", chroma_client)

    print("\nBuilding index: calendar_lesson.study_year_code")
    rows = con.execute(
        "SELECT DISTINCT study_year_code FROM calendar_lesson WHERE study_year_code IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__study_year_code", chroma_client)

    print("\nBuilding index: calendar_lesson.curriculum")
    rows = con.execute(
        "SELECT DISTINCT curriculum FROM calendar_lesson WHERE curriculum IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__curriculum", chroma_client)

    print("\nBuilding index: calendar_lesson.date")
    rows = con.execute(
        "SELECT DISTINCT date FROM calendar_lesson WHERE date IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__date", chroma_client)

    print("\nBuilding index: calendar_lesson.department")
    rows = con.execute(
        "SELECT DISTINCT department FROM calendar_lesson WHERE department IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__department", chroma_client)

    print("\nBuilding index: calendar_lesson.room_name")
    rows = con.execute(
        "SELECT DISTINCT room_name FROM calendar_lesson WHERE room_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__room_name", chroma_client)

    print("\nBuilding index: calendar_lesson.site_name")
    rows = con.execute(
        "SELECT DISTINCT site_name FROM calendar_lesson WHERE site_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__site_name", chroma_client)

    print("\nBuilding index: calendar_lesson.address")
    rows = con.execute(
        "SELECT DISTINCT address FROM calendar_lesson WHERE address IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__address", chroma_client)

    print("\nBuilding index: calendar_lesson.professors")
    rows = con.execute(
        "SELECT DISTINCT professors FROM calendar_lesson WHERE professors IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "calendar_lesson__professors", chroma_client)


# ------------ ROOM EVENT ---------------

    print("\nBuilding index: room_event.site_name")
    rows = con.execute(
        "SELECT DISTINCT site_name FROM room_event WHERE site_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_event__site_name", chroma_client)

    print("\nBuilding index: room_event.room_name")
    rows = con.execute(
        "SELECT DISTINCT room_name FROM room_event WHERE room_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_event__room_name", chroma_client)

    print("\nBuilding index: room_event.name_event")
    rows = con.execute(
        "SELECT DISTINCT name_event FROM room_event WHERE name_event IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_event__name_event", chroma_client) 

    print("\nBuilding index: room_event.professors")
    rows = con.execute(
        "SELECT DISTINCT professors FROM room_event WHERE professors IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_event__professors", chroma_client)

    print("\nBuilding index: room_event.event_type")
    rows = con.execute(
        "SELECT DISTINCT event_type FROM room_event WHERE event_type IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_event__event_type", chroma_client)

# ------------ ROOM INFO ---------------

    print("\nBuilding index: room_info.site_name")
    rows = con.execute(
        "SELECT DISTINCT site_name FROM room_info WHERE site_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_info__site_name", chroma_client)

    print("\nBuilding index: room_info.room_name")
    rows = con.execute(
        "SELECT DISTINCT room_name FROM room_info WHERE room_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_info__room_name", chroma_client)

    print("\nBuilding index: room_info.address")
    rows = con.execute(
        "SELECT DISTINCT address FROM room_info WHERE address IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_info__address", chroma_client)

    print("\nBuilding index: room_info.floor")
    rows = con.execute(
        "SELECT DISTINCT floor FROM room_info WHERE floor IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_info__floor", chroma_client)

    print("\nBuilding index: room_info.room_type")
    rows = con.execute(
        "SELECT DISTINCT room_type FROM room_info WHERE room_type IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "room_info__room_type", chroma_client)

    con.close()
    print(f"\nAll indexes persisted to: {chroma_dir.resolve()}")


if __name__ == "__main__":
    build_all_indexes(Path(DEFAULT_DB), Path(DEFAULT_CHROMA_DIR))