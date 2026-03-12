"""
03_build_index.py
-----------------
Builds and persists ChromaDB embedding indexes for columns used as
search filters in natural language queries.

Indexed columns:
  - insegnamento.course_name       (e.g. "sw dev" → "SOFTWARE DEVELOPMENT METHODS")
  - insegnamento.professors_raw    (e.g. "Paolo Vercesi" → "VERCESI PAOLO (014095)")
  - insegnamento.degree_program    (e.g. "informatica" → "COMPUTER ENGINEERING")
  - insegnamento.academic_year     (e.g. "2025-2026" → "2025/2026")
  - insegnamento.period            (e.g. "primo semestre" → "S1")
  - personale.nome                 (fuzzy professor name matching)

Usage:
    python 03_build_index.py
    python 03_build_index.py --db university.db --chroma-dir ./chroma_store
"""

import argparse
import sqlite3
from pathlib import Path

import chromadb
from llama_index.core import Settings

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import TextNode
from llama_index.vector_stores.chroma import ChromaVectorStore

DEFAULT_DB = "university.db"
DEFAULT_CHROMA_DIR = "./chroma_store"

from index_utils import *


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


# ------------ insegnamento ---------------

    # --- insegnamento.course_name ---
    print("\nBuilding index: insegnamento.course_name")
    rows = con.execute(
        "SELECT DISTINCT course_name FROM insegnamento WHERE course_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "insegnamento__course_name", chroma_client)


    # --- insegnamento.professors_raw ---
    print("\nBuilding index: insegnamento.professors_raw")
    rows = con.execute(
        "SELECT DISTINCT professors_raw FROM insegnamento WHERE professors_raw IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "insegnamento__professors_raw", chroma_client)

    # --- insegnamento.degree_program ---
    print("\nBuilding index: insegnamento.degree_program")
    rows = con.execute(
        "SELECT DISTINCT degree_program FROM insegnamento WHERE degree_program IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "insegnamento__degree_program", chroma_client)

    # --- insegnamento.academic_year ---
    # Handles user input like "2025-2026" or "anno scorso" → exact DB value
    print("\nBuilding index: insegnamento.academic_year")
    rows = con.execute(
        "SELECT DISTINCT academic_year FROM insegnamento WHERE academic_year IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "insegnamento__academic_year", chroma_client)

    # --- insegnamento.period ---
    # Handles user input like "primo semestre" or "semestre 1" → "S1"
    print("\nBuilding index: insegnamento.period")
    rows = con.execute(
        "SELECT DISTINCT period FROM insegnamento WHERE period IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "insegnamento__period", chroma_client)



# ------------ PERSONALE ---------------

    # --- personale.nome ---
    print("\nBuilding index: personale.nome")
    rows = con.execute(
        "SELECT DISTINCT nome FROM personale WHERE nome IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "personale__nome", chroma_client)



# ------------ Evento (lezione) ---------------

    # --- evento.department ---
    print("\nBuilding index: evento.department")
    rows = con.execute(
        "SELECT DISTINCT department FROM evento WHERE department IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__department", chroma_client)

    # --- evento.study_course ---
    print("\nBuilding index: evento.study_course")
    rows = con.execute(
        "SELECT DISTINCT study_course FROM evento WHERE study_course IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__study_course", chroma_client)

    # --- evento.subject_name ---
    print("\nBuilding index: evento.subject_name")
    rows = con.execute(
        "SELECT DISTINCT subject_name FROM evento WHERE subject_name IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__subject_name", chroma_client)


    # --- evento.curriculum ---
    print("\nBuilding index: evento.curriculum")
    rows = con.execute(
        "SELECT DISTINCT curriculum FROM evento WHERE curriculum IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__curriculum", chroma_client)

    # --- evento.date ---
    print("\nBuilding index: evento.date_iso")
    rows = con.execute(
        "SELECT DISTINCT date_iso FROM evento WHERE date_iso IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__date_iso", chroma_client)


    # --- evento.full_location ---
    print("\nBuilding index: evento.full_location")
    rows = con.execute(
        "SELECT DISTINCT full_location FROM evento WHERE full_location IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__full_location", chroma_client)


    # --- evento.professor ---
    print("\nBuilding index: evento.professor")
    rows = con.execute(
        "SELECT DISTINCT professor FROM evento WHERE professor IS NOT NULL"
    ).fetchall()
    build_column_index([r[0] for r in rows], "evento__professor", chroma_client)





    con.close()
    print(f"\nAll indexes persisted to: {chroma_dir.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ChromaDB column indexes")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite file path")
    parser.add_argument(
        "--chroma-dir", default=DEFAULT_CHROMA_DIR, help="ChromaDB persistence directory"
    )
    args = parser.parse_args()
    build_all_indexes(Path(args.db), Path(args.chroma_dir))