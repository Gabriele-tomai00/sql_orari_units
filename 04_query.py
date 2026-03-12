"""
04_query.py
-----------
Natural Language → SQL → Answer pipeline for the university database.

Uses:
  - LlamaIndex SQLTableRetrieverQueryEngine
  - ChromaDB (persistent) for column-level fuzzy value matching

Flow:
  1. User query arrives in natural language
  2. Column retrievers scan Chroma indexes to find the best-matching
     DB values (e.g. "sw development" → "Software Development")
  3. Those values are injected as context hints for the LLM
  4. LLM generates the correct SQL query
  5. SQL is executed against the SQLite DB
  6. LLM synthesizes the final natural language answer

Usage:
    python 04_query.py
    python 04_query.py --db university.db --chroma-dir ./chroma_store
    python 04_query.py --query "A che ora è la lezione di Robotics?"
"""

import argparse
from pathlib import Path

import chromadb
from llama_index.core import Settings, SQLDatabase, StorageContext, VectorStoreIndex
from llama_index.core.indices.struct_store.sql_query import SQLTableRetrieverQueryEngine
from llama_index.core.objects import ObjectIndex, SQLTableNodeMapping, SQLTableSchema
from llama_index.core.prompts import PromptTemplate
from llama_index.vector_stores.chroma import ChromaVectorStore
from sqlalchemy import create_engine

from index_utils import *

DEFAULT_DB = "university.db"
DEFAULT_CHROMA_DIR = "./chroma_store"

# ---------------------------------------------------------------------------
# Global prompt — rules for SQL generation applied to all tables
# ---------------------------------------------------------------------------

TEXT_TO_SQL_PROMPT = PromptTemplate(
    "Sei un assistente universitario dell'Università di Trieste (UniTS).\n"
    "Dato lo schema del database e la domanda dell'utente, genera una query SQL SQLite corretta.\n"
    "\n"
    "Regole:\n"
    "- Usa sempre UPPER() o LIKE per confronti su colonne testuali\n"
    "- Per professors_raw e professor cerca sempre per cognome con LIKE '%COGNOME%'\n"
    "- Per le date usa il formato ISO YYYY-MM-DD (colonna date_iso)\n"
    "- Non filtrare mai per academic_year o period se non esplicitamente richiesto\n"
    "- Se la domanda riguarda orari, aule o date di lezioni usa la tabella 'evento'\n"
    "- Se la domanda riguarda chi insegna un corso o il codice Teams usa la tabella 'insegnamento'\n"
    "- Se la domanda riguarda informazioni su una persona (email, ruolo, dipartimento) usa 'personale'\n"
    "- Rispondi sempre in italiano nella fase di sintesi\n"
    "\n"
    "Schema disponibile:\n"
    "{schema}\n"
    "\n"
    "Domanda: {query_str}\n"
    "\n"
    "SQL (solo la query, nessun commento):"
)

# ---------------------------------------------------------------------------
# Load a persisted ChromaDB collection as a LlamaIndex retriever
# ---------------------------------------------------------------------------

def load_column_retriever(
    collection_name: str,
    chroma_client: chromadb.PersistentClient,
    similarity_top_k: int = 3,
):
    """
    Loads an existing Chroma collection and returns a retriever.
    Raises ValueError if the collection does not exist (run 03_build_index.py first).
    """
    try:
        chroma_collection = chroma_client.get_collection(collection_name)
    except Exception as exc:
        raise ValueError(
            f"Collection '{collection_name}' not found in ChromaDB. "
            "Run 03_build_index.py first."
        ) from exc

    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    index = VectorStoreIndex.from_vector_store(vector_store)
    return index.as_retriever(similarity_top_k=similarity_top_k)


# ---------------------------------------------------------------------------
# Build the full query engine
# ---------------------------------------------------------------------------

def build_query_engine(db_path: Path, chroma_dir: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    sql_database = SQLDatabase(
        engine,
        include_tables=["insegnamento", "personale", "evento"],
    )

    # --- Table schema index (in-memory) ---
    # context_str is critical: the table retriever uses it to decide
    # which table to query. Words here must match the user's natural language.
    table_node_mapping = SQLTableNodeMapping(sql_database)
    table_schema_objs = [
        SQLTableSchema(
            table_name="insegnamento",
            context_str=(
                "Contains university courses and their academic details. "
                "Use for questions about: which professor teaches a course, "
                "Teams code of a course, degree program, academic year, semester period. "
                "Key columns: "
                "course_name (course name, UPPERCASE), "
                "professors_raw (professor names, UPPERCASE, format 'SURNAME NAME (ID)', "
                "multiple professors separated by '#'), "
                "degree_program (degree program name, UPPERCASE), "
                "academic_year (e.g. '2025/2026'), "
                "period (semester: 'S1' = first, 'S2' = second), "
                "teams_code (Microsoft Teams code), "
                "course_code."
            ),
        ),
        SQLTableSchema(
            table_name="personale",
            context_str=(
                "Contains university staff and professors personal details. "
                "Use for questions about: a person's email, phone, role, department. "
                "Note: not all professors appear here — some only appear in "
                "insegnamento.professors_raw (e.g. contract lecturers). "
                "Key columns: nome (full name), role, department, email, phone."
            ),
        ),
        SQLTableSchema(
            table_name="evento",
            context_str=(
                "Contains scheduled lessons and academic calendar events (orario lezioni). "
                "Use for questions about: lesson times, start hour, end hour, "
                "lesson date, classroom, building, which lessons happen on a specific day, "
                "whether a lesson is cancelled. "
                "Key columns: "
                "subject_name (course name), "
                "date_iso (date in format YYYY-MM-DD), "
                "read_time (human readable date in Italian, e.g. '3 novembre 2025'), "
                "start_time (lesson start, e.g. '10:00'), "
                "end_time (lesson end, e.g. '13:00'), "
                "full_location (classroom and building, e.g. 'Aula 301 [Edificio Gorizia]'), "
                "professor (professor names, may contain multiple separated by comma), "
                "study_course (degree program name), "
                "department, "
                "cancelled ('yes' if cancelled, 'no' otherwise)."
            ),
        ),
    ]

    obj_index = ObjectIndex.from_objects(
        table_schema_objs,
        table_node_mapping,
        VectorStoreIndex,
    )

    # --- Column retrievers loaded from ChromaDB on disk ---
    chroma_client = chromadb.PersistentClient(path=str(chroma_dir))

    cols_retrievers = {
        "insegnamento": {
            "course_name": load_column_retriever(
                "insegnamento__course_name", chroma_client, similarity_top_k=3
            ),
            "professors_raw": load_column_retriever(
                "insegnamento__professors_raw", chroma_client, similarity_top_k=3
            ),
            "degree_program": load_column_retriever(
                "insegnamento__degree_program", chroma_client, similarity_top_k=2
            ),
            "academic_year": load_column_retriever(
                "insegnamento__academic_year", chroma_client, similarity_top_k=1
            ),
            "period": load_column_retriever(
                "insegnamento__period", chroma_client, similarity_top_k=1
            ),
        },
        "personale": {
            "nome": load_column_retriever(
                "personale__nome", chroma_client, similarity_top_k=3
            ),
        },
        "evento": {
            "subject_name": load_column_retriever(
                "evento__subject_name", chroma_client, similarity_top_k=3
            ),
            "professor": load_column_retriever(
                "evento__professor", chroma_client, similarity_top_k=3
            ),
            "full_location": load_column_retriever(
                "evento__full_location", chroma_client, similarity_top_k=2
            ),
            "study_course": load_column_retriever(
                "evento__study_course", chroma_client, similarity_top_k=2
            ),
            "department": load_column_retriever(
                "evento__department", chroma_client, similarity_top_k=2
            ),
            "date_iso": load_column_retriever(
                "evento__date_iso", chroma_client, similarity_top_k=1
            ),
        },
    }

    query_engine = SQLTableRetrieverQueryEngine(
        sql_database,
        obj_index.as_retriever(similarity_top_k=2),
        cols_retrievers=cols_retrievers,
        llm=Settings.llm,                          # always use the globally configured LLM
        text_to_sql_prompt=TEXT_TO_SQL_PROMPT,     # custom prompt with UniTS-specific rules
    )

    return query_engine


# ---------------------------------------------------------------------------
# Interactive loop
# ---------------------------------------------------------------------------

def interactive_loop(query_engine) -> None:
    print("\nUniversity Query System — type 'exit' to quit\n")
    while True:
        user_input = input("Query> ").strip()
        if user_input.lower() in ("exit", "quit", "q"):
            break
        if not user_input:
            continue
        try:
            response = query_engine.query(user_input)
            print(f"\nAnswer: {response}\n")
            # Show the generated SQL for transparency
            if hasattr(response, "metadata") and response.metadata:
                sql = response.metadata.get("sql_query")
                if sql:
                    print(f"[SQL] {sql}\n")
        except Exception as exc:
            print(f"[ERROR] {exc}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="University NL query engine")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite file path")
    parser.add_argument(
        "--chroma-dir", default=DEFAULT_CHROMA_DIR, help="ChromaDB persistence directory"
    )
    parser.add_argument("--query", default=None, help="Single query (non-interactive)")
    args = parser.parse_args()

    qe = build_query_engine(Path(args.db), Path(args.chroma_dir))

    if args.query:
        response = qe.query(args.query)
        print(f"\nAnswer: {response}")
        if hasattr(response, "metadata") and response.metadata:
            sql = response.metadata.get("sql_query")
            if sql:
                print(f"[SQL] {sql}")
    else:
        interactive_loop(qe)