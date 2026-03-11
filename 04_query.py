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
    python 04_query.py --query "Who teaches Machine Learning?"
"""

import argparse
import os
from pathlib import Path

from llama_index.core import Settings
import chromadb
from llama_index.core import SQLDatabase, VectorStoreIndex, StorageContext
from llama_index.core.indices.struct_store.sql_query import (
    SQLTableRetrieverQueryEngine,
)
from llama_index.core.objects import (
    SQLTableNodeMapping,
    ObjectIndex,
    SQLTableSchema,
)
from llama_index.vector_stores.chroma import ChromaVectorStore
from sqlalchemy import create_engine
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

DEFAULT_DB = "university.db"
DEFAULT_CHROMA_DIR = "./chroma_store"

from index_utils import *


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
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # Re-attach the index to the existing vector store (no re-embedding)
    index = VectorStoreIndex.from_vector_store(
        vector_store,
    )
    return index.as_retriever(similarity_top_k=similarity_top_k)


# ---------------------------------------------------------------------------
# Build the full query engine
# ---------------------------------------------------------------------------

def build_query_engine(db_path: Path, chroma_dir: Path):
    # SQLAlchemy engine pointing at our SQLite university DB
    engine = create_engine(f"sqlite:///{db_path}")
    sql_database = SQLDatabase(
        engine,
        include_tables=["insegnamento", "personale"],
    )

    # --- Table schema index (in-memory, small) ---
    # Provides context hints about table schemas to the LLM
    table_node_mapping = SQLTableNodeMapping(sql_database)
    table_schema_objs = [
        SQLTableSchema(
            table_name="insegnamento",
            context_str=(
                "Contains university courses. "
                "Key columns: course_name (name of the course), "
                "professors_raw (professor names, may contain multiple separated by '#' or other symbol and may be contained an ID), "
                "degree_program (bachelor/master program name), "
                "academic_year, period."
            ),
        ),
        SQLTableSchema(
            table_name="personale",
            context_str=(
                "Contains university staff/professors. "
                "Key columns: nome (full name), role, department, email, phone."
            ),
        ),
    ]

    obj_index = ObjectIndex.from_objects(
        table_schema_objs,
        table_node_mapping,
        VectorStoreIndex,
    )

    # --- Column retrievers (loaded from ChromaDB on disk) ---
    chroma_client = chromadb.PersistentClient(path=str(chroma_dir))

    cols_retrievers = {
        "insegnamento": {
            "course_name": load_column_retriever(
                "insegnamento__course_name", chroma_client, similarity_top_k=3
            ),
            "degree_program": load_column_retriever(
                "insegnamento__degree_program", chroma_client, similarity_top_k=2
            ),
        },
        "personale": {
            "nome": load_column_retriever(
                "personale__nome", chroma_client, similarity_top_k=3
            ),
        },
    }

    query_engine = SQLTableRetrieverQueryEngine(
        sql_database,
        obj_index.as_retriever(similarity_top_k=2),
        cols_retrievers=cols_retrievers,
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