"""
04_query.py
-----------
Natural Language → SQL → Answer pipeline for the university database.

Uses:
  - LlamaIndex SQLTableRetrieverQueryEngine
  - ChromaDB (persistent) for column-level fuzzy value matching
  - Dynamic table routing: column retrievers are applied only to tables
    that are semantically relevant to the user query (two-stage retrieval)

Flow:
  1. User query arrives in natural language
  2. [Stage 1] Table router selects the 1-2 most relevant tables
  3. [Stage 2] Column retrievers run ONLY on those tables to find
     the best-matching DB values (e.g. "informatica" → "INGEGNERIA INFORMATICA")
  4. Those values are injected as context hints for the LLM
  5. LLM generates the correct SQL query
  6. SQL is executed against the SQLite DB
  7. LLM synthesizes the final natural language answer

Usage:
    python 04_query.py
    python 04_query.py --query "A che ora è la lezione di Robotics?"
"""

import argparse
from pathlib import Path



from index_utils import *

DEFAULT_DB = "2025-2026_data/university.db"
DEFAULT_CHROMA_DIR = "2025-2026_data/chroma_store"



# ---------------------------------------------------------------------------
# Interactive loop
# ---------------------------------------------------------------------------

def interactive_loop(query_engine: RoutedSQLQueryEngine) -> None:
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
    parser.add_argument("--query", default=None, help="Single query (non-interactive)")
    args = parser.parse_args()

    query_engine = build_query_engine(Path(DEFAULT_DB), Path(DEFAULT_CHROMA_DIR))

    if args.query:
        response = query_engine.query(args.query)
        print(f"\nAnswer: {response}")
        if hasattr(response, "metadata") and response.metadata:
            sql = response.metadata.get("sql_query")
            if sql:
                print(f"[SQL] {sql}")
    else:
        interactive_loop(query_engine)