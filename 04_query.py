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
     DB values (e.g. "ingegneria informatica" → "Ingegneria Elettronica e Informatica")
  3. Those values are injected as context hints for the LLM
  4. LLM generates the correct SQL query
  5. SQL is executed against the SQLite DB
  6. LLM synthesizes the final natural language answer

Usage:
    python 04_query.py
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

DEFAULT_DB = "2025-2026_data/university.db"
DEFAULT_CHROMA_DIR = "2025-2026_data/chroma_store"

# ---------------------------------------------------------------------------
# Global prompt — rules for SQL generation applied to all tables
# ---------------------------------------------------------------------------

TEXT_TO_SQL_PROMPT = PromptTemplate(
"Sei un assistente universitario per l'Università degli Studi di Trieste (UniTS).\n"
    "Dato lo schema del database e la domanda dell'utente, genera una query SQL SQLite corretta.\n"
    "\n"
    "Regole:\n"
    "- Usa solo i nomi di colonne presenti nello schema fornito."
    " - Quando la domanda non specifica un limite, restituisci al massimo 40 righe usando LIMIT."
    "- Usa sempre UPPER() o LIKE quando confronti colonne di testo\n"
    "- Le date nel database sono memorizzate in formato ISO YYYY-MM-DD, "
    "- Non filtrare mai per anno_accademico o periodo a meno che non venga esplicitamente richiesto\n"
    "- Se la domanda riguarda gli orari delle lezioni o le date delle lezioni, usa la tabella 'lezione'\n"
    "- Se la domanda riguarda un'aula, cerca nella tabella 'evento_aula' (questo è il calendario di occupazione delle aule) "
    "e se non trovi nulla prova a cercare nella tabella 'lezione'\n"
    "- Se la domanda riguarda chi insegna un corso o il codice Teams, usa la tabella 'insegnamento'\n"
    "- Se la domanda riguarda informazioni su una persona (email, ruolo, dipartimento), usa la tabella 'personale'\n"
    "- Non eseguire JOIN SQL perché le colonne spesso non sono normalizzate (eccetto il formato ISO per date e orari). "
    "Cerca invece in tabelle diverse, prima in una e poi in un'altra "
    "(ad esempio trova un evento in 'evento_aula' e se l'utente vuole più informazioni, cerca per data, ora e aula "
    "(nota che il nome potrebbe essere leggermente diverso) in 'lezione')\n"
    "- Per query su un'intera settimana o un intervallo di date, usa SELECT con solo le colonne essenziali: "
    "data, ora_inizio, ora_fine, nome_materia, nome_aula, nome_edificio, url. "
    "Non usare SELECT * per query che potrebbero restituire molte righe.\n"
    "- Non esistono relazioni tra le tabelle a causa di dati non normalizzati\n"
    "- Rispondi sempre in italiano nella fase di sintesi finale\n"
    "- Se la domanda fa riferimento a una data relativa (ad esempio oggi, ieri, domani...), "
    "nella risposta includi anche la data esplicita nel formato appropriato\n"
    "\n"
    "Schema disponibile:\n"
    "{schema}\n"
    "\n"
    "Domanda: {query_str}\n"
    "\n"
    "SQL (solo la query, senza commenti):"
)


class LoggingRetriever:
    """
    Wraps a LlamaIndex retriever and logs the nodes it returns,
    showing what DB values were matched for the user query.
    """

    def __init__(self, inner_retriever, column_label: str):
        self._inner = inner_retriever
        self._label = column_label

    def retrieve(self, query: str):
        nodes = self._inner.retrieve(query)
        if nodes:
            print(f"\n  [COLUMN RETRIEVER — {self._label}]")
            print(f"  Query : '{query}'")
            print(f"  Matches ({len(nodes)}):")
            for i, n in enumerate(nodes, 1):
                score = f"{n.score:.4f}" if n.score is not None else "n/a"
                print(f"    {i}. '{n.node.get_content()}' (score: {score})")
        else:
            print(f"\n  [COLUMN RETRIEVER — {self._label}] No matches for '{query}'")
        return nodes

    # Proxy all other attribute accesses to the inner retriever
    def __getattr__(self, name):
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# Load a persisted ChromaDB collection as a LlamaIndex retriever
# ---------------------------------------------------------------------------

def load_column_retriever(
    collection_name: str,
    chroma_client: chromadb.PersistentClient,
    top_k: int = 3,
    label: str = "",
) -> LoggingRetriever:
    try:
        chroma_collection = chroma_client.get_collection(collection_name)
    except Exception as exc:
        raise ValueError(
            f"Collection '{collection_name}' not found. Run 03_build_index.py first."
        ) from exc

    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    index = VectorStoreIndex.from_vector_store(vector_store)
    retriever = index.as_retriever(similarity_top_k=top_k)
    return LoggingRetriever(retriever, label or collection_name)



# ---------------------------------------------------------------------------
# Build the full query engine
# ---------------------------------------------------------------------------

def build_query_engine(db_path: Path, chroma_dir: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    sql_database = SQLDatabase(
        engine,
        include_tables=["personale", "insegnamento", "lezione", "evento_aula", "info_aula"],
    )

    # --- Table schema index (in-memory) ---
    # context_str is critical: the table retriever uses it to decide
    # which table to query. Words here must match the user's natural language.
    table_node_mapping = SQLTableNodeMapping(sql_database)
    table_schema_objs = [

SQLTableSchema(
            table_name="personale",
            context_str=(
                "Contains university staff and professors. "
                "Key columns: nome (full name), role, department, email, phone."
            ),
        ),
        SQLTableSchema(
            table_name="insegnamento",
            context_str=(
                "Contains university courses (subjects) and their academic details. "
                "Use for questions about: which professor teaches a course, "
                "Teams code of a course, degree program, academic year, semester period. "
                "Key columns: "
                "subject_name (course name), "
                "professors (professor names, can be more then one) and the relative main_professor_id"
                "degree_program_name and"
                "degree_program_name_eng (official degree program name in English), "
                "degree_program_code (e.g. 'IN22'), "
                "academic_year (e.g. '2025/2026' but take into accout the user can write for exemple 2025-2026), "
                "period (semester: 'S1' = first, 'S2' = second), "
                "teams_code (Microsoft Teams code), "
                "study_code (course code, e.g. '472MI-1')."
                "last_update of these information"
            ),
        ),
        SQLTableSchema(
            table_name="lezione",
            context_str=(
                "Contains scheduled lessons and academic calendar events. "
                "Use for questions about: lesson times, start hour, end hour, "
                "lesson date, classroom, building, which lessons happen on a specific day, "
                "whether a lesson is cancelled. "
                "Key columns: "
                "subject_name (course name), "
                "date (iso format), "
                "start_time (lesson start, e.g. '10:00'), "
                "end_time (lesson end, e.g. '13:00'), "
                "department, "
                "room_name (classroom like Aula 301) and  site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relatives ID of the room and site/building"
                "professors (professor names, may contain multiple separated by comma), "
                "degree_program_name (degree program name), "
                "degree_program_code (e.g. 'IN22'), "
                "cancelled ('yes' if cancelled, 'no' otherwise)."
            ),
        ),
        SQLTableSchema(
            table_name="evento_aula",
            context_str=(
                "Contains classroom booking events and room occupancy schedule. "
                "Use for questions about: which events or activities are scheduled in a room, "
                "room availability, event times, who is involved in a room event. "
                "Key columns: "

                "room_name (classroom like Aula 301) and  site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relatives ID of the room and site/building"

                "name_event (event or activity name), "
                "date (iso format), "
                "start_time (event start, e.g. '10:00'), "
                "end_time (event end, e.g. '13:00'), "
                "professors (professors or persons involved), "
            ),
        ),
        SQLTableSchema(
            table_name="info_aula",
            context_str=(
                "Contains static info about a classrom. "
                "Use for questions about: where is the room, how big it is"
                "is there wifi in that room. "
                "Key columns: "

                "room_name (classroom like Aula 301) and  site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relatives ID of the room and site/building"

                "floor of the building, "
                "room_type (if it's considered small, big, if it's aula magna...), "
                "capacity (how many students can stay there) "
                "accessible (it's not about disability. If no maybe there are work in progress, specify only it the value is no) "
                "maps_url (you can see on google maps the position of the building where the room is), "
                "equipment (if there is wifi, Proiettore, lavagne...), "
                "url (url with more info about the room)"
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
        "personale": {
            "nome_and_surname": load_column_retriever("personale__nome_and_surname", chroma_client, top_k=5),
            "role":             load_column_retriever("personale__role",             chroma_client, top_k=5),
            "department":       load_column_retriever("personale__department",       chroma_client, top_k=5),
        },
        "insegnamento": {
            "degree_program_name":     load_column_retriever("insegnamento__degree_program_name",     chroma_client, top_k=5),
            "degree_program_name_eng": load_column_retriever("insegnamento__degree_program_name_eng", chroma_client, top_k=5),
            "subject_name":            load_column_retriever("insegnamento__subject_name",            chroma_client, top_k=5),
            "professors":              load_column_retriever("insegnamento__professors",              chroma_client, top_k=5),
            "period":                  load_column_retriever("insegnamento__period",                  chroma_client, top_k=1),
        },
        "lezione": {
            "degree_program_name": load_column_retriever("lezione__degree_program_name", chroma_client, top_k=5),
            "subject_name":        load_column_retriever("lezione__subject_name",        chroma_client, top_k=5),
            "study_year_code":     load_column_retriever("lezione__study_year_code",     chroma_client, top_k=5),
            "curriculum":          load_column_retriever("lezione__curriculum",          chroma_client, top_k=5),
            "date":                load_column_retriever("lezione__date",                chroma_client, top_k=1),
            "department":          load_column_retriever("lezione__department",          chroma_client, top_k=5),
            "room_name":           load_column_retriever("lezione__room_name",           chroma_client, top_k=5),
            "site_name":           load_column_retriever("lezione__site_name",           chroma_client, top_k=5),
            "address":             load_column_retriever("lezione__address",             chroma_client, top_k=5),
            "professors":          load_column_retriever("lezione__professors",          chroma_client, top_k=5),
        },
        "evento_aula": {
            "site_name":  load_column_retriever("evento_aula__site_name",  chroma_client, top_k=5),
            "room_name":  load_column_retriever("evento_aula__room_name",  chroma_client, top_k=5),
            "name_event": load_column_retriever("evento_aula__name_event", chroma_client, top_k=5),
            "professors": load_column_retriever("evento_aula__professors", chroma_client, top_k=5),
        },
        "info_aula": {
            "site_name":    load_column_retriever("info_aula__site_name",  chroma_client, top_k=5),
            "room_name":    load_column_retriever("info_aula__room_name",  chroma_client, top_k=5),
            "address":      load_column_retriever("info_aula__address", chroma_client, top_k=5),
            "floor":        load_column_retriever("info_aula__floor", chroma_client, top_k=5),
            "room_type":    load_column_retriever("info_aula__room_type", chroma_client, top_k=5),

        },
    }

    query_engine = SQLTableRetrieverQueryEngine(
        sql_database,
        obj_index.as_retriever(similarity_top_k=5),
        cols_retrievers=cols_retrievers,
        llm=Settings.llm,                          # always use the globally configured LLM
        #text_to_sql_prompt=TEXT_TO_SQL_PROMPT,     # custom prompt with UniTS-specific rules
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
    parser.add_argument("--query", default=None, help="Single query (non-interactive)")
    args = parser.parse_args()
        
    qe = build_query_engine(Path(DEFAULT_DB), Path(DEFAULT_CHROMA_DIR))


    if args.query:
        response = qe.query(args.query)
        print(f"\nAnswer: {response}")
        if hasattr(response, "metadata") and response.metadata:
            sql = response.metadata.get("sql_query")
            if sql:
                print(f"[SQL] {sql}")
    else:
        interactive_loop(qe)