from llama_index.core import Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.llms.openai_like import OpenAILike
from llama_index.core.prompts import PromptTemplate
from sqlalchemy import create_engine
import chromadb
from llama_index.core import Settings, SQLDatabase, StorageContext, VectorStoreIndex
from llama_index.core.indices.struct_store.sql_query import SQLTableRetrieverQueryEngine
from llama_index.core.objects import ObjectIndex, SQLTableNodeMapping, SQLTableSchema
from llama_index.core.schema import TextNode
from llama_index.vector_stores.chroma import ChromaVectorStore
from pathlib import Path

def get_prompt_from_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

Settings.embed_model = HuggingFaceEmbedding(model_name="intfloat/multilingual-e5-small")

Settings.llm = OpenAILike(
    model="ggml-org/gpt-oss-120b-GGUF",
    api_base="http://172.30.42.129:8080/v1",
    api_key="not_necessary",
    context_window=8192,
    max_tokens=1024,
    temperature=0.1,
    is_chat_model=True,
    system_prompt=get_prompt_from_file("prompt_for_llm.txt"),
    timeout=30,
)



# ---------------------------------------------------------------------------
# Logging wrapper for column retrievers
# ---------------------------------------------------------------------------

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
# Table router — selects relevant tables for a given query
# ---------------------------------------------------------------------------

def build_table_router(embed_model) -> VectorStoreIndex:
    """
    Builds an in-memory VectorStoreIndex over TABLE_DOMAINS descriptions.
    Used at query time to select which tables are relevant.
    Each node carries the table name in its metadata.
    """
    nodes = [
        TextNode(text=description, metadata={"table": table_name})
        for table_name, description in TABLE_DOMAINS.items()
    ]
    return VectorStoreIndex(nodes, embed_model=embed_model)


def route_tables(query: str, table_router_index: VectorStoreIndex) -> list[str]:
    """
    Returns the list of table names most relevant to the query.
    Prints routing decisions for transparency.
    """
    retriever = table_router_index.as_retriever(similarity_top_k=TABLE_ROUTER_TOP_K)
    matched = retriever.retrieve(query)
    selected = [n.metadata["table"] for n in matched]

    print(f"\n  [TABLE ROUTER] Query: '{query}'")
    for n in matched:
        score = f"{n.score:.4f}" if n.score is not None else "n/a"
        print(f"    → {n.metadata['table']} (score: {score})")

    return selected



# ---------------------------------------------------------------------------
# Global prompt — rules for SQL generation applied to all tables
# ---------------------------------------------------------------------------

TEXT_TO_SQL_PROMPT = PromptTemplate(
    "Sei un assistente universitario per l'Università degli Studi di Trieste (UniTS).\n"
    "Dato lo schema del database e la domanda dell'utente, genera una query SQL SQLite corretta.\n"
    "\n"
    "Regole:\n"
    "- Usa SEMPRE i valori esatti suggeriti dal contesto (colonne hint) per costruire "
    "le condizioni WHERE, non riformulare il testo originale dell'utente. "
    "Se il contesto suggerisce 'TREVISAN MARTINO', usa LIKE '%TREVISAN MARTINO%', "
    "anche se l'utente ha scritto 'Martino Trevisan'.\n"
    "- Usa solo i nomi di colonne presenti nello schema fornito.\n"
    "- Quando la domanda non specifica un limite, restituisci al massimo 40 righe usando LIMIT.\n"
    "- Usa sempre UPPER() o LIKE quando confronti colonne di testo.\n"
    "- Le date nel database sono memorizzate in formato ISO YYYY-MM-DD.\n"
    "- Non filtrare mai per anno_accademico o periodo a meno che non venga esplicitamente richiesto.\n"
    "- Se la domanda riguarda gli orari delle lezioni o le date delle lezioni, usa la tabella 'lezione'.\n"
    "- Se la domanda riguarda un'aula, cerca nella tabella 'evento_aula' (questo è il calendario di "
    "occupazione delle aule) e se non trovi nulla prova a cercare nella tabella 'lezione'.\n"
    "- Se la domanda riguarda chi insegna un corso o il codice Teams, usa la tabella 'insegnamento'.\n"
    "- Se la domanda riguarda informazioni su una persona (email, ruolo, dipartimento), usa la tabella 'personale'.\n"
    "- Non eseguire JOIN SQL perché le colonne spesso non sono normalizzate.\n"
    "- Per query su un'intera settimana o un intervallo di date, usa SELECT con solo le colonne "
    "essenziali: data, ora_inizio, ora_fine, nome_materia, nome_aula, nome_edificio, url.\n"
    "- Non esistono relazioni tra le tabelle a causa di dati non normalizzati.\n"
    "- Rispondi sempre in italiano nella fase di sintesi finale.\n"
    "- Se la domanda fa riferimento a una data relativa (oggi, ieri, domani...), "
    "nella risposta includi anche la data esplicita nel formato appropriato.\n"

    "- Quando si cerca per nome professore e il column retriever suggerisce più nomi distinti "
    "(es. 'DE LORENZO ANDREA' e 'DE LORENZO GIUDITTA'), usa LIKE separati per ogni nome "
    "con OR, mai una LIKE generica sul cognome solo. "
    "Includi sempre la colonna professors nel SELECT per permettere di distinguere "
    "i risultati nella risposta finale.\n"
    "- Nella risposta finale, se i risultati contengono insegnamenti di professori diversi, "
    "raggruppa esplicitamente per professore invece di fare una lista unica.\n"
    "\n"
    "Schema disponibile:\n"
    "{schema}\n"
    "\n"
    "Domanda: {query_str}\n"
    "\n"
    "SQL (solo la query, senza commenti):"
)

# ---------------------------------------------------------------------------
# Routed query engine — wraps SQLTableRetrieverQueryEngine with dynamic routing
# ---------------------------------------------------------------------------

class RoutedSQLQueryEngine:
    """
    At each query:
      1. Routes the query to the 1-2 most relevant tables (table router)
      2. Passes only those tables' column retrievers to a fresh
         SQLTableRetrieverQueryEngine instance
      3. Delegates query execution to that engine

    Re-creating SQLTableRetrieverQueryEngine per query is cheap —
    it is a plain Python object with no re-embedding cost.
    """

    def __init__(self, sql_database, obj_index, all_cols_retrievers, table_router_index):
        self._sql_database = sql_database
        self._obj_index = obj_index
        self._all_cols_retrievers = all_cols_retrievers
        self._table_router_index = table_router_index

    def query(self, query_str: str):
        # Stage 1: select relevant tables
        relevant_tables = route_tables(query_str, self._table_router_index)

        # Stage 2: filter cols_retrievers to selected tables only
        routed_cols = {
            table: (
                self._all_cols_retrievers[table] if table in relevant_tables else {}
            )
            for table in self._all_cols_retrievers
        }

        # Build a fresh engine with only the relevant column retrievers
        engine = SQLTableRetrieverQueryEngine(
            self._sql_database,
            self._obj_index.as_retriever(similarity_top_k=5),
            cols_retrievers=routed_cols,
            llm=Settings.llm,
            text_to_sql_prompt=TEXT_TO_SQL_PROMPT,
        )
        return engine.query(query_str)



# ---------------------------------------------------------------------------
# Table router — semantic description used to pick relevant tables per query
# ---------------------------------------------------------------------------

# Each entry describes the domain of the table in natural language.
# The table router embeds these descriptions and finds the closest ones
# to the user query, so only those tables' column retrievers are used.
TABLE_DOMAINS = {
    "personale": (
        "staff person professor docente employee contact "
        "personale nome email phone telefono ruolo dipartimento"
    ),
    "insegnamento": (
        "course insegnamento subject materia degree laurea professor docente "
        "teams code codice periodo semestre academic-year anno accademico"
    ),
    "lezione": (
        "lesson lecture lezione schedule orario timetable "
        "quando date data ora start end aula edificio cancelled annullata "
        "subject materia professor docente degree"
    ),
    "evento_aula": (
        "event evento booking prenotazione aula room occupancy occupazione "
        "calendar calendario edificio building schedule orario"
    ),
    "info_aula": (
        "classroom aula room info details dettagli building edificio "
        "floor piano capacity capienza equipment attrezzature wifi "
        "proiettore accessible accessibile maps mappa indirizzo address"
    ),
}

# How many tables to select per query (increase to 3 for cross-table queries)
TABLE_ROUTER_TOP_K = 2



# ---------------------------------------------------------------------------
# Build the full query engine
# ---------------------------------------------------------------------------

def build_query_engine(db_path: Path, chroma_dir: Path) -> RoutedSQLQueryEngine:
    engine = create_engine(f"sqlite:///{db_path}")
    sql_database = SQLDatabase(
        engine,
        include_tables=["personale", "insegnamento", "lezione", "evento_aula", "info_aula"],
    )

    # --- Table schema index (used by SQLTableRetrieverQueryEngine internally) ---
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
                "professors (professor names, can be more then one) and the relative main_professor_id, "
                "degree_program_name and "
                "degree_program_name_eng (official degree program name in English), "
                "degree_program_code (e.g. 'IN22'), "
                "academic_year (e.g. '2025/2026' but take into account the user can write for example 2025-2026), "
                "period (semester: 'S1' = first, 'S2' = second), "
                "teams_code (Microsoft Teams code), "
                "study_code (course code, e.g. '472MI-1'). "
                "last_update of these information."
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
                "room_name (classroom like Aula 301) and site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relative IDs of the room and site/building, "
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
                "room_name (classroom like Aula 301) and site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relative IDs of the room and site/building, "
                "name_event (event or activity name), "
                "date (iso format), "
                "start_time (event start, e.g. '10:00'), "
                "end_time (event end, e.g. '13:00'), "
                "professors (professors or persons involved)."
            ),
        ),
        SQLTableSchema(
            table_name="info_aula",
            context_str=(
                "Contains static info about a classroom. "
                "Use for questions about: where is the room, how big it is, "
                "is there wifi in that room. "
                "Key columns: "
                "room_name (classroom like Aula 301) and site_name (building like Edificio Gorizia), "
                "room_code and site_code are the relative IDs of the room and site/building, "
                "floor of the building, "
                "room_type (if it's considered small, big, if it's aula magna...), "
                "capacity (how many students can stay there), "
                "accessible (it's not about disability. If no maybe there are works in progress, specify only if the value is no), "
                "maps_url (you can see on google maps the position of the building where the room is), "
                "equipment (if there is wifi, Proiettore, lavagne...), "
                "url (url with more info about the room)."
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

    all_cols_retrievers = {
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
            "site_name": load_column_retriever("info_aula__site_name", chroma_client, top_k=5),
            "room_name": load_column_retriever("info_aula__room_name", chroma_client, top_k=5),
            "address":   load_column_retriever("info_aula__address",   chroma_client, top_k=5),
            "floor":     load_column_retriever("info_aula__floor",     chroma_client, top_k=5),
            "room_type": load_column_retriever("info_aula__room_type", chroma_client, top_k=5),
        },
    }

    # --- Table router (in-memory, built from TABLE_DOMAINS descriptions) ---
    table_router_index = build_table_router(embed_model=Settings.embed_model)

    return RoutedSQLQueryEngine(
        sql_database=sql_database,
        obj_index=obj_index,
        all_cols_retrievers=all_cols_retrievers,
        table_router_index=table_router_index,
    )