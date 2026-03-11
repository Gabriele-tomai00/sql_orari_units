"""
03_test_sql.py
--------------
Manual SQL and FTS5 tests against university.db.
No ML or LlamaIndex involved — pure SQLite queries.

Usage:
    python 03_test_sql.py
    python 03_test_sql.py --db path/to/university.db
"""

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = "university.db"


def run(con: sqlite3.Connection, label: str, sql: str, params: tuple = ()) -> None:
    """Execute a query, print results with a label."""
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    print(f"{'='*60}")
    rows = con.execute(sql, params).fetchall()
    if not rows:
        print("  (no results)")
    for row in rows:
        print(" ", dict(row))


def run_tests(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # ------------------------------------------------------------------
    # Basic counts
    # ------------------------------------------------------------------
    run(con, "Row counts",
        """SELECT
             (SELECT COUNT(*) FROM personale)    AS personale_count,
             (SELECT COUNT(*) FROM insegnamenti) AS insegnamenti_count
        """)

    # ------------------------------------------------------------------
    # personale — basic lookups
    # ------------------------------------------------------------------
    run(con, "personale — find by exact name",
        "SELECT * FROM personale WHERE nome = 'Bedon Chiara'")

    run(con, "personale — find by partial name (LIKE)",
        "SELECT id, nome, role, email FROM personale WHERE nome LIKE '%Bedon%'")

    run(con, "personale — find by email domain",
        "SELECT id, nome, email FROM personale WHERE email LIKE '%@units.it' LIMIT 5")

    run(con, "personale — roles breakdown",
        "SELECT role, COUNT(*) AS cnt FROM personale GROUP BY role ORDER BY cnt DESC LIMIT 10")

    # ------------------------------------------------------------------
    # insegnamenti — basic lookups
    # ------------------------------------------------------------------
    run(con, "insegnamenti — find course by exact name",
        "SELECT * FROM insegnamenti WHERE course_name = 'ANALISI DELLE STRUTTURE'")

    run(con, "insegnamenti — find by professor (LIKE)",
        "SELECT course_name, professors_raw, teams_code FROM insegnamenti WHERE professors_raw LIKE '%BEDON%'")

    run(con, "insegnamenti — find by teams code",
        "SELECT course_name, professors_raw FROM insegnamenti WHERE teams_code = 'slw8irt'")

    run(con, "insegnamenti — courses with multiple professors (# separator)",
        "SELECT course_name, professors_raw FROM insegnamenti WHERE professors_raw LIKE '%#%' LIMIT 5")

    run(con, "insegnamenti — courses per degree program",
        "SELECT degree_program, COUNT(*) AS cnt FROM insegnamenti GROUP BY degree_program ORDER BY cnt DESC LIMIT 10")

    run(con, "insegnamenti — courses with no Teams code",
        "SELECT COUNT(*) AS cnt FROM insegnamenti WHERE teams_code = 'N/A' OR teams_code IS NULL")

    # ------------------------------------------------------------------
    # FTS5 — full-text search
    # ------------------------------------------------------------------
    run(con, "FTS — search 'BEDON' in insegnamenti",
        """SELECT i.course_name, i.professors_raw
           FROM insegnamenti_fts fts
           JOIN insegnamenti i ON i.id = fts.rowid
           WHERE insegnamenti_fts MATCH 'BEDON'""")

    run(con, "FTS — search 'VERCESI' in insegnamenti",
        """SELECT i.course_name, i.professors_raw
           FROM insegnamenti_fts fts
           JOIN insegnamenti i ON i.id = fts.rowid
           WHERE insegnamenti_fts MATCH 'VERCESI'""")

    run(con, "FTS — search professor name in personale",
        """SELECT p.nome, p.role, p.email
           FROM personale_fts fts
           JOIN personale p ON p.id = fts.rowid
           WHERE personale_fts MATCH 'Bedon'""")

    run(con, "FTS — search course keyword 'STRUTTURE'",
        """SELECT i.course_name, i.degree_program, i.professors_raw
           FROM insegnamenti_fts fts
           JOIN insegnamenti i ON i.id = fts.rowid
           WHERE insegnamenti_fts MATCH 'STRUTTURE'""")

    # ------------------------------------------------------------------
    # Cross-table manual join via LIKE
    # Shows the limitation that LlamaIndex (04/05) will solve semantically
    # ------------------------------------------------------------------
    run(con, "Cross-table — courses by BEDON CHIARA with her contact info (manual LIKE join)",
        """SELECT i.course_name, i.teams_code, p.email, p.phone
           FROM insegnamenti i
           LEFT JOIN personale p ON (
               instr(upper(p.nome), 'BEDON') > 0
               AND instr(upper(p.nome), 'CHIARA') > 0
           )
           WHERE i.professors_raw LIKE '%BEDON%'""")

    con.close()
    print(f"\n{'='*60}")
    print("All tests done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL tests against university.db")
    parser.add_argument("--db", default=DEFAULT_DB)
    args = parser.parse_args()

    run_tests(Path(args.db))