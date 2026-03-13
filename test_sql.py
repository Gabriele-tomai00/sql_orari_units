"""
print_info_aula.py
------------------
Prints the first N rows of the info_aula table from the university SQLite database.

Usage:
    python print_info_aula.py
    python print_info_aula.py --db path/to/university.db --n 10
"""

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = "2025-2026_data/university.db"
DEFAULT_N = 5


def print_info_aula(db_path: Path, n: int) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row  # allows column access by name

    cursor = con.execute(f"SELECT * FROM info_aula LIMIT ?", (n,))
    rows = cursor.fetchall()

    if not rows:
        print("No rows found in info_aula.")
        return

    # Print column headers
    columns = rows[0].keys()
    col_width = 20
    header = " | ".join(col.ljust(col_width) for col in columns)
    separator = "-" * len(header)

    print(f"\nFirst {n} rows of 'info_aula':\n")
    print(header)
    print(separator)

    for row in rows:
        print(" | ".join(str(row[col] if row[col] is not None else "NULL").ljust(col_width) for col in columns))

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print first N rows of info_aula table")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite file path")
    parser.add_argument("--n", type=int, default=DEFAULT_N, help="Number of rows to print")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found at {db_path.resolve()}")
        exit(1)

    print_info_aula(db_path, args.n)