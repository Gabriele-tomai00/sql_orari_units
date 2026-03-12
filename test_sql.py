import sqlite3

con = sqlite3.connect("university.db")
con.row_factory = sqlite3.Row  # allows access by column name

for table in ["personale", "insegnamento", "lezione"]:
    row = con.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()
    if row:
        print(f"\n=== {table} ===")
        for key in row.keys():
            print(f"  {key}: {row[key]}")
    else:
        print(f"\n=== {table} === (empty)")





print("=== insegnamento.subject_code (first 20) ===")
rows = con.execute("SELECT DISTINCT subject_code FROM insegnamento LIMIT 20").fetchall()
for r in rows:
    print(" ", r[0])

print("\n=== lezione.subject_code (first 20) ===")
rows = con.execute("SELECT DISTINCT subject_code FROM lezione LIMIT 20").fetchall()
for r in rows:
    print(" ", r[0])

print("\n=== overlap check ===")
# Check if stripping 'EC' from lezione matches insegnamento
match = con.execute("""
    SELECT COUNT(*) FROM lezione e
    WHERE LTRIM(e.subject_code, 'EC') IN (
        SELECT subject_code FROM insegnamento
    )
""").fetchone()
total = con.execute("SELECT COUNT(*) FROM lezione").fetchone()
print(f"  Events with matching insegnamento (after stripping 'EC'): {match[0]} / {total[0]}")

con.close()


