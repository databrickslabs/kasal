import sqlite3
import sys
from pathlib import Path

"""
Drops any unique index on prompttemplate.name in a SQLite DB.
Usage: python -m src.scripts.drop_prompttemplate_unique_sqlite /absolute/path/to/app.db
"""

if len(sys.argv) != 2:
    print("Usage: drop_prompttemplate_unique_sqlite.py /absolute/path/to/app.db")
    sys.exit(1)

db_path = Path(sys.argv[1])
if not db_path.exists():
    print(f"DB not found: {db_path}")
    sys.exit(2)

conn = sqlite3.connect(str(db_path))
cur = conn.cursor()

try:
    # List indexes on prompttemplate
    cur.execute("PRAGMA index_list('prompttemplate')")
    indexes = cur.fetchall()
    dropped = []
    for idx in indexes:
        # (seq, name, unique, origin, partial) in modern SQLite
        idx_name = idx[1]
        is_unique = bool(idx[2])
        if not is_unique:
            continue
        # Get index columns
        cur.execute(f"PRAGMA index_info('{idx_name}')")
        cols = cur.fetchall()
        col_names = [c[2] for c in cols]
        if len(col_names) == 1 and col_names[0] == 'name':
            try:
                cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
                dropped.append(idx_name)
            except Exception as e:
                print(f"Warning: failed to drop index {idx_name}: {e}")
    # Also drop a common name-based unique index if present
    try:
        cur.execute("DROP INDEX IF EXISTS ix_prompttemplate_name")
    except Exception:
        pass
    conn.commit()
    print("Dropped indexes:", ", ".join(dropped) or "none")
finally:
    conn.close()

