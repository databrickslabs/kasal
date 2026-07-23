import sqlite3
import sys
from pathlib import Path

"""
Rebuilds the 'prompttemplate' table in a SQLite DB to remove any implicit UNIQUE(name)
constraint, preserving data. Also recreates a non-unique index on group_id.

Usage: python src/backend/src/scripts/fix_prompttemplate_sqlite_rebuild.py /absolute/path/to/app.db
"""

DDL_CREATE = """
CREATE TABLE prompttemplate_new (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  description VARCHAR NULL,
  template TEXT NOT NULL,
  is_active BOOLEAN DEFAULT 1,
  group_id VARCHAR(100) NULL,
  created_by_email VARCHAR(255) NULL,
  created_at DATETIME,
  updated_at DATETIME
);
"""

DDL_CREATE_IDX = """
CREATE INDEX IF NOT EXISTS ix_prompttemplate_group_id ON prompttemplate(group_id);
"""

COPY_SQL = """
INSERT INTO prompttemplate_new (id, name, description, template, is_active, group_id, created_by_email, created_at, updated_at)
SELECT id, name, description, template, is_active, group_id, created_by_email, created_at, updated_at
FROM prompttemplate;
"""

if len(sys.argv) != 2:
    print("Usage: fix_prompttemplate_sqlite_rebuild.py /absolute/path/to/app.db")
    sys.exit(1)

db_path = Path(sys.argv[1])
if not db_path.exists():
    print(f"DB not found: {db_path}")
    sys.exit(2)

conn = sqlite3.connect(str(db_path))
cur = conn.cursor()

try:
    cur.execute("PRAGMA foreign_keys=OFF;")
    cur.execute("BEGIN TRANSACTION;")

    # Create new table without unique(name)
    cur.execute(DDL_CREATE)

    # Copy data over
    cur.execute(COPY_SQL)

    # Drop old table and rename
    cur.execute("DROP TABLE prompttemplate;")
    cur.execute("ALTER TABLE prompttemplate_new RENAME TO prompttemplate;")

    # Recreate index on group_id (non-unique)
    cur.execute(DDL_CREATE_IDX)

    cur.execute("COMMIT;")
    print("Rebuilt prompttemplate table without UNIQUE(name).")
finally:
    cur.execute("PRAGMA foreign_keys=ON;")
    conn.close()

