import os
import sys
import sqlite3

# Ensure backend src is importable when running from repo
CURRENT_DIR = os.path.dirname(__file__)
# Determine backend dir by going up from scripts/migrations to src/backend
backend_dir = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
src_dir = os.path.join(backend_dir, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Try to use environment first; importing settings in this standalone script can be brittle
settings = None
try:
    from src.config.settings import settings as _settings  # type: ignore
    settings = _settings
except Exception:
    settings = None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    cols = [row[1] for row in cur.fetchall()]
    return column in cols


def index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
    return cur.fetchone() is not None


def resolve_db_path() -> str:
    # 1) ENV var
    env_path = os.environ.get('SQLITE_DB_PATH')
    if env_path:
        return env_path if os.path.isabs(env_path) else os.path.abspath(os.path.join(backend_dir, env_path))

    # 2) settings if import succeeded
    if settings is not None:
        sqlite_path = getattr(settings, 'SQLITE_DB_PATH', './app.db') or './app.db'
        return sqlite_path if os.path.isabs(sqlite_path) else os.path.abspath(os.path.join(backend_dir, sqlite_path))

    # 3) common defaults
    candidate_repo_root = os.path.abspath(os.path.join(backend_dir, '..', '..'))
    repo_default = os.path.join(candidate_repo_root, 'kasal.db')
    if os.path.exists(repo_default):
        return repo_default

    return os.path.join(backend_dir, 'app.db')


def main() -> int:
    db_path = resolve_db_path()

    if not os.path.exists(db_path):
        print(f"[ERROR] SQLite DB not found at: {db_path}")
        return 1

    print(f"[INFO] Using SQLite DB: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute('PRAGMA foreign_keys=off;')
        conn.execute('BEGIN;')

        # 1) Add group_id column if missing
        if not column_exists(conn, 'mcp_servers', 'group_id'):
            print('[INFO] Adding column mcp_servers.group_id ...')
            conn.execute("ALTER TABLE mcp_servers ADD COLUMN group_id TEXT;")
        else:
            print('[INFO] Column mcp_servers.group_id already exists - skipping')

        # 2) Create unique index on (name, group_id) to mirror model constraint (SQLite allows multiple NULLs)
        if not index_exists(conn, 'uq_mcpserver_name_group'):
            print('[INFO] Creating unique index uq_mcpserver_name_group on (name, group_id) ...')
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_mcpserver_name_group ON mcp_servers(name, group_id);")
        else:
            print('[INFO] Index uq_mcpserver_name_group already exists - skipping')

        conn.commit()
        print('[SUCCESS] Migration completed successfully.')
        return 0
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Migration failed: {e}")
        return 2
    finally:
        try:
            conn.execute('PRAGMA foreign_keys=on;')
        except Exception:
            pass
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())

