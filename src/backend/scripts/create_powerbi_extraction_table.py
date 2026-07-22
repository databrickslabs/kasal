"""
One-time, NON-DESTRUCTIVE creation of the ``powerbi_extraction`` table.

For deployments connected to a PRE-EXISTING Lakebase (or any Postgres) that was
provisioned before this table existed. It runs ``CREATE TABLE IF NOT EXISTS`` +
``CREATE INDEX IF NOT EXISTS`` only — it NEVER drops, alters, or touches any
other table or row. Safe to run repeatedly (idempotent).

DO NOT use the Configuration → Lakebase → "Schema Only" / "Migrate" UI buttons for
this: they pass recreate_schema=true, which runs DROP SCHEMA kasal CASCADE and
wipes all existing data. This script is the safe alternative.

Usage (from src/backend, with the app's env / venv):

    # Uses the app's configured DATABASE_URI (respects Lakebase when enabled):
    .venv/bin/python scripts/create_powerbi_extraction_table.py

    # Or target an explicit Postgres/Lakebase URL:
    .venv/bin/python scripts/create_powerbi_extraction_table.py \
        --url "postgresql+asyncpg://user:token@host:5432/databricks_postgres"

    # Preview the DDL without executing:
    .venv/bin/python scripts/create_powerbi_extraction_table.py --dry-run
"""
import argparse
import asyncio
import sys

# The table this script ensures. Kept in sync with
# src/models/powerbi_extraction.py — regenerate the DDL with:
#   from sqlalchemy.schema import CreateTable; from sqlalchemy.dialects import postgresql
#   print(CreateTable(PowerBIExtraction.__table__).compile(dialect=postgresql.dialect()))
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS powerbi_extraction (
    id SERIAL NOT NULL,
    execution_id VARCHAR(100),
    workspace_id VARCHAR(100),
    dataset_id VARCHAR(100),
    report_id VARCHAR(100),
    relationships JSON,
    measures JSON,
    admin_tables JSON,
    report_definition JSON,
    proposed_config JSON,
    warnings JSON,
    relationships_count INTEGER,
    measures_count INTEGER,
    measures_with_dax_count INTEGER,
    admin_tables_count INTEGER,
    summary TEXT,
    group_id VARCHAR(100),
    created_by_email VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
)
"""

CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_execution_id ON powerbi_extraction (execution_id)",
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_workspace_id ON powerbi_extraction (workspace_id)",
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_dataset_id ON powerbi_extraction (dataset_id)",
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_group_id ON powerbi_extraction (group_id)",
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_group_created ON powerbi_extraction (group_id, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_powerbi_extraction_workspace_dataset ON powerbi_extraction (workspace_id, dataset_id)",
]


async def _run(url: str, schema: str) -> None:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            # Scope to the app schema when one is used (Lakebase uses 'kasal').
            if schema:
                await conn.execute(text(f"SET search_path TO {schema}"))
            existed = await conn.execute(
                text("SELECT to_regclass(:t)"),
                {"t": (f"{schema}.powerbi_extraction" if schema else "powerbi_extraction")},
            )
            already = existed.scalar() is not None
            await conn.execute(text(CREATE_TABLE_SQL))
            for stmt in CREATE_INDEX_SQL:
                await conn.execute(text(stmt))
        if already:
            print("✓ powerbi_extraction already existed — no change (indexes ensured).")
        else:
            print("✓ Created powerbi_extraction table + indexes (no existing data touched).")
    finally:
        await engine.dispose()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default=None,
                    help="DB URL (async driver). Defaults to the app's settings.DATABASE_URI.")
    ap.add_argument("--schema", default="kasal",
                    help="Schema to create the table in (default: kasal; use '' for the default schema).")
    ap.add_argument("--dry-run", action="store_true", help="Print the DDL and exit.")
    args = ap.parse_args()

    if args.dry_run:
        print(CREATE_TABLE_SQL.strip())
        for s in CREATE_INDEX_SQL:
            print(s + ";")
        return 0

    url = args.url
    if not url:
        from src.config.settings import settings
        url = str(settings.DATABASE_URI)

    if not url.startswith(("postgresql+asyncpg://", "postgresql+psycopg://", "sqlite+aiosqlite://")):
        print(f"Refusing to run against an unrecognized async URL: {url.split('://')[0]}://…",
              file=sys.stderr)
        return 2

    # SQLite has no schemas; ignore --schema there.
    schema = "" if url.startswith("sqlite") else args.schema
    asyncio.run(_run(url, schema))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
