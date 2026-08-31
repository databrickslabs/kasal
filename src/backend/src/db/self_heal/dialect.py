"""Dialect helpers shared by every self-heal step.

Ask the CONNECTION which dialect it is, never ``settings.DATABASE_URI``: the
self-heal runs against the local engine in ``init_db`` and again against the
freshly activated Lakebase engine from the ``main.py`` lifespan, and on that
second pass the configured URI still says sqlite.
"""

import logging

from src.config.settings import settings

logger = logging.getLogger(__name__)


def _conn_is_sqlite(conn) -> bool:
    """Whether THIS connection is SQLite — asked of the connection, not the env.

    Every ``_ensure_*`` helper branches on dialect, and reading
    ``settings.DATABASE_URI`` to decide is wrong on the path that matters most.
    ``run_schema_self_heal`` is called TWICE: once in ``init_db`` against the
    local engine, and again from the ``main.py`` lifespan against the freshly
    activated LAKEBASE engine. On that second call ``DATABASE_URI`` still says
    ``sqlite`` — it describes the configured default, not the connection in hand —
    so the helpers took the SQLite branch and issued SQLite-flavoured DDL at
    PostgreSQL.

    The failure was invisible because each helper swallows its exception with a
    warning. Columns added BEFORE a Lakebase was provisioned were unaffected
    (``create_all`` had made the whole table), so this stayed latent until the
    first column added AFTER one existed: `modelconfig.thinking_budget_tokens`
    never landed, and every read of the model catalogue — which is every LLM call
    — 500'd with "column modelconfig.thinking_budget_tokens does not exist".

    Asking the connection removes the coupling entirely.
    """
    try:
        return conn.engine.dialect.name == "sqlite"
    except Exception:  # noqa: BLE001 — fall back to the configured default
        return str(settings.DATABASE_URI).startswith("sqlite")


async def _pg_columns(conn, table: str) -> set[str]:
    """Column names of ``table`` on PostgreSQL; empty set if it does not exist.

    The Postgres counterpart of ``PRAGMA table_info``. Reading the catalogue
    before altering matters for more than efficiency: Postgres checks OWNERSHIP
    before it checks existence, so ``ADD COLUMN IF NOT EXISTS`` raises 42501
    "must be owner" on a table this role does not own EVEN WHEN the column is
    already there and the statement would do nothing.

    Unqualified so it follows ``search_path``, matching the DDL that uses it.
    """
    res = await conn.exec_driver_sql(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' "
        "AND table_schema = ANY (current_schemas(false))"
    )
    return {row[0] for row in res.fetchall()}
