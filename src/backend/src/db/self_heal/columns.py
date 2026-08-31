"""Column self-heal steps: ADD COLUMN on tables that predate a mapped column.

``create_all`` never ALTERs an existing table and Alembic does not run at
startup, so every column added to an existing table needs an entry here — a
migration file alone changes nothing at runtime. Each step is a table and its
late-arriving columns; ``ensure_columns`` does the work.
"""

import logging

from src.db.self_heal.dialect import _conn_is_sqlite, _pg_columns
from src.db.self_heal.tables import ensure_table
from src.db.self_heal.vectors import _ensure_pgvector_embedding_columns

logger = logging.getLogger(__name__)

#: (column, DDL after the name on SQLite, DDL after the name on PostgreSQL).
#: The DDL is the type plus any DEFAULT — nullable and default-free unless it
#: says otherwise, so every ALTER is safe on a populated table.
ColumnSpec = tuple[str, str, str]


async def ensure_columns(conn, table: str, columns: list[ColumnSpec]) -> None:
    """Add whichever of ``columns`` ``table`` is missing. A no-op on a table that
    does not exist yet — ``create_all`` or the table step owns creation.

    Reads the catalogue FIRST on both dialects. SQLite has no ``ADD COLUMN IF
    NOT EXISTS``; and on PostgreSQL an ALTER on a table this role does not own
    raises 42501 "must be owner" even when the column is already there —
    Postgres checks ownership before existence — and that error aborted the
    whole self-heal transaction on the deployed app, leaving
    ``agents.thinking_budget_tokens`` missing. Only the genuinely missing
    columns get an ALTER, so the common case never touches the failure path.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql(f"PRAGMA table_info({table})")
            existing = {row[1] for row in res.fetchall()}
        else:
            existing = await _pg_columns(conn, table)
        if not existing:
            return  # table not created yet
        for name, sqlite_ddl, pg_ddl in columns:
            if name in existing:
                continue
            if is_sqlite:
                await conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN {name} {sqlite_ddl}"
                )
            else:
                await conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {pg_ddl}"
                )
            logger.info(f"Added {table}.{name} column")
        logger.info(f"Ensured {table} columns")
    except Exception as e:
        logger.warning(f"Could not ensure {table} columns: {e}")


async def _ensure_documentation_embeddings_columns(conn) -> None:
    """documentation_embeddings.group_id / file_path — knowledge-ingest fallback,
    built-in doc seeding and group-scoped search all reference them. Then the
    knowledge_embeddings table (uploaded knowledge when no Lakebase backend is
    active), its per-user ``created_by``, and the pgvector columns on both."""
    await ensure_columns(
        conn,
        "documentation_embeddings",
        [
            ("group_id", "VARCHAR(100)", "VARCHAR(100)"),
            ("file_path", "VARCHAR", "VARCHAR"),
        ],
    )
    await ensure_table(conn, "src.models.documentation_embedding", "KnowledgeEmbedding")
    await ensure_columns(
        conn, "knowledge_embeddings", [("created_by", "VARCHAR(255)", "VARCHAR(255)")]
    )
    await _ensure_pgvector_embedding_columns(conn)


async def _ensure_chat_sessions_columns(conn) -> None:
    """chat_sessions: the refresh-reconnect marker, the per-session preview that
    moved off browser IndexedDB onto the server, and the rolling context
    summary. Without them saving a preview or the running-job marker fails."""
    await ensure_columns(
        conn,
        "chat_sessions",
        [
            ("running_job_id", "VARCHAR", "VARCHAR"),
            ("preview_type", "VARCHAR(50)", "VARCHAR(50)"),
            ("preview_data", "TEXT", "TEXT"),
            ("preview_title", "VARCHAR(512)", "VARCHAR(512)"),
            ("context_summary", "TEXT", "TEXT"),
            ("context_summary_upto", "TIMESTAMP", "TIMESTAMP"),
        ],
    )


async def _ensure_agent_columns(conn) -> None:
    """agents: skills, and the per-agent thinking / output-token overrides (NULL
    inherits the model row). A database missing one accepts the field from the
    UI and silently drops it on save — "my selection did not persist"."""
    await ensure_columns(
        conn,
        "agents",
        [
            ("skills", "TEXT", "JSONB"),
            ("thinking_budget_tokens", "INTEGER", "INTEGER"),
            ("reasoning_effort", "TEXT", "VARCHAR"),
            ("max_tokens", "INTEGER", "INTEGER"),
        ],
    )


async def _ensure_execution_history_columns(conn) -> None:
    """executionhistory: the run a resume came from, the saved crew a run was
    built from, and which agent runtime ran it (NULL reads as "kasal"). Because
    SQLAlchemy selects every mapped column, a missing one breaks EVERY read of
    the table, not just the path that wanted the field."""
    await ensure_columns(
        conn,
        "executionhistory",
        [
            ("resumed_from_execution_id", "INTEGER", "INTEGER"),
            ("crew_id", "TEXT", "UUID"),
            ("harness", "TEXT", "VARCHAR(20)"),
        ],
    )


async def _ensure_crew_columns(conn) -> None:
    """crews.reasoning_config — the saved reasoning budget. The removed planner
    columns (crews.planning / planning_llm, schedule.planning) are deliberately
    NOT dropped here: an unmapped nullable column is harmless, and DROP COLUMN
    on every startup is riskier than leaving it behind."""
    await ensure_columns(conn, "crews", [("reasoning_config", "TEXT", "JSONB")])


async def _ensure_ui_config_columns(conn) -> None:
    """ui_config: the table itself first — ``init_db`` skips ``create_all`` once
    any table exists, so installs older than ui_config never got it and the UI
    Configurator 500'd with "no such table" — then the Predefined-UI columns
    (catalog + branding, and the per-component toggles)."""
    await ensure_table(conn, "src.models.ui_config", "UIConfig")
    await ensure_columns(
        conn,
        "ui_config",
        [
            (c, "TEXT", "TEXT")
            for c in ("catalog_json", "style_json", "disabled_components")
        ],
    )


async def _ensure_modelconfig_columns(conn) -> None:
    """modelconfig: per-model params, and the Anthropic thinking knobs (which of
    the two applies is decided by transport.thinking_mode()). A missing column
    here fails the first SELECT of the model catalogue — which is every LLM
    call. JSON rather than TEXT: SQLAlchemy's JSON type reads a TEXT column fine
    on SQLite, and on PostgreSQL the native type is what the ORM decodes."""
    await ensure_columns(
        conn,
        "modelconfig",
        [
            ("params", "JSON", "JSON"),
            ("unsupported_params", "JSON", "JSON"),
            ("thinking_budget_tokens", "INTEGER", "INTEGER"),
            ("reasoning_effort", "VARCHAR", "VARCHAR"),
        ],
    )


async def _ensure_databricks_config_columns(conn) -> None:
    """databricksconfig.ai_gateway_enabled — defaults to false (serving-endpoint
    routing) so an existing install keeps its behaviour."""
    await ensure_columns(
        conn,
        "databricksconfig",
        [("ai_gateway_enabled", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT false")],
    )
