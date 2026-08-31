"""Column self-heal steps: ADD COLUMN on tables that predate a mapped column.

``create_all`` never ALTERs an existing table and Alembic does not run at
startup, so every column added to an existing table needs an idempotent step
here — a migration file alone changes nothing at runtime.

SQLite: ``PRAGMA table_info`` then plain ``ALTER TABLE … ADD COLUMN``.
PostgreSQL: read ``information_schema.columns`` first — Postgres checks
OWNERSHIP before existence, so an unconditional ``ADD COLUMN IF NOT EXISTS``
raises 42501 on a table this role does not own even when nothing would change —
then ALTER only what is missing.
"""

import logging

from src.db.self_heal.dialect import _conn_is_sqlite, _pg_columns
from src.db.self_heal.vectors import _ensure_pgvector_embedding_columns

logger = logging.getLogger(__name__)


async def _ensure_documentation_embeddings_columns(conn) -> None:
    """Idempotently add group_id/file_path to documentation_embeddings.

    create_all never ALTERs an existing table, so app DBs created before these
    columns existed are missing them — which breaks knowledge ingest fallback,
    built-in doc seeding, and group-scoped search (all reference the columns).
    Safe to run on every startup; the embedding column is unchanged here.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql(
                "PRAGMA table_info(documentation_embeddings)"
            )
            cols = {row[1] for row in res.fetchall()}
            if not cols:
                return  # table not created yet
            if "group_id" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE documentation_embeddings ADD COLUMN group_id VARCHAR(100)"
                )
            if "file_path" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE documentation_embeddings ADD COLUMN file_path VARCHAR"
                )
        else:
            # Ask before altering. `ADD COLUMN IF NOT EXISTS` is a no-op when the
            # column is there, but Postgres checks OWNERSHIP before it checks
            # existence — so on a table owned by a previous deploy's service
            # principal it raises 42501 "must be owner" for work that did not need
            # doing. That error is what aborted the shared transaction and took
            # the other 23 self-heal steps down with it, leaving
            # agents.thinking_budget_tokens missing. Skipping the ALTER when the
            # columns already exist keeps the common case off the failure path.
            existing = await _pg_columns(conn, "documentation_embeddings")
            if not existing:
                return  # table not created yet
            for name, ddl_type in (
                ("group_id", "VARCHAR(100)"),
                ("file_path", "VARCHAR"),
            ):
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE documentation_embeddings "
                        f"ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                    )
        logger.info("Ensured documentation_embeddings group_id/file_path columns")
    except Exception as e:
        logger.warning(f"Could not ensure documentation_embeddings columns: {e}")

    # Ensure the dedicated knowledge_embeddings table exists (uploaded-knowledge
    # fallback when no Lakebase backend is active). create_all is skipped on
    # existing DBs, so create it explicitly here.
    try:
        from src.models.documentation_embedding import KnowledgeEmbedding

        def _create_knowledge_table(sync_conn):
            KnowledgeEmbedding.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_knowledge_table)
        logger.info("Ensured knowledge_embeddings table exists")
    except Exception as e:
        logger.warning(f"Could not ensure knowledge_embeddings table: {e}")

    # Self-heal: knowledge_embeddings.created_by (per-user isolation of
    # uploaded knowledge). checkfirst-create above never ALTERs a table that
    # already exists, so pre-existing DBs need the column added here.
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(knowledge_embeddings)")
            cols = {row[1] for row in res.fetchall()}
            if cols and "created_by" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE knowledge_embeddings ADD COLUMN created_by VARCHAR(255)"
                )
                logger.info(
                    "Added knowledge_embeddings.created_by column (SQLite self-heal)"
                )
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE knowledge_embeddings ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)"
            )
        logger.info("Ensured knowledge_embeddings.created_by column")
    except Exception as e:
        logger.warning(f"Could not ensure knowledge_embeddings.created_by column: {e}")

    await _ensure_pgvector_embedding_columns(conn)


async def _ensure_chat_sessions_columns(conn) -> None:
    """Idempotently add the running_job_id + preview_* columns to chat_sessions.

    These back the refresh-reconnect marker and the per-session preview, which
    moved off browser IndexedDB onto the server. create_all never ALTERs an
    existing table, so DBs created before these columns existed (e.g. customer
    instances we can't migrate manually) are missing them — which would break
    saving/reading previews and the running-job marker. Safe to run every
    startup; all columns are nullable with no default.
    """
    is_sqlite = _conn_is_sqlite(conn)
    columns = [
        ("running_job_id", "VARCHAR"),
        ("preview_type", "VARCHAR(50)"),
        ("preview_data", "TEXT"),
        ("preview_title", "VARCHAR(512)"),
        ("context_summary", "TEXT"),
        ("context_summary_upto", "TIMESTAMP"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(chat_sessions)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (handled by _ensure_chat_sessions_table)
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE chat_sessions ADD COLUMN {name} {ddl_type}"
                    )
                    logger.info(f"Added chat_sessions.{name} column (SQLite self-heal)")
        else:
            for name, ddl_type in columns:
                await conn.exec_driver_sql(
                    f"ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                )
            logger.info("Ensured chat_sessions preview/running_job_id columns")
    except Exception as e:
        logger.warning(f"Could not ensure chat_sessions columns: {e}")


async def _ensure_agent_columns(conn) -> None:
    """Idempotently add the agents columns added after the table shipped.

    ``create_all`` never ALTERs an existing table, so a database created before a
    column existed would accept the field from the UI and silently drop it on
    save — the failure reads as "my selection did not persist", with nothing
    anywhere saying why. All nullable, safe every startup."""
    is_sqlite = _conn_is_sqlite(conn)
    # (name, sqlite type, postgres type)
    columns = [
        ("skills", "TEXT", "JSONB"),
        # Per-agent thinking / output-token overrides; NULL inherits the model row.
        ("thinking_budget_tokens", "INTEGER", "INTEGER"),
        ("reasoning_effort", "TEXT", "VARCHAR"),
        ("max_tokens", "INTEGER", "INTEGER"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(agents)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for name, sqlite_type, _pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE agents ADD COLUMN {name} {sqlite_type}"
                    )
                    logger.info(f"Added agents.{name} column (SQLite self-heal)")
        else:
            # Read the catalogue first — see _pg_columns: an ALTER on a table this
            # role does not own raises "must be owner" even when the column is
            # already present, and that error aborts the whole self-heal
            # transaction.
            existing = await _pg_columns(conn, "agents")
            if not existing:
                return  # table not created yet
            for name, _sqlite_type, pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE agents ADD COLUMN IF NOT EXISTS {name} {pg_type}"
                    )
                    logger.info(f"Added agents.{name} column")
            logger.info("Ensured agents columns (skills, thinking/output overrides)")
    except Exception as e:
        logger.warning(f"Could not ensure agents columns: {e}")


async def _ensure_execution_history_columns(conn) -> None:
    """Idempotently add the executionhistory columns added after it shipped.

    ``create_all`` never ALTERs an existing table, so a database created before
    a column existed keeps a schema the model no longer matches — and because
    SQLAlchemy selects every mapped column, EVERY read of executionhistory then
    fails with "no such column", not just the path that wanted the new field.
    There are Alembic migrations for these too; this covers dev databases built
    from the models with no alembic_version at all.
    """
    is_sqlite = _conn_is_sqlite(conn)
    # (name, sqlite type, postgres type)
    columns = [
        # Resuming a run creates a NEW execution pointing at the one it came from.
        ("resumed_from_execution_id", "INTEGER", "INTEGER"),
        # The saved crew a run was built from — the crew half of flow_id, and
        # what lets a resume rebuild from the current definition instead of
        # replaying the frozen inputs snapshot.
        ("crew_id", "TEXT", "UUID"),
        # Which agent runtime ran this execution. NULL on pre-existing rows,
        # which read as "kasal" — the only engine there was.
        ("harness", "TEXT", "VARCHAR(20)"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(executionhistory)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for name, sqlite_type, _pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE executionhistory ADD COLUMN {name} {sqlite_type}"
                    )
                    logger.info(
                        f"Added executionhistory.{name} column (SQLite self-heal)"
                    )
        else:
            # Read the catalogue first — see _pg_columns: an ALTER on a table
            # this role does not own raises "must be owner" even when the column
            # is already present, and that error aborts the whole self-heal
            # transaction.
            existing = await _pg_columns(conn, "executionhistory")
            if not existing:
                return  # table not created yet
            for name, _sqlite_type, pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE executionhistory "
                        f"ADD COLUMN IF NOT EXISTS {name} {pg_type}"
                    )
                    logger.info(f"Added executionhistory.{name} column")
            logger.info("Ensured executionhistory columns")
    except Exception as e:
        logger.warning(f"Could not ensure executionhistory columns: {e}")


async def _ensure_crew_columns(conn) -> None:
    """Idempotently add reasoning_config to crews. create_all never ALTERs an
    existing table, so DBs created before this column existed (e.g. deployed
    customer instances) would silently drop the saved reasoning budget
    ({"reasoning_effort": ...}) on save/reload. Safe to run every startup; column
    is nullable JSON/TEXT.

    NOTE: the removed planner columns (crews.planning / crews.planning_llm /
    schedule.planning) are dropped by the alembic migration
    20260725_drop_planner_columns. They are deliberately NOT dropped here — a
    self-healing deployed DB keeps the orphan columns harmlessly (nullable and no
    longer mapped), and running DROP COLUMN on every startup is riskier than
    leaving them behind."""
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(crews)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            if "reasoning_config" not in existing:
                await conn.exec_driver_sql(
                    "ALTER TABLE crews ADD COLUMN reasoning_config TEXT"
                )
                logger.info("Added crews.reasoning_config column (SQLite self-heal)")
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE crews ADD COLUMN IF NOT EXISTS reasoning_config JSONB"
            )
            logger.info("Ensured crews.reasoning_config column")
    except Exception as e:
        logger.warning(f"Could not ensure crews.reasoning_config column: {e}")


async def _ensure_ui_config_columns(conn) -> None:
    """Idempotently create ui_config and add its Predefined-UI columns.

    The TABLE was previously left to ``create_all`` — but ``init_db`` skips
    ``create_all`` entirely once the database has more than one table
    ("Tables already exist"), so on any install created before ui_config shipped the
    table simply never appeared. Opening the UI Configurator then 500'd on every
    request with ``no such table: ui_config``, which is why every other
    later-than-the-DB table in this module has its own checkfirst-create.

    The COLUMNS still need the ALTER pass: create_all never ALTERs an existing
    table, so DBs created before catalog_json/style_json existed would silently
    drop a workspace's A2UI catalog + branding on save/reload.

    Safe to run every startup (checkfirst-create, nullable TEXT columns).
    """
    try:
        from src.models.ui_config import UIConfig

        def _create_ui_config_table(sync_conn):
            UIConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_ui_config_table)
        logger.info("Ensured ui_config table exists")
    except Exception as e:
        logger.warning(f"Could not ensure ui_config table: {e}")
    is_sqlite = _conn_is_sqlite(conn)
    # `disabled_components` arrived with the per-component toggles: create_all
    # never ALTERs an existing table, so without it every database provisioned
    # before the toggles would raise on the first read of a UI config.
    columns = ("catalog_json", "style_json", "disabled_components")
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(ui_config)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for col in columns:
                if col not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE ui_config ADD COLUMN {col} TEXT"
                    )
                    logger.info(f"Added ui_config.{col} column (SQLite self-heal)")
        else:
            for col in columns:
                await conn.exec_driver_sql(
                    f"ALTER TABLE ui_config ADD COLUMN IF NOT EXISTS {col} TEXT"
                )
            logger.info(
                "Ensured ui_config catalog_json/style_json/disabled_components columns"
            )
    except Exception as e:
        logger.warning(f"Could not ensure ui_config columns: {e}")


async def _ensure_modelconfig_columns(conn) -> None:
    """Idempotently add the modelconfig columns added after the table shipped.

    ``create_all`` never ALTERs an existing table and Alembic does not run at
    startup here, so without this every database provisioned before these
    columns existed would raise on the first SELECT of the model catalogue —
    which is every LLM call. All nullable with no default, safe every startup.

    JSON rather than TEXT: SQLAlchemy's JSON type reads a TEXT column fine on
    SQLite (it stores JSON as text anyway), and on PostgreSQL the native type is
    what the ORM expects to decode.
    """
    is_sqlite = _conn_is_sqlite(conn)
    columns = [
        ("params", "JSON"),
        ("unsupported_params", "JSON"),
        # Anthropic thinking depth. Which of the two applies is decided by
        # transport.thinking_mode(); see models/model_config.py.
        ("thinking_budget_tokens", "INTEGER"),
        ("reasoning_effort", "VARCHAR"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(modelconfig)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE modelconfig ADD COLUMN {name} {ddl_type}"
                    )
                    logger.info(f"Added modelconfig.{name} column (SQLite self-heal)")
        else:
            # Catalogue first — see _pg_columns and _ensure_agent_columns.
            existing = await _pg_columns(conn, "modelconfig")
            if not existing:
                return  # table not created yet
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE modelconfig ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                    )
                    logger.info(f"Added modelconfig.{name} column")
            logger.info("Ensured modelconfig params/unsupported_params columns")
    except Exception as e:
        logger.warning(f"Could not ensure modelconfig columns: {e}")


async def _ensure_databricks_config_columns(conn) -> None:
    """Idempotently add ai_gateway_enabled to databricksconfig.

    create_all never ALTERs an existing table, so app DBs created before this
    column existed (e.g. customer instances deployed from the marketplace that
    we cannot migrate manually) are missing it — which breaks reading/writing
    the Databricks configuration. Safe to run on every startup; defaults to
    false (serving-endpoints routing) to preserve existing behavior.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(databricksconfig)")
            cols = {row[1] for row in res.fetchall()}
            if not cols:
                return  # table not created yet
            if "ai_gateway_enabled" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE databricksconfig ADD COLUMN ai_gateway_enabled BOOLEAN DEFAULT 0"
                )
                logger.info(
                    "Added databricksconfig.ai_gateway_enabled column (SQLite self-heal)"
                )
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE databricksconfig ADD COLUMN IF NOT EXISTS ai_gateway_enabled BOOLEAN DEFAULT false"
            )
            logger.info("Ensured databricksconfig.ai_gateway_enabled column")
    except Exception as e:
        logger.warning(
            f"Could not ensure databricksconfig.ai_gateway_enabled column: {e}"
        )
