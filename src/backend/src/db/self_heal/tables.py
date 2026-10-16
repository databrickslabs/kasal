"""Table self-heal steps: checkfirst-create tables added after a DB shipped,
the indexes the hot polling paths need, and table-level constraint changes
(the ``decision_config`` foreign key drop).

``init_db`` skips ``create_all`` once a database has any table, so a table
added later never appears on an existing install unless a step here creates
it. A brand-new table needs no ALTER, so ``__table__.create(checkfirst=True)``
reaches SQLite, PostgreSQL and Lakebase alike. ``ensure_table`` imports the
model lazily: importing this package must never pull the model graph in.
"""

import importlib
import logging

from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection

from src.db.self_heal.dialect import _conn_is_sqlite, _index_exists
from src.db.self_heal.report import run_isolated

logger = logging.getLogger(__name__)


async def ensure_table(conn: AsyncConnection, module: str, *models: str) -> None:
    """checkfirst-create the tables of ``models`` (class names in ``module``).

    A failure RAISES into the step's SAVEPOINT (see ``self_heal/report.py``).
    """
    mod = importlib.import_module(module)
    tables = [getattr(mod, name).__table__ for name in models]

    def _create(sync_conn: Connection) -> None:
        for table in tables:
            table.create(sync_conn, checkfirst=True)

    await conn.run_sync(_create)
    logger.info(f"Ensured {', '.join(t.name for t in tables)} table(s) exist")


async def ensure_index(conn: AsyncConnection, table: str, name: str, ddl: str) -> None:
    """Run ``ddl`` (a CREATE INDEX on ``table``) unless ``table`` has index ``name``.

    Reads the catalogue first for the same reason ``ensure_columns`` does: on
    PostgreSQL ``CREATE INDEX IF NOT EXISTS`` checks table ownership before it
    checks existence, so on a table another role owns it fails even when the
    index is there. A failure RAISES into the caller's SAVEPOINT.
    """
    if await _index_exists(conn, table, name):
        return
    await conn.exec_driver_sql(ddl)
    logger.info("Created index %s", name)


async def _ensure_chat_sessions_table(conn: AsyncConnection) -> None:
    """Named chat-mode sessions."""
    await ensure_table(conn, "src.models.chat_session", "ChatSession")


async def _ensure_model_billing_rates_table(conn: AsyncConnection) -> None:
    """Teamspace model prices used for usage estimates."""
    await ensure_table(conn, "src.models.model_billing_rate", "ModelBillingRate")


async def _ensure_chat_assets_table(conn: AsyncConnection) -> None:
    """Images attached in the chat, kept whole so they can be shown."""
    await ensure_table(conn, "src.models.chat_asset", "ChatAsset")


async def _ensure_workflow_recipes_table(conn: AsyncConnection) -> None:
    """Executed crews kept for reuse."""
    await ensure_table(conn, "src.models.workflow_recipe", "WorkflowRecipe")


async def _ensure_workflow_recipe_trials_table(conn: AsyncConnection) -> None:
    """The reuse measurement ledger — without it every trial write is a silent no-op and the effectiveness report stays empty."""
    await ensure_table(conn, "src.models.workflow_recipe_trial", "WorkflowRecipeTrial")


async def _ensure_mlflow_config_table(conn: AsyncConnection) -> None:
    """Per-group MLflow settings."""
    await ensure_table(conn, "src.models.mlflow_config", "MLflowConfig")


async def _ensure_a2a_push_configs_table(conn: AsyncConnection) -> None:
    """A2A push-notification configs."""
    await ensure_table(conn, "src.models.a2a_push_config", "A2APushConfig")


async def _ensure_skills_tables(conn: AsyncConnection) -> None:
    """Skills and their files."""
    await ensure_table(conn, "src.models.skill", "Skill", "SkillFile")


async def _ensure_a2a_agents_table(conn: AsyncConnection) -> None:
    """Registered A2A agents."""
    await ensure_table(conn, "src.models.a2a_agent", "A2AAgent")


async def _ensure_crew_publications_table(conn: AsyncConnection) -> None:
    """Published crews (the MCP/A2A catalogue)."""
    await ensure_table(conn, "src.models.crew_publication", "Publication")


async def _ensure_crew_feedback_table(conn: AsyncConnection) -> None:
    """Thumbs feedback on catalogued crews."""
    await ensure_table(conn, "src.models.crew_feedback", "CrewFeedback")


async def _ensure_powerbi_extraction_table(conn: AsyncConnection) -> None:
    """Power BI extraction artifacts, per Pipeline Config Generator run."""
    await ensure_table(conn, "src.models.powerbi_extraction", "PowerBIExtraction")


async def _ensure_prompt_optimization_runs_table(conn: AsyncConnection) -> None:
    """Prompt-optimization runs."""
    await ensure_table(
        conn, "src.models.prompt_optimization_run", "PromptOptimizationRun"
    )


async def _ensure_memory_maintenance_table(conn: AsyncConnection) -> None:
    """Memory-maintenance watermarks."""
    await ensure_table(
        conn, "src.models.memory_maintenance", "MemoryMaintenanceWatermark"
    )


async def _ensure_trigger_queue_table(conn: AsyncConnection) -> None:
    """The event-trigger queue — the /triggers API and the consumer hit "no such table" without it."""
    await ensure_table(conn, "src.models.trigger_queue", "TriggerQueue")


async def _ensure_event_choreography_tables(conn: AsyncConnection) -> None:
    """Event subscriptions and emit rules."""
    await ensure_table(
        conn, "src.models.event_subscription", "EventSubscription", "EmitRule"
    )


async def _ensure_hot_polling_indexes(conn: AsyncConnection) -> None:
    """Idempotently add the indexes the run-polling queries filter/sort on.

    create_all only creates indexes for NEW tables, so existing deployed DBs
    sequential-scan/sort the two biggest, fastest-growing tables on every 2s
    poll: executionhistory (list: group_id + ORDER BY created_at DESC; trace
    broadcaster: status IN ('RUNNING', ...) every second) and execution_trace
    (run-scoped reads/deletes on run_id, ordered reads on created_at).
    CREATE INDEX IF NOT EXISTS is valid on both SQLite and PostgreSQL.

    Each index has its own SAVEPOINT, so one that cannot be built (a table
    another role owns) neither aborts the pass nor stops the others."""
    indexes = (
        (
            "idx_executionhistory_group_created",
            "executionhistory",
            "group_id, created_at",
        ),
        ("ix_executionhistory_status", "executionhistory", "status"),
        ("ix_executionhistory_created_at", "executionhistory", "created_at"),
        ("ix_execution_trace_run_id", "execution_trace", "run_id"),
        ("ix_execution_trace_created_at", "execution_trace", "created_at"),
    )
    applied = True
    for name, table, columns in indexes:
        ddl = f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns})"

        async def _create(
            c: AsyncConnection, table: str = table, name: str = name, ddl: str = ddl
        ) -> None:
            await ensure_index(c, table, name, ddl)

        step = f"_ensure_hot_polling_indexes.{name}"
        applied = await run_isolated(conn, step, _create) and applied
    if applied:
        logger.info("Ensured hot-polling indexes on executionhistory/execution_trace")


async def _ensure_decision_config_table(conn: AsyncConnection) -> None:
    await ensure_table(conn, "src.models.decision_config", "DecisionConfig")


async def _drop_decision_config_group_fk(conn: AsyncConnection) -> None:
    """Drop ``decision_config.group_id -> groups.id`` on installs that have it.

    Personal workspaces (``user_<email>``) have no ``groups`` row, so the key
    made the decision-model switch impossible to save there. ``create_all`` never alters an
    existing table, and migration ``20260927_decision_config_drop_group_fk``
    only reaches Alembic users, so this is what heals SQLite, PostgreSQL and
    Lakebase installs. Idempotent: it does nothing once no such key exists.
    """
    if _conn_is_sqlite(conn):
        fks = (
            await conn.exec_driver_sql("PRAGMA foreign_key_list(decision_config)")
        ).fetchall()
        # Row shape: (id, seq, table, from, to, on_update, on_delete, match).
        if not any(row[2] == "groups" for row in fks):
            return
        # SQLite cannot drop a constraint in place: rebuild from the model,
        # which no longer declares the key. Nothing references decision_config.
        from src.models.decision_config import DecisionConfig

        await conn.exec_driver_sql(
            "ALTER TABLE decision_config RENAME TO decision_config__with_fk"
        )
        table = DecisionConfig.metadata.tables[DecisionConfig.__tablename__]
        await conn.run_sync(lambda sync_conn: table.create(sync_conn))
        await conn.exec_driver_sql(
            "INSERT INTO decision_config (group_id, enabled) "
            "SELECT group_id, enabled FROM decision_config__with_fk"
        )
        await conn.exec_driver_sql("DROP TABLE decision_config__with_fk")
        logger.info("Dropped the decision_config -> groups foreign key (SQLite)")
        return

    names = (
        await conn.exec_driver_sql(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = to_regclass('decision_config') "
            "AND confrelid = to_regclass('groups') AND contype = 'f'"
        )
    ).fetchall()
    for (name,) in names:
        quoted = name.replace('"', '""')
        await conn.exec_driver_sql(
            f'ALTER TABLE decision_config DROP CONSTRAINT "{quoted}"'
        )
        logger.info("Dropped the decision_config foreign key %s", name)
