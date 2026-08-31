"""Table self-heal steps: checkfirst-create tables added after a DB shipped,
plus the indexes the hot polling paths need.

``init_db`` skips ``create_all`` once a database has any table, so a table
added later never appears on an existing install unless a step here creates
it. A brand-new table needs no ALTER, so ``__table__.create(checkfirst=True)``
reaches SQLite, PostgreSQL and Lakebase alike. ``ensure_table`` imports the
model lazily: importing this package must never pull the model graph in.
"""

import importlib
import logging

logger = logging.getLogger(__name__)


async def ensure_table(conn, module: str, *models: str) -> None:
    """checkfirst-create the tables of ``models`` (class names in ``module``)."""
    try:
        mod = importlib.import_module(module)
        tables = [getattr(mod, name).__table__ for name in models]

        def _create(sync_conn):
            for table in tables:
                table.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info(f"Ensured {', '.join(t.name for t in tables)} table(s) exist")
    except Exception as e:
        logger.warning(f"Could not ensure {', '.join(models)} table(s): {e}")


async def _ensure_chat_sessions_table(conn) -> None:
    """Named chat-mode sessions."""
    await ensure_table(conn, "src.models.chat_session", "ChatSession")


async def _ensure_workflow_recipes_table(conn) -> None:
    """Executed crews kept for reuse."""
    await ensure_table(conn, "src.models.workflow_recipe", "WorkflowRecipe")


async def _ensure_workflow_recipe_trials_table(conn) -> None:
    """The reuse measurement ledger — without it every trial write is a silent no-op and the effectiveness report stays empty."""
    await ensure_table(conn, "src.models.workflow_recipe_trial", "WorkflowRecipeTrial")


async def _ensure_mlflow_config_table(conn) -> None:
    """Per-group MLflow settings."""
    await ensure_table(conn, "src.models.mlflow_config", "MLflowConfig")


async def _ensure_a2a_push_configs_table(conn) -> None:
    """A2A push-notification configs."""
    await ensure_table(conn, "src.models.a2a_push_config", "A2APushConfig")


async def _ensure_skills_tables(conn) -> None:
    """Skills and their files."""
    await ensure_table(conn, "src.models.skill", "Skill", "SkillFile")


async def _ensure_a2a_agents_table(conn) -> None:
    """Registered A2A agents."""
    await ensure_table(conn, "src.models.a2a_agent", "A2AAgent")


async def _ensure_crew_publications_table(conn) -> None:
    """Published crews (the MCP/A2A catalogue)."""
    await ensure_table(conn, "src.models.crew_publication", "Publication")


async def _ensure_crew_feedback_table(conn) -> None:
    """Thumbs feedback on catalogued crews."""
    await ensure_table(conn, "src.models.crew_feedback", "CrewFeedback")


async def _ensure_powerbi_extraction_table(conn) -> None:
    """Power BI extraction artifacts, per Pipeline Config Generator run."""
    await ensure_table(conn, "src.models.powerbi_extraction", "PowerBIExtraction")


async def _ensure_prompt_optimization_runs_table(conn) -> None:
    """Prompt-optimization runs."""
    await ensure_table(
        conn, "src.models.prompt_optimization_run", "PromptOptimizationRun"
    )


async def _ensure_memory_maintenance_table(conn) -> None:
    """Memory-maintenance watermarks."""
    await ensure_table(
        conn, "src.models.memory_maintenance", "MemoryMaintenanceWatermark"
    )


async def _ensure_trigger_queue_table(conn) -> None:
    """The event-trigger queue — the /triggers API and the consumer hit "no such table" without it."""
    await ensure_table(conn, "src.models.trigger_queue", "TriggerQueue")


async def _ensure_event_choreography_tables(conn) -> None:
    """Event subscriptions and emit rules."""
    await ensure_table(
        conn, "src.models.event_subscription", "EventSubscription", "EmitRule"
    )


async def _ensure_hot_polling_indexes(conn) -> None:
    """Idempotently add the indexes the run-polling queries filter/sort on.

    create_all only creates indexes for NEW tables, so existing deployed DBs
    sequential-scan/sort the two biggest, fastest-growing tables on every 2s
    poll: executionhistory (list: group_id + ORDER BY created_at DESC; trace
    broadcaster: status IN ('RUNNING', ...) every second) and execution_trace
    (run-scoped reads/deletes on run_id, ordered reads on created_at).
    CREATE INDEX IF NOT EXISTS is valid on both SQLite and PostgreSQL."""
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_executionhistory_group_created "
        "ON executionhistory (group_id, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_executionhistory_status "
        "ON executionhistory (status)",
        "CREATE INDEX IF NOT EXISTS ix_executionhistory_created_at "
        "ON executionhistory (created_at)",
        "CREATE INDEX IF NOT EXISTS ix_execution_trace_run_id "
        "ON execution_trace (run_id)",
        "CREATE INDEX IF NOT EXISTS ix_execution_trace_created_at "
        "ON execution_trace (created_at)",
    )
    for stmt in statements:
        try:
            await conn.exec_driver_sql(stmt)
        except Exception as e:
            logger.warning(
                f"Could not ensure polling index ({stmt.split(' ON ', 1)[0]}): {e}"
            )
    logger.info("Ensured hot-polling indexes on executionhistory/execution_trace")
