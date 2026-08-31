"""Table self-heal steps: checkfirst-create tables added after a DB shipped,
plus the indexes the hot polling paths need.

A brand-new table needs no ALTER, so ``Model.__table__.create(checkfirst=True)``
reaches SQLite, PostgreSQL and Lakebase alike. Models are imported inside each
step so importing this package never pulls the model graph in.
"""

import logging

logger = logging.getLogger(__name__)


async def _ensure_chat_sessions_table(conn) -> None:
    """Idempotently create the chat_sessions table (named chat-mode sessions).

    create_all is skipped on existing DBs, so DBs created before this table
    was added need it created explicitly here.
    """
    try:
        from src.models.chat_session import ChatSession

        def _create_chat_sessions_table(sync_conn):
            ChatSession.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_chat_sessions_table)
        logger.info("Ensured chat_sessions table exists")
    except Exception as e:
        logger.warning(f"Could not ensure chat_sessions table: {e}")


async def _ensure_workflow_recipes_table(conn) -> None:
    """Idempotently create the workflow_recipes table (executed crews kept for reuse).

    create_all is skipped on existing DBs, so this is how the table reaches
    already-deployed installs. No alembic revision: the migration graph
    currently has many heads, and a new table needs no ALTER — checkfirst-create
    covers SQLite, PostgreSQL and Lakebase identically.
    """
    try:
        from src.models.workflow_recipe import WorkflowRecipe

        def _create_workflow_recipes_table(sync_conn):
            WorkflowRecipe.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_workflow_recipes_table)
        logger.info("Ensured workflow_recipes table exists")
    except Exception as e:
        logger.warning(f"Could not ensure workflow_recipes table: {e}")


async def _ensure_workflow_recipe_trials_table(conn) -> None:
    """Idempotently create workflow_recipe_trials (the reuse measurement ledger).

    Same reasoning as the recipes table above: a brand-new table needs no ALTER,
    so a checkfirst-create reaches already-deployed installs identically on
    SQLite, PostgreSQL and Lakebase. Missing this table must never break
    generation — the recording path swallows its own errors — but without it
    every trial write is a silent no-op and the effectiveness report stays empty.
    """
    try:
        from src.models.workflow_recipe_trial import WorkflowRecipeTrial

        def _create_workflow_recipe_trials_table(sync_conn):
            WorkflowRecipeTrial.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_workflow_recipe_trials_table)
        logger.info("Ensured workflow_recipe_trials table exists")
    except Exception as e:
        logger.warning(f"Could not ensure workflow_recipe_trials table: {e}")


async def _ensure_mlflow_config_table(conn) -> None:
    """Idempotently create the mlflowconfig table (per-group MLflow settings).

    create_all is skipped on existing DBs, so a database created before this
    model landed (the common local-dev case) never gets the table and every
    ``GET /mlflow/settings`` 500s with "no such table: mlflowconfig". New table,
    no ALTER, so a checkfirst-create reaches SQLite, PostgreSQL and Lakebase
    identically — same pattern as _ensure_prompt_optimization_runs_table.
    """
    try:
        from src.models.mlflow_config import MLflowConfig

        def _create(sync_conn):
            MLflowConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured mlflowconfig table exists")
    except Exception as e:
        logger.warning(f"Could not ensure mlflowconfig table: {e}")


async def _ensure_a2a_push_configs_table(conn) -> None:
    """Idempotently create a2a_push_configs (A2A webhook registrations).

    New table, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here.
    """
    try:
        from src.models.a2a_push_config import A2APushConfig

        def _create(sync_conn):
            A2APushConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured a2a_push_configs table exists")
    except Exception as e:
        logger.warning(f"Could not ensure a2a_push_configs table: {e}")


async def _ensure_skills_tables(conn) -> None:
    """Idempotently create skills + skill_files (Agent Skills storage).

    New tables, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here. Files after
    skills: the child carries the foreign key.
    """
    try:
        from src.models.skill import Skill, SkillFile

        def _create(sync_conn):
            Skill.__table__.create(sync_conn, checkfirst=True)
            SkillFile.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured skills tables exist")
    except Exception as e:
        logger.warning(f"Could not ensure skills tables: {e}")


async def _ensure_a2a_agents_table(conn) -> None:
    """Idempotently create a2a_agents (remote agents Kasal can call).

    New table, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here.
    """
    try:
        from src.models.a2a_agent import A2AAgent

        def _create(sync_conn):
            A2AAgent.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured a2a_agents table exists")
    except Exception as e:
        logger.warning(f"Could not ensure a2a_agents table: {e}")


async def _ensure_crew_publications_table(conn) -> None:
    """Idempotently create `publications` (the external-publication registry).

    A brand-new table, so a checkfirst-create reaches already-deployed installs
    identically on SQLite, PostgreSQL and Lakebase — same reasoning as the
    tables above, and the Alembic migration carries the same guard.

    Without it the MCP tool list and the A2A Agent Card both fail: they read
    this table to answer "what has this workspace published", and an unhandled
    UndefinedTable turns a discovery request into a 500.
    """
    try:
        from src.models.crew_publication import Publication

        def _create_crew_publications_table(sync_conn):
            Publication.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_crew_publications_table)
        logger.info("Ensured publications table exists")
    except Exception as e:
        logger.warning(f"Could not ensure publications table: {e}")


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


async def _ensure_crew_feedback_table(conn) -> None:
    """Idempotently create the crew_feedback table (thumbs feedback on
    cataloged crews). create_all is skipped on existing DBs."""
    try:
        from src.models.crew_feedback import CrewFeedback

        def _create_crew_feedback_table(sync_conn):
            CrewFeedback.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_crew_feedback_table)
        logger.info("Ensured crew_feedback table exists")
    except Exception as e:
        logger.warning(f"Could not ensure crew_feedback table: {e}")


async def _ensure_powerbi_extraction_table(conn) -> None:
    """Idempotently create the powerbi_extraction table (raw Power BI extraction
    artifacts persisted per Pipeline Config Generator run, for SQL querying).
    create_all is skipped on existing DBs, so DBs created before this table
    existed need this self-heal."""
    try:
        from src.models.powerbi_extraction import PowerBIExtraction

        def _create_powerbi_extraction_table(sync_conn):
            PowerBIExtraction.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_powerbi_extraction_table)
        logger.info("Ensured powerbi_extraction table exists")
    except Exception as e:
        logger.warning(f"Could not ensure powerbi_extraction table: {e}")


async def _ensure_prompt_optimization_runs_table(conn) -> None:
    """Idempotently create the prompt_optimization_runs table (durable GEPA
    optimization runs, including the before-image an apply can be reverted
    from). create_all is skipped on existing DBs, so DBs created before this
    table existed need this self-heal."""
    try:
        from src.models.prompt_optimization_run import PromptOptimizationRun

        def _create_prompt_optimization_runs_table(sync_conn):
            PromptOptimizationRun.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_prompt_optimization_runs_table)
        logger.info("Ensured prompt_optimization_runs table exists")
    except Exception as e:
        logger.warning(f"Could not ensure prompt_optimization_runs table: {e}")


async def _ensure_memory_maintenance_table(conn) -> None:
    """Idempotently create memory_maintenance_watermarks.

    Per-scope record of when memory maintenance last ran, which is what makes
    the scheduled sweep's coverage depend on store size rather than on how often
    someone happens to run a crew. Same reasoning as the tables above: a brand
    new table needs no ALTER, so a checkfirst-create reaches already-deployed
    installs identically on SQLite, PostgreSQL and Lakebase. A missing table
    must never break anything — the sweep swallows its own errors — but without
    it every tick would re-maintain the same scopes."""
    try:
        from src.models.memory_maintenance import MemoryMaintenanceWatermark

        def _create_memory_maintenance_table(sync_conn):
            MemoryMaintenanceWatermark.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_memory_maintenance_table)
        logger.info("Ensured memory_maintenance_watermarks table exists")
    except Exception as e:
        logger.warning(f"Could not ensure memory_maintenance_watermarks table: {e}")


async def _ensure_trigger_queue_table(conn) -> None:
    """Create the ``triggerqueue`` table on an existing DB.

    The event-trigger queue was added after most DBs were provisioned, and
    ``create_all`` never touches an existing database — so without this the
    ``/triggers`` API and the consumer hit "no such table: triggerqueue" on both
    the local SQLite ``app.db`` and any pre-existing Lakebase. Idempotent via
    ``checkfirst=True``.
    """
    try:
        from src.models.trigger_queue import TriggerQueue

        def _create(sync_conn):
            TriggerQueue.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured triggerqueue table exists")
    except Exception as e:
        logger.warning(f"Could not ensure triggerqueue table: {e}")


async def _ensure_event_choreography_tables(conn) -> None:
    """Create the ``eventsubscription`` + ``emitrule`` tables on an existing DB.

    Same rationale as ``_ensure_trigger_queue_table``: ``create_all`` never
    touches an existing database, so these event-choreography config tables must
    be self-healed on startup (SQLite and Lakebase). Idempotent via
    ``checkfirst=True``.
    """
    try:
        from src.models.event_subscription import EmitRule, EventSubscription

        def _create(sync_conn):
            EventSubscription.__table__.create(sync_conn, checkfirst=True)
            EmitRule.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured eventsubscription + emitrule tables exist")
    except Exception as e:
        logger.warning(f"Could not ensure event choreography tables: {e}")
