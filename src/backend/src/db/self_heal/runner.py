"""The self-heal pass: every step, each isolated in its own SAVEPOINT.

Steps are imported by name into this module so a test can
``patch.multiple("src.db.self_heal.runner", …)`` the ones it wants to observe.
"""

import logging

from src.db.self_heal.columns import (
    _ensure_agent_columns,
    _ensure_chat_sessions_columns,
    _ensure_crew_columns,
    _ensure_databricks_config_columns,
    _ensure_documentation_embeddings_columns,
    _ensure_execution_history_columns,
    _ensure_group_users_columns,
    _ensure_modelconfig_columns,
    _ensure_ui_config_columns,
)
from src.db.self_heal.data import (
    _disable_bi_specialist_crew_memory,
    _heal_engine_config_names,
    _heal_personal_group_names,
)
from src.db.self_heal.dialect import _conn_is_sqlite
from src.db.self_heal.tables import (
    _ensure_a2a_agents_table,
    _ensure_a2a_push_configs_table,
    _ensure_chat_assets_table,
    _ensure_chat_sessions_table,
    _ensure_crew_feedback_table,
    _ensure_crew_publications_table,
    _ensure_event_choreography_tables,
    _ensure_hot_polling_indexes,
    _ensure_memory_maintenance_table,
    _ensure_mlflow_config_table,
    _ensure_powerbi_extraction_table,
    _ensure_prompt_optimization_runs_table,
    _ensure_skills_tables,
    _ensure_trigger_queue_table,
    _ensure_workflow_recipe_trials_table,
    _ensure_workflow_recipes_table,
)

logger = logging.getLogger(__name__)


async def run_schema_self_heal(conn) -> None:
    """Create missing tables and add missing columns on an existing DB.

    ``create_all`` fully creates a NEW table but never ALTERs an existing one, so
    DBs provisioned before a table/column was added won't have it. Each ``_ensure_*``
    helper here is idempotent and NON-DESTRUCTIVE — ``CREATE TABLE IF NOT EXISTS``
    for tables, ``ADD COLUMN IF NOT EXISTS`` for columns — so this is safe to run
    on every startup and against any engine's connection.

    Runs against BOTH the local/default engine (in ``init_db``) AND, critically,
    the active Lakebase engine after the runtime hot-swap (``main.py`` lifespan) —
    the latter is the only path that heals a customer's PRE-EXISTING Lakebase,
    which ``init_db`` alone misses because it fires before Lakebase activation.

    Each step runs in its own SAVEPOINT. Every helper already catches its own
    exception and logs a warning, which LOOKS like isolation but is not: on
    PostgreSQL a failed statement aborts the whole transaction, so every
    subsequent statement on the same connection dies with
    ``InFailedSQLTransactionError`` — "current transaction is aborted, commands
    ignored until end of transaction block". Swallowing the first error therefore
    converted one skippable failure into a silent, total no-op.

    That is not hypothetical. On the deployed app ``documentation_embeddings`` was
    left owned by a PREVIOUS deploy's service principal, so its ALTER failed with
    ``must be owner of table`` — and because it runs FIRST, it took the other 23
    steps with it. ``agents.thinking_budget_tokens`` was never added, and agent
    creation failed with ``column "thinking_budget_tokens" of relation "agents"
    does not exist``. Rolling back to a savepoint makes each step independently
    skippable, which is what the per-helper try/except was always meant to give.
    """
    # On Lakebase, act as the shared ``databricks_superuser`` role for the whole
    # heal. Every identity on the instance (each app's SPN + the admins) is a
    # member of it, so this bypasses table-ownership checks — without it, a table
    # created by a DIFFERENT principal makes ADD COLUMN fail with "must be owner",
    # and because that abort poisons the transaction it took EVERY later step
    # with it (crew_id included). It also lets us install pgvector below. Both are
    # best-effort: on a plain Postgres with no such role they no-op and the heal
    # proceeds as the connecting role, exactly as before. Skipped on SQLite.
    if not _conn_is_sqlite(conn):
        from src.services.databricks.lakebase.superuser import (
            enable_pgvector_async,
            enter_superuser_async,
        )

        await enter_superuser_async(conn)
        # Enable pgvector up front so the vector-column heals (documentation_
        # embeddings / knowledge_embeddings / workflow_recipes) can add their
        # embedding columns instead of falling back to the vector-free schema.
        await enable_pgvector_async(conn)

    steps = (
        _ensure_documentation_embeddings_columns,
        _ensure_databricks_config_columns,
        _ensure_chat_assets_table,
        _ensure_chat_sessions_table,
        _ensure_chat_sessions_columns,
        _ensure_workflow_recipes_table,
        _ensure_workflow_recipe_trials_table,
        _ensure_crew_publications_table,
        _ensure_trigger_queue_table,
        _ensure_event_choreography_tables,
        _ensure_a2a_push_configs_table,
        _ensure_a2a_agents_table,
        _ensure_skills_tables,
        _ensure_crew_feedback_table,
        _ensure_powerbi_extraction_table,
        _ensure_prompt_optimization_runs_table,
        _ensure_mlflow_config_table,
        _ensure_memory_maintenance_table,
        _ensure_agent_columns,
        _ensure_crew_columns,
        _ensure_group_users_columns,
        _ensure_execution_history_columns,
        _ensure_ui_config_columns,
        _ensure_modelconfig_columns,
        _ensure_hot_polling_indexes,
        _heal_personal_group_names,
        _heal_engine_config_names,
        _disable_bi_specialist_crew_memory,
    )
    for step in steps:
        await _run_self_heal_step(conn, step)


async def _run_self_heal_step(conn, step) -> None:
    """Run one self-heal step so its failure cannot abort the ones after it.

    ``conn.begin_nested()`` is a SAVEPOINT: releasing it on success keeps the
    work, rolling it back on failure returns the transaction to a usable state.
    Without it a single failed DDL poisons every later step (see
    ``run_schema_self_heal``).

    Falls back to calling the step directly if the connection cannot nest — a
    mock in tests, or a driver without savepoint support. The helpers still log
    their own warnings, so behaviour there is exactly what it was before.
    """
    try:
        nested = conn.begin_nested()
    except Exception:  # noqa: BLE001 — no savepoint support; degrade, don't fail
        await step(conn)
        return
    try:
        async with nested:
            await step(conn)
    except Exception as exc:  # noqa: BLE001 — one broken step must not stop the rest
        # The step logged the cause; this records that it was ISOLATED, which is
        # the difference between "one table skipped" and "nothing healed".
        logger.warning(
            f"Schema self-heal step {step.__name__} rolled back, continuing: {exc}"
        )
