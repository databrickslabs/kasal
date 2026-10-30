"""The self-heal pass: every step, each isolated in its own SAVEPOINT.

Steps are imported by name into this module so a test can
``patch.multiple("src.db.self_heal.runner", …)`` the ones it wants to observe.
"""

from sqlalchemy.ext.asyncio import AsyncConnection

from src.db.self_heal.columns import (
    _ensure_agent_columns,
    _ensure_chat_sessions_columns,
    _ensure_crew_columns,
    _ensure_databricks_config_columns,
    _ensure_documentation_embeddings_columns,
    _ensure_execution_history_columns,
    _ensure_flow_states_columns,
    _ensure_flow_states_group_index,
    _ensure_group_users_columns,
    _ensure_knowledge_embeddings_table,
    _ensure_memory_backend_columns,
    _ensure_mlflow_config_columns,
    _ensure_modelconfig_columns,
    _ensure_powerbi_extraction_columns,
    _ensure_publications_columns,
    _ensure_schedule_columns,
    _ensure_ui_config_columns,
    _ensure_users_columns,
    _ensure_users_personal_group_index,
)
from src.db.self_heal.data import (
    _assign_personal_workspace_ids,
    _disable_bi_specialist_crew_memory,
    _heal_engine_config_names,
    _heal_personal_group_names,
    _migrate_genie_one_mcp_urls,
)
from src.db.self_heal.dialect import _conn_is_sqlite
from src.db.self_heal.report import HealGap, Step, heal_pass, run_isolated
from src.db.self_heal.tables import (
    _drop_decision_config_group_fk,
    _ensure_a2a_agents_table,
    _ensure_a2a_push_configs_table,
    _ensure_chat_assets_table,
    _ensure_chat_sessions_table,
    _ensure_crew_feedback_table,
    _ensure_crew_publications_table,
    _ensure_decision_config_table,
    _ensure_event_choreography_tables,
    _ensure_hot_polling_indexes,
    _ensure_memory_maintenance_table,
    _ensure_mlflow_config_table,
    _ensure_model_billing_rates_table,
    _ensure_powerbi_extraction_table,
    _ensure_prompt_optimization_runs_table,
    _ensure_skills_tables,
    _ensure_trigger_queue_table,
    _ensure_workflow_recipe_trials_table,
    _ensure_workflow_recipes_table,
)
from src.db.self_heal.vectors import _ensure_pgvector_embedding_columns


async def run_schema_self_heal(
    conn: AsyncConnection, *, record: bool = True
) -> tuple[HealGap, ...]:
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

    Each step runs in its own SAVEPOINT, and a step does NOT catch its own error:
    the error has to reach the savepoint so it rolls back (see
    ``self_heal/report.py``). On PostgreSQL a failed statement aborts the whole
    transaction, so a helper that swallowed its error left every later step dying
    with "current transaction is aborted" — and the final COMMIT silently became
    a ROLLBACK, discarding ``create_all``'s new tables on the Lakebase boot path.

    That is not hypothetical. On the deployed app ``documentation_embeddings`` was
    left owned by a PREVIOUS deploy's service principal, so its ALTER failed with
    ``must be owner of table`` — and because it runs FIRST, it took the other 23
    steps with it. ``agents.thinking_budget_tokens`` was never added, and agent
    creation failed with ``column "thinking_budget_tokens" of relation "agents"
    does not exist``.

    Returns the steps that did not apply. They are logged at ERROR and shown by
    ``GET /health/db``; they never stop startup. Pass ``record=False`` when
    healing a database the app does not run on yet (a migration target), so its
    result does not stand in for the active database's.
    """
    with heal_pass(conn, record=record) as gaps:
        # On Lakebase, act as the shared ``databricks_superuser`` role for the
        # whole heal. Every identity on the instance (each app's SPN + the
        # admins) is a member of it, so this bypasses table-ownership checks:
        # without it, a table created by a DIFFERENT principal makes ADD COLUMN
        # fail with "must be owner". It also lets us install pgvector below. Both
        # are best-effort and savepoint-isolated: on a plain Postgres with no such
        # role they no-op and the heal proceeds as the connecting role. Skipped
        # on SQLite.
        if not _conn_is_sqlite(conn):
            from src.db.lakebase_ddl import (
                enable_pgvector_async,
                enter_superuser_async,
            )

            await enter_superuser_async(conn)
            # Enable pgvector up front so the vector-column heals (documentation_
            # embeddings / knowledge_embeddings / workflow_recipes) can add their
            # embedding columns instead of falling back to the vector-free schema.
            await enable_pgvector_async(conn)

        steps: tuple[Step, ...] = (
            _ensure_decision_config_table,
            _drop_decision_config_group_fk,
            _ensure_documentation_embeddings_columns,
            _ensure_knowledge_embeddings_table,
            _ensure_databricks_config_columns,
            _ensure_chat_assets_table,
            _ensure_chat_sessions_table,
            _ensure_chat_sessions_columns,
            _ensure_publications_columns,
            _ensure_users_columns,
            _ensure_users_personal_group_index,
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
            _ensure_powerbi_extraction_columns,
            _ensure_prompt_optimization_runs_table,
            _ensure_mlflow_config_table,
            _ensure_mlflow_config_columns,
            _ensure_model_billing_rates_table,
            _ensure_memory_maintenance_table,
            _ensure_memory_backend_columns,
            _ensure_agent_columns,
            _ensure_crew_columns,
            _ensure_group_users_columns,
            _ensure_execution_history_columns,
            _ensure_schedule_columns,
            _ensure_flow_states_columns,
            _ensure_flow_states_group_index,
            _ensure_ui_config_columns,
            _ensure_modelconfig_columns,
            # After every table step, so a vector table created on this pass
            # (workflow_recipes, knowledge_embeddings) gets its column too.
            _ensure_pgvector_embedding_columns,
            _ensure_hot_polling_indexes,
            _heal_personal_group_names,
            _assign_personal_workspace_ids,
            _heal_engine_config_names,
            _disable_bi_specialist_crew_memory,
            _migrate_genie_one_mcp_urls,
        )
        for step in steps:
            await _run_self_heal_step(conn, step)
    return tuple(gaps)


async def _run_self_heal_step(conn: AsyncConnection, step: Step) -> bool:
    """Run one self-heal step so its failure cannot abort the ones after it.

    ``conn.begin_nested()`` is a SAVEPOINT: releasing it on success keeps the
    work, rolling it back on failure returns the transaction to a usable state
    and undoes whatever the step did before it failed. The failure is logged and
    recorded as a gap (``self_heal/report.py``). True when the step applied.
    """
    return await run_isolated(conn, step.__name__, step)
