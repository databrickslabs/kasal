"""Data self-heal steps: one-off row fixes that must survive redeploys.

These rewrite ROWS, not schema — renames that outlived the code that produced
them, and a seed that shipped with the wrong flag. Each is idempotent and
scoped tightly enough to run on every startup.
"""

import logging

from src.db.self_heal.dialect import _conn_is_sqlite

logger = logging.getLogger(__name__)


async def _disable_bi_specialist_crew_memory(conn) -> None:
    """Idempotently disable crew/agent memory for the pre-seeded 'bi-specialist'
    workspace. These are deterministic ETL crews (PBI config extraction → UCMV
    generation → validation → deploy) that pass all data via the flow handoff and
    tool configs, NOT via cross-run semantic recall. With memory on, CrewAI
    auto-saves each task's output (incl. the ~174K-char pipeline-config JSON) and
    recalls it workspace-wide into every later agent prompt — overflowing the
    model's context window (200K), which forces a fallback to a larger model and
    stalls the HITL approval gate. The seed now ships memory=False, but the seeder
    is insert-only (it skips a group that already exists), so DBs seeded before
    this change keep memory on. This self-heals them. Safe to run every startup —
    it only flips rows that are still True, scoped to the bi-specialist group."""
    is_sqlite = _conn_is_sqlite(conn)
    # SQLite stores booleans as 0/1; Postgres uses true/false. exec_driver_sql
    # with a literal keeps this dialect-agnostic enough for both.
    true_val = "1" if is_sqlite else "true"
    false_val = "0" if is_sqlite else "false"
    try:
        for table in ("crews", "agents"):
            if is_sqlite:
                res = await conn.exec_driver_sql(f"PRAGMA table_info({table})")
                cols = {row[1] for row in res.fetchall()}
                if "memory" not in cols or "group_id" not in cols:
                    continue  # table/columns not present yet (fresh DB handled by seed)
            await conn.exec_driver_sql(
                f"UPDATE {table} SET memory = {false_val} "
                f"WHERE group_id = 'bi-specialist' AND memory = {true_val}"
            )
        logger.info("Ensured bi-specialist crews/agents have memory disabled")
    except Exception as e:
        logger.warning(f"Could not disable bi-specialist crew memory: {e}")


async def _heal_personal_group_names(conn) -> None:
    """One-time data heal for the workspace→teamspace rename: auto-created
    personal groups persisted the old display name ("Personal Workspace - …")
    in groups.name, which surfaces in the admin group list. The personal tenant
    is now the "Personal Space" (it is not a teamspace), so rewrite the prefix
    in place. Idempotent (the WHERE prefix no longer matches after the first
    run) and DML-only, so it also runs on deployments where DDL is unavailable."""
    try:
        res = await conn.exec_driver_sql(
            "UPDATE \"groups\" SET name = REPLACE(name, 'Personal Workspace', 'Personal Space') "
            "WHERE name LIKE 'Personal Workspace%'"
        )
        renamed = getattr(res, "rowcount", 0) or 0
        if renamed > 0:
            logger.info(f"Renamed {renamed} personal group(s) to 'Personal Space'")
    except Exception as e:
        logger.warning(f"Could not heal personal group names: {e}")


#: The engineconfig keys that existed before the crewai→kasal rename. The heal
#: below is scoped to these, and must stay scoped: `crewai` is a REAL engine
#: name again, so an unscoped rewrite would silently un-select it on every boot.
_LEGACY_ENGINE_CONFIG_KEYS = (
    "flow_enabled",
    "otel_app_telemetry_enabled",
    "otel_app_telemetry_log_level",
)


async def _heal_engine_config_names(conn) -> None:
    """One-time data heal for the crewai→kasal engine rename: engine_configs
    rows persisted engine_name='crewai' (the legacy name of what is now the
    kasal engine). Rewrite in place so lookups keyed on 'kasal' find them.
    Idempotent (the WHERE no longer matches after the first run) and DML-only,
    so it also runs on deployments where DDL is unavailable.

    SCOPED to the keys that predate the rename. CrewAI is a selectable engine
    again, so `engine_name = 'crewai'` is once more a legitimate value; an
    unscoped UPDATE here would rewrite a live CrewAI configuration to `kasal`
    at every startup, and the symptom — "the engine keeps switching back" —
    would point nowhere near this function."""
    try:
        keys = "', '".join(_LEGACY_ENGINE_CONFIG_KEYS)
        res = await conn.exec_driver_sql(
            "UPDATE engineconfig SET engine_name = 'kasal' "
            f"WHERE engine_name = 'crewai' AND config_key IN ('{keys}')"
        )
        renamed = getattr(res, "rowcount", 0) or 0
        if renamed > 0:
            logger.info(
                f"Renamed {renamed} engineconfig row(s) from 'crewai' to 'kasal'"
            )
    except Exception as e:
        logger.warning(f"Could not heal engine_config engine names: {e}")
