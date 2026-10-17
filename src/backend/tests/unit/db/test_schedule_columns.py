"""``schedule.crew_id`` and ``schedule.timezone`` on an existing database.

``create_all`` never ALTERs a table, so an install made before the columns gets
them from the self-heal step at startup (``_ensure_schedule_columns``); Alembic
users get migration ``20261017_schedule_timezone_and_crew``. Both must keep the
rows, and both leave the new columns NULL — the old behaviour.
"""

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal import columns as heal
from src.db.self_heal import runner

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "20261017_schedule_timezone_and_crew.py"
)

# The schedule table as an install from before the two columns has it.
OLD_SCHEMA = (
    "CREATE TABLE schedule ("
    "id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, "
    "cron_expression VARCHAR NOT NULL, execution_type VARCHAR(20) NOT NULL, "
    "flow_id CHAR(32), group_id VARCHAR(100))"
)
OLD_ROW = "INSERT INTO schedule VALUES (1, 'nightly', '0 9 * * *', 'crew', NULL, 'g1')"


def _columns_sync(conn) -> set:
    return {c["name"] for c in inspect(conn).get_columns("schedule")}


# --- self-heal ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_heal_adds_both_columns_to_an_existing_sqlite_table():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(OLD_SCHEMA)
            await conn.exec_driver_sql(OLD_ROW)

            await heal._ensure_schedule_columns(conn)
            cols = await conn.run_sync(_columns_sync)
            assert {"crew_id", "timezone"} <= cols

            row = (
                await conn.exec_driver_sql(
                    "SELECT name, crew_id, timezone FROM schedule"
                )
            ).fetchall()
            assert [tuple(r) for r in row] == [("nightly", None, None)]

            # Idempotent: a second pass adds nothing.
            await heal._ensure_schedule_columns(conn)
            info = (
                await conn.exec_driver_sql("PRAGMA table_info(schedule)")
            ).fetchall()
            names = [r[1] for r in info]
            assert names.count("crew_id") == names.count("timezone") == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_self_heal_is_quiet_without_the_table():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await heal._ensure_schedule_columns(conn)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_the_step_runs_in_the_startup_pass():
    conn = MagicMock()
    conn.engine.dialect.name = "sqlite"
    conn.exec_driver_sql = AsyncMock(return_value=MagicMock())
    conn.run_sync = AsyncMock()
    ran = []

    async def recorder(_conn):
        ran.append("schedule")

    with patch.object(runner, "_ensure_schedule_columns", new=recorder):
        await runner.run_schema_self_heal(conn)
    assert ran == ["schedule"]


@pytest.mark.asyncio
async def test_the_orm_reads_a_healed_table():
    """SQLAlchemy selects every mapped column, so a missing one breaks every
    read of the table. After the heal, the model loads the old row."""
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    from src.models.schedule import Schedule

    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                OLD_SCHEMA.replace(
                    "group_id VARCHAR(100))",
                    "group_id VARCHAR(100), agents_yaml JSON, tasks_yaml JSON, "
                    "nodes JSON, edges JSON, flow_config JSON, inputs JSON, "
                    "is_active BOOLEAN, model VARCHAR, last_run_at DATETIME, "
                    "next_run_at DATETIME, created_at DATETIME, "
                    "updated_at DATETIME, created_by_email VARCHAR(255))",
                )
            )
            await conn.exec_driver_sql(
                "INSERT INTO schedule (id, name, cron_expression, execution_type, "
                "group_id) VALUES (1, 'nightly', '0 9 * * *', 'crew', 'g1')"
            )
            await heal._ensure_schedule_columns(conn)
        async with AsyncSession(engine) as session:
            loaded = (await session.execute(select(Schedule))).scalar_one()
            assert (loaded.crew_id, loaded.timezone) == (None, None)
    finally:
        await engine.dispose()


# --- migration ----------------------------------------------------------------


def _migration():
    spec = importlib.util.spec_from_file_location("schedule_tz_crew", MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(conn, fn) -> None:
    with Operations.context(MigrationContext.configure(conn)):
        fn()


def test_migration_revision_follows_the_head():
    module = _migration()
    assert module.revision == "20261017_schedule_timezone_and_crew"
    assert module.down_revision == "20260927_decision_config_drop_group_fk"


def test_migration_adds_the_columns_on_an_existing_sqlite_db():
    module = _migration()
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql(OLD_SCHEMA)
        conn.exec_driver_sql(OLD_ROW)

        _run(conn, module.upgrade)
        assert {"crew_id", "timezone"} <= _columns_sync(conn)
        assert conn.exec_driver_sql(
            "SELECT name, timezone FROM schedule"
        ).fetchall() == [("nightly", None)]

        # Guarded: a second upgrade (or one after the self-heal) is a no-op.
        _run(conn, module.upgrade)

        _run(conn, module.downgrade)
        assert not {"crew_id", "timezone"} & _columns_sync(conn)
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM schedule").scalar() == 1
    engine.dispose()
