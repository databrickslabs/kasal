"""Dropping ``decision_config.group_id -> groups.id``: self-heal and migration.

Personal workspaces have no ``groups`` row, so the key made the decision-model switch
impossible to save there. Existing installs are healed at startup
(``_drop_decision_config_group_fk``); Alembic users get migration
``20260927_decision_config_drop_group_fk``. Both must keep the rows.
"""

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal import tables as heal

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "20260927_decision_config_drop_group_fk.py"
)

OLD_SCHEMA = (
    "CREATE TABLE decision_config ("
    "group_id VARCHAR(100) NOT NULL PRIMARY KEY "
    "REFERENCES groups (id) ON DELETE CASCADE, "
    "enabled BOOLEAN DEFAULT 0 NOT NULL)"
)


def _foreign_keys_on(engine) -> None:
    @event.listens_for(engine, "connect")
    def _on(dbapi_conn, _record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON")


def _seed_old_schema(conn) -> None:
    conn.exec_driver_sql("CREATE TABLE groups (id VARCHAR(100) PRIMARY KEY)")
    conn.exec_driver_sql("INSERT INTO groups (id) VALUES ('team-a')")
    conn.exec_driver_sql(OLD_SCHEMA)
    conn.exec_driver_sql("INSERT INTO decision_config VALUES ('team-a', 1)")


# --- self-heal ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_heal_drops_the_key_on_sqlite_and_keeps_rows():
    engine = create_async_engine("sqlite+aiosqlite://")
    _foreign_keys_on(engine.sync_engine)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_seed_old_schema)

            await heal._drop_decision_config_group_fk(conn)

            fks = (
                await conn.exec_driver_sql("PRAGMA foreign_key_list(decision_config)")
            ).fetchall()
            assert fks == []
            rows = (
                await conn.exec_driver_sql(
                    "SELECT group_id, enabled FROM decision_config"
                )
            ).fetchall()
            assert [tuple(r) for r in rows] == [("team-a", 1)]
            # A personal workspace (no groups row) can now be stored.
            await conn.exec_driver_sql(
                "INSERT INTO decision_config VALUES ('user_dev_localhost', 1)"
            )

            # Idempotent: a second pass leaves the rebuilt table alone.
            await heal._drop_decision_config_group_fk(conn)
            count = (
                await conn.exec_driver_sql("SELECT COUNT(*) FROM decision_config")
            ).scalar()
            assert count == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_self_heal_is_quiet_when_the_table_does_not_exist():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await heal._drop_decision_config_group_fk(conn)
            names = (
                await conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            ).fetchall()
            assert names == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_self_heal_drops_the_named_constraint_on_postgres():
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    catalogue = MagicMock()
    catalogue.fetchall = MagicMock(return_value=[("decision_config_group_id_fkey",)])
    conn.exec_driver_sql = AsyncMock(side_effect=[catalogue, MagicMock()])

    await heal._drop_decision_config_group_fk(conn)

    statements = [c.args[0] for c in conn.exec_driver_sql.await_args_list]
    assert "confrelid = to_regclass('groups')" in statements[0]
    assert statements[1] == (
        'ALTER TABLE decision_config DROP CONSTRAINT "decision_config_group_id_fkey"'
    )


@pytest.mark.asyncio
async def test_self_heal_does_nothing_on_postgres_without_the_key():
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    catalogue = MagicMock()
    catalogue.fetchall = MagicMock(return_value=[])
    conn.exec_driver_sql = AsyncMock(return_value=catalogue)

    await heal._drop_decision_config_group_fk(conn)

    assert conn.exec_driver_sql.await_count == 1


# --- migration ----------------------------------------------------------------


def _migration():
    spec = importlib.util.spec_from_file_location("drop_group_fk", MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(conn, fn) -> None:
    with Operations.context(MigrationContext.configure(conn)):
        fn()


def _group_fks(conn) -> list:
    return [
        fk
        for fk in inspect(conn).get_foreign_keys("decision_config")
        if fk["referred_table"] == "groups"
    ]


def test_migration_revision_follows_the_single_head():
    module = _migration()
    assert module.revision == "20260927_decision_config_drop_group_fk"
    assert module.down_revision == "20260927_mlflow_local_tracking_uri"


def test_migration_upgrade_and_downgrade_on_sqlite():
    module = _migration()
    engine = create_engine("sqlite://")
    _foreign_keys_on(engine)
    with engine.begin() as conn:
        _seed_old_schema(conn)

        _run(conn, module.upgrade)
        assert _group_fks(conn) == []
        pk = inspect(conn).get_pk_constraint("decision_config")
        assert pk["constrained_columns"] == ["group_id"]
        conn.exec_driver_sql(
            "INSERT INTO decision_config VALUES ('user_dev_localhost', 1)"
        )
        # Guarded: running it again is a no-op.
        _run(conn, module.upgrade)

        _run(conn, module.downgrade)
        assert len(_group_fks(conn)) == 1
        rows = conn.exec_driver_sql(
            "SELECT group_id, enabled FROM decision_config"
        ).fetchall()
        # The personal-workspace row cannot survive the restored key.
        assert [tuple(r) for r in rows] == [("team-a", 1)]

        _run(conn, module.upgrade)
        assert _group_fks(conn) == []
        assert (
            conn.exec_driver_sql("SELECT COUNT(*) FROM decision_config").scalar() == 1
        )
    engine.dispose()


def test_migration_is_a_noop_without_the_table():
    module = _migration()
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _run(conn, module.upgrade)
        _run(conn, module.downgrade)
        assert inspect(conn).get_table_names() == []
    engine.dispose()
