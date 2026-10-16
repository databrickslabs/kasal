"""A failing self-heal step rolls back alone, on a real SQLite and a real PostgreSQL.

Deployed apps never run Alembic: ``create_all`` plus the self-heal steps are
what upgrade an existing database, and on the Lakebase boot path both run in ONE
transaction (``db/app_resource.py``). On PostgreSQL a failed statement aborts
that transaction. If a step catches its own error, its SAVEPOINT is released
instead of rolled back, the RELEASE fails, every later SAVEPOINT fails, and the
final COMMIT silently becomes a ROLLBACK, taking ``create_all``'s new tables with
it. Mocks cannot show that, so these tests run the real pass on real databases.

The PostgreSQL cases need ``KASAL_TEST_POSTGRES_URL`` (an asyncpg URL for a role
that may create databases, pgvector available); they skip without it. CI sets it
in the ``migrations`` job of ``.github/workflows/quality.yml``.
"""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable

import pytest
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

import src.db.all_models  # noqa: F401 — register every table on Base.metadata
from src.db.base import Base
from src.db.self_heal import report
from src.db.self_heal.columns import ensure_columns
from src.db.self_heal.dialect import _conn_is_sqlite, _index_exists, _pg_columns
from src.db.self_heal.runner import run_schema_self_heal

_PG_URL = os.environ.get("KASAL_TEST_POSTGRES_URL", "")

#: A table no heal step creates, so only ``create_all`` can bring it back.
_NEW_TABLE = "billing_alerts"
#: A column only a LATER step (``_ensure_modelconfig_columns``) adds back.
_LATER_COLUMN = ("modelconfig", "reasoning_effort")


@pytest.fixture(autouse=True)
def _fresh_heal_reports(monkeypatch):
    """The pass keeps its report process-wide; never leak one to other tests."""
    monkeypatch.setattr(report, "_last_reports", {})


@asynccontextmanager
async def _fresh_postgres() -> AsyncIterator[tuple[AsyncEngine, URL]]:
    """A new database with pgvector, dropped afterwards; skip without a server."""
    if not _PG_URL:
        pytest.skip("KASAL_TEST_POSTGRES_URL is not set; no PostgreSQL to test on")
    admin = create_async_engine(
        _PG_URL, isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    name = f"kasal_heal_{uuid.uuid4().hex[:12]}"
    async with admin.connect() as conn:
        await conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
    url = make_url(_PG_URL).set(database=name)
    pg = create_async_engine(url, poolclass=NullPool)
    try:
        async with pg.begin() as conn:
            await conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS vector")
        yield pg, url
    finally:
        await pg.dispose()
        async with admin.connect() as conn:
            await conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.dispose()


@pytest.fixture(params=["sqlite", "postgresql"])
async def engine(request, tmp_path) -> AsyncIterator[AsyncEngine]:
    if request.param == "sqlite":
        sqlite = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'heal.db'}")
        try:
            yield sqlite
        finally:
            await sqlite.dispose()
        return
    async with _fresh_postgres() as (pg, _url):
        yield pg


@pytest.fixture
async def postgres() -> AsyncIterator[tuple[AsyncEngine, URL]]:
    async with _fresh_postgres() as database:
        yield database


async def _columns(conn: AsyncConnection, table: str) -> set[str]:
    if _conn_is_sqlite(conn):
        rows = (await conn.exec_driver_sql(f"PRAGMA table_info({table})")).fetchall()
        return {row[1] for row in rows}
    return await _pg_columns(conn, table)


async def _an_older_install(engine: AsyncEngine) -> None:
    """The schema as an earlier release left it, with data a data step fixes."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with engine.begin() as conn:
        await conn.exec_driver_sql(f"DROP TABLE {_NEW_TABLE}")
        table, column = _LATER_COLUMN
        await conn.exec_driver_sql(f"ALTER TABLE {table} DROP COLUMN {column}")
        # Core insert, so the model's Python-side defaults fill the NOT NULLs.
        groups = Base.metadata.tables["groups"]
        await conn.execute(
            groups.insert().values(id="g1", name="Personal Workspace - a")
        )


async def _boot(engine: AsyncEngine) -> None:
    """What ``initialize_resource_database`` does: create_all + heal, one transaction."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await run_schema_self_heal(conn)


async def _assert_the_rest_of_the_upgrade_landed(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        assert await _columns(conn, _NEW_TABLE), "create_all's new table was lost"
        table, column = _LATER_COLUMN
        assert column in await _columns(conn, table), "a later column step was lost"
        name = (
            await conn.exec_driver_sql("SELECT name FROM \"groups\" WHERE id = 'g1'")
        ).scalar()
        assert name == "Personal Space - a", "a data step was lost"


def _named(name: str, step: Callable[[AsyncConnection], Awaitable[None]]):
    step.__name__ = name
    return step


async def _agent_columns_with_a_broken_column(conn: AsyncConnection) -> None:
    """The real ``ensure_columns``, given one good and one invalid column.

    The invalid DDL fails on each dialect for its own reason, whatever the
    table holds: SQLite cannot ADD a UNIQUE column, PostgreSQL has no such type.
    """
    await ensure_columns(
        conn,
        "agents",
        [
            ("heal_probe_ok", "TEXT", "TEXT"),
            ("heal_probe_broken", "TEXT UNIQUE", "no_such_type"),
        ],
    )


@pytest.mark.asyncio
class TestAFailingColumnStep:
    async def test_the_other_steps_and_create_alls_tables_survive(self, engine):
        await _an_older_install(engine)
        failing = _named("_ensure_agent_columns", _agent_columns_with_a_broken_column)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("src.db.self_heal.runner._ensure_agent_columns", failing)
            await _boot(engine)

        await _assert_the_rest_of_the_upgrade_landed(engine)

    async def test_the_failed_step_is_rolled_back_whole(self, engine):
        """Its first column applied before the second failed; neither may stay."""
        await _an_older_install(engine)
        failing = _named("_ensure_agent_columns", _agent_columns_with_a_broken_column)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("src.db.self_heal.runner._ensure_agent_columns", failing)
            await _boot(engine)

        async with engine.connect() as conn:
            assert "heal_probe_ok" not in await _columns(conn, "agents")


@pytest.mark.asyncio
class TestTheGapIsReported:
    """Logged at ERROR and kept for /health/db; startup still completes."""

    async def test_the_failed_step_is_named_in_the_log_and_the_summary(
        self, engine, caplog, monkeypatch
    ):
        await _an_older_install(engine)
        failing = _named("_ensure_agent_columns", _agent_columns_with_a_broken_column)
        monkeypatch.setattr("src.db.self_heal.runner._ensure_agent_columns", failing)
        with caplog.at_level(logging.WARNING, logger="src.db.self_heal"):
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                gaps = await run_schema_self_heal(conn)

        assert [gap.step for gap in gaps] == ["_ensure_agent_columns"]
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any("INCOMPLETE" in m and "_ensure_agent_columns" in m for m in errors)
        summary = report.schema_heal_summary()
        assert summary["status"] == "incomplete"
        assert summary["failed_steps"] == ["_ensure_agent_columns"]

    async def test_a_clean_pass_reports_ok(self, engine):
        await _an_older_install(engine)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            gaps = await run_schema_self_heal(conn)

        assert gaps == ()
        assert report.schema_heal_summary()["status"] == "ok"

    async def test_a_migration_targets_heal_does_not_replace_the_report(
        self, engine, monkeypatch
    ):
        """record=False (the migrate paths) logs but keeps the active report."""
        await _an_older_install(engine)
        failing = _named("_ensure_agent_columns", _agent_columns_with_a_broken_column)
        monkeypatch.setattr("src.db.self_heal.runner._ensure_agent_columns", failing)
        async with engine.begin() as conn:
            gaps = await run_schema_self_heal(conn, record=False)

        assert [gap.step for gap in gaps] == ["_ensure_agent_columns"]
        assert report.schema_heal_summary()["status"] == "not_run"


@pytest.mark.asyncio
class TestAFailingDataStep:
    async def test_the_other_steps_and_create_alls_tables_survive(self, engine):
        """The Genie One URL step reads ``mcp_servers.auth_type``; take it away."""
        await _an_older_install(engine)
        async with engine.begin() as conn:
            await conn.exec_driver_sql("ALTER TABLE mcp_servers DROP COLUMN auth_type")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            gaps = await run_schema_self_heal(conn)

        assert [gap.step for gap in gaps] == ["_migrate_genie_one_mcp_urls"]
        await _assert_the_rest_of_the_upgrade_landed(engine)


@asynccontextmanager
async def _roles(admin: AsyncEngine) -> AsyncIterator[tuple[str, str]]:
    """An owner role and a login role with GRANT ALL; both dropped afterwards."""
    suffix = uuid.uuid4().hex[:8]
    owner, app = f"heal_owner_{suffix}", f"heal_app_{suffix}"
    async with admin.begin() as conn:
        await conn.exec_driver_sql(f'CREATE ROLE "{owner}"')
        await conn.exec_driver_sql(f"CREATE ROLE \"{app}\" LOGIN PASSWORD '{suffix}'")
        await conn.exec_driver_sql(
            f'GRANT USAGE, CREATE ON SCHEMA public TO "{owner}", "{app}"'
        )
    try:
        yield owner, app
    finally:
        async with admin.begin() as conn:
            await conn.exec_driver_sql(f'REASSIGN OWNED BY "{owner}" TO CURRENT_USER')
            for role in (owner, app):
                await conn.exec_driver_sql(f'DROP OWNED BY "{role}"')
                await conn.exec_driver_sql(f'DROP ROLE "{role}"')


@pytest.mark.asyncio
class TestOnPostgresOnly:
    async def test_tables_another_role_owns_heal_with_no_gap_when_nothing_is_missing(
        self, postgres
    ):
        """A re-created app, or a DBA-owned schema: the app may read and write
        (GRANT ALL) but not ALTER. PostgreSQL checks ownership before existence,
        so any ALTER / CREATE INDEX ... IF NOT EXISTS fails there even when the
        column or index exists. Every step must read the catalogue first, or a
        healthy install reports INCOMPLETE and a degraded /health/db each boot."""
        admin, url = postgres
        async with _roles(admin) as (owner, app):
            async with admin.begin() as conn:  # an earlier app built and healed it
                await conn.exec_driver_sql(f'SET ROLE "{owner}"')
                await conn.run_sync(Base.metadata.create_all)
                assert await run_schema_self_heal(conn) == ()
            async with admin.begin() as conn:
                for kind in ("TABLES", "SEQUENCES"):
                    await conn.exec_driver_sql(
                        f'GRANT ALL ON ALL {kind} IN SCHEMA public TO "{app}"'
                    )
            password = app.rsplit("_", 1)[1]
            as_app = create_async_engine(
                url.set(username=app, password=password), poolclass=NullPool
            )
            try:
                async with as_app.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                    gaps = await run_schema_self_heal(conn)
            finally:
                await as_app.dispose()

        assert gaps == ()
        assert report.schema_heal_summary()["status"] == "ok"

    async def test_a_vector_table_made_on_this_pass_gets_its_index_on_this_pass(
        self, postgres
    ):
        """The heal-only paths (no create_all first): the pgvector step runs after
        the table steps, so a recreated workflow_recipes is finished in one pass."""
        admin, _url = postgres
        async with admin.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with admin.begin() as conn:
            await conn.exec_driver_sql("DROP TABLE workflow_recipes CASCADE")

        async with admin.begin() as conn:
            gaps = await run_schema_self_heal(conn)

        assert gaps == ()
        async with admin.connect() as conn:
            assert "embedding" in await _pg_columns(conn, "workflow_recipes")
            assert await _index_exists(
                conn, "workflow_recipes", "idx_workflow_recipes_embedding"
            )
