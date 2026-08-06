"""UI config: the table gets created, and a global default reaches new teamspaces.

Two bugs, both hit by opening the UI Configurator from a teamspace that had never
configured it.

**1. The table did not exist.**

``ui_config`` was left to ``create_all`` — but ``init_db`` SKIPS ``create_all``
entirely once the database holds more than one table ("Tables already exist"). So
on any install created before ui_config shipped, the table simply never appeared
and every request 500'd::

    sqlite3.OperationalError: no such table: ui_config
    [SQL: SELECT ui_config.id, ... WHERE ui_config.group_id = ?]

That is why every other later-than-the-DB table in ``db/session.py`` carries its
own checkfirst-create. ui_config was the one that did not; measured against the
real app.db it was the ONLY model in that position — the other 44 unhealed tables
are original-schema and present on every install.

**2. There was no global default.**

Resolution was an exact ``group_id`` match, so a new teamspace saw nothing an admin
had configured globally and silently fell back to the schema defaults. "Configure
it once for everyone, override per teamspace" was not expressible.

Now two levels, like MCP servers and model configs: the workspace's own row wins,
else the ``group_id IS NULL`` default, else the schema defaults. Writes stay EXACT
— editing the resolved row would let one teamspace overwrite the global default for
everyone.
"""

import sqlite3
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.db.all_models  # noqa: F401  (register every model)
from src.db.base import Base
from src.db.session import _ensure_ui_config_columns
from src.schemas.ui_config import UIConfigUpdate
from src.services.settings.ui import UIConfigService

OTHER = "some_other_space"
MINE = "hitashi_65a74581"


@asynccontextmanager
async def _sessions():
    """A DB with the ui_config table, created the way an install gets it.

    An async-generator FIXTURE consumed by a sync one is what pytest-asyncio
    declines to wire up, so this is an explicit context manager instead.
    """
    path = Path(tempfile.mkdtemp()) / "ui.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
class TestTheTableIsCreated:
    async def test_the_self_heal_creates_it_when_missing(self):
        """THE 500. create_all is skipped on an existing DB, so this is the only
        thing that can make the table on an install that predates it."""
        path = Path(tempfile.mkdtemp()) / "noui.db"
        eng = create_async_engine(f"sqlite+aiosqlite:///{path}")
        try:
            async with eng.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                await conn.exec_driver_sql("DROP TABLE ui_config")

            with sqlite3.connect(path) as con:
                assert not con.execute(
                    "select count(*) from sqlite_master where name='ui_config'"
                ).fetchone()[0]

            async with eng.begin() as conn:
                await _ensure_ui_config_columns(conn)
        finally:
            await eng.dispose()

        with sqlite3.connect(path) as con:
            assert con.execute(
                "select count(*) from sqlite_master where name='ui_config'"
            ).fetchone()[0]
            columns = {r[1] for r in con.execute("PRAGMA table_info(ui_config)")}
        # Including the two the ALTER pass exists for.
        assert {"catalog_json", "style_json", "group_id", "enabled"} <= columns

    async def test_it_is_idempotent(self):
        """Runs on every startup; a second pass must not fail on the existing table."""
        path = Path(tempfile.mkdtemp()) / "idem.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                await _ensure_ui_config_columns(conn)
                await _ensure_ui_config_columns(conn)
        finally:
            await engine.dispose()


@pytest.mark.asyncio
class TestTwoLevelResolution:
    async def test_an_unconfigured_teamspace_gets_the_schema_defaults(self):
        async with _sessions() as sessions:
            async with sessions() as session:
                config = await UIConfigService(session, group_id=MINE).get_config()
            assert config.enabled is True

    async def test_a_global_default_reaches_a_teamspace_with_no_row(self):
        """What the user asked for: configure once, applies everywhere."""
        async with _sessions() as sessions:
            async with sessions() as session:
                await UIConfigService(session, group_id=None).update_config(
                    UIConfigUpdate(
                        enabled=False,
                        catalog_type="full",
                        catalog_json=None,
                        style_json='{"brand":"global"}',
                    )
                )
            async with sessions() as session:
                config = await UIConfigService(session, group_id=MINE).get_config()
            assert config.style_json == '{"brand":"global"}'
            assert config.enabled is False

    async def test_a_teamspace_row_overrides_the_default(self):
        async with _sessions() as sessions:
            async with sessions() as session:
                await UIConfigService(session, group_id=None).update_config(
                    UIConfigUpdate(
                        enabled=False,
                        catalog_type="full",
                        catalog_json=None,
                        style_json='{"brand":"global"}',
                    )
                )
            async with sessions() as session:
                await UIConfigService(session, group_id=MINE).update_config(
                    UIConfigUpdate(
                        enabled=True,
                        catalog_type="minimal",
                        catalog_json=None,
                        style_json='{"brand":"mine"}',
                    )
                )
            async with sessions() as session:
                mine = await UIConfigService(session, group_id=MINE).get_config()
            assert mine.style_json == '{"brand":"mine"}'
            assert mine.enabled is True

    async def test_an_override_does_not_touch_the_global_default(self):
        """The reason writes must be EXACT.

        get_for_group resolves to the global row for a workspace with none of its
        own; if the write path used that, the first teamspace to save would rewrite
        the default for every other one.
        """
        async with _sessions() as sessions:
            async with sessions() as session:
                await UIConfigService(session, group_id=None).update_config(
                    UIConfigUpdate(
                        enabled=False,
                        catalog_type="full",
                        catalog_json=None,
                        style_json='{"brand":"global"}',
                    )
                )
            async with sessions() as session:
                await UIConfigService(session, group_id=MINE).update_config(
                    UIConfigUpdate(
                        enabled=True,
                        catalog_type="minimal",
                        catalog_json=None,
                        style_json='{"brand":"mine"}',
                    )
                )
            async with sessions() as session:
                default = await UIConfigService(session, group_id=None).get_config()
                other = await UIConfigService(session, group_id=OTHER).get_config()

            assert default.style_json == '{"brand":"global"}', "the default was edited"
            assert default.enabled is False
            # And an unrelated teamspace still inherits the untouched default.
            assert other.style_json == '{"brand":"global"}'

    async def test_the_default_itself_never_falls_back(self):
        """group_id=None asks for the default; there is nothing below it."""
        async with _sessions() as sessions:
            async with sessions() as session:
                config = await UIConfigService(session, group_id=None).get_config()
            # Schema defaults, not an error and not another group's row.
            assert config.enabled is True


@pytest.mark.asyncio
class TestTheWritePathUsesTheExactLookup:
    async def test_each_teamspace_gets_its_own_row(self):
        """Two saves from two teamspaces must produce two rows, not one edited twice."""
        async with _sessions() as sessions:
            for group in (MINE, OTHER):
                async with sessions() as session:
                    await UIConfigService(session, group_id=group).update_config(
                        UIConfigUpdate(
                            enabled=True,
                            catalog_type="minimal",
                            catalog_json=None,
                            style_json=f'{{"brand":"{group}"}}',
                        )
                    )
            async with sessions() as session:
                a = await UIConfigService(session, group_id=MINE).get_config()
                b = await UIConfigService(session, group_id=OTHER).get_config()
            assert a.style_json == f'{{"brand":"{MINE}"}}'
            assert b.style_json == f'{{"brand":"{OTHER}"}}'
