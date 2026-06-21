"""Unit test for the crews.reasoning_config self-heal (_ensure_crew_columns).

create_all never ALTERs an existing table, so deployed DBs created before the
reasoning_config column need this idempotent ALTER on startup. Verifies it adds
the column on SQLite and is safe to run repeatedly.
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import session as sess


@pytest.mark.asyncio
async def test_ensure_crew_columns_adds_reasoning_config_and_is_idempotent():
    engine = create_async_engine("sqlite+aiosqlite://")  # in-memory, single shared conn
    try:
        async with engine.begin() as conn:
            # crews table WITHOUT reasoning_config (pre-migration shape)
            await conn.exec_driver_sql("CREATE TABLE crews (id TEXT, name TEXT)")

            fake_settings = MagicMock()
            fake_settings.DATABASE_URI = "sqlite+aiosqlite://"
            with patch.object(sess, "settings", fake_settings):
                await sess._ensure_crew_columns(conn)
                cols = {r[1] for r in (await conn.exec_driver_sql("PRAGMA table_info(crews)")).fetchall()}
                assert "reasoning_config" in cols

                # idempotent: second run must not raise or duplicate the column
                await sess._ensure_crew_columns(conn)
                cols2 = [r[1] for r in (await conn.exec_driver_sql("PRAGMA table_info(crews)")).fetchall()]
                assert cols2.count("reasoning_config") == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_ensure_crew_columns_noop_when_table_missing():
    """If the crews table doesn't exist yet (fresh DB → create_all handles it),
    the self-heal must quietly do nothing rather than error."""
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            fake_settings = MagicMock()
            fake_settings.DATABASE_URI = "sqlite+aiosqlite://"
            with patch.object(sess, "settings", fake_settings):
                await sess._ensure_crew_columns(conn)  # must not raise
    finally:
        await engine.dispose()
