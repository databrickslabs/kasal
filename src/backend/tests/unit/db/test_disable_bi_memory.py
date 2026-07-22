"""Unit test for the bi-specialist crew-memory self-heal
(_disable_bi_specialist_crew_memory).

The BI pipeline crews used to run with memory enabled, which auto-saved the
~174K-char pipeline-config JSON to workspace memory and recalled it into every
later prompt → context-window overflow. Memory is now disabled for these crews,
but the seeder is insert-only so already-seeded DBs keep memory on. This startup
self-heal flips them off. Verifies it disables only the bi-specialist group, is
idempotent, and leaves other groups untouched.
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import session as sess


async def _seed(conn):
    for t in ("crews", "agents"):
        await conn.exec_driver_sql(
            f"CREATE TABLE {t} (id TEXT, group_id TEXT, memory BOOLEAN)"
        )
        await conn.exec_driver_sql(
            f"INSERT INTO {t} VALUES "
            f"('a','bi-specialist',1),('b','bi-specialist',0),('c','other-group',1)"
        )


@pytest.mark.asyncio
async def test_disables_only_bi_specialist_and_is_idempotent():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await _seed(conn)
            fake_settings = MagicMock()
            fake_settings.DATABASE_URI = "sqlite+aiosqlite://"
            with patch.object(sess, "settings", fake_settings):
                await sess._disable_bi_specialist_crew_memory(conn)
                for t in ("crews", "agents"):
                    rows = (await conn.exec_driver_sql(
                        f"SELECT id, group_id, memory FROM {t} ORDER BY id"
                    )).fetchall()
                    mem = {r[0]: r[2] for r in rows}
                    assert mem["a"] == 0 and mem["b"] == 0  # bi-specialist off
                    assert mem["c"] == 1                    # other group untouched

                # idempotent: second run is a no-op, no error
                await sess._disable_bi_specialist_crew_memory(conn)
                rows = (await conn.exec_driver_sql(
                    "SELECT memory FROM crews WHERE group_id='other-group'"
                )).fetchall()
                assert rows[0][0] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_noop_when_tables_missing():
    """Fresh DB (tables not created yet) → self-heal must quietly do nothing."""
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            fake_settings = MagicMock()
            fake_settings.DATABASE_URI = "sqlite+aiosqlite://"
            with patch.object(sess, "settings", fake_settings):
                await sess._disable_bi_specialist_crew_memory(conn)  # must not raise
    finally:
        await engine.dispose()
