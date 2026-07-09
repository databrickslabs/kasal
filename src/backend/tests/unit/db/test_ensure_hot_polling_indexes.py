"""Unit tests for the hot-polling index self-heal (_ensure_hot_polling_indexes).

create_all only creates indexes for NEW tables, so deployed DBs created before
these indexes existed scan/sort executionhistory + execution_trace on every
poll. Verifies the self-heal creates them on SQLite, is idempotent, and stays
quiet when the tables don't exist yet (fresh DB → create_all handles them).
"""
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import session as sess

EXPECTED = {
    "executionhistory": {
        "idx_executionhistory_group_created",
        "ix_executionhistory_status",
        "ix_executionhistory_created_at",
    },
    "execution_trace": {
        "ix_execution_trace_run_id",
        "ix_execution_trace_created_at",
    },
}


async def _index_names(conn, table: str) -> set:
    rows = (await conn.exec_driver_sql(f"PRAGMA index_list({table})")).fetchall()
    return {r[1] for r in rows}


@pytest.mark.asyncio
async def test_creates_polling_indexes_and_is_idempotent():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE executionhistory "
                "(id INTEGER PRIMARY KEY, group_id TEXT, created_at TIMESTAMP, status TEXT)"
            )
            await conn.exec_driver_sql(
                "CREATE TABLE execution_trace "
                "(id INTEGER PRIMARY KEY, run_id INTEGER, created_at TIMESTAMP)"
            )

            await sess._ensure_hot_polling_indexes(conn)
            for table, expected in EXPECTED.items():
                assert expected <= await _index_names(conn, table)

            # Idempotent: a second run must not raise or duplicate anything.
            await sess._ensure_hot_polling_indexes(conn)
            for table, expected in EXPECTED.items():
                assert expected <= await _index_names(conn, table)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_quiet_when_tables_missing():
    """Fresh DB (create_all builds tables + indexes itself): the self-heal must
    swallow the per-statement failures rather than break startup."""
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await sess._ensure_hot_polling_indexes(conn)  # must not raise
    finally:
        await engine.dispose()
