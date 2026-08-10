"""The shared-owner elevation + pgvector enable, and their savepoint isolation.

Both statements are ALLOWED to fail (a plain Postgres has no databricks_superuser
role; a non-superuser cannot CREATE EXTENSION). On PostgreSQL a failed statement
aborts the whole transaction unless it is rolled back to a SAVEPOINT — so each
runs in its own begin_nested, and a failure must return False WITHOUT poisoning
the connection for the caller's next statement.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.databricks.lakebase.superuser import (
    SUPERUSER_ROLE,
    enable_pgvector_async,
    enter_superuser_async,
)


def _conn(execute_side_effect=None, nested_supported: bool = True) -> MagicMock:
    conn = MagicMock()
    conn.execute = AsyncMock(side_effect=execute_side_effect)
    if nested_supported:
        sp = AsyncMock()
        sp.__aenter__ = AsyncMock(return_value=sp)
        sp.__aexit__ = AsyncMock(return_value=False)
        conn.begin_nested = MagicMock(return_value=sp)
    else:
        conn.begin_nested = MagicMock(side_effect=NotImplementedError("no savepoints"))
    return conn


@pytest.mark.asyncio
class TestEnterSuperuser:
    async def test_sets_role_and_returns_true_on_success(self):
        conn = _conn()
        assert await enter_superuser_async(conn) is True
        sql = conn.execute.call_args[0][0]
        assert f'SET ROLE "{SUPERUSER_ROLE}"' in str(sql)

    async def test_missing_role_returns_false_not_raises(self):
        """A Postgres without databricks_superuser must degrade, not blow up."""
        conn = _conn(
            execute_side_effect=Exception('role "databricks_superuser" does not exist')
        )
        assert await enter_superuser_async(conn) is False

    async def test_failure_is_rolled_back_to_savepoint(self):
        """The failure path must use begin_nested so the txn stays usable."""
        conn = _conn(execute_side_effect=Exception("nope"))
        await enter_superuser_async(conn)
        conn.begin_nested.assert_called_once()

    async def test_runs_bare_when_no_savepoint_support(self):
        conn = _conn(nested_supported=False)
        assert await enter_superuser_async(conn) is True


@pytest.mark.asyncio
class TestEnablePgvector:
    async def test_creates_extension_and_returns_true(self):
        conn = _conn()
        assert await enable_pgvector_async(conn) is True
        sql = str(conn.execute.call_args[0][0])
        assert "CREATE EXTENSION IF NOT EXISTS vector" in sql

    async def test_denied_returns_false_not_raises(self):
        conn = _conn(
            execute_side_effect=Exception("permission denied to create extension")
        )
        assert await enable_pgvector_async(conn) is False
