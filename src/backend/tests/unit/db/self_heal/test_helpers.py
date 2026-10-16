"""The helpers every column / table / index self-heal step delegates to.

``ensure_columns`` and ``ensure_index`` must read the catalogue BEFORE altering
on both dialects (SQLite has no ADD COLUMN IF NOT EXISTS; on PostgreSQL DDL on a
table this role does not own raises "must be owner" even when the column or
index exists). ``ensure_table`` must create every model it is given, checkfirst.
All three RAISE on failure: the runner's SAVEPOINT has to see the error to roll
the step back (src/db/self_heal/report.py).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal.columns import ensure_columns
from src.db.self_heal.tables import ensure_index, ensure_table


async def _sqlite_columns(conn, table: str) -> list[str]:
    res = await conn.exec_driver_sql(f"PRAGMA table_info({table})")
    return [r[1] for r in res.fetchall()]


def _pg_conn(present: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    catalogue = MagicMock()
    catalogue.fetchall = MagicMock(return_value=[(c,) for c in present])
    conn.exec_driver_sql = AsyncMock(
        side_effect=[catalogue] + [MagicMock() for _ in range(8)]
    )
    return conn


def _statements(conn) -> list[str]:
    return [c.args[0] for c in conn.exec_driver_sql.await_args_list]


@pytest.mark.asyncio
class TestEnsureColumnsOnSqlite:
    async def test_adds_only_the_missing_columns_and_is_idempotent(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        try:
            async with engine.begin() as conn:
                await conn.exec_driver_sql("CREATE TABLE t (id TEXT, a TEXT)")
                spec = [("a", "TEXT", "TEXT"), ("b", "INTEGER", "INTEGER")]
                await ensure_columns(conn, "t", spec)
                assert await _sqlite_columns(conn, "t") == ["id", "a", "b"]
                await ensure_columns(
                    conn, "t", spec
                )  # second run: no duplicate, no error
                assert await _sqlite_columns(conn, "t") == ["id", "a", "b"]
        finally:
            await engine.dispose()

    async def test_a_table_that_does_not_exist_is_left_to_create_all(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        try:
            async with engine.begin() as conn:
                await ensure_columns(conn, "nope", [("a", "TEXT", "TEXT")])  # no raise
        finally:
            await engine.dispose()

    async def test_uses_the_sqlite_ddl_and_no_if_not_exists(self):
        conn = MagicMock()
        conn.engine.dialect.name = "sqlite"
        pragma = MagicMock()
        pragma.fetchall = MagicMock(return_value=[(0, "id")])
        conn.exec_driver_sql = AsyncMock(side_effect=[pragma, MagicMock()])
        await ensure_columns(
            conn, "t", [("flag", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT false")]
        )
        assert _statements(conn) == [
            "PRAGMA table_info(t)",
            "ALTER TABLE t ADD COLUMN flag BOOLEAN DEFAULT 0",
        ]


@pytest.mark.asyncio
class TestEnsureColumnsOnPostgres:
    async def test_reads_the_catalogue_then_alters_only_what_is_missing(self):
        conn = _pg_conn(["id", "a"])
        await ensure_columns(
            conn, "t", [("a", "TEXT", "JSONB"), ("b", "TEXT", "JSONB")]
        )
        stmts = _statements(conn)
        assert "pg_attribute" in stmts[0]
        assert stmts[1:] == ["ALTER TABLE t ADD COLUMN IF NOT EXISTS b JSONB"]

    async def test_no_alter_at_all_when_every_column_is_present(self):
        """No ALTER means no "must be owner" for work that need not happen."""
        conn = _pg_conn(["id", "a", "b"])
        await ensure_columns(conn, "t", [("a", "TEXT", "TEXT"), ("b", "TEXT", "TEXT")])
        assert len(_statements(conn)) == 1  # the catalogue read only

    async def test_an_empty_catalogue_means_the_table_is_not_there_yet(self):
        conn = _pg_conn([])
        await ensure_columns(conn, "t", [("a", "TEXT", "TEXT")])
        assert len(_statements(conn)) == 1

    async def test_a_driver_error_is_raised_to_the_savepoint(self):
        """Swallowing it left the PostgreSQL transaction aborted, so the whole
        pass — and create_all's new tables — were lost at COMMIT."""
        conn = MagicMock()
        conn.engine.dialect.name = "postgresql"
        conn.exec_driver_sql = AsyncMock(side_effect=RuntimeError("must be owner"))
        with pytest.raises(RuntimeError, match="must be owner"):
            await ensure_columns(conn, "t", [("a", "TEXT", "TEXT")])


@pytest.mark.asyncio
class TestEnsureTable:
    async def test_creates_every_model_it_is_given_checkfirst(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        try:
            async with engine.begin() as conn:
                await ensure_table(conn, "src.models.skill", "Skill", "SkillFile")
                await ensure_table(
                    conn, "src.models.skill", "Skill", "SkillFile"
                )  # checkfirst
                res = await conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                names = {r[0] for r in res.fetchall()}
            assert {"skills", "skill_files"} <= names, names
        finally:
            await engine.dispose()

    async def test_hands_one_sync_callable_to_run_sync(self):
        conn = MagicMock()
        conn.run_sync = AsyncMock()
        await ensure_table(conn, "src.models.chat_session", "ChatSession")
        conn.run_sync.assert_awaited_once()

    async def test_a_failure_is_raised_to_the_savepoint(self):
        conn = MagicMock()
        conn.run_sync = AsyncMock(side_effect=Exception("locked"))
        with pytest.raises(Exception, match="locked"):
            await ensure_table(conn, "src.models.chat_session", "ChatSession")

    async def test_an_unknown_model_raises_before_any_ddl(self):
        conn = MagicMock()
        conn.run_sync = AsyncMock()
        with pytest.raises(AttributeError):
            await ensure_table(conn, "src.models.chat_session", "NoSuchModel")
        conn.run_sync.assert_not_awaited()


@pytest.mark.asyncio
class TestEnsureIndex:
    async def test_creates_a_missing_index_once(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        ddl = "CREATE INDEX IF NOT EXISTS ix_t_a ON t (a)"
        try:
            async with engine.begin() as conn:
                await conn.exec_driver_sql("CREATE TABLE t (a TEXT)")
                await ensure_index(conn, "t", "ix_t_a", ddl)
                await ensure_index(conn, "t", "ix_t_a", ddl)
                rows = await conn.exec_driver_sql("PRAGMA index_list(t)")
                assert [r[1] for r in rows.fetchall()] == ["ix_t_a"]
        finally:
            await engine.dispose()

    async def test_an_existing_index_is_never_re_created(self):
        """On PostgreSQL CREATE INDEX IF NOT EXISTS still needs ownership."""
        conn = MagicMock()
        conn.engine.dialect.name = "postgresql"
        found = MagicMock()
        found.fetchone = MagicMock(return_value=(1,))
        conn.exec_driver_sql = AsyncMock(return_value=found)
        await ensure_index(
            conn, "t", "ix_t_a", "CREATE INDEX IF NOT EXISTS ix_t_a ON t (a)"
        )
        statements = _statements(conn)
        assert len(statements) == 1 and "pg_index" in statements[0]
