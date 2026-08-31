"""group_users.allow_agent_builder / allow_flow_builder self-heal.

The columns shipped with an Alembic migration only. Alembic never runs at
startup and create_all never ALTERs an existing table, so every database
older than the columns failed to load a membership. This is the step that
was missing, and the guard that the model and the heal stay in step.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal.columns import _ensure_group_users_columns
from src.db.self_heal.runner import run_schema_self_heal
from src.models.group import GroupUser

#: The table as a database created before commit 1e687ed5 has it.
_LEGACY_GROUP_USERS = (
    "CREATE TABLE group_users ("
    "id VARCHAR(100) PRIMARY KEY, group_id VARCHAR(100) NOT NULL, "
    "user_id VARCHAR(255) NOT NULL, role VARCHAR(50) NOT NULL, "
    "status VARCHAR(50) NOT NULL, joined_at DATETIME, auto_created BOOLEAN, "
    "created_at DATETIME, updated_at DATETIME)"
)


async def _columns(conn) -> set[str]:
    res = await conn.exec_driver_sql("PRAGMA table_info(group_users)")
    return {r[1] for r in res.fetchall()}


@pytest.mark.asyncio
class TestOnSqlite:
    async def test_a_legacy_table_gains_every_mapped_column(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        try:
            async with engine.begin() as conn:
                await conn.exec_driver_sql(_LEGACY_GROUP_USERS)
                await _ensure_group_users_columns(conn)
                cols = await _columns(conn)
                assert {"allow_agent_builder", "allow_flow_builder"} <= cols
                # The guard: whatever the model maps, the heal must provide.
                mapped = {c.name for c in GroupUser.__table__.columns}
                assert mapped <= cols, mapped - cols
                await _ensure_group_users_columns(conn)  # idempotent
                assert await _columns(conn) == cols
        finally:
            await engine.dispose()

    async def test_no_table_yet_is_left_to_create_all(self):
        engine = create_async_engine("sqlite+aiosqlite://")
        try:
            async with engine.begin() as conn:
                await _ensure_group_users_columns(conn)  # must not raise
        finally:
            await engine.dispose()


@pytest.mark.asyncio
class TestOnPostgres:
    def _conn(self, present):
        conn = MagicMock()
        conn.engine.dialect.name = "postgresql"
        catalogue = MagicMock()
        catalogue.fetchall = MagicMock(return_value=[(c,) for c in present])
        conn.exec_driver_sql = AsyncMock(
            side_effect=[catalogue, MagicMock(), MagicMock()]
        )
        return conn

    async def test_catalogue_first_then_add_column_if_not_exists(self):
        conn = self._conn(["id", "group_id", "user_id", "role"])
        await _ensure_group_users_columns(conn)
        stmts = [c.args[0] for c in conn.exec_driver_sql.await_args_list]
        assert "information_schema.columns" in stmts[0]
        assert stmts[1:] == [
            "ALTER TABLE group_users ADD COLUMN IF NOT EXISTS allow_agent_builder BOOLEAN",
            "ALTER TABLE group_users ADD COLUMN IF NOT EXISTS allow_flow_builder BOOLEAN",
        ]
        assert not any("PRAGMA" in s for s in stmts)

    async def test_nothing_is_altered_when_both_are_present(self):
        conn = self._conn(["id", "allow_agent_builder", "allow_flow_builder"])
        await _ensure_group_users_columns(conn)
        assert conn.exec_driver_sql.await_count == 1


@pytest.mark.asyncio
class TestItIsPartOfThePass:
    async def test_run_schema_self_heal_invokes_it(self):
        conn = MagicMock()
        conn.engine.dialect.name = "sqlite"
        conn.exec_driver_sql = AsyncMock(return_value=MagicMock())
        conn.run_sync = AsyncMock()
        ran = []

        async def recorder(_c):
            ran.append("group_users")

        with patch("src.db.self_heal.runner._ensure_group_users_columns", new=recorder):
            await run_schema_self_heal(conn)
        assert ran == ["group_users"]
