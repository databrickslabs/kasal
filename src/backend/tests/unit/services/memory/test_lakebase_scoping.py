"""How the Lakebase backend scopes reads, writes and deletes.

The three rules, and why each is what it is:

* **read** → ``group_id``, or ``session_id`` + ``group_id`` when the chat
  "Workspace memory" switch is off. ``crew_id`` is NEVER a read-scoping key: it
  is a hash of crew STRUCTURE that changes every time that structure does (e.g.
  with each chat prompt), so scoping reads by it walls a run off from its own
  history. The retired Databricks Vector Search backend did filter reads by it,
  which is what made chat memory there close to unrecallable across turns.
* **bulk delete** → ``crew_id`` + ``group_id``. A filter-shaped delete names no
  records, so one crew's pruning must not sweep away another's.
* **delete by record ids** → scoped like a read. The ids came from a scoped
  read, and this is what makes consolidation possible for chat at all.

``group_id`` is the tenant boundary and is present in every mode.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.memory.engine import MemoryRecord
from src.services.memory.storage.lakebase import LakebaseStorageBackend


def _backend(workspace_wide: bool = True) -> LakebaseStorageBackend:
    return LakebaseStorageBackend(
        table_name="crew_memory",
        crew_id="group_1_crew_abc",
        group_id="group_1",
        session_id="sess_1",
        embedding_dimension=4,
        workspace_wide=workspace_wide,
    )


def _session_ctx(session):
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _capture_delete(backend, **kwargs):
    """Run adelete against a mocked session and return (sql, params)."""
    session = AsyncMock()
    session.execute = AsyncMock()
    with patch(
        "src.services.memory.storage.lakebase.get_lakebase_session",
        return_value=_session_ctx(session),
    ):
        await backend.adelete(**kwargs)
    return str(session.execute.call_args[0][0]), session.execute.call_args[0][1]


class TestReadScoping:
    def test_workspace_read_is_group_only(self):
        where, params = _backend()._tenant_where()
        assert where == ["group_id = :group_id"]
        assert params == {"group_id": "group_1"}

    def test_session_read_adds_session_id(self):
        where, params = _backend(workspace_wide=False)._tenant_where()
        assert set(where) == {"session_id = :session_id", "group_id = :group_id"}
        assert params == {"session_id": "sess_1", "group_id": "group_1"}

    def test_crew_id_never_narrows_a_read(self):
        for workspace_wide in (True, False):
            where, params = _backend(workspace_wide)._tenant_where()
            assert not any("crew_id" in clause for clause in where)
            assert "crew_id" not in params

    def test_group_id_is_always_present(self):
        for workspace_wide in (True, False):
            where, _ = _backend(workspace_wide)._tenant_where()
            assert "group_id = :group_id" in where


class TestBulkDeleteScoping:
    """A filter-shaped delete is scoped exactly like a read — never by crew."""

    @pytest.mark.asyncio
    async def test_scope_prefix_delete_is_not_crew_scoped(self):
        sql, params = await _capture_delete(_backend(), scope_prefix="/group_1")
        assert "crew_id" not in sql
        assert "crew_id" not in params
        assert "group_id = :group_id" in sql

    @pytest.mark.asyncio
    async def test_older_than_delete_is_not_crew_scoped(self):
        from datetime import datetime, timezone

        sql, _ = await _capture_delete(
            _backend(), older_than=datetime(2026, 1, 1, tzinfo=timezone.utc)
        )
        assert "crew_id" not in sql
        assert "created_at < :older_than" in sql

    @pytest.mark.asyncio
    async def test_bulk_delete_honours_session_scope(self):
        """With workspace memory off, a reset must not reach past the session."""
        sql, params = await _capture_delete(
            _backend(workspace_wide=False), scope_prefix="/group_1"
        )
        assert "session_id = :session_id" in sql
        assert params["session_id"] == "sess_1"

    @pytest.mark.asyncio
    async def test_reset_clears_the_whole_workspace_scope(self):
        """``reset`` used to be crew-scoped, so clearing a workspace's memory
        removed only what the current crew happened to have written."""
        sql, params = await _capture_delete(_backend(), scope_prefix="/group_1")
        assert params["group_id"] == "group_1"
        assert "crew_id" not in sql


class TestDeleteByRecordIds:
    """Named ids came from a scoped read — this is what unblocks chat dedupe.

    ``crew_id`` changes with every chat prompt, so under crew scoping a turn's
    consolidation could only ever see that ONE turn's write and could never
    dedupe against earlier turns.
    """

    @pytest.mark.asyncio
    async def test_id_delete_is_not_crew_scoped(self):
        sql, params = await _capture_delete(_backend(), record_ids=["rec-1", "rec-2"])
        assert "crew_id" not in sql
        assert "id = ANY(:record_ids)" in sql
        assert "group_id = :group_id" in sql  # tenancy still enforced
        assert params["record_ids"] == ["rec-1", "rec-2"]

    @pytest.mark.asyncio
    async def test_id_delete_honours_session_scope(self):
        sql, params = await _capture_delete(
            _backend(workspace_wide=False), record_ids=["rec-1"]
        )
        assert "session_id = :session_id" in sql
        assert params["session_id"] == "sess_1"


class TestWriteTagging:
    """crew_id stays on the row — it is provenance, just not a read filter."""

    @pytest.mark.asyncio
    async def test_save_stamps_crew_group_and_session(self):
        backend = _backend()
        session = AsyncMock()
        session.execute = AsyncMock()
        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            return_value=_session_ctx(session),
        ):
            await backend.asave(
                [MemoryRecord(content="fact", embedding=[0.1, 0.2, 0.3, 0.4])]
            )
        params = session.execute.call_args[0][1]
        assert params["crew_id"] == "group_1_crew_abc"
        assert params["group_id"] == "group_1"
        assert params["session_id"] == "sess_1"
