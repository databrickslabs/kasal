"""Unit tests for knowledge_embedding_session routing.

Knowledge/document embeddings go to the Lakebase memory instance when the
active memory backend is Lakebase, otherwise to the app database.
"""
import pytest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch

from src.services.knowledge_embedding_session import (
    resolve_lakebase_instance,
    knowledge_embedding_session,
    ensure_lakebase_doc_table,
)
from src.schemas.memory_backend import MemoryBackendType


def _cfg(backend_type, instance_name="my-lb", has_lakebase=True):
    lakebase = SimpleNamespace(instance_name=instance_name) if has_lakebase else None
    return SimpleNamespace(backend_type=backend_type, lakebase_config=lakebase)


def _patch_config(config):
    svc = MagicMock()
    svc.get_active_config = AsyncMock(return_value=config)
    return patch("src.services.memory_config_service.MemoryConfigService", return_value=svc)


class TestResolveLakebaseInstance:
    @pytest.mark.asyncio
    async def test_returns_instance_for_lakebase_backend(self):
        with _patch_config(_cfg(MemoryBackendType.LAKEBASE, "my-lb")):
            inst = await resolve_lakebase_instance(MagicMock(), "g1")
        assert inst == "my-lb"

    @pytest.mark.asyncio
    async def test_env_default_when_instance_missing(self, monkeypatch):
        monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "env-lb")
        with _patch_config(_cfg(MemoryBackendType.LAKEBASE, instance_name=None)):
            inst = await resolve_lakebase_instance(MagicMock(), "g1")
        assert inst == "env-lb"

    @pytest.mark.asyncio
    async def test_none_for_default_backend(self):
        with _patch_config(_cfg(MemoryBackendType.DEFAULT)):
            inst = await resolve_lakebase_instance(MagicMock(), "g1")
        assert inst is None

    @pytest.mark.asyncio
    async def test_none_when_no_active_config(self):
        with _patch_config(None):
            inst = await resolve_lakebase_instance(MagicMock(), "g1")
        assert inst is None

    @pytest.mark.asyncio
    async def test_none_on_error(self):
        svc = MagicMock()
        svc.get_active_config = AsyncMock(side_effect=RuntimeError("boom"))
        with patch("src.services.memory_config_service.MemoryConfigService", return_value=svc):
            inst = await resolve_lakebase_instance(MagicMock(), "g1")
        assert inst is None


class TestEnsureLakebaseDocTable:
    @staticmethod
    def _session(existing_columns):
        session = MagicMock()
        select_result = MagicMock()
        select_result.fetchall.return_value = [(c,) for c in existing_columns]
        session.execute = AsyncMock(return_value=select_result)
        return session

    @pytest.mark.asyncio
    async def test_creates_table_complete_when_missing(self):
        session = self._session([])  # table does not exist
        await ensure_lakebase_doc_table(session)
        executed = " ".join(str(c.args[0]) for c in session.execute.call_args_list)
        assert "CREATE TABLE IF NOT EXISTS knowledge_embeddings" in executed
        assert "embedding vector(1024)" in executed
        assert "hnsw" in executed
        # No ALTER needed for a freshly created table.
        assert "ADD COLUMN" not in executed

    @pytest.mark.asyncio
    async def test_noop_when_columns_already_present(self):
        session = self._session(["id", "embedding", "group_id", "file_path", "created_by"])
        await ensure_lakebase_doc_table(session)
        # Only the information_schema probe ran; no CREATE/ALTER.
        assert session.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_alters_existing_table_missing_columns(self):
        session = self._session(["id", "source", "title", "content"])
        await ensure_lakebase_doc_table(session)
        executed = " ".join(str(c.args[0]) for c in session.execute.call_args_list)
        assert "ADD COLUMN IF NOT EXISTS embedding vector(1024)" in executed
        assert "ADD COLUMN IF NOT EXISTS group_id" in executed

    @pytest.mark.asyncio
    async def test_raises_clear_error_when_not_owner(self):
        session = MagicMock()
        select_result = MagicMock()
        select_result.fetchall.return_value = [("id",), ("source",)]  # missing pgvector cols

        async def _execute(stmt):
            s = str(stmt)
            if "information_schema" in s:
                return select_result
            if "ALTER TABLE" in s:
                raise RuntimeError("must be owner of table documentation_embeddings")
            return MagicMock()

        session.execute = AsyncMock(side_effect=_execute)
        with pytest.raises(RuntimeError, match="owner"):
            await ensure_lakebase_doc_table(session)


class TestKnowledgeEmbeddingSession:
    @pytest.mark.asyncio
    async def test_app_session_when_no_lakebase(self):
        app = MagicMock()
        with patch(
            "src.services.knowledge_embedding_session.resolve_lakebase_instance",
            new=AsyncMock(return_value=None),
        ):
            async with knowledge_embedding_session(app, "g1") as (sess, is_lakebase):
                assert sess is app
                assert is_lakebase is False

    @pytest.mark.asyncio
    async def test_lakebase_session_when_active(self):
        app = MagicMock()
        lb = MagicMock()
        lb.execute = AsyncMock()  # used by the SET ROLE assumption

        @asynccontextmanager
        async def fake_get_lakebase_session(**kwargs):
            assert kwargs["instance_name"] == "my-lb"
            yield lb

        with patch(
            "src.services.knowledge_embedding_session.resolve_lakebase_instance",
            new=AsyncMock(return_value="my-lb"),
        ):
            with patch(
                "src.db.lakebase_session.get_lakebase_session",
                new=fake_get_lakebase_session,
            ):
                async with knowledge_embedding_session(app, "g1", "tok") as (sess, is_lakebase):
                    assert sess is lb
                    assert is_lakebase is True
        # SET ROLE was attempted on the Lakebase session.
        executed = " ".join(str(c.args[0]) for c in lb.execute.call_args_list)
        assert "SET ROLE" in executed
