"""
Unit tests for src/engines/crewai/callbacks/storage_callbacks.py.

Covers:
  - DatabaseStorage.__init__()
  - DatabaseStorage.execute()

All external dependencies (repository, DB) are mocked via AsyncMock.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_repository(record_id: int = 42):
    """Return an async-capable repository mock."""
    repo = MagicMock()
    record = MagicMock()
    record.id = record_id
    repo.create = AsyncMock(return_value=record)
    return repo


def _make_storage(repository=None, task_key: str = "task-1"):
    """Instantiate DatabaseStorage with sensible defaults."""
    from src.engines.crewai.callbacks.storage_callbacks import DatabaseStorage

    if repository is None:
        repository = _make_repository()

    storage = DatabaseStorage(repository=repository, task_key=task_key)
    return storage


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestDatabaseStorageInit:

    def test_stores_repository(self):
        repo = _make_repository()
        storage = _make_storage(repository=repo)
        assert storage.repository is repo

    def test_task_key_stored_from_parent(self):
        storage = _make_storage(task_key="my-task")
        assert storage.task_key == "my-task"

    def test_default_max_retries_from_parent(self):
        storage = _make_storage()
        assert storage.max_retries == 3  # CrewAICallback default

    def test_metadata_initialised_as_empty_dict(self):
        storage = _make_storage()
        assert storage.metadata == {}


# ---------------------------------------------------------------------------
# execute — output conversion
# ---------------------------------------------------------------------------


class TestDatabaseStorageExecuteConversion:

    @pytest.mark.asyncio
    async def test_model_dump_used_when_available(self):
        """Output with model_dump() should use that method."""
        repo = _make_repository()
        storage = _make_storage(repository=repo)

        output = MagicMock()
        output.model_dump.return_value = {"key": "value"}
        del output.dict  # ensure fallback ordering doesn't interfere

        await storage.execute(output)

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["data"] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_dict_method_used_when_model_dump_absent(self):
        """Output with dict() but no model_dump() should use dict()."""
        repo = _make_repository()
        storage = _make_storage(repository=repo)

        output = MagicMock(spec=["dict", "__dict__"])
        output.dict.return_value = {"from_dict": True}

        await storage.execute(output)

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["data"] == {"from_dict": True}

    @pytest.mark.asyncio
    async def test_dunder_dict_used_as_fallback(self):
        """Plain object with __dict__ but no model_dump/dict."""

        class PlainOutput:
            pass

        obj = PlainOutput()
        obj.result = "hello"

        repo = _make_repository()
        storage = _make_storage(repository=repo)

        await storage.execute(obj)

        call_kwargs = repo.create.call_args.kwargs
        assert "result" in call_kwargs["data"]

    @pytest.mark.asyncio
    async def test_string_output_wrapped_in_output_key(self):
        """Plain string should be stored as {'output': '<string>'}."""
        repo = _make_repository()
        storage = _make_storage(repository=repo)

        await storage.execute("plain string output")

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["data"] == {"output": "plain string output"}

    @pytest.mark.asyncio
    async def test_integer_output_wrapped_in_output_key(self):
        """Integer has no model_dump, dict, or __dict__ — should be stringified."""
        repo = _make_repository()
        storage = _make_storage(repository=repo)

        await storage.execute(99)

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["data"] == {"output": "99"}


# ---------------------------------------------------------------------------
# execute — repository interaction
# ---------------------------------------------------------------------------


class TestDatabaseStorageExecuteRepository:

    @pytest.mark.asyncio
    async def test_repository_create_called_once(self):
        repo = _make_repository()
        storage = _make_storage(repository=repo, task_key="t-key")

        await storage.execute("data")

        repo.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_task_key_passed_to_repository(self):
        repo = _make_repository()
        storage = _make_storage(repository=repo, task_key="specific-task")

        await storage.execute("some output")

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["task_key"] == "specific-task"

    @pytest.mark.asyncio
    async def test_metadata_passed_to_repository(self):
        repo = _make_repository()
        storage = _make_storage(repository=repo)
        storage.metadata = {"run_id": "r-1"}

        await storage.execute("output")

        call_kwargs = repo.create.call_args.kwargs
        assert call_kwargs["metadata"] == {"run_id": "r-1"}

    @pytest.mark.asyncio
    async def test_created_at_is_datetime(self):
        repo = _make_repository()
        storage = _make_storage(repository=repo)

        await storage.execute("output")

        call_kwargs = repo.create.call_args.kwargs
        assert isinstance(call_kwargs["created_at"], datetime)

    @pytest.mark.asyncio
    async def test_returns_record_id(self):
        repo = _make_repository(record_id=77)
        storage = _make_storage(repository=repo)

        result = await storage.execute("output")

        assert result == 77

    @pytest.mark.asyncio
    async def test_returns_record_id_zero(self):
        """Edge case: record.id == 0 should still be returned."""
        repo = _make_repository(record_id=0)
        storage = _make_storage(repository=repo)

        result = await storage.execute("output")

        assert result == 0

    @pytest.mark.asyncio
    async def test_repository_exception_propagates(self):
        """If repository.create raises, execute should propagate it."""
        repo = MagicMock()
        repo.create = AsyncMock(side_effect=RuntimeError("DB error"))

        storage = _make_storage(repository=repo)

        with pytest.raises(RuntimeError, match="DB error"):
            await storage.execute("output")


# ---------------------------------------------------------------------------
# execute — logging
# ---------------------------------------------------------------------------


class TestDatabaseStorageLogging:

    @pytest.mark.asyncio
    async def test_logs_info_with_record_id(self, caplog):
        import logging

        repo = _make_repository(record_id=55)
        storage = _make_storage(repository=repo)

        with caplog.at_level(logging.INFO):
            await storage.execute("output")

        assert "55" in caplog.text
