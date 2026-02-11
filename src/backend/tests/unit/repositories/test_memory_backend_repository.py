"""Unit tests for MemoryBackendRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.repositories.memory_backend_repository import MemoryBackendRepository
from src.models.memory_backend import MemoryBackend, MemoryBackendTypeEnum


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.delete = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def repo(mock_session):
    return MemoryBackendRepository(mock_session)


class TestGetByGroupId:

    @pytest.mark.asyncio
    async def test_returns_backends_for_group(self, repo, mock_session):
        backends = [MagicMock(spec=MemoryBackend), MagicMock(spec=MemoryBackend)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = backends
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_group_id("group-1")

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_list_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.get_by_group_id("group-1")

        assert result == []


class TestGetDefaultByGroupId:

    @pytest.mark.asyncio
    async def test_returns_default_backend(self, repo, mock_session):
        backend = MagicMock(spec=MemoryBackend, is_default=True)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = backend
        mock_session.execute.return_value = mock_result

        result = await repo.get_default_by_group_id("group-1")

        assert result == backend

    @pytest.mark.asyncio
    async def test_returns_none_when_no_default(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.get_default_by_group_id("group-1")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.get_default_by_group_id("group-1")

        assert result is None


class TestGetByName:

    @pytest.mark.asyncio
    async def test_returns_backend_by_name(self, repo, mock_session):
        backend = MagicMock(spec=MemoryBackend, name="my-backend")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = backend
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_name("group-1", "my-backend")

        assert result == backend

    @pytest.mark.asyncio
    async def test_returns_none_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.get_by_name("group-1", "missing")

        assert result is None


class TestSetDefault:

    @pytest.mark.asyncio
    async def test_sets_new_default(self, repo, mock_session):
        # Mock existing defaults
        existing = MagicMock(spec=MemoryBackend, is_default=True)
        defaults_result = MagicMock()
        defaults_result.scalars.return_value.all.return_value = [existing]

        # Mock the new backend
        new_backend = MagicMock(spec=MemoryBackend, group_id="group-1", is_default=False)

        # Mock get() call for the target backend
        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = new_backend

        mock_session.execute.side_effect = [defaults_result, get_result]

        result = await repo.set_default("group-1", "backend-2")

        assert result is True
        assert existing.is_default is False
        assert new_backend.is_default is True

    @pytest.mark.asyncio
    async def test_returns_false_when_backend_not_found(self, repo, mock_session):
        defaults_result = MagicMock()
        defaults_result.scalars.return_value.all.return_value = []

        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = None

        mock_session.execute.side_effect = [defaults_result, get_result]

        result = await repo.set_default("group-1", "missing")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_wrong_group(self, repo, mock_session):
        defaults_result = MagicMock()
        defaults_result.scalars.return_value.all.return_value = []

        backend = MagicMock(spec=MemoryBackend, group_id="other-group")
        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = backend

        mock_session.execute.side_effect = [defaults_result, get_result]

        result = await repo.set_default("group-1", "backend-1")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.set_default("group-1", "backend-1")

        assert result is False


class TestGetByType:

    @pytest.mark.asyncio
    async def test_returns_backends_by_type(self, repo, mock_session):
        backends = [MagicMock(spec=MemoryBackend)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = backends
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_type("group-1", MemoryBackendTypeEnum.DATABRICKS)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.get_by_type("group-1", MemoryBackendTypeEnum.DATABRICKS)

        assert result == []


class TestGetAll:

    @pytest.mark.asyncio
    async def test_returns_all_backends(self, repo, mock_session):
        backends = [MagicMock(spec=MemoryBackend)] * 3
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = backends
        mock_session.execute.return_value = mock_result

        result = await repo.get_all()

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.get_all()

        assert result == []


class TestDeleteAllByGroupId:

    @pytest.mark.asyncio
    async def test_deletes_all_for_group(self, repo, mock_session):
        backends = [MagicMock(spec=MemoryBackend), MagicMock(spec=MemoryBackend)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = backends
        mock_session.execute.return_value = mock_result

        result = await repo.delete_all_by_group_id("group-1")

        assert result == 2
        assert mock_session.delete.call_count == 2
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_backends(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.delete_all_by_group_id("group-1")

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        result = await repo.delete_all_by_group_id("group-1")

        assert result == 0
