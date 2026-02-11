"""Unit tests for UserRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.repositories.user_repository import UserRepository
from src.models.user import User


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def repo(mock_session):
    return UserRepository(User, mock_session)


class TestUserRepositoryGetByEmail:

    @pytest.mark.asyncio
    async def test_returns_user_when_found(self, repo, mock_session):
        user = MagicMock(spec=User)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = user
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_email("test@example.com")

        assert result == user
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_email("missing@example.com")

        assert result is None


class TestUserRepositoryGetByUsername:

    @pytest.mark.asyncio
    async def test_returns_user_when_found(self, repo, mock_session):
        user = MagicMock(spec=User)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = user
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_username("testuser")

        assert result == user

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_username("missing")

        assert result is None


class TestUserRepositoryUpdateLastLogin:

    @pytest.mark.asyncio
    async def test_executes_update_query(self, repo, mock_session):
        mock_session.execute.return_value = MagicMock()

        await repo.update_last_login("user-123")

        mock_session.execute.assert_called_once()


class TestUserRepositorySearchUsers:

    @pytest.mark.asyncio
    async def test_returns_matching_users(self, repo, mock_session):
        users = [MagicMock(spec=User), MagicMock(spec=User)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = users
        mock_session.execute.return_value = mock_result

        result = await repo.search_users("test")

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_match(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.search_users("nonexistent")

        assert result == []

    @pytest.mark.asyncio
    async def test_respects_limit_parameter(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        await repo.search_users("test", limit=5)

        mock_session.execute.assert_called_once()


class TestUserRepositoryCount:

    @pytest.mark.asyncio
    async def test_returns_count(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalar.return_value = 42
        mock_session.execute.return_value = mock_result

        result = await repo.count()

        assert result == 42

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_users(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalar.return_value = 0
        mock_session.execute.return_value = mock_result

        result = await repo.count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_scalar_is_none(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.count()

        assert result == 0
