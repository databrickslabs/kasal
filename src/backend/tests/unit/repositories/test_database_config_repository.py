"""
Unit tests for DatabaseConfigRepository.

Tests get_by_key, upsert, and delete_by_key operations with mocked AsyncSession.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.repositories.database_config_repository import DatabaseConfigRepository
from src.models.database_config import LakebaseConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    return session


def scalar_result(obj):
    scalars = MagicMock()
    scalars.first.return_value = obj
    result = MagicMock()
    result.scalars.return_value = scalars
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def session():
    return make_session()


@pytest.fixture
def repo(session):
    return DatabaseConfigRepository(LakebaseConfig, session)


@pytest.fixture
def sample_config():
    cfg = MagicMock(spec=LakebaseConfig)
    cfg.key = "lakebase_connection"
    cfg.value = {"host": "example.com", "port": 5432}
    return cfg


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_stores_model_and_session(self, session):
        repo = DatabaseConfigRepository(LakebaseConfig, session)
        assert repo.model is LakebaseConfig
        assert repo.session is session


# ---------------------------------------------------------------------------
# get_by_key
# ---------------------------------------------------------------------------

class TestGetByKey:
    @pytest.mark.asyncio
    async def test_returns_config_when_found(self, repo, session, sample_config):
        session.execute.return_value = scalar_result(sample_config)
        result = await repo.get_by_key("lakebase_connection")
        assert result is sample_config
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        session.execute.return_value = scalar_result(None)
        result = await repo.get_by_key("nonexistent_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_executes_query_for_key(self, repo, session):
        session.execute.return_value = scalar_result(None)
        await repo.get_by_key("some_key")
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_different_keys_produce_independent_calls(self, repo, session):
        session.execute.return_value = scalar_result(None)
        await repo.get_by_key("key1")
        await repo.get_by_key("key2")
        assert session.execute.await_count == 2


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------

class TestUpsert:
    @pytest.mark.asyncio
    async def test_updates_existing_config(self, repo, session, sample_config):
        """When key exists, update its value and return it."""
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            result = await repo.upsert("lakebase_connection", {"host": "new.host"})

        assert result is sample_config
        assert sample_config.value == {"host": "new.host"}
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_creates_new_config_when_not_found(self, repo, session):
        """When key does not exist, create and return new config."""
        new_cfg = MagicMock(spec=LakebaseConfig)

        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get, \
             patch("src.repositories.database_config_repository.LakebaseConfig", return_value=new_cfg) as mock_model:
            # Patch the model class used inside upsert
            repo.model = mock_model
            mock_get.return_value = None
            result = await repo.upsert("new_key", {"setting": "value"})

        assert result is new_cfg
        session.add.assert_called_once_with(new_cfg)
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_upsert_calls_get_by_key_first(self, repo, session, sample_config):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            await repo.upsert("lakebase_connection", {})
        mock_get.assert_awaited_once_with("lakebase_connection")

    @pytest.mark.asyncio
    async def test_upsert_returns_config_type(self, repo, session, sample_config):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            result = await repo.upsert("lakebase_connection", {"x": 1})
        # Returns the existing config object unchanged (except for value update)
        assert result is sample_config

    @pytest.mark.asyncio
    async def test_upsert_sets_correct_value_on_existing(self, repo, session, sample_config):
        new_value = {"host": "updated.host", "port": 9999}
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            await repo.upsert("lakebase_connection", new_value)
        assert sample_config.value == new_value


# ---------------------------------------------------------------------------
# delete_by_key
# ---------------------------------------------------------------------------

class TestDeleteByKey:
    @pytest.mark.asyncio
    async def test_returns_true_when_deleted(self, repo, session, sample_config):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            result = await repo.delete_by_key("lakebase_connection")
        assert result is True
        session.delete.assert_awaited_once_with(sample_config)
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, repo, session):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            result = await repo.delete_by_key("nonexistent_key")
        assert result is False
        session.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flushes_after_delete(self, repo, session, sample_config):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = sample_config
            await repo.delete_by_key("lakebase_connection")
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_calls_get_by_key_before_deleting(self, repo, session):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            await repo.delete_by_key("k")
        mock_get.assert_awaited_once_with("k")

    @pytest.mark.asyncio
    async def test_does_not_flush_when_key_not_found(self, repo, session):
        with patch.object(repo, "get_by_key", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            await repo.delete_by_key("ghost_key")
        session.flush.assert_not_awaited()
