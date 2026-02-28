"""Unit tests for ModelConfigRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.repositories.model_config_repository import ModelConfigRepository
from src.models.model_config import ModelConfig


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def repo(mock_session):
    return ModelConfigRepository(mock_session)


class TestFindAll:

    @pytest.mark.asyncio
    async def test_returns_all_configs(self, repo, mock_session):
        configs = [MagicMock(spec=ModelConfig), MagicMock(spec=ModelConfig)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = configs
        mock_session.execute.return_value = mock_result

        result = await repo.find_all()

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.find_all()

        assert result == []


class TestFindByKey:

    @pytest.mark.asyncio
    async def test_returns_config_when_found(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, key="gpt-4")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.find_by_key("gpt-4")

        assert result == config

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.find_by_key("missing")

        assert result is None


class TestFindByKeyAndGroup:

    @pytest.mark.asyncio
    async def test_returns_config_for_group(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.find_by_key_and_group("gpt-4", "group-1")

        assert result == config

    @pytest.mark.asyncio
    async def test_returns_none_when_not_in_group(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.find_by_key_and_group("gpt-4", "other-group")

        assert result is None


class TestFindEnabledModels:

    @pytest.mark.asyncio
    async def test_returns_enabled_models(self, repo, mock_session):
        configs = [MagicMock(spec=ModelConfig, enabled=True)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = configs
        mock_session.execute.return_value = mock_result

        result = await repo.find_enabled_models()

        assert len(result) == 1


class TestToggleEnabled:

    @pytest.mark.asyncio
    async def test_toggle_returns_true_when_found(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, enabled=False)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_enabled("gpt-4", True)

        assert result is True
        assert config.enabled is True

    @pytest.mark.asyncio
    async def test_toggle_returns_false_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_enabled("missing", True)

        assert result is False

    @pytest.mark.asyncio
    async def test_toggle_raises_on_db_error(self, repo, mock_session):
        mock_result = MagicMock()
        config = MagicMock(spec=ModelConfig)
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result
        mock_session.flush.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.toggle_enabled("gpt-4", True)


class TestToggleEnabledInGroup:

    @pytest.mark.asyncio
    async def test_toggle_in_group_returns_true(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, enabled=False)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_enabled_in_group("gpt-4", "group-1", True)

        assert result is True

    @pytest.mark.asyncio
    async def test_toggle_in_group_returns_false_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_enabled_in_group("gpt-4", "group-1", True)

        assert result is False

    @pytest.mark.asyncio
    async def test_toggle_in_group_raises_on_db_error(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, enabled=False)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result
        mock_session.flush.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.toggle_enabled_in_group("gpt-4", "group-1", True)


class TestEnableAllModels:

    @pytest.mark.asyncio
    async def test_enable_all_returns_true(self, repo, mock_session):
        mock_session.execute.return_value = MagicMock()

        result = await repo.enable_all_models()

        assert result is True
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_enable_all_raises_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.enable_all_models()


class TestDisableAllModels:

    @pytest.mark.asyncio
    async def test_disable_all_returns_true(self, repo, mock_session):
        mock_session.execute.return_value = MagicMock()

        result = await repo.disable_all_models()

        assert result is True

    @pytest.mark.asyncio
    async def test_disable_all_raises_on_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.disable_all_models()


class TestUpsertModel:

    @pytest.mark.asyncio
    async def test_updates_existing_model(self, repo, mock_session):
        existing = MagicMock(spec=ModelConfig, key="gpt-4", name="GPT-4")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing
        mock_session.execute.return_value = mock_result

        result = await repo.upsert_model("gpt-4", {"name": "GPT-4 Updated", "provider": "openai"})

        assert result == existing
        assert existing.name == "GPT-4 Updated"

    @pytest.mark.asyncio
    async def test_creates_new_model_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.upsert_model("new-model", {
            "name": "New Model",
            "provider": "openai",
            "temperature": 0.7,
        })

        assert result is not None
        mock_session.add.assert_called_once()


class TestDeleteByKey:

    @pytest.mark.asyncio
    async def test_deletes_existing_model(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, id="cfg-1", key="gpt-4")
        # First call returns the model (find_by_key), second call is the delete
        find_result = MagicMock()
        find_result.scalars.return_value.first.return_value = config
        delete_result = MagicMock(rowcount=1)
        mock_session.execute.side_effect = [find_result, delete_result]

        result = await repo.delete_by_key("gpt-4")

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.delete_by_key("missing")

        assert result is False

    @pytest.mark.asyncio
    async def test_delete_raises_on_db_error(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, id="cfg-1", key="gpt-4")
        find_result = MagicMock()
        find_result.scalars.return_value.first.return_value = config
        mock_session.execute.side_effect = [find_result, Exception("DB error")]

        with pytest.raises(Exception, match="DB error"):
            await repo.delete_by_key("gpt-4")

    @pytest.mark.asyncio
    async def test_upsert_raises_on_db_error(self, repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.upsert_model("bad-key", {"name": "bad"})


class TestFindAllGlobal:

    @pytest.mark.asyncio
    async def test_returns_global_configs(self, repo, mock_session):
        configs = [MagicMock(spec=ModelConfig, group_id=None)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = configs
        mock_session.execute.return_value = mock_result

        result = await repo.find_all_global()

        assert len(result) == 1


class TestFindGlobalByKey:

    @pytest.mark.asyncio
    async def test_returns_global_config(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig, key="gpt-4", group_id=None)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.find_global_by_key("gpt-4")

        assert result == config


class TestToggleGlobalEnabled:

    @pytest.mark.asyncio
    async def test_toggle_global_returns_true(self, repo, mock_session):
        config = MagicMock(spec=ModelConfig)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = config
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_global_enabled("gpt-4", True)

        assert result is True

    @pytest.mark.asyncio
    async def test_toggle_global_returns_false_when_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.toggle_global_enabled("missing", True)

        assert result is False
