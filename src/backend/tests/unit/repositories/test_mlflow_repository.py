"""Unit tests for MLflowRepository – covers all methods including auto-create."""
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.repositories.mlflow_repository import MLflowRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo():
    """Build an MLflowRepository with mocked collaborators."""
    session = AsyncMock()
    repo = MLflowRepository(session)
    repo.dbx_repo = AsyncMock()
    repo._base_repo = AsyncMock()
    return repo


def _fake_config(**overrides):
    cfg = MagicMock()
    cfg.id = overrides.get("id", 42)
    cfg.mlflow_enabled = overrides.get("mlflow_enabled", False)
    cfg.evaluation_enabled = overrides.get("evaluation_enabled", False)
    return cfg


# ---------------------------------------------------------------------------
# set_enabled
# ---------------------------------------------------------------------------
class TestSetEnabled:
    @pytest.mark.asyncio
    async def test_updates_existing_config(self):
        repo = _make_repo()
        existing = _fake_config(id=7)
        repo.dbx_repo.get_active_config.return_value = existing
        repo._base_repo.update.return_value = existing

        result = await repo.set_enabled(True, group_id="g1")

        repo.dbx_repo.get_active_config.assert_awaited_once_with(group_id="g1")
        repo._base_repo.update.assert_awaited_once_with(7, {"mlflow_enabled": True})
        assert result is True

    @pytest.mark.asyncio
    async def test_auto_creates_config_when_missing(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        created = _fake_config(id=99)
        repo.dbx_repo.create_config.return_value = created
        repo._base_repo.update.return_value = created

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://my-ws.cloud.databricks.com"}):
            result = await repo.set_enabled(True, group_id="g2")

        repo.dbx_repo.create_config.assert_awaited_once()
        call_data = repo.dbx_repo.create_config.call_args[0][0]
        assert call_data["workspace_url"] == "https://my-ws.cloud.databricks.com"
        assert call_data["group_id"] == "g2"
        assert call_data["is_active"] is True
        repo._base_repo.update.assert_awaited_once_with(99, {"mlflow_enabled": True})
        repo.session.commit.assert_awaited_once()
        assert result is True

    @pytest.mark.asyncio
    async def test_auto_creates_config_no_host_env(self):
        """When DATABRICKS_HOST is not set, workspace_url defaults to empty string."""
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        created = _fake_config(id=10)
        repo.dbx_repo.create_config.return_value = created
        repo._base_repo.update.return_value = created

        with patch.dict(os.environ, {}, clear=True):
            result = await repo.set_enabled(False, group_id="g3")

        call_data = repo.dbx_repo.create_config.call_args[0][0]
        assert call_data["workspace_url"] == ""
        assert result is True


# ---------------------------------------------------------------------------
# set_evaluation_enabled
# ---------------------------------------------------------------------------
class TestSetEvaluationEnabled:
    @pytest.mark.asyncio
    async def test_updates_existing_config(self):
        repo = _make_repo()
        existing = _fake_config(id=5)
        repo.dbx_repo.get_active_config.return_value = existing
        repo._base_repo.update.return_value = existing

        result = await repo.set_evaluation_enabled(True, group_id="g1")

        repo._base_repo.update.assert_awaited_once_with(5, {"evaluation_enabled": True})
        assert result is True

    @pytest.mark.asyncio
    async def test_auto_creates_config_when_missing(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        created = _fake_config(id=88)
        repo.dbx_repo.create_config.return_value = created
        repo._base_repo.update.return_value = created

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://ws.databricks.com"}):
            result = await repo.set_evaluation_enabled(True, group_id="g4")

        repo.dbx_repo.create_config.assert_awaited_once()
        repo._base_repo.update.assert_awaited_once_with(88, {"evaluation_enabled": True})
        assert result is True


# ---------------------------------------------------------------------------
# is_enabled / is_evaluation_enabled (read-only, no auto-create)
# ---------------------------------------------------------------------------
class TestReadMethods:
    @pytest.mark.asyncio
    async def test_is_enabled_true(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = _fake_config(mlflow_enabled=True)
        assert await repo.is_enabled(group_id="g") is True

    @pytest.mark.asyncio
    async def test_is_enabled_no_config(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        assert await repo.is_enabled(group_id="g") is False

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_true(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = _fake_config(evaluation_enabled=True)
        assert await repo.is_evaluation_enabled(group_id="g") is True

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_no_config(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        assert await repo.is_evaluation_enabled(group_id="g") is False


# ---------------------------------------------------------------------------
# get_evaluation_judge_model
# ---------------------------------------------------------------------------
class TestGetEvaluationJudgeModel:
    @pytest.mark.asyncio
    async def test_returns_model_when_set(self):
        repo = _make_repo()
        cfg = _fake_config()
        cfg.evaluation_judge_model = "databricks-claude-sonnet-4"
        repo.dbx_repo.get_active_config.return_value = cfg
        assert await repo.get_evaluation_judge_model(group_id="g") == "databricks-claude-sonnet-4"

    @pytest.mark.asyncio
    async def test_returns_none_for_blank(self):
        repo = _make_repo()
        cfg = _fake_config()
        cfg.evaluation_judge_model = "   "
        repo.dbx_repo.get_active_config.return_value = cfg
        assert await repo.get_evaluation_judge_model(group_id="g") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_config(self):
        repo = _make_repo()
        repo.dbx_repo.get_active_config.return_value = None
        assert await repo.get_evaluation_judge_model(group_id="g") is None
