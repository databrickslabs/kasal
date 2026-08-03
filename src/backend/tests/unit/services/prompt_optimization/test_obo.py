"""Tests for OBO auth around the MLflow prompt-registry write.

The behaviors that matter in production:
- with a user token, the SP OAuth env is cleared and DATABRICKS_TOKEN is set,
  so MLflow authenticates as the USER (who usually has UC access) not the app SP;
- the original env is always restored, even on error;
- a UC PERMISSION_DENIED is re-raised as a friendly ValueError carrying the
  exact GRANT to run.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization.gepa import obo


def _set_sp_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret")
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "oauth-m2m")
    monkeypatch.setenv("DATABRICKS_HOST", "https://ws.example.com")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)


def test_obo_env_swaps_to_user_and_clears_sp(monkeypatch):
    _set_sp_env(monkeypatch)
    with obo.obo_databricks_env("user-tok", "https://ws.example.com") as active:
        assert active is True
        # SP creds cleared so DATABRICKS_TOKEN is honored, not shadowed.
        assert "DATABRICKS_CLIENT_ID" not in os.environ
        assert "DATABRICKS_CLIENT_SECRET" not in os.environ
        assert "DATABRICKS_AUTH_TYPE" not in os.environ
        assert os.environ["DATABRICKS_TOKEN"] == "user-tok"
    # Restored after the block.
    assert os.environ["DATABRICKS_CLIENT_ID"] == "app-sp"
    assert os.environ["DATABRICKS_AUTH_TYPE"] == "oauth-m2m"
    assert "DATABRICKS_TOKEN" not in os.environ


def test_obo_env_noop_without_token(monkeypatch):
    _set_sp_env(monkeypatch)
    with obo.obo_databricks_env(None) as active:
        assert active is False
        # Untouched: falls back to the app SP.
        assert os.environ["DATABRICKS_CLIENT_ID"] == "app-sp"


def test_obo_env_restores_on_error(monkeypatch):
    _set_sp_env(monkeypatch)
    with pytest.raises(RuntimeError):
        with obo.obo_databricks_env("user-tok"):
            raise RuntimeError("boom")
    assert os.environ["DATABRICKS_CLIENT_ID"] == "app-sp"
    assert "DATABRICKS_TOKEN" not in os.environ


def test_register_prompt_obo_success_uses_user_token(monkeypatch):
    _set_sp_env(monkeypatch)
    fake_mlflow = MagicMock()
    client = fake_mlflow.MlflowClient.return_value
    client.register_prompt.return_value = MagicMock(uri="prompts:/x/1")

    with patch.dict("sys.modules", {"mlflow": fake_mlflow}):
        result = obo.register_prompt_obo(
            registry_uri="databricks-uc",
            prompt_name="ai_specialist.kasal.kasal_crew_x",
            template="hello",
            user_token="user-tok",
        )
    assert result.uri == "prompts:/x/1"
    client.register_prompt.assert_called_once()


def test_register_prompt_obo_permission_denied_becomes_grant_hint(monkeypatch):
    _set_sp_env(monkeypatch)
    fake_mlflow = MagicMock()
    client = fake_mlflow.MlflowClient.return_value
    client.register_prompt.side_effect = Exception(
        "PERMISSION_DENIED: Permission denied to update prompt in schema kasal."
    )

    with patch.dict("sys.modules", {"mlflow": fake_mlflow}):
        with pytest.raises(ValueError) as ei:
            obo.register_prompt_obo(
                registry_uri="databricks-uc",
                prompt_name="ai_specialist.kasal.kasal_crew_x",
                template="hello",
                user_token="user-tok",
            )
    msg = str(ei.value)
    # Names the resolved catalog + schema and the CREATE MODEL grant.
    assert "ai_specialist.kasal" in msg
    assert "CREATE MODEL" in msg
    assert "USE CATALOG ON CATALOG ai_specialist" in msg
