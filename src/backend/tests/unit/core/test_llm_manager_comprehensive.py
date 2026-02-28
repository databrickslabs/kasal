"""
Comprehensive unit tests for LLMManager.

Tests cover:
- Module-level constants
- _get_group_id_from_context (success, no context, exception, required vs optional)
- completion (success, failure, threading)
- configure_crewai_llm (all provider branches: deepseek, openai, anthropic, ollama,
  databricks standard, databricks gpt-5, databricks codex, gemini, fallback)
- get_llm (delegates to configure_crewai_llm with UserContext)
- get_embedding circuit breaker
"""

import time as _time
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import Dict, Any, List, Optional
import os
import logging

from src.core.llm_manager import (
    LLMManager,
    log_file_path,
    log_dir,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


class TestModuleConstants:
    """Test module-level constants are properly defined."""

    def test_log_dir_is_string(self):
        assert isinstance(log_dir, str)

    def test_log_file_path_ends_with_llm_log(self):
        assert log_file_path.endswith("llm.log")


# ---------------------------------------------------------------------------
# Class attributes
# ---------------------------------------------------------------------------


class TestClassAttributes:
    """Test LLMManager class attributes and circuit breaker config."""

    def test_embedding_failure_tracking_attributes(self):
        assert isinstance(LLMManager._embedding_failures, dict)
        assert isinstance(LLMManager._embedding_failure_threshold, int)
        assert isinstance(LLMManager._circuit_reset_time, int)

    def test_circuit_breaker_defaults(self):
        assert LLMManager._embedding_failure_threshold == 3
        assert LLMManager._circuit_reset_time == 300

    def test_embedding_failures_manipulation(self):
        LLMManager._embedding_failures.clear()
        LLMManager._embedding_failures["test"] = {"count": 1, "last_failure": _time.time()}
        assert LLMManager._embedding_failures["test"]["count"] == 1
        LLMManager._embedding_failures.clear()

    def test_static_methods_exist(self):
        for name in ("_get_group_id_from_context", "completion", "configure_crewai_llm", "get_llm", "get_embedding"):
            assert callable(getattr(LLMManager, name))


# ---------------------------------------------------------------------------
# _get_group_id_from_context
# ---------------------------------------------------------------------------


class TestGetGroupIdFromContext:
    """Test _get_group_id_from_context method."""

    @patch("src.core.llm_manager.LLMManager._get_group_id_from_context.__wrapped__" if False else "builtins.__import__", side_effect=lambda *a, **kw: __import__(*a, **kw))
    def _helper(self, _):
        pass

    def test_returns_group_id_when_available(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = "group-abc"
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx):
            result = LLMManager._get_group_id_from_context(required=True)
        assert result == "group-abc"

    def test_raises_when_required_and_no_group_id(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = None
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx):
            with pytest.raises(ValueError, match="group_id is required"):
                LLMManager._get_group_id_from_context(required=True)

    def test_returns_none_when_not_required_and_no_group_id(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = None
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx):
            result = LLMManager._get_group_id_from_context(required=False)
        assert result is None

    def test_returns_none_when_context_is_none(self):
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=None):
            result = LLMManager._get_group_id_from_context(required=False)
        assert result is None

    def test_handles_exception_and_raises_when_required(self):
        with patch("src.utils.user_context.UserContext.get_group_context", side_effect=RuntimeError("boom")):
            with pytest.raises(ValueError, match="group_id is required"):
                LLMManager._get_group_id_from_context(required=True)

    def test_handles_exception_and_returns_none_when_not_required(self):
        with patch("src.utils.user_context.UserContext.get_group_context", side_effect=RuntimeError("boom")):
            result = LLMManager._get_group_id_from_context(required=False)
        assert result is None

    def test_returns_none_when_context_has_empty_string_group_id(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = ""
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx):
            result = LLMManager._get_group_id_from_context(required=False)
        assert result is None


# ---------------------------------------------------------------------------
# completion
# ---------------------------------------------------------------------------


class TestCompletion:
    """Test LLMManager.completion async method."""

    @pytest.mark.asyncio
    async def test_completion_success(self):
        mock_llm = MagicMock()
        mock_llm.call.return_value = "response text"

        with (
            patch.object(LLMManager, "_get_group_id_from_context", return_value="group-1"),
            patch.object(LLMManager, "configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm),
            patch("asyncio.to_thread", new_callable=AsyncMock, return_value="response text"),
        ):
            result = await LLMManager.completion(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
            )

        assert result == "response text"

    @pytest.mark.asyncio
    async def test_completion_raises_on_llm_error(self):
        mock_llm = MagicMock()

        with (
            patch.object(LLMManager, "_get_group_id_from_context", return_value="group-1"),
            patch.object(LLMManager, "configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm),
            patch("asyncio.to_thread", new_callable=AsyncMock, side_effect=RuntimeError("LLM error")),
        ):
            with pytest.raises(RuntimeError, match="LLM error"):
                await LLMManager.completion(
                    messages=[{"role": "user", "content": "hello"}],
                    model="test-model",
                )

    @pytest.mark.asyncio
    async def test_completion_sets_max_tokens(self):
        mock_llm = MagicMock()

        with (
            patch.object(LLMManager, "_get_group_id_from_context", return_value="group-1"),
            patch.object(LLMManager, "configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm),
            patch("asyncio.to_thread", new_callable=AsyncMock, return_value="ok"),
        ):
            await LLMManager.completion(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
                max_tokens=8000,
            )

        assert mock_llm.max_tokens == 8000


# ---------------------------------------------------------------------------
# configure_crewai_llm — helper
# ---------------------------------------------------------------------------


def _make_model_config(name, provider, context_window=128000, max_output_tokens=4096, extra=None):
    """Build a model config dict matching what ModelConfigService returns."""
    config = {
        "name": name,
        "provider": provider,
        "temperature": 0.7,
        "context_window": context_window,
        "max_output_tokens": max_output_tokens,
    }
    if extra:
        config.update(extra)
    return config


def _patch_session_and_config(model_config_dict):
    """Create patches for request_scoped_session and ModelConfigService.

    request_scoped_session is imported inside configure_crewai_llm via
    ``from src.db.session import request_scoped_session``, so we patch
    at the original module location.
    """
    mock_session = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_session
    mock_ctx.__aexit__.return_value = None

    mock_service = AsyncMock()
    mock_service.get_model_config.return_value = model_config_dict

    return (
        patch("src.db.session.request_scoped_session", return_value=mock_ctx),
        patch("src.core.llm_manager.ModelConfigService", return_value=mock_service),
    )


# ---------------------------------------------------------------------------
# configure_crewai_llm
# ---------------------------------------------------------------------------


class TestConfigureCrewaiLlm:
    """Test configure_crewai_llm for each provider branch."""

    @pytest.mark.asyncio
    async def test_raises_without_group_id(self):
        with pytest.raises(ValueError, match="group_id is REQUIRED"):
            await LLMManager.configure_crewai_llm("test-model", "", None)

    @pytest.mark.asyncio
    async def test_raises_when_model_not_found(self):
        p_session, p_service = _patch_session_and_config(None)
        with p_session, p_service:
            with pytest.raises(ValueError, match="not found in the database"):
                await LLMManager.configure_crewai_llm("missing-model", "group-1", None)

    @pytest.mark.asyncio
    async def test_deepseek_provider(self):
        config = _make_model_config("deepseek-chat", "deepseek")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="ds-key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            result = await LLMManager.configure_crewai_llm("deepseek-chat", "group-1", 0.5)
            MockLLM.assert_called_once()
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["model"] == "deepseek/deepseek-chat"
            assert call_kwargs["api_key"] == "ds-key"
            assert call_kwargs["temperature"] == 0.5

    @pytest.mark.asyncio
    async def test_openai_provider(self):
        config = _make_model_config("gpt-4o", "openai")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="sk-key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            result = await LLMManager.configure_crewai_llm("gpt-4o", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["model"] == "gpt-4o"
            assert call_kwargs["api_key"] == "sk-key"

    @pytest.mark.asyncio
    async def test_openai_gpt5_drop_params(self):
        config = _make_model_config("gpt-5", "openai", max_output_tokens=128000)
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="sk-key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            await LLMManager.configure_crewai_llm("gpt-5", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["timeout"] == 300
            assert "additional_drop_params" in call_kwargs
            assert "max_completion_tokens" in call_kwargs

    @pytest.mark.asyncio
    async def test_anthropic_provider(self):
        config = _make_model_config("claude-3-5-sonnet-20241022", "anthropic")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="ant-key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            await LLMManager.configure_crewai_llm("claude-3-5-sonnet-20241022", "group-1", 0.3)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["model"] == "anthropic/claude-3-5-sonnet-20241022"

    @pytest.mark.asyncio
    async def test_ollama_provider_normalizes_hyphen(self):
        config = _make_model_config("llama3.2-latest", "ollama")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            await LLMManager.configure_crewai_llm("llama3.2-latest", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            # Hyphens should be replaced with colons for Ollama
            assert call_kwargs["model"] == "ollama/llama3.2:latest"

    @pytest.mark.asyncio
    async def test_databricks_standard_model(self):
        config = _make_model_config("databricks-llama-4-maverick", "databricks", max_output_tokens=8000)
        p_session, p_service = _patch_session_and_config(config)

        mock_auth = MagicMock()
        mock_auth.token = "db-token"
        mock_auth.workspace_url = "https://example.com"
        mock_auth.auth_method = "PAT"

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="user-tok"),
            patch("src.core.llm_manager.DatabricksURLUtils.construct_serving_endpoints_url", return_value="https://example.com/serving-endpoints"),
            patch("src.core.llm_manager.DatabricksRetryLLM") as MockRetryLLM,
        ):
            await LLMManager.configure_crewai_llm("databricks-llama-4-maverick", "group-1", 0.7)
            MockRetryLLM.assert_called_once()
            call_kwargs = MockRetryLLM.call_args[1]
            assert call_kwargs["model"] == "databricks/databricks-llama-4-maverick"
            assert call_kwargs["api_key"] == "db-token"
            assert call_kwargs["timeout"] == 297  # non-GPT-5

    @pytest.mark.asyncio
    async def test_databricks_gpt5_model(self):
        config = _make_model_config("databricks-gpt-5", "databricks", max_output_tokens=128000)
        p_session, p_service = _patch_session_and_config(config)

        mock_auth = MagicMock()
        mock_auth.token = "db-token"
        mock_auth.workspace_url = "https://example.com"
        mock_auth.auth_method = "PAT"

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="user-tok"),
            patch("src.core.llm_manager.DatabricksURLUtils.construct_serving_endpoints_url", return_value="https://example.com/serving-endpoints"),
            patch("src.core.llm_manager.DatabricksRetryLLM") as MockRetryLLM,
        ):
            await LLMManager.configure_crewai_llm("databricks-gpt-5", "group-1", None)
            call_kwargs = MockRetryLLM.call_args[1]
            assert call_kwargs["timeout"] == 300  # GPT-5 gets 300s
            assert "additional_drop_params" in call_kwargs
            assert "max_completion_tokens" in call_kwargs
            # Temperature should NOT be set for GPT-5 (even if passed)
            assert "temperature" not in call_kwargs

    @pytest.mark.asyncio
    async def test_databricks_codex_model(self):
        """gpt-5-3-codex should return DatabricksCodexCompletion."""
        config = _make_model_config("databricks-gpt-5-3-codex", "databricks", max_output_tokens=128000)
        p_session, p_service = _patch_session_and_config(config)

        mock_auth = MagicMock()
        mock_auth.token = "db-token"
        mock_auth.workspace_url = "https://example.com"
        mock_auth.auth_method = "PAT"

        mock_codex_cls = MagicMock()
        mock_codex_instance = MagicMock()
        mock_codex_cls.return_value = mock_codex_instance

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="user-tok"),
            patch("src.core.llm_manager.DatabricksURLUtils.construct_serving_endpoints_url", return_value="https://example.com/serving-endpoints"),
            patch("src.core.llm_handlers.databricks_codex_handler.DatabricksCodexCompletion", mock_codex_cls),
        ):
            result = await LLMManager.configure_crewai_llm("databricks-gpt-5-3-codex", "group-1", None)
            mock_codex_cls.assert_called_once()
            call_kwargs = mock_codex_cls.call_args[1]
            assert call_kwargs["model"] == "databricks-gpt-5-3-codex"
            assert call_kwargs["timeout"] == 300

    @pytest.mark.asyncio
    async def test_databricks_no_auth_available(self):
        """When auth returns None, api_key should be None."""
        config = _make_model_config("databricks-llama-4-maverick", "databricks")
        p_session, p_service = _patch_session_and_config(config)

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None),
            patch("src.utils.user_context.UserContext.get_user_token", return_value=None),
            patch("src.core.llm_manager.DatabricksRetryLLM") as MockRetryLLM,
        ):
            await LLMManager.configure_crewai_llm("databricks-llama-4-maverick", "group-1", None)
            call_kwargs = MockRetryLLM.call_args[1]
            assert "api_key" not in call_kwargs

    @pytest.mark.asyncio
    async def test_databricks_import_error_raises(self):
        """ImportError for databricks_auth should re-raise."""
        config = _make_model_config("databricks-llama-4-maverick", "databricks")
        p_session, p_service = _patch_session_and_config(config)

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", side_effect=ImportError("no module")),
            patch("src.utils.user_context.UserContext.get_user_token", return_value=None),
        ):
            with pytest.raises(ImportError, match="databricks_auth module is required"):
                await LLMManager.configure_crewai_llm("databricks-llama-4-maverick", "group-1", None)

    @pytest.mark.asyncio
    async def test_gemini_provider(self):
        config = _make_model_config("gemini-2.0-flash", "gemini")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="gem-key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
            patch.dict(os.environ, {}, clear=False),
        ):
            await LLMManager.configure_crewai_llm("gemini-2.0-flash", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["model"] == "gemini/gemini-2.0-flash"

    @pytest.mark.asyncio
    async def test_gemini_no_api_key_sets_env(self):
        config = _make_model_config("gemini-2.0-flash", "gemini")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value=None),
            patch("src.core.llm_manager.LLM") as MockLLM,
            patch.dict(os.environ, {}, clear=False),
        ):
            await LLMManager.configure_crewai_llm("gemini-2.0-flash", "group-1", None)
            # Should still create LLM without api_key
            assert MockLLM.called

    @pytest.mark.asyncio
    async def test_fallback_provider(self):
        config = _make_model_config("custom-model", "custom_provider")
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            await LLMManager.configure_crewai_llm("custom-model", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs["model"] == "custom_provider/custom-model"

    @pytest.mark.asyncio
    async def test_non_gpt5_databricks_gets_temperature(self):
        config = _make_model_config("databricks-llama-4-maverick", "databricks")
        p_session, p_service = _patch_session_and_config(config)

        mock_auth = MagicMock()
        mock_auth.token = "db-token"
        mock_auth.workspace_url = "https://example.com"
        mock_auth.auth_method = "PAT"

        with (
            p_session,
            p_service,
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"),
            patch("src.core.llm_manager.DatabricksURLUtils.construct_serving_endpoints_url", return_value="https://example.com/serving-endpoints"),
            patch("src.core.llm_manager.DatabricksRetryLLM") as MockRetryLLM,
        ):
            await LLMManager.configure_crewai_llm("databricks-llama-4-maverick", "group-1", 0.5)
            call_kwargs = MockRetryLLM.call_args[1]
            assert call_kwargs["temperature"] == 0.5

    @pytest.mark.asyncio
    async def test_max_output_tokens_non_gpt5(self):
        config = _make_model_config("gpt-4o", "openai", max_output_tokens=4096)
        p_session, p_service = _patch_session_and_config(config)
        with (
            p_session,
            p_service,
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="key"),
            patch("src.core.llm_manager.LLM") as MockLLM,
        ):
            await LLMManager.configure_crewai_llm("gpt-4o", "group-1", None)
            call_kwargs = MockLLM.call_args[1]
            assert call_kwargs.get("max_tokens") == 4096
            assert "max_completion_tokens" not in call_kwargs


# ---------------------------------------------------------------------------
# get_llm
# ---------------------------------------------------------------------------


class TestGetLlm:
    """Test LLMManager.get_llm method."""

    @pytest.mark.asyncio
    async def test_get_llm_success(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = "group-1"
        mock_llm = MagicMock()

        with (
            patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx),
            patch.object(LLMManager, "configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm),
        ):
            result = await LLMManager.get_llm("test-model", temperature=0.5)
            assert result == mock_llm

    @pytest.mark.asyncio
    async def test_get_llm_raises_without_group_id(self):
        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = None

        with patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx):
            with pytest.raises(ValueError, match="group_id is REQUIRED"):
                await LLMManager.get_llm("test-model")

    @pytest.mark.asyncio
    async def test_get_llm_raises_when_no_context(self):
        with patch("src.utils.user_context.UserContext.get_group_context", return_value=None):
            with pytest.raises(ValueError, match="group_id is REQUIRED"):
                await LLMManager.get_llm("test-model")


# ---------------------------------------------------------------------------
# get_embedding — circuit breaker
# ---------------------------------------------------------------------------


class TestGetEmbeddingCircuitBreaker:
    """Test circuit breaker logic in get_embedding."""

    def setup_method(self):
        LLMManager._embedding_failures.clear()

    def teardown_method(self):
        LLMManager._embedding_failures.clear()

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_returns_none(self):
        """When circuit is open, should return None immediately."""
        LLMManager._embedding_failures["databricks"] = {
            "count": 5,
            "last_failure": _time.time(),
        }

        with patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"):
            result = await LLMManager.get_embedding("test text")

        assert result is None

    @pytest.mark.asyncio
    async def test_circuit_breaker_resets_after_timeout(self):
        """After reset time, circuit should close and allow retry."""
        LLMManager._embedding_failures["databricks"] = {
            "count": 5,
            "last_failure": _time.time() - 400,  # older than reset_time (300s)
        }

        # The circuit should be reset, so it will attempt the call.
        # Mock the auth to make it fail gracefully (return None from auth)
        with (
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"),
        ):
            result = await LLMManager.get_embedding("test text")
        # Returns None because auth is None, but circuit was reset
        assert result is None
        # Circuit should be reset
        assert LLMManager._embedding_failures.get("databricks", {}).get("count", 0) == 0

    @pytest.mark.asyncio
    async def test_embedding_tracks_failures(self):
        """Exceptions should increment failure count."""
        with (
            patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, side_effect=RuntimeError("auth boom")),
            patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"),
        ):
            result = await LLMManager.get_embedding("test text")
        assert result is None
        assert LLMManager._embedding_failures["databricks"]["count"] == 1

    @pytest.mark.asyncio
    async def test_embedding_with_ollama_provider(self):
        """Test embedder_config with ollama provider routes correctly."""
        embedder_config = {"provider": "ollama", "config": {"model": "nomic-embed"}}

        # Build a mock that works with: async with ClientSession(...) as session: async with session.post(...) as resp:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"embeddings": [[0.1, 0.2, 0.3]]})

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_http_session = MagicMock()
        mock_http_session.post.return_value = mock_post_ctx

        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_http_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)

        assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_embedding_with_google_provider(self):
        """Test embedder_config with google provider routes correctly."""
        embedder_config = {"provider": "google", "config": {"model": "text-embedding-004"}}

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"embedding": {"values": [0.4, 0.5]}})

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_http_session = MagicMock()
        mock_http_session.post.return_value = mock_post_ctx

        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_http_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = "group-1"

        with (
            patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx),
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="gem-key"),
            patch("aiohttp.ClientSession", return_value=mock_session_ctx),
        ):
            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)

        assert result == [0.4, 0.5]

    @pytest.mark.asyncio
    async def test_embedding_with_openai_provider(self):
        """Test default/openai provider for embeddings."""
        embedder_config = {"provider": "openai", "config": {"model": "text-embedding-ada-002"}}

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": [{"embedding": [0.6, 0.7]}]})

        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_http_session = MagicMock()
        mock_http_session.post.return_value = mock_post_ctx

        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_http_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_ctx = MagicMock()
        mock_ctx.primary_group_id = "group-1"

        with (
            patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_ctx),
            patch("src.core.llm_manager.ApiKeysService.get_provider_api_key", new_callable=AsyncMock, return_value="oai-key"),
            patch("aiohttp.ClientSession", return_value=mock_session_ctx),
        ):
            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)

        assert result == [0.6, 0.7]
