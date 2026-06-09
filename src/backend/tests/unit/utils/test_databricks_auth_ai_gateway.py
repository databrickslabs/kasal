"""Unit tests for AI-Gateway-related additions in src/utils/databricks_auth.py.

Covers:
- reset_auth_config_cache(): invalidates the module-level singleton's cached
  config (_config_loaded -> False, _workspace_host -> None).
- AuthContext.get_litellm_params(): returns an api_base routed through
  DatabricksURLUtils.construct_llm_base_url, i.e. /serving-endpoints when the
  AI Gateway is off and /ai-gateway/mlflow/v1 when DATABRICKS_ENABLE_AI_GATEWAY
  is "true".
"""
import os
import pytest

from src.utils.databricks_auth import (
    AuthContext,
    reset_auth_config_cache,
    _databricks_auth,
)
from src.utils.databricks_url_utils import DatabricksURLUtils


# ---------------------------------------------------------------------------
# reset_auth_config_cache
# ---------------------------------------------------------------------------

class TestResetAuthConfigCache:
    """Tests for the module-level reset_auth_config_cache() helper."""

    def test_flips_config_loaded_to_false(self):
        """reset_auth_config_cache() sets the singleton's _config_loaded to False."""
        orig_loaded = _databricks_auth._config_loaded
        orig_host = _databricks_auth._workspace_host
        try:
            # Simulate a previously-loaded config.
            _databricks_auth._config_loaded = True
            _databricks_auth._workspace_host = "https://cached-host.databricks.com"

            reset_auth_config_cache()

            assert _databricks_auth._config_loaded is False
            assert _databricks_auth._workspace_host is None
        finally:
            _databricks_auth._config_loaded = orig_loaded
            _databricks_auth._workspace_host = orig_host

    def test_idempotent_when_already_reset(self):
        """Calling reset on an already-reset singleton leaves it reset (no error)."""
        orig_loaded = _databricks_auth._config_loaded
        orig_host = _databricks_auth._workspace_host
        try:
            _databricks_auth._config_loaded = False
            _databricks_auth._workspace_host = None

            reset_auth_config_cache()

            assert _databricks_auth._config_loaded is False
            assert _databricks_auth._workspace_host is None
        finally:
            _databricks_auth._config_loaded = orig_loaded
            _databricks_auth._workspace_host = orig_host


# ---------------------------------------------------------------------------
# AuthContext.get_litellm_params
# ---------------------------------------------------------------------------

class TestGetLitellmParams:
    """Tests for AuthContext.get_litellm_params() api_base routing."""

    def _make_ctx(self):
        return AuthContext(
            token="t",
            workspace_url="https://h.databricks.com",
            auth_method="pat",
        )

    def test_api_key_passthrough(self):
        """api_key always mirrors the auth token."""
        env_var = DatabricksURLUtils.AI_GATEWAY_ENV_VAR
        saved = os.environ.get(env_var)
        try:
            os.environ[env_var] = "false"
            params = self._make_ctx().get_litellm_params()
            assert params["api_key"] == "t"
        finally:
            if saved is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = saved

    def test_serving_endpoints_when_gateway_off(self):
        """Gateway off -> api_base is the /serving-endpoints base."""
        env_var = DatabricksURLUtils.AI_GATEWAY_ENV_VAR
        saved = os.environ.get(env_var)
        try:
            os.environ[env_var] = "false"
            params = self._make_ctx().get_litellm_params()
            assert params["api_base"] == "https://h.databricks.com/serving-endpoints"
        finally:
            if saved is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = saved

    def test_ai_gateway_base_when_gateway_on(self):
        """Gateway on (env=true) -> api_base is the /ai-gateway/mlflow/v1 base."""
        env_var = DatabricksURLUtils.AI_GATEWAY_ENV_VAR
        saved = os.environ.get(env_var)
        try:
            os.environ[env_var] = "true"
            params = self._make_ctx().get_litellm_params()
            assert params["api_base"] == "https://h.databricks.com/ai-gateway/mlflow/v1"
        finally:
            if saved is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = saved
