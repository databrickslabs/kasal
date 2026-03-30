"""
Unit tests for EmbedderConfigBuilder._get_databricks_endpoint DATABRICKS_HOST fallback
and user_token passthrough to get_auth_context.

Tests cover:
- DATABRICKS_HOST env var fallback when auth context and DB both fail
- https:// prefix normalization for bare hostnames
- Trailing slash stripping
- Env var not set returns empty string
- user_token is forwarded to get_auth_context
- Auth context success short-circuits env var fallback
- DB success short-circuits env var fallback
"""
import os
import sys
from unittest.mock import MagicMock

# Set database type to sqlite for testing
os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

# Mock heavy third-party modules before src.engines imports
_crewai_mock = MagicMock()
_crewai_tools_mock = MagicMock()

_MODULES_TO_MOCK = {
    'crewai': _crewai_mock,
    'crewai.tools': _crewai_mock.tools,
    'crewai.events': _crewai_mock.events,
    'crewai.flow': _crewai_mock.flow,
    'crewai.flow.flow': _crewai_mock.flow.flow,
    'crewai.flow.persistence': _crewai_mock.flow.persistence,
    'crewai.llm': _crewai_mock.llm,
    'crewai.memory': _crewai_mock.memory,
    'crewai.memory.storage': _crewai_mock.memory.storage,
    'crewai.memory.storage.rag_storage': _crewai_mock.memory.storage.rag_storage,
    'crewai.project': _crewai_mock.project,
    'crewai.tasks': _crewai_mock.tasks,
    'crewai.tasks.llm_guardrail': _crewai_mock.tasks.llm_guardrail,
    'crewai.tasks.task_output': _crewai_mock.tasks.task_output,
    'crewai.utilities': _crewai_mock.utilities,
    'crewai.utilities.converter': _crewai_mock.utilities.converter,
    'crewai.utilities.evaluators': _crewai_mock.utilities.evaluators,
    'crewai.utilities.evaluators.task_evaluator': _crewai_mock.utilities.evaluators.task_evaluator,
    'crewai.utilities.exceptions': _crewai_mock.utilities.exceptions,
    'crewai.utilities.internal_instructor': _crewai_mock.utilities.internal_instructor,
    'crewai.utilities.paths': _crewai_mock.utilities.paths,
    'crewai.utilities.printer': _crewai_mock.utilities.printer,
    'crewai.knowledge': _crewai_mock.knowledge,
    'crewai.llms': _crewai_mock.llms,
    'crewai.llms.providers': _crewai_mock.llms.providers,
    'crewai.llms.providers.openai': _crewai_mock.llms.providers.openai,
    'crewai.llms.providers.openai.completion': _crewai_mock.llms.providers.openai.completion,
    'crewai.events.types': _crewai_mock.events.types,
    'crewai.events.types.llm_events': _crewai_mock.events.types.llm_events,
    'crewai_tools': _crewai_tools_mock,
    'asyncpg': MagicMock(),
    'chromadb': MagicMock(),
}

_originals = {}
for _mod_name, _mock_obj in _MODULES_TO_MOCK.items():
    _originals[_mod_name] = sys.modules.get(_mod_name)
    sys.modules[_mod_name] = _mock_obj

import pytest
from unittest.mock import patch, AsyncMock
from src.engines.crewai.config.embedder_config_builder import EmbedderConfigBuilder

# Restore modules immediately after import
for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


class TestGetDatabricksEndpointEnvFallback:
    """Tests for _get_databricks_endpoint DATABRICKS_HOST env var fallback."""

    @pytest.mark.asyncio
    async def test_env_var_fallback_bare_hostname(self):
        """DATABRICKS_HOST without https:// prefix gets normalized."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None):
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {'DATABRICKS_HOST': 'e2-demo.cloud.databricks.com'}):
                    result = await builder._get_databricks_endpoint()

        assert result == 'https://e2-demo.cloud.databricks.com'

    @pytest.mark.asyncio
    async def test_env_var_fallback_with_https_prefix(self):
        """DATABRICKS_HOST already having https:// is not double-prefixed."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None):
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {'DATABRICKS_HOST': 'https://workspace.databricks.com'}):
                    result = await builder._get_databricks_endpoint()

        assert result == 'https://workspace.databricks.com'

    @pytest.mark.asyncio
    async def test_env_var_fallback_strips_trailing_slash(self):
        """DATABRICKS_HOST trailing slash is stripped."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None):
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {'DATABRICKS_HOST': 'e2-demo.cloud.databricks.com/'}):
                    result = await builder._get_databricks_endpoint()

        assert result == 'https://e2-demo.cloud.databricks.com'

    @pytest.mark.asyncio
    async def test_env_var_not_set_returns_empty(self):
        """When DATABRICKS_HOST is not set and auth/db fail, returns empty string."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None):
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {}, clear=False):
                    # Remove DATABRICKS_HOST if present
                    env_copy = os.environ.copy()
                    env_copy.pop('DATABRICKS_HOST', None)
                    with patch.dict(os.environ, env_copy, clear=True):
                        result = await builder._get_databricks_endpoint()

        assert result == ''

    @pytest.mark.asyncio
    async def test_auth_context_success_skips_env_var(self):
        """When get_auth_context succeeds, DATABRICKS_HOST env var is NOT used."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        mock_auth = MagicMock()
        mock_auth.workspace_url = 'https://from-auth.databricks.com'
        mock_auth.auth_method = 'obo'

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=mock_auth):
            with patch.dict(os.environ, {'DATABRICKS_HOST': 'from-env.databricks.com'}):
                result = await builder._get_databricks_endpoint()

        assert result == 'https://from-auth.databricks.com'

    @pytest.mark.asyncio
    async def test_db_success_skips_env_var(self):
        """When database config succeeds, DATABRICKS_HOST env var is NOT used."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        mock_db_config = MagicMock()
        mock_db_config.workspace_url = 'https://from-db.databricks.com'

        mock_session = AsyncMock()
        mock_service = MagicMock()
        mock_service.get_databricks_config = AsyncMock(return_value=mock_db_config)

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None):
            with patch('src.db.session.request_scoped_session') as mock_rss:
                mock_rss.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_rss.return_value.__aexit__ = AsyncMock(return_value=None)
                with patch('src.services.databricks_service.DatabricksService', return_value=mock_service):
                    with patch.object(
                        EmbedderConfigBuilder, '_get_databricks_endpoint',
                        wraps=builder._get_databricks_endpoint
                    ):
                        # We need to patch DatabricksURLUtils at the right level
                        with patch('src.engines.crewai.config.embedder_config_builder.DatabricksURLUtils') as mock_utils:
                            mock_utils.normalize_workspace_url.return_value = 'https://from-db.databricks.com'
                            with patch.dict(os.environ, {'DATABRICKS_HOST': 'from-env.databricks.com'}):
                                result = await builder._get_databricks_endpoint()

        assert result == 'https://from-db.databricks.com'


class TestGetDatabricksEndpointUserToken:
    """Tests for user_token passthrough to get_auth_context."""

    @pytest.mark.asyncio
    async def test_user_token_passed_to_get_auth_context(self):
        """Verify user_token is forwarded to get_auth_context for OBO auth."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="test_tok")

        mock_auth = MagicMock()
        mock_auth.workspace_url = 'https://workspace.databricks.com'
        mock_auth.auth_method = 'obo'

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=mock_auth) as mock_get_auth:
            result = await builder._get_databricks_endpoint()

        mock_get_auth.assert_called_once_with(user_token="test_tok")
        assert result == 'https://workspace.databricks.com'

    @pytest.mark.asyncio
    async def test_none_user_token_passed_to_get_auth_context(self):
        """Verify None user_token is still forwarded (service-level auth)."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token=None)

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, return_value=None) as mock_get_auth:
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {}, clear=False):
                    env_copy = os.environ.copy()
                    env_copy.pop('DATABRICKS_HOST', None)
                    with patch.dict(os.environ, env_copy, clear=True):
                        await builder._get_databricks_endpoint()

        mock_get_auth.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    async def test_auth_context_exception_falls_through_to_env(self):
        """When get_auth_context raises, code falls through to DB then env var."""
        config = {'agents': [], 'group_id': 'test'}
        builder = EmbedderConfigBuilder(config, user_token="tok")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock, side_effect=Exception("auth boom")):
            with patch('src.db.session.request_scoped_session', side_effect=Exception("no db")):
                with patch.dict(os.environ, {'DATABRICKS_HOST': 'fallback.databricks.com'}):
                    result = await builder._get_databricks_endpoint()

        assert result == 'https://fallback.databricks.com'
