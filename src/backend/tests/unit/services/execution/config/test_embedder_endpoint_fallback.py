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

# No sys.modules stubbing: kasal_engine is a real vendored package here, and
# stubbing it cached MagicMock-holding src.services modules that broke unrelated
# suites on the same worker. See test_embedder_config_builder.py.
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from src.services.execution.config.embedder_config_builder import EmbedderConfigBuilder


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
                with patch('src.services.databricks.workspace.service.DatabricksService', return_value=mock_service):
                    with patch.object(
                        EmbedderConfigBuilder, '_get_databricks_endpoint',
                        wraps=builder._get_databricks_endpoint
                    ):
                        # We need to patch DatabricksURLUtils at the right level
                        with patch('src.services.execution.config.embedder_config_builder.DatabricksURLUtils') as mock_utils:
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
