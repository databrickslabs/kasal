"""
Unit tests for EmbedderConfigBuilder

Includes regression test for critical bug fix where crew_kwargs was replaced with empty dict.
"""
import pytest
from unittest.mock import patch, AsyncMock
from src.engines.crewai.config.embedder_config_builder import EmbedderConfigBuilder


class TestEmbedderConfigBuilder:
    """Test EmbedderConfigBuilder class"""

    @pytest.mark.asyncio
    async def test_configure_databricks_embedder_preserves_crew_kwargs(self):
        """
        REGRESSION TEST: Verify that _configure_databricks_embedder preserves crew_kwargs.

        This tests the fix for a critical bug where crew_kwargs was replaced with an empty dict {},
        causing all base fields (agents, tasks, process, verbose, memory) to be lost, which resulted
        in "1 validation error for Crew" when trying to create the Crew instance.

        The bug was in embedder_config_builder.py where _configure_databricks_embedder returned
        `return {}, databricks_embedder, embedder_config` instead of returning the crew_kwargs it received.
        """
        config = {
            'agents': [
                {
                    'role': 'test_agent',
                    'embedder_config': {
                        'provider': 'databricks',
                        'config': {'model': 'databricks-gte-large-en'}
                    }
                }
            ]
        }

        builder = EmbedderConfigBuilder(config, user_token="test_token")

        # Initial crew_kwargs with all required base fields
        initial_crew_kwargs = {
            'agents': ['agent1', 'agent2'],
            'tasks': ['task1', 'task2'],
            'process': 'sequential',
            'verbose': True,
            'memory': True
        }

        # Mock the auth and database calls
        with patch('src.utils.databricks_auth.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_api_key:
                with patch.object(builder, '_get_databricks_endpoint', new_callable=AsyncMock, return_value='https://example.databricks.com'):
                    # Setup mocks
                    mock_auth.return_value = ({'Authorization': 'Bearer token'}, None)
                    mock_api_key.return_value = 'test_key'

                    # Call configure_embedder
                    result_kwargs, custom_embedder, embedder_config = await builder.configure_embedder(initial_crew_kwargs)

                    # CRITICAL ASSERTIONS: Verify that ALL base fields are preserved
                    assert 'agents' in result_kwargs, "agents field was lost - regression detected!"
                    assert 'tasks' in result_kwargs, "tasks field was lost - regression detected!"
                    assert 'process' in result_kwargs, "process field was lost - regression detected!"
                    assert 'verbose' in result_kwargs, "verbose field was lost - regression detected!"
                    assert 'memory' in result_kwargs, "memory field was lost - regression detected!"

                    # Verify values are unchanged
                    assert result_kwargs['agents'] == ['agent1', 'agent2']
                    assert result_kwargs['tasks'] == ['task1', 'task2']
                    assert result_kwargs['process'] == 'sequential'
                    assert result_kwargs['verbose'] is True
                    assert result_kwargs['memory'] is True

                    # Verify custom embedder was created
                    assert custom_embedder is not None
                    assert embedder_config is not None

    @pytest.mark.asyncio
    async def test_configure_databricks_embedder_error_path_preserves_crew_kwargs(self):
        """
        REGRESSION TEST: Verify error paths also preserve crew_kwargs.

        Even when errors occur (auth failure, no API key), the method must return
        crew_kwargs instead of an empty dict to prevent losing base fields.
        """
        config = {
            'agents': [
                {
                    'role': 'test_agent',
                    'embedder_config': {
                        'provider': 'databricks',
                        'config': {'model': 'databricks-gte-large-en'}
                    }
                }
            ]
        }

        builder = EmbedderConfigBuilder(config, user_token=None)

        initial_crew_kwargs = {
            'agents': ['agent1'],
            'tasks': ['task1'],
            'process': 'sequential',
            'verbose': True,
            'memory': True
        }

        # Mock auth to fail (no token, no API key)
        with patch('src.utils.databricks_auth.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_api_key:
                mock_auth.return_value = (None, "Auth failed")
                mock_api_key.return_value = None

                # Call configure_embedder - should fail gracefully
                result_kwargs, custom_embedder, embedder_config = await builder.configure_embedder(initial_crew_kwargs)

                # CRITICAL: Even on error, crew_kwargs must be preserved
                assert 'agents' in result_kwargs, "agents field was lost on error path - regression detected!"
                assert 'tasks' in result_kwargs, "tasks field was lost on error path - regression detected!"
                assert 'process' in result_kwargs, "process field was lost on error path - regression detected!"
                assert result_kwargs['agents'] == ['agent1']
                assert result_kwargs['tasks'] == ['task1']

                # Embedder should be None due to error
                assert custom_embedder is None
                assert embedder_config is None

    @pytest.mark.asyncio
    async def test_configure_embedder_with_no_embedder_config(self):
        """
        Test that configure_embedder defaults to Databricks when no embedder config found.

        The system always defaults to Databricks embedder if no valid embedder config is provided.
        This test verifies that crew_kwargs base fields are preserved even when defaulting.
        """
        config = {
            'agents': [
                {'role': 'test_agent'}  # No embedder_config
            ]
        }

        builder = EmbedderConfigBuilder(config, user_token="test_token")

        initial_crew_kwargs = {
            'agents': ['agent1'],
            'tasks': ['task1'],
            'process': 'sequential',
            'verbose': True,
            'memory': True
        }

        with patch('src.utils.databricks_auth.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_api_key:
                with patch.object(builder, '_get_databricks_endpoint', new_callable=AsyncMock, return_value='https://example.databricks.com'):
                    mock_auth.return_value = ({'Authorization': 'Bearer token'}, None)
                    mock_api_key.return_value = 'test_key'

                    result_kwargs, custom_embedder, embedder_config = await builder.configure_embedder(initial_crew_kwargs)

                    # Base fields must be preserved
                    assert 'agents' in result_kwargs, "agents field lost with default embedder"
                    assert 'tasks' in result_kwargs, "tasks field lost with default embedder"
                    assert result_kwargs['agents'] == ['agent1']
                    assert result_kwargs['tasks'] == ['task1']

                    # Databricks embedder should be created as default
                    assert custom_embedder is not None
                    assert embedder_config is not None

    @pytest.mark.asyncio
    async def test_configure_embedder_with_openai_provider(self):
        """Test that configure_embedder preserves crew_kwargs with OpenAI provider."""
        config = {
            'agents': [
                {
                    'role': 'test_agent',
                    'embedder_config': {
                        'provider': 'openai',
                        'config': {'model': 'text-embedding-ada-002'}
                    }
                }
            ]
        }

        builder = EmbedderConfigBuilder(config, user_token="test_token")

        initial_crew_kwargs = {
            'agents': ['agent1'],
            'tasks': ['task1'],
            'process': 'sequential',
            'verbose': True,
            'memory': True
        }

        with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_api_key:
            mock_api_key.return_value = 'test_openai_key'

            result_kwargs, custom_embedder, embedder_config = await builder.configure_embedder(initial_crew_kwargs)

            # Base fields must be preserved
            assert 'agents' in result_kwargs
            assert 'tasks' in result_kwargs
            assert 'process' in result_kwargs
            assert result_kwargs['agents'] == ['agent1']

            # Embedder should be configured
            assert 'embedder' in result_kwargs
            assert result_kwargs['embedder']['provider'] == 'openai'
