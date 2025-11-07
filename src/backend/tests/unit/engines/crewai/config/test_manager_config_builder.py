import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from crewai import Process

from src.engines.crewai.config.manager_config_builder import ManagerConfigBuilder


class TestManagerConfigBuilder:
    """Test suite for ManagerConfigBuilder class."""

    @pytest.fixture
    def config_with_group_id(self):
        """Sample config with group_id."""
        return {
            "group_id": "test_group_123",
            "model": "databricks-llama-4-maverick",
            "crew": {
                "process": "hierarchical"
            }
        }

    @pytest.fixture
    def config_without_group_id(self):
        """Sample config without group_id."""
        return {
            "model": "databricks-llama-4-maverick",
            "crew": {
                "process": "hierarchical"
            }
        }

    @pytest.fixture
    def manager_builder(self, config_with_group_id):
        """ManagerConfigBuilder instance with group_id."""
        return ManagerConfigBuilder(config_with_group_id)

    @pytest.mark.asyncio
    async def test_configure_manager_hierarchical_with_requested_model(self, manager_builder, config_with_group_id):
        """Test hierarchical manager configuration with requested model."""
        crew_kwargs = {}
        mock_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_llm) as mock_configure:

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_llm
            mock_configure.assert_called_once_with("databricks-llama-4-maverick", "test_group_123")

    @pytest.mark.asyncio
    async def test_configure_manager_hierarchical_without_requested_model(self, config_with_group_id):
        """Test hierarchical manager configuration falls back when no model specified."""
        config_with_group_id.pop('model', None)  # Remove model
        manager_builder = ManagerConfigBuilder(config_with_group_id)

        crew_kwargs = {}
        mock_fallback_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_fallback_llm) as mock_configure:

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            # Should use fallback databricks-llama-4-maverick
            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_fallback_llm
            mock_configure.assert_called_with("databricks-llama-4-maverick", "test_group_123")

    @pytest.mark.asyncio
    async def test_configure_manager_hierarchical_without_group_id(self, config_without_group_id):
        """Test hierarchical manager configuration fails gracefully without group_id."""
        manager_builder = ManagerConfigBuilder(config_without_group_id)
        crew_kwargs = {}

        result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

        # Should not add manager_llm when group_id is missing
        assert 'manager_llm' not in result

    @pytest.mark.asyncio
    async def test_configure_manager_hierarchical_llm_creation_fails_uses_fallback(self, manager_builder):
        """Test hierarchical manager falls back when LLM creation fails."""
        crew_kwargs = {}
        mock_fallback_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm') as mock_configure:
            # First call fails, second call (fallback) succeeds
            mock_configure.side_effect = [Exception("Model not found"), mock_fallback_llm]

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            # Should have fallback manager_llm
            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_fallback_llm
            # Should be called twice: once for requested model, once for fallback
            assert mock_configure.call_count == 2

    @pytest.mark.asyncio
    async def test_configure_manager_with_manager_llm_string_in_config(self, manager_builder):
        """Test manager configuration converts string manager_llm to LLM object."""
        crew_kwargs = {}
        manager_builder.config['crew']['manager_llm'] = "gpt-4"
        mock_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_llm) as mock_configure:

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_llm
            mock_configure.assert_called_with("gpt-4", "test_group_123")

    @pytest.mark.asyncio
    async def test_configure_manager_with_manager_llm_object_in_config(self, manager_builder):
        """Test manager configuration uses existing LLM object."""
        crew_kwargs = {}
        existing_llm = MagicMock()
        manager_builder.config['crew']['manager_llm'] = existing_llm

        result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

        assert 'manager_llm' in result
        assert result['manager_llm'] == existing_llm

    @pytest.mark.asyncio
    async def test_configure_manager_with_manager_agent_in_config(self, manager_builder):
        """Test manager configuration with manager_agent."""
        crew_kwargs = {}
        manager_agent_config = {
            "role": "Manager",
            "goal": "Manage the team",
            "backstory": "Experienced manager"
        }
        manager_builder.config['crew']['manager_agent'] = manager_agent_config
        mock_agent = MagicMock()
        mock_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.create_agent',
                   return_value=mock_agent) as mock_create_agent, \
             patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_llm):

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            assert 'manager_agent' in result
            assert result['manager_agent'] == mock_agent
            assert 'manager_llm' not in result  # Should not have manager_llm when agent is provided

    @pytest.mark.asyncio
    async def test_configure_manager_sequential_with_planning(self, config_with_group_id):
        """Test sequential process with planning enabled."""
        config_with_group_id['crew']['planning'] = True
        manager_builder = ManagerConfigBuilder(config_with_group_id)

        crew_kwargs = {}
        mock_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_llm) as mock_configure:

            result = await manager_builder.configure_manager(crew_kwargs, Process.sequential)

            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_llm
            mock_configure.assert_called_with("databricks-llama-4-maverick", "test_group_123")

    @pytest.mark.asyncio
    async def test_configure_manager_sequential_without_planning(self, manager_builder):
        """Test sequential process without planning enabled."""
        crew_kwargs = {}

        result = await manager_builder.configure_manager(crew_kwargs, Process.sequential)

        # Should not add manager_llm when planning is False
        assert 'manager_llm' not in result

    @pytest.mark.asyncio
    async def test_set_fallback_manager_llm_success(self, manager_builder):
        """Test fallback manager LLM creation succeeds."""
        crew_kwargs = {}
        mock_fallback_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_fallback_llm) as mock_configure:

            result = await manager_builder._set_fallback_manager_llm(crew_kwargs)

            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_fallback_llm
            mock_configure.assert_called_once_with("databricks-llama-4-maverick", "test_group_123")

    @pytest.mark.asyncio
    async def test_set_fallback_manager_llm_fails_gracefully(self, manager_builder):
        """Test fallback manager LLM handles failure gracefully."""
        crew_kwargs = {}

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   side_effect=Exception("Fallback failed")):

            result = await manager_builder._set_fallback_manager_llm(crew_kwargs)

            # Should return crew_kwargs without manager_llm, but not raise exception
            assert 'manager_llm' not in result

    @pytest.mark.asyncio
    async def test_set_fallback_manager_llm_without_group_id(self, config_without_group_id):
        """Test fallback manager LLM without group_id."""
        manager_builder = ManagerConfigBuilder(config_without_group_id)
        crew_kwargs = {}

        result = await manager_builder._set_fallback_manager_llm(crew_kwargs)

        # Should not add manager_llm when group_id is missing
        assert 'manager_llm' not in result

    @pytest.mark.asyncio
    async def test_configure_manager_with_databricks_prefix_retry(self, manager_builder):
        """Test manager configuration retries with databricks/ prefix."""
        crew_kwargs = {}
        manager_builder.config['crew']['manager_llm'] = "databricks-llama-4-maverick"  # Without prefix
        mock_llm = MagicMock()

        with patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm') as mock_configure:
            # First call (without prefix) fails, second call (with prefix) succeeds
            mock_configure.side_effect = [
                Exception("Model not found"),
                mock_llm
            ]

            result = await manager_builder.configure_manager(crew_kwargs, Process.hierarchical)

            assert 'manager_llm' in result
            assert result['manager_llm'] == mock_llm
            # Should be called twice: once without prefix, once with prefix
            assert mock_configure.call_count == 2
            mock_configure.assert_any_call("databricks-llama-4-maverick", "test_group_123")
            mock_configure.assert_any_call("databricks/databricks-llama-4-maverick", "test_group_123")
