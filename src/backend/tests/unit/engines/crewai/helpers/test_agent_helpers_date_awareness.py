"""
Unit tests for inject_date and date_format parameters in agent_helpers.create_agent function.

These tests verify the CrewAI 1.9+ date awareness feature integration:
- inject_date: Boolean to inject current date into agent context
- date_format: String format for the injected date (e.g., "%Y-%m-%d")

Test Coverage:
1. inject_date=True passed to Agent when provided
2. date_format passed to Agent when provided
3. inject_date NOT passed when None in agent_config
4. date_format NOT passed when None in agent_config
5. Both parameters work together successfully
6. Edge cases: empty string, False values, various format strings
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from typing import List, Any

from src.engines.crewai.helpers.agent_helpers import create_agent


class TestCreateAgentDateAwareness:
    """Test suite for inject_date and date_format parameters in create_agent function."""

    @pytest.fixture
    def base_agent_config(self):
        """Base agent configuration without date awareness settings."""
        return {
            "role": "Test Agent",
            "goal": "Test agent goal",
            "backstory": "Test agent backstory",
            "verbose": True,
            "allow_delegation": False,
        }

    @pytest.fixture
    def mock_tools(self):
        """Mock tools list."""
        tool1 = MagicMock()
        tool1.name = "tool1"
        return [tool1]

    @pytest.fixture
    def mock_config(self):
        """Mock global config with required group_id."""
        return {
            "api_keys": {"openai": "test_key"},
            "group_id": "test-group-123"
        }

    @pytest.mark.asyncio
    async def test_inject_date_true_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that inject_date=True is passed to the Agent when provided."""
        agent_key = "test_agent"
        agent_config = {**base_agent_config, "inject_date": True}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            # Mock async session for MCP integration
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called with inject_date=True
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "inject_date" in call_kwargs
                assert call_kwargs["inject_date"] is True

    @pytest.mark.asyncio
    async def test_inject_date_false_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that inject_date=False is passed to the Agent when explicitly set."""
        agent_key = "test_agent"
        agent_config = {**base_agent_config, "inject_date": False}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called with inject_date=False
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "inject_date" in call_kwargs
                assert call_kwargs["inject_date"] is False

    @pytest.mark.asyncio
    async def test_date_format_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that date_format is passed to the Agent when provided."""
        agent_key = "test_agent"
        date_format = "%Y-%m-%d"
        agent_config = {**base_agent_config, "date_format": date_format}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called with date_format
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "date_format" in call_kwargs
                assert call_kwargs["date_format"] == date_format

    @pytest.mark.asyncio
    async def test_inject_date_none_not_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that inject_date is NOT passed to the Agent when it's None."""
        agent_key = "test_agent"
        agent_config = {**base_agent_config, "inject_date": None}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called WITHOUT inject_date
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "inject_date" not in call_kwargs

    @pytest.mark.asyncio
    async def test_date_format_none_not_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that date_format is NOT passed to the Agent when it's None."""
        agent_key = "test_agent"
        agent_config = {**base_agent_config, "date_format": None}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called WITHOUT date_format
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "date_format" not in call_kwargs

    @pytest.mark.asyncio
    async def test_inject_date_not_in_config_not_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that inject_date is NOT passed when not present in agent_config."""
        agent_key = "test_agent"
        # base_agent_config does not include inject_date

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=base_agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called WITHOUT inject_date
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "inject_date" not in call_kwargs

    @pytest.mark.asyncio
    async def test_date_format_not_in_config_not_passed_to_agent(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that date_format is NOT passed when not present in agent_config."""
        agent_key = "test_agent"
        # base_agent_config does not include date_format

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=base_agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called WITHOUT date_format
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "date_format" not in call_kwargs

    @pytest.mark.asyncio
    async def test_both_inject_date_and_date_format_passed_together(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that both inject_date and date_format are passed when both provided."""
        agent_key = "test_agent"
        date_format = "%B %d, %Y"
        agent_config = {
            **base_agent_config,
            "inject_date": True,
            "date_format": date_format
        }

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify Agent was called with both parameters
                mock_agent_class.assert_called_once()
                call_kwargs = mock_agent_class.call_args[1]
                assert "inject_date" in call_kwargs
                assert call_kwargs["inject_date"] is True
                assert "date_format" in call_kwargs
                assert call_kwargs["date_format"] == date_format

    @pytest.mark.asyncio
    async def test_agent_created_successfully_with_date_awareness(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that agent is created successfully with date awareness parameters."""
        agent_key = "test_agent"
        agent_config = {
            **base_agent_config,
            "inject_date": True,
            "date_format": "%Y-%m-%d %H:%M:%S"
        }

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_instance.role = agent_config["role"]
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Verify agent was created and returned
                assert result == mock_agent_instance
                mock_agent_class.assert_called_once()


class TestCreateAgentDateFormatVariations:
    """Test various date_format string variations."""

    @pytest.fixture
    def base_agent_config(self):
        """Base agent configuration."""
        return {
            "role": "Test Agent",
            "goal": "Test agent goal",
            "backstory": "Test agent backstory",
        }

    @pytest.fixture
    def mock_tools(self):
        """Mock tools list."""
        return []

    @pytest.fixture
    def mock_config(self):
        """Mock global config."""
        return {"group_id": "test-group-123"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("date_format,description", [
        ("%Y-%m-%d", "ISO format"),
        ("%m/%d/%Y", "US format"),
        ("%d/%m/%Y", "European format"),
        ("%B %d, %Y", "Long month name format"),
        ("%Y-%m-%d %H:%M:%S", "ISO with time"),
        ("%A, %B %d, %Y", "Full weekday and month"),
        ("%Y%m%d", "Compact date format"),
    ])
    async def test_various_date_formats(
        self, base_agent_config, mock_tools, mock_config, date_format, description
    ):
        """Test that various date_format strings are passed correctly."""
        agent_key = "test_agent"
        agent_config = {**base_agent_config, "date_format": date_format}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["date_format"] == date_format, \
                    f"Failed for {description}"

    @pytest.mark.asyncio
    async def test_date_format_empty_string_passed(
        self, base_agent_config, mock_tools, mock_config
    ):
        """Test that empty string date_format is passed (non-None value)."""
        agent_key = "test_agent"
        # Empty string is a non-None value, so it should be passed
        agent_config = {**base_agent_config, "date_format": ""}

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )

                # Empty string is technically a valid value (non-None)
                # The code checks `agent_config[param] is not None`
                # Empty string passes this check
                call_kwargs = mock_agent_class.call_args[1]
                # Note: Empty string is falsy but not None, so it should be passed
                # Based on: `if param in agent_config and agent_config[param] is not None:`
                # Empty string "" is not None, so it should be included
                assert "date_format" in call_kwargs
                assert call_kwargs["date_format"] == ""


class TestCreateAgentDateAwarenessWithOtherParams:
    """Test date awareness parameters alongside other additional parameters."""

    @pytest.fixture
    def base_agent_config(self):
        """Base agent configuration."""
        return {
            "role": "Test Agent",
            "goal": "Test agent goal",
            "backstory": "Test agent backstory",
        }

    @pytest.fixture
    def mock_config(self):
        """Mock global config."""
        return {"group_id": "test-group-123"}

    @pytest.mark.asyncio
    async def test_date_params_with_reasoning_params(
        self, base_agent_config, mock_config
    ):
        """Test inject_date/date_format work alongside reasoning params."""
        agent_key = "test_agent"
        agent_config = {
            **base_agent_config,
            "inject_date": True,
            "date_format": "%Y-%m-%d",
            "reasoning": True,
            "max_reasoning_attempts": 5,
        }

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=[],
                    config=mock_config
                )

                call_kwargs = mock_agent_class.call_args[1]
                # Date awareness params
                assert call_kwargs["inject_date"] is True
                assert call_kwargs["date_format"] == "%Y-%m-%d"
                # Reasoning params (also in additional_params list)
                assert call_kwargs["reasoning"] is True
                assert call_kwargs["max_reasoning_attempts"] == 5

    @pytest.mark.asyncio
    async def test_date_params_with_iteration_params(
        self, base_agent_config, mock_config
    ):
        """Test inject_date/date_format work alongside max_iter and max_rpm."""
        agent_key = "test_agent"
        agent_config = {
            **base_agent_config,
            "inject_date": True,
            "date_format": "%m/%d/%Y",
            "max_iter": 10,
            "max_rpm": 20,
        }

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=[],
                    config=mock_config
                )

                call_kwargs = mock_agent_class.call_args[1]
                # Date awareness params
                assert call_kwargs["inject_date"] is True
                assert call_kwargs["date_format"] == "%m/%d/%Y"
                # Iteration params
                assert call_kwargs["max_iter"] == 10
                assert call_kwargs["max_rpm"] == 20

    @pytest.mark.asyncio
    async def test_all_additional_params_together(
        self, base_agent_config, mock_config
    ):
        """Test all additional_params including date awareness work together."""
        agent_key = "test_agent"
        agent_config = {
            **base_agent_config,
            # All params from additional_params list
            "max_iter": 5,
            "max_rpm": 15,
            "memory": True,
            "code_execution_mode": "safe",
            "max_context_window_size": 4096,
            "max_tokens": 2048,
            "reasoning": True,
            "max_reasoning_attempts": 3,
            "inject_date": True,
            "date_format": "%Y-%m-%d %H:%M",
        }

        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance

            mock_llm = MagicMock()
            mock_llm.model = "gpt-4o"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp_integration:
                mock_mcp_integration.create_mcp_tools_for_agent = AsyncMock(return_value=[])

                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=[],
                    config=mock_config
                )

                call_kwargs = mock_agent_class.call_args[1]
                # Verify all additional params are present
                assert call_kwargs["max_iter"] == 5
                assert call_kwargs["max_rpm"] == 15
                assert call_kwargs["memory"] is True
                assert call_kwargs["code_execution_mode"] == "safe"
                assert call_kwargs["max_context_window_size"] == 4096
                assert call_kwargs["max_tokens"] == 2048
                assert call_kwargs["reasoning"] is True
                assert call_kwargs["max_reasoning_attempts"] == 3
                assert call_kwargs["inject_date"] is True
                assert call_kwargs["date_format"] == "%Y-%m-%d %H:%M"
