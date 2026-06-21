"""
Unit tests for AgentConfig module.

Tests the functionality of agent configuration for CrewAI flows.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from src.engines.crewai.flow.modules.agent_adapter import AgentConfig


@pytest.fixture
def mock_agent_data():
    """Create mock agent data."""
    agent = MagicMock()
    agent.id = "agent-123"
    agent.name = "Test Agent"
    agent.role = "Data Analyst"
    agent.goal = "Analyze data"
    agent.backstory = "An experienced data analyst"
    agent.allow_delegation = True
    agent.tools = ["tool-1", "tool-2"]
    agent.llm = "gpt-4"
    agent.model = "gpt-4"
    agent.memory = True
    agent.max_iter = 10
    agent.max_rpm = 5
    agent.config = {"temperature": 0.7}
    return agent


@pytest.fixture
def mock_flow_data():
    """Create mock flow data."""
    flow = MagicMock()
    flow.nodes = [
        {
            "id": "agent-agent-123",
            "data": {
                "tools": ["tool-3", "tool-4"]
            }
        },
        {
            "id": "other-node",
            "data": {}
        }
    ]
    return flow


@pytest.fixture
def mock_tool_factory():
    """Create mock tool factory."""
    factory = AsyncMock()
    factory.initialize = AsyncMock()
    factory.create_tool = MagicMock()

    # Mock tools
    mock_tool1 = MagicMock()
    mock_tool1.name = "Tool 1"
    mock_tool2 = MagicMock()
    mock_tool2.name = "Tool 2"

    def create_tool_side_effect(tool_id, tool_config_override=None):
        if tool_id == "tool-1":
            return mock_tool1
        elif tool_id == "tool-2":
            return mock_tool2
        elif tool_id == "tool-3":
            return mock_tool1
        elif tool_id == "tool-4":
            return mock_tool2
        return None

    factory.create_tool.side_effect = create_tool_side_effect
    return factory


@pytest.fixture
def mock_llm():
    """Create mock LLM."""
    return MagicMock()


class TestAgentConfig:
    """Test cases for AgentConfig class."""

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_success(self, mock_tool_factory_class, mock_agent_data, mock_tool_factory, mock_llm):
        """Test successful agent configuration with tools (delegates to the shared
        build_agent — flow only sources tools + normalizes the spec)."""
        mock_tool_factory_class.return_value = mock_tool_factory
        mock_agent_instance = MagicMock()

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=mock_agent_instance) as mock_build:
            result = await AgentConfig.configure_agent_and_tools(mock_agent_data)

        assert result == mock_agent_instance
        mock_tool_factory.initialize.assert_called_once()
        mock_build.assert_called_once()

    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_no_agent_data(self):
        """Test agent configuration with no agent data."""
        result = await AgentConfig.configure_agent_and_tools(None)
        assert result is None

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_tool_factory_error(self, mock_tool_factory_class, mock_agent_data):
        """Test agent configuration when tool factory initialization fails."""
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize.side_effect = Exception("Tool factory error")
        mock_tool_factory_class.return_value = mock_tool_factory

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(mock_agent_data)

        assert result is not None
        mock_tool_factory.initialize.assert_called_once()

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_from_flow_nodes(self, mock_tool_factory_class, mock_flow_data, mock_tool_factory):
        """Test agent configuration using tools from flow nodes."""
        mock_tool_factory_class.return_value = mock_tool_factory

        # Agent without direct tools
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        agent_data.role = "Data Analyst"
        agent_data.goal = "Analyze data"
        agent_data.backstory = "An experienced data analyst"
        agent_data.tools = []  # No direct tools

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(agent_data, mock_flow_data)

        assert result is not None

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_exception(self, mock_tool_factory_class, mock_agent_data):
        """Test agent configuration with exception."""
        mock_tool_factory_class.side_effect = Exception("Configuration error")

        result = await AgentConfig.configure_agent_and_tools(mock_agent_data)

        assert result is None

    def test_normalize_tools_list_from_list(self):
        """Test normalizing tools list from list input."""
        tools_data = ["tool-1", "tool-2", 123]
        result = AgentConfig._normalize_tools_list(tools_data)
        assert result == ["tool-1", "tool-2", "123"]

    def test_normalize_tools_list_from_string(self):
        """Test normalizing tools list from JSON string input."""
        tools_data = '["tool-1", "tool-2"]'
        result = AgentConfig._normalize_tools_list(tools_data)
        assert result == ["tool-1", "tool-2"]

    def test_normalize_tools_list_invalid_string(self):
        """Test normalizing tools list from invalid JSON string."""
        tools_data = 'invalid json'
        result = AgentConfig._normalize_tools_list(tools_data)
        assert result == []

    def test_normalize_tools_list_empty(self):
        """Test normalizing empty tools list."""
        result = AgentConfig._normalize_tools_list([])
        assert result == []

    @pytest.mark.asyncio
    async def test_normalize_tools_list_other_types(self):
        """Test normalizing tools list with other data types."""
        # Test with None
        result = AgentConfig._normalize_tools_list(None)
        assert result == []

        # Test with integer
        result = AgentConfig._normalize_tools_list(123)
        assert result == []

        # Test with dict
        result = AgentConfig._normalize_tools_list({"tool1": "value"})
        assert result == []

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_no_tools_attribute(self, mock_tool_factory_class):
        """Test configuring agent without tools attribute."""
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory_class.return_value = mock_tool_factory

        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        del agent_data.tools  # No tools attribute

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(agent_data)

        assert result is not None

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_none_tools_attribute(self, mock_tool_factory_class):
        """Test configuring agent with None tools attribute."""
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory_class.return_value = mock_tool_factory

        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.tools = None  # None tools

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(agent_data)

        assert result is not None

    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_flow_with_no_nodes_attr(self, mock_tool_factory_class):
        """Test configuring agent with flow data that has no nodes attribute."""
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory_class.return_value = mock_tool_factory

        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.tools = []  # Empty tools

        flow_data = MagicMock()
        del flow_data.nodes  # No nodes attribute

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(agent_data, flow_data)

        assert result is not None


class TestAgentConfigIntegration:
    """Integration tests for AgentConfig."""

    @patch('src.engines.crewai.flow.modules.agent_adapter.LoggerManager')
    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_full_agent_configuration_flow(self, mock_tool_factory_class, mock_logger_manager):
        """Test the complete agent configuration flow (delegates to shared build_agent)."""
        # Setup mocks
        mock_logger = MagicMock()
        mock_logger_manager.get_instance.return_value.crew = mock_logger

        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory.create_tool = MagicMock(return_value=MagicMock())
        mock_tool_factory_class.return_value = mock_tool_factory

        mock_agent_instance = MagicMock()

        # Create agent data
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        agent_data.role = "Data Analyst"
        agent_data.goal = "Analyze data"
        agent_data.backstory = "An experienced data analyst"
        agent_data.allow_delegation = True
        agent_data.tools = ["tool-1", "tool-2"]
        agent_data.llm = "gpt-4"
        agent_data.memory = True
        agent_data.max_iter = 10
        agent_data.max_rpm = 5
        agent_data.config = {"temperature": 0.7}

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=mock_agent_instance) as mock_build:
            result = await AgentConfig.configure_agent_and_tools(agent_data)

        assert result == mock_agent_instance
        mock_tool_factory.initialize.assert_called_once()
        mock_build.assert_called_once()

    @patch('src.engines.crewai.flow.modules.agent_adapter.LoggerManager')
    @patch('src.engines.crewai.flow.modules.agent_adapter.ToolFactory')
    @pytest.mark.asyncio
    async def test_full_agent_configuration_with_date_awareness(self, mock_tool_factory_class, mock_logger_manager):
        """Date awareness params flow through the spec into the shared build_agent."""
        # Setup mocks
        mock_logger = MagicMock()
        mock_logger_manager.get_instance.return_value.crew = mock_logger

        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory.create_tool = MagicMock(return_value=MagicMock())
        mock_tool_factory_class.return_value = mock_tool_factory

        mock_agent_instance = MagicMock()

        # Create agent data with date awareness parameters
        agent_data = MagicMock()
        agent_data.id = "agent-456"
        agent_data.name = "Date Aware Agent"
        agent_data.role = "Research Analyst"
        agent_data.goal = "Research and analyze current events"
        agent_data.backstory = "An analyst who needs to know the current date"
        agent_data.allow_delegation = False
        agent_data.tools = ["tool-1"]
        agent_data.llm = "gpt-4"
        agent_data.memory = True
        agent_data.max_iter = 5
        agent_data.config = {}
        # Date awareness params
        agent_data.inject_date = True
        agent_data.date_format = "%Y-%m-%d"

        with patch('src.engines.crewai.common.agent_tools.build_agent_with_tools',
                   new_callable=AsyncMock, return_value=mock_agent_instance) as mock_build:
            result = await AgentConfig.configure_agent_and_tools(agent_data)

        assert result == mock_agent_instance
        mock_build.assert_called_once()

        # Date awareness params are carried in the spec passed to the shared builder.
        spec = mock_build.call_args.args[0]
        assert spec.get("inject_date") is True
        assert spec.get("date_format") == "%Y-%m-%d"
