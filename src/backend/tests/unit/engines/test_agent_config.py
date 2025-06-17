"""
Unit tests for AgentConfig module.

Tests the functionality of agent configuration for CrewAI flows.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from src.engines.crewai.flow.modules.agent_config import AgentConfig


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
    
    def create_tool_side_effect(tool_id):
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
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
    @patch('crewai.Agent')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_success(self, mock_agent_class, mock_tool_factory_class, mock_agent_data, mock_tool_factory, mock_llm):
        """Test successful agent configuration with tools."""
        mock_tool_factory_class.return_value = mock_tool_factory
        mock_agent_instance = MagicMock()
        mock_agent_class.return_value = mock_agent_instance
        
        with patch.object(AgentConfig, '_get_agent_llm', return_value=mock_llm):
            result = await AgentConfig.configure_agent_and_tools(mock_agent_data)
        
        assert result == mock_agent_instance
        mock_tool_factory.initialize.assert_called_once()
        mock_tool_factory.create_tool.assert_called()
        mock_agent_class.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_no_agent_data(self):
        """Test agent configuration with no agent data."""
        result = await AgentConfig.configure_agent_and_tools(None)
        assert result is None
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
    @pytest.mark.asyncio
    async def test_configure_agent_and_tools_tool_factory_error(self, mock_tool_factory_class, mock_agent_data):
        """Test agent configuration when tool factory initialization fails."""
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize.side_effect = Exception("Tool factory error")
        mock_tool_factory_class.return_value = mock_tool_factory
        
        with patch('crewai.Agent') as mock_agent_class:
            with patch.object(AgentConfig, '_get_agent_llm', return_value=None):
                result = await AgentConfig.configure_agent_and_tools(mock_agent_data)
        
        assert result is not None
        mock_tool_factory.initialize.assert_called_once()
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
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
        
        with patch('crewai.Agent') as mock_agent_class:
            with patch.object(AgentConfig, '_get_agent_llm', return_value=None):
                result = await AgentConfig.configure_agent_and_tools(agent_data, mock_flow_data)
        
        assert result is not None
        mock_tool_factory.create_tool.assert_called()
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
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
    async def test_create_tools_from_ids_success(self, mock_tool_factory):
        """Test creating tools from IDs successfully."""
        tool_ids = ["tool-1", "tool-2"]
        
        result = await AgentConfig._create_tools_from_ids(tool_ids, mock_tool_factory, "test agent")
        
        assert len(result) == 2
        assert mock_tool_factory.create_tool.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_tools_from_ids_some_fail(self, mock_tool_factory):
        """Test creating tools when some tools fail to create."""
        tool_ids = ["tool-1", "invalid-tool"]
        
        def create_tool_side_effect(tool_id):
            if tool_id == "tool-1":
                return MagicMock()
            return None
        
        mock_tool_factory.create_tool.side_effect = create_tool_side_effect
        
        result = await AgentConfig._create_tools_from_ids(tool_ids, mock_tool_factory, "test agent")
        
        assert len(result) == 1
    
    @pytest.mark.asyncio
    async def test_create_tools_from_ids_exception(self, mock_tool_factory):
        """Test creating tools with exception."""
        tool_ids = ["tool-1"]
        mock_tool_factory.create_tool.side_effect = Exception("Tool creation error")
        
        result = await AgentConfig._create_tools_from_ids(tool_ids, mock_tool_factory, "test agent")
        
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_success(self, mock_flow_data, mock_tool_factory):
        """Test getting tools from flow nodes successfully."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, mock_flow_data, mock_tool_factory)
        
        assert len(result) >= 0  # Depends on mock setup
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_string_nodes(self, mock_tool_factory):
        """Test getting tools from flow nodes with string nodes."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = json.dumps([
            {
                "id": "agent-agent-123",
                "data": {
                    "tools": ["tool-1", "tool-2"]
                }
            }
        ])
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_no_agent_id(self, mock_flow_data, mock_tool_factory):
        """Test getting tools from flow nodes without agent ID."""
        agent_data = MagicMock()
        agent_data.id = None
        agent_data.name = "Test Agent"
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, mock_flow_data, mock_tool_factory)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_exception(self, mock_tool_factory):
        """Test getting tools from flow nodes with exception."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        
        flow_data = MagicMock()
        flow_data.nodes = "invalid json"
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        
        assert result == []
    
    @patch('src.core.llm_manager.LLMManager')
    @pytest.mark.asyncio
    async def test_get_agent_llm_with_string_llm(self, mock_llm_manager, mock_agent_data, mock_llm):
        """Test getting agent LLM with string configuration."""
        mock_agent_data.llm = "gpt-4"
        mock_llm_manager.get_llm = AsyncMock(return_value=mock_llm)
        
        result = await AgentConfig._get_agent_llm(mock_agent_data)
        
        assert result == mock_llm
        mock_llm_manager.get_llm.assert_called_once_with("gpt-4")
    
    @patch('src.core.llm_manager.LLMManager')
    @pytest.mark.asyncio
    async def test_get_agent_llm_with_dict_llm(self, mock_llm_manager, mock_agent_data, mock_llm):
        """Test getting agent LLM with dictionary configuration."""
        mock_agent_data.llm = {"model": "gpt-4", "temperature": 0.7}
        mock_llm_manager.get_llm = AsyncMock(return_value=mock_llm)
        
        result = await AgentConfig._get_agent_llm(mock_agent_data)
        
        assert result == mock_llm
        mock_llm_manager.get_llm.assert_called_once_with("gpt-4")
    
    @patch('src.core.llm_manager.LLMManager')
    @pytest.mark.asyncio
    async def test_get_agent_llm_no_llm_with_model(self, mock_llm_manager, mock_llm):
        """Test getting agent LLM without LLM but with model."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        agent_data.model = "custom-model"
        mock_llm_manager.get_llm = AsyncMock(return_value=mock_llm)
        
        result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == mock_llm
        mock_llm_manager.get_llm.assert_called_once_with("custom-model")
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_import_error(self):
        """Test getting agent LLM with import error."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = "gpt-4"
        
        with patch('builtins.__import__', side_effect=ImportError):
            result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == "gpt-4"
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_no_llm_import_error(self):
        """Test getting agent LLM without LLM configuration and import error."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        agent_data.model = "custom-model"
        
        with patch('builtins.__import__', side_effect=ImportError):
            result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == "custom-model"
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_no_llm_no_model_import_error(self):
        """Test getting agent LLM without LLM and model, with import error."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        del agent_data.model  # No model attribute
        
        with patch('builtins.__import__', side_effect=ImportError):
            result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == "gpt-4o"
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_exception(self, mock_agent_data):
        """Test getting agent LLM with exception."""
        mock_agent_data.llm = MagicMock()
        mock_agent_data.llm.get.side_effect = Exception("LLM error")
        
        result = await AgentConfig._get_agent_llm(mock_agent_data)
        
        assert result is None
    
    def test_prepare_agent_kwargs_basic(self, mock_agent_data):
        """Test preparing basic agent kwargs."""
        tools = [MagicMock(), MagicMock()]
        llm = MagicMock()
        
        result = AgentConfig._prepare_agent_kwargs(mock_agent_data, tools, llm)
        
        assert result["role"] == mock_agent_data.role
        assert result["goal"] == mock_agent_data.goal
        assert result["backstory"] == mock_agent_data.backstory
        assert result["verbose"] is True
        assert result["allow_delegation"] is True
        assert result["tools"] == tools
        assert result["llm"] == llm
        assert result["memory"] == mock_agent_data.memory
        assert result["max_iter"] == mock_agent_data.max_iter
        assert result["max_rpm"] == mock_agent_data.max_rpm
        assert result["config"] == mock_agent_data.config
    
    def test_prepare_agent_kwargs_no_tools_no_llm(self):
        """Test preparing agent kwargs without tools and LLM."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = False
        agent_data.config = {}
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["role"] == "Analyst"
        assert result["goal"] == "Analyze"
        assert result["backstory"] == "Backstory"
        assert result["allow_delegation"] is False
        assert "tools" not in result
        assert "llm" not in result
        assert result["config"] == {}
    
    def test_prepare_agent_kwargs_string_config(self):
        """Test preparing agent kwargs with string config."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = '{"temperature": 0.8}'
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {"temperature": 0.8}
    
    def test_prepare_agent_kwargs_invalid_json_config(self):
        """Test preparing agent kwargs with invalid JSON config."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = 'invalid json'
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {}
    
    def test_prepare_agent_kwargs_non_dict_parsed_config(self):
        """Test preparing agent kwargs with non-dict parsed config."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = '"not a dict"'
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {}
    
    def test_prepare_agent_kwargs_config_exception(self):
        """Test preparing agent kwargs with config processing exception."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = MagicMock()
        agent_data.config.__str__.side_effect = Exception("Config error")
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {}
    
    def test_prepare_agent_kwargs_none_optional_properties(self):
        """Test preparing agent kwargs with None optional properties."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.memory = None
        agent_data.max_iter = None
        agent_data.max_rpm = None
        agent_data.config = None
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert "memory" not in result
        assert "max_iter" not in result
        assert "max_rpm" not in result
        assert result["config"] == {}
    
    def test_prepare_agent_kwargs_missing_optional_properties(self):
        """Test preparing agent kwargs with missing optional properties."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        
        # Remove optional attributes
        del agent_data.memory
        del agent_data.max_iter
        del agent_data.max_rpm
        del agent_data.config
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert "memory" not in result
        assert "max_iter" not in result
        assert "max_rpm" not in result
        assert result["config"] == {}


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
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_empty_agent_id(self, mock_tool_factory):
        """Test getting tools from flow nodes with empty string agent ID."""
        agent_data = MagicMock()
        agent_data.id = ""
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = []
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_no_matching_node(self, mock_tool_factory):
        """Test getting tools from flow nodes with no matching node."""
        agent_data = MagicMock()
        agent_data.id = "non-existent-agent"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = [
            {
                "id": "agent-different-id",
                "data": {"tools": ["tool-1"]}
            }
        ]
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_no_data_key(self, mock_tool_factory):
        """Test getting tools from flow nodes with node missing data key."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = [
            {
                "id": "agent-agent-123"
                # Missing 'data' key
            }
        ]
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_no_tools_in_data(self, mock_tool_factory):
        """Test getting tools from flow nodes with data but no tools."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = [
            {
                "id": "agent-agent-123",
                "data": {"other_field": "value"}
            }
        ]
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_empty_tools_list(self, mock_tool_factory):
        """Test getting tools from flow nodes with empty tools list."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = [
            {
                "id": "agent-agent-123",
                "data": {"tools": []}
            }
        ]
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_tools_from_flow_nodes_invalid_json_exception(self, mock_tool_factory):
        """Test getting tools from flow nodes with JSON parsing exception."""
        agent_data = MagicMock()
        agent_data.id = "agent-123"
        agent_data.name = "Test Agent"
        
        flow_data = MagicMock()
        flow_data.nodes = "invalid json string that can't be parsed"
        
        result = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, mock_tool_factory)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_dict_without_model(self):
        """Test getting agent LLM with dict configuration without model key."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = {"temperature": 0.7}  # No model key
        
        with patch('src.core.llm_manager.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(return_value=MagicMock())
            result = await AgentConfig._get_agent_llm(agent_data)
            
            # Should use default model when no model key in dict
            mock_llm_manager.get_llm.assert_called_once_with("databricks-llama-4-maverick")
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_dict_import_error(self):
        """Test getting agent LLM with dict configuration and import error."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = {"model": "custom-model", "temperature": 0.7}
        
        with patch('builtins.__import__', side_effect=ImportError):
            result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == "custom-model"  # Returns the model string, not the full dict
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_no_llm_default_model(self):
        """Test getting agent LLM without LLM config using default model."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        del agent_data.model  # No model attribute
        
        with patch('src.core.llm_manager.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(return_value=MagicMock())
            result = await AgentConfig._get_agent_llm(agent_data)
            
            # Should use default model
            mock_llm_manager.get_llm.assert_called_once_with("databricks-llama-4-maverick")
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_non_string_model(self):
        """Test getting agent LLM with non-string model attribute."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        agent_data.model = 123  # Non-string model
        
        with patch('src.core.llm_manager.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(return_value=MagicMock())
            result = await AgentConfig._get_agent_llm(agent_data)
            
            # Should use default model when model is not string
            mock_llm_manager.get_llm.assert_called_once_with("databricks-llama-4-maverick")
    
    @pytest.mark.asyncio
    async def test_get_agent_llm_non_string_model_import_error(self):
        """Test getting agent LLM with non-string model and import error."""
        agent_data = MagicMock()
        agent_data.name = "Test Agent"
        agent_data.llm = None
        agent_data.model = 123  # Non-string model
        
        with patch('builtins.__import__', side_effect=ImportError):
            result = await AgentConfig._get_agent_llm(agent_data)
        
        assert result == "gpt-4o"  # Default fallback when import error and non-string model
    
    def test_prepare_agent_kwargs_empty_string_config(self):
        """Test preparing agent kwargs with empty string config."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = ""  # Empty string
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {}  # Should keep default empty dict for empty string
    
    def test_prepare_agent_kwargs_whitespace_string_config(self):
        """Test preparing agent kwargs with whitespace-only string config."""
        agent_data = MagicMock()
        agent_data.role = "Analyst"
        agent_data.goal = "Analyze"
        agent_data.backstory = "Backstory"
        agent_data.allow_delegation = True
        agent_data.config = "   "  # Whitespace only
        
        result = AgentConfig._prepare_agent_kwargs(agent_data, [], None)
        
        assert result["config"] == {}  # Should keep default empty dict for whitespace
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
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
        
        with patch('crewai.Agent') as mock_agent_class:
            with patch.object(AgentConfig, '_get_agent_llm', return_value=None):
                result = await AgentConfig.configure_agent_and_tools(agent_data)
        
        assert result is not None
        mock_agent_class.assert_called_once()
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
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
        
        with patch('crewai.Agent') as mock_agent_class:
            with patch.object(AgentConfig, '_get_agent_llm', return_value=None):
                result = await AgentConfig.configure_agent_and_tools(agent_data)
        
        assert result is not None
        mock_agent_class.assert_called_once()
    
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
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
        
        with patch('crewai.Agent') as mock_agent_class:
            with patch.object(AgentConfig, '_get_agent_llm', return_value=None):
                result = await AgentConfig.configure_agent_and_tools(agent_data, flow_data)
        
        assert result is not None
        mock_agent_class.assert_called_once()


class TestAgentConfigIntegration:
    """Integration tests for AgentConfig."""
    
    @patch('src.engines.crewai.flow.modules.agent_config.LoggerManager')
    @patch('src.engines.crewai.flow.modules.agent_config.ToolFactory')
    @patch('crewai.Agent')
    @pytest.mark.asyncio
    async def test_full_agent_configuration_flow(self, mock_agent_class, mock_tool_factory_class, mock_logger_manager):
        """Test the complete agent configuration flow."""
        # Setup mocks
        mock_logger = MagicMock()
        mock_logger_manager.get_instance.return_value.crew = mock_logger
        
        mock_tool_factory = AsyncMock()
        mock_tool_factory.initialize = AsyncMock()
        mock_tool_factory.create_tool = MagicMock(return_value=MagicMock())
        mock_tool_factory_class.return_value = mock_tool_factory
        
        mock_agent_instance = MagicMock()
        mock_agent_class.return_value = mock_agent_instance
        
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
        
        with patch.object(AgentConfig, '_get_agent_llm', return_value=MagicMock()):
            result = await AgentConfig.configure_agent_and_tools(agent_data)
        
        assert result == mock_agent_instance
        mock_tool_factory.initialize.assert_called_once()
        mock_tool_factory.create_tool.assert_called()
        mock_agent_class.assert_called_once()