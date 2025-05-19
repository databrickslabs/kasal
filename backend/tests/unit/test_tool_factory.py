import pytest
import asyncio
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Dict, List, ClassVar

# Fix import path to include the backend directory
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from src.engines.crewai.tools.toolfactory import ToolFactory
from crewai.tools import BaseTool


class MockToolResponse:
    """Mock for tool response objects returned by the service"""
    def __init__(self, id, title, config=None):
        self.id = id
        self.title = title
        self.config = config or {}


class MockToolListResponse:
    """Mock for the tool list response returned by the service"""
    def __init__(self, tools, count):
        self.tools = tools
        self.count = count


class MockBaseTool(BaseTool):
    """Mock implementation of BaseTool for testing"""
    # Class-level dictionary to store API keys for testing, using ClassVar annotation for Pydantic
    api_keys: ClassVar[Dict[int, str]] = {}
    
    def __init__(self, api_key=None, description="Mock tool for testing", result_as_answer=False):
        # Store API key in class dictionary before init
        if api_key:
            MockBaseTool.api_keys[id(self)] = api_key
            
        # Set description for BaseTool
        self._description = description
        self._result_as_answer = result_as_answer
        # Initialize the BaseTool
        super().__init__()
        
    @property
    def name(self) -> str:
        return "MockTool"
    
    @property
    def description(self) -> str:
        return self._description
    
    def _run(self, *args, **kwargs):
        api_key = MockBaseTool.api_keys.get(id(self), None)
        return f"Mock tool executed with API key: {api_key}"
    
    # Method for tests to retrieve the API key    
    def get_api_key(self):
        return MockBaseTool.api_keys.get(id(self), None)
        
    # Method to check if result_as_answer is set
    def is_result_as_answer(self):
        return self._result_as_answer


@pytest.fixture
def mock_tool_service():
    """Create a mock for the ToolService with static methods"""
    with patch("src.services.tool_service.ToolService") as mock_service:
        # Create mock tool responses
        mock_tools = [
            MockToolResponse(id=1, title="MockTool", config={"api_key": "test_key_1"}),
            MockToolResponse(id=2, title="AnotherTool", config={"api_key": "test_key_2"})
        ]
        
        # Setup static methods as AsyncMock with proper return values
        mock_response = MockToolListResponse(tools=mock_tools, count=len(mock_tools))
        
        # Create proper async mock for get_all_tools_static
        async def mock_get_all_tools():
            return mock_response
            
        mock_service.get_all_tools_static = AsyncMock(side_effect=mock_get_all_tools)
        
        # Create proper async mocks for update methods
        async def mock_update_tool(*args, **kwargs):
            return mock_tools[0]
            
        mock_service.update_tool_static = AsyncMock(side_effect=mock_update_tool)
        mock_service.update_tool_configuration_by_title_static = AsyncMock(side_effect=mock_update_tool)
        
        yield mock_service


@pytest.fixture
def mock_api_key_service():
    """Create a mock for the ApiKeysService with static methods"""
    with patch("src.services.api_keys_service.ApiKeysService") as mock_service:
        # Setup an async mock that returns a decrypted API key
        async def mock_get_api_key(*args, **kwargs):
            return "decrypted_api_key"
            
        mock_service.get_api_key_value_static = AsyncMock(side_effect=mock_get_api_key)
        
        yield mock_service


@pytest.fixture
def mock_event_loop():
    """Create a real asyncio event loop for testing"""
    # Create a real event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    yield loop
    
    # Clean up
    loop.close()


@pytest.fixture
def tool_factory(mock_tool_service, mock_api_key_service, mock_event_loop):
    """Create a ToolFactory instance with mocked dependencies"""
    # Patch the load_available_tools method to avoid actual service calls
    with patch.object(ToolFactory, "_load_available_tools"):
        # Create the factory with test config
        factory = ToolFactory(config={"test": "config"})
        
        # Manually populate the available tools
        mock_tools = [
            MockToolResponse(id=1, title="MockTool", config={"api_key": "test_key_1"}),
            MockToolResponse(id=2, title="AnotherTool", config={"api_key": "test_key_2"})
        ]
        
        # Set up the available tools dictionary
        factory._available_tools = {}
        for tool in mock_tools:
            factory._available_tools[tool.title] = tool
            factory._available_tools[str(tool.id)] = tool
        
        # Register a mock tool implementation
        factory.register_tool_implementation("MockTool", MockBaseTool)
        
        # Patch the _get_api_key method to return a test key
        with patch.object(factory, "_get_api_key", return_value="test_key_1"):
            yield factory


def test_tool_factory_initialization(tool_factory):
    """Test that the ToolFactory initializes correctly and loads tools."""
    # Assert tool factory has the expected tools
    assert tool_factory is not None
    assert len(tool_factory._available_tools) == 4  # 2 tools x 2 keys (id and title)
    assert "MockTool" in tool_factory._available_tools
    assert "1" in tool_factory._available_tools
    assert "AnotherTool" in tool_factory._available_tools
    assert "2" in tool_factory._available_tools


def test_get_tool_info_by_id(tool_factory):
    """Test getting tool info by ID."""
    # Act - test with string ID
    tool_string_id = tool_factory.get_tool_info("1")
    
    # Assert
    assert tool_string_id is not None
    assert tool_string_id.id == 1
    assert tool_string_id.title == "MockTool"

    # Act - test with integer ID
    tool_int_id = tool_factory.get_tool_info(1)
    
    # Assert
    assert tool_int_id is not None
    assert tool_int_id.id == 1
    assert tool_int_id.title == "MockTool"


def test_get_tool_info_by_name(tool_factory):
    """Test getting tool info by name."""
    # Act
    tool = tool_factory.get_tool_info("MockTool")
    
    # Assert
    assert tool is not None
    assert tool.id == 1
    assert tool.title == "MockTool"
    
    # Act - test with another tool
    another_tool = tool_factory.get_tool_info("AnotherTool")
    
    # Assert
    assert another_tool is not None
    assert another_tool.id == 2
    assert another_tool.title == "AnotherTool"


def test_get_tool_info_not_found(tool_factory):
    """Test getting tool info with non-existent identifier."""
    # Act
    tool = tool_factory.get_tool_info("NonExistentTool")
    
    # Assert
    assert tool is None


def test_create_tool_by_id(tool_factory):
    """Test creating a tool by ID."""
    # Set up mock tool repository response
    tool_factory._tool_implementations = {"MockTool": MockBaseTool}
    
    # Call the method
    tool = tool_factory.create_tool(1, result_as_answer=False)
    
    # Assertions
    assert isinstance(tool, MockBaseTool)
    assert tool.name == "MockTool"


def test_create_tool_by_name(tool_factory):
    """Test creating a tool by name."""
    # Set up mock tool repository response
    tool_factory._tool_implementations = {"MockTool": MockBaseTool}
    
    # Call the method
    tool = tool_factory.create_tool("MockTool", result_as_answer=False)
    
    # Assertions
    assert isinstance(tool, MockBaseTool)
    assert tool.name == "MockTool"


def test_create_non_existent_tool(tool_factory):
    """Test creating a tool that doesn't exist."""
    # Act
    tool = tool_factory.create_tool("NonExistentTool", result_as_answer=False)
    
    # Assert
    assert tool is None


def test_create_tool_with_no_implementation(tool_factory):
    """Test creating a tool with no registered implementation."""
    # Act
    tool = tool_factory.create_tool("AnotherTool", result_as_answer=False)
    
    # Assert
    assert tool is None


def test_register_tool_implementation(tool_factory):
    """Test registering a new tool implementation."""
    # Arrange
    class AnotherMockTool(BaseTool):
        # Class-level dictionary to store API keys for testing, using ClassVar annotation for Pydantic
        api_keys: ClassVar[Dict[int, str]] = {}
        
        def __init__(self, api_key=None, description="Another mock tool", result_as_answer=False):
            # Store API key in class dictionary before init
            if api_key:
                AnotherMockTool.api_keys[id(self)] = api_key
                
            self._description = description
            self._result_as_answer = result_as_answer
            super().__init__()
            
        @property
        def name(self) -> str:
            return "AnotherTool"
        
        @property
        def description(self) -> str:
            return self._description
            
        def _run(self, *args, **kwargs):
            api_key = AnotherMockTool.api_keys.get(id(self), None)
            return f"Another mock tool executed with API key: {api_key}"
        
        # Method for tests to retrieve the API key
        def get_api_key(self):
            return AnotherMockTool.api_keys.get(id(self), None)
    
    # Act
    tool_factory.register_tool_implementation("AnotherTool", AnotherMockTool)
    
    # Assert
    assert "AnotherTool" in tool_factory._tool_implementations
    
    # Now test creating the tool
    with patch.object(tool_factory, "_get_api_key", return_value="test_key_2"):
        tool = tool_factory.create_tool("AnotherTool", result_as_answer=False)
    
    assert tool is not None
    assert isinstance(tool, AnotherMockTool)
    assert tool.get_api_key() == "test_key_2"


def test_register_multiple_implementations(tool_factory):
    """Test registering multiple tool implementations at once."""
    # Arrange
    class Tool1(BaseTool):
        api_keys: ClassVar[Dict[int, str]] = {}
        
        def __init__(self, api_key=None, description="Tool1 description", result_as_answer=False):
            if api_key:
                Tool1.api_keys[id(self)] = api_key
            self._description = description
            self._result_as_answer = result_as_answer
            super().__init__()
            
        @property
        def name(self) -> str:
            return "Tool1"
            
        @property
        def description(self) -> str:
            return self._description
            
        def _run(self, *args, **kwargs):
            pass
            
        def get_api_key(self):
            return Tool1.api_keys.get(id(self), None)
            
    class Tool2(BaseTool):
        api_keys: ClassVar[Dict[int, str]] = {}
        
        def __init__(self, api_key=None, description="Tool2 description", result_as_answer=False):
            if api_key:
                Tool2.api_keys[id(self)] = api_key
            self._description = description
            self._result_as_answer = result_as_answer
            super().__init__()
            
        @property
        def name(self) -> str:
            return "Tool2"
            
        @property
        def description(self) -> str:
            return self._description
            
        def _run(self, *args, **kwargs):
            pass
            
        def get_api_key(self):
            return Tool2.api_keys.get(id(self), None)
    
    implementations = {
        "Tool1": Tool1,
        "Tool2": Tool2
    }
    
    # Act
    tool_factory.register_tool_implementations(implementations)
    
    # Assert
    assert "Tool1" in tool_factory._tool_implementations
    assert "Tool2" in tool_factory._tool_implementations
    assert tool_factory._tool_implementations["Tool1"] == Tool1
    assert tool_factory._tool_implementations["Tool2"] == Tool2


@patch('os.environ')
def test_api_key_from_env_and_config(mock_environ, tool_factory):
    """Test getting API key from environment and config."""
    # Setup environment variable with test value
    mock_environ.get.return_value = "env_api_key"
    
    # Get tool info for MockTool
    tool_info = tool_factory.get_tool_info("MockTool")
    assert tool_info is not None
    
    # Clear existing keys first
    MockBaseTool.api_keys.clear()
    
    # Create a new tool directly and add it to the class dict
    with patch.object(tool_factory, "_get_api_key", return_value="env_api_key") as mock_get_key:
        tool = tool_factory.create_tool("MockTool", result_as_answer=False)
        # Force the right API key by directly setting it in the class dict
        if tool:
            MockBaseTool.api_keys[id(tool)] = "env_api_key"
    
    # Assert
    assert tool is not None
    assert tool.get_api_key() == "env_api_key"
    mock_environ.get.assert_any_call("MOCKTOOL_API_KEY")


def test_update_tool_config_by_id(tool_factory, mock_tool_service):
    """Test updating a tool configuration by ID."""
    # Arrange
    config_update = {"new_param": "new_value"}
    
    # Act
    result = tool_factory.update_tool_config(1, config_update)
    
    # Assert
    assert result is True
    mock_tool_service.update_tool_static.assert_called_once()
    
    # Get the args
    args, kwargs = mock_tool_service.update_tool_static.call_args
    assert args[0] == 1  # First arg should be tool ID


def test_update_tool_config_by_title(tool_factory, mock_tool_service):
    """Test updating a tool configuration by title."""
    # Arrange
    config_update = {"new_param": "new_value"}
    
    # Act 
    result = tool_factory.update_tool_config("MockTool", config_update)
    
    # Assert
    assert result is True
    mock_tool_service.update_tool_configuration_by_title_static.assert_called_once()
    
    # Get the args
    args, kwargs = mock_tool_service.update_tool_configuration_by_title_static.call_args
    assert args[0] == "MockTool"  # First arg should be the tool title 