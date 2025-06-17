"""
Unit tests for ToolService.

Tests the functionality of tool operations including
tool CRUD operations, tool enabling/disabling, and configuration management.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from src.services.tool_service import ToolService
from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse


# Mock models
class MockTool:
    def __init__(self, id=1, title="Test Tool", type="custom", description="Test Description",
                 parameters=None, config=None, enabled=True, icon="default-icon", 
                 created_at=None, updated_at=None):
        from datetime import datetime
        self.id = id
        self.title = title
        self.type = type
        self.description = description
        self.parameters = parameters or {"param1": "value1"}
        self.config = config or {"config1": "setting1"}
        self.enabled = enabled
        self.icon = icon
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_repository():
    """Create a mock ToolRepository."""
    return AsyncMock()


@pytest.fixture
def tool_service(mock_repository):
    """Create a ToolService instance with mock repository."""
    return ToolService(repository=mock_repository)


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def mock_tool():
    """Create a mock tool."""
    return MockTool()


@pytest.fixture
def tool_create_data():
    """Create sample ToolCreate data."""
    return {
        "title": "New Tool",
        "description": "A new test tool",
        "icon": "new-tool-icon",
        "config": {"setting": "value"},
        "enabled": True
    }


@pytest.fixture
def tool_update_data():
    """Create sample ToolUpdate data."""
    return {
        "title": "Updated Tool",
        "description": "An updated tool description",
        "icon": "updated-icon",
        "config": {"new_setting": "new_value"},
        "enabled": False
    }


class TestToolService:
    """Test cases for ToolService."""
    
    def test_tool_service_initialization_with_repository(self, mock_repository):
        """Test ToolService initialization with repository."""
        service = ToolService(repository=mock_repository)
        assert service.repository == mock_repository
    
    def test_tool_service_initialization_with_session(self, mock_session):
        """Test ToolService initialization with session."""
        with patch('src.services.tool_service.ToolRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            
            service = ToolService(session=mock_session)
            
            MockRepository.assert_called_once_with(mock_session)
            assert service.repository == mock_repo
    
    def test_tool_service_initialization_no_args(self):
        """Test ToolService initialization without repository or session."""
        with pytest.raises(ValueError) as exc_info:
            ToolService()
        assert "Either session or repository must be provided" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work(self):
        """Test creating service from unit of work."""
        mock_uow = AsyncMock()
        mock_repo = AsyncMock()
        mock_uow.tool_repository = mock_repo
        
        service = await ToolService.from_unit_of_work(mock_uow)
        
        assert service.repository == mock_repo
    
    @pytest.mark.asyncio
    async def test_get_all_tools_success(self, tool_service, mock_repository):
        """Test successful retrieval of all tools."""
        mock_tools = [MockTool(id=1, title="Tool 1"), MockTool(id=2, title="Tool 2")]
        mock_repository.list.return_value = mock_tools
        
        result = await tool_service.get_all_tools()
        
        assert isinstance(result, ToolListResponse)
        assert result.count == 2
        assert len(result.tools) == 2
        mock_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_tools_empty(self, tool_service, mock_repository):
        """Test retrieval of all tools when no tools exist."""
        mock_repository.list.return_value = []
        
        result = await tool_service.get_all_tools()
        
        assert isinstance(result, ToolListResponse)
        assert result.count == 0
        assert len(result.tools) == 0
    
    @pytest.mark.asyncio
    async def test_get_enabled_tools_success(self, tool_service, mock_repository):
        """Test successful retrieval of enabled tools."""
        mock_tools = [MockTool(id=1, enabled=True), MockTool(id=2, enabled=True)]
        mock_repository.find_enabled.return_value = mock_tools
        
        result = await tool_service.get_enabled_tools()
        
        assert isinstance(result, ToolListResponse)
        assert result.count == 2
        assert len(result.tools) == 2
        mock_repository.find_enabled.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_tool_by_id_success(self, tool_service, mock_repository, mock_tool):
        """Test successful tool retrieval by ID."""
        mock_repository.get.return_value = mock_tool
        
        result = await tool_service.get_tool_by_id(1)
        
        assert isinstance(result, ToolResponse)
        assert result.id == mock_tool.id
        assert result.title == mock_tool.title
        mock_repository.get.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_get_tool_by_id_not_found(self, tool_service, mock_repository):
        """Test tool retrieval by ID when tool not found."""
        mock_repository.get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.get_tool_by_id(999)
        
        assert exc_info.value.status_code == 404
        assert "Tool with ID 999 not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_tool_success(self, tool_service, mock_repository, tool_create_data, mock_tool):
        """Test successful tool creation."""
        mock_repository.create.return_value = mock_tool
        
        tool_create = ToolCreate(**tool_create_data)
        result = await tool_service.create_tool(tool_create)
        
        assert isinstance(result, ToolResponse)
        assert result.id == mock_tool.id
        mock_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tool_error(self, tool_service, mock_repository, tool_create_data):
        """Test tool creation with error."""
        mock_repository.create.side_effect = Exception("Database error")
        
        tool_create = ToolCreate(**tool_create_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.create_tool(tool_create)
        
        assert exc_info.value.status_code == 500
        assert "Failed to create tool" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_tool_success(self, tool_service, mock_repository, tool_update_data, mock_tool):
        """Test successful tool update."""
        mock_repository.get.return_value = mock_tool
        mock_repository.update.return_value = mock_tool
        
        tool_update = ToolUpdate(**tool_update_data)
        result = await tool_service.update_tool(1, tool_update)
        
        assert isinstance(result, ToolResponse)
        assert result.id == mock_tool.id
        mock_repository.get.assert_called_once_with(1)
        mock_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_tool_not_found(self, tool_service, mock_repository, tool_update_data):
        """Test tool update when tool not found."""
        mock_repository.get.return_value = None
        
        tool_update = ToolUpdate(**tool_update_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.update_tool(999, tool_update)
        
        assert exc_info.value.status_code == 404
        assert "Tool with ID 999 not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_tool_error(self, tool_service, mock_repository, tool_update_data, mock_tool):
        """Test tool update with error."""
        mock_repository.get.return_value = mock_tool
        mock_repository.update.side_effect = Exception("Update failed")
        
        tool_update = ToolUpdate(**tool_update_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.update_tool(1, tool_update)
        
        assert exc_info.value.status_code == 500
        assert "Failed to update tool" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_delete_tool_success(self, tool_service, mock_repository, mock_tool):
        """Test successful tool deletion."""
        mock_repository.get.return_value = mock_tool
        mock_repository.delete.return_value = None
        
        result = await tool_service.delete_tool(1)
        
        assert result is True
        mock_repository.get.assert_called_once_with(1)
        mock_repository.delete.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_delete_tool_not_found(self, tool_service, mock_repository):
        """Test tool deletion when tool not found."""
        mock_repository.get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.delete_tool(999)
        
        assert exc_info.value.status_code == 404
        assert "Tool with ID 999 not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_delete_tool_error(self, tool_service, mock_repository, mock_tool):
        """Test tool deletion with error."""
        mock_repository.get.return_value = mock_tool
        mock_repository.delete.side_effect = Exception("Deletion failed")
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.delete_tool(1)
        
        assert exc_info.value.status_code == 500
        assert "Failed to delete tool" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_toggle_tool_enabled_success(self, tool_service, mock_repository, mock_tool):
        """Test successful tool toggle."""
        mock_tool.enabled = False  # Toggled state
        mock_repository.toggle_enabled.return_value = mock_tool
        
        result = await tool_service.toggle_tool_enabled(1)
        
        assert isinstance(result, ToggleResponse)
        assert result.enabled is False
        assert "disabled" in result.message
        mock_repository.toggle_enabled.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_toggle_tool_enabled_not_found(self, tool_service, mock_repository):
        """Test tool toggle when tool not found."""
        mock_repository.toggle_enabled.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.toggle_tool_enabled(999)
        
        assert exc_info.value.status_code == 404
        assert "Tool with ID 999 not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_toggle_tool_enabled_error(self, tool_service, mock_repository):
        """Test tool toggle with error."""
        mock_repository.toggle_enabled.side_effect = Exception("Toggle failed")
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.toggle_tool_enabled(1)
        
        assert exc_info.value.status_code == 500
        assert "Failed to toggle tool" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_tool_config_by_name_success(self, tool_service, mock_repository, mock_tool):
        """Test successful tool config retrieval by name."""
        mock_repository.find_by_title.return_value = mock_tool
        
        result = await tool_service.get_tool_config_by_name("Test Tool")
        
        assert result == mock_tool.config
        mock_repository.find_by_title.assert_called_once_with("Test Tool")
    
    @pytest.mark.asyncio
    async def test_get_tool_config_by_name_not_found(self, tool_service, mock_repository):
        """Test tool config retrieval by name when tool not found."""
        mock_repository.find_by_title.return_value = None
        
        result = await tool_service.get_tool_config_by_name("Nonexistent Tool")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_tool_config_by_name_no_config(self, tool_service, mock_repository):
        """Test tool config retrieval when tool has no config attribute."""
        mock_tool = MockTool()
        delattr(mock_tool, 'config')  # Remove config attribute
        mock_repository.find_by_title.return_value = mock_tool
        
        result = await tool_service.get_tool_config_by_name("Test Tool")
        
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_get_tool_config_by_name_error(self, tool_service, mock_repository):
        """Test tool config retrieval with error."""
        mock_repository.find_by_title.side_effect = Exception("Database error")
        
        result = await tool_service.get_tool_config_by_name("Test Tool")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_tool_configuration_by_title_success(self, tool_service, mock_repository, mock_tool):
        """Test successful tool configuration update by title."""
        new_config = {"new_setting": "new_value"}
        mock_repository.update_configuration_by_title.return_value = mock_tool
        
        result = await tool_service.update_tool_configuration_by_title("Test Tool", new_config)
        
        assert isinstance(result, ToolResponse)
        assert result.id == mock_tool.id
        mock_repository.update_configuration_by_title.assert_called_once_with("Test Tool", new_config)
    
    @pytest.mark.asyncio
    async def test_update_tool_configuration_by_title_not_found(self, tool_service, mock_repository):
        """Test tool configuration update by title when tool not found."""
        new_config = {"new_setting": "new_value"}
        mock_repository.update_configuration_by_title.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.update_tool_configuration_by_title("Nonexistent Tool", new_config)
        
        assert exc_info.value.status_code == 404
        assert "Tool with title 'Nonexistent Tool' not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_tool_configuration_by_title_error(self, tool_service, mock_repository):
        """Test tool configuration update by title with error."""
        new_config = {"new_setting": "new_value"}
        mock_repository.update_configuration_by_title.side_effect = Exception("Update failed")
        
        with pytest.raises(HTTPException) as exc_info:
            await tool_service.update_tool_configuration_by_title("Test Tool", new_config)
        
        assert exc_info.value.status_code == 500
        assert "Failed to update tool configuration by title" in str(exc_info.value.detail)
    
    def test_method_existence(self, tool_service):
        """Test that all expected methods exist."""
        expected_methods = [
            'get_all_tools',
            'get_enabled_tools',
            'get_tool_by_id',
            'create_tool',
            'update_tool',
            'delete_tool',
            'toggle_tool_enabled',
            'get_tool_config_by_name',
            'update_tool_configuration_by_title'
        ]
        
        for method_name in expected_methods:
            assert hasattr(tool_service, method_name)
            assert callable(getattr(tool_service, method_name))
    
    def test_class_method_existence(self):
        """Test that class methods exist."""
        assert hasattr(ToolService, 'from_unit_of_work')
        assert callable(ToolService.from_unit_of_work)
    
    @pytest.mark.asyncio
    async def test_tool_response_validation(self, tool_service, mock_repository, mock_tool):
        """Test that tool responses are properly validated."""
        mock_repository.get.return_value = mock_tool
        
        result = await tool_service.get_tool_by_id(1)
        
        # Verify ToolResponse structure
        assert hasattr(result, 'id')
        assert hasattr(result, 'title')
        assert hasattr(result, 'description')
        assert hasattr(result, 'icon')
        assert hasattr(result, 'enabled')
        assert hasattr(result, 'created_at')
        assert hasattr(result, 'updated_at')
        assert result.id == mock_tool.id
        assert result.title == mock_tool.title
    
    @pytest.mark.asyncio
    async def test_tool_list_response_validation(self, tool_service, mock_repository):
        """Test that tool list responses are properly validated."""
        mock_tools = [MockTool(id=1), MockTool(id=2)]
        mock_repository.list.return_value = mock_tools
        
        result = await tool_service.get_all_tools()
        
        # Verify ToolListResponse structure
        assert hasattr(result, 'tools')
        assert hasattr(result, 'count')
        assert isinstance(result.tools, list)
        assert result.count == len(mock_tools)
    
    @pytest.mark.asyncio
    async def test_toggle_response_validation(self, tool_service, mock_repository, mock_tool):
        """Test that toggle responses are properly validated."""
        mock_tool.enabled = True
        mock_repository.toggle_enabled.return_value = mock_tool
        
        result = await tool_service.toggle_tool_enabled(1)
        
        # Verify ToggleResponse structure
        assert hasattr(result, 'message')
        assert hasattr(result, 'enabled')
        assert isinstance(result.message, str)
        assert isinstance(result.enabled, bool)
        assert result.enabled is True
        assert "enabled" in result.message
    
    @pytest.mark.asyncio
    async def test_error_logging(self, tool_service, mock_repository, tool_create_data):
        """Test that errors are properly logged."""
        mock_repository.create.side_effect = Exception("Database connection failed")
        
        with patch('src.services.tool_service.logger') as mock_logger:
            tool_create = ToolCreate(**tool_create_data)
            with pytest.raises(HTTPException):
                await tool_service.create_tool(tool_create)
        
        # Verify error was logged
        mock_logger.error.assert_called()
        log_call = mock_logger.error.call_args[0][0]
        assert "Failed to create tool" in log_call
    
    @pytest.mark.asyncio
    async def test_update_exclude_unset(self, tool_service, mock_repository, mock_tool):
        """Test that tool update excludes unset fields."""
        mock_repository.get.return_value = mock_tool
        mock_repository.update.return_value = mock_tool
        
        # Create partial update data
        tool_update = ToolUpdate(title="Updated Title")
        await tool_service.update_tool(1, tool_update)
        
        # Verify update was called with exclude_unset data
        call_args = mock_repository.update.call_args
        update_data = call_args[0][1]  # Second argument (update_data)
        
        # Should only contain the title field, not other unset fields
        assert "title" in update_data
        # Other fields should not be present since they weren't set
        assert len(update_data) == 1  # Only title should be in the update data