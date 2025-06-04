"""
Unit tests for ToolService.

Tests the functionality of the tool service including
creating, updating, deleting, and managing tools.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.tool_service import ToolService
from src.schemas.tool import ToolCreate, ToolUpdate
from src.models.tool import Tool
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_tool_repository():
    """Create a mock tool repository."""
    repo = AsyncMock()
    
    # Create mock tool objects
    mock_tool = MagicMock(spec=Tool)
    mock_tool.id = uuid.uuid4()
    mock_tool.title = "Web Search Tool"
    mock_tool.description = "Search the web for information"
    mock_tool.config = {"api_key": "test_key", "max_results": 10}
    mock_tool.result_as_answer = False
    mock_tool.created_at = datetime.now(UTC)
    mock_tool.updated_at = datetime.now(UTC)
    mock_tool.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_tool
    repo.list.return_value = [mock_tool]
    repo.create.return_value = mock_tool
    repo.update.return_value = mock_tool
    repo.delete.return_value = True
    repo.get_by_title.return_value = mock_tool
    repo.search.return_value = [mock_tool]
    
    return repo


@pytest.fixture
def tool_create_data():
    """Create test data for tool creation."""
    return ToolCreate(
        title="Test Tool",
        description="A test tool for unit testing",
        config={
            "api_key": "test_api_key",
            "max_results": 10,
            "timeout": 30
        },
        result_as_answer=False
    )


@pytest.fixture
def tool_update_data():
    """Create test data for tool updates."""
    return ToolUpdate(
        title="Updated Tool",
        description="Updated tool description",
        config={"api_key": "updated_key", "max_results": 20}
    )


class TestToolService:
    """Test cases for ToolService."""
    
    @pytest.mark.asyncio
    async def test_create_tool_success(self, mock_uow, mock_tool_repository, tool_create_data):
        """Test successful tool creation."""
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.create(tool_create_data)
            
            assert result is not None
            assert result.title == "Web Search Tool"
            mock_tool_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tool_validation_error(self, mock_uow, mock_tool_repository):
        """Test tool creation with invalid data."""
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            # Test with invalid data (empty title)
            invalid_data = ToolCreate(
                title="",  # Empty title should fail validation
                description="Test description",
                config={},
                result_as_answer=False
            )
            
            mock_tool_repository.create.side_effect = ValueError("Title cannot be empty")
            
            with pytest.raises(ValueError, match="Title cannot be empty"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_tool_by_id(self, mock_uow, mock_tool_repository):
        """Test getting a tool by ID."""
        tool_id = uuid.uuid4()
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.get(tool_id)
            
            assert result is not None
            assert result.title == "Web Search Tool"
            mock_tool_repository.get.assert_called_once_with(tool_id)
    
    @pytest.mark.asyncio
    async def test_get_tool_not_found(self, mock_uow, mock_tool_repository):
        """Test getting a non-existent tool."""
        tool_id = uuid.uuid4()
        mock_tool_repository.get.return_value = None
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.get(tool_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_tool_success(self, mock_uow, mock_tool_repository, tool_update_data):
        """Test successful tool update."""
        tool_id = uuid.uuid4()
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.update(tool_id, tool_update_data)
            
            assert result is not None
            mock_tool_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_tool_success(self, mock_uow, mock_tool_repository):
        """Test successful tool deletion."""
        tool_id = uuid.uuid4()
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.delete(tool_id)
            
            assert result is True
            mock_tool_repository.delete.assert_called_once_with(tool_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_tools(self, mock_uow, mock_tool_repository):
        """Test listing all tools."""
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].title == "Web Search Tool"
            mock_tool_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_tools_static(self, mock_tool_repository):
        """Test static method for getting all tools."""
        mock_tool_repository.list.return_value = [mock_tool_repository.get.return_value]
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository), \
             patch("src.services.tool_service.get_db_session") as mock_get_session:
            
            mock_session = AsyncMock()
            mock_get_session.return_value.__aenter__.return_value = mock_session
            
            result = await ToolService.get_all_tools_static()
            
            assert result is not None
            assert hasattr(result, 'tools')
    
    @pytest.mark.asyncio
    async def test_update_tool_static(self, mock_tool_repository):
        """Test static method for updating tool."""
        tool_id = 1
        update_data = {"title": "Updated Tool"}
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository), \
             patch("src.services.tool_service.get_db_session") as mock_get_session:
            
            mock_session = AsyncMock()
            mock_get_session.return_value.__aenter__.return_value = mock_session
            
            result = await ToolService.update_tool_static(tool_id, update_data)
            
            assert result is not None
            mock_tool_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_tool_configuration_by_title_static(self, mock_tool_repository):
        """Test static method for updating tool configuration by title."""
        tool_title = "Web Search Tool"
        config_update = {"api_key": "new_key"}
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository), \
             patch("src.services.tool_service.get_db_session") as mock_get_session:
            
            mock_session = AsyncMock()
            mock_get_session.return_value.__aenter__.return_value = mock_session
            
            result = await ToolService.update_tool_configuration_by_title_static(
                tool_title, config_update
            )
            
            assert result is not None
            mock_tool_repository.get_by_title.assert_called_once_with(tool_title)
    
    @pytest.mark.asyncio
    async def test_validate_tool_config(self, mock_uow):
        """Test validation of tool configuration."""
        service = ToolService(mock_uow)
        
        # Test valid configuration
        valid_config = {
            "api_key": "test_key",
            "max_results": 10,
            "timeout": 30
        }
        
        service._validate_config(valid_config)  # Should not raise
        
        # Test invalid configuration (missing required field)
        invalid_config = {
            "max_results": 10
            # Missing api_key
        }
        
        with pytest.raises(ValueError, match="Missing required configuration"):
            service._validate_config(invalid_config)
    
    @pytest.mark.asyncio
    async def test_validate_config_types(self, mock_uow):
        """Test validation of configuration value types."""
        service = ToolService(mock_uow)
        
        # Test invalid types
        invalid_config = {
            "api_key": "test_key",
            "max_results": "should_be_integer",  # Wrong type
            "timeout": 30
        }
        
        with pytest.raises(ValueError, match="Invalid configuration type"):
            service._validate_config(invalid_config)
    
    @pytest.mark.asyncio
    async def test_tool_activation_deactivation(self, mock_uow, mock_tool_repository):
        """Test tool activation and deactivation."""
        tool_id = uuid.uuid4()
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            # Test activation
            result = await service.activate(tool_id)
            assert result is not None
            
            # Test deactivation
            result = await service.deactivate(tool_id)
            assert result is not None
            
            # Verify repository calls
            assert mock_tool_repository.update.call_count == 2
    
    @pytest.mark.asyncio
    async def test_tool_configuration_encryption(self, mock_uow, mock_tool_repository):
        """Test tool configuration encryption for sensitive data."""
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            # Test configuration with sensitive data
            sensitive_config = {
                "api_key": "sensitive_key",
                "password": "secret_password",
                "max_results": 10
            }
            
            encrypted_config = service._encrypt_sensitive_config(sensitive_config)
            
            # Verify sensitive fields are encrypted
            assert encrypted_config["api_key"] != "sensitive_key"
            assert encrypted_config["password"] != "secret_password"
            assert encrypted_config["max_results"] == 10  # Non-sensitive field unchanged
    
    @pytest.mark.asyncio
    async def test_tool_export_import(self, mock_uow, mock_tool_repository):
        """Test tool export and import functionality."""
        tool_id = uuid.uuid4()
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            # Test export
            export_data = await service.export_tool(tool_id)
            
            assert export_data is not None
            assert "title" in export_data
            assert "description" in export_data
            assert "config" in export_data
            
            # Test import
            imported_tool = await service.import_tool(export_data)
            
            assert imported_tool is not None
            mock_tool_repository.create.assert_called()
    
    @pytest.mark.asyncio
    async def test_tool_metrics(self, mock_uow, mock_tool_repository):
        """Test tool metrics collection."""
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            metrics = await service.get_tool_metrics()
            
            assert "total_tools" in metrics
            assert "active_tools" in metrics
            assert "tools_by_category" in metrics
            assert metrics["total_tools"] >= 0
            assert metrics["active_tools"] >= 0
    
    @pytest.mark.asyncio
    async def test_tool_usage_tracking(self, mock_uow, mock_tool_repository):
        """Test tool usage tracking."""
        tool_id = uuid.uuid4()
        usage_data = {
            "execution_count": 5,
            "success_rate": 0.8,
            "average_execution_time": 2.5
        }
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            result = await service.update_usage_metrics(tool_id, usage_data)
            
            assert result is not None
            mock_tool_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_duplicate_tool_title(self, mock_uow, mock_tool_repository, tool_create_data):
        """Test creating tool with duplicate title."""
        # Mock repository to return existing tool with same title
        mock_tool_repository.get_by_title.return_value = MagicMock()
        mock_tool_repository.create.side_effect = ValueError("Tool title already exists")
        
        with patch("src.services.tool_service.ToolRepository", return_value=mock_tool_repository):
            service = ToolService(mock_uow)
            
            with pytest.raises(ValueError, match="Tool title already exists"):
                await service.create(tool_create_data)
    
    @pytest.mark.asyncio
    async def test_tool_compatibility_check(self, mock_uow):
        """Test tool compatibility checking."""
        service = ToolService(mock_uow)
        
        # Test compatible configuration
        compatible_config = {
            "api_version": "v2",
            "required_features": ["search", "filter"]
        }
        
        assert service._check_compatibility(compatible_config) is True
        
        # Test incompatible configuration
        incompatible_config = {
            "api_version": "v1",  # Outdated version
            "required_features": ["deprecated_feature"]
        }
        
        assert service._check_compatibility(incompatible_config) is False