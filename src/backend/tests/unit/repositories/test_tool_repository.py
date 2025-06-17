"""
Unit tests for ToolRepository.

Tests the functionality of tool repository including
CRUD operations, enabled/disabled filtering, configuration updates, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from src.repositories.tool_repository import ToolRepository, SyncToolRepository
from src.models.tool import Tool


# Mock tool model
class MockTool:
    def __init__(self, id=1, title="Test Tool", description="Test Description",
                 enabled=True, config=None, created_at=None, updated_at=None):
        self.id = id
        self.title = title
        self.description = description
        self.enabled = enabled
        self.config = config or {}
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def tool_repository(mock_async_session):
    """Create a tool repository with async session."""
    return ToolRepository(session=mock_async_session)


@pytest.fixture
def sample_tools():
    """Create sample tools for testing."""
    return [
        MockTool(id=1, title="Tool 1", enabled=True),
        MockTool(id=2, title="Tool 2", enabled=False),
        MockTool(id=3, title="Tool 3", enabled=True)
    ]


@pytest.fixture
def sample_tool_data():
    """Create sample tool data for creation."""
    return {
        "title": "new_tool",
        "description": "A new test tool",
        "enabled": True,
        "config": {"key": "value"}
    }


class TestToolRepositoryInit:
    """Test cases for ToolRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ToolRepository(session=mock_async_session)
        
        assert repository.model == Tool
        assert repository.session == mock_async_session


class TestToolRepositoryFindByTitle:
    """Test cases for find_by_title method."""
    
    @pytest.mark.asyncio
    async def test_find_by_title_success(self, tool_repository, mock_async_session):
        """Test successful tool search by title."""
        tool = MockTool(title="test_tool")
        mock_result = MockResult([tool])
        mock_async_session.execute.return_value = mock_result
        
        result = await tool_repository.find_by_title("test_tool")
        
        assert result == tool
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Tool)))
    
    @pytest.mark.asyncio
    async def test_find_by_title_not_found(self, tool_repository, mock_async_session):
        """Test find by title when tool not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await tool_repository.find_by_title("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_title_exception_handling(self, tool_repository, mock_async_session):
        """Test find by title with database exception."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await tool_repository.find_by_title("test_tool")


class TestToolRepositoryFindEnabled:
    """Test cases for find_enabled method."""
    
    @pytest.mark.asyncio
    async def test_find_enabled_success(self, tool_repository, mock_async_session, sample_tools):
        """Test successful retrieval of enabled tools."""
        enabled_tools = [tool for tool in sample_tools if tool.enabled]
        mock_result = MockResult(enabled_tools)
        mock_async_session.execute.return_value = mock_result
        
        result = await tool_repository.find_enabled()
        
        assert len(result) == len(enabled_tools)
        assert all(tool.enabled for tool in result)
        mock_async_session.execute.assert_called_once()
        
        # Verify the query filters for enabled=True
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Tool)))
    
    @pytest.mark.asyncio
    async def test_find_enabled_no_enabled_tools(self, tool_repository, mock_async_session):
        """Test find enabled when no tools are enabled."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await tool_repository.find_enabled()
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_exception_handling(self, tool_repository, mock_async_session):
        """Test find enabled with database exception."""
        mock_async_session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            await tool_repository.find_enabled()


class TestToolRepositoryToggleEnabled:
    """Test cases for toggle_enabled method."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success(self, tool_repository, mock_async_session):
        """Test successful toggling of tool enabled status."""
        tool = MockTool(id=1, enabled=True)
        
        with patch.object(tool_repository, 'get', return_value=tool):
            result = await tool_repository.toggle_enabled(1)
            
            assert result == tool
            assert tool.enabled is False  # Should be toggled
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(tool)
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_from_false_to_true(self, tool_repository, mock_async_session):
        """Test toggling tool from disabled to enabled."""
        tool = MockTool(id=1, enabled=False)
        
        with patch.object(tool_repository, 'get', return_value=tool):
            result = await tool_repository.toggle_enabled(1)
            
            assert result == tool
            assert tool.enabled is True  # Should be toggled
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(tool)
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_tool_not_found(self, tool_repository, mock_async_session):
        """Test toggle enabled when tool not found."""
        with patch.object(tool_repository, 'get', return_value=None):
            result = await tool_repository.toggle_enabled(999)
            
            assert result is None
            mock_async_session.commit.assert_not_called()
            mock_async_session.refresh.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_database_error(self, tool_repository, mock_async_session):
        """Test toggle enabled with database error during commit."""
        tool = MockTool(id=1, enabled=True)
        
        with patch.object(tool_repository, 'get', return_value=tool):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await tool_repository.toggle_enabled(1)
            
            mock_async_session.rollback.assert_called_once()


class TestToolRepositoryUpdateConfigurationByTitle:
    """Test cases for update_configuration_by_title method."""
    
    @pytest.mark.asyncio
    async def test_update_configuration_by_title_success(self, tool_repository, mock_async_session):
        """Test successful configuration update by title."""
        tool = MockTool(title="test_tool", config={"old": "config"})
        new_config = {"new": "config", "updated": True}
        
        with patch.object(tool_repository, 'find_by_title', return_value=tool):
            result = await tool_repository.update_configuration_by_title("test_tool", new_config)
            
            assert result == tool
            assert tool.config == new_config
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(tool)
    
    @pytest.mark.asyncio
    async def test_update_configuration_by_title_tool_not_found(self, tool_repository, mock_async_session):
        """Test configuration update when tool not found."""
        new_config = {"new": "config"}
        
        with patch.object(tool_repository, 'find_by_title', return_value=None):
            result = await tool_repository.update_configuration_by_title("nonexistent", new_config)
            
            assert result is None
            mock_async_session.commit.assert_not_called()
            mock_async_session.refresh.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_configuration_by_title_empty_config(self, tool_repository, mock_async_session):
        """Test configuration update with empty config."""
        tool = MockTool(title="test_tool", config={"old": "config"})
        new_config = {}
        
        with patch.object(tool_repository, 'find_by_title', return_value=tool):
            result = await tool_repository.update_configuration_by_title("test_tool", new_config)
            
            assert result == tool
            assert tool.config == {}
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_configuration_by_title_database_error(self, tool_repository, mock_async_session):
        """Test configuration update with database error."""
        tool = MockTool(title="test_tool")
        new_config = {"new": "config"}
        
        with patch.object(tool_repository, 'find_by_title', return_value=tool):
            mock_async_session.commit.side_effect = Exception("Update failed")
            
            with pytest.raises(Exception, match="Update failed"):
                await tool_repository.update_configuration_by_title("test_tool", new_config)
            
            mock_async_session.rollback.assert_called_once()


class TestToolRepositoryEnableAll:
    """Test cases for enable_all method."""
    
    @pytest.mark.asyncio
    async def test_enable_all_success(self, tool_repository, mock_async_session, sample_tools):
        """Test successful enabling of all tools."""
        # Mock the update statement execution
        mock_async_session.execute.return_value = MagicMock()
        
        # Mock the list method to return all tools (now enabled)
        enabled_tools = [MockTool(id=tool.id, title=tool.title, enabled=True) for tool in sample_tools]
        with patch.object(tool_repository, 'list', return_value=enabled_tools):
            result = await tool_repository.enable_all()
            
            assert len(result) == len(sample_tools)
            assert all(tool.enabled for tool in result)
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
            
            # Verify the update statement was constructed correctly
            call_args = mock_async_session.execute.call_args[0][0]
            assert hasattr(call_args, 'compile')  # Should be an update statement
    
    @pytest.mark.asyncio
    async def test_enable_all_no_disabled_tools(self, tool_repository, mock_async_session):
        """Test enable all when no tools are disabled."""
        # Mock update statement (should update 0 rows)
        mock_async_session.execute.return_value = MagicMock()
        
        # Mock list method to return empty list
        with patch.object(tool_repository, 'list', return_value=[]):
            result = await tool_repository.enable_all()
            
            assert result == []
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_all_database_error(self, tool_repository, mock_async_session):
        """Test enable all with database error."""
        mock_async_session.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(Exception, match="Update failed"):
            await tool_repository.enable_all()
        
        mock_async_session.rollback.assert_called_once()


class TestToolRepositoryDisableAll:
    """Test cases for disable_all method."""
    
    @pytest.mark.asyncio
    async def test_disable_all_success(self, tool_repository, mock_async_session, sample_tools):
        """Test successful disabling of all tools."""
        # Mock the update statement execution
        mock_async_session.execute.return_value = MagicMock()
        
        # Mock the list method to return all tools (now disabled)
        disabled_tools = [MockTool(id=tool.id, title=tool.title, enabled=False) for tool in sample_tools]
        with patch.object(tool_repository, 'list', return_value=disabled_tools):
            result = await tool_repository.disable_all()
            
            assert len(result) == len(sample_tools)
            assert all(not tool.enabled for tool in result)
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
            
            # Verify the update statement was constructed correctly
            call_args = mock_async_session.execute.call_args[0][0]
            assert hasattr(call_args, 'compile')  # Should be an update statement
    
    @pytest.mark.asyncio
    async def test_disable_all_no_enabled_tools(self, tool_repository, mock_async_session):
        """Test disable all when no tools are enabled."""
        # Mock update statement (should update 0 rows)
        mock_async_session.execute.return_value = MagicMock()
        
        # Mock list method to return empty list
        with patch.object(tool_repository, 'list', return_value=[]):
            result = await tool_repository.disable_all()
            
            assert result == []
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_database_error(self, tool_repository, mock_async_session):
        """Test disable all with database error."""
        mock_async_session.execute.side_effect = Exception("Disable failed")
        
        with pytest.raises(Exception, match="Disable failed"):
            await tool_repository.disable_all()
        
        mock_async_session.rollback.assert_called_once()


class TestSyncToolRepository:
    """Test cases for SyncToolRepository."""
    
    @pytest.fixture
    def mock_sync_session(self):
        """Create a mock sync database session."""
        session = MagicMock()
        session.query.return_value = session
        session.filter.return_value = session
        session.first.return_value = None
        session.all.return_value = []
        return session
    
    @pytest.fixture
    def sync_tool_repository(self, mock_sync_session):
        """Create a sync tool repository."""
        return SyncToolRepository(db=mock_sync_session)
    
    def test_sync_init_success(self, mock_sync_session):
        """Test successful sync repository initialization."""
        repository = SyncToolRepository(db=mock_sync_session)
        assert repository.db == mock_sync_session
    
    def test_find_by_id_success(self, sync_tool_repository, mock_sync_session):
        """Test successful find by ID in sync repository."""
        tool = MockTool(id=1)
        mock_sync_session.first.return_value = tool
        
        result = sync_tool_repository.find_by_id(1)
        
        assert result == tool
        mock_sync_session.query.assert_called_once_with(Tool)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_id_not_found(self, sync_tool_repository, mock_sync_session):
        """Test find by ID when tool not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_tool_repository.find_by_id(999)
        
        assert result is None
        mock_sync_session.query.assert_called_once_with(Tool)
    
    def test_find_by_title_sync(self, sync_tool_repository, mock_sync_session):
        """Test find by title in sync repository."""
        tool = MockTool(title="test_tool")
        mock_sync_session.first.return_value = tool
        
        result = sync_tool_repository.find_by_title("test_tool")
        
        assert result == tool
        mock_sync_session.query.assert_called_once_with(Tool)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_all_sync(self, sync_tool_repository, mock_sync_session):
        """Test find all in sync repository."""
        tools = [MockTool(id=1), MockTool(id=2)]
        mock_sync_session.all.return_value = tools
        
        result = sync_tool_repository.find_all()
        
        assert result == tools
        mock_sync_session.query.assert_called_once_with(Tool)
    
    def test_find_by_ids_sync(self, sync_tool_repository, mock_sync_session):
        """Test find by IDs in sync repository."""
        tools = [MockTool(id=1), MockTool(id=3)]
        mock_sync_session.all.return_value = tools
        
        result = sync_tool_repository.find_by_ids([1, 3, 5])
        
        assert result == tools
        mock_sync_session.query.assert_called_once_with(Tool)
        mock_sync_session.filter.assert_called_once()


class TestToolRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_title_flow(self, tool_repository, mock_async_session):
        """Test the flow from create to find by title."""
        tool_data = {"title": "integration_tool", "enabled": True}
        
        with patch('src.repositories.tool_repository.Tool') as mock_tool_class:
            created_tool = MockTool(title="integration_tool")
            mock_tool_class.return_value = created_tool
            
            # Mock find_by_title for retrieval
            mock_result = MockResult([created_tool])
            mock_async_session.execute.return_value = mock_result
            
            # Create tool using inherited create method
            with patch.object(tool_repository, 'create', return_value=created_tool) as mock_create:
                create_result = await tool_repository.create(tool_data)
                
                # Find tool by title
                find_result = await tool_repository.find_by_title("integration_tool")
                
                assert create_result == created_tool
                assert find_result == created_tool
                mock_create.assert_called_once_with(tool_data)
                mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_all_then_find_enabled_flow(self, tool_repository, mock_async_session):
        """Test enabling all tools then finding enabled tools."""
        # Mock enable_all
        enabled_tools = [MockTool(id=1, enabled=True), MockTool(id=2, enabled=True)]
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(tool_repository, 'list', return_value=enabled_tools):
            enable_result = await tool_repository.enable_all()
            
            # Mock find_enabled
            mock_result = MockResult(enabled_tools)
            mock_async_session.execute.return_value = mock_result
            
            find_result = await tool_repository.find_enabled()
            
            assert len(enable_result) == 2
            assert len(find_result) == 2
            assert all(tool.enabled for tool in enable_result)
            assert all(tool.enabled for tool in find_result)
    
    @pytest.mark.asyncio
    async def test_toggle_then_update_config_flow(self, tool_repository, mock_async_session):
        """Test toggling tool status then updating configuration."""
        tool = MockTool(id=1, title="config_tool", enabled=False)
        
        # Toggle enabled
        with patch.object(tool_repository, 'get', return_value=tool):
            toggle_result = await tool_repository.toggle_enabled(1)
            
            assert toggle_result.enabled is True
            
            # Update configuration
            new_config = {"api_key": "new_key", "timeout": 30}
            with patch.object(tool_repository, 'find_by_title', return_value=tool):
                config_result = await tool_repository.update_configuration_by_title("config_tool", new_config)
                
                assert config_result == tool
                assert tool.config == new_config
                # Verify both operations committed
                assert mock_async_session.commit.call_count == 2


class TestToolRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_title_database_error(self, tool_repository, mock_async_session):
        """Test find by title with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await tool_repository.find_by_title("test_tool")
    
    @pytest.mark.asyncio
    async def test_find_enabled_database_error(self, tool_repository, mock_async_session):
        """Test find enabled with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await tool_repository.find_enabled()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_get_error(self, tool_repository, mock_async_session):
        """Test toggle enabled when get method fails."""
        with patch.object(tool_repository, 'get', side_effect=Exception("Get failed")):
            with pytest.raises(Exception, match="Get failed"):
                await tool_repository.toggle_enabled(1)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_configuration_find_error(self, tool_repository, mock_async_session):
        """Test configuration update when find_by_title fails."""
        with patch.object(tool_repository, 'find_by_title', side_effect=Exception("Find failed")):
            with pytest.raises(Exception, match="Find failed"):
                await tool_repository.update_configuration_by_title("test_tool", {})
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_all_list_error(self, tool_repository, mock_async_session):
        """Test enable all when list method fails."""
        # Mock successful update but failed list
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(tool_repository, 'list', side_effect=Exception("List failed")):
            with pytest.raises(Exception, match="List failed"):
                await tool_repository.enable_all()
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_commit_error(self, tool_repository, mock_async_session):
        """Test disable all with commit error."""
        mock_async_session.execute.return_value = MagicMock()
        mock_async_session.commit.side_effect = Exception("Commit failed")
        
        with pytest.raises(Exception, match="Commit failed"):
            await tool_repository.disable_all()
        
        mock_async_session.rollback.assert_called_once()


class TestToolRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_multiple_times(self, tool_repository, mock_async_session):
        """Test toggling tool enabled status multiple times."""
        tool = MockTool(id=1, enabled=True)
        
        with patch.object(tool_repository, 'get', return_value=tool):
            # First toggle
            result1 = await tool_repository.toggle_enabled(1)
            assert result1.enabled is False
            
            # Second toggle
            result2 = await tool_repository.toggle_enabled(1)
            assert result2.enabled is True
            
            # Third toggle
            result3 = await tool_repository.toggle_enabled(1)
            assert result3.enabled is False
            
            # Should have committed 3 times
            assert mock_async_session.commit.call_count == 3
    
    @pytest.mark.asyncio
    async def test_update_configuration_none_config(self, tool_repository, mock_async_session):
        """Test configuration update with None config."""
        tool = MockTool(title="test_tool", config={"old": "config"})
        
        with patch.object(tool_repository, 'find_by_title', return_value=tool):
            result = await tool_repository.update_configuration_by_title("test_tool", None)
            
            assert result == tool
            assert tool.config is None
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_disable_all_empty_database(self, tool_repository, mock_async_session):
        """Test enable/disable all with empty database."""
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(tool_repository, 'list', return_value=[]):
            # Enable all
            enable_result = await tool_repository.enable_all()
            assert enable_result == []
            
            # Disable all
            disable_result = await tool_repository.disable_all()
            assert disable_result == []
            
            # Should have executed updates even with empty database
            assert mock_async_session.execute.call_count == 2
            assert mock_async_session.commit.call_count == 2
    
    @pytest.mark.asyncio
    async def test_find_by_title_case_sensitivity(self, tool_repository, mock_async_session):
        """Test find by title case sensitivity."""
        tool = MockTool(title="Test Tool")
        mock_result = MockResult([tool])
        mock_async_session.execute.return_value = mock_result
        
        # Exact match should work
        result = await tool_repository.find_by_title("Test Tool")
        assert result == tool
        
        # Different case should not find anything (depends on database collation)
        mock_result_empty = MockResult([])
        mock_async_session.execute.return_value = mock_result_empty
        
        result = await tool_repository.find_by_title("test tool")
        assert result is None
    
    @pytest.mark.asyncio
    async def test_configuration_update_complex_config(self, tool_repository, mock_async_session):
        """Test configuration update with complex nested configuration."""
        tool = MockTool(title="complex_tool")
        complex_config = {
            "api": {
                "endpoint": "https://api.example.com",
                "version": "v2",
                "auth": {
                    "type": "bearer",
                    "token": "secret_token"
                }
            },
            "settings": {
                "timeout": 30,
                "retries": 3,
                "features": ["feature1", "feature2"]
            },
            "metadata": {
                "created": "2023-01-01",
                "version": "1.0.0"
            }
        }
        
        with patch.object(tool_repository, 'find_by_title', return_value=tool):
            result = await tool_repository.update_configuration_by_title("complex_tool", complex_config)
            
            assert result == tool
            assert tool.config == complex_config
            assert tool.config["api"]["auth"]["type"] == "bearer"
            assert tool.config["settings"]["features"] == ["feature1", "feature2"]
            mock_async_session.commit.assert_called_once()