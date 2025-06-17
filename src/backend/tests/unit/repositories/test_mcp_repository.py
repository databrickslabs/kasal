"""
Unit tests for MCP repositories.

Tests the functionality of MCPServerRepository, MCPSettingsRepository, and SyncMCPServerRepository
including CRUD operations, enabled status management, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.mcp_repository import MCPServerRepository, MCPSettingsRepository, SyncMCPServerRepository
from src.models.mcp_server import MCPServer
from src.models.mcp_settings import MCPSettings


# Mock MCP server model
class MockMCPServer:
    def __init__(self, id=1, name="test_server", enabled=True, 
                 config=None, created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.config = config or {}
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)


# Mock MCP settings model
class MockMCPSettings:
    def __init__(self, id=1, global_enabled=False, created_at=None, updated_at=None):
        self.id = id
        self.global_enabled = global_enabled
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)


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
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query.return_value = session
    session.filter.return_value = session
    session.first.return_value = None
    session.all.return_value = []
    return session


@pytest.fixture
def mcp_server_repository(mock_async_session):
    """Create an MCP server repository with async session."""
    return MCPServerRepository(session=mock_async_session)


@pytest.fixture
def mcp_settings_repository(mock_async_session):
    """Create an MCP settings repository with async session."""
    return MCPSettingsRepository(session=mock_async_session)


@pytest.fixture
def sync_mcp_server_repository(mock_sync_session):
    """Create a sync MCP server repository."""
    return SyncMCPServerRepository(db=mock_sync_session)


@pytest.fixture
def sample_mcp_servers():
    """Create sample MCP servers for testing."""
    return [
        MockMCPServer(id=1, name="server1", enabled=True),
        MockMCPServer(id=2, name="server2", enabled=False),
        MockMCPServer(id=3, name="server3", enabled=True)
    ]


@pytest.fixture
def sample_mcp_settings():
    """Create sample MCP settings for testing."""
    return MockMCPSettings(id=1, global_enabled=True)


class TestMCPServerRepositoryInit:
    """Test cases for MCPServerRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = MCPServerRepository(session=mock_async_session)
        
        assert repository.model == MCPServer
        assert repository.session == mock_async_session


class TestMCPServerRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test successful server search by name."""
        server = sample_mcp_servers[0]
        mock_result = MockResult([server])
        mock_async_session.execute.return_value = mock_result
        
        result = await mcp_server_repository.find_by_name("server1")
        
        assert result == server
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(MCPServer)))
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, mcp_server_repository, mock_async_session):
        """Test find by name when server not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await mcp_server_repository.find_by_name("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_exception_handling(self, mcp_server_repository, mock_async_session):
        """Test find by name with database exception."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await mcp_server_repository.find_by_name("test_server")


class TestMCPServerRepositoryFindEnabled:
    """Test cases for find_enabled method."""
    
    @pytest.mark.asyncio
    async def test_find_enabled_success(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test successful retrieval of enabled servers."""
        enabled_servers = [server for server in sample_mcp_servers if server.enabled]
        mock_result = MockResult(enabled_servers)
        mock_async_session.execute.return_value = mock_result
        
        result = await mcp_server_repository.find_enabled()
        
        assert len(result) == len(enabled_servers)
        assert all(server.enabled for server in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_no_enabled_servers(self, mcp_server_repository, mock_async_session):
        """Test find enabled when no servers are enabled."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await mcp_server_repository.find_enabled()
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_exception_handling(self, mcp_server_repository, mock_async_session):
        """Test find enabled with database exception."""
        mock_async_session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            await mcp_server_repository.find_enabled()


class TestMCPServerRepositoryToggleEnabled:
    """Test cases for toggle_enabled method."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success_enabled_to_disabled(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test successfully toggling enabled server to disabled."""
        server = sample_mcp_servers[0]  # enabled=True
        
        with patch.object(mcp_server_repository, 'get', return_value=server):
            result = await mcp_server_repository.toggle_enabled(1)
            
            assert result == server
            assert server.enabled is False  # Should be toggled
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(server)
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success_disabled_to_enabled(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test successfully toggling disabled server to enabled."""
        server = sample_mcp_servers[1]  # enabled=False
        
        with patch.object(mcp_server_repository, 'get', return_value=server):
            result = await mcp_server_repository.toggle_enabled(2)
            
            assert result == server
            assert server.enabled is True  # Should be toggled
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(server)
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_server_not_found(self, mcp_server_repository, mock_async_session):
        """Test toggle enabled when server not found."""
        with patch.object(mcp_server_repository, 'get', return_value=None):
            result = await mcp_server_repository.toggle_enabled(999)
            
            assert result is None
            mock_async_session.commit.assert_not_called()
            mock_async_session.refresh.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_database_error(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test toggle enabled with database error."""
        server = sample_mcp_servers[0]
        
        with patch.object(mcp_server_repository, 'get', return_value=server):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with patch('logging.error') as mock_logger:
                with pytest.raises(Exception, match="Commit failed"):
                    await mcp_server_repository.toggle_enabled(1)
                
                mock_logger.assert_called_once()
                mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_get_error(self, mcp_server_repository, mock_async_session):
        """Test toggle enabled when get method fails."""
        with patch.object(mcp_server_repository, 'get', side_effect=Exception("Get failed")):
            with patch('logging.error') as mock_logger:
                with pytest.raises(Exception, match="Get failed"):
                    await mcp_server_repository.toggle_enabled(1)
                
                mock_logger.assert_called_once()
                mock_async_session.rollback.assert_called_once()


class TestMCPSettingsRepositoryInit:
    """Test cases for MCPSettingsRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = MCPSettingsRepository(session=mock_async_session)
        
        assert repository.model == MCPSettings
        assert repository.session == mock_async_session


class TestMCPSettingsRepositoryGetSettings:
    """Test cases for get_settings method."""
    
    @pytest.mark.asyncio
    async def test_get_settings_existing_settings(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test getting existing settings."""
        mock_result = MockResult([sample_mcp_settings])
        mock_async_session.execute.return_value = mock_result
        
        result = await mcp_settings_repository.get_settings()
        
        assert result == sample_mcp_settings
        mock_async_session.execute.assert_called_once()
        mock_async_session.add.assert_not_called()
        mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_settings_create_default(self, mcp_settings_repository, mock_async_session):
        """Test creating default settings when none exist."""
        # First call returns empty result, second call after creation returns the created settings
        mock_empty_result = MockResult([])
        mock_async_session.execute.return_value = mock_empty_result
        
        with patch('src.repositories.mcp_repository.MCPSettings') as mock_settings_class:
            mock_settings = MockMCPSettings(global_enabled=False)
            mock_settings_class.return_value = mock_settings
            
            result = await mcp_settings_repository.get_settings()
            
            assert result == mock_settings
            mock_async_session.add.assert_called_once_with(mock_settings)
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(mock_settings)
            mock_settings_class.assert_called_once_with(global_enabled=False)
    
    @pytest.mark.asyncio
    async def test_get_settings_database_error(self, mcp_settings_repository, mock_async_session):
        """Test get settings with database error."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await mcp_settings_repository.get_settings()


class TestMCPSettingsRepositoryUpdateGlobalEnabled:
    """Test cases for update_global_enabled method."""
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_to_true(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test updating global enabled to True."""
        sample_mcp_settings.global_enabled = False  # Start with False
        
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            result = await mcp_settings_repository.update_global_enabled(True)
            
            assert result == sample_mcp_settings
            assert sample_mcp_settings.global_enabled is True
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(sample_mcp_settings)
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_to_false(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test updating global enabled to False."""
        sample_mcp_settings.global_enabled = True  # Start with True
        
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            result = await mcp_settings_repository.update_global_enabled(False)
            
            assert result == sample_mcp_settings
            assert sample_mcp_settings.global_enabled is False
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(sample_mcp_settings)
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_no_change(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test updating global enabled to same value."""
        sample_mcp_settings.global_enabled = True  # Start with True
        
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            result = await mcp_settings_repository.update_global_enabled(True)
            
            assert result == sample_mcp_settings
            assert sample_mcp_settings.global_enabled is True
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(sample_mcp_settings)
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_get_settings_error(self, mcp_settings_repository, mock_async_session):
        """Test update global enabled when get_settings fails."""
        with patch.object(mcp_settings_repository, 'get_settings', side_effect=Exception("Get failed")):
            with pytest.raises(Exception, match="Get failed"):
                await mcp_settings_repository.update_global_enabled(True)
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_commit_error(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test update global enabled with commit error."""
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await mcp_settings_repository.update_global_enabled(True)


class TestSyncMCPServerRepositoryInit:
    """Test cases for SyncMCPServerRepository initialization."""
    
    def test_init_success(self, mock_sync_session):
        """Test successful initialization."""
        repository = SyncMCPServerRepository(db=mock_sync_session)
        assert repository.db == mock_sync_session


class TestSyncMCPServerRepositoryFindById:
    """Test cases for find_by_id method."""
    
    def test_find_by_id_success(self, sync_mcp_server_repository, mock_sync_session, sample_mcp_servers):
        """Test successful find by ID."""
        server = sample_mcp_servers[0]
        mock_sync_session.first.return_value = server
        
        result = sync_mcp_server_repository.find_by_id(1)
        
        assert result == server
        mock_sync_session.query.assert_called_once_with(MCPServer)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_id_not_found(self, sync_mcp_server_repository, mock_sync_session):
        """Test find by ID when server not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_mcp_server_repository.find_by_id(999)
        
        assert result is None
        mock_sync_session.query.assert_called_once_with(MCPServer)
    
    def test_find_by_id_database_error(self, sync_mcp_server_repository, mock_sync_session):
        """Test find by ID with database error."""
        mock_sync_session.query.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            sync_mcp_server_repository.find_by_id(1)


class TestSyncMCPServerRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    def test_find_by_name_success(self, sync_mcp_server_repository, mock_sync_session, sample_mcp_servers):
        """Test successful find by name."""
        server = sample_mcp_servers[0]
        mock_sync_session.first.return_value = server
        
        result = sync_mcp_server_repository.find_by_name("server1")
        
        assert result == server
        mock_sync_session.query.assert_called_once_with(MCPServer)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_name_not_found(self, sync_mcp_server_repository, mock_sync_session):
        """Test find by name when server not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_mcp_server_repository.find_by_name("nonexistent")
        
        assert result is None
        mock_sync_session.query.assert_called_once_with(MCPServer)
    
    def test_find_by_name_database_error(self, sync_mcp_server_repository, mock_sync_session):
        """Test find by name with database error."""
        mock_sync_session.filter.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            sync_mcp_server_repository.find_by_name("test")


class TestSyncMCPServerRepositoryFindAll:
    """Test cases for find_all method."""
    
    def test_find_all_success(self, sync_mcp_server_repository, mock_sync_session, sample_mcp_servers):
        """Test successful find all."""
        mock_sync_session.all.return_value = sample_mcp_servers
        
        result = sync_mcp_server_repository.find_all()
        
        assert result == sample_mcp_servers
        mock_sync_session.query.assert_called_once_with(MCPServer)
    
    def test_find_all_empty_result(self, sync_mcp_server_repository, mock_sync_session):
        """Test find all with empty result."""
        mock_sync_session.all.return_value = []
        
        result = sync_mcp_server_repository.find_all()
        
        assert result == []
        mock_sync_session.query.assert_called_once_with(MCPServer)
    
    def test_find_all_database_error(self, sync_mcp_server_repository, mock_sync_session):
        """Test find all with database error."""
        mock_sync_session.all.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            sync_mcp_server_repository.find_all()


class TestSyncMCPServerRepositoryFindEnabled:
    """Test cases for find_enabled method."""
    
    def test_find_enabled_success(self, sync_mcp_server_repository, mock_sync_session, sample_mcp_servers):
        """Test successful find enabled."""
        enabled_servers = [server for server in sample_mcp_servers if server.enabled]
        mock_sync_session.all.return_value = enabled_servers
        
        result = sync_mcp_server_repository.find_enabled()
        
        assert result == enabled_servers
        mock_sync_session.query.assert_called_once_with(MCPServer)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_enabled_no_enabled_servers(self, sync_mcp_server_repository, mock_sync_session):
        """Test find enabled when no servers are enabled."""
        mock_sync_session.all.return_value = []
        
        result = sync_mcp_server_repository.find_enabled()
        
        assert result == []
        mock_sync_session.query.assert_called_once_with(MCPServer)
    
    def test_find_enabled_database_error(self, sync_mcp_server_repository, mock_sync_session):
        """Test find enabled with database error."""
        mock_sync_session.filter.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            sync_mcp_server_repository.find_enabled()


class TestMCPRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_name_workflow(self, mcp_server_repository, mock_async_session):
        """Test workflow of creating server then finding by name."""
        server_data = {"name": "integration_server", "enabled": True}
        
        with patch('src.repositories.mcp_repository.MCPServer') as mock_server_class:
            created_server = MockMCPServer(name="integration_server")
            mock_server_class.return_value = created_server
            
            # Mock find_by_name for retrieval
            mock_result = MockResult([created_server])
            mock_async_session.execute.return_value = mock_result
            
            # Create server using inherited create method
            with patch.object(mcp_server_repository, 'create', return_value=created_server) as mock_create:
                create_result = await mcp_server_repository.create(server_data)
                
                # Find server by name
                find_result = await mcp_server_repository.find_by_name("integration_server")
                
                assert create_result == created_server
                assert find_result == created_server
                mock_create.assert_called_once_with(server_data)
                mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_then_toggle_workflow(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test workflow of finding enabled servers then toggling one."""
        enabled_servers = [server for server in sample_mcp_servers if server.enabled]
        
        # Mock find_enabled
        mock_result = MockResult(enabled_servers)
        mock_async_session.execute.return_value = mock_result
        
        found_servers = await mcp_server_repository.find_enabled()
        
        assert len(found_servers) == len(enabled_servers)
        
        # Now test toggling one of the enabled servers
        server_to_toggle = found_servers[0]
        original_status = server_to_toggle.enabled
        
        with patch.object(mcp_server_repository, 'get', return_value=server_to_toggle):
            toggle_result = await mcp_server_repository.toggle_enabled(server_to_toggle.id)
            
            assert toggle_result == server_to_toggle
            assert server_to_toggle.enabled != original_status
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_settings_then_update_workflow(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test workflow of getting settings then updating global enabled."""
        # Mock get_settings
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            settings = await mcp_settings_repository.get_settings()
            assert settings == sample_mcp_settings
            
            original_enabled = settings.global_enabled
            
            # Update global enabled
            updated_settings = await mcp_settings_repository.update_global_enabled(not original_enabled)
            
            assert updated_settings == sample_mcp_settings
            assert settings.global_enabled != original_enabled
            mock_async_session.commit.assert_called_once()


class TestMCPRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_database_error(self, mcp_server_repository, mock_async_session):
        """Test find by name with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await mcp_server_repository.find_by_name("test_server")
    
    @pytest.mark.asyncio
    async def test_find_enabled_database_error(self, mcp_server_repository, mock_async_session):
        """Test find enabled with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await mcp_server_repository.find_enabled()
    
    @pytest.mark.asyncio
    async def test_get_settings_create_default_error(self, mcp_settings_repository, mock_async_session):
        """Test get settings when creating default settings fails."""
        mock_empty_result = MockResult([])
        mock_async_session.execute.return_value = mock_empty_result
        mock_async_session.commit.side_effect = Exception("Commit failed")
        
        with patch('src.repositories.mcp_repository.MCPSettings') as mock_settings_class:
            mock_settings = MockMCPSettings()
            mock_settings_class.return_value = mock_settings
            
            with pytest.raises(Exception, match="Commit failed"):
                await mcp_settings_repository.get_settings()
    
    def test_sync_repository_query_error(self, sync_mcp_server_repository, mock_sync_session):
        """Test sync repository with query error."""
        mock_sync_session.query.side_effect = Exception("Query error")
        
        with pytest.raises(Exception, match="Query error"):
            sync_mcp_server_repository.find_all()


class TestMCPRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_multiple_times(self, mcp_server_repository, mock_async_session, sample_mcp_servers):
        """Test toggling enabled status multiple times."""
        server = sample_mcp_servers[0]
        original_status = server.enabled
        
        with patch.object(mcp_server_repository, 'get', return_value=server):
            # First toggle
            result1 = await mcp_server_repository.toggle_enabled(1)
            assert server.enabled != original_status
            
            # Second toggle (should return to original)
            result2 = await mcp_server_repository.toggle_enabled(1)
            assert server.enabled == original_status
            
            assert result1 == server
            assert result2 == server
            assert mock_async_session.commit.call_count == 2
    
    @pytest.mark.asyncio
    async def test_update_global_enabled_boolean_coercion(self, mcp_settings_repository, mock_async_session, sample_mcp_settings):
        """Test update global enabled with non-boolean values."""
        with patch.object(mcp_settings_repository, 'get_settings', return_value=sample_mcp_settings):
            # Test with truthy value
            result1 = await mcp_settings_repository.update_global_enabled("true")
            assert sample_mcp_settings.global_enabled == "true"  # Should be set as-is
            
            # Test with falsy value
            result2 = await mcp_settings_repository.update_global_enabled(0)
            assert sample_mcp_settings.global_enabled == 0  # Should be set as-is
    
    def test_sync_repository_filter_chaining(self, sync_mcp_server_repository, mock_sync_session, sample_mcp_servers):
        """Test that sync repository properly chains filter operations."""
        enabled_servers = [server for server in sample_mcp_servers if server.enabled]
        mock_sync_session.all.return_value = enabled_servers
        
        result = sync_mcp_server_repository.find_enabled()
        
        # Verify the query was chained correctly
        mock_sync_session.query.assert_called_once_with(MCPServer)
        mock_sync_session.filter.assert_called_once()
        mock_sync_session.all.assert_called_once()
        assert result == enabled_servers
    
    @pytest.mark.asyncio
    async def test_get_settings_empty_database(self, mcp_settings_repository, mock_async_session):
        """Test get settings when database is completely empty."""
        # Mock empty result for initial query
        mock_empty_result = MockResult([])
        mock_async_session.execute.return_value = mock_empty_result
        
        with patch('src.repositories.mcp_repository.MCPSettings') as mock_settings_class:
            mock_settings = MockMCPSettings(global_enabled=False)
            mock_settings_class.return_value = mock_settings
            
            result = await mcp_settings_repository.get_settings()
            
            assert result == mock_settings
            # Verify default settings were created with correct values
            mock_settings_class.assert_called_once_with(global_enabled=False)
            mock_async_session.add.assert_called_once_with(mock_settings)
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(mock_settings)