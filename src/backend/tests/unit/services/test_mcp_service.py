"""
Unit tests for MCPService.

Tests the functionality of MCP server management operations including
CRUD operations, connection testing, settings management, and error handling.
"""
import pytest
import asyncio
import aiohttp
import logging
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import HTTPException, status

from src.services.mcp_service import MCPService

logger = logging.getLogger(__name__)


def create_async_context_manager(return_value):
    """Helper function to create proper async context manager mocks."""
    @asynccontextmanager
    async def mock_context():
        yield return_value
    
    return mock_context()
from src.schemas.mcp import (
    MCPServerCreate,
    MCPServerUpdate,
    MCPServerResponse,
    MCPServerListResponse,
    MCPToggleResponse,
    MCPTestConnectionRequest,
    MCPTestConnectionResponse,
    MCPSettingsResponse,
    MCPSettingsUpdate
)


# Mock models
class MockMCPServer:
    def __init__(self, id=1, name="test_server", server_url="http://test.com", 
                 server_type="sse", enabled=True, encrypted_api_key=None,
                 timeout_seconds=30, max_retries=3, model_mapping_enabled=False,
                 rate_limit=60, additional_config=None,
                 created_at=None, updated_at=None):
        from datetime import datetime
        self.id = id
        self.name = name
        self.server_url = server_url
        self.server_type = server_type
        self.enabled = enabled
        self.encrypted_api_key = encrypted_api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.model_mapping_enabled = model_mapping_enabled
        self.rate_limit = rate_limit
        self.additional_config = additional_config or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()


class MockMCPSettings:
    def __init__(self, id=1, global_enabled=True, created_at=None, updated_at=None):
        from datetime import datetime
        self.id = id
        self.global_enabled = global_enabled
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()


@pytest.fixture
def mock_server_repository():
    """Create a mock MCPServerRepository."""
    return MagicMock()


@pytest.fixture
def mock_settings_repository():
    """Create a mock MCPSettingsRepository."""
    return MagicMock()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return MagicMock()


@pytest.fixture
def mcp_service(mock_server_repository, mock_settings_repository):
    """Create an MCPService instance with mocked repositories."""
    return MCPService(
        server_repository=mock_server_repository,
        settings_repository=mock_settings_repository
    )


@pytest.fixture
def mcp_service_with_session(mock_session):
    """Create an MCPService instance with mock session."""
    with patch('src.services.mcp_service.MCPServerRepository') as mock_server_repo_class, \
         patch('src.services.mcp_service.MCPSettingsRepository') as mock_settings_repo_class:
        
        mock_server_repo = MagicMock()
        mock_settings_repo = MagicMock()
        mock_server_repo_class.return_value = mock_server_repo
        mock_settings_repo_class.return_value = mock_settings_repo
        
        service = MCPService(session=mock_session)
        service.server_repository = mock_server_repo
        service.settings_repository = mock_settings_repo
        return service


@pytest.fixture
def mock_uow():
    """Create a mock UnitOfWork."""
    uow = MagicMock()
    uow.mcp_server_repository = MagicMock()
    uow.mcp_settings_repository = MagicMock()
    return uow


class TestMCPServiceInitialization:
    """Test MCPService initialization."""
    
    def test_init_with_repositories(self, mock_server_repository, mock_settings_repository):
        """Test initialization with repositories."""
        service = MCPService(
            server_repository=mock_server_repository,
            settings_repository=mock_settings_repository
        )
        
        assert service.server_repository is mock_server_repository
        assert service.settings_repository is mock_settings_repository
    
    def test_init_with_session(self, mock_session):
        """Test initialization with session."""
        with patch('src.services.mcp_service.MCPServerRepository') as mock_server_repo_class, \
             patch('src.services.mcp_service.MCPSettingsRepository') as mock_settings_repo_class:
            
            service = MCPService(session=mock_session)
            
            mock_server_repo_class.assert_called_once_with(mock_session)
            mock_settings_repo_class.assert_called_once_with(mock_session)
    
    def test_init_without_arguments_raises_error(self):
        """Test initialization without arguments raises ValueError."""
        with pytest.raises(ValueError, match="Either session or repositories must be provided"):
            MCPService()
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work(self, mock_uow):
        """Test creation from UnitOfWork."""
        service = await MCPService.from_unit_of_work(mock_uow)
        
        assert service.server_repository is mock_uow.mcp_server_repository
        assert service.settings_repository is mock_uow.mcp_settings_repository


class TestGetAllServers:
    """Test get_all_servers method."""
    
    @pytest.mark.asyncio
    async def test_get_all_servers_success(self, mcp_service, mock_server_repository):
        """Test successful retrieval of all servers."""
        mock_servers = [
            MockMCPServer(id=1, name="server1"),
            MockMCPServer(id=2, name="server2")
        ]
        mock_server_repository.list = AsyncMock(return_value=mock_servers)
        
        # Create proper mock responses that behave like MCPServerResponse objects
        mock_response1 = MCPServerResponse(
            id=1, name="server1", server_url="http://test.com", server_type="sse",
            enabled=True, timeout_seconds=30, max_retries=3, model_mapping_enabled=False,
            rate_limit=60, api_key="test_key", created_at=mock_servers[0].created_at,
            updated_at=mock_servers[0].updated_at, additional_config={}
        )
        mock_response2 = MCPServerResponse(
            id=2, name="server2", server_url="http://test.com", server_type="sse",
            enabled=True, timeout_seconds=30, max_retries=3, model_mapping_enabled=False,
            rate_limit=60, api_key="test_key", created_at=mock_servers[1].created_at,
            updated_at=mock_servers[1].updated_at, additional_config={}
        )
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate:
            mock_validate.side_effect = [mock_response1, mock_response2]
            
            result = await mcp_service.get_all_servers()
            
            assert isinstance(result, MCPServerListResponse)
            assert result.count == 2
            assert len(result.servers) == 2
            # API keys should be cleared in list response
            assert result.servers[0].api_key == ""
            assert result.servers[1].api_key == ""
    
    @pytest.mark.asyncio
    async def test_get_all_servers_empty_list(self, mcp_service, mock_server_repository):
        """Test retrieval when no servers exist."""
        mock_server_repository.list = AsyncMock(return_value=[])
        
        result = await mcp_service.get_all_servers()
        
        assert isinstance(result, MCPServerListResponse)
        assert result.count == 0
        assert len(result.servers) == 0


class TestGetEnabledServers:
    """Test get_enabled_servers method."""
    
    @pytest.mark.asyncio
    async def test_get_enabled_servers_success(self, mcp_service, mock_server_repository):
        """Test successful retrieval of enabled servers."""
        mock_servers = [
            MockMCPServer(id=1, name="server1", enabled=True),
            MockMCPServer(id=2, name="server2", enabled=True)
        ]
        mock_server_repository.find_enabled = AsyncMock(return_value=mock_servers)
        
        # Create proper mock responses that behave like MCPServerResponse objects
        mock_response1 = MCPServerResponse(
            id=1, name="server1", server_url="http://test.com", server_type="sse",
            enabled=True, timeout_seconds=30, max_retries=3, model_mapping_enabled=False,
            rate_limit=60, api_key="test_key", created_at=mock_servers[0].created_at,
            updated_at=mock_servers[0].updated_at, additional_config={}
        )
        mock_response2 = MCPServerResponse(
            id=2, name="server2", server_url="http://test.com", server_type="sse",
            enabled=True, timeout_seconds=30, max_retries=3, model_mapping_enabled=False,
            rate_limit=60, api_key="test_key", created_at=mock_servers[1].created_at,
            updated_at=mock_servers[1].updated_at, additional_config={}
        )
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate:
            mock_validate.side_effect = [mock_response1, mock_response2]
            
            result = await mcp_service.get_enabled_servers()
            
            assert isinstance(result, MCPServerListResponse)
            assert result.count == 2
            # API keys should be cleared
            assert result.servers[0].api_key == ""
            assert result.servers[1].api_key == ""


class TestGetServerById:
    """Test get_server_by_id method."""
    
    @pytest.mark.asyncio
    async def test_get_server_by_id_success(self, mcp_service, mock_server_repository):
        """Test successful retrieval of server by ID."""
        mock_server = MockMCPServer(id=1, encrypted_api_key="encrypted_key")
        mock_server_repository.get = AsyncMock(return_value=mock_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.decrypt_value', return_value="decrypted_key"):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.get_server_by_id(1)
            
            assert result is mock_response
            assert mock_response.api_key == "decrypted_key"
            mock_server_repository.get.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_get_server_by_id_not_found(self, mcp_service, mock_server_repository):
        """Test server not found error."""
        mock_server_repository.get = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.get_server_by_id(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_server_by_id_decryption_error(self, mcp_service, mock_server_repository):
        """Test handling of decryption errors."""
        mock_server = MockMCPServer(id=1, encrypted_api_key="encrypted_key")
        mock_server_repository.get = AsyncMock(return_value=mock_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.get_server_by_id(1)
            
            assert result is mock_response
            assert mock_response.api_key == ""
    
    @pytest.mark.asyncio
    async def test_get_server_by_id_no_encrypted_key(self, mcp_service, mock_server_repository):
        """Test retrieval when server has no encrypted API key."""
        mock_server = MockMCPServer(id=1, encrypted_api_key=None)
        mock_server_repository.get = AsyncMock(return_value=mock_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate:
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.get_server_by_id(1)
            
            assert result is mock_response
            # Should not attempt decryption


class TestCreateServer:
    """Test create_server method."""
    
    @pytest.mark.asyncio
    async def test_create_server_success(self, mcp_service, mock_server_repository):
        """Test successful server creation."""
        server_data = MCPServerCreate(
            name="test_server",
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key"
        )
        
        mock_server_repository.find_by_name = AsyncMock(return_value=None)
        mock_created_server = MockMCPServer(id=1, name="test_server")
        mock_server_repository.create = AsyncMock(return_value=mock_created_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.encrypt_value', return_value="encrypted_key"):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.create_server(server_data)
            
            assert result is mock_response
            assert mock_response.api_key == "test_key"
            mock_server_repository.create.assert_called_once()
            
            # Verify create was called with encrypted API key
            create_args = mock_server_repository.create.call_args[0][0]
            assert create_args["encrypted_api_key"] == "encrypted_key"
            assert "api_key" not in create_args
    
    @pytest.mark.asyncio
    async def test_create_server_name_conflict(self, mcp_service, mock_server_repository):
        """Test server creation with name conflict."""
        server_data = MCPServerCreate(
            name="existing_server",
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key"
        )
        
        existing_server = MockMCPServer(id=1, name="existing_server")
        mock_server_repository.find_by_name = AsyncMock(return_value=existing_server)
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.create_server(server_data)
        
        assert exc_info.value.status_code == status.HTTP_409_CONFLICT
        assert "already exists" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_server_database_error(self, mcp_service, mock_server_repository):
        """Test server creation with database error."""
        server_data = MCPServerCreate(
            name="test_server",
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key"
        )
        
        mock_server_repository.find_by_name = AsyncMock(return_value=None)
        mock_server_repository.create = AsyncMock(side_effect=Exception("Database error"))
        
        with patch('src.services.mcp_service.EncryptionUtils.encrypt_value', return_value="encrypted_key"):
            with pytest.raises(HTTPException) as exc_info:
                await mcp_service.create_server(server_data)
            
            assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "Failed to create MCP server" in str(exc_info.value.detail)


class TestUpdateServer:
    """Test update_server method."""
    
    @pytest.mark.asyncio
    async def test_update_server_success(self, mcp_service, mock_server_repository):
        """Test successful server update."""
        server_data = MCPServerUpdate(
            name="updated_server",
            api_key="new_key"
        )
        
        existing_server = MockMCPServer(id=1, name="old_server")
        updated_server = MockMCPServer(id=1, name="updated_server", encrypted_api_key="encrypted_new_key")
        
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.update = AsyncMock(return_value=updated_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.encrypt_value', return_value="encrypted_new_key"), \
             patch('src.services.mcp_service.EncryptionUtils.decrypt_value', return_value="new_key"):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.update_server(1, server_data)
            
            assert result is mock_response
            assert mock_response.api_key == "new_key"
            
            # Verify update was called with encrypted API key
            update_args = mock_server_repository.update.call_args[0][1]
            assert update_args["encrypted_api_key"] == "encrypted_new_key"
    
    @pytest.mark.asyncio
    async def test_update_server_not_found(self, mcp_service, mock_server_repository):
        """Test update server when server not found."""
        server_data = MCPServerUpdate(name="updated_server")
        mock_server_repository.get = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.update_server(999, server_data)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_server_without_api_key(self, mcp_service, mock_server_repository):
        """Test update server without API key."""
        server_data = MCPServerUpdate(name="updated_server")
        
        existing_server = MockMCPServer(id=1, name="old_server")
        updated_server = MockMCPServer(id=1, name="updated_server", encrypted_api_key="old_encrypted_key")
        
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.update = AsyncMock(return_value=updated_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.decrypt_value', return_value="old_key"):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.update_server(1, server_data)
            
            # Verify update was called without encrypted_api_key
            update_args = mock_server_repository.update.call_args[0][1]
            assert "encrypted_api_key" not in update_args
    
    @pytest.mark.asyncio
    async def test_update_server_database_error(self, mcp_service, mock_server_repository):
        """Test update server with database error."""
        server_data = MCPServerUpdate(name="updated_server")
        existing_server = MockMCPServer(id=1, name="old_server")
        
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.update = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.update_server(1, server_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to update MCP server" in str(exc_info.value.detail)


class TestDeleteServer:
    """Test delete_server method."""
    
    @pytest.mark.asyncio
    async def test_delete_server_success(self, mcp_service, mock_server_repository):
        """Test successful server deletion."""
        existing_server = MockMCPServer(id=1, name="test_server")
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.delete = AsyncMock()
        
        result = await mcp_service.delete_server(1)
        
        assert result is True
        mock_server_repository.delete.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_delete_server_not_found(self, mcp_service, mock_server_repository):
        """Test delete server when server not found."""
        mock_server_repository.get = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.delete_server(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_delete_server_database_error(self, mcp_service, mock_server_repository):
        """Test delete server with database error."""
        existing_server = MockMCPServer(id=1, name="test_server")
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.delete = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.delete_server(1)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to delete MCP server" in str(exc_info.value.detail)


class TestToggleServerEnabled:
    """Test toggle_server_enabled method."""
    
    @pytest.mark.asyncio
    async def test_toggle_server_enabled_success(self, mcp_service, mock_server_repository):
        """Test successful server toggle."""
        toggled_server = MockMCPServer(id=1, name="test_server", enabled=False)
        mock_server_repository.toggle_enabled = AsyncMock(return_value=toggled_server)
        
        result = await mcp_service.toggle_server_enabled(1)
        
        assert isinstance(result, MCPToggleResponse)
        assert result.enabled is False
        assert "disabled" in result.message
        mock_server_repository.toggle_enabled.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_toggle_server_enabled_not_found(self, mcp_service, mock_server_repository):
        """Test toggle when server not found."""
        mock_server_repository.toggle_enabled = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.toggle_server_enabled(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_toggle_server_enabled_database_error(self, mcp_service, mock_server_repository):
        """Test toggle with database error."""
        mock_server_repository.toggle_enabled = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.toggle_server_enabled(1)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to toggle MCP server" in str(exc_info.value.detail)


class TestTestConnection:
    """Test test_connection method."""
    
    @pytest.mark.asyncio
    async def test_test_connection_sse(self, mcp_service):
        """Test connection testing for SSE server."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        expected_response = MCPTestConnectionResponse(
            success=True,
            message="Successfully connected to MCP SSE server"
        )
        
        with patch.object(mcp_service, '_test_sse_connection', return_value=expected_response):
            result = await mcp_service.test_connection(test_data)
            
            assert result == expected_response
    
    @pytest.mark.asyncio
    async def test_test_connection_streamable(self, mcp_service):
        """Test connection testing for streamable server."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        expected_response = MCPTestConnectionResponse(
            success=True,
            message="Successfully connected to MCP server"
        )
        
        with patch.object(mcp_service, '_test_streamable_connection', return_value=expected_response):
            result = await mcp_service.test_connection(test_data)
            
            assert result == expected_response
    
    @pytest.mark.asyncio
    async def test_test_connection_unsupported_type(self, mcp_service):
        """Test connection testing for unsupported server type."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="websocket",
            api_key="test_key",
            timeout_seconds=30
        )
        
        result = await mcp_service.test_connection(test_data)
        
        assert result.success is False
        assert "Unsupported server type" in result.message


class TestTestSSEConnection:
    """Test _test_sse_connection method."""
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_success_with_correct_content_type(self, mcp_service):
        """Test successful SSE connection with correct content type."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Create a mock response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "text/event-stream"}
        
        # Mock the entire session and context managers properly
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is True
            assert "Successfully connected to MCP SSE server" in result.message
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_success_with_data(self, mcp_service):
        """Test successful SSE connection with data but wrong content type."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.content.read = AsyncMock(return_value=b"some data")
        
        mock_session = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        # Mock the entire session and context managers properly
        with patch('aiohttp.ClientSession') as mock_session_class, \
             patch('asyncio.wait_for', return_value=b"some data"):
            
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is True
            assert "Content-Type is not text/event-stream" in result.message
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_http_error(self, mcp_service):
        """Test SSE connection with HTTP error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.text = AsyncMock(return_value="Not Found")
        
        # Mock the entire session and context managers properly
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is False
            assert "HTTP 404" in result.message
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_connector_error(self, mcp_service):
        """Test SSE connection with connector error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://invalid.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Test the connector error handling (line 387)
        os_error = OSError("Connection failed")
        os_error.errno = 111  # Connection refused
        os_error.strerror = "Connection failed"
        connector_error = aiohttp.ClientConnectorError(
            connection_key=MagicMock(), os_error=os_error
        )
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.side_effect = connector_error
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is False
            assert "Failed to connect" in result.message
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_timeout(self, mcp_service):
        """Test SSE connection with timeout."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=1
        )
        
        # Mock session with timeout error
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.side_effect = asyncio.TimeoutError()
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is False
            assert "timed out after 1 seconds" in result.message
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_no_data_timeout(self, mcp_service):
        """Test SSE connection with no data timeout."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.content.read = AsyncMock()
        
        # Mock the entire session and context managers properly
        with patch('aiohttp.ClientSession') as mock_session_class, \
             patch('asyncio.wait_for', side_effect=asyncio.TimeoutError()):
            
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is False
            assert "no data received" in result.message


class TestTestStreamableConnection:
    """Test _test_streamable_connection method."""
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_success(self, mcp_service):
        """Test successful streamable connection."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock successful JSON response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json = AsyncMock(return_value={"status": "ok"})
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is True
            assert "Successfully connected to Streamable API server" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_init_error(self, mcp_service):
        """Test streamable connection with initialization error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock connection error
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            # Simulate connection error
            mock_session.get.side_effect = aiohttp.ClientConnectorError(
                connection_key=MagicMock(),
                os_error=OSError("Connection failed")
            )
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Failed to connect" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_no_json(self, mcp_service):
        """Test streamable connection with non-JSON response."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock successful non-JSON response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.text = AsyncMock(return_value="OK")
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is True
            assert "Successfully connected to Streamable server (non-JSON response)" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_auth_error(self, mcp_service):
        """Test streamable connection with authentication error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="invalid_key",
            timeout_seconds=30
        )
        
        # Mock 401 response
        mock_response = MagicMock()
        mock_response.status = 401
        mock_response.text = AsyncMock(return_value="Unauthorized")
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Authentication failed" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_not_found(self, mcp_service):
        """Test streamable connection with 404 error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/nonexistent",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock 404 response
        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.text = AsyncMock(return_value="Not found")
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Endpoint not found" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_timeout(self, mcp_service):
        """Test streamable connection with timeout."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=1
        )
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            # Simulate timeout
            mock_session.get.side_effect = asyncio.TimeoutError()
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Connection timed out after 1 seconds" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_json_parse_error(self, mcp_service):
        """Test streamable connection with invalid JSON response."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock response with JSON content type but invalid JSON
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json = AsyncMock(side_effect=Exception("Invalid JSON"))
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is True
            assert "Connected to server but response is not valid JSON" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_general_error(self, mcp_service):
        """Test streamable connection with unexpected error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        with patch('aiohttp.ClientSession', side_effect=Exception("Unexpected error")):
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Error testing connection: Unexpected error" in result.message
    
    @pytest.mark.asyncio
    async def test_test_streamable_connection_other_http_error(self, mcp_service):
        """Test streamable connection with other HTTP error."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com/api/mcp/",
            server_type="streamable",
            api_key="test_key",
            timeout_seconds=30
        )
        
        # Mock 500 response
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_streamable_connection(test_data)
            
            assert result.success is False
            assert "Failed to connect: HTTP 500" in result.message


class TestGetSettings:
    """Test get_settings method."""
    
    @pytest.mark.asyncio
    async def test_get_settings_success(self, mcp_service, mock_settings_repository):
        """Test successful settings retrieval."""
        mock_settings = MockMCPSettings(id=1, global_enabled=True)
        mock_settings_repository.get_settings = AsyncMock(return_value=mock_settings)
        
        result = await mcp_service.get_settings()
        
        assert isinstance(result, MCPSettingsResponse)
        assert result.global_enabled is True
        mock_settings_repository.get_settings.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_settings_error(self, mcp_service, mock_settings_repository):
        """Test settings retrieval with error."""
        mock_settings_repository.get_settings = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.get_settings()
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Error getting MCP settings" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_settings_model_validate_error(self, mcp_service, mock_settings_repository):
        """Test settings retrieval with model validation error (line 487)."""
        mock_settings = MockMCPSettings(id=1, global_enabled=True)
        mock_settings_repository.get_settings = AsyncMock(return_value=mock_settings)
        
        # Mock model_validate to raise an exception
        with patch.object(MCPSettingsResponse, 'model_validate', side_effect=Exception("Validation error")):
            with pytest.raises(HTTPException) as exc_info:
                await mcp_service.get_settings()
            
            assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "Error getting MCP settings" in str(exc_info.value.detail)


class TestUpdateSettings:
    """Test update_settings method."""
    
    @pytest.mark.asyncio
    async def test_update_settings_success(self, mcp_service, mock_settings_repository):
        """Test successful settings update."""
        settings_data = MCPSettingsUpdate(global_enabled=False)
        current_settings = MockMCPSettings(id=1, global_enabled=True)
        updated_settings = MockMCPSettings(id=1, global_enabled=False)
        
        mock_settings_repository.get_settings = AsyncMock(return_value=current_settings)
        mock_settings_repository.update = AsyncMock(return_value=updated_settings)
        
        result = await mcp_service.update_settings(settings_data)
        
        assert isinstance(result, MCPSettingsResponse)
        assert result.global_enabled is False
        mock_settings_repository.get_settings.assert_called_once()
        mock_settings_repository.update.assert_called_once_with(1, settings_data.model_dump())
    
    @pytest.mark.asyncio
    async def test_update_settings_error(self, mcp_service, mock_settings_repository):
        """Test settings update with error."""
        settings_data = MCPSettingsUpdate(global_enabled=False)
        mock_settings_repository.get_settings = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.update_settings(settings_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Error updating MCP settings" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_settings_model_validate_error(self, mcp_service, mock_settings_repository):
        """Test settings update with model validation error (lines 510-513)."""
        settings_data = MCPSettingsUpdate(global_enabled=False)
        current_settings = MockMCPSettings(id=1, global_enabled=True)
        updated_settings = MockMCPSettings(id=1, global_enabled=False)
        
        mock_settings_repository.get_settings = AsyncMock(return_value=current_settings)
        mock_settings_repository.update = AsyncMock(return_value=updated_settings)
        
        # Mock model_validate to raise an exception
        with patch.object(MCPSettingsResponse, 'model_validate', side_effect=Exception("Validation error")):
            with pytest.raises(HTTPException) as exc_info:
                await mcp_service.update_settings(settings_data)
            
            assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "Error updating MCP settings" in str(exc_info.value.detail)


class TestErrorHandling:
    """Test comprehensive error handling."""
    
    @pytest.mark.asyncio
    async def test_http_exception_propagation(self, mcp_service, mock_server_repository):
        """Test that HTTPExceptions are properly propagated."""
        mock_server_repository.get = AsyncMock(side_effect=HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        ))
        
        with pytest.raises(HTTPException) as exc_info:
            await mcp_service.get_server_by_id(1)
        
        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
        assert exc_info.value.detail == "Access denied"
    
    @pytest.mark.asyncio
    async def test_general_exception_handling(self, mcp_service, mock_server_repository):
        """Test handling of general exceptions."""
        mock_server_repository.list = AsyncMock(side_effect=Exception("Unexpected error"))
        
        # For methods that don't explicitly handle exceptions, they should bubble up
        with pytest.raises(Exception, match="Unexpected error"):
            await mcp_service.get_all_servers()


class TestWithSession:
    """Test MCPService with session initialization."""
    
    @pytest.mark.asyncio
    async def test_service_with_session_operations(self, mcp_service_with_session):
        """Test that service with session works correctly."""
        # Test that the service can perform operations
        mcp_service_with_session.server_repository.list = AsyncMock(return_value=[])
        
        result = await mcp_service_with_session.get_all_servers()
        
        assert isinstance(result, MCPServerListResponse)
        assert result.count == 0


class TestUpdateServerDecryptionError:
    """Test update_server decryption error scenarios."""
    
    @pytest.mark.asyncio
    async def test_update_server_decryption_error_on_response(self, mcp_service, mock_server_repository):
        """Test update server when decryption fails on response."""
        server_data = MCPServerUpdate(name="updated_server")
        
        existing_server = MockMCPServer(id=1, name="old_server")
        updated_server = MockMCPServer(id=1, name="updated_server", encrypted_api_key="invalid_key")
        
        mock_server_repository.get = AsyncMock(return_value=existing_server)
        mock_server_repository.update = AsyncMock(return_value=updated_server)
        
        with patch.object(MCPServerResponse, 'model_validate') as mock_validate, \
             patch('src.services.mcp_service.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
            
            mock_response = MagicMock()
            mock_validate.return_value = mock_response
            
            result = await mcp_service.update_server(1, server_data)
            
            assert result is mock_response
            assert mock_response.api_key == ""


class TestToggleServerEnabledMessage:
    """Test toggle_server_enabled message generation."""
    
    @pytest.mark.asyncio
    async def test_toggle_server_enabled_message(self, mcp_service, mock_server_repository):
        """Test toggle server enabled status message generation."""
        enabled_server = MockMCPServer(id=1, name="test_server", enabled=True)
        mock_server_repository.toggle_enabled = AsyncMock(return_value=enabled_server)
        
        result = await mcp_service.toggle_server_enabled(1)
        
        assert isinstance(result, MCPToggleResponse)
        assert result.enabled is True
        assert "enabled" in result.message


class TestSSEConnectionEdgeCases:
    """Test SSE connection edge cases for better coverage."""
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_general_exception(self, mcp_service):
        """Test SSE connection with general exception."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="test_key",
            timeout_seconds=30
        )
        
        with patch('aiohttp.ClientSession', side_effect=Exception("Unexpected error")):
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is False
            assert "Error testing connection" in result.message
    
    
    @pytest.mark.asyncio
    async def test_test_sse_connection_no_api_key(self, mcp_service):
        """Test SSE connection without API key."""
        test_data = MCPTestConnectionRequest(
            server_url="http://test.com",
            server_type="sse",
            api_key="",  # Empty API key
            timeout_seconds=30
        )
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "text/event-stream"}
        
        # Mock the entire session and context managers properly
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.get.return_value.__aexit__.return_value = None
            
            result = await mcp_service._test_sse_connection(test_data)
            
            assert result.success is True
            # Should not add Authorization header when API key is empty
            mock_session.get.assert_called_once_with(test_data.server_url, headers={})


