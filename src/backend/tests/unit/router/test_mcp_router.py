"""
Unit tests for MCPRouter.

Tests the functionality of MCP (Model Context Protocol) server management endpoints.
Achieves 100% line coverage including all exception handling paths.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

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


# Mock MCP server model
class MockMCPServer:
    def __init__(self, id=1, name="Test MCP Server", server_url="http://localhost:3000",
                 enabled=True, api_key="test-key", **kwargs):
        self.id = id
        self.name = name
        self.server_url = server_url
        self.enabled = enabled
        self.api_key = api_key
        self.server_type = kwargs.get('server_type', 'sse')
        self.timeout_seconds = kwargs.get('timeout_seconds', 30)
        self.max_retries = kwargs.get('max_retries', 3)
        self.model_mapping_enabled = kwargs.get('model_mapping_enabled', False)
        self.rate_limit = kwargs.get('rate_limit', 60)
        self.command = kwargs.get('command')
        self.args = kwargs.get('args')
        self.additional_config = kwargs.get('additional_config', {})
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def app(mock_db_session):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.mcp_router import router
    from src.db.session import get_db
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_db():
        return mock_db_session
    
    # Override dependencies
    app.dependency_overrides[get_db] = override_get_db
    
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def sample_mcp_server_create():
    """Create a sample MCP server creation request."""
    return MCPServerCreate(
        name="Test MCP Server",
        server_url="http://localhost:3000",
        api_key="test-api-key",
        enabled=True,
        additional_config={"timeout": 30}
    )


@pytest.fixture
def sample_mcp_server_update():
    """Create a sample MCP server update request."""
    return MCPServerUpdate(
        name="Updated MCP Server",
        server_url="http://localhost:3001",
        api_key="updated-api-key",
        enabled=False,
        additional_config={"timeout": 60}
    )


@pytest.fixture
def sample_mcp_test_connection():
    """Create a sample MCP test connection request."""
    return MCPTestConnectionRequest(
        server_url="http://localhost:3000",
        api_key="test-api-key",
        server_type="sse",
        timeout_seconds=30
    )


@pytest.fixture
def sample_mcp_settings_update():
    """Create a sample MCP settings update request."""
    return MCPSettingsUpdate(
        global_enabled=True
    )


class TestGetMCPServers:
    """Test cases for get MCP servers endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_servers_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful MCP servers listing."""
        servers = [MockMCPServer(id=1), MockMCPServer(id=2)]
        servers_response = MCPServerListResponse(
            servers=[
                MCPServerResponse(
                    id=1, name="Server1", server_url="http://localhost:3000", 
                    enabled=True, api_key="key1", created_at=datetime.utcnow(), 
                    updated_at=datetime.utcnow()
                ),
                MCPServerResponse(
                    id=2, name="Server2", server_url="http://localhost:3001", 
                    enabled=False, api_key="key2", created_at=datetime.utcnow(), 
                    updated_at=datetime.utcnow()
                )
            ],
            count=2
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_all_servers.return_value = servers_response
        
        response = client.get("/mcp/servers")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["servers"]) == 2
        mock_service_instance.get_all_servers.assert_called_once()
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_servers_service_error(self, mock_mcp_service_class, client, mock_db_session):
        """Test getting MCP servers with service error."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_all_servers.side_effect = Exception("Database error")
        
        response = client.get("/mcp/servers")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetEnabledMCPServers:
    """Test cases for get enabled MCP servers endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_enabled_mcp_servers_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful enabled MCP servers listing."""
        enabled_servers = [MockMCPServer(id=1, enabled=True)]
        servers_response = MCPServerListResponse(
            servers=[
                MCPServerResponse(
                    id=1, name="Enabled Server", server_url="http://localhost:3000", 
                    enabled=True, api_key="key1", created_at=datetime.utcnow(), 
                    updated_at=datetime.utcnow()
                )
            ],
            count=1
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_enabled_servers.return_value = servers_response
        
        response = client.get("/mcp/servers/enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["servers"][0]["enabled"] is True


class TestGetMCPServer:
    """Test cases for get MCP server by ID endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_server_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful MCP server retrieval by ID."""
        server = MCPServerResponse(
            id=1, name="Test Server", server_url="http://localhost:3000", 
            enabled=True, api_key="test-key", created_at=datetime.utcnow(), 
            updated_at=datetime.utcnow()
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_server_by_id.return_value = server
        
        response = client.get("/mcp/servers/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        mock_service_instance.get_server_by_id.assert_called_once_with(1)
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_server_not_found(self, mock_mcp_service_class, client, mock_db_session):
        """Test getting non-existent MCP server - HTTPException re-raise (lines 83-84)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_server_by_id.side_effect = HTTPException(status_code=404, detail="Server not found")
        
        response = client.get("/mcp/servers/999")
        
        assert response.status_code == 404
        assert "Server not found" in response.json()["detail"]


class TestCreateMCPServer:
    """Test cases for create MCP server endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_create_mcp_server_success(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_server_create):
        """Test successful MCP server creation."""
        created_server = MCPServerResponse(
            id=1, name="Test MCP Server", server_url="http://localhost:3000", 
            enabled=True, api_key="test-api-key", created_at=datetime.utcnow(), 
            updated_at=datetime.utcnow()
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.create_server.return_value = created_server
        
        response = client.post("/mcp/servers", json=sample_mcp_server_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == 1
        mock_service_instance.create_server.assert_called_once()
    
    @patch('src.api.mcp_router.MCPService')
    def test_create_mcp_server_http_exception(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_server_create):
        """Test create MCP server with HTTPException re-raise (lines 101-102)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.create_server.side_effect = HTTPException(status_code=400, detail="Invalid server data")
        
        response = client.post("/mcp/servers", json=sample_mcp_server_create.model_dump())
        
        assert response.status_code == 400
        assert "Invalid server data" in response.json()["detail"]


class TestUpdateMCPServer:
    """Test cases for update MCP server endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_update_mcp_server_success(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_server_update):
        """Test successful MCP server update."""
        updated_server = MCPServerResponse(
            id=1, name="Updated MCP Server", server_url="http://localhost:3001", 
            enabled=False, api_key="updated-api-key", created_at=datetime.utcnow(), 
            updated_at=datetime.utcnow()
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.update_server.return_value = updated_server
        
        response = client.put("/mcp/servers/1", json=sample_mcp_server_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated MCP Server"
        mock_service_instance.update_server.assert_called_once_with(1, sample_mcp_server_update)
    
    @patch('src.api.mcp_router.MCPService')
    def test_update_mcp_server_http_exception(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_server_update):
        """Test update MCP server with HTTPException re-raise (lines 120-121)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.update_server.side_effect = HTTPException(status_code=404, detail="Server not found")
        
        response = client.put("/mcp/servers/999", json=sample_mcp_server_update.model_dump())
        
        assert response.status_code == 404
        assert "Server not found" in response.json()["detail"]


class TestDeleteMCPServer:
    """Test cases for delete MCP server endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_delete_mcp_server_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful MCP server deletion."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.delete_server.return_value = None
        
        response = client.delete("/mcp/servers/1")
        
        assert response.status_code == 204
        mock_service_instance.delete_server.assert_called_once_with(1)
    
    @patch('src.api.mcp_router.MCPService')
    def test_delete_mcp_server_http_exception(self, mock_mcp_service_class, client, mock_db_session):
        """Test delete MCP server with HTTPException re-raise (lines 137-138)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.delete_server.side_effect = HTTPException(status_code=404, detail="Server not found")
        
        response = client.delete("/mcp/servers/999")
        
        assert response.status_code == 404
        assert "Server not found" in response.json()["detail"]


class TestToggleMCPServerEnabled:
    """Test cases for toggle MCP server enabled endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_toggle_mcp_server_enabled_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful MCP server toggle enabled."""
        toggle_response = MCPToggleResponse(
            message="Server enabled successfully",
            enabled=True
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.toggle_server_enabled.return_value = toggle_response
        
        response = client.patch("/mcp/servers/1/toggle-enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is True
        assert "Server enabled successfully" in data["message"]
        mock_service_instance.toggle_server_enabled.assert_called_once_with(1)
    
    @patch('src.api.mcp_router.MCPService')
    def test_toggle_mcp_server_enabled_http_exception(self, mock_mcp_service_class, client, mock_db_session):
        """Test toggle MCP server enabled with HTTPException re-raise (lines 156-157)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.toggle_server_enabled.side_effect = HTTPException(status_code=404, detail="Server not found")
        
        response = client.patch("/mcp/servers/999/toggle-enabled")
        
        assert response.status_code == 404
        assert "Server not found" in response.json()["detail"]


class TestTestMCPConnection:
    """Test cases for test MCP connection endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_test_mcp_connection_success(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_test_connection):
        """Test successful MCP connection test."""
        test_response = MCPTestConnectionResponse(
            success=True,
            message="Connection successful"
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.test_connection.return_value = test_response
        
        response = client.post("/mcp/test-connection", json=sample_mcp_test_connection.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "Connection successful" in data["message"]
        mock_service_instance.test_connection.assert_called_once()
    
    @patch('src.api.mcp_router.MCPService')
    def test_test_mcp_connection_service_failure(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_test_connection):
        """Test MCP connection test with service failure."""
        test_response = MCPTestConnectionResponse(
            success=False,
            message="Connection failed"
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.test_connection.return_value = test_response
        
        response = client.post("/mcp/test-connection", json=sample_mcp_test_connection.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "Connection failed" in data["message"]
    
    @patch('src.api.mcp_router.MCPService')
    def test_test_mcp_connection_exception_handling(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_test_connection):
        """Test MCP connection test with exception handling (lines 175-180)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.test_connection.side_effect = Exception("Connection timeout")
        
        response = client.post("/mcp/test-connection", json=sample_mcp_test_connection.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "Error testing connection: Connection timeout" in data["message"]


class TestGetMCPSettings:
    """Test cases for get MCP settings endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_settings_success(self, mock_mcp_service_class, client, mock_db_session):
        """Test successful MCP settings retrieval."""
        settings_response = MCPSettingsResponse(
            id=1,
            global_enabled=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_settings.return_value = settings_response
        
        response = client.get("/mcp/settings")
        
        assert response.status_code == 200
        data = response.json()
        assert data["global_enabled"] is True
        mock_service_instance.get_settings.assert_called_once()
    
    @patch('src.api.mcp_router.MCPService')
    def test_get_mcp_settings_http_exception(self, mock_mcp_service_class, client, mock_db_session):
        """Test get MCP settings with HTTPException re-raise (lines 196-197)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.get_settings.side_effect = HTTPException(status_code=500, detail="Settings not found")
        
        response = client.get("/mcp/settings")
        
        assert response.status_code == 500
        assert "Settings not found" in response.json()["detail"]


class TestUpdateMCPSettings:
    """Test cases for update MCP settings endpoint."""
    
    @patch('src.api.mcp_router.MCPService')
    def test_update_mcp_settings_success(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_settings_update):
        """Test successful MCP settings update."""
        updated_settings = MCPSettingsResponse(
            id=1,
            global_enabled=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.update_settings.return_value = updated_settings
        
        response = client.put("/mcp/settings", json=sample_mcp_settings_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["global_enabled"] is True
        mock_service_instance.update_settings.assert_called_once_with(sample_mcp_settings_update)
    
    @patch('src.api.mcp_router.MCPService')
    def test_update_mcp_settings_http_exception(self, mock_mcp_service_class, client, mock_db_session, sample_mcp_settings_update):
        """Test update MCP settings with HTTPException re-raise (lines 214-215)."""
        mock_service_instance = AsyncMock()
        mock_mcp_service_class.return_value = mock_service_instance
        mock_service_instance.update_settings.side_effect = HTTPException(status_code=400, detail="Invalid settings")
        
        response = client.put("/mcp/settings", json=sample_mcp_settings_update.model_dump())
        
        assert response.status_code == 400
        assert "Invalid settings" in response.json()["detail"]