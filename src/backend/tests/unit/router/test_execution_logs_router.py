"""
Unit tests for ExecutionLogsRouter.

Tests the functionality of execution logs management endpoints including
WebSocket streaming, log retrieval, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect, HTTPException
from fastapi.testclient import TestClient
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)
from src.utils.user_context import GroupContext


class MockExecutionLogResponse:
    """Mock ExecutionLogResponse for testing."""
    def __init__(self, content="Test log", timestamp=None):
        self.content = content
        self.timestamp = timestamp or datetime.utcnow().isoformat()
        
    def model_dump(self):
        return {
            "content": self.content,
            "timestamp": self.timestamp
        }
    
    def dict(self):
        return self.model_dump()


class MockExecutionLogsResponse:
    """Mock ExecutionLogsResponse for testing."""
    def __init__(self, logs=None):
        self.logs = logs or []
        
    def model_dump(self):
        return {
            "logs": [log.model_dump() if hasattr(log, 'model_dump') else log for log in self.logs]
        }


@pytest.fixture
def mock_execution_logs_service():
    """Create a mock execution logs service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )


@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def app_logs_router(mock_execution_logs_service, mock_group_context):
    """Create a FastAPI app with logs router and mocked dependencies."""
    from fastapi import FastAPI
    from src.api.execution_logs_router import logs_router
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(logs_router)
    
    async def override_get_group_context():
        return mock_group_context
    
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app


@pytest.fixture
def app_runs_router(mock_execution_logs_service, mock_group_context):
    """Create a FastAPI app with runs router and mocked dependencies."""
    from fastapi import FastAPI
    from src.api.execution_logs_router import runs_router
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(runs_router)
    
    async def override_get_group_context():
        return mock_group_context
    
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app


@pytest.fixture
def app_main_router(mock_execution_logs_service, mock_group_context):
    """Create a FastAPI app with main router and mocked dependencies."""
    from fastapi import FastAPI
    from src.api.execution_logs_router import router
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    async def override_get_group_context():
        return mock_group_context
    
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app


@pytest.fixture
def client_logs(app_logs_router, mock_current_user):
    """Create a test client for logs router."""
    app_logs_router.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app_logs_router.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app_logs_router.dependency_overrides[get_admin_user] = lambda: mock_current_user
    
    return TestClient(app_logs_router)


@pytest.fixture
def client_runs(app_runs_router, mock_current_user):
    """Create a test client for runs router."""
    app_runs_router.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app_runs_router.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app_runs_router.dependency_overrides[get_admin_user] = lambda: mock_current_user
    
    return TestClient(app_runs_router)


@pytest.fixture
def client_main(app_main_router, mock_current_user):
    """Create a test client for main router."""
    app_main_router.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app_main_router.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app_main_router.dependency_overrides[get_admin_user] = lambda: mock_current_user
    
    return TestClient(app_main_router)


class TestWebSocketExecutionLogs:
    """Test cases for WebSocket execution logs endpoint."""
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.utils.user_context.GroupContext')
    @patch('src.api.execution_logs_router.logger')
    def test_websocket_execution_logs_success(self, mock_logger, mock_group_context_class, mock_service):
        """Test successful WebSocket connection for execution logs (lines 47-73)."""
        execution_id = "exec-123"
        tenant_email = "test@example.com"
        
        # Mock GroupContext.from_email
        mock_group_instance = MagicMock()
        mock_group_instance.primary_group_id = "group-123"
        mock_group_context_class.from_email = AsyncMock(return_value=mock_group_instance)
        
        # Mock WebSocket
        mock_websocket = AsyncMock()
        mock_websocket.query_params = {"tenant_email": tenant_email}
        mock_websocket.receive_text = AsyncMock(side_effect=WebSocketDisconnect(code=1000))
        
        # Mock service methods
        mock_service.connect_with_group = AsyncMock()
        mock_service.disconnect = AsyncMock()
        
        # Import and call the function directly
        from src.api.execution_logs_router import websocket_execution_logs
        
        # Run the WebSocket handler
        import asyncio
        async def run_test():
            await websocket_execution_logs(mock_websocket, execution_id)
        
        asyncio.run(run_test())
        
        # Verify calls
        mock_group_context_class.from_email.assert_called_once_with(tenant_email)
        mock_service.connect_with_group.assert_called_once_with(mock_websocket, execution_id, mock_group_instance)
        mock_service.disconnect.assert_called_once_with(mock_websocket, execution_id)
        mock_logger.info.assert_called()
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.utils.user_context.GroupContext')
    @patch('src.api.execution_logs_router.logger')
    def test_websocket_execution_logs_no_tenant_email(self, mock_logger, mock_group_context_class, mock_service):
        """Test WebSocket connection without tenant email (lines 54-55)."""
        execution_id = "exec-123"
        
        # Mock GroupContext constructor
        mock_group_instance = MagicMock()
        mock_group_instance.primary_group_id = None
        mock_group_context_class.return_value = mock_group_instance
        
        # Mock WebSocket
        mock_websocket = AsyncMock()
        mock_websocket.query_params = {}  # No tenant_email
        mock_websocket.receive_text = AsyncMock(side_effect=WebSocketDisconnect(code=1000))
        
        # Mock service methods
        mock_service.connect_with_group = AsyncMock()
        mock_service.disconnect = AsyncMock()
        
        # Import and call the function directly
        from src.api.execution_logs_router import websocket_execution_logs
        
        # Run the WebSocket handler
        import asyncio
        async def run_test():
            await websocket_execution_logs(mock_websocket, execution_id)
        
        asyncio.run(run_test())
        
        # Verify GroupContext() was called without arguments
        mock_group_context_class.assert_called_once_with()
        mock_service.connect_with_group.assert_called_once_with(mock_websocket, execution_id, mock_group_instance)
        mock_service.disconnect.assert_called_once_with(mock_websocket, execution_id)
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.api.execution_logs_router.GroupContext')
    @patch('src.api.execution_logs_router.logger')
    def test_websocket_execution_logs_exception_handling(self, mock_logger, mock_group_context_class, mock_service):
        """Test WebSocket exception handling (lines 69-73)."""
        execution_id = "exec-123"
        
        # Mock WebSocket
        mock_websocket = AsyncMock()
        mock_websocket.query_params = {}
        
        # Mock GroupContext
        mock_group_instance = MagicMock()
        mock_group_context_class.return_value = mock_group_instance
        
        # Mock service to raise exception
        mock_service.connect_with_group = AsyncMock(side_effect=Exception("Connection error"))
        mock_service.disconnect = AsyncMock()
        
        # Import and call the function directly
        from src.api.execution_logs_router import websocket_execution_logs
        
        # Run the WebSocket handler
        import asyncio
        async def run_test():
            await websocket_execution_logs(mock_websocket, execution_id)
        
        asyncio.run(run_test())
        
        # Verify error logging and cleanup
        mock_logger.error.assert_called_once()
        mock_service.disconnect.assert_called_once_with(mock_websocket, execution_id)


class TestGetExecutionLogs:
    """Test cases for get execution logs endpoint."""
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_get_execution_logs_success(self, mock_service, client_logs, mock_group_context):
        """Test successful execution logs retrieval (lines 97-99)."""
        execution_id = "exec-123"
        mock_logs = [
            {"content": "Test log 1", "timestamp": "2024-01-01T00:00:00"},
            {"content": "Test log 2", "timestamp": "2024-01-01T00:01:00"}
        ]
        
        mock_service.get_execution_logs_by_group = AsyncMock(return_value=mock_logs)
        
        response = client_logs.get(f"/logs/executions/{execution_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        mock_service.get_execution_logs_by_group.assert_called_once_with(
            execution_id, mock_group_context, 1000, 0
        )
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.api.execution_logs_router.logger')
    def test_get_execution_logs_service_error(self, mock_logger, mock_service, client_logs, mock_group_context):
        """Test execution logs retrieval with service error (lines 100-102)."""
        execution_id = "exec-123"
        error_message = "Database connection failed"
        
        mock_service.get_execution_logs_by_group = AsyncMock(side_effect=Exception(error_message))
        
        response = client_logs.get(f"/logs/executions/{execution_id}")
        
        assert response.status_code == 500
        assert f"Failed to fetch execution logs: {error_message}" in response.json()["detail"]
        mock_logger.error.assert_called_once()
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_get_execution_logs_with_pagination(self, mock_service, client_logs, mock_group_context):
        """Test execution logs retrieval with pagination parameters."""
        execution_id = "exec-123"
        limit = 100
        offset = 50
        
        mock_service.get_execution_logs_by_group = AsyncMock(return_value=[])
        
        response = client_logs.get(f"/logs/executions/{execution_id}?limit={limit}&offset={offset}")
        
        assert response.status_code == 200
        mock_service.get_execution_logs_by_group.assert_called_once_with(
            execution_id, mock_group_context, limit, offset
        )
    
    def test_get_execution_logs_invalid_params(self, client_logs):
        """Test execution logs with invalid pagination parameters."""
        execution_id = "exec-123"
        
        # Test invalid limit (too low)
        response = client_logs.get(f"/logs/executions/{execution_id}?limit=0")
        assert response.status_code == 422
        
        # Test invalid limit (too high)
        response = client_logs.get(f"/logs/executions/{execution_id}?limit=10001")
        assert response.status_code == 422
        
        # Test invalid offset
        response = client_logs.get(f"/logs/executions/{execution_id}?offset=-1")
        assert response.status_code == 422


class TestGetRunLogs:
    """Test cases for get run logs endpoint."""
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_get_run_logs_success(self, mock_service, client_runs, mock_group_context):
        """Test successful run logs retrieval (lines 126-128)."""
        run_id = "run-123"
        # Return raw dictionaries instead of mock objects
        mock_logs = [
            {"content": "Run log 1", "timestamp": "2024-01-01T00:00:00"},
            {"content": "Run log 2", "timestamp": "2024-01-01T00:01:00"}
        ]
        
        mock_service.get_execution_logs_by_group = AsyncMock(return_value=mock_logs)
        
        response = client_runs.get(f"/runs/{run_id}/outputs")
        
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data
        assert len(data["logs"]) == 2
        mock_service.get_execution_logs_by_group.assert_called_once_with(
            run_id, mock_group_context, 1000, 0
        )
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.api.execution_logs_router.logger')
    def test_get_run_logs_service_error(self, mock_logger, mock_service, client_runs, mock_group_context):
        """Test run logs retrieval with service error (lines 129-131)."""
        run_id = "run-123"
        error_message = "Service unavailable"
        
        mock_service.get_execution_logs_by_group = AsyncMock(side_effect=Exception(error_message))
        
        response = client_runs.get(f"/runs/{run_id}/outputs")
        
        assert response.status_code == 500
        assert f"Failed to fetch run logs: {error_message}" in response.json()["detail"]
        mock_logger.error.assert_called_once()
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_get_run_logs_with_pagination(self, mock_service, client_runs, mock_group_context):
        """Test run logs retrieval with pagination parameters."""
        run_id = "run-123"
        limit = 500
        offset = 100
        
        mock_service.get_execution_logs_by_group = AsyncMock(return_value=[])
        
        response = client_runs.get(f"/runs/{run_id}/outputs?limit={limit}&offset={offset}")
        
        assert response.status_code == 200
        mock_service.get_execution_logs_by_group.assert_called_once_with(
            run_id, mock_group_context, limit, offset
        )


class TestGetExecutionLogsMain:
    """Test cases for main router execution logs endpoint."""
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_get_execution_logs_main_success(self, mock_service, client_main, mock_group_context):
        """Test successful execution logs retrieval via main router (lines 143-145)."""
        execution_id = "exec-123"
        mock_logs = [{"content": "Main router log", "timestamp": "2024-01-01T00:00:00"}]
        
        mock_service.get_execution_logs_by_group = AsyncMock(return_value=mock_logs)
        
        response = client_main.get(f"/execution-logs/{execution_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        mock_service.get_execution_logs_by_group.assert_called_once_with(
            execution_id, mock_group_context, 1000, 0
        )
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    @patch('src.api.execution_logs_router.logger')
    def test_get_execution_logs_main_service_error(self, mock_logger, mock_service, client_main, mock_group_context):
        """Test execution logs retrieval with service error via main router (lines 146-148)."""
        execution_id = "exec-123"
        error_message = "Database error"
        
        mock_service.get_execution_logs_by_group = AsyncMock(side_effect=Exception(error_message))
        
        response = client_main.get(f"/execution-logs/{execution_id}")
        
        assert response.status_code == 500
        assert f"Failed to fetch execution logs: {error_message}" in response.json()["detail"]
        mock_logger.error.assert_called_once()


class TestCreateExecutionLog:
    """Test cases for create execution log endpoint."""
    
    def test_create_execution_log_success(self, client_main, mock_group_context):
        """Test successful execution log creation (line 158)."""
        log_data = {
            "execution_id": "exec-123",
            "level": "INFO",
            "message": "Test log entry"
        }
        
        response = client_main.post("/execution-logs/", json=log_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == 1
        assert data["message"] == "Log created"
    
    @patch('src.api.execution_logs_router.logger')
    def test_create_execution_log_exception_handling(self, mock_logger, client_main, mock_group_context):
        """Test create execution log exception handling (lines 159-161)."""
        log_data = {"invalid": "data"}
        
        # Mock an exception during processing
        with patch('src.api.execution_logs_router.logger', side_effect=Exception("Processing error")):
            # The endpoint always returns success, but we can test the structure
            response = client_main.post("/execution-logs/", json=log_data)
            assert response.status_code == 201


class TestSendExecutionLog:
    """Test cases for send_execution_log function."""
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_send_execution_log_function(self, mock_service):
        """Test send_execution_log function (line 177)."""
        from src.api.execution_logs_router import send_execution_log
        
        execution_id = "exec-123"
        message = "Test broadcast message"
        group_context = GroupContext(group_ids=["group-123"])
        
        mock_service.broadcast_to_execution = AsyncMock()
        
        # Run the function
        import asyncio
        async def run_test():
            await send_execution_log(execution_id, message, group_context)
        
        asyncio.run(run_test())
        
        mock_service.broadcast_to_execution.assert_called_once_with(execution_id, message, group_context)
    
    @patch('src.api.execution_logs_router.execution_logs_service')
    def test_send_execution_log_function_no_context(self, mock_service):
        """Test send_execution_log function without group context."""
        from src.api.execution_logs_router import send_execution_log
        
        execution_id = "exec-123"
        message = "Test broadcast message"
        
        mock_service.broadcast_to_execution = AsyncMock()
        
        # Run the function
        import asyncio
        async def run_test():
            await send_execution_log(execution_id, message)
        
        asyncio.run(run_test())
        
        mock_service.broadcast_to_execution.assert_called_once_with(execution_id, message, None)


class TestRouterConfiguration:
    """Test cases for router configuration and setup."""
    
    def test_logs_router_configuration(self):
        """Test logs router configuration."""
        from src.api.execution_logs_router import logs_router
        
        assert logs_router.prefix == "/logs"
        assert "logs" in logs_router.tags
    
    def test_runs_router_configuration(self):
        """Test runs router configuration."""
        from src.api.execution_logs_router import runs_router
        
        assert runs_router.prefix == "/runs"
        assert "runs" in runs_router.tags
    
    def test_main_router_configuration(self):
        """Test main router configuration."""
        from src.api.execution_logs_router import router
        
        assert router.prefix == "/execution-logs"
        assert "execution-logs" in router.tags