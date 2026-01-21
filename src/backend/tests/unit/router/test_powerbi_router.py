"""
Unit tests for PowerBIRouter.

Tests the functionality of Power BI integration endpoints including
configuration management, DAX query execution, and status checks.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.powerbi_config import PowerBIConfigCreate, DAXQueryRequest, DAXQueryResponse


# Mock Power BI config response
class MockPowerBIConfigResponse:
    def __init__(self, tenant_id="test-tenant", client_id="test-client",
                 workspace_id="test-workspace", semantic_model_id="test-model",
                 enabled=True):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.workspace_id = workspace_id
        self.semantic_model_id = semantic_model_id
        self.enabled = enabled
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "workspace_id": self.workspace_id,
            "semantic_model_id": self.semantic_model_id,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_powerbi_service():
    """Create a mock Power BI service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "test-group"
    context.group_email = "test@example.com"
    return context


@pytest.fixture
def app(mock_powerbi_service, mock_db_session, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.powerbi_router import router, get_powerbi_service
    from src.core.dependencies import get_db, get_group_context

    app = FastAPI()
    app.include_router(router)

    # Override dependencies
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[get_powerbi_service] = lambda session=None, group_context=None: mock_powerbi_service
    app.dependency_overrides[get_group_context] = lambda: mock_group_context

    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def mock_workspace_admin():
    """Mock workspace admin check."""
    def mock_is_admin(context):
        return True
    return mock_is_admin


@pytest.fixture
def mock_non_admin():
    """Mock non-admin check."""
    def mock_is_admin(context):
        return False
    return mock_is_admin


class TestPowerBIRouterConfigEndpoints:
    """Test cases for Power BI configuration endpoints."""

    def test_set_powerbi_config_success(self, client, mock_powerbi_service):
        """Test successful Power BI configuration setting."""
        with patch('src.api.powerbi_router.is_workspace_admin', return_value=True):
            config_data = {
                "tenant_id": "test-tenant",
                "client_id": "test-client",
                "workspace_id": "test-workspace",
                "semantic_model_id": "test-model",
                "enabled": True
            }

            # Mock repository response
            mock_config = MagicMock()
            mock_config.tenant_id = config_data["tenant_id"]
            mock_config.client_id = config_data["client_id"]
            mock_config.workspace_id = config_data["workspace_id"]
            mock_config.semantic_model_id = config_data["semantic_model_id"]
            mock_config.is_enabled = config_data["enabled"]
            mock_config.is_active = True

            mock_powerbi_service.repository.create_config.return_value = mock_config

            response = client.post("/powerbi/config", json=config_data)

            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Power BI configuration saved successfully"
            assert "config" in data

    def test_set_powerbi_config_not_admin(self, client, mock_powerbi_service):
        """Test Power BI configuration setting by non-admin."""
        with patch('src.api.powerbi_router.is_workspace_admin', return_value=False):
            config_data = {
                "tenant_id": "test-tenant",
                "client_id": "test-client",
                "enabled": True
            }

            response = client.post("/powerbi/config", json=config_data)

            assert response.status_code == 403
            assert "admin" in response.json()["detail"].lower()

    def test_set_powerbi_config_error(self, client, mock_powerbi_service):
        """Test Power BI configuration setting with service error."""
        with patch('src.api.powerbi_router.is_workspace_admin', return_value=True):
            config_data = {
                "tenant_id": "test-tenant",
                "client_id": "test-client",
                "enabled": True
            }

            mock_powerbi_service.repository.create_config.side_effect = Exception("Database error")

            response = client.post("/powerbi/config", json=config_data)

            assert response.status_code == 500
            assert "error" in response.json()["detail"].lower()

    def test_get_powerbi_config_success(self, client, mock_powerbi_service):
        """Test successful Power BI configuration retrieval."""
        mock_config = MagicMock()
        mock_config.tenant_id = "test-tenant"
        mock_config.client_id = "test-client"
        mock_config.workspace_id = "test-workspace"
        mock_config.semantic_model_id = "test-model"
        mock_config.is_enabled = True

        mock_powerbi_service.repository.get_active_config.return_value = mock_config

        response = client.get("/powerbi/config")

        assert response.status_code == 200
        data = response.json()
        assert data["tenant_id"] == "test-tenant"
        assert data["client_id"] == "test-client"
        assert data["enabled"] is True

    def test_get_powerbi_config_not_found(self, client, mock_powerbi_service):
        """Test Power BI configuration retrieval when not configured."""
        mock_powerbi_service.repository.get_active_config.return_value = None

        response = client.get("/powerbi/config")

        assert response.status_code == 200
        data = response.json()
        assert data["tenant_id"] == ""
        assert data["enabled"] is False

    def test_get_powerbi_config_error(self, client, mock_powerbi_service):
        """Test Power BI configuration retrieval with service error."""
        mock_powerbi_service.repository.get_active_config.side_effect = Exception("Database error")

        response = client.get("/powerbi/config")

        assert response.status_code == 500


class TestPowerBIRouterQueryEndpoint:
    """Test cases for DAX query execution endpoint."""

    def test_execute_dax_query_success(self, client, mock_powerbi_service):
        """Test successful DAX query execution."""
        query_request = {
            "dax_query": "EVALUATE 'Sales'",
            "semantic_model_id": "test-model"
        }

        mock_response = DAXQueryResponse(
            status="success",
            data=[{"Region": "East", "Total": 1000}],
            row_count=1,
            columns=["Region", "Total"],
            execution_time_ms=250
        )

        mock_powerbi_service.execute_dax_query.return_value = mock_response

        response = client.post("/powerbi/query", json=query_request)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["row_count"] == 1
        assert len(data["data"]) == 1

    def test_execute_dax_query_missing_query(self, client):
        """Test DAX query execution with missing query."""
        query_request = {
            "semantic_model_id": "test-model"
        }

        response = client.post("/powerbi/query", json=query_request)

        assert response.status_code == 422  # Validation error

    def test_execute_dax_query_service_error(self, client, mock_powerbi_service):
        """Test DAX query execution with service error."""
        query_request = {
            "dax_query": "EVALUATE 'Sales'",
            "semantic_model_id": "test-model"
        }

        mock_powerbi_service.execute_dax_query.side_effect = Exception("Query execution failed")

        response = client.post("/powerbi/query", json=query_request)

        assert response.status_code == 500
        assert "error" in response.json()["detail"].lower()

    def test_execute_dax_query_http_exception(self, client, mock_powerbi_service):
        """Test DAX query execution with HTTP exception."""
        query_request = {
            "dax_query": "EVALUATE 'Sales'",
            "semantic_model_id": "test-model"
        }

        mock_powerbi_service.execute_dax_query.side_effect = HTTPException(
            status_code=400,
            detail="Invalid DAX query"
        )

        response = client.post("/powerbi/query", json=query_request)

        assert response.status_code == 400
        assert "Invalid DAX query" in response.json()["detail"]


class TestPowerBIRouterStatusEndpoint:
    """Test cases for Power BI status endpoint."""

    def test_check_powerbi_status_configured(self, client, mock_powerbi_service):
        """Test status check when Power BI is configured."""
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.workspace_id = "test-workspace"
        mock_config.semantic_model_id = "test-model"

        mock_powerbi_service.repository.get_active_config.return_value = mock_config

        response = client.get("/powerbi/status")

        assert response.status_code == 200
        data = response.json()
        assert data["configured"] is True
        assert data["enabled"] is True
        assert "ready" in data["message"].lower()

    def test_check_powerbi_status_not_configured(self, client, mock_powerbi_service):
        """Test status check when Power BI is not configured."""
        mock_powerbi_service.repository.get_active_config.return_value = None

        response = client.get("/powerbi/status")

        assert response.status_code == 200
        data = response.json()
        assert data["configured"] is False
        assert data["enabled"] is False

    def test_check_powerbi_status_disabled(self, client, mock_powerbi_service):
        """Test status check when Power BI is configured but disabled."""
        mock_config = MagicMock()
        mock_config.is_enabled = False
        mock_config.workspace_id = "test-workspace"
        mock_config.semantic_model_id = "test-model"

        mock_powerbi_service.repository.get_active_config.return_value = mock_config

        response = client.get("/powerbi/status")

        assert response.status_code == 200
        data = response.json()
        assert data["configured"] is True
        assert data["enabled"] is False
        assert "disabled" in data["message"].lower()

    def test_check_powerbi_status_error(self, client, mock_powerbi_service):
        """Test status check with service error."""
        mock_powerbi_service.repository.get_active_config.side_effect = Exception("Database error")

        response = client.get("/powerbi/status")

        assert response.status_code == 500


class TestPowerBIRouterMultiTenancy:
    """Test cases for multi-tenant functionality."""

    def test_config_uses_group_context(self, client, mock_powerbi_service, mock_group_context):
        """Test that configuration uses group context."""
        with patch('src.api.powerbi_router.is_workspace_admin', return_value=True):
            config_data = {
                "tenant_id": "test-tenant",
                "client_id": "test-client",
                "enabled": True
            }

            mock_config = MagicMock()
            mock_powerbi_service.repository.create_config.return_value = mock_config

            response = client.post("/powerbi/config", json=config_data)

            assert response.status_code == 200

            # Verify create_config was called with group_id
            call_args = mock_powerbi_service.repository.create_config.call_args[0][0]
            assert call_args["group_id"] == "test-group"
            assert call_args["created_by_email"] == "test@example.com"

    def test_query_uses_group_context(self, client, mock_powerbi_service):
        """Test that queries use group context."""
        query_request = {
            "dax_query": "EVALUATE 'Sales'",
            "semantic_model_id": "test-model"
        }

        mock_response = DAXQueryResponse(
            status="success",
            data=[],
            row_count=0,
            columns=[],
            execution_time_ms=100
        )

        mock_powerbi_service.execute_dax_query.return_value = mock_response

        response = client.post("/powerbi/query", json=query_request)

        assert response.status_code == 200

        # Verify service was called (group_id is set during service initialization)
        mock_powerbi_service.execute_dax_query.assert_called_once()
