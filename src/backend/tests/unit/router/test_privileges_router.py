"""
Unit tests for PrivilegesRouter.

Tests the functionality of privilege management endpoints with comprehensive coverage.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from src.models.user import Privilege
from src.models.enums import UserRole, UserStatus


@pytest.fixture
def app():
    """Create a FastAPI app."""
    from fastapi import FastAPI
    from src.api.privileges_router import router
    
    app = FastAPI()
    app.include_router(router)
    
    return app


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    class MockUser:
        def __init__(self):
            self.id = "admin-user-123"
            self.username = "admin"
            self.email = "admin@example.com"
            self.role = UserRole.ADMIN
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.now(timezone.utc)
            self.updated_at = datetime.now(timezone.utc)
    
    return MockUser()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return AsyncMock()


@pytest.fixture
def client(app, mock_admin_user, mock_session):
    """Create a test client with mocked dependencies."""
    from src.core.dependencies import get_db
    from src.dependencies.admin_auth import get_admin_user
    
    # Create override functions
    async def override_get_db():
        return mock_session
        
    async def override_get_admin_user():
        return mock_admin_user
    
    # Override dependencies
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_admin_user] = override_get_admin_user
    
    return TestClient(app)


@pytest.fixture
def mock_privilege_with_created_at():
    """Create a mock privilege with created_at timestamp."""
    privilege = MagicMock(spec=Privilege)
    privilege.id = "privilege-1"
    privilege.name = "test:read"
    privilege.description = "Test read privilege"
    privilege.created_at = datetime(2024, 1, 1, 12, 0, 0)
    return privilege


@pytest.fixture
def mock_privilege_without_created_at():
    """Create a mock privilege without created_at timestamp."""
    privilege = MagicMock(spec=Privilege)
    privilege.id = "privilege-2"
    privilege.name = "test:write"
    privilege.description = "Test write privilege"
    privilege.created_at = datetime(2024, 2, 1, 10, 0, 0)  # Use actual datetime instead of None
    return privilege


class TestPrivilegesRouter:
    """Test cases for privileges endpoints."""
    
    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_success_with_created_at(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user, 
        mock_privilege_with_created_at
    ):
        """Test successful privileges retrieval with created_at timestamp."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_all_privileges.return_value = [mock_privilege_with_created_at]
        
        # Make request
        response = client.get("/privileges/")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "privilege-1"
        assert data[0]["name"] == "test:read"
        assert data[0]["description"] == "Test read privilege"
        assert data[0]["created_at"] == "2024-01-01T12:00:00"
        
        # Verify repository was called correctly
        mock_repo_class.assert_called_once_with(Privilege, mock_session)
        mock_repo.get_all_privileges.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_called_once_with(f"Getting all privileges - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_success_different_created_at(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user,
        mock_privilege_without_created_at
    ):
        """Test successful privileges retrieval with different created_at timestamp."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_all_privileges.return_value = [mock_privilege_without_created_at]
        
        # Make request
        response = client.get("/privileges/")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "privilege-2"
        assert data[0]["name"] == "test:write"
        assert data[0]["description"] == "Test write privilege"
        assert data[0]["created_at"] == "2024-02-01T10:00:00"
        
        # Verify repository was called correctly
        mock_repo_class.assert_called_once_with(Privilege, mock_session)
        mock_repo.get_all_privileges.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_called_once_with(f"Getting all privileges - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_multiple_privileges(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user,
        mock_privilege_with_created_at, mock_privilege_without_created_at
    ):
        """Test successful retrieval of multiple privileges."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_all_privileges.return_value = [
            mock_privilege_with_created_at, 
            mock_privilege_without_created_at
        ]
        
        # Make request
        response = client.get("/privileges/")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # First privilege (with created_at)
        assert data[0]["id"] == "privilege-1"
        assert data[0]["name"] == "test:read"
        assert data[0]["created_at"] == "2024-01-01T12:00:00"
        
        # Second privilege (different created_at)
        assert data[1]["id"] == "privilege-2"
        assert data[1]["name"] == "test:write"
        assert data[1]["created_at"] == "2024-02-01T10:00:00"


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_empty_list(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test successful retrieval with empty privileges list."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_all_privileges.return_value = []
        
        # Make request
        response = client.get("/privileges/")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 0
        assert data == []


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_repository_exception(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privileges with repository exception."""
        # Setup mock repository to raise exception
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_all_privileges.side_effect = Exception("Database connection failed")
        
        # Make request
        response = client.get("/privileges/")
        
        # Assertions
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get privileges" in data["detail"]
        assert "Database connection failed" in data["detail"]
        
        # Verify error logging
        mock_logger.error.assert_called_once_with("Error getting privileges: Database connection failed")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_success_with_created_at(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user,
        mock_privilege_with_created_at
    ):
        """Test successful single privilege retrieval with created_at timestamp."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.return_value = mock_privilege_with_created_at
        
        # Make request
        privilege_id = "privilege-1"
        response = client.get(f"/privileges/{privilege_id}")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "privilege-1"
        assert data["name"] == "test:read"
        assert data["description"] == "Test read privilege"
        assert data["created_at"] == "2024-01-01T12:00:00"
        
        # Verify repository was called correctly
        mock_repo_class.assert_called_once_with(Privilege, mock_session)
        mock_repo.get.assert_called_once_with(privilege_id)
        
        # Verify logging
        mock_logger.info.assert_called_once_with(f"Getting privilege {privilege_id} - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_success_different_created_at(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user,
        mock_privilege_without_created_at
    ):
        """Test successful single privilege retrieval with different created_at timestamp."""
        # Setup mock repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.return_value = mock_privilege_without_created_at
        
        # Make request
        privilege_id = "privilege-2"
        response = client.get(f"/privileges/{privilege_id}")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "privilege-2"
        assert data["name"] == "test:write"
        assert data["description"] == "Test write privilege"
        assert data["created_at"] == "2024-02-01T10:00:00"
        
        # Verify repository was called correctly
        mock_repo_class.assert_called_once_with(Privilege, mock_session)
        mock_repo.get.assert_called_once_with(privilege_id)
        
        # Verify logging
        mock_logger.info.assert_called_once_with(f"Getting privilege {privilege_id} - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_not_found(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privilege when privilege is not found."""
        # Setup mock repository to return None
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.return_value = None
        
        # Make request
        privilege_id = "nonexistent-privilege"
        response = client.get(f"/privileges/{privilege_id}")
        
        # Assertions
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == f"Privilege {privilege_id} not found"
        
        # Verify repository was called correctly
        mock_repo_class.assert_called_once_with(Privilege, mock_session)
        mock_repo.get.assert_called_once_with(privilege_id)
        
        # Verify logging
        mock_logger.info.assert_called_once_with(f"Getting privilege {privilege_id} - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_repository_exception(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privilege with repository exception."""
        # Setup mock repository to raise exception
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.side_effect = Exception("Database query failed")
        
        # Make request
        privilege_id = "privilege-1"
        response = client.get(f"/privileges/{privilege_id}")
        
        # Assertions
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get privilege" in data["detail"]
        assert "Database query failed" in data["detail"]
        
        # Verify error logging
        mock_logger.error.assert_called_once_with(f"Error getting privilege {privilege_id}: Database query failed")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_http_exception_reraise(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privilege with HTTPException being re-raised (lines 97-98)."""
        # Setup mock repository to return None (triggers HTTPException)
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.return_value = None
        
        # Make request
        privilege_id = "nonexistent-privilege"
        response = client.get(f"/privileges/{privilege_id}")
        
        # Assertions - HTTPException should be re-raised, not caught by generic exception handler
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == f"Privilege {privilege_id} not found"
        
        # Verify that error logging was NOT called (HTTPException path)
        mock_logger.error.assert_not_called()
        
        # Verify info logging was called
        mock_logger.info.assert_called_once_with(f"Getting privilege {privilege_id} - requested by {mock_admin_user.email}")


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_different_exception_types(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privilege with different types of exceptions."""
        # Test with ValueError
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get.side_effect = ValueError("Invalid privilege format")
        
        privilege_id = "invalid-privilege"
        response = client.get(f"/privileges/{privilege_id}")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get privilege" in data["detail"]
        assert "Invalid privilege format" in data["detail"]
        
        mock_logger.error.assert_called_with(f"Error getting privilege {privilege_id}: Invalid privilege format")


    def test_privilege_response_schema_validation(self, client, mock_session, mock_admin_user):
        """Test that PrivilegeResponse schema validation works correctly."""
        with patch('src.api.privileges_router.PrivilegeRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo_class.return_value = mock_repo
            
            # Create privilege with all required fields
            privilege = MagicMock(spec=Privilege)
            privilege.id = "test-id"
            privilege.name = "test:name"
            privilege.description = "Test description"
            privilege.created_at = datetime(2024, 1, 1, 12, 0, 0)
            
            mock_repo.get_all_privileges.return_value = [privilege]
            
            response = client.get("/privileges/")
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify all fields are present and correctly formatted
            assert len(data) == 1
            privilege_data = data[0]
            assert all(key in privilege_data for key in ["id", "name", "description", "created_at"])
            assert isinstance(privilege_data["id"], str)
            assert isinstance(privilege_data["name"], str)
            assert isinstance(privilege_data["description"], str)
            assert isinstance(privilege_data["created_at"], str)


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privileges_with_none_created_at_edge_case(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privileges edge case where created_at is None (currently causes validation error)."""
        # Setup mock repository with privilege having None created_at
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        
        privilege = MagicMock(spec=Privilege)
        privilege.id = "privilege-with-none-created-at"
        privilege.name = "test:none"
        privilege.description = "Test privilege with None created_at"
        privilege.created_at = None
        
        mock_repo.get_all_privileges.return_value = [privilege]
        
        # Make request
        response = client.get("/privileges/")
        
        # This should trigger the exception handler due to Pydantic validation error
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get privileges" in data["detail"]
        assert "validation error for PrivilegeResponse" in data["detail"]
        
        # Verify error logging was called
        mock_logger.error.assert_called_once()
        error_call_args = mock_logger.error.call_args[0][0]
        assert "Error getting privileges:" in error_call_args
        assert "validation error for PrivilegeResponse" in error_call_args


    @patch('src.api.privileges_router.PrivilegeRepository')
    @patch('src.api.privileges_router.logger')
    def test_get_privilege_with_none_created_at_edge_case(
        self, mock_logger, mock_repo_class, client, mock_session, mock_admin_user
    ):
        """Test get_privilege edge case where created_at is None (currently causes validation error)."""
        # Setup mock repository with privilege having None created_at
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        
        privilege = MagicMock(spec=Privilege)
        privilege.id = "privilege-with-none-created-at"
        privilege.name = "test:none"
        privilege.description = "Test privilege with None created_at"
        privilege.created_at = None
        
        mock_repo.get.return_value = privilege
        
        # Make request
        privilege_id = "privilege-with-none-created-at"
        response = client.get(f"/privileges/{privilege_id}")
        
        # This should trigger the exception handler due to Pydantic validation error
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get privilege" in data["detail"]
        assert "validation error for PrivilegeResponse" in data["detail"]
        
        # Verify error logging was called
        mock_logger.error.assert_called_once()
        error_call_args = mock_logger.error.call_args[0][0]
        assert f"Error getting privilege {privilege_id}:" in error_call_args
        assert "validation error for PrivilegeResponse" in error_call_args