"""
Unit tests for UsersRouter.

Tests the functionality of user management endpoints including
current user operations, admin operations, and external identities.
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.enums import UserRole, UserStatus


# Mock user model
class MockUser:
    def __init__(
        self,
        id="user-123",
        username="testuser",
        email="test@example.com",
        role=UserRole.REGULAR,
        status=UserStatus.ACTIVE,
    ):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.is_system_admin = False
        self.is_personal_workspace_manager = False
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


# Mock external identity
class MockExternalIdentity:
    def __init__(self, id="ext-123", user_id="user-123", provider="github"):
        from datetime import datetime

        self.id = id
        self.user_id = user_id
        self.provider = provider
        self.provider_user_id = "github-123"
        self.email = "test@github.com"
        self.profile_data = None
        self.created_at = datetime.now()
        self.last_login = None


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    return MockUser(id="current-user-123", username="currentuser")


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    return MockUser(id="admin-123", username="admin", role=UserRole.ADMIN)


@pytest.fixture
def mock_user_service():
    """Create a mock user service."""
    service = AsyncMock()
    return service


@pytest.fixture
def client(mock_current_user, mock_session):
    """Create a test client with dependency overrides."""
    from fastapi import FastAPI

    from src.api.users_router import router
    from src.db.session import get_db
    from src.dependencies.admin_auth import (
        get_admin_user,
        get_authenticated_user,
        get_system_admin_user,
        require_authenticated_user,
    )
    from tests.unit.api.conftest import register_exception_handlers

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    # Override the actual dependency functions for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user
    app.dependency_overrides[get_system_admin_user] = lambda: mock_current_user
    app.dependency_overrides[get_db] = lambda: mock_session

    return TestClient(app)


class TestCurrentUserEndpoints:
    """Test cases for current user endpoints."""

    def test_read_users_me(self, client, mock_current_user, mock_user_service):
        """Test getting current user information."""
        from datetime import datetime

        from src.models.enums import UserRole, UserStatus

        user_with_profile = {
            "id": "current-user-123",
            "username": "currentuser",
            "email": "current@example.com",
            "role": UserRole.REGULAR,
            "status": UserStatus.ACTIVE,
            "is_system_admin": False,
            "is_personal_workspace_manager": False,
            "display_name": "Current User",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "last_login": None,
        }

        mock_user_service.get_user_complete.return_value = user_with_profile

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            response = client.get("/users/me")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "current-user-123"
        assert data["username"] == "currentuser"

    def test_update_users_me(self, client, mock_current_user, mock_user_service):
        """Test updating current user information."""
        from datetime import datetime

        from src.models.enums import UserRole, UserStatus

        update_data = {"username": "renamed"}
        updated_user = {
            "id": "current-user-123",
            "username": "renamed",
            "email": "current@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {"id": "profile-123", "user_id": "current-user-123"},
        }

        mock_user_service.update_user.return_value = updated_user

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.AuthenticatedUserDep",
                    return_value=mock_current_user,
                ):
                    response = client.put("/users/me", json=update_data)

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "renamed"

    def skip_test_update_users_profile(
        self, client, mock_current_user, mock_user_service
    ):
        """Test updating current user profile."""
        from datetime import datetime

        from src.models.enums import UserRole, UserStatus

        profile_data = {
            "display_name": "Updated Name",
            "avatar_url": "http://example.com/avatar.jpg",
        }
        updated_user = {
            "id": "current-user-123",
            "username": "currentuser",
            "email": "test@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {
                "id": "profile-123",
                "user_id": "current-user-123",
                "display_name": "Updated Name",
                "avatar_url": "http://example.com/avatar.jpg",
                "preferences": None,
            },
        }

        mock_user_service.update_user_profile.return_value = updated_user

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.AuthenticatedUserDep",
                    return_value=mock_current_user,
                ):
                    response = client.put("/users/me/profile", json=profile_data)

        assert response.status_code == 200
        data = response.json()
        assert data["profile"]["display_name"] == "Updated Name"

    def skip_test_read_users_external_identities(
        self, client, mock_current_user, mock_user_service
    ):
        """Test getting current user's external identities."""
        mock_identities = [MockExternalIdentity()]
        mock_user_service.get_user_external_identities.return_value = mock_identities

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.AuthenticatedUserDep",
                    return_value=mock_current_user,
                ):
                    response = client.get("/users/me/external-identities")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["provider"] == "github"

    def skip_test_delete_external_identity_success(
        self, client, mock_current_user, mock_user_service
    ):
        """Test successfully deleting an external identity."""
        mock_user_service.remove_external_identity.return_value = True

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.AuthenticatedUserDep",
                    return_value=mock_current_user,
                ):
                    response = client.delete("/users/me/external-identities/github")

        assert response.status_code == 204

    def skip_test_delete_external_identity_not_found(
        self, client, mock_current_user, mock_user_service
    ):
        """Test deleting non-existent external identity."""
        mock_user_service.remove_external_identity.return_value = False

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.AuthenticatedUserDep",
                    return_value=mock_current_user,
                ):
                    response = client.delete(
                        "/users/me/external-identities/nonexistent"
                    )

        assert response.status_code == 404
        assert "No external identity found" in response.json()["detail"]


class TestAdminEndpoints:
    """Test cases for admin endpoints."""

    def test_read_users_no_filters(self, client, mock_admin_user, mock_user_service):
        """Test getting users list without filters."""
        mock_users = [MockUser(id="user-1"), MockUser(id="user-2")]
        mock_user_service.get_users.return_value = mock_users

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.get("/users/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

        # Verify get_users was called with correct params
        mock_user_service.get_users.assert_called_once_with(
            skip=0, limit=100, filters={}, search=None
        )

    def test_read_users_with_filters(self, client, mock_admin_user, mock_user_service):
        """Test getting users list with filters."""
        mock_users = [MockUser(role=UserRole.ADMIN)]
        mock_user_service.get_users.return_value = mock_users

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.get(
                        "/users/?role=admin&status=active&search=test"
                    )

        assert response.status_code == 200

        # Verify filters were passed correctly
        mock_user_service.get_users.assert_called_once_with(
            skip=0,
            limit=100,
            filters={"role": "admin", "status": "active"},
            search="test",
        )

    def test_read_user_success(self, client, mock_admin_user, mock_user_service):
        """Test getting a specific user by ID."""
        from datetime import datetime

        from src.models.enums import UserRole, UserStatus

        user_complete = {
            "id": "user-123",
            "username": "testuser",
            "email": "test@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {"id": "profile-123", "user_id": "user-123"},
            "external_identities": [],
        }

        mock_user_service.get_user_complete.return_value = user_complete

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.get("/users/user-123")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "user-123"

    def test_read_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test getting non-existent user."""
        mock_user_service.get_user_complete.return_value = None

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.get("/users/nonexistent")

        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    def test_update_user_success(self, client, mock_admin_user, mock_user_service):
        """Test updating a user."""
        update_data = {"username": "renamed"}
        updated_user = MockUser(username="renamed")

        mock_user_service.update_user.return_value = updated_user

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.put("/users/user-123", json=update_data)

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "renamed"

    def test_update_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test updating non-existent user."""
        update_data = {"username": "renamed"}
        mock_user_service.update_user.return_value = None

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.put("/users/nonexistent", json=update_data)

        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    def skip_test_assign_user_role_success(
        self, client, mock_admin_user, mock_user_service
    ):
        """Test assigning a role to a user."""
        role_data = {"role_id": "admin"}
        updated_user = MockUser(role=UserRole.ADMIN)

        mock_user_service.assign_role.return_value = updated_user

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.put("/users/user-123/role", json=role_data)

        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "admin"

    def skip_test_assign_user_role_not_found(
        self, client, mock_admin_user, mock_user_service
    ):
        """Test assigning role to non-existent user."""
        role_data = {"role_id": "admin"}
        mock_user_service.assign_role.return_value = None

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.put("/users/nonexistent/role", json=role_data)

        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    def test_delete_user_success(self, client, mock_admin_user, mock_user_service):
        """Test deleting a user."""
        mock_user_service.delete_user.return_value = True

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.delete("/users/user-123")

        assert response.status_code == 204

    def test_delete_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test deleting non-existent user."""
        mock_user_service.delete_user.return_value = False

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.delete("/users/nonexistent")

        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    def test_read_users_with_pagination(
        self, client, mock_admin_user, mock_user_service
    ):
        """Test getting users with pagination parameters."""
        mock_users = [MockUser(id=f"user-{i}") for i in range(5)]
        mock_user_service.get_users.return_value = mock_users

        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            with patch("src.api.users_router.SessionDep", return_value=AsyncMock()):
                with patch(
                    "src.api.users_router.SystemAdminUserDep",
                    return_value=mock_admin_user,
                ):
                    response = client.get("/users/?skip=10&limit=5")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5

        # Verify pagination params were passed
        mock_user_service.get_users.assert_called_once_with(
            skip=10, limit=5, filters={}, search=None
        )


class TestGlobalUserRoutesAreSystemAdminOnly:
    """Audit F01. The global user routes used to accept an admin of ANY
    workspace, and a user's email is what authentication resolves by — a
    workspace admin could point a system admin's row at an address they
    control and sign in as them. The real gate runs here: only the identity
    lookup is replaced."""

    @pytest.fixture
    def gated_client(self, mock_session):
        from fastapi import FastAPI

        from src.api.users_router import router
        from src.db.session import get_db
        from tests.unit.api.conftest import register_exception_handlers

        app = FastAPI()
        app.include_router(router)
        register_exception_handlers(app)
        app.dependency_overrides[get_db] = lambda: mock_session
        return TestClient(app)

    def _as(self, user):
        return patch(
            "src.dependencies.admin_auth.require_authenticated_user",
            new=AsyncMock(return_value=user),
        )

    @pytest.mark.parametrize(
        "method,path,body",
        [
            ("get", "/users/", None),
            ("get", "/users/user-9", None),
            ("put", "/users/user-9", {"username": "renamed"}),
            ("put", "/users/user-9/permissions", {"is_system_admin": True}),
            ("delete", "/users/user-9", None),
        ],
    )
    def test_a_workspace_admin_is_refused(self, gated_client, method, path, body):
        workspace_admin = MockUser(id="ws-admin")  # is_system_admin is False
        with self._as(workspace_admin), patch("src.api.users_router.UserService"):
            response = gated_client.request(method.upper(), path, json=body)
        assert response.status_code == 403

    def test_a_system_admin_is_not(self, gated_client, mock_user_service):
        sysadmin = MockUser(id="sys-admin")
        sysadmin.is_system_admin = True
        mock_user_service.update_user.return_value = MockUser(id="user-9")
        with (
            self._as(sysadmin),
            patch("src.api.users_router.UserService", return_value=mock_user_service),
        ):
            response = gated_client.put("/users/user-9", json={"username": "renamed"})
        assert response.status_code == 200

    def test_no_route_rebinds_an_email(self, client, mock_user_service):
        # `client` resolves the caller as a system admin for the global route
        # and as the current user for /me; the schema refuses either way.
        with patch("src.api.users_router.UserService", return_value=mock_user_service):
            admin_edit = client.put(
                "/users/user-9", json={"email": "attacker@example.com"}
            )
            self_edit = client.put("/users/me", json={"email": "me@example.com"})
        assert admin_edit.status_code == 422
        assert self_edit.status_code == 422
        mock_user_service.update_user.assert_not_called()
