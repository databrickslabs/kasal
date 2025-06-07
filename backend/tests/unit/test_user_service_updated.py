"""
Unit tests for UserService (updated version).

Tests the functionality of the updated user service including
RBAC features, group management, and enhanced user operations.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.user_service import UserService
from src.schemas.user import UserCreate, UserUpdate
from src.models.user import User
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
def mock_user_repository():
    """Create a mock user repository."""
    repo = AsyncMock()
    
    # Create mock user objects
    mock_user = MagicMock(spec=User)
    mock_user.id = uuid.uuid4()
    mock_user.username = "testuser"
    mock_user.email = "test@example.com"
    mock_user.full_name = "Test User"
    mock_user.is_active = True
    mock_user.is_superuser = False
    mock_user.created_at = datetime.now(UTC)
    mock_user.roles = ["user"]
    mock_user.groups = []
    mock_user.tenant_id = uuid.uuid4()
    
    # Setup repository method returns
    repo.get.return_value = mock_user
    repo.list.return_value = [mock_user]
    repo.create.return_value = mock_user
    repo.update.return_value = mock_user
    repo.delete.return_value = True
    repo.get_by_username.return_value = mock_user
    repo.get_by_email.return_value = mock_user
    
    return repo


@pytest.fixture
def user_create_data():
    """Create test data for user creation."""
    return UserCreate(
        username="newuser",
        email="newuser@example.com",
        password="SecurePassword123!",
        full_name="New User",
        tenant_id=uuid.uuid4()
    )


@pytest.fixture
def user_update_data():
    """Create test data for user updates."""
    return UserUpdate(
        full_name="Updated User",
        email="updated@example.com",
        is_active=False
    )


class TestUserService:
    """Test cases for UserService."""
    
    @pytest.mark.asyncio
    async def test_create_user_success(self, mock_uow, mock_user_repository, user_create_data):
        """Test successful user creation with RBAC."""
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.user_service.get_password_hash") as mock_hash:
            
            mock_hash.return_value = "$2b$12$hashedpassword"
            mock_user_repository.username_exists.return_value = False
            mock_user_repository.email_exists.return_value = False
            
            service = UserService(mock_uow)
            
            result = await service.create(user_create_data)
            
            assert result is not None
            assert result.username == "testuser"
            mock_user_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_user_duplicate_username(self, mock_uow, mock_user_repository, user_create_data):
        """Test user creation with duplicate username."""
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            mock_user_repository.username_exists.return_value = True
            
            service = UserService(mock_uow)
            
            with pytest.raises(ValueError, match="Username already exists"):
                await service.create(user_create_data)
    
    @pytest.mark.asyncio
    async def test_assign_roles_to_user(self, mock_uow, mock_user_repository):
        """Test assigning roles to user."""
        user_id = uuid.uuid4()
        roles = ["admin", "editor"]
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.assign_roles(user_id, roles)
            
            assert result is True
            mock_user_repository.assign_role.assert_called()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_roles_from_user(self, mock_uow, mock_user_repository):
        """Test removing roles from user."""
        user_id = uuid.uuid4()
        roles = ["editor"]
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.remove_roles(user_id, roles)
            
            assert result is True
            mock_user_repository.remove_role.assert_called()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_user_to_groups(self, mock_uow, mock_user_repository):
        """Test adding user to groups."""
        user_id = uuid.uuid4()
        group_ids = [uuid.uuid4(), uuid.uuid4()]
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.add_to_groups(user_id, group_ids)
            
            assert result is True
            mock_user_repository.add_to_group.assert_called()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_user_from_groups(self, mock_uow, mock_user_repository):
        """Test removing user from groups."""
        user_id = uuid.uuid4()
        group_ids = [uuid.uuid4()]
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.remove_from_groups(user_id, group_ids)
            
            assert result is True
            mock_user_repository.remove_from_group.assert_called()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_user_permission(self, mock_uow, mock_user_repository):
        """Test checking if user has specific permission."""
        user_id = uuid.uuid4()
        permission = "read_data"
        
        mock_user_repository.has_permission.return_value = True
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.has_permission(user_id, permission)
            
            assert result is True
            mock_user_repository.has_permission.assert_called_once_with(user_id, permission)
    
    @pytest.mark.asyncio
    async def test_get_user_permissions(self, mock_uow, mock_user_repository):
        """Test getting all user permissions."""
        user_id = uuid.uuid4()
        mock_permissions = ["read_data", "write_data", "admin"]
        mock_user_repository.get_permissions.return_value = mock_permissions
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.get_permissions(user_id)
            
            assert len(result) == 3
            assert "admin" in result
            mock_user_repository.get_permissions.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_get_user_effective_roles(self, mock_uow, mock_user_repository):
        """Test getting user's effective roles (direct + inherited)."""
        user_id = uuid.uuid4()
        mock_roles = [
            {"name": "admin", "source": "direct"},
            {"name": "editor", "source": "group:developers"}
        ]
        mock_user_repository.get_effective_roles.return_value = mock_roles
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.get_effective_roles(user_id)
            
            assert len(result) == 2
            assert result[0]["source"] == "direct"
            mock_user_repository.get_effective_roles.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_change_password_success(self, mock_uow, mock_user_repository):
        """Test successful password change."""
        user_id = uuid.uuid4()
        old_password = "OldPassword123!"
        new_password = "NewPassword123!"
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.user_service.verify_password") as mock_verify, \
             patch("src.services.user_service.get_password_hash") as mock_hash:
            
            mock_verify.return_value = True
            mock_hash.return_value = "$2b$12$newhash"
            
            service = UserService(mock_uow)
            
            result = await service.change_password(user_id, old_password, new_password)
            
            assert result is True
            mock_user_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_deactivate_user(self, mock_uow, mock_user_repository):
        """Test user deactivation."""
        user_id = uuid.uuid4()
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.deactivate(user_id)
            
            assert result is True
            mock_user_repository.deactivate.assert_called_once_with(user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_reactivate_user(self, mock_uow, mock_user_repository):
        """Test user reactivation."""
        user_id = uuid.uuid4()
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.reactivate(user_id)
            
            assert result is True
            mock_user_repository.activate.assert_called_once_with(user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_operations(self, mock_uow, mock_user_repository):
        """Test bulk user operations."""
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        role_ids = [uuid.uuid4(), uuid.uuid4()]
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            # Bulk assign roles
            result = await service.bulk_assign_roles(user_ids, role_ids)
            assert result == 4  # 2 users * 2 roles
            
            # Bulk remove roles
            result = await service.bulk_remove_roles(user_ids, role_ids)
            assert result == 4
            
            mock_uow.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_user_session_management(self, mock_uow, mock_user_repository):
        """Test user session management."""
        user_id = uuid.uuid4()
        session_id = "session_123"
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            # Create session
            result = await service.create_session(user_id, session_id)
            assert result is True
            
            # Get active sessions
            mock_sessions = [{"id": session_id, "created_at": datetime.now(UTC)}]
            mock_user_repository.get_active_sessions.return_value = mock_sessions
            
            sessions = await service.get_active_sessions(user_id)
            assert len(sessions) == 1
            
            # Invalidate session
            result = await service.invalidate_session(session_id)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_password_reset_flow(self, mock_uow, mock_user_repository):
        """Test password reset flow."""
        email = "test@example.com"
        reset_token = "reset_token_123"
        new_password = "NewPassword123!"
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.user_service.generate_reset_token") as mock_generate, \
             patch("src.services.user_service.get_password_hash") as mock_hash:
            
            mock_generate.return_value = reset_token
            mock_hash.return_value = "$2b$12$newhash"
            mock_user_repository.verify_password_reset_token.return_value = uuid.uuid4()
            
            service = UserService(mock_uow)
            
            # Request password reset
            result = await service.request_password_reset(email)
            assert result == reset_token
            
            # Reset password with token
            result = await service.reset_password_with_token(reset_token, new_password)
            assert result is True
            
            mock_uow.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_user_preferences_management(self, mock_uow, mock_user_repository):
        """Test user preferences management."""
        user_id = uuid.uuid4()
        preferences = {"theme": "dark", "language": "en", "notifications": True}
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            result = await service.update_preferences(user_id, preferences)
            
            assert result is True
            mock_user_repository.update_preferences.assert_called_once_with(user_id, preferences)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_user_audit_logging(self, mock_uow, mock_user_repository, user_create_data):
        """Test that user operations are audited."""
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.user_service.audit_logger") as mock_audit, \
             patch("src.services.user_service.get_password_hash"):
            
            mock_user_repository.username_exists.return_value = False
            mock_user_repository.email_exists.return_value = False
            
            service = UserService(mock_uow)
            
            await service.create(user_create_data)
            
            mock_audit.log_user_creation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_user_validation_rules(self, mock_uow):
        """Test user validation rules."""
        service = UserService(mock_uow)
        
        # Test valid username
        service._validate_username("valid_user123")  # Should not raise
        
        # Test invalid usernames
        invalid_usernames = ["", "a", "user with spaces", "user@invalid"]
        for username in invalid_usernames:
            with pytest.raises(ValueError):
                service._validate_username(username)
        
        # Test valid email
        service._validate_email("user@example.com")  # Should not raise
        
        # Test invalid emails
        invalid_emails = ["", "not-an-email", "user@", "@domain.com"]
        for email in invalid_emails:
            with pytest.raises(ValueError):
                service._validate_email(email)
    
    @pytest.mark.asyncio
    async def test_user_authorization_checks(self, mock_uow, mock_user_repository):
        """Test user authorization checks."""
        user_id = uuid.uuid4()
        other_user_id = uuid.uuid4()
        
        # Mock current user
        current_user = MagicMock()
        current_user.id = user_id
        current_user.is_superuser = False
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            # User can access their own data
            result = service._can_access_user(current_user, user_id)
            assert result is True
            
            # User cannot access other user's data
            result = service._can_access_user(current_user, other_user_id)
            assert result is False
            
            # Superuser can access any user's data
            current_user.is_superuser = True
            result = service._can_access_user(current_user, other_user_id)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_user_metrics_collection(self, mock_uow, mock_user_repository):
        """Test user metrics collection."""
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            metrics = await service.get_user_metrics()
            
            assert "total_users" in metrics
            assert "active_users" in metrics
            assert "users_by_role" in metrics
            assert "recent_registrations" in metrics
            mock_user_repository.get_metrics.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_user_data_export(self, mock_uow, mock_user_repository):
        """Test user data export functionality."""
        user_id = uuid.uuid4()
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            export_data = await service.export_user_data(user_id)
            
            assert "profile" in export_data
            assert "roles" in export_data
            assert "groups" in export_data
            assert "permissions" in export_data
            mock_user_repository.get_user_export_data.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_user_compliance_operations(self, mock_uow, mock_user_repository):
        """Test GDPR/compliance operations."""
        user_id = uuid.uuid4()
        
        with patch("src.services.user_service.UserRepository", return_value=mock_user_repository):
            service = UserService(mock_uow)
            
            # Anonymize user data
            result = await service.anonymize_user(user_id)
            assert result is True
            
            # Purge user data
            result = await service.purge_user_data(user_id)
            assert result is True
            
            mock_user_repository.anonymize_user.assert_called_once_with(user_id)
            mock_user_repository.purge_user_data.assert_called_once_with(user_id)
            mock_uow.commit.assert_called()