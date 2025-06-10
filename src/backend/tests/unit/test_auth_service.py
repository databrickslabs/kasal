"""
Unit tests for AuthService.

Tests the functionality of the authentication service including
user authentication, token management, and security features.
"""
import pytest
import uuid
from datetime import datetime, UTC, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.auth_service import AuthService
from src.schemas.user import UserCreate, UserLogin
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
    mock_user.hashed_password = "$2b$12$hashedpassword"
    mock_user.is_active = True
    mock_user.is_superuser = False
    mock_user.created_at = datetime.now(UTC)
    mock_user.last_login = None
    
    # Setup repository method returns
    repo.get.return_value = mock_user
    repo.get_by_username.return_value = mock_user
    repo.get_by_email.return_value = mock_user
    repo.create.return_value = mock_user
    repo.update.return_value = mock_user
    
    return repo


@pytest.fixture
def user_create_data():
    """Create test data for user creation."""
    return UserCreate(
        username="testuser",
        email="test@example.com",
        password="SecurePassword123!",
        full_name="Test User"
    )


@pytest.fixture
def user_login_data():
    """Create test data for user login."""
    return UserLogin(
        username="testuser",
        password="SecurePassword123!"
    )


class TestAuthService:
    """Test cases for AuthService."""
    
    @pytest.mark.asyncio
    async def test_register_user_success(self, mock_uow, mock_user_repository, user_create_data):
        """Test successful user registration."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.get_password_hash") as mock_hash:
            
            mock_hash.return_value = "$2b$12$hashedpassword"
            mock_user_repository.get_by_username.return_value = None  # User doesn't exist
            mock_user_repository.get_by_email.return_value = None  # Email not taken
            
            service = AuthService(mock_uow)
            
            result = await service.register_user(user_create_data)
            
            assert result is not None
            assert result.username == "testuser"
            assert result.email == "test@example.com"
            mock_user_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_register_user_duplicate_username(self, mock_uow, mock_user_repository, user_create_data):
        """Test user registration with duplicate username."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            mock_user_repository.get_by_username.return_value = MagicMock()  # User exists
            
            service = AuthService(mock_uow)
            
            with pytest.raises(ValueError, match="Username already exists"):
                await service.register_user(user_create_data)
    
    @pytest.mark.asyncio
    async def test_register_user_duplicate_email(self, mock_uow, mock_user_repository, user_create_data):
        """Test user registration with duplicate email."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            mock_user_repository.get_by_username.return_value = None
            mock_user_repository.get_by_email.return_value = MagicMock()  # Email exists
            
            service = AuthService(mock_uow)
            
            with pytest.raises(ValueError, match="Email already registered"):
                await service.register_user(user_create_data)
    
    @pytest.mark.asyncio
    async def test_authenticate_user_success(self, mock_uow, mock_user_repository, user_login_data):
        """Test successful user authentication."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_password") as mock_verify:
            
            mock_verify.return_value = True
            
            service = AuthService(mock_uow)
            
            result = await service.authenticate_user(user_login_data.username, user_login_data.password)
            
            assert result is not None
            assert result.username == "testuser"
            mock_user_repository.get_by_username.assert_called_once_with("testuser")
    
    @pytest.mark.asyncio
    async def test_authenticate_user_invalid_username(self, mock_uow, mock_user_repository):
        """Test authentication with invalid username."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            mock_user_repository.get_by_username.return_value = None
            
            service = AuthService(mock_uow)
            
            result = await service.authenticate_user("nonexistent", "password")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_authenticate_user_invalid_password(self, mock_uow, mock_user_repository):
        """Test authentication with invalid password."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_password") as mock_verify:
            
            mock_verify.return_value = False
            
            service = AuthService(mock_uow)
            
            result = await service.authenticate_user("testuser", "wrongpassword")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_authenticate_inactive_user(self, mock_uow, mock_user_repository):
        """Test authentication with inactive user."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            inactive_user = MagicMock()
            inactive_user.is_active = False
            mock_user_repository.get_by_username.return_value = inactive_user
            
            service = AuthService(mock_uow)
            
            result = await service.authenticate_user("testuser", "password")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_create_access_token(self, mock_uow):
        """Test access token creation."""
        service = AuthService(mock_uow)
        
        user_data = {"sub": "testuser", "user_id": str(uuid.uuid4())}
        
        with patch("src.services.auth_service.create_access_token") as mock_create_token:
            mock_create_token.return_value = "mock_access_token"
            
            token = service.create_access_token(user_data)
            
            assert token == "mock_access_token"
            mock_create_token.assert_called_once_with(user_data)
    
    @pytest.mark.asyncio
    async def test_create_refresh_token(self, mock_uow):
        """Test refresh token creation."""
        service = AuthService(mock_uow)
        
        user_data = {"sub": "testuser", "user_id": str(uuid.uuid4())}
        
        with patch("src.services.auth_service.create_refresh_token") as mock_create_token:
            mock_create_token.return_value = "mock_refresh_token"
            
            token = service.create_refresh_token(user_data)
            
            assert token == "mock_refresh_token"
            mock_create_token.assert_called_once_with(user_data)
    
    @pytest.mark.asyncio
    async def test_verify_token_valid(self, mock_uow):
        """Test verification of valid token."""
        service = AuthService(mock_uow)
        
        valid_token = "valid_token"
        expected_payload = {"sub": "testuser", "user_id": str(uuid.uuid4())}
        
        with patch("src.services.auth_service.verify_token") as mock_verify:
            mock_verify.return_value = expected_payload
            
            payload = service.verify_token(valid_token)
            
            assert payload == expected_payload
            mock_verify.assert_called_once_with(valid_token)
    
    @pytest.mark.asyncio
    async def test_verify_token_invalid(self, mock_uow):
        """Test verification of invalid token."""
        service = AuthService(mock_uow)
        
        invalid_token = "invalid_token"
        
        with patch("src.services.auth_service.verify_token") as mock_verify:
            mock_verify.return_value = None
            
            payload = service.verify_token(invalid_token)
            
            assert payload is None
    
    @pytest.mark.asyncio
    async def test_refresh_access_token(self, mock_uow, mock_user_repository):
        """Test refreshing access token with valid refresh token."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_token") as mock_verify, \
             patch("src.services.auth_service.create_access_token") as mock_create:
            
            user_id = str(uuid.uuid4())
            mock_verify.return_value = {"sub": "testuser", "user_id": user_id}
            mock_create.return_value = "new_access_token"
            
            service = AuthService(mock_uow)
            
            new_token = await service.refresh_access_token("valid_refresh_token")
            
            assert new_token == "new_access_token"
            mock_user_repository.get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_last_login(self, mock_uow, mock_user_repository):
        """Test updating user's last login timestamp."""
        user_id = uuid.uuid4()
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            service = AuthService(mock_uow)
            
            await service.update_last_login(user_id)
            
            mock_user_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
            
            # Verify last_login was updated
            update_args = mock_user_repository.update.call_args[0]
            assert "last_login" in update_args[1]
    
    @pytest.mark.asyncio
    async def test_change_password_success(self, mock_uow, mock_user_repository):
        """Test successful password change."""
        user_id = uuid.uuid4()
        old_password = "OldPassword123!"
        new_password = "NewPassword123!"
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_password") as mock_verify, \
             patch("src.services.auth_service.get_password_hash") as mock_hash:
            
            mock_verify.return_value = True
            mock_hash.return_value = "$2b$12$newhash"
            
            service = AuthService(mock_uow)
            
            result = await service.change_password(user_id, old_password, new_password)
            
            assert result is True
            mock_user_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_change_password_invalid_old_password(self, mock_uow, mock_user_repository):
        """Test password change with invalid old password."""
        user_id = uuid.uuid4()
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_password") as mock_verify:
            
            mock_verify.return_value = False
            
            service = AuthService(mock_uow)
            
            result = await service.change_password(user_id, "wrong_password", "new_password")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_reset_password_request(self, mock_uow, mock_user_repository):
        """Test password reset request."""
        email = "test@example.com"
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.generate_reset_token") as mock_generate:
            
            mock_generate.return_value = "reset_token_123"
            
            service = AuthService(mock_uow)
            
            token = await service.request_password_reset(email)
            
            assert token == "reset_token_123"
            mock_user_repository.get_by_email.assert_called_once_with(email)
    
    @pytest.mark.asyncio
    async def test_reset_password_with_token(self, mock_uow, mock_user_repository):
        """Test password reset with valid token."""
        reset_token = "valid_reset_token"
        new_password = "NewPassword123!"
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository), \
             patch("src.services.auth_service.verify_reset_token") as mock_verify, \
             patch("src.services.auth_service.get_password_hash") as mock_hash:
            
            user_id = str(uuid.uuid4())
            mock_verify.return_value = {"user_id": user_id}
            mock_hash.return_value = "$2b$12$newhash"
            
            service = AuthService(mock_uow)
            
            result = await service.reset_password(reset_token, new_password)
            
            assert result is True
            mock_user_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_password_strength(self, mock_uow):
        """Test password strength validation."""
        service = AuthService(mock_uow)
        
        # Test strong password
        strong_password = "StrongPassword123!"
        assert service.validate_password_strength(strong_password) is True
        
        # Test weak passwords
        weak_passwords = [
            "weak",  # Too short
            "password",  # No uppercase/numbers
            "PASSWORD",  # No lowercase/numbers
            "Password",  # No numbers/special chars
            "Password123"  # No special chars
        ]
        
        for weak_password in weak_passwords:
            assert service.validate_password_strength(weak_password) is False
    
    @pytest.mark.asyncio
    async def test_check_user_permissions(self, mock_uow, mock_user_repository):
        """Test user permission checking."""
        user_id = uuid.uuid4()
        required_permission = "admin:read"
        
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            # Mock user with permissions
            mock_user = MagicMock()
            mock_user.permissions = ["admin:read", "admin:write"]
            mock_user_repository.get.return_value = mock_user
            
            service = AuthService(mock_uow)
            
            # Test user has permission
            has_permission = await service.check_user_permission(user_id, required_permission)
            assert has_permission is True
            
            # Test user doesn't have permission
            has_permission = await service.check_user_permission(user_id, "admin:delete")
            assert has_permission is False
    
    @pytest.mark.asyncio
    async def test_user_session_management(self, mock_uow):
        """Test user session management."""
        service = AuthService(mock_uow)
        
        user_id = str(uuid.uuid4())
        session_id = "session_123"
        
        # Create session
        session = service.create_user_session(user_id, session_id)
        assert session is not None
        assert session["user_id"] == user_id
        assert session["session_id"] == session_id
        
        # Validate session
        is_valid = service.validate_session(session_id)
        assert is_valid is True
        
        # Invalidate session
        service.invalidate_session(session_id)
        is_valid = service.validate_session(session_id)
        assert is_valid is False
    
    @pytest.mark.asyncio
    async def test_account_lockout_mechanism(self, mock_uow, mock_user_repository):
        """Test account lockout after failed attempts."""
        with patch("src.services.auth_service.UserRepository", return_value=mock_user_repository):
            service = AuthService(mock_uow)
            
            username = "testuser"
            
            # Simulate multiple failed attempts
            for _ in range(5):
                service.record_failed_attempt(username)
            
            # Check if account is locked
            is_locked = service.is_account_locked(username)
            assert is_locked is True
            
            # Try to authenticate locked account
            result = await service.authenticate_user(username, "correct_password")
            assert result is None  # Should fail due to lockout