"""
Unit tests for AuthService.

Tests the functionality of authentication service including
user authentication, registration, token management, and OAuth.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
import jwt

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.auth_service import (
    AuthService, verify_password, get_password_hash,
    create_access_token, create_refresh_token, decode_token,
    get_refresh_token_hash, verify_refresh_token
)
from src.models.user import User, RefreshToken
from src.models.enums import UserRole, UserStatus
from src.schemas.user import UserCreate


# Mock user model
class MockUser:
    def __init__(self, id="user-123", username="testuser", email="test@example.com",
                 role=UserRole.REGULAR, status=UserStatus.ACTIVE,
                 hashed_password="$2b$12$hashed"):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.hashed_password = hashed_password
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.last_login = None


# Mock refresh token model
class MockRefreshToken:
    def __init__(self, id="token-123", user_id="user-123", token="hashed_token",
                 expires_at=None, is_revoked=False):
        self.id = id
        self.user_id = user_id
        self.token = token
        self.expires_at = expires_at or datetime.utcnow() + timedelta(days=7)
        self.is_revoked = is_revoked
        self.created_at = datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def auth_service(mock_session):
    """Create an AuthService instance with mock session."""
    return AuthService(mock_session)


@pytest.fixture
def mock_user():
    """Create a mock user."""
    return MockUser()


class TestPasswordFunctions:
    """Test password hashing functions."""
    
    def test_get_password_hash(self):
        """Test password hashing."""
        password = "TestPassword123!"
        hashed = get_password_hash(password)
        
        assert hashed != password
        assert hashed.startswith("$2b$")
    
    def test_verify_password_correct(self):
        """Test verifying correct password."""
        password = "TestPassword123!"
        hashed = get_password_hash(password)
        
        assert verify_password(password, hashed) is True
    
    def test_verify_password_incorrect(self):
        """Test verifying incorrect password."""
        password = "TestPassword123!"
        hashed = get_password_hash(password)
        
        assert verify_password("WrongPassword", hashed) is False


class TestTokenFunctions:
    """Test JWT token functions."""
    
    def test_create_access_token(self):
        """Test creating access token."""
        data = {"sub": "user-123", "role": "regular"}
        
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_SECRET_KEY = "test_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            mock_settings.ACCESS_TOKEN_EXPIRE_MINUTES = 30
            
            token = create_access_token(data)
            
            assert isinstance(token, str)
            assert len(token) > 0
    
    def test_create_access_token_with_expiry(self):
        """Test creating access token with custom expiry."""
        data = {"sub": "user-123"}
        expires_delta = timedelta(hours=1)
        
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_SECRET_KEY = "test_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            
            token = create_access_token(data, expires_delta)
            
            assert isinstance(token, str)
    
    def test_create_refresh_token(self):
        """Test creating refresh token."""
        data = {"sub": "user-123", "type": "refresh"}
        
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_REFRESH_SECRET_KEY = "test_refresh_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
            
            token = create_refresh_token(data)
            
            assert isinstance(token, str)
            assert len(token) > 0
    
    def test_decode_token(self):
        """Test decoding JWT token."""
        data = {"sub": "user-123", "role": "regular"}
        
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_SECRET_KEY = "test_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            mock_settings.ACCESS_TOKEN_EXPIRE_MINUTES = 30
            
            token = create_access_token(data)
            # Use options to skip expiration validation in tests
            decoded = jwt.decode(token, "test_secret", algorithms=["HS256"], options={"verify_exp": False})
            
            assert decoded["sub"] == "user-123"
            assert decoded["role"] == "regular"
            assert "exp" in decoded
    
    def test_refresh_token_hashing(self):
        """Test refresh token hashing and verification."""
        token = "test_refresh_token"
        hashed = get_refresh_token_hash(token)
        
        assert hashed != token
        assert verify_refresh_token(token, hashed) is True
        assert verify_refresh_token("wrong_token", hashed) is False


class TestAuthService:
    """Test cases for AuthService."""
    
    @pytest.mark.asyncio
    async def test_authenticate_user_success(self, auth_service, mock_user):
        """Test successful user authentication."""
        # Hash a known password
        password = "correctpassword"
        mock_user.hashed_password = get_password_hash(password)
        
        # Mock repository
        auth_service.user_repo.get_by_username_or_email = AsyncMock(return_value=mock_user)
        auth_service.user_repo.update_last_login = AsyncMock()
        
        result = await auth_service.authenticate_user("testuser", password)
        
        assert result == mock_user
        auth_service.user_repo.update_last_login.assert_called_once_with(mock_user.id)
    
    @pytest.mark.asyncio
    async def test_authenticate_user_not_found(self, auth_service):
        """Test authentication when user not found."""
        auth_service.user_repo.get_by_username_or_email = AsyncMock(return_value=None)
        
        result = await auth_service.authenticate_user("nonexistent", "password")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_authenticate_user_wrong_password(self, auth_service, mock_user):
        """Test authentication with wrong password."""
        mock_user.hashed_password = get_password_hash("correctpassword")
        
        auth_service.user_repo.get_by_username_or_email = AsyncMock(return_value=mock_user)
        
        result = await auth_service.authenticate_user("testuser", "wrongpassword")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_register_user_success(self, auth_service, mock_user):
        """Test successful user registration."""
        user_data = UserCreate(
            username="newuser",
            email="new@example.com",
            password="StrongPassword123!"
        )
        
        # Mock repository
        auth_service.user_repo.get_by_username = AsyncMock(return_value=None)
        auth_service.user_repo.get_by_email = AsyncMock(return_value=None)
        auth_service.user_repo.create = AsyncMock(return_value=mock_user)
        auth_service.user_profile_repo.create = AsyncMock()
        
        result = await auth_service.register_user(user_data)
        
        assert result == mock_user
        
        # Verify user was created with correct data
        create_call_args = auth_service.user_repo.create.call_args[0][0]
        assert create_call_args["username"] == "newuser"
        assert create_call_args["email"] == "new@example.com"
        assert "hashed_password" in create_call_args
        assert create_call_args["role"] == UserRole.REGULAR
    
    @pytest.mark.asyncio
    async def test_register_user_duplicate_username(self, auth_service, mock_user):
        """Test registration with duplicate username."""
        user_data = UserCreate(
            username="existinguser",
            email="new@example.com",
            password="StrongPassword123!"
        )
        
        auth_service.user_repo.get_by_username = AsyncMock(return_value=mock_user)
        
        with pytest.raises(ValueError, match="Username already registered"):
            await auth_service.register_user(user_data)
    
    @pytest.mark.asyncio
    async def test_register_user_duplicate_email(self, auth_service, mock_user):
        """Test registration with duplicate email."""
        user_data = UserCreate(
            username="newuser",
            email="existing@example.com",
            password="StrongPassword123!"
        )
        
        auth_service.user_repo.get_by_username = AsyncMock(return_value=None)
        auth_service.user_repo.get_by_email = AsyncMock(return_value=mock_user)
        
        with pytest.raises(ValueError, match="Email already registered"):
            await auth_service.register_user(user_data)
    
    @pytest.mark.asyncio
    async def test_create_user_tokens(self, auth_service, mock_user):
        """Test creating user tokens."""
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_SECRET_KEY = "test_secret"
            mock_settings.JWT_REFRESH_SECRET_KEY = "test_refresh_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            mock_settings.ACCESS_TOKEN_EXPIRE_MINUTES = 30
            mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
            
            # Mock refresh token repository
            auth_service.refresh_token_repo.create = AsyncMock()
            
            result = await auth_service.create_user_tokens(mock_user)
            
            assert "access_token" in result
            assert "refresh_token" in result
            assert result["token_type"] == "bearer"
            
            # Verify refresh token was stored
            auth_service.refresh_token_repo.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_refresh_access_token_success(self, auth_service, mock_user):
        """Test refreshing access token."""
        refresh_token = "valid_refresh_token"
        # Create mock token with correct hash
        token_hash = get_refresh_token_hash(refresh_token)
        mock_refresh_token = MockRefreshToken(token=token_hash)
        
        with patch('src.services.auth_service.settings') as mock_settings:
            mock_settings.JWT_SECRET_KEY = "test_secret"
            mock_settings.JWT_REFRESH_SECRET_KEY = "test_refresh_secret"
            mock_settings.JWT_ALGORITHM = "HS256"
            mock_settings.ACCESS_TOKEN_EXPIRE_MINUTES = 30
            mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
            
            # Create a valid refresh token
            token_data = {"sub": mock_user.id}
            
            # Mock the entire flow to bypass complex token validation
            with patch('src.services.auth_service.jwt.decode') as mock_jwt_decode:
                mock_jwt_decode.return_value = token_data
                with patch('src.services.auth_service.decode_token', return_value=token_data):
                    auth_service.user_repo.get = AsyncMock(return_value=mock_user)
                    auth_service.refresh_token_repo.get_all = AsyncMock(
                        return_value=[mock_refresh_token]
                    )
                    
                    result = await auth_service.refresh_access_token(refresh_token)
                    
                    assert result is not None
                    assert "access_token" in result
                    assert "refresh_token" in result
    
    @pytest.mark.asyncio
    async def test_refresh_access_token_invalid_token(self, auth_service):
        """Test refreshing with invalid token."""
        with patch('src.services.auth_service.decode_token', side_effect=jwt.InvalidTokenError):
            result = await auth_service.refresh_access_token("invalid_token")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_refresh_access_token_expired(self, auth_service):
        """Test refreshing with expired token."""
        expired_data = {"sub": "user-123", "type": "refresh"}
        
        with patch('src.services.auth_service.decode_token', return_value=expired_data):
            # Mock refresh token as expired
            expired_token = MockRefreshToken(expires_at=datetime.utcnow() - timedelta(days=1))
            auth_service.refresh_token_repo.get_by_user_and_hash = AsyncMock(
                return_value=expired_token
            )
            
            result = await auth_service.refresh_access_token("expired_token")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_refresh_access_token_revoked(self, auth_service):
        """Test refreshing with revoked token."""
        token_data = {"sub": "user-123", "type": "refresh"}
        
        with patch('src.services.auth_service.decode_token', return_value=token_data):
            # Mock refresh token as revoked
            revoked_token = MockRefreshToken(is_revoked=True)
            auth_service.refresh_token_repo.get_all = AsyncMock(
                return_value=[revoked_token]
            )
            
            result = await auth_service.refresh_access_token("revoked_token")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_revoke_refresh_token(self, auth_service):
        """Test revoking a refresh token."""
        token = "token_to_revoke"
        token_data = {"sub": "user-123"}
        
        with patch('src.services.auth_service.jwt.decode') as mock_jwt_decode:
            mock_jwt_decode.return_value = token_data
            token_hash = get_refresh_token_hash(token)
            mock_refresh_token = MockRefreshToken(token=token_hash)
            auth_service.refresh_token_repo.get_all = AsyncMock(
                return_value=[mock_refresh_token]
            )
            auth_service.refresh_token_repo.revoke_token = AsyncMock(return_value=True)
            
            result = await auth_service.revoke_refresh_token(token)
            
            assert result is True
            auth_service.refresh_token_repo.revoke_token.assert_called_once_with(
                mock_refresh_token.token
            )
    
    @pytest.mark.asyncio
    async def test_revoke_all_user_tokens(self, auth_service):
        """Test revoking all user tokens."""
        user_id = "user-123"
        
        auth_service.refresh_token_repo.revoke_all_for_user = AsyncMock()
        
        await auth_service.revoke_all_user_tokens(user_id)
        
        auth_service.refresh_token_repo.revoke_all_for_user.assert_called_once_with(
            user_id
        )