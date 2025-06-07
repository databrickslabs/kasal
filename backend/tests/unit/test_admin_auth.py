"""
Unit tests for AdminAuth dependency.

Tests the functionality of the admin authentication dependency
including permission checks and access control.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.dependencies.admin_auth import AdminAuth, get_admin_user, require_admin, require_superuser


@pytest.fixture
def mock_current_user():
    """Create a mock current user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "admin_user"
    user.is_active = True
    user.is_superuser = True
    user.roles = ["admin"]
    return user


@pytest.fixture
def mock_regular_user():
    """Create a mock regular user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "regular_user"
    user.is_active = True
    user.is_superuser = False
    user.roles = ["user"]
    return user


@pytest.fixture
def mock_inactive_user():
    """Create a mock inactive user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "inactive_user"
    user.is_active = False
    user.is_superuser = True
    user.roles = ["admin"]
    return user


class TestAdminAuth:
    """Test cases for AdminAuth dependency."""
    
    def test_admin_auth_init(self):
        """Test AdminAuth initialization."""
        admin_auth = AdminAuth()
        assert admin_auth is not None
    
    def test_admin_auth_with_admin_user(self, mock_current_user):
        """Test AdminAuth with admin user."""
        admin_auth = AdminAuth()
        
        # Should not raise exception for admin user
        result = admin_auth.check_admin_access(mock_current_user)
        assert result == mock_current_user
    
    def test_admin_auth_with_regular_user(self, mock_regular_user):
        """Test AdminAuth with regular user."""
        admin_auth = AdminAuth()
        
        # Should raise exception for regular user
        with pytest.raises(HTTPException) as exc_info:
            admin_auth.check_admin_access(mock_regular_user)
        
        assert exc_info.value.status_code == 403
        assert "Admin access required" in str(exc_info.value.detail)
    
    def test_admin_auth_with_inactive_user(self, mock_inactive_user):
        """Test AdminAuth with inactive user."""
        admin_auth = AdminAuth()
        
        # Should raise exception for inactive user
        with pytest.raises(HTTPException) as exc_info:
            admin_auth.check_admin_access(mock_inactive_user)
        
        assert exc_info.value.status_code == 403
        assert "Account is inactive" in str(exc_info.value.detail)
    
    def test_admin_auth_with_none_user(self):
        """Test AdminAuth with None user."""
        admin_auth = AdminAuth()
        
        # Should raise exception for None user
        with pytest.raises(HTTPException) as exc_info:
            admin_auth.check_admin_access(None)
        
        assert exc_info.value.status_code == 401
        assert "Authentication required" in str(exc_info.value.detail)
    
    def test_check_role_permissions(self, mock_current_user):
        """Test role permission checking."""
        admin_auth = AdminAuth()
        
        # Test user with admin role
        assert admin_auth.has_role(mock_current_user, "admin") is True
        assert admin_auth.has_role(mock_current_user, "user") is False
    
    def test_check_superuser_status(self, mock_current_user, mock_regular_user):
        """Test superuser status checking."""
        admin_auth = AdminAuth()
        
        # Test superuser
        assert admin_auth.is_superuser(mock_current_user) is True
        
        # Test regular user
        assert admin_auth.is_superuser(mock_regular_user) is False
    
    def test_check_multiple_roles(self):
        """Test checking multiple roles."""
        admin_auth = AdminAuth()
        
        user_with_multiple_roles = MagicMock()
        user_with_multiple_roles.roles = ["admin", "editor", "viewer"]
        
        assert admin_auth.has_any_role(user_with_multiple_roles, ["admin", "manager"]) is True
        assert admin_auth.has_any_role(user_with_multiple_roles, ["manager", "developer"]) is False
        assert admin_auth.has_all_roles(user_with_multiple_roles, ["admin", "editor"]) is True
        assert admin_auth.has_all_roles(user_with_multiple_roles, ["admin", "manager"]) is False


class TestGetAdminUser:
    """Test cases for get_admin_user dependency function."""
    
    @pytest.mark.asyncio
    async def test_get_admin_user_success(self, mock_current_user):
        """Test successful admin user retrieval."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_current_user):
            result = await get_admin_user()
            
            assert result == mock_current_user
    
    @pytest.mark.asyncio
    async def test_get_admin_user_not_admin(self, mock_regular_user):
        """Test admin user retrieval with non-admin user."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_regular_user):
            with pytest.raises(HTTPException) as exc_info:
                await get_admin_user()
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_admin_user_inactive(self, mock_inactive_user):
        """Test admin user retrieval with inactive user."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_inactive_user):
            with pytest.raises(HTTPException) as exc_info:
                await get_admin_user()
            
            assert exc_info.value.status_code == 403


class TestRequireAdmin:
    """Test cases for require_admin dependency function."""
    
    @pytest.mark.asyncio
    async def test_require_admin_success(self, mock_current_user):
        """Test successful admin requirement check."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_current_user):
            result = await require_admin()
            
            assert result == mock_current_user
    
    @pytest.mark.asyncio
    async def test_require_admin_not_admin(self, mock_regular_user):
        """Test admin requirement with non-admin user."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_regular_user):
            with pytest.raises(HTTPException) as exc_info:
                await require_admin()
            
            assert exc_info.value.status_code == 403
            assert "Admin privileges required" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_require_admin_with_role_check(self):
        """Test admin requirement with specific role check."""
        user_with_editor_role = MagicMock()
        user_with_editor_role.id = uuid.uuid4()
        user_with_editor_role.username = "editor_user"
        user_with_editor_role.is_active = True
        user_with_editor_role.is_superuser = False
        user_with_editor_role.roles = ["admin", "editor"]  # Has admin role
        
        with patch("src.dependencies.admin_auth.get_current_user", return_value=user_with_editor_role):
            result = await require_admin()
            
            assert result == user_with_editor_role


class TestRequireSuperuser:
    """Test cases for require_superuser dependency function."""
    
    @pytest.mark.asyncio
    async def test_require_superuser_success(self, mock_current_user):
        """Test successful superuser requirement check."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_current_user):
            result = await require_superuser()
            
            assert result == mock_current_user
    
    @pytest.mark.asyncio
    async def test_require_superuser_not_superuser(self, mock_regular_user):
        """Test superuser requirement with non-superuser."""
        with patch("src.dependencies.admin_auth.get_current_user", return_value=mock_regular_user):
            with pytest.raises(HTTPException) as exc_info:
                await require_superuser()
            
            assert exc_info.value.status_code == 403
            assert "Superuser privileges required" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_require_superuser_admin_but_not_superuser(self):
        """Test superuser requirement with admin user who is not superuser."""
        admin_not_superuser = MagicMock()
        admin_not_superuser.id = uuid.uuid4()
        admin_not_superuser.username = "admin_user"
        admin_not_superuser.is_active = True
        admin_not_superuser.is_superuser = False  # Not superuser
        admin_not_superuser.roles = ["admin"]
        
        with patch("src.dependencies.admin_auth.get_current_user", return_value=admin_not_superuser):
            with pytest.raises(HTTPException) as exc_info:
                await require_superuser()
            
            assert exc_info.value.status_code == 403


class TestPermissionHelpers:
    """Test cases for permission helper functions."""
    
    def test_check_resource_permission(self):
        """Test resource-specific permission checking."""
        from src.dependencies.admin_auth import check_resource_permission
        
        user = MagicMock()
        user.permissions = ["read:users", "write:groups", "admin:*"]
        
        # Test specific resource permissions
        assert check_resource_permission(user, "read", "users") is True
        assert check_resource_permission(user, "write", "users") is False
        assert check_resource_permission(user, "write", "groups") is True
        
        # Test admin wildcard permission
        assert check_resource_permission(user, "delete", "anything") is True
    
    def test_check_tenant_access(self):
        """Test tenant-specific access checking."""
        from src.dependencies.admin_auth import check_tenant_access
        
        user = MagicMock()
        user.tenant_id = "tenant_123"
        user.is_superuser = False
        
        # Test same tenant access
        assert check_tenant_access(user, "tenant_123") is True
        
        # Test different tenant access
        assert check_tenant_access(user, "tenant_456") is False
        
        # Test superuser access to any tenant
        user.is_superuser = True
        assert check_tenant_access(user, "tenant_456") is True
    
    def test_check_permission_hierarchy(self):
        """Test permission hierarchy checking."""
        from src.dependencies.admin_auth import check_permission_hierarchy
        
        user = MagicMock()
        user.roles = ["editor"]
        
        role_hierarchy = {
            "superuser": 0,
            "admin": 1,
            "editor": 2,
            "viewer": 3
        }
        
        # Test permission hierarchy
        assert check_permission_hierarchy(user, "viewer", role_hierarchy) is True  # Can manage lower
        assert check_permission_hierarchy(user, "editor", role_hierarchy) is False  # Cannot manage same
        assert check_permission_hierarchy(user, "admin", role_hierarchy) is False  # Cannot manage higher
    
    def test_validate_api_key_permissions(self):
        """Test API key permission validation."""
        from src.dependencies.admin_auth import validate_api_key_permissions
        
        api_key = MagicMock()
        api_key.permissions = ["read:*", "write:users"]
        api_key.is_active = True
        api_key.expires_at = None
        
        # Test valid API key
        assert validate_api_key_permissions(api_key, "read", "groups") is True
        assert validate_api_key_permissions(api_key, "write", "users") is True
        assert validate_api_key_permissions(api_key, "delete", "users") is False
        
        # Test inactive API key
        api_key.is_active = False
        assert validate_api_key_permissions(api_key, "read", "groups") is False
    
    def test_enforce_rate_limiting(self):
        """Test rate limiting enforcement."""
        from src.dependencies.admin_auth import enforce_rate_limiting
        
        user = MagicMock()
        user.id = "user_123"
        
        # Test rate limiting (should not raise for first few calls)
        for _ in range(5):
            enforce_rate_limiting(user, action="api_call")
        
        # Mock exceeding rate limit
        with patch("src.dependencies.admin_auth.redis_client") as mock_redis:
            mock_redis.get.return_value = b"100"  # High count
            
            with pytest.raises(HTTPException) as exc_info:
                enforce_rate_limiting(user, action="api_call", limit=10)
            
            assert exc_info.value.status_code == 429
    
    def test_log_security_event(self):
        """Test security event logging."""
        from src.dependencies.admin_auth import log_security_event
        
        user = MagicMock()
        user.id = "user_123"
        user.username = "testuser"
        
        with patch("src.dependencies.admin_auth.security_logger") as mock_logger:
            log_security_event(
                user=user,
                event_type="login_attempt",
                success=True,
                ip_address="192.168.1.1",
                details={"method": "password"}
            )
            
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0][0]
            assert "login_attempt" in call_args
            assert "user_123" in call_args