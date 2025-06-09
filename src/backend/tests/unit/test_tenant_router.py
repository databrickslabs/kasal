"""
Unit tests for TenantRouter.

Tests the functionality of the tenant router including
tenant CRUD operations, member management, and access control.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.tenant import TenantCreate, TenantUpdate
from src.models.tenant import Tenant


@pytest.fixture
def mock_tenant_service():
    """Create a mock tenant service."""
    service = AsyncMock()
    
    # Create mock tenant objects
    mock_tenant = MagicMock(spec=Tenant)
    mock_tenant.id = uuid.uuid4()
    mock_tenant.name = "Test Tenant"
    mock_tenant.description = "Test tenant for testing"
    mock_tenant.domain = "test.example.com"
    mock_tenant.is_active = True
    mock_tenant.created_at = "2024-01-01T00:00:00Z"
    
    # Setup service method returns
    service.get.return_value = mock_tenant
    service.list.return_value = [mock_tenant]
    service.create.return_value = mock_tenant
    service.update.return_value = mock_tenant
    service.delete.return_value = True
    service.get_by_domain.return_value = mock_tenant
    
    return service


@pytest.fixture
def mock_current_user():
    """Create a mock current user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "testuser"
    user.is_superuser = True
    user.tenant_id = uuid.uuid4()
    return user


@pytest.fixture
def tenant_create_data():
    """Create test data for tenant creation."""
    return TenantCreate(
        name="Test Tenant",
        description="A test tenant for testing",
        domain="test.example.com"
    )


@pytest.fixture
def tenant_update_data():
    """Create test data for tenant updates."""
    return TenantUpdate(
        description="Updated test tenant",
        is_active=False
    )


class TestTenantRouter:
    """Test cases for TenantRouter."""
    
    @pytest.mark.asyncio
    async def test_create_tenant_success(self, mock_tenant_service, mock_current_user, tenant_create_data):
        """Test successful tenant creation."""
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import create_tenant
            
            result = await create_tenant(tenant_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "Test Tenant"
            mock_tenant_service.create.assert_called_once_with(tenant_create_data)
    
    @pytest.mark.asyncio
    async def test_create_tenant_unauthorized(self, mock_tenant_service, tenant_create_data):
        """Test tenant creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        
        with patch("src.api.tenant_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import create_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await create_tenant(tenant_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_tenant_success(self, mock_tenant_service, mock_current_user):
        """Test successful tenant retrieval."""
        tenant_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import get_tenant
            
            result = await get_tenant(tenant_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "Test Tenant"
            mock_tenant_service.get.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_get_tenant_not_found(self, mock_tenant_service, mock_current_user):
        """Test getting a non-existent tenant."""
        tenant_id = uuid.uuid4()
        mock_tenant_service.get.return_value = None
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import get_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await get_tenant(tenant_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_tenants_success(self, mock_tenant_service, mock_current_user):
        """Test successful tenant listing."""
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import list_tenants
            
            result = await list_tenants(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].name == "Test Tenant"
            mock_tenant_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_tenant_success(self, mock_tenant_service, mock_current_user, tenant_update_data):
        """Test successful tenant update."""
        tenant_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import update_tenant
            
            result = await update_tenant(tenant_id, tenant_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_tenant_service.update.assert_called_once_with(tenant_id, tenant_update_data)
    
    @pytest.mark.asyncio
    async def test_update_tenant_not_found(self, mock_tenant_service, mock_current_user, tenant_update_data):
        """Test updating a non-existent tenant."""
        tenant_id = uuid.uuid4()
        mock_tenant_service.update.return_value = None
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import update_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await update_tenant(tenant_id, tenant_update_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_tenant_success(self, mock_tenant_service, mock_current_user):
        """Test successful tenant deletion."""
        tenant_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import delete_tenant
            
            result = await delete_tenant(tenant_id, current_user=mock_current_user)
            
            assert result["message"] == "Tenant deleted successfully"
            mock_tenant_service.delete.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_delete_tenant_not_found(self, mock_tenant_service, mock_current_user):
        """Test deleting a non-existent tenant."""
        tenant_id = uuid.uuid4()
        mock_tenant_service.delete.return_value = False
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import delete_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_tenant(tenant_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_tenant_by_domain(self, mock_tenant_service, mock_current_user):
        """Test getting tenant by domain."""
        domain = "test.example.com"
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import get_tenant_by_domain
            
            result = await get_tenant_by_domain(domain, current_user=mock_current_user)
            
            assert result is not None
            assert result.domain == "test.example.com"
            mock_tenant_service.get_by_domain.assert_called_once_with(domain)
    
    @pytest.mark.asyncio
    async def test_get_tenant_members(self, mock_tenant_service, mock_current_user):
        """Test getting tenant members."""
        tenant_id = uuid.uuid4()
        mock_members = [
            {"id": uuid.uuid4(), "username": "user1", "email": "user1@test.com"},
            {"id": uuid.uuid4(), "username": "user2", "email": "user2@test.com"}
        ]
        mock_tenant_service.get_members.return_value = mock_members
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import get_tenant_members
            
            result = await get_tenant_members(tenant_id, current_user=mock_current_user)
            
            assert len(result) == 2
            assert result[0]["username"] == "user1"
            mock_tenant_service.get_members.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_add_tenant_member(self, mock_tenant_service, mock_current_user):
        """Test adding member to tenant."""
        tenant_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import add_tenant_member
            
            result = await add_tenant_member(tenant_id, {"user_id": str(user_id)}, current_user=mock_current_user)
            
            assert result["message"] == "Member added successfully"
            mock_tenant_service.add_member.assert_called_once_with(tenant_id, user_id)
    
    @pytest.mark.asyncio
    async def test_remove_tenant_member(self, mock_tenant_service, mock_current_user):
        """Test removing member from tenant."""
        tenant_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import remove_tenant_member
            
            result = await remove_tenant_member(tenant_id, user_id, current_user=mock_current_user)
            
            assert result["message"] == "Member removed successfully"
            mock_tenant_service.remove_member.assert_called_once_with(tenant_id, user_id)
    
    @pytest.mark.asyncio
    async def test_tenant_validation(self, mock_tenant_service, mock_current_user):
        """Test tenant data validation."""
        invalid_tenant_data = TenantCreate(
            name="",  # Invalid empty name
            description="Test tenant",
            domain="invalid-domain"  # Invalid domain format
        )
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            mock_tenant_service.create.side_effect = ValueError("Invalid tenant data")
            
            from src.api.tenant_router import create_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await create_tenant(invalid_tenant_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_tenant_cross_access_check(self, mock_tenant_service):
        """Test that users can only access their own tenant data."""
        tenant_id = uuid.uuid4()
        other_tenant_user = MagicMock()
        other_tenant_user.id = uuid.uuid4()
        other_tenant_user.tenant_id = uuid.uuid4()  # Different tenant
        other_tenant_user.is_superuser = False
        
        with patch("src.api.tenant_router.get_current_user", return_value=other_tenant_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import get_tenant
            
            with pytest.raises(HTTPException) as exc_info:
                await get_tenant(tenant_id, current_user=other_tenant_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_tenant_deactivation(self, mock_tenant_service, mock_current_user):
        """Test tenant deactivation."""
        tenant_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import deactivate_tenant
            
            result = await deactivate_tenant(tenant_id, current_user=mock_current_user)
            
            assert result["message"] == "Tenant deactivated successfully"
            mock_tenant_service.deactivate.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_tenant_reactivation(self, mock_tenant_service, mock_current_user):
        """Test tenant reactivation."""
        tenant_id = uuid.uuid4()
        
        with patch("src.api.tenant_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.tenant_router.TenantService", return_value=mock_tenant_service):
            
            from src.api.tenant_router import reactivate_tenant
            
            result = await reactivate_tenant(tenant_id, current_user=mock_current_user)
            
            assert result["message"] == "Tenant reactivated successfully"
            mock_tenant_service.reactivate.assert_called_once_with(tenant_id)