"""
Unit tests for TenantService (updated version).

Tests the functionality of the updated tenant service including
tenant management, member operations, and multi-tenancy features.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.tenant_service import TenantService
from src.schemas.tenant import TenantCreate, TenantUpdate
from src.models.tenant import Tenant
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
def mock_tenant_repository():
    """Create a mock tenant repository."""
    repo = AsyncMock()
    
    # Create mock tenant objects
    mock_tenant = MagicMock(spec=Tenant)
    mock_tenant.id = uuid.uuid4()
    mock_tenant.name = "Test Tenant"
    mock_tenant.description = "A test tenant"
    mock_tenant.domain = "test.example.com"
    mock_tenant.is_active = True
    mock_tenant.created_at = datetime.now(UTC)
    mock_tenant.updated_at = datetime.now(UTC)
    mock_tenant.settings = {"feature_flags": {"ai_enabled": True}}
    
    # Setup repository method returns
    repo.get.return_value = mock_tenant
    repo.list.return_value = [mock_tenant]
    repo.create.return_value = mock_tenant
    repo.update.return_value = mock_tenant
    repo.delete.return_value = True
    repo.get_by_domain.return_value = mock_tenant
    repo.get_by_name.return_value = mock_tenant
    
    return repo


@pytest.fixture
def tenant_create_data():
    """Create test data for tenant creation."""
    return TenantCreate(
        name="Test Tenant",
        description="A test tenant for testing",
        domain="test.example.com",
        settings={"feature_flags": {"ai_enabled": True}}
    )


@pytest.fixture
def tenant_update_data():
    """Create test data for tenant updates."""
    return TenantUpdate(
        description="Updated test tenant",
        is_active=False,
        settings={"feature_flags": {"ai_enabled": False}}
    )


class TestTenantService:
    """Test cases for TenantService."""
    
    @pytest.mark.asyncio
    async def test_create_tenant_success(self, mock_uow, mock_tenant_repository, tenant_create_data):
        """Test successful tenant creation."""
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            mock_tenant_repository.get_by_domain.return_value = None  # Domain not taken
            mock_tenant_repository.get_by_name.return_value = None  # Name not taken
            
            service = TenantService(mock_uow)
            
            result = await service.create(tenant_create_data)
            
            assert result is not None
            assert result.name == "Test Tenant"
            mock_tenant_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tenant_duplicate_domain(self, mock_uow, mock_tenant_repository, tenant_create_data):
        """Test tenant creation with duplicate domain."""
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            mock_tenant_repository.get_by_domain.return_value = MagicMock()  # Domain exists
            
            service = TenantService(mock_uow)
            
            with pytest.raises(ValueError, match="Domain already exists"):
                await service.create(tenant_create_data)
    
    @pytest.mark.asyncio
    async def test_create_tenant_duplicate_name(self, mock_uow, mock_tenant_repository, tenant_create_data):
        """Test tenant creation with duplicate name."""
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            mock_tenant_repository.get_by_domain.return_value = None
            mock_tenant_repository.get_by_name.return_value = MagicMock()  # Name exists
            
            service = TenantService(mock_uow)
            
            with pytest.raises(ValueError, match="Tenant name already exists"):
                await service.create(tenant_create_data)
    
    @pytest.mark.asyncio
    async def test_get_tenant_by_id(self, mock_uow, mock_tenant_repository):
        """Test getting a tenant by ID."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.get(tenant_id)
            
            assert result is not None
            assert result.name == "Test Tenant"
            mock_tenant_repository.get.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_get_tenant_by_domain(self, mock_uow, mock_tenant_repository):
        """Test getting a tenant by domain."""
        domain = "test.example.com"
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.get_by_domain(domain)
            
            assert result is not None
            assert result.domain == domain
            mock_tenant_repository.get_by_domain.assert_called_once_with(domain)
    
    @pytest.mark.asyncio
    async def test_update_tenant_success(self, mock_uow, mock_tenant_repository, tenant_update_data):
        """Test successful tenant update."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.update(tenant_id, tenant_update_data)
            
            assert result is not None
            mock_tenant_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_tenant_success(self, mock_uow, mock_tenant_repository):
        """Test successful tenant deletion."""
        tenant_id = uuid.uuid4()
        
        # Mock that tenant has no active members
        mock_tenant_repository.has_active_members.return_value = False
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.delete(tenant_id)
            
            assert result is True
            mock_tenant_repository.delete.assert_called_once_with(tenant_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_tenant_with_active_members(self, mock_uow, mock_tenant_repository):
        """Test deletion of tenant with active members should fail."""
        tenant_id = uuid.uuid4()
        
        # Mock that tenant has active members
        mock_tenant_repository.has_active_members.return_value = True
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            with pytest.raises(ValueError, match="Cannot delete tenant with active members"):
                await service.delete(tenant_id)
    
    @pytest.mark.asyncio
    async def test_add_member_to_tenant(self, mock_uow, mock_tenant_repository):
        """Test adding member to tenant."""
        tenant_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.add_member(tenant_id, user_id)
            
            assert result is True
            mock_tenant_repository.add_member.assert_called_once_with(tenant_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_member_from_tenant(self, mock_uow, mock_tenant_repository):
        """Test removing member from tenant."""
        tenant_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.remove_member(tenant_id, user_id)
            
            assert result is True
            mock_tenant_repository.remove_member.assert_called_once_with(tenant_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_tenant_members(self, mock_uow, mock_tenant_repository):
        """Test getting tenant members."""
        tenant_id = uuid.uuid4()
        mock_members = [
            {"id": uuid.uuid4(), "username": "user1", "email": "user1@test.com"},
            {"id": uuid.uuid4(), "username": "user2", "email": "user2@test.com"}
        ]
        mock_tenant_repository.get_members.return_value = mock_members
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.get_members(tenant_id)
            
            assert len(result) == 2
            assert result[0]["username"] == "user1"
            mock_tenant_repository.get_members.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_update_tenant_settings(self, mock_uow, mock_tenant_repository):
        """Test updating tenant settings."""
        tenant_id = uuid.uuid4()
        settings = {
            "feature_flags": {"ai_enabled": False, "analytics_enabled": True},
            "limits": {"max_users": 100, "max_storage_gb": 50}
        }
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.update_settings(tenant_id, settings)
            
            assert result is True
            mock_tenant_repository.update_settings.assert_called_once_with(tenant_id, settings)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_deactivate_tenant(self, mock_uow, mock_tenant_repository):
        """Test tenant deactivation."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.deactivate(tenant_id)
            
            assert result is True
            mock_tenant_repository.deactivate.assert_called_once_with(tenant_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_reactivate_tenant(self, mock_uow, mock_tenant_repository):
        """Test tenant reactivation."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            result = await service.reactivate(tenant_id)
            
            assert result is True
            mock_tenant_repository.reactivate.assert_called_once_with(tenant_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_domain_format(self, mock_uow):
        """Test domain format validation."""
        service = TenantService(mock_uow)
        
        # Test valid domains
        valid_domains = ["example.com", "sub.example.com", "test-domain.org"]
        for domain in valid_domains:
            service._validate_domain(domain)  # Should not raise
        
        # Test invalid domains
        invalid_domains = ["", "invalid", "domain with spaces", ".invalid.com"]
        for domain in invalid_domains:
            with pytest.raises(ValueError, match="Invalid domain format"):
                service._validate_domain(domain)
    
    @pytest.mark.asyncio
    async def test_tenant_resource_quotas(self, mock_uow, mock_tenant_repository):
        """Test tenant resource quota management."""
        tenant_id = uuid.uuid4()
        quotas = {
            "max_users": 100,
            "max_storage_gb": 50,
            "max_api_calls_per_month": 10000
        }
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Set quotas
            result = await service.set_resource_quotas(tenant_id, quotas)
            assert result is True
            
            # Check quota usage
            mock_usage = {"users": 25, "storage_gb": 10, "api_calls_this_month": 2500}
            mock_tenant_repository.get_resource_usage.return_value = mock_usage
            
            usage = await service.get_resource_usage(tenant_id)
            assert usage["users"] == 25
            
            # Check quota limits
            result = await service.check_quota_limits(tenant_id, "users", 5)
            assert result is True  # Adding 5 users should be within limits
    
    @pytest.mark.asyncio
    async def test_tenant_isolation_validation(self, mock_uow, mock_tenant_repository):
        """Test tenant data isolation validation."""
        tenant_id = uuid.uuid4()
        other_tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Test that tenant can only access its own data
            result = service._validate_tenant_access(tenant_id, tenant_id)
            assert result is True
            
            # Test that tenant cannot access other tenant's data
            result = service._validate_tenant_access(tenant_id, other_tenant_id)
            assert result is False
    
    @pytest.mark.asyncio
    async def test_tenant_backup_and_restore(self, mock_uow, mock_tenant_repository):
        """Test tenant backup and restore operations."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Create backup
            backup_data = {
                "tenant_info": {"name": "Test Tenant", "domain": "test.example.com"},
                "users": [],
                "settings": {},
                "timestamp": datetime.now(UTC).isoformat()
            }
            mock_tenant_repository.create_backup.return_value = backup_data
            
            result = await service.create_backup(tenant_id)
            assert "tenant_info" in result
            
            # Restore from backup
            result = await service.restore_from_backup(tenant_id, backup_data)
            assert result is True
            
            mock_tenant_repository.restore_from_backup.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_tenant_analytics_and_metrics(self, mock_uow, mock_tenant_repository):
        """Test tenant analytics and metrics collection."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            mock_metrics = {
                "user_count": 25,
                "active_users_last_30_days": 20,
                "storage_used_gb": 15.5,
                "api_calls_last_month": 5000,
                "feature_usage": {"ai_enabled": True, "analytics_enabled": False}
            }
            mock_tenant_repository.get_analytics.return_value = mock_metrics
            
            result = await service.get_analytics(tenant_id)
            
            assert result["user_count"] == 25
            assert result["active_users_last_30_days"] == 20
            mock_tenant_repository.get_analytics.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_tenant_compliance_operations(self, mock_uow, mock_tenant_repository):
        """Test tenant compliance and data protection operations."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Export tenant data
            export_data = await service.export_tenant_data(tenant_id)
            assert "tenant_info" in export_data
            mock_tenant_repository.export_data.assert_called_once_with(tenant_id)
            
            # Anonymize tenant data
            result = await service.anonymize_tenant_data(tenant_id)
            assert result is True
            mock_tenant_repository.anonymize_data.assert_called_once_with(tenant_id)
            
            # Purge tenant data
            result = await service.purge_tenant_data(tenant_id)
            assert result is True
            mock_tenant_repository.purge_data.assert_called_once_with(tenant_id)
    
    @pytest.mark.asyncio
    async def test_tenant_audit_logging(self, mock_uow, mock_tenant_repository, tenant_create_data):
        """Test that tenant operations are audited."""
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository), \
             patch("src.services.tenant_service.audit_logger") as mock_audit:
            
            mock_tenant_repository.get_by_domain.return_value = None
            mock_tenant_repository.get_by_name.return_value = None
            
            service = TenantService(mock_uow)
            
            await service.create(tenant_create_data)
            
            mock_audit.log_tenant_creation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_tenant_subscription_management(self, mock_uow, mock_tenant_repository):
        """Test tenant subscription and billing management."""
        tenant_id = uuid.uuid4()
        subscription_data = {
            "plan": "premium",
            "billing_cycle": "monthly",
            "features": ["ai_enabled", "analytics_enabled", "priority_support"]
        }
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Update subscription
            result = await service.update_subscription(tenant_id, subscription_data)
            assert result is True
            
            # Get billing information
            mock_billing = {
                "current_plan": "premium",
                "next_billing_date": "2024-02-01",
                "usage_this_period": {"api_calls": 5000, "storage_gb": 15}
            }
            mock_tenant_repository.get_billing_info.return_value = mock_billing
            
            billing_info = await service.get_billing_info(tenant_id)
            assert billing_info["current_plan"] == "premium"
    
    @pytest.mark.asyncio
    async def test_multi_tenant_query_filtering(self, mock_uow, mock_tenant_repository):
        """Test multi-tenant query filtering."""
        tenant_id = uuid.uuid4()
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Test that queries are automatically filtered by tenant
            result = await service.get_tenant_scoped_data(tenant_id, "users")
            
            mock_tenant_repository.get_scoped_data.assert_called_once_with(tenant_id, "users")
    
    @pytest.mark.asyncio
    async def test_cross_tenant_data_sharing(self, mock_uow, mock_tenant_repository):
        """Test controlled cross-tenant data sharing."""
        source_tenant_id = uuid.uuid4()
        target_tenant_id = uuid.uuid4()
        resource_id = uuid.uuid4()
        permissions = ["read"]
        
        with patch("src.services.tenant_service.TenantRepository", return_value=mock_tenant_repository):
            service = TenantService(mock_uow)
            
            # Share resource between tenants
            result = await service.share_resource(source_tenant_id, target_tenant_id, resource_id, permissions)
            assert result is True
            
            # Revoke shared access
            result = await service.revoke_shared_resource(source_tenant_id, target_tenant_id, resource_id)
            assert result is True
            
            mock_tenant_repository.share_resource.assert_called_once()
            mock_tenant_repository.revoke_shared_resource.assert_called_once()