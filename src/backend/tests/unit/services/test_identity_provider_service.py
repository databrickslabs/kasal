"""
Unit tests for IdentityProviderService.

Tests the functionality of identity provider operations including
provider CRUD operations, configuration management, and usage statistics.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.services.identity_provider_service import IdentityProviderService
from src.schemas.user import IdentityProviderCreate, IdentityProviderUpdate, IdentityProviderType, IdentityProviderConfig


# Mock models
class MockIdentityProvider:
    def __init__(self, id="provider-123", name="test_provider", provider_type="oauth",
                 enabled=True, is_default=False, config=None, created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.provider_type = provider_type
        self.enabled = enabled
        self.is_default = is_default
        self.config = config or '{"client_id": "test_client", "client_secret": "test_secret"}'
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


class MockExternalIdentity:
    def __init__(self, id="ext-123", provider="test_provider", provider_user_id="user123",
                 user_id="user-456", email="test@example.com"):
        self.id = id
        self.provider = provider
        self.provider_user_id = provider_user_id
        self.user_id = user_id
        self.email = email


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def identity_provider_service(mock_session):
    """Create an IdentityProviderService instance with mock session."""
    with patch('src.services.identity_provider_service.IdentityProviderRepository') as MockProviderRepo, \
         patch('src.services.identity_provider_service.ExternalIdentityRepository') as MockExternalRepo:
        
        mock_provider_repo = AsyncMock()
        mock_external_repo = AsyncMock()
        MockProviderRepo.return_value = mock_provider_repo
        MockExternalRepo.return_value = mock_external_repo
        
        service = IdentityProviderService(mock_session)
        service.provider_repo = mock_provider_repo
        service.external_identity_repo = mock_external_repo
        return service


@pytest.fixture
def mock_identity_provider():
    """Create a mock identity provider."""
    return MockIdentityProvider()


@pytest.fixture
def mock_external_identity():
    """Create a mock external identity."""
    return MockExternalIdentity()


@pytest.fixture
def provider_create_data():
    """Create sample IdentityProviderCreate data."""
    return {
        "name": "test_oauth",
        "type": "oauth",
        "enabled": True,
        "is_default": False,
        "config": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "authorization_endpoint": "https://example.com/oauth/authorize",
            "token_endpoint": "https://example.com/oauth/token"
        }
    }


@pytest.fixture
def provider_update_data():
    """Create sample IdentityProviderUpdate data."""
    return {
        "name": "updated_oauth",
        "enabled": False,
        "config": {
            "client_id": "updated_client_id",
            "client_secret": "updated_client_secret"
        }
    }


class TestIdentityProviderService:
    """Test cases for IdentityProviderService."""
    
    def test_identity_provider_service_initialization(self, identity_provider_service, mock_session):
        """Test IdentityProviderService initialization."""
        assert identity_provider_service.session == mock_session
        assert identity_provider_service.provider_repo is not None
        assert identity_provider_service.external_identity_repo is not None
    
    @pytest.mark.asyncio
    async def test_get_provider_success(self, identity_provider_service, mock_identity_provider):
        """Test successful provider retrieval by ID."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        
        result = await identity_provider_service.get_provider("provider-123")
        
        assert result == mock_identity_provider
        identity_provider_service.provider_repo.get.assert_called_once_with("provider-123")
    
    @pytest.mark.asyncio
    async def test_get_provider_not_found(self, identity_provider_service):
        """Test provider retrieval when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        result = await identity_provider_service.get_provider("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_provider_by_name_success(self, identity_provider_service, mock_identity_provider):
        """Test successful provider retrieval by name."""
        identity_provider_service.provider_repo.get_by_name.return_value = mock_identity_provider
        
        result = await identity_provider_service.get_provider_by_name("test_provider")
        
        assert result == mock_identity_provider
        identity_provider_service.provider_repo.get_by_name.assert_called_once_with("test_provider")
    
    @pytest.mark.asyncio
    async def test_get_provider_by_name_not_found(self, identity_provider_service):
        """Test provider retrieval by name when provider not found."""
        identity_provider_service.provider_repo.get_by_name.return_value = None
        
        result = await identity_provider_service.get_provider_by_name("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_providers_all(self, identity_provider_service):
        """Test getting all providers."""
        mock_providers = [MockIdentityProvider(id="1"), MockIdentityProvider(id="2")]
        identity_provider_service.provider_repo.get_all.return_value = mock_providers
        
        result = await identity_provider_service.get_providers()
        
        assert result == mock_providers
        identity_provider_service.provider_repo.get_all.assert_called_once_with(filters=None, skip=0, limit=100)
    
    @pytest.mark.asyncio
    async def test_get_providers_enabled_only(self, identity_provider_service):
        """Test getting only enabled providers."""
        mock_providers = [MockIdentityProvider(enabled=True)]
        identity_provider_service.provider_repo.get_all.return_value = mock_providers
        
        result = await identity_provider_service.get_providers(enabled_only=True)
        
        assert result == mock_providers
        identity_provider_service.provider_repo.get_all.assert_called_once_with(filters={"enabled": True}, skip=0, limit=100)
    
    @pytest.mark.asyncio
    async def test_get_providers_with_pagination(self, identity_provider_service):
        """Test getting providers with pagination."""
        mock_providers = [MockIdentityProvider()]
        identity_provider_service.provider_repo.get_all.return_value = mock_providers
        
        result = await identity_provider_service.get_providers(skip=10, limit=20)
        
        assert result == mock_providers
        identity_provider_service.provider_repo.get_all.assert_called_once_with(filters=None, skip=10, limit=20)
    
    @pytest.mark.asyncio
    async def test_create_provider_success(self, identity_provider_service, provider_create_data, mock_identity_provider):
        """Test successful provider creation."""
        identity_provider_service.provider_repo.get_by_name.return_value = None  # No existing provider
        identity_provider_service.provider_repo.create.return_value = mock_identity_provider
        
        provider_create = IdentityProviderCreate(**provider_create_data)
        result = await identity_provider_service.create_provider(provider_create)
        
        assert result == mock_identity_provider
        identity_provider_service.provider_repo.get_by_name.assert_called_once_with(provider_create_data["name"])
        identity_provider_service.provider_repo.create.assert_called_once()
        
        # Verify that config was JSON encoded
        call_args = identity_provider_service.provider_repo.create.call_args[0][0]
        assert isinstance(call_args["config"], str)
        parsed_config = json.loads(call_args["config"])
        assert parsed_config["client_id"] == provider_create_data["config"]["client_id"]
    
    @pytest.mark.asyncio
    async def test_create_provider_duplicate_name(self, identity_provider_service, provider_create_data, mock_identity_provider):
        """Test provider creation with duplicate name."""
        identity_provider_service.provider_repo.get_by_name.return_value = mock_identity_provider  # Existing provider
        
        provider_create = IdentityProviderCreate(**provider_create_data)
        
        with pytest.raises(ValueError) as exc_info:
            await identity_provider_service.create_provider(provider_create)
        
        assert "already exists" in str(exc_info.value)
        assert provider_create_data["name"] in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_provider_success(self, identity_provider_service, provider_update_data, mock_identity_provider):
        """Test successful provider update."""
        identity_provider_service.provider_repo.get.side_effect = [mock_identity_provider, mock_identity_provider]
        identity_provider_service.provider_repo.get_by_name.return_value = None  # No name conflict
        identity_provider_service.provider_repo.update.return_value = None
        
        provider_update = IdentityProviderUpdate(**provider_update_data)
        result = await identity_provider_service.update_provider("provider-123", provider_update)
        
        assert result == mock_identity_provider
        identity_provider_service.provider_repo.update.assert_called_once()
        
        # Verify that config was JSON encoded
        call_args = identity_provider_service.provider_repo.update.call_args[0][1]
        assert isinstance(call_args["config"], str)
        parsed_config = json.loads(call_args["config"])
        assert parsed_config["client_id"] == provider_update_data["config"]["client_id"]
    
    @pytest.mark.asyncio
    async def test_update_provider_not_found(self, identity_provider_service, provider_update_data):
        """Test provider update when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        provider_update = IdentityProviderUpdate(**provider_update_data)
        result = await identity_provider_service.update_provider("nonexistent", provider_update)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_provider_duplicate_name(self, identity_provider_service, provider_update_data, mock_identity_provider):
        """Test provider update with duplicate name."""
        existing_provider = MockIdentityProvider(id="other-id", name="updated_oauth")
        mock_identity_provider.name = "old_name"  # Different from update name
        
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.provider_repo.get_by_name.return_value = existing_provider  # Name conflict
        
        provider_update = IdentityProviderUpdate(**provider_update_data)
        
        with pytest.raises(ValueError) as exc_info:
            await identity_provider_service.update_provider("provider-123", provider_update)
        
        assert "already exists" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_provider_same_name(self, identity_provider_service, mock_identity_provider):
        """Test provider update with same name (no conflict)."""
        mock_identity_provider.name = "same_name"
        identity_provider_service.provider_repo.get.side_effect = [mock_identity_provider, mock_identity_provider]
        identity_provider_service.provider_repo.update.return_value = None
        
        provider_update = IdentityProviderUpdate(name="same_name", enabled=False)
        result = await identity_provider_service.update_provider("provider-123", provider_update)
        
        assert result == mock_identity_provider
        # Should not call get_by_name since name is same
        identity_provider_service.provider_repo.get_by_name.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_provider_success(self, identity_provider_service, mock_identity_provider):
        """Test successful provider deletion."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.external_identity_repo.get_all_by_provider_name.return_value = []  # No users
        identity_provider_service.provider_repo.delete.return_value = None
        
        result = await identity_provider_service.delete_provider("provider-123")
        
        assert result is True
        identity_provider_service.provider_repo.delete.assert_called_once_with("provider-123")
    
    @pytest.mark.asyncio
    async def test_delete_provider_not_found(self, identity_provider_service):
        """Test provider deletion when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        result = await identity_provider_service.delete_provider("nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_provider_in_use(self, identity_provider_service, mock_identity_provider, mock_external_identity):
        """Test provider deletion when provider is in use."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.external_identity_repo.get_all_by_provider_name.return_value = [mock_external_identity]
        
        with pytest.raises(ValueError) as exc_info:
            await identity_provider_service.delete_provider("provider-123")
        
        assert "Cannot delete provider" in str(exc_info.value)
        assert "being used by 1 users" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_toggle_provider_status_success(self, identity_provider_service, mock_identity_provider):
        """Test successful provider status toggle."""
        identity_provider_service.provider_repo.get.side_effect = [mock_identity_provider, mock_identity_provider]
        identity_provider_service.provider_repo.update.return_value = None
        
        result = await identity_provider_service.toggle_provider_status("provider-123", False)
        
        assert result == mock_identity_provider
        identity_provider_service.provider_repo.update.assert_called_once_with("provider-123", {"enabled": False})
    
    @pytest.mark.asyncio
    async def test_toggle_provider_status_not_found(self, identity_provider_service):
        """Test provider status toggle when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        result = await identity_provider_service.toggle_provider_status("nonexistent", True)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_provider_config_success(self, identity_provider_service, mock_identity_provider):
        """Test successful provider config retrieval."""
        config_dict = {"client_id": "test_client", "client_secret": "test_secret"}
        mock_identity_provider.config = json.dumps(config_dict)
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        
        result = await identity_provider_service.get_provider_config("provider-123")
        
        assert result == config_dict
    
    @pytest.mark.asyncio
    async def test_get_provider_config_not_found(self, identity_provider_service):
        """Test provider config retrieval when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        result = await identity_provider_service.get_provider_config("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_provider_config_no_config(self, identity_provider_service, mock_identity_provider):
        """Test provider config retrieval when provider has no config."""
        mock_identity_provider.config = None
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        
        result = await identity_provider_service.get_provider_config("provider-123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_provider_config_invalid_json(self, identity_provider_service, mock_identity_provider):
        """Test provider config retrieval when config is invalid JSON."""
        mock_identity_provider.config = "invalid json"
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        
        result = await identity_provider_service.get_provider_config("provider-123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_provider_usage_stats_success(self, identity_provider_service, mock_identity_provider, mock_external_identity):
        """Test successful provider usage stats retrieval."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.external_identity_repo.get_all_by_provider_name.return_value = [mock_external_identity, mock_external_identity]
        
        result = await identity_provider_service.get_provider_usage_stats("provider-123")
        
        assert result["provider_id"] == "provider-123"
        assert result["provider_name"] == mock_identity_provider.name
        assert result["provider_type"] == mock_identity_provider.provider_type
        assert result["count"] == 2
        assert result["enabled"] == mock_identity_provider.enabled
    
    @pytest.mark.asyncio
    async def test_get_provider_usage_stats_not_found(self, identity_provider_service):
        """Test provider usage stats retrieval when provider not found."""
        identity_provider_service.provider_repo.get.return_value = None
        
        result = await identity_provider_service.get_provider_usage_stats("nonexistent")
        
        assert result["error"] == "Provider not found"
        assert result["count"] == 0
    
    def test_method_existence(self, identity_provider_service):
        """Test that all expected methods exist."""
        expected_methods = [
            'get_provider',
            'get_provider_by_name',
            'get_providers',
            'create_provider',
            'update_provider',
            'delete_provider',
            'toggle_provider_status',
            'get_provider_config',
            'get_provider_usage_stats'
        ]
        
        for method_name in expected_methods:
            assert hasattr(identity_provider_service, method_name)
            assert callable(getattr(identity_provider_service, method_name))
    
    @pytest.mark.asyncio
    async def test_config_json_handling_in_create(self, identity_provider_service, provider_create_data, mock_identity_provider):
        """Test that config dictionary is properly converted to JSON string during creation."""
        identity_provider_service.provider_repo.get_by_name.return_value = None
        identity_provider_service.provider_repo.create.return_value = mock_identity_provider
        
        provider_create = IdentityProviderCreate(**provider_create_data)
        await identity_provider_service.create_provider(provider_create)
        
        # Verify that the config was converted to JSON string
        call_args = identity_provider_service.provider_repo.create.call_args[0][0]
        assert isinstance(call_args["config"], str)
        
        # Verify that the JSON string can be parsed back and contains the expected data
        parsed_config = json.loads(call_args["config"])
        assert parsed_config["client_id"] == "test_client_id"
        assert parsed_config["client_secret"] == "test_client_secret"
    
    @pytest.mark.asyncio
    async def test_config_json_handling_in_update(self, identity_provider_service, mock_identity_provider):
        """Test that config dictionary is properly converted to JSON string during update."""
        identity_provider_service.provider_repo.get.side_effect = [mock_identity_provider, mock_identity_provider]
        identity_provider_service.provider_repo.update.return_value = None
        
        config_obj = IdentityProviderConfig(client_id="new_client_id")
        provider_update = IdentityProviderUpdate(config=config_obj)
        await identity_provider_service.update_provider("provider-123", provider_update)
        
        # Verify that the config was converted to JSON string
        call_args = identity_provider_service.provider_repo.update.call_args[0][1]
        assert isinstance(call_args["config"], str)
        
        # Verify that the JSON string can be parsed back and contains the expected data
        parsed_config = json.loads(call_args["config"])
        assert parsed_config["client_id"] == "new_client_id"
    
    @pytest.mark.asyncio
    async def test_error_handling_in_create(self, identity_provider_service, provider_create_data):
        """Test error handling during provider creation."""
        identity_provider_service.provider_repo.get_by_name.return_value = None
        identity_provider_service.provider_repo.create.side_effect = Exception("Database error")
        
        provider_create = IdentityProviderCreate(**provider_create_data)
        
        with pytest.raises(Exception) as exc_info:
            await identity_provider_service.create_provider(provider_create)
        
        assert "Database error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_error_handling_in_update(self, identity_provider_service, provider_update_data, mock_identity_provider):
        """Test error handling during provider update."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.provider_repo.get_by_name.return_value = None
        identity_provider_service.provider_repo.update.side_effect = Exception("Update failed")
        
        provider_update = IdentityProviderUpdate(**provider_update_data)
        
        with pytest.raises(Exception) as exc_info:
            await identity_provider_service.update_provider("provider-123", provider_update)
        
        assert "Update failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_error_handling_in_delete(self, identity_provider_service, mock_identity_provider):
        """Test error handling during provider deletion."""
        identity_provider_service.provider_repo.get.return_value = mock_identity_provider
        identity_provider_service.external_identity_repo.get_all_by_provider_name.return_value = []
        identity_provider_service.provider_repo.delete.side_effect = Exception("Delete failed")
        
        with pytest.raises(Exception) as exc_info:
            await identity_provider_service.delete_provider("provider-123")
        
        assert "Delete failed" in str(exc_info.value)