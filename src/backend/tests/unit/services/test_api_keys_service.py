"""
Unit tests for ApiKeysService.

Tests the functionality of API key management service including
CRUD operations, encryption/decryption, and provider setup.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import os

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.services.api_keys_service import ApiKeysService
from src.models.api_key import ApiKey
from src.repositories.api_key_repository import ApiKeyRepository
from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate


# Mock API key model
class MockApiKey:
    def __init__(self, id="key-123", name="TEST_API_KEY", encrypted_value="encrypted_value",
                 description="Test API Key", created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.encrypted_value = encrypted_value
        self.description = description
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.value = None  # This will be set dynamically during tests


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    return MagicMock(spec=Session)


@pytest.fixture
def mock_repository():
    """Create a mock API key repository."""
    return AsyncMock(spec=ApiKeyRepository)


@pytest.fixture
def api_keys_service_with_repository(mock_repository):
    """Create an API keys service with mocked repository."""
    return ApiKeysService(repository=mock_repository)


@pytest.fixture
def api_keys_service_with_async_session(mock_async_session):
    """Create an API keys service with async session."""
    with patch('src.services.api_keys_service.ApiKeyRepository') as mock_repo_class:
        mock_repo_instance = AsyncMock()
        mock_repo_class.return_value = mock_repo_instance
        service = ApiKeysService(session=mock_async_session)
        service.repository = mock_repo_instance
        return service


@pytest.fixture
def api_keys_service_with_sync_session(mock_sync_session):
    """Create an API keys service with sync session."""
    with patch('src.services.api_keys_service.ApiKeyRepository') as mock_repo_class:
        mock_repo_instance = MagicMock()
        mock_repo_class.return_value = mock_repo_instance
        service = ApiKeysService(session=mock_sync_session)
        service.repository = mock_repo_instance
        return service


@pytest.fixture
def sample_api_key_create():
    """Create a sample API key creation schema."""
    return ApiKeyCreate(
        name="NEW_API_KEY",
        value="test-api-key-value",
        description="New test API key"
    )


@pytest.fixture
def sample_api_key_update():
    """Create a sample API key update schema."""
    return ApiKeyUpdate(
        value="updated-api-key-value",
        description="Updated test API key"
    )


class TestApiKeysServiceInit:
    """Test cases for ApiKeysService initialization."""
    
    def test_init_with_repository(self, mock_repository):
        """Test initialization with repository."""
        service = ApiKeysService(repository=mock_repository)
        
        assert service.repository == mock_repository
        assert service.session is None
        assert service.is_async is True
        assert service.encryption_utils is not None
    
    def test_init_with_async_session(self, mock_async_session):
        """Test initialization with async session."""
        with patch('src.services.api_keys_service.ApiKeyRepository') as mock_repo_class:
            service = ApiKeysService(session=mock_async_session)
            
            assert service.session == mock_async_session
            assert service.is_async is True
            mock_repo_class.assert_called_once_with(mock_async_session)
    
    def test_init_with_sync_session(self, mock_sync_session):
        """Test initialization with sync session."""
        with patch('src.services.api_keys_service.ApiKeyRepository') as mock_repo_class:
            service = ApiKeysService(session=mock_sync_session)
            
            assert service.session == mock_sync_session
            assert service.is_async is False
            mock_repo_class.assert_called_once_with(mock_sync_session)
    
    def test_init_without_session_or_repository(self):
        """Test initialization without session or repository raises ValueError."""
        with pytest.raises(ValueError, match="Either session or repository must be provided"):
            ApiKeysService()


class TestApiKeysServiceFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_async_success(self, api_keys_service_with_repository, mock_repository):
        """Test successful async find by name."""
        api_key = MockApiKey(name="OPENAI_API_KEY")
        mock_repository.find_by_name.return_value = api_key
        
        result = await api_keys_service_with_repository.find_by_name("OPENAI_API_KEY")
        
        assert result == api_key
        mock_repository.find_by_name.assert_called_once_with("OPENAI_API_KEY")
    
    @pytest.mark.asyncio
    async def test_find_by_name_async_not_found(self, api_keys_service_with_repository, mock_repository):
        """Test async find by name when key not found."""
        mock_repository.find_by_name.return_value = None
        
        result = await api_keys_service_with_repository.find_by_name("NONEXISTENT_KEY")
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("NONEXISTENT_KEY")
    
    @pytest.mark.asyncio
    async def test_find_by_name_sync_fallback(self, api_keys_service_with_sync_session):
        """Test find by name falls back to sync method for sync sessions."""
        with patch.object(api_keys_service_with_sync_session, 'find_by_name_sync') as mock_sync:
            mock_sync.return_value = MockApiKey()
            
            result = await api_keys_service_with_sync_session.find_by_name("TEST_KEY")
            
            assert result is not None
            mock_sync.assert_called_once_with("TEST_KEY")
    
    def test_find_by_name_sync_success(self, api_keys_service_with_sync_session):
        """Test successful sync find by name."""
        api_key = MockApiKey(name="SYNC_KEY")
        api_keys_service_with_sync_session.repository.find_by_name_sync.return_value = api_key
        
        result = api_keys_service_with_sync_session.find_by_name_sync("SYNC_KEY")
        
        assert result == api_key
        api_keys_service_with_sync_session.repository.find_by_name_sync.assert_called_once_with("SYNC_KEY")
    
    def test_find_by_name_sync_with_async_session_raises_error(self, api_keys_service_with_repository):
        """Test sync method with async session raises TypeError."""
        with pytest.raises(TypeError, match="This method requires a synchronous session"):
            api_keys_service_with_repository.find_by_name_sync("TEST_KEY")


class TestApiKeysServiceCreate:
    """Test cases for create_api_key method."""
    
    @pytest.mark.asyncio
    async def test_create_api_key_success(self, api_keys_service_with_repository, mock_repository, sample_api_key_create):
        """Test successful API key creation."""
        created_key = MockApiKey(
            name=sample_api_key_create.name,
            description=sample_api_key_create.description
        )
        mock_repository.create.return_value = created_key
        
        with patch('src.services.api_keys_service.EncryptionUtils.encrypt_value', return_value="encrypted_value"):
            result = await api_keys_service_with_repository.create_api_key(sample_api_key_create)
            
            assert result == created_key
            assert result.value == sample_api_key_create.value  # Should be set for response
            mock_repository.create.assert_called_once()
            call_args = mock_repository.create.call_args[0][0]
            assert call_args["name"] == "NEW_API_KEY"
            assert call_args["encrypted_value"] == "encrypted_value"
            assert call_args["description"] == "New test API key"
    
    @pytest.mark.asyncio
    async def test_create_api_key_without_description(self, api_keys_service_with_repository, mock_repository):
        """Test API key creation without description."""
        api_key_data = ApiKeyCreate(name="NO_DESC_KEY", value="test-value")
        created_key = MockApiKey(name="NO_DESC_KEY", description="")
        mock_repository.create.return_value = created_key
        
        with patch('src.services.api_keys_service.EncryptionUtils.encrypt_value', return_value="encrypted"):
            result = await api_keys_service_with_repository.create_api_key(api_key_data)
            
            assert result == created_key
            call_args = mock_repository.create.call_args[0][0]
            assert call_args["description"] == ""


class TestApiKeysServiceUpdate:
    """Test cases for update_api_key method."""
    
    @pytest.mark.asyncio
    async def test_update_api_key_success(self, api_keys_service_with_repository, mock_repository, sample_api_key_update):
        """Test successful API key update."""
        existing_key = MockApiKey(name="EXISTING_KEY")
        updated_key = MockApiKey(
            name="EXISTING_KEY",
            description=sample_api_key_update.description
        )
        
        mock_repository.find_by_name.return_value = existing_key
        mock_repository.update.return_value = updated_key
        
        with patch('src.services.api_keys_service.EncryptionUtils.encrypt_value', return_value="new_encrypted"):
            result = await api_keys_service_with_repository.update_api_key("EXISTING_KEY", sample_api_key_update)
            
            assert result == updated_key
            assert result.value == sample_api_key_update.value
            mock_repository.find_by_name.assert_called_once_with("EXISTING_KEY")
            mock_repository.update.assert_called_once()
            update_args = mock_repository.update.call_args[0]
            assert update_args[0] == existing_key.id
            assert update_args[1]["encrypted_value"] == "new_encrypted"
            assert update_args[1]["description"] == sample_api_key_update.description
    
    @pytest.mark.asyncio
    async def test_update_api_key_not_found(self, api_keys_service_with_repository, mock_repository, sample_api_key_update):
        """Test update when API key doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await api_keys_service_with_repository.update_api_key("NONEXISTENT", sample_api_key_update)
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("NONEXISTENT")
        mock_repository.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_api_key_without_description(self, api_keys_service_with_repository, mock_repository):
        """Test update without description."""
        update_data = ApiKeyUpdate(value="new-value")
        existing_key = MockApiKey()
        updated_key = MockApiKey()
        
        mock_repository.find_by_name.return_value = existing_key
        mock_repository.update.return_value = updated_key
        
        with patch('src.services.api_keys_service.EncryptionUtils.encrypt_value', return_value="encrypted"):
            result = await api_keys_service_with_repository.update_api_key("TEST_KEY", update_data)
            
            update_args = mock_repository.update.call_args[0][1]
            assert "description" not in update_args
            assert update_args["encrypted_value"] == "encrypted"


class TestApiKeysServiceDelete:
    """Test cases for delete_api_key method."""
    
    @pytest.mark.asyncio
    async def test_delete_api_key_success(self, api_keys_service_with_repository, mock_repository):
        """Test successful API key deletion."""
        existing_key = MockApiKey(id="key-123", name="DELETE_ME")
        mock_repository.find_by_name.return_value = existing_key
        mock_repository.delete.return_value = True
        
        result = await api_keys_service_with_repository.delete_api_key("DELETE_ME")
        
        assert result is True
        mock_repository.find_by_name.assert_called_once_with("DELETE_ME")
        mock_repository.delete.assert_called_once_with("key-123")
    
    @pytest.mark.asyncio
    async def test_delete_api_key_not_found(self, api_keys_service_with_repository, mock_repository):
        """Test delete when API key doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await api_keys_service_with_repository.delete_api_key("NONEXISTENT")
        
        assert result is False
        mock_repository.find_by_name.assert_called_once_with("NONEXISTENT")
        mock_repository.delete.assert_not_called()


class TestApiKeysServiceGetAll:
    """Test cases for get_all_api_keys method."""
    
    @pytest.mark.asyncio
    async def test_get_all_api_keys_success(self, api_keys_service_with_repository, mock_repository):
        """Test successful retrieval of all API keys."""
        keys = [
            MockApiKey(name="KEY1", encrypted_value="encrypted1"),
            MockApiKey(name="KEY2", encrypted_value="encrypted2")
        ]
        mock_repository.find_all.return_value = keys
        
        with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', side_effect=["decrypted1", "decrypted2"]):
            result = await api_keys_service_with_repository.get_all_api_keys()
            
            assert len(result) == 2
            assert result[0].value == "decrypted1"
            assert result[1].value == "decrypted2"
            mock_repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_api_keys_decryption_error(self, api_keys_service_with_repository, mock_repository):
        """Test handling decryption errors."""
        keys = [MockApiKey(name="BAD_KEY", encrypted_value="bad_encrypted")]
        mock_repository.find_all.return_value = keys
        
        with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await api_keys_service_with_repository.get_all_api_keys()
                
                assert len(result) == 1
                assert result[0].value == ""  # Should be empty on decryption failure
                mock_logger.error.assert_called_once()


class TestApiKeysServiceGetMetadata:
    """Test cases for get_api_keys_metadata method."""
    
    @pytest.mark.asyncio
    async def test_get_api_keys_metadata_with_values(self, api_keys_service_with_repository, mock_repository):
        """Test metadata retrieval with set values."""
        keys = [
            MockApiKey(name="SET_KEY", encrypted_value="has_value"),
            MockApiKey(name="EMPTY_KEY", encrypted_value=""),
            MockApiKey(name="NULL_KEY", encrypted_value=None)
        ]
        mock_repository.find_all.return_value = keys
        
        result = await api_keys_service_with_repository.get_api_keys_metadata()
        
        assert len(result) == 3
        assert result[0].value == "Set"
        assert result[1].value == "Not set"
        assert result[2].value == "Not set"


class TestApiKeysServiceClassMethods:
    """Test cases for class methods."""
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_success(self):
        """Test successful API key value retrieval."""
        mock_api_key = MockApiKey(name="TEST_KEY", encrypted_value="encrypted")
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = mock_api_key
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', return_value="decrypted"):
                    result = await ApiKeysService.get_api_key_value(key_name="TEST_KEY")
                    
                    assert result == "decrypted"
                    mock_service.find_by_name.assert_called_once_with("TEST_KEY")
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_not_found(self):
        """Test API key value retrieval when key not found."""
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = None
                mock_from_uow.return_value = mock_service
                
                result = await ApiKeysService.get_api_key_value(key_name="NONEXISTENT")
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_backwards_compatibility(self):
        """Test API key value retrieval with backwards compatibility (db as first param)."""
        mock_api_key = MockApiKey(name="COMPAT_KEY", encrypted_value="encrypted")
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = mock_api_key
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', return_value="decrypted"):
                    # Test backwards compatibility where db is passed as first param
                    result = await ApiKeysService.get_api_key_value("COMPAT_KEY")
                    
                    assert result == "decrypted"
                    mock_service.find_by_name.assert_called_once_with("COMPAT_KEY")
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_decryption_error(self):
        """Test API key value retrieval with decryption error."""
        mock_api_key = MockApiKey(name="BAD_KEY", encrypted_value="bad_encrypted")
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = mock_api_key
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
                    with patch('src.services.api_keys_service.logger') as mock_logger:
                        result = await ApiKeysService.get_api_key_value(key_name="BAD_KEY")
                        
                        assert result is None
                        mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_success(self, mock_async_session):
        """Test successful provider API key setup."""
        with patch.object(ApiKeysService, 'get_api_key_value', return_value="test-key-value"):
            result = await ApiKeysService.setup_provider_api_key(mock_async_session, "TEST_KEY")
            
            assert result is True
            assert os.environ.get("TEST_KEY") == "test-key-value"
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_not_found(self, mock_async_session):
        """Test provider API key setup when key not found."""
        with patch.object(ApiKeysService, 'get_api_key_value', return_value=None):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.setup_provider_api_key(mock_async_session, "MISSING_KEY")
                
                assert result is False
                mock_logger.warning.assert_called_once()
    
    def test_setup_provider_api_key_sync_success(self, mock_sync_session):
        """Test successful sync provider API key setup."""
        mock_api_key = MockApiKey(name="SYNC_KEY", encrypted_value="encrypted")
        
        with patch.object(ApiKeysService, '__init__', return_value=None):
            service_instance = ApiKeysService.__new__(ApiKeysService)
            service_instance.find_by_name_sync = MagicMock(return_value=mock_api_key)
            
            with patch('src.services.api_keys_service.ApiKeysService', return_value=service_instance):
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', return_value="decrypted"):
                    result = ApiKeysService.setup_provider_api_key_sync(mock_sync_session, "SYNC_KEY")
                    
                    assert result is True
                    assert os.environ.get("SYNC_KEY") == "decrypted"
    
    def test_setup_provider_api_key_sync_not_found(self, mock_sync_session):
        """Test sync provider API key setup when key not found."""
        with patch.object(ApiKeysService, '__init__', return_value=None):
            service_instance = ApiKeysService.__new__(ApiKeysService)
            service_instance.find_by_name_sync = MagicMock(return_value=None)
            
            with patch('src.services.api_keys_service.ApiKeysService', return_value=service_instance):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = ApiKeysService.setup_provider_api_key_sync(mock_sync_session, "MISSING_KEY")
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    def test_setup_provider_api_key_sync_no_encrypted_value(self, mock_sync_session):
        """Test sync provider API key setup when key has no encrypted value."""
        mock_api_key = MockApiKey(name="EMPTY_KEY", encrypted_value="")
        
        with patch.object(ApiKeysService, '__init__', return_value=None):
            service_instance = ApiKeysService.__new__(ApiKeysService)
            service_instance.find_by_name_sync = MagicMock(return_value=mock_api_key)
            
            with patch('src.services.api_keys_service.ApiKeysService', return_value=service_instance):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = ApiKeysService.setup_provider_api_key_sync(mock_sync_session, "EMPTY_KEY")
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    def test_setup_provider_api_key_sync_exception(self, mock_sync_session):
        """Test sync provider API key setup with exception."""
        with patch.object(ApiKeysService, '__init__', return_value=None):
            service_instance = ApiKeysService.__new__(ApiKeysService)
            service_instance.find_by_name_sync = MagicMock(side_effect=Exception("Database error"))
            
            with patch('src.services.api_keys_service.ApiKeysService', return_value=service_instance):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = ApiKeysService.setup_provider_api_key_sync(mock_sync_session, "ERROR_KEY")
                    
                    assert result is False
                    mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_openai_api_key_success(self):
        """Test successful OpenAI API key setup."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value="openai-key"):
            with patch('src.core.unit_of_work.UnitOfWork'):
                result = await ApiKeysService.setup_openai_api_key()
                
                assert result is True
                assert os.environ.get("OPENAI_API_KEY") == "openai-key"
    
    @pytest.mark.asyncio
    async def test_setup_openai_api_key_not_found(self):
        """Test OpenAI API key setup when key not found."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value=None):
            with patch('src.core.unit_of_work.UnitOfWork'):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = await ApiKeysService.setup_openai_api_key()
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_openai_api_key_exception(self):
        """Test OpenAI API key setup with exception."""
        with patch.object(ApiKeysService, 'get_provider_api_key', side_effect=Exception("Database error")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.setup_openai_api_key()
                
                assert result is False
                mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_all_api_keys_async(self):
        """Test setup all API keys with async methods."""
        with patch.object(ApiKeysService, 'setup_openai_api_key', return_value=True) as mock_openai:
            with patch.object(ApiKeysService, 'setup_anthropic_api_key', return_value=True) as mock_anthropic:
                with patch.object(ApiKeysService, 'setup_deepseek_api_key', return_value=True) as mock_deepseek:
                    with patch.object(ApiKeysService, 'setup_gemini_api_key', return_value=True) as mock_gemini:
                        await ApiKeysService.setup_all_api_keys()
                        
                        mock_openai.assert_called_once()
                        mock_anthropic.assert_called_once()
                        mock_deepseek.assert_called_once()
                        mock_gemini.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_all_api_keys_sync(self, mock_sync_session):
        """Test setup all API keys with sync session."""
        with patch.object(ApiKeysService, 'setup_provider_api_key_sync', return_value=True) as mock_sync_setup:
            await ApiKeysService.setup_all_api_keys(mock_sync_session)
            
            assert mock_sync_setup.call_count == 4
            calls = mock_sync_setup.call_args_list
            key_names = [call[0][1] for call in calls]
            assert "OPENAI_API_KEY" in key_names
            assert "ANTHROPIC_API_KEY" in key_names
            assert "DEEPSEEK_API_KEY" in key_names
            assert "GEMINI_API_KEY" in key_names
    
    @pytest.mark.asyncio
    async def test_setup_anthropic_api_key_success(self):
        """Test successful Anthropic API key setup."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value="anthropic-key"):
            with patch('src.core.unit_of_work.UnitOfWork'):
                result = await ApiKeysService.setup_anthropic_api_key()
                
                assert result is True
                assert os.environ.get("ANTHROPIC_API_KEY") == "anthropic-key"
    
    @pytest.mark.asyncio
    async def test_setup_anthropic_api_key_not_found(self):
        """Test Anthropic API key setup when key not found."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value=None):
            with patch('src.core.unit_of_work.UnitOfWork'):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = await ApiKeysService.setup_anthropic_api_key()
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_anthropic_api_key_exception(self):
        """Test Anthropic API key setup with exception."""
        with patch.object(ApiKeysService, 'get_provider_api_key', side_effect=Exception("Database error")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.setup_anthropic_api_key()
                
                assert result is False
                mock_logger.error.assert_called_once()
                
    @pytest.mark.asyncio
    async def test_setup_deepseek_api_key_success(self):
        """Test successful DeepSeek API key setup."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value="deepseek-key"):
            with patch('src.core.unit_of_work.UnitOfWork'):
                result = await ApiKeysService.setup_deepseek_api_key()
                
                assert result is True
                assert os.environ.get("DEEPSEEK_API_KEY") == "deepseek-key"
    
    @pytest.mark.asyncio
    async def test_setup_deepseek_api_key_not_found(self):
        """Test DeepSeek API key setup when key not found."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value=None):
            with patch('src.core.unit_of_work.UnitOfWork'):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = await ApiKeysService.setup_deepseek_api_key()
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_deepseek_api_key_exception(self):
        """Test DeepSeek API key setup with exception."""
        with patch.object(ApiKeysService, 'get_provider_api_key', side_effect=Exception("Database error")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.setup_deepseek_api_key()
                
                assert result is False
                mock_logger.error.assert_called_once()
                
    @pytest.mark.asyncio
    async def test_setup_gemini_api_key_success(self):
        """Test successful Gemini API key setup."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value="gemini-key"):
            with patch('src.core.unit_of_work.UnitOfWork'):
                result = await ApiKeysService.setup_gemini_api_key()
                
                assert result is True
                assert os.environ.get("GEMINI_API_KEY") == "gemini-key"
    
    @pytest.mark.asyncio
    async def test_setup_gemini_api_key_not_found(self):
        """Test Gemini API key setup when key not found."""
        with patch.object(ApiKeysService, 'get_provider_api_key', return_value=None):
            with patch('src.core.unit_of_work.UnitOfWork'):
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = await ApiKeysService.setup_gemini_api_key()
                    
                    assert result is False
                    mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_gemini_api_key_exception(self):
        """Test Gemini API key setup with exception."""
        with patch.object(ApiKeysService, 'get_provider_api_key', side_effect=Exception("Database error")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.setup_gemini_api_key()
                
                assert result is False
                mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_success(self):
        """Test successful provider API key retrieval."""
        mock_api_key = MockApiKey(name="OPENAI_API_KEY", encrypted_value="encrypted")
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = mock_api_key
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', return_value="decrypted"):
                    result = await ApiKeysService.get_provider_api_key("openai")
                    
                    assert result == "decrypted"
                    mock_service.find_by_name.assert_called_once_with("OPENAI_API_KEY")
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_not_found(self):
        """Test provider API key retrieval when key not found."""
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = None
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.logger') as mock_logger:
                    result = await ApiKeysService.get_provider_api_key("nonexistent")
                    
                    assert result is None
                    mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_decryption_error(self):
        """Test provider API key retrieval with decryption error."""
        mock_api_key = MockApiKey(name="PROVIDER_KEY", encrypted_value="bad_encrypted")
        
        with patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            with patch.object(ApiKeysService, 'from_unit_of_work') as mock_from_uow:
                mock_service = AsyncMock()
                mock_service.find_by_name.return_value = mock_api_key
                mock_from_uow.return_value = mock_service
                
                with patch('src.services.api_keys_service.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
                    with patch('src.services.api_keys_service.logger') as mock_logger:
                        result = await ApiKeysService.get_provider_api_key("provider")
                        
                        assert result is None
                        mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_general_exception(self):
        """Test provider API key retrieval with general exception."""
        with patch('src.core.unit_of_work.UnitOfWork', side_effect=Exception("Database connection failed")):
            with patch('src.services.api_keys_service.logger') as mock_logger:
                result = await ApiKeysService.get_provider_api_key("provider")
                
                assert result is None
                mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work(self):
        """Test creating service from UnitOfWork."""
        mock_uow = AsyncMock()
        mock_uow.api_key_repository = AsyncMock()
        
        result = await ApiKeysService.from_unit_of_work(mock_uow)
        
        assert isinstance(result, ApiKeysService)
        assert result.repository == mock_uow.api_key_repository