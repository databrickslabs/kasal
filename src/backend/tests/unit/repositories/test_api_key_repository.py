"""
Unit tests for ApiKeyRepository.

Tests the functionality of API key repository including
CRUD operations, name-based queries, and encryption handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy import select

from src.repositories.api_key_repository import ApiKeyRepository
from src.models.api_key import ApiKey


# Mock API key model
class MockApiKey:
    def __init__(self, id="key-123", name="TEST_API_KEY", encrypted_value="encrypted_test_value",
                 description="Test API Key", created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.encrypted_value = encrypted_value
        self.description = description
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    return MagicMock()


@pytest.fixture
def api_key_repository_async(mock_async_session):
    """Create an API key repository with async session."""
    return ApiKeyRepository(session=mock_async_session)


@pytest.fixture
def api_key_repository_sync():
    """Create an API key repository with sync session."""
    # Create a proper mock sync session
    mock_sync_session = MagicMock(spec=Session)
    repo = ApiKeyRepository.__new__(ApiKeyRepository)
    repo.model = ApiKey
    repo.session = mock_sync_session
    return repo


@pytest.fixture
def sample_api_keys():
    """Create sample API keys for testing."""
    return [
        MockApiKey(id="key-1", name="OPENAI_API_KEY", encrypted_value="encrypted_openai"),
        MockApiKey(id="key-2", name="ANTHROPIC_API_KEY", encrypted_value="encrypted_anthropic"),
        MockApiKey(id="key-3", name="CUSTOM_KEY", encrypted_value="encrypted_custom")
    ]


class TestApiKeyRepositoryInit:
    """Test cases for ApiKeyRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ApiKeyRepository(session=mock_async_session)
        
        assert repository.session == mock_async_session
        assert repository.model == ApiKey


class TestApiKeyRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, api_key_repository_async, mock_async_session):
        """Test successful find by name."""
        api_key = MockApiKey(name="OPENAI_API_KEY")
        mock_result = MockResult([api_key])
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_by_name("OPENAI_API_KEY")
        
        assert result == api_key
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(ApiKey)))
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, api_key_repository_async, mock_async_session):
        """Test find by name when key not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_by_name("NONEXISTENT_KEY")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_multiple_keys_returns_first(self, api_key_repository_async, mock_async_session):
        """Test find by name returns first result when multiple exist."""
        key1 = MockApiKey(id="key-1", name="SAME_NAME")
        key2 = MockApiKey(id="key-2", name="SAME_NAME")
        mock_result = MockResult([key1, key2])
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_by_name("SAME_NAME")
        
        assert result == key1
        mock_async_session.execute.assert_called_once()


class TestApiKeyRepositoryFindByNameSync:
    """Test cases for find_by_name_sync method."""
    
    def test_find_by_name_sync_success(self, api_key_repository_sync):
        """Test successful sync find by name."""
        api_key = MockApiKey(name="SYNC_KEY")
        mock_result = MockResult([api_key])
        api_key_repository_sync.session.execute.return_value = mock_result
        
        result = api_key_repository_sync.find_by_name_sync("SYNC_KEY")
        
        assert result == api_key
        api_key_repository_sync.session.execute.assert_called_once()
    
    def test_find_by_name_sync_not_found(self, api_key_repository_sync):
        """Test sync find by name when key not found."""
        mock_result = MockResult([])
        api_key_repository_sync.session.execute.return_value = mock_result
        
        result = api_key_repository_sync.find_by_name_sync("NONEXISTENT")
        
        assert result is None
        api_key_repository_sync.session.execute.assert_called_once()
    
    def test_find_by_name_sync_with_async_session_raises_error(self, api_key_repository_async):
        """Test sync method with async session raises TypeError."""
        with pytest.raises(TypeError, match="Session must be a synchronous SQLAlchemy Session"):
            api_key_repository_async.find_by_name_sync("TEST_KEY")


class TestApiKeyRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, api_key_repository_async, mock_async_session, sample_api_keys):
        """Test successful find all API keys."""
        mock_result = MockResult(sample_api_keys)
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_all()
        
        assert len(result) == 3
        assert result == sample_api_keys
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(ApiKey)))
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, api_key_repository_async, mock_async_session):
        """Test find all when no API keys exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_returns_list(self, api_key_repository_async, mock_async_session, sample_api_keys):
        """Test find all returns a list (not generator)."""
        mock_result = MockResult(sample_api_keys)
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_all()
        
        assert isinstance(result, list)
        assert len(result) == 3


class TestApiKeyRepositoryGetApiKeyValue:
    """Test cases for get_api_key_value method."""
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_success(self, api_key_repository_async):
        """Test successful API key value retrieval."""
        api_key = MockApiKey(name="TEST_KEY", encrypted_value="encrypted_value")
        
        with patch.object(api_key_repository_async, 'find_by_name', return_value=api_key):
            with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', return_value="decrypted_value"):
                result = await api_key_repository_async.get_api_key_value("TEST_KEY")
                
                assert result == "decrypted_value"
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_not_found(self, api_key_repository_async):
        """Test API key value retrieval when key not found."""
        with patch.object(api_key_repository_async, 'find_by_name', return_value=None):
            result = await api_key_repository_async.get_api_key_value("NONEXISTENT")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_decryption_error(self, api_key_repository_async):
        """Test API key value retrieval when decryption fails."""
        api_key = MockApiKey(name="BAD_KEY", encrypted_value="bad_encrypted_value")
        
        with patch.object(api_key_repository_async, 'find_by_name', return_value=api_key):
            with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', side_effect=Exception("Decryption failed")):
                result = await api_key_repository_async.get_api_key_value("BAD_KEY")
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_import_error(self, api_key_repository_async):
        """Test API key value retrieval when EncryptionUtils import fails."""
        api_key = MockApiKey(name="IMPORT_ERROR_KEY", encrypted_value="encrypted_value")
        
        # Mock the import to fail when EncryptionUtils is imported
        def mock_import(name, *args, **kwargs):
            if name == 'src.utils.encryption_utils':
                raise ImportError("Module not found")
            return __import__(name, *args, **kwargs)
        
        with patch.object(api_key_repository_async, 'find_by_name', return_value=api_key):
            with patch('builtins.__import__', side_effect=mock_import):
                result = await api_key_repository_async.get_api_key_value("IMPORT_ERROR_KEY")
                
                assert result is None


class TestApiKeyRepositoryGetProviderApiKey:
    """Test cases for get_provider_api_key method."""
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_success(self, api_key_repository_async):
        """Test successful provider API key retrieval."""
        with patch.object(api_key_repository_async, 'get_api_key_value', return_value="provider_key_value"):
            result = await api_key_repository_async.get_provider_api_key("openai")
            
            assert result == "provider_key_value"
            api_key_repository_async.get_api_key_value.assert_called_once_with("OPENAI_API_KEY")
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_not_found(self, api_key_repository_async):
        """Test provider API key retrieval when key not found."""
        with patch.object(api_key_repository_async, 'get_api_key_value', return_value=None):
            result = await api_key_repository_async.get_provider_api_key("nonexistent")
            
            assert result is None
            api_key_repository_async.get_api_key_value.assert_called_once_with("NONEXISTENT_API_KEY")
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_case_insensitive(self, api_key_repository_async):
        """Test provider API key handles lowercase provider names."""
        with patch.object(api_key_repository_async, 'get_api_key_value', return_value="anthropic_key"):
            result = await api_key_repository_async.get_provider_api_key("anthropic")
            
            assert result == "anthropic_key"
            api_key_repository_async.get_api_key_value.assert_called_once_with("ANTHROPIC_API_KEY")
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_mixed_case(self, api_key_repository_async):
        """Test provider API key handles mixed case provider names."""
        with patch.object(api_key_repository_async, 'get_api_key_value', return_value="databricks_key"):
            result = await api_key_repository_async.get_provider_api_key("DataBricks")
            
            assert result == "databricks_key"
            api_key_repository_async.get_api_key_value.assert_called_once_with("DATABRICKS_API_KEY")


class TestApiKeyRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_to_get_api_key_value_flow(self, api_key_repository_async, mock_async_session):
        """Test the flow from find_by_name to get_api_key_value."""
        api_key = MockApiKey(name="INTEGRATION_KEY", encrypted_value="encrypted_integration")
        mock_result = MockResult([api_key])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', return_value="decrypted_integration"):
            result = await api_key_repository_async.get_api_key_value("INTEGRATION_KEY")
            
            assert result == "decrypted_integration"
            mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_provider_to_find_by_name_flow(self, api_key_repository_async, mock_async_session):
        """Test the flow from get_provider_api_key to find_by_name."""
        api_key = MockApiKey(name="PROVIDER_API_KEY", encrypted_value="encrypted_provider")
        mock_result = MockResult([api_key])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', return_value="decrypted_provider"):
            result = await api_key_repository_async.get_provider_api_key("provider")
            
            assert result == "decrypted_provider"
            mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_with_actual_query_structure(self, api_key_repository_async, mock_async_session, sample_api_keys):
        """Test find_all with verification of query structure."""
        mock_result = MockResult(sample_api_keys)
        mock_async_session.execute.return_value = mock_result
        
        result = await api_key_repository_async.find_all()
        
        assert len(result) == 3
        assert all(isinstance(key, MockApiKey) for key in result)
        # Verify that execute was called with a select statement
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Basic check that it's a SQL query


class TestApiKeyRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_session_error(self, api_key_repository_async, mock_async_session):
        """Test find by name when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Database connection error")
        
        with pytest.raises(Exception, match="Database connection error"):
            await api_key_repository_async.find_by_name("ERROR_KEY")
    
    @pytest.mark.asyncio
    async def test_find_all_session_error(self, api_key_repository_async, mock_async_session):
        """Test find all when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Database connection error")
        
        with pytest.raises(Exception, match="Database connection error"):
            await api_key_repository_async.find_all()
    
    def test_find_by_name_sync_type_checking(self, api_key_repository_async):
        """Test that find_by_name_sync properly checks session type."""
        # This should raise TypeError since we're using an async session
        with pytest.raises(TypeError, match="Session must be a synchronous SQLAlchemy Session"):
            api_key_repository_async.find_by_name_sync("TEST")
    
    @pytest.mark.asyncio
    async def test_get_api_key_value_with_none_encrypted_value(self, api_key_repository_async):
        """Test get_api_key_value when encrypted_value is None."""
        api_key = MockApiKey(name="NULL_VALUE_KEY", encrypted_value=None)

        with patch.object(api_key_repository_async, 'find_by_name', return_value=api_key):
            with patch('src.utils.encryption_utils.EncryptionUtils.decrypt_value', side_effect=Exception("Cannot decrypt None")):
                result = await api_key_repository_async.get_api_key_value("NULL_VALUE_KEY")

                assert result is None


# ============================================================================
# find_by_name_sync / find_all / get_api_key_value / delete — additional
# branch coverage using plain mocks (async vs sync session, decrypt failure,
# delete exception/not-found/success paths).
# ============================================================================

class _GroupScopedApiKey:
    """Lightweight mock distinct from MockApiKey above — this module's
    find_all/find_by_name_sync branches key off group_id, which MockApiKey
    does not model."""

    def __init__(self, id=1, name="OPENAI_API_KEY", encrypted_value="enc_val", group_id=None):
        self.id = id
        self.name = name
        self.encrypted_value = encrypted_value
        self.group_id = group_id


def _make_async_session():
    s = AsyncMock(spec=AsyncSession)
    s.execute = AsyncMock()
    s.flush = AsyncMock()
    s.rollback = AsyncMock()
    return s


def _make_sync_session():
    s = MagicMock(spec=Session)
    return s


def _make_scalars(items):
    scalars = MagicMock()
    scalars.first.return_value = items[0] if items else None
    scalars.all.return_value = items
    return scalars


def _make_result(items):
    result = MagicMock()
    result.scalars.return_value = _make_scalars(items)
    return result


def test_find_by_name_sync_raises_type_error_for_async():
    async_session = _make_async_session()
    repo = ApiKeyRepository(session=async_session)
    with pytest.raises(TypeError):
        repo.find_by_name_sync("OPENAI_API_KEY")


def test_find_by_name_sync_returns_key():
    sync_session = _make_sync_session()
    key = _GroupScopedApiKey()
    scalars = MagicMock()
    scalars.first.return_value = key
    result = MagicMock()
    result.scalars.return_value = scalars
    sync_session.execute.return_value = result
    repo = ApiKeyRepository(session=sync_session)
    found = repo.find_by_name_sync("OPENAI_API_KEY")
    assert found is key


def test_find_by_name_sync_with_group_id_not_found():
    sync_session = _make_sync_session()
    scalars = MagicMock()
    scalars.first.return_value = None
    result = MagicMock()
    result.scalars.return_value = scalars
    sync_session.execute.return_value = result
    repo = ApiKeyRepository(session=sync_session)
    found = repo.find_by_name_sync("OPENAI_API_KEY", group_id="g1")
    assert found is None


@pytest.mark.asyncio
async def test_find_all_no_group():
    async_session = _make_async_session()
    keys = [_GroupScopedApiKey(id=1), _GroupScopedApiKey(id=2)]
    async_session.execute.return_value = _make_result(keys)
    repo = ApiKeyRepository(session=async_session)
    result = await repo.find_all()
    assert len(result) == 2


@pytest.mark.asyncio
async def test_find_all_with_group():
    async_session = _make_async_session()
    keys = [_GroupScopedApiKey(id=1, group_id="g1")]
    async_session.execute.return_value = _make_result(keys)
    repo = ApiKeyRepository(session=async_session)
    result = await repo.find_all(group_id="g1")
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_api_key_value_returns_none_when_not_found():
    async_session = _make_async_session()
    async_session.execute.return_value = _make_result([])
    repo = ApiKeyRepository(session=async_session)
    result = await repo.get_api_key_value("MISSING_KEY")
    assert result is None


@pytest.mark.asyncio
async def test_get_api_key_value_decryption_success():
    async_session = _make_async_session()
    key = _GroupScopedApiKey(encrypted_value="encrypted_123")
    async_session.execute.return_value = _make_result([key])
    repo = ApiKeyRepository(session=async_session)

    # EncryptionUtils is imported locally inside the method
    mock_encryption = MagicMock()
    mock_encryption.decrypt_value.return_value = "decrypted_value"
    with patch.dict('sys.modules', {
        'src.utils.encryption_utils': MagicMock(EncryptionUtils=mock_encryption)
    }):
        result = await repo.get_api_key_value("OPENAI_API_KEY")

    assert result == "decrypted_value"


@pytest.mark.asyncio
async def test_get_api_key_value_decryption_fails_returns_none():
    async_session = _make_async_session()
    key = _GroupScopedApiKey(encrypted_value="bad_enc")
    async_session.execute.return_value = _make_result([key])
    repo = ApiKeyRepository(session=async_session)

    mock_encryption = MagicMock()
    mock_encryption.decrypt_value.side_effect = Exception("decrypt error")
    with patch.dict('sys.modules', {
        'src.utils.encryption_utils': MagicMock(EncryptionUtils=mock_encryption)
    }):
        result = await repo.get_api_key_value("OPENAI_API_KEY")

    assert result is None


@pytest.mark.asyncio
async def test_delete_exception_raises_and_rolls_back():
    async_session = _make_async_session()
    async_session.execute.side_effect = Exception("DB error")
    repo = ApiKeyRepository(session=async_session)

    with pytest.raises(Exception, match="DB error"):
        await repo.delete(1)

    async_session.rollback.assert_called_once()


@pytest.mark.asyncio
async def test_delete_not_found_returns_false():
    async_session = _make_async_session()
    result_mock = MagicMock()
    result_mock.rowcount = 0
    async_session.execute.return_value = result_mock
    repo = ApiKeyRepository(session=async_session)

    result = await repo.delete(999)
    assert result is False


@pytest.mark.asyncio
async def test_delete_success_returns_true():
    async_session = _make_async_session()
    result_mock = MagicMock()
    result_mock.rowcount = 1
    async_session.execute.return_value = result_mock
    repo = ApiKeyRepository(session=async_session)

    result = await repo.delete(1)
    assert result is True