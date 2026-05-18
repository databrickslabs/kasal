"""
Coverage tests for repositories/api_key_repository.py
Covers: find_by_name_sync (lines 53-61), find_all (63-77),
get_api_key_value (79-98), delete exception (138-148)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.repositories.api_key_repository import ApiKeyRepository
from src.models.api_key import ApiKey


class MockApiKey:
    def __init__(self, id=1, name="OPENAI_API_KEY", encrypted_value="enc_val", group_id=None):
        self.id = id
        self.name = name
        self.encrypted_value = encrypted_value
        self.group_id = group_id


def make_async_session():
    s = AsyncMock(spec=AsyncSession)
    s.execute = AsyncMock()
    s.flush = AsyncMock()
    s.rollback = AsyncMock()
    return s


def make_sync_session():
    s = MagicMock(spec=Session)
    return s


def make_scalars(items):
    scalars = MagicMock()
    scalars.first.return_value = items[0] if items else None
    scalars.all.return_value = items
    return scalars


def make_result(items):
    result = MagicMock()
    result.scalars.return_value = make_scalars(items)
    return result


# ---- find_by_name_sync ----

def test_find_by_name_sync_raises_type_error_for_async():
    async_session = make_async_session()
    repo = ApiKeyRepository(session=async_session)
    with pytest.raises(TypeError):
        repo.find_by_name_sync("OPENAI_API_KEY")


def test_find_by_name_sync_success():
    sync_session = make_sync_session()
    key = MockApiKey()
    scalars = MagicMock()
    scalars.first.return_value = key
    result = MagicMock()
    result.scalars.return_value = scalars
    sync_session.execute.return_value = result
    repo = ApiKeyRepository(session=sync_session)
    found = repo.find_by_name_sync("OPENAI_API_KEY")
    assert found is key


def test_find_by_name_sync_with_group_id():
    sync_session = make_sync_session()
    scalars = MagicMock()
    scalars.first.return_value = None
    result = MagicMock()
    result.scalars.return_value = scalars
    sync_session.execute.return_value = result
    repo = ApiKeyRepository(session=sync_session)
    found = repo.find_by_name_sync("OPENAI_API_KEY", group_id="g1")
    assert found is None


# ---- find_all ----

@pytest.mark.asyncio
async def test_find_all_no_group():
    async_session = make_async_session()
    keys = [MockApiKey(id=1), MockApiKey(id=2)]
    async_session.execute.return_value = make_result(keys)
    repo = ApiKeyRepository(session=async_session)
    result = await repo.find_all()
    assert len(result) == 2


@pytest.mark.asyncio
async def test_find_all_with_group():
    async_session = make_async_session()
    keys = [MockApiKey(id=1, group_id="g1")]
    async_session.execute.return_value = make_result(keys)
    repo = ApiKeyRepository(session=async_session)
    result = await repo.find_all(group_id="g1")
    assert len(result) == 1


# ---- get_api_key_value ----

@pytest.mark.asyncio
async def test_get_api_key_value_not_found():
    async_session = make_async_session()
    async_session.execute.return_value = make_result([])
    repo = ApiKeyRepository(session=async_session)
    result = await repo.get_api_key_value("MISSING_KEY")
    assert result is None


@pytest.mark.asyncio
async def test_get_api_key_value_decryption_success():
    async_session = make_async_session()
    key = MockApiKey(encrypted_value="encrypted_123")
    async_session.execute.return_value = make_result([key])
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
async def test_get_api_key_value_decryption_fails():
    async_session = make_async_session()
    key = MockApiKey(encrypted_value="bad_enc")
    async_session.execute.return_value = make_result([key])
    repo = ApiKeyRepository(session=async_session)

    mock_encryption = MagicMock()
    mock_encryption.decrypt_value.side_effect = Exception("decrypt error")
    with patch.dict('sys.modules', {
        'src.utils.encryption_utils': MagicMock(EncryptionUtils=mock_encryption)
    }):
        result = await repo.get_api_key_value("OPENAI_API_KEY")

    assert result is None


# ---- delete (exception path) ----

@pytest.mark.asyncio
async def test_delete_exception_raises():
    async_session = make_async_session()
    async_session.execute.side_effect = Exception("DB error")
    repo = ApiKeyRepository(session=async_session)

    with pytest.raises(Exception, match="DB error"):
        await repo.delete(1)

    async_session.rollback.assert_called_once()


@pytest.mark.asyncio
async def test_delete_not_found():
    async_session = make_async_session()
    result_mock = MagicMock()
    result_mock.rowcount = 0
    async_session.execute.return_value = result_mock
    repo = ApiKeyRepository(session=async_session)

    result = await repo.delete(999)
    assert result is False


@pytest.mark.asyncio
async def test_delete_success():
    async_session = make_async_session()
    result_mock = MagicMock()
    result_mock.rowcount = 1
    async_session.execute.return_value = result_mock
    repo = ApiKeyRepository(session=async_session)

    result = await repo.delete(1)
    assert result is True
