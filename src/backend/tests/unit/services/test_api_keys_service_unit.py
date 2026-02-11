"""
Comprehensive unit tests for ApiKeysService.

Covers all public methods, branches, security checks, and error paths
in src/services/api_keys_service.py.
"""

import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from types import SimpleNamespace

from src.services.api_keys_service import ApiKeysService
from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate

# Save a direct reference to the real static method before any patches.
# This allows us to call it even when the ApiKeysService name is patched
# inside the module.
_real_setup_provider_api_key_sync = ApiKeysService.setup_provider_api_key_sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_api_key(**overrides):
    """Return a lightweight mock that behaves like an ApiKey ORM instance."""
    defaults = dict(
        id=1,
        name="OPENAI_API_KEY",
        encrypted_value="enc_value_123",
        description="test key",
        group_id="grp_1",
        created_by_email="user@example.com",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _build_service(group_id="grp_1"):
    """
    Construct an ApiKeysService with a mocked AsyncSession and
    a mocked repository, returning (service, mock_repo).
    """
    session = AsyncMock()
    with patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
         patch("src.services.api_keys_service.EncryptionUtils"):
        repo = AsyncMock()
        RepoClass.return_value = repo
        service = ApiKeysService(session, group_id=group_id)
        # Replace the repository with the mock we control
        service.repository = repo
    return service, repo


def _mock_async_session_factory():
    """
    Build a mock that works as an async context manager, mimicking
    ``async with async_session_factory() as session:``.
    Returns (factory_mock, session_mock) so callers can configure
    the session mock's repository behaviour.
    """
    mock_session = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    factory_mock = MagicMock(return_value=mock_ctx)
    return factory_mock, mock_session


# ===========================================================================
# __init__
# ===========================================================================

class TestInit:
    """Tests for the constructor."""

    def test_init_sets_attributes(self):
        session = AsyncMock()
        with patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils"):
            service = ApiKeysService(session, group_id="g1")

        assert service.group_id == "g1"
        assert service.is_async is True
        assert service.session is None
        RepoClass.assert_called_once_with(session)

    def test_init_default_group_id_is_none(self):
        session = AsyncMock()
        with patch("src.services.api_keys_service.ApiKeyRepository"), \
             patch("src.services.api_keys_service.EncryptionUtils"):
            service = ApiKeysService(session)

        assert service.group_id is None


# ===========================================================================
# find_by_name (async)
# ===========================================================================

class TestFindByName:
    """Tests for the async find_by_name method."""

    @pytest.mark.asyncio
    async def test_find_by_name_returns_key(self):
        service, repo = _build_service(group_id="grp_1")
        expected = _make_api_key()
        repo.find_by_name = AsyncMock(return_value=expected)

        result = await service.find_by_name("OPENAI_API_KEY")

        repo.find_by_name.assert_awaited_once_with("OPENAI_API_KEY", group_id="grp_1")
        assert result is expected

    @pytest.mark.asyncio
    async def test_find_by_name_returns_none_when_not_found(self):
        service, repo = _build_service(group_id="grp_1")
        repo.find_by_name = AsyncMock(return_value=None)

        result = await service.find_by_name("MISSING_KEY")
        assert result is None

    @pytest.mark.asyncio
    async def test_find_by_name_raises_when_no_group_id(self):
        """Security: group_id is mandatory for multi-tenant isolation."""
        service, _repo = _build_service(group_id=None)

        with pytest.raises(ValueError, match="SECURITY"):
            await service.find_by_name("OPENAI_API_KEY")

    @pytest.mark.asyncio
    async def test_find_by_name_delegates_to_sync_when_not_async(self):
        """When is_async is False, find_by_name should call find_by_name_sync."""
        service, repo = _build_service(group_id="grp_1")
        service.is_async = False
        # Provide a sync session to avoid TypeError in find_by_name_sync
        from sqlalchemy.orm import Session
        service.session = MagicMock(spec=Session)
        expected = _make_api_key()
        repo.find_by_name_sync = MagicMock(return_value=expected)

        result = await service.find_by_name("KEY")

        repo.find_by_name_sync.assert_called_once_with("KEY", group_id="grp_1")
        assert result is expected


# ===========================================================================
# find_by_name_sync
# ===========================================================================

class TestFindByNameSync:
    """Tests for the synchronous find_by_name_sync method."""

    def test_sync_requires_sync_session(self):
        service, _repo = _build_service(group_id="grp_1")
        # Default session on service is None which is not a Session
        with pytest.raises(TypeError, match="synchronous session"):
            service.find_by_name_sync("KEY")

    def test_sync_raises_when_no_group_id(self):
        from sqlalchemy.orm import Session
        service, _repo = _build_service(group_id=None)
        service.session = MagicMock(spec=Session)

        with pytest.raises(ValueError, match="SECURITY"):
            service.find_by_name_sync("KEY")

    def test_sync_returns_key_on_success(self):
        from sqlalchemy.orm import Session
        service, repo = _build_service(group_id="grp_1")
        service.session = MagicMock(spec=Session)
        expected = _make_api_key()
        repo.find_by_name_sync = MagicMock(return_value=expected)

        result = service.find_by_name_sync("OPENAI_API_KEY")

        repo.find_by_name_sync.assert_called_once_with("OPENAI_API_KEY", group_id="grp_1")
        assert result is expected

    def test_sync_returns_none_when_not_found(self):
        from sqlalchemy.orm import Session
        service, repo = _build_service(group_id="grp_1")
        service.session = MagicMock(spec=Session)
        repo.find_by_name_sync = MagicMock(return_value=None)

        result = service.find_by_name_sync("MISSING")
        assert result is None


# ===========================================================================
# create_api_key
# ===========================================================================

class TestCreateApiKey:
    """Tests for create_api_key."""

    @pytest.mark.asyncio
    async def test_creates_key_with_encrypted_value(self):
        service, repo = _build_service(group_id="grp_1")
        created = _make_api_key()
        repo.create = AsyncMock(return_value=created)

        data = ApiKeyCreate(name="OPENAI_API_KEY", value="sk-abc123", description="OpenAI")

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "encrypted_sk"
            result = await service.create_api_key(data, created_by_email="user@example.com")

        EU.encrypt_value.assert_called_once_with("sk-abc123")
        repo.create.assert_awaited_once()
        call_dict = repo.create.call_args[0][0]
        assert call_dict["name"] == "OPENAI_API_KEY"
        assert call_dict["encrypted_value"] == "encrypted_sk"
        assert call_dict["description"] == "OpenAI"
        assert call_dict["group_id"] == "grp_1"
        assert call_dict["created_by_email"] == "user@example.com"
        # The returned object should have the plaintext value set for the response
        assert result.value == "sk-abc123"

    @pytest.mark.asyncio
    async def test_creates_key_with_empty_description_when_none(self):
        service, repo = _build_service()
        created = _make_api_key()
        repo.create = AsyncMock(return_value=created)

        data = ApiKeyCreate(name="KEY", value="v1")  # description defaults to None

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "enc"
            await service.create_api_key(data)

        call_dict = repo.create.call_args[0][0]
        assert call_dict["description"] == ""

    @pytest.mark.asyncio
    async def test_creates_key_without_email(self):
        service, repo = _build_service()
        created = _make_api_key()
        repo.create = AsyncMock(return_value=created)
        data = ApiKeyCreate(name="K", value="v")

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "enc"
            await service.create_api_key(data)

        call_dict = repo.create.call_args[0][0]
        assert call_dict["created_by_email"] is None


# ===========================================================================
# update_api_key
# ===========================================================================

class TestUpdateApiKey:
    """Tests for update_api_key."""

    @pytest.mark.asyncio
    async def test_updates_existing_key(self):
        service, repo = _build_service(group_id="grp_1")
        existing = _make_api_key(id=42)
        repo.find_by_name = AsyncMock(return_value=existing)
        updated = _make_api_key(id=42)
        repo.update = AsyncMock(return_value=updated)

        data = ApiKeyUpdate(value="new-secret", description="updated desc")

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "enc_new"
            result = await service.update_api_key("OPENAI_API_KEY", data)

        EU.encrypt_value.assert_called_once_with("new-secret")
        repo.update.assert_awaited_once()
        update_id, update_dict = repo.update.call_args[0]
        assert update_id == 42
        assert update_dict["encrypted_value"] == "enc_new"
        assert update_dict["description"] == "updated desc"
        assert result.value == "new-secret"

    @pytest.mark.asyncio
    async def test_updates_key_without_description(self):
        """When description is None, the update dict should NOT include description."""
        service, repo = _build_service(group_id="grp_1")
        existing = _make_api_key(id=10)
        repo.find_by_name = AsyncMock(return_value=existing)
        updated_obj = _make_api_key(id=10)
        repo.update = AsyncMock(return_value=updated_obj)

        data = ApiKeyUpdate(value="new-val")  # description=None by default

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "enc"
            result = await service.update_api_key("KEY", data)

        _, update_dict = repo.update.call_args[0]
        assert "description" not in update_dict

    @pytest.mark.asyncio
    async def test_update_returns_none_when_key_not_found(self):
        service, repo = _build_service(group_id="grp_1")
        repo.find_by_name = AsyncMock(return_value=None)

        data = ApiKeyUpdate(value="val")
        result = await service.update_api_key("MISSING", data)

        assert result is None
        repo.update.assert_not_awaited()


# ===========================================================================
# delete_api_key
# ===========================================================================

class TestDeleteApiKey:
    """Tests for delete_api_key."""

    @pytest.mark.asyncio
    async def test_deletes_existing_key(self):
        service, repo = _build_service(group_id="grp_1")
        existing = _make_api_key(id=7)
        repo.find_by_name = AsyncMock(return_value=existing)
        repo.delete = AsyncMock(return_value=True)

        result = await service.delete_api_key("OPENAI_API_KEY")

        repo.delete.assert_awaited_once_with(7)
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_key_not_found(self):
        service, repo = _build_service(group_id="grp_1")
        repo.find_by_name = AsyncMock(return_value=None)

        result = await service.delete_api_key("MISSING")

        assert result is False
        repo.delete.assert_not_awaited()


# ===========================================================================
# get_all_api_keys
# ===========================================================================

class TestGetAllApiKeys:
    """Tests for get_all_api_keys."""

    @pytest.mark.asyncio
    async def test_returns_decrypted_keys(self):
        service, repo = _build_service(group_id="grp_1")
        key1 = _make_api_key(id=1, name="K1", encrypted_value="enc1")
        key2 = _make_api_key(id=2, name="K2", encrypted_value="enc2")
        repo.find_all = AsyncMock(return_value=[key1, key2])

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.decrypt_value.side_effect = lambda v: f"decrypted_{v}"
            result = await service.get_all_api_keys()

        repo.find_all.assert_awaited_once_with(group_id="grp_1")
        assert len(result) == 2
        assert result[0].value == "decrypted_enc1"
        assert result[1].value == "decrypted_enc2"

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_keys(self):
        service, repo = _build_service(group_id="grp_1")
        repo.find_all = AsyncMock(return_value=[])

        result = await service.get_all_api_keys()
        assert result == []

    @pytest.mark.asyncio
    async def test_sets_empty_value_on_decryption_failure(self):
        service, repo = _build_service(group_id="grp_1")
        key = _make_api_key(encrypted_value="bad_enc")
        repo.find_all = AsyncMock(return_value=[key])

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.decrypt_value.side_effect = Exception("decrypt fail")
            result = await service.get_all_api_keys()

        assert result[0].value == ""


# ===========================================================================
# get_api_keys_metadata
# ===========================================================================

class TestGetApiKeysMetadata:
    """Tests for get_api_keys_metadata."""

    @pytest.mark.asyncio
    async def test_returns_set_status_for_key_with_value(self):
        service, repo = _build_service(group_id="grp_1")
        key = _make_api_key(encrypted_value="some_encrypted_data")
        repo.find_all = AsyncMock(return_value=[key])

        result = await service.get_api_keys_metadata()

        assert result[0].value == "Set"

    @pytest.mark.asyncio
    async def test_returns_not_set_for_empty_encrypted_value(self):
        service, repo = _build_service(group_id="grp_1")
        key = _make_api_key(encrypted_value="")
        repo.find_all = AsyncMock(return_value=[key])

        result = await service.get_api_keys_metadata()

        assert result[0].value == "Not set"

    @pytest.mark.asyncio
    async def test_returns_not_set_for_none_encrypted_value(self):
        service, repo = _build_service(group_id="grp_1")
        key = _make_api_key(encrypted_value=None)
        repo.find_all = AsyncMock(return_value=[key])

        result = await service.get_api_keys_metadata()

        assert result[0].value == "Not set"

    @pytest.mark.asyncio
    async def test_returns_not_set_for_whitespace_encrypted_value(self):
        service, repo = _build_service(group_id="grp_1")
        key = _make_api_key(encrypted_value="   ")
        repo.find_all = AsyncMock(return_value=[key])

        result = await service.get_api_keys_metadata()

        assert result[0].value == "Not set"

    @pytest.mark.asyncio
    async def test_metadata_filters_by_group(self):
        service, repo = _build_service(group_id="grp_2")
        repo.find_all = AsyncMock(return_value=[])

        await service.get_api_keys_metadata()

        repo.find_all.assert_awaited_once_with(group_id="grp_2")


# ===========================================================================
# get_api_key_value (classmethod)
# ===========================================================================

class TestGetApiKeyValue:
    """Tests for the classmethod get_api_key_value."""

    @pytest.mark.asyncio
    async def test_returns_decrypted_value(self):
        fake_key = _make_api_key(encrypted_value="enc_val")
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=fake_key)
            RepoClass.return_value = repo_mock
            EU.decrypt_value.return_value = "plain_value"

            result = await ApiKeysService.get_api_key_value(
                key_name="OPENAI_API_KEY", group_id="grp_1"
            )

        assert result == "plain_value"

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.get_api_key_value(key_name="K", group_id=None)

    @pytest.mark.asyncio
    async def test_returns_none_when_key_not_found(self):
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils"):
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=None)
            RepoClass.return_value = repo_mock

            result = await ApiKeysService.get_api_key_value(
                key_name="MISSING", group_id="grp_1"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_decryption_error(self):
        fake_key = _make_api_key(encrypted_value="bad")
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=fake_key)
            RepoClass.return_value = repo_mock
            EU.decrypt_value.side_effect = Exception("bad cipher")

            result = await ApiKeysService.get_api_key_value(
                key_name="K", group_id="grp_1"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_handles_string_as_first_argument(self):
        """When db is passed as a string, it is treated as key_name."""
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            repo_mock = AsyncMock()
            fake_key = _make_api_key(encrypted_value="enc")
            repo_mock.find_by_name = AsyncMock(return_value=fake_key)
            RepoClass.return_value = repo_mock
            EU.decrypt_value.return_value = "decrypted"

            # Pass a string as the `db` positional argument
            result = await ApiKeysService.get_api_key_value(
                "MY_KEY_NAME", group_id="grp_1"
            )

        assert result == "decrypted"
        # The find_by_name should have been called with key_name="MY_KEY_NAME"
        repo_mock.find_by_name.assert_awaited_once_with("MY_KEY_NAME", group_id="grp_1")


# ===========================================================================
# setup_provider_api_key (classmethod, async)
# ===========================================================================

class TestSetupProviderApiKey:
    """Tests for the async classmethod setup_provider_api_key."""

    @pytest.mark.asyncio
    async def test_sets_env_var_on_success(self):
        with patch.object(
            ApiKeysService, "get_api_key_value", new_callable=AsyncMock, return_value="secret"
        ):
            result = await ApiKeysService.setup_provider_api_key(
                AsyncMock(), "MY_API_KEY"
            )

        assert result is True
        assert os.environ.get("MY_API_KEY") == "secret"
        # Clean up
        os.environ.pop("MY_API_KEY", None)

    @pytest.mark.asyncio
    async def test_returns_false_when_value_not_found(self):
        with patch.object(
            ApiKeysService, "get_api_key_value", new_callable=AsyncMock, return_value=None
        ):
            result = await ApiKeysService.setup_provider_api_key(
                AsyncMock(), "MISSING_KEY"
            )

        assert result is False


# ===========================================================================
# setup_provider_api_key_sync (staticmethod)
# ===========================================================================

class TestSetupProviderApiKeySync:
    """Tests for the sync staticmethod setup_provider_api_key_sync.

    The method internally creates ``ApiKeysService(db)`` which sets
    ``self.session = None``.  Since the ``find_by_name_sync`` check on
    ``self.session`` would then fail, we patch ``ApiKeysService`` inside
    the service module so the constructor returns a mock we control.
    We use the saved reference ``_real_setup_provider_api_key_sync``
    to call the real function while the module-level name is patched.
    """

    def test_sets_env_var_on_success(self):
        from sqlalchemy.orm import Session
        mock_session = MagicMock(spec=Session)

        fake_key = _make_api_key(encrypted_value="enc_val")
        mock_svc = MagicMock()
        mock_svc.find_by_name_sync.return_value = fake_key

        with patch("src.services.api_keys_service.ApiKeysService", return_value=mock_svc), \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.decrypt_value.return_value = "decrypted_secret"
            result = _real_setup_provider_api_key_sync(mock_session, "MY_KEY")

        assert result is True
        assert os.environ.get("MY_KEY") == "decrypted_secret"
        os.environ.pop("MY_KEY", None)

    def test_returns_false_when_key_not_found(self):
        from sqlalchemy.orm import Session
        mock_session = MagicMock(spec=Session)

        mock_svc = MagicMock()
        mock_svc.find_by_name_sync.return_value = None

        with patch("src.services.api_keys_service.ApiKeysService", return_value=mock_svc), \
             patch("src.services.api_keys_service.EncryptionUtils"):
            result = _real_setup_provider_api_key_sync(mock_session, "MISSING")

        assert result is False

    def test_returns_false_when_encrypted_value_is_none(self):
        from sqlalchemy.orm import Session
        mock_session = MagicMock(spec=Session)

        fake_key = _make_api_key(encrypted_value=None)
        mock_svc = MagicMock()
        mock_svc.find_by_name_sync.return_value = fake_key

        with patch("src.services.api_keys_service.ApiKeysService", return_value=mock_svc), \
             patch("src.services.api_keys_service.EncryptionUtils"):
            result = _real_setup_provider_api_key_sync(mock_session, "K")

        assert result is False

    def test_returns_false_on_exception(self):
        from sqlalchemy.orm import Session
        mock_session = MagicMock(spec=Session)

        mock_svc = MagicMock()
        mock_svc.find_by_name_sync.side_effect = Exception("db error")

        with patch("src.services.api_keys_service.ApiKeysService", return_value=mock_svc), \
             patch("src.services.api_keys_service.EncryptionUtils"):
            result = _real_setup_provider_api_key_sync(mock_session, "K")

        assert result is False


# ===========================================================================
# setup_openai_api_key
# ===========================================================================

class TestSetupOpenaiApiKey:
    """Tests for setup_openai_api_key."""

    @pytest.mark.asyncio
    async def test_sets_openai_env_var(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value="sk-openai"
        ):
            result = await ApiKeysService.setup_openai_api_key(group_id="grp_1")

        assert result is True
        assert os.environ.get("OPENAI_API_KEY") == "sk-openai"
        os.environ.pop("OPENAI_API_KEY", None)

    @pytest.mark.asyncio
    async def test_returns_false_when_no_key(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value=None
        ):
            result = await ApiKeysService.setup_openai_api_key(group_id="grp_1")

        assert result is False

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.setup_openai_api_key(group_id=None)

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, side_effect=Exception("network error")
        ):
            result = await ApiKeysService.setup_openai_api_key(group_id="grp_1")

        assert result is False


# ===========================================================================
# setup_anthropic_api_key
# ===========================================================================

class TestSetupAnthropicApiKey:
    """Tests for setup_anthropic_api_key."""

    @pytest.mark.asyncio
    async def test_sets_anthropic_env_var(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value="sk-ant"
        ):
            result = await ApiKeysService.setup_anthropic_api_key(group_id="grp_1")

        assert result is True
        assert os.environ.get("ANTHROPIC_API_KEY") == "sk-ant"
        os.environ.pop("ANTHROPIC_API_KEY", None)

    @pytest.mark.asyncio
    async def test_returns_false_when_no_key(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value=None
        ):
            result = await ApiKeysService.setup_anthropic_api_key(group_id="grp_1")

        assert result is False

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.setup_anthropic_api_key(group_id=None)

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, side_effect=Exception("fail")
        ):
            result = await ApiKeysService.setup_anthropic_api_key(group_id="grp_1")

        assert result is False


# ===========================================================================
# setup_deepseek_api_key
# ===========================================================================

class TestSetupDeepseekApiKey:
    """Tests for setup_deepseek_api_key."""

    @pytest.mark.asyncio
    async def test_sets_deepseek_env_var(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value="sk-ds"
        ):
            result = await ApiKeysService.setup_deepseek_api_key(group_id="grp_1")

        assert result is True
        assert os.environ.get("DEEPSEEK_API_KEY") == "sk-ds"
        os.environ.pop("DEEPSEEK_API_KEY", None)

    @pytest.mark.asyncio
    async def test_returns_false_when_no_key(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value=None
        ):
            result = await ApiKeysService.setup_deepseek_api_key(group_id="grp_1")

        assert result is False

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.setup_deepseek_api_key(group_id=None)

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, side_effect=Exception("fail")
        ):
            result = await ApiKeysService.setup_deepseek_api_key(group_id="grp_1")

        assert result is False


# ===========================================================================
# setup_gemini_api_key
# ===========================================================================

class TestSetupGeminiApiKey:
    """Tests for setup_gemini_api_key."""

    @pytest.mark.asyncio
    async def test_sets_gemini_env_var(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value="sk-gem"
        ):
            result = await ApiKeysService.setup_gemini_api_key(group_id="grp_1")

        assert result is True
        assert os.environ.get("GEMINI_API_KEY") == "sk-gem"
        os.environ.pop("GEMINI_API_KEY", None)

    @pytest.mark.asyncio
    async def test_returns_false_when_no_key(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, return_value=None
        ):
            result = await ApiKeysService.setup_gemini_api_key(group_id="grp_1")

        assert result is False

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.setup_gemini_api_key(group_id=None)

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        with patch.object(
            ApiKeysService, "get_provider_api_key",
            new_callable=AsyncMock, side_effect=Exception("fail")
        ):
            result = await ApiKeysService.setup_gemini_api_key(group_id="grp_1")

        assert result is False


# ===========================================================================
# setup_all_api_keys
# ===========================================================================

class TestSetupAllApiKeys:
    """Tests for setup_all_api_keys."""

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.setup_all_api_keys(group_id=None)

    @pytest.mark.asyncio
    async def test_calls_all_provider_setups_async(self):
        with patch.object(ApiKeysService, "setup_openai_api_key", new_callable=AsyncMock, return_value=True) as m_openai, \
             patch.object(ApiKeysService, "setup_anthropic_api_key", new_callable=AsyncMock, return_value=True) as m_ant, \
             patch.object(ApiKeysService, "setup_deepseek_api_key", new_callable=AsyncMock, return_value=True) as m_ds, \
             patch.object(ApiKeysService, "setup_gemini_api_key", new_callable=AsyncMock, return_value=True) as m_gem:

            await ApiKeysService.setup_all_api_keys(group_id="grp_1")

        m_openai.assert_awaited_once_with(group_id="grp_1")
        m_ant.assert_awaited_once_with(group_id="grp_1")
        m_ds.assert_awaited_once_with(group_id="grp_1")
        m_gem.assert_awaited_once_with(group_id="grp_1")

    @pytest.mark.asyncio
    async def test_sync_path_with_sync_session(self):
        """When a sync Session is provided, falls back to sync provider setup."""
        from sqlalchemy.orm import Session
        mock_sync_session = MagicMock(spec=Session)

        with patch.object(
            ApiKeysService, "setup_provider_api_key_sync"
        ) as mock_sync:
            await ApiKeysService.setup_all_api_keys(
                db=mock_sync_session, group_id="grp_1"
            )

        assert mock_sync.call_count == 4
        # Verify the key names that were set up
        called_key_names = [c.args[1] for c in mock_sync.call_args_list]
        assert "OPENAI_API_KEY" in called_key_names
        assert "ANTHROPIC_API_KEY" in called_key_names
        assert "DEEPSEEK_API_KEY" in called_key_names
        assert "GEMINI_API_KEY" in called_key_names

    @pytest.mark.asyncio
    async def test_async_path_when_db_is_none(self):
        """When db is None (not a sync Session), uses async path."""
        with patch.object(ApiKeysService, "setup_openai_api_key", new_callable=AsyncMock) as m_openai, \
             patch.object(ApiKeysService, "setup_anthropic_api_key", new_callable=AsyncMock) as m_ant, \
             patch.object(ApiKeysService, "setup_deepseek_api_key", new_callable=AsyncMock) as m_ds, \
             patch.object(ApiKeysService, "setup_gemini_api_key", new_callable=AsyncMock) as m_gem:

            await ApiKeysService.setup_all_api_keys(db=None, group_id="grp_1")

        m_openai.assert_awaited_once()
        m_ant.assert_awaited_once()
        m_ds.assert_awaited_once()
        m_gem.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_path_when_db_is_async_session(self):
        """When db is an AsyncSession (not a sync Session), uses async path."""
        async_session = AsyncMock()

        with patch.object(ApiKeysService, "setup_openai_api_key", new_callable=AsyncMock) as m_openai, \
             patch.object(ApiKeysService, "setup_anthropic_api_key", new_callable=AsyncMock), \
             patch.object(ApiKeysService, "setup_deepseek_api_key", new_callable=AsyncMock), \
             patch.object(ApiKeysService, "setup_gemini_api_key", new_callable=AsyncMock):

            await ApiKeysService.setup_all_api_keys(db=async_session, group_id="grp_1")

        m_openai.assert_awaited_once_with(group_id="grp_1")


# ===========================================================================
# get_provider_api_key (classmethod)
# ===========================================================================

class TestGetProviderApiKey:
    """Tests for the classmethod get_provider_api_key."""

    @pytest.mark.asyncio
    async def test_returns_decrypted_key(self):
        fake_key = _make_api_key(encrypted_value="enc_openai")
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=fake_key)
            RepoClass.return_value = repo_mock
            EU.decrypt_value.return_value = "sk-plain"

            result = await ApiKeysService.get_provider_api_key("openai", group_id="grp_1")

        assert result == "sk-plain"
        repo_mock.find_by_name.assert_awaited_once_with("OPENAI_API_KEY", group_id="grp_1")

    @pytest.mark.asyncio
    async def test_raises_when_no_group_id(self):
        with pytest.raises(ValueError, match="SECURITY"):
            await ApiKeysService.get_provider_api_key("openai", group_id=None)

    @pytest.mark.asyncio
    async def test_returns_none_when_key_not_found(self):
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils"):
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=None)
            RepoClass.return_value = repo_mock

            result = await ApiKeysService.get_provider_api_key("openai", group_id="grp_1")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_decryption_error(self):
        fake_key = _make_api_key(encrypted_value="bad")
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils") as EU:
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=fake_key)
            RepoClass.return_value = repo_mock
            EU.decrypt_value.side_effect = Exception("cipher error")

            result = await ApiKeysService.get_provider_api_key("openai", group_id="grp_1")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_outer_exception(self):
        """Test the outer try/except that catches session factory errors."""
        factory_mock = MagicMock(side_effect=Exception("session factory broke"))

        with patch("src.db.session.async_session_factory", factory_mock):
            result = await ApiKeysService.get_provider_api_key("openai", group_id="grp_1")

        assert result is None

    @pytest.mark.asyncio
    async def test_uppercases_provider_name(self):
        """Verify provider name is uppercased when building key_name."""
        factory_mock, mock_session = _mock_async_session_factory()

        with patch("src.db.session.async_session_factory", factory_mock), \
             patch("src.services.api_keys_service.ApiKeyRepository") as RepoClass, \
             patch("src.services.api_keys_service.EncryptionUtils"):
            repo_mock = AsyncMock()
            repo_mock.find_by_name = AsyncMock(return_value=None)
            RepoClass.return_value = repo_mock

            await ApiKeysService.get_provider_api_key("deepseek", group_id="g1")

        repo_mock.find_by_name.assert_awaited_once_with("DEEPSEEK_API_KEY", group_id="g1")


# ===========================================================================
# Edge cases / cross-cutting
# ===========================================================================

class TestEdgeCases:
    """Cross-cutting edge case tests."""

    @pytest.mark.asyncio
    async def test_create_then_update_preserves_group(self):
        """Verify group_id flows through create and update paths correctly."""
        service, repo = _build_service(group_id="tenant_A")
        created = _make_api_key(id=99, group_id="tenant_A")
        repo.create = AsyncMock(return_value=created)
        repo.find_by_name = AsyncMock(return_value=created)
        repo.update = AsyncMock(return_value=created)

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.encrypt_value.return_value = "enc"
            await service.create_api_key(ApiKeyCreate(name="K", value="v"))
            create_dict = repo.create.call_args[0][0]
            assert create_dict["group_id"] == "tenant_A"

            await service.update_api_key("K", ApiKeyUpdate(value="v2"))
            repo.find_by_name.assert_awaited_with("K", group_id="tenant_A")

    @pytest.mark.asyncio
    async def test_multiple_keys_in_get_all(self):
        """get_all_api_keys handles many keys without issue."""
        service, repo = _build_service(group_id="grp_1")
        keys = [_make_api_key(id=i, name=f"K{i}", encrypted_value=f"e{i}") for i in range(50)]
        repo.find_all = AsyncMock(return_value=keys)

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.decrypt_value.side_effect = lambda v: f"d_{v}"
            result = await service.get_all_api_keys()

        assert len(result) == 50
        assert result[0].value == "d_e0"
        assert result[49].value == "d_e49"

    @pytest.mark.asyncio
    async def test_mixed_decryption_failures_in_get_all(self):
        """Some keys decrypt fine, some fail -- service continues."""
        service, repo = _build_service(group_id="grp_1")
        keys = [
            _make_api_key(id=1, name="OK", encrypted_value="good"),
            _make_api_key(id=2, name="BAD", encrypted_value="bad"),
            _make_api_key(id=3, name="OK2", encrypted_value="good2"),
        ]
        repo.find_all = AsyncMock(return_value=keys)

        def _decrypt(val):
            if val == "bad":
                raise Exception("corrupt")
            return f"plain_{val}"

        with patch("src.services.api_keys_service.EncryptionUtils") as EU:
            EU.decrypt_value.side_effect = _decrypt
            result = await service.get_all_api_keys()

        assert result[0].value == "plain_good"
        assert result[1].value == ""  # failed decryption
        assert result[2].value == "plain_good2"

    @pytest.mark.asyncio
    async def test_metadata_with_mixed_values(self):
        """Metadata correctly distinguishes set vs not-set keys."""
        service, repo = _build_service(group_id="grp_1")
        keys = [
            _make_api_key(id=1, encrypted_value="has_value"),
            _make_api_key(id=2, encrypted_value=""),
            _make_api_key(id=3, encrypted_value=None),
            _make_api_key(id=4, encrypted_value="  "),
        ]
        repo.find_all = AsyncMock(return_value=keys)

        result = await service.get_api_keys_metadata()

        assert result[0].value == "Set"
        assert result[1].value == "Not set"
        assert result[2].value == "Not set"
        assert result[3].value == "Not set"

    @pytest.mark.asyncio
    async def test_delete_then_find_returns_false(self):
        """After deleting a key, finding it should return None."""
        service, repo = _build_service(group_id="grp_1")
        existing = _make_api_key(id=5)
        # First call returns the key (for delete), second returns None (for find)
        repo.find_by_name = AsyncMock(side_effect=[existing, None])
        repo.delete = AsyncMock(return_value=True)

        deleted = await service.delete_api_key("K")
        assert deleted is True

        found = await service.find_by_name("K")
        assert found is None
