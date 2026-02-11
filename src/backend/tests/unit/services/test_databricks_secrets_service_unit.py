"""
Comprehensive unit tests for DatabricksSecretsService.

Covers all methods, branches, error paths, and edge cases:
  - __init__ and dependency injection
  - Lazy property databricks_service
  - set_databricks_service / set_api_keys_service
  - validate_databricks_config
  - get_databricks_secrets
  - get_databricks_secret_value
  - set_databricks_secret_value
  - delete_databricks_secret
  - create_databricks_secret_scope
  - set_databricks_token
  - setup_provider_api_key (classmethod)
  - _setup_provider_api_key_sync (staticmethod)
  - get_personal_access_token
  - get_provider_api_key
  - get_all_databricks_tokens

IMPORTANT patching notes:
  - DatabricksService, ApiKeysService, UserContext, and get_auth_context are
    imported LOCALLY inside methods (not at module top level). Therefore we
    must patch them at their *source* module, not on the secrets service module.
  - aiohttp IS imported at module level, so we patch it on the service module.
"""

import base64
import os
import sys
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.databricks_secrets_service import DatabricksSecretsService

# Patch paths for locally-imported dependencies (imported inside methods)
_PATCH_DS = "src.services.databricks_service.DatabricksService"
_PATCH_AUTH = "src.utils.databricks_auth.get_auth_context"
_PATCH_AKS = "src.services.api_keys_service.ApiKeysService"
_PATCH_UC = "src.utils.user_context.UserContext"
# aiohttp is imported at module top level, so patch on the service module
_PATCH_AIOHTTP_SESSION = "src.services.databricks_secrets_service.aiohttp.ClientSession"


# ---------------------------------------------------------------------------
# Helper: build a DatabricksSecretsService with mocked internals
# ---------------------------------------------------------------------------

def _make_service(group_id=None):
    """Return a DatabricksSecretsService with mocked repository."""
    with patch(
        "src.services.databricks_secrets_service.DatabricksConfigRepository"
    ) as MockRepo:
        MockRepo.return_value = MagicMock()
        service = DatabricksSecretsService(session=MagicMock(), group_id=group_id)
    return service


def _make_config(is_enabled=True, workspace_url="https://example.com", secret_scope="my-scope"):
    """Return a mock Databricks config object."""
    return SimpleNamespace(
        is_enabled=is_enabled,
        workspace_url=workspace_url,
        secret_scope=secret_scope,
    )


def _make_auth_context(token="fake-token"):
    """Return a mock auth context."""
    return SimpleNamespace(token=token)


def _mock_aiohttp_response(status, json_data=None, text_data=""):
    """Build a mock aiohttp response with async helpers."""
    response = AsyncMock()
    response.status = status
    if json_data is not None:
        response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value=text_data)
    return response


def _mock_aiohttp_session(response):
    """
    Build a mock aiohttp.ClientSession context manager chain:
        async with aiohttp.ClientSession() as session:
            async with session.post(...) as resp:
    """
    inner_cm = AsyncMock()
    inner_cm.__aenter__ = AsyncMock(return_value=response)
    inner_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=inner_cm)

    outer_cm = AsyncMock()
    outer_cm.__aenter__ = AsyncMock(return_value=mock_session)
    outer_cm.__aexit__ = AsyncMock(return_value=False)

    return outer_cm


# ===========================================================================
# Tests: Initialization and Dependency Injection
# ===========================================================================


class TestInit:
    """Tests for __init__ and initial attribute setup."""

    def test_init_defaults(self):
        service = _make_service()
        assert service.session is not None
        assert service.group_id is None
        assert service.api_keys_service is None
        assert service._databricks_service is None

    def test_init_with_group_id(self):
        service = _make_service(group_id="grp-123")
        assert service.group_id == "grp-123"

    def test_databricks_repository_is_set(self):
        service = _make_service()
        assert service.databricks_repository is not None


class TestLazyDatabricksService:
    """Tests for the lazy-loaded databricks_service property."""

    def test_lazy_creates_instance(self):
        service = _make_service()
        with patch(_PATCH_DS) as MockDS:
            mock_ds_instance = MagicMock()
            MockDS.return_value = mock_ds_instance

            result = service.databricks_service

            MockDS.assert_called_once_with(service.session)
            assert result is mock_ds_instance

    def test_lazy_returns_cached(self):
        service = _make_service()
        sentinel = MagicMock()
        service._databricks_service = sentinel

        assert service.databricks_service is sentinel

    def test_set_databricks_service_when_already_set(self):
        """set_databricks_service should not overwrite an existing instance
        because the property check sees it is not None."""
        service = _make_service()
        # Pre-set so property returns non-None
        existing = MagicMock()
        service._databricks_service = existing

        new_one = MagicMock()
        service.set_databricks_service(new_one)
        # hasattr + property check -> property returns existing (not None) -> no-op
        assert service._databricks_service is existing

    def test_set_api_keys_service(self):
        service = _make_service()
        mock_aks = MagicMock()
        service.set_api_keys_service(mock_aks)
        assert service.api_keys_service is mock_aks


# ===========================================================================
# Tests: validate_databricks_config
# ===========================================================================


class TestValidateDatabricksConfig:
    """Tests for validate_databricks_config."""

    @pytest.mark.asyncio
    async def test_returns_url_and_scope(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        url, scope = await service.validate_databricks_config()
        assert url == "https://example.com"
        assert scope == "my-scope"

    @pytest.mark.asyncio
    async def test_raises_when_no_config(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=None)
        service._databricks_service = mock_ds

        with pytest.raises(ValueError, match="not found"):
            await service.validate_databricks_config()

    @pytest.mark.asyncio
    async def test_raises_when_disabled(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(is_enabled=False)
        )
        service._databricks_service = mock_ds

        with pytest.raises(ValueError, match="disabled"):
            await service.validate_databricks_config()

    @pytest.mark.asyncio
    async def test_default_workspace_url_when_none(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(workspace_url=None)
        )
        service._databricks_service = mock_ds

        url, scope = await service.validate_databricks_config()
        assert url == ""
        assert scope == "my-scope"

    @pytest.mark.asyncio
    async def test_lazy_import_when_service_none(self):
        """When _databricks_service is None, the property triggers lazy import."""
        service = _make_service()
        with patch(_PATCH_DS) as MockDS:
            mock_ds_instance = AsyncMock()
            mock_ds_instance.get_databricks_config = AsyncMock(
                return_value=_make_config()
            )
            MockDS.return_value = mock_ds_instance

            url, scope = await service.validate_databricks_config()
            assert url == "https://example.com"


# ===========================================================================
# Tests: get_databricks_secrets
# ===========================================================================


class TestGetDatabricksSecrets:
    """Tests for get_databricks_secrets."""

    @pytest.mark.asyncio
    async def test_returns_secrets_with_values(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(
            200, json_data={"secrets": [{"key": "MY_KEY"}]}
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            # Mock get_databricks_secret_value for the nested calls
            service.get_databricks_secret_value = AsyncMock(return_value="secret-val")
            result = await service.get_databricks_secrets("my-scope")

        assert len(result) == 1
        assert result[0]["name"] == "MY_KEY"
        assert result[0]["value"] == "secret-val"
        assert result[0]["scope"] == "my-scope"
        assert result[0]["source"] == "databricks"

    @pytest.mark.asyncio
    async def test_returns_empty_when_config_missing(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=None)
        service._databricks_service = mock_ds

        result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_disabled(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(is_enabled=False)
        )
        service._databricks_service = mock_ds

        result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_workspace_url(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(workspace_url="")
        )
        service._databricks_service = mock_ds

        result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_auth_fails(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        with patch(_PATCH_AUTH, new_callable=AsyncMock, return_value=None):
            result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_api_error(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(403, text_data="Forbidden")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(side_effect=RuntimeError("boom"))
        service._databricks_service = mock_ds

        result = await service.get_databricks_secrets("scope")
        assert result == []

    @pytest.mark.asyncio
    async def test_multiple_secrets_returned(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(
            200,
            json_data={
                "secrets": [
                    {"key": "KEY_A"},
                    {"key": "KEY_B"},
                    {"key": "KEY_C"},
                ]
            },
        )
        mock_session = _mock_aiohttp_session(api_response)

        values = {"KEY_A": "val-a", "KEY_B": "val-b", "KEY_C": "val-c"}

        async def mock_get_value(scope, key):
            return values.get(key, "")

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            service.get_databricks_secret_value = AsyncMock(side_effect=mock_get_value)
            result = await service.get_databricks_secrets("scope")

        assert len(result) == 3
        names = [s["name"] for s in result]
        assert "KEY_A" in names
        assert "KEY_B" in names
        assert "KEY_C" in names


# ===========================================================================
# Tests: get_databricks_secret_value
# ===========================================================================


class TestGetDatabricksSecretValue:
    """Tests for get_databricks_secret_value."""

    @pytest.mark.asyncio
    async def test_returns_decoded_base64_value(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        encoded = base64.b64encode(b"my-secret").decode("utf-8")
        api_response = _mock_aiohttp_response(200, json_data={"value": encoded})
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secret_value("scope", "key")

        assert result == "my-secret"

    @pytest.mark.asyncio
    async def test_returns_raw_value_when_base64_decode_fails(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        # Not valid base64
        api_response = _mock_aiohttp_response(
            200, json_data={"value": "not-base64!!!"}
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secret_value("scope", "key")

        assert result == "not-base64!!!"

    @pytest.mark.asyncio
    async def test_returns_empty_when_config_invalid(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=None)
        service._databricks_service = mock_ds

        result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_when_disabled(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(is_enabled=False)
        )
        service._databricks_service = mock_ds

        result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_workspace_url(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(workspace_url="")
        )
        service._databricks_service = mock_ds

        result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_when_auth_fails(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        with patch(_PATCH_AUTH, new_callable=AsyncMock, return_value=None):
            result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_on_api_error(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(500, text_data="Server Error")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(side_effect=RuntimeError("oops"))
        service._databricks_service = mock_ds

        result = await service.get_databricks_secret_value("scope", "key")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_string_when_value_empty(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(200, json_data={"value": ""})
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secret_value("scope", "key")
        # base64.b64decode("") returns b"", decoded to ""
        assert result == ""


# ===========================================================================
# Tests: set_databricks_secret_value
# ===========================================================================


class TestSetDatabricksSecretValue:
    """Tests for set_databricks_secret_value."""

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds
        service.create_databricks_secret_scope = AsyncMock(return_value=True)

        api_response = _mock_aiohttp_response(200)
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.set_databricks_secret_value("scope", "key", "value")

        assert result is True
        service.create_databricks_secret_scope.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_config_invalid(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=None)
        service._databricks_service = mock_ds

        result = await service.set_databricks_secret_value("scope", "key", "value")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_disabled(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(is_enabled=False)
        )
        service._databricks_service = mock_ds

        result = await service.set_databricks_secret_value("scope", "key", "value")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_workspace_url(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(workspace_url="")
        )
        service._databricks_service = mock_ds

        result = await service.set_databricks_secret_value("scope", "key", "value")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_auth_fails(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        with patch(_PATCH_AUTH, new_callable=AsyncMock, return_value=None):
            result = await service.set_databricks_secret_value(
                "scope", "key", "value"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_api_error(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds
        service.create_databricks_secret_scope = AsyncMock(return_value=True)

        api_response = _mock_aiohttp_response(500, text_data="Error")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.set_databricks_secret_value(
                "scope", "key", "value"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(side_effect=RuntimeError("err"))
        service._databricks_service = mock_ds

        result = await service.set_databricks_secret_value("scope", "key", "value")
        assert result is False


# ===========================================================================
# Tests: delete_databricks_secret
# ===========================================================================


class TestDeleteDatabricksSecret:
    """Tests for delete_databricks_secret."""

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(200)
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.delete_databricks_secret("scope", "key")

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_config_invalid(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=None)
        service._databricks_service = mock_ds

        result = await service.delete_databricks_secret("scope", "key")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_disabled(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(is_enabled=False)
        )
        service._databricks_service = mock_ds

        result = await service.delete_databricks_secret("scope", "key")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_workspace_url(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(
            return_value=_make_config(workspace_url="")
        )
        service._databricks_service = mock_ds

        result = await service.delete_databricks_secret("scope", "key")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_auth_fails(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        with patch(_PATCH_AUTH, new_callable=AsyncMock, return_value=None):
            result = await service.delete_databricks_secret("scope", "key")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_api_error(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(404, text_data="Not Found")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.delete_databricks_secret("scope", "key")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(side_effect=RuntimeError("err"))
        service._databricks_service = mock_ds

        result = await service.delete_databricks_secret("scope", "key")
        assert result is False


# ===========================================================================
# Tests: create_databricks_secret_scope
# ===========================================================================


class TestCreateDatabricksSecretScope:
    """Tests for create_databricks_secret_scope."""

    @pytest.mark.asyncio
    async def test_returns_true_on_200(self):
        service = _make_service()
        api_response = _mock_aiohttp_response(200)
        mock_session = _mock_aiohttp_session(api_response)

        with patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_when_scope_already_exists(self):
        service = _make_service()
        api_response = _mock_aiohttp_response(
            400, text_data="Scope 'x' already exists"
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_when_resource_already_exists(self):
        service = _make_service()
        api_response = _mock_aiohttp_response(
            400, text_data="RESOURCE_ALREADY_EXISTS"
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_400_other_error(self):
        service = _make_service()
        api_response = _mock_aiohttp_response(400, text_data="Invalid request")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_other_status(self):
        service = _make_service()
        api_response = _mock_aiohttp_response(500, text_data="Internal Server Error")
        mock_session = _mock_aiohttp_session(api_response)

        with patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        service = _make_service()

        with patch(
            _PATCH_AIOHTTP_SESSION, side_effect=RuntimeError("connection error")
        ):
            result = await service.create_databricks_secret_scope(
                "https://example.com", "token", "scope"
            )
        assert result is False


# ===========================================================================
# Tests: set_databricks_token
# ===========================================================================


class TestSetDatabricksToken:
    """Tests for set_databricks_token convenience method."""

    @pytest.mark.asyncio
    async def test_delegates_to_set_secret_value(self):
        service = _make_service()
        service.set_databricks_secret_value = AsyncMock(return_value=True)

        result = await service.set_databricks_token("scope", "my-token")

        assert result is True
        service.set_databricks_secret_value.assert_awaited_once_with(
            "scope", "DATABRICKS_TOKEN", "my-token"
        )

    @pytest.mark.asyncio
    async def test_propagates_false(self):
        service = _make_service()
        service.set_databricks_secret_value = AsyncMock(return_value=False)

        result = await service.set_databricks_token("scope", "my-token")
        assert result is False


# ===========================================================================
# Tests: setup_provider_api_key (classmethod)
# ===========================================================================


class TestSetupProviderApiKey:
    """Tests for the classmethod setup_provider_api_key.

    This method does local imports of ApiKeysService, UserContext, and
    DatabricksService inside the method body, so we patch at their source
    modules.

    NOTE: The source code on line 428 does:
        service.databricks_service = DatabricksService(db)
    But databricks_service is a @property with no setter. This assignment
    always raises AttributeError, which is caught by the except block on
    line 435. As a result, the Databricks secrets fallback path always
    fails silently in the current code. Tests below reflect this behavior.
    """

    @pytest.mark.asyncio
    async def test_returns_true_when_key_found_in_api_keys_service(self):
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC:
            MockAKS.get_api_key_value = AsyncMock(return_value="the-key-value")
            mock_gc = SimpleNamespace(primary_group_id="grp-1")
            MockUC.get_group_context.return_value = mock_gc

            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "OPENAI_API_KEY"
            )

        assert result is True
        assert os.environ.get("OPENAI_API_KEY") == "the-key-value"
        os.environ.pop("OPENAI_API_KEY", None)

    @pytest.mark.asyncio
    async def test_databricks_fallback_fails_due_to_property_no_setter(self):
        """When ApiKeysService returns None, the code tries to fall back to
        Databricks secrets by creating an instance and assigning
        service.databricks_service = ... but since that is a read-only property,
        this always fails silently. Verify the key is not found."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC, patch(
            _PATCH_DS
        ):
            MockAKS.get_api_key_value = AsyncMock(return_value=None)
            MockUC.get_group_context.return_value = SimpleNamespace(
                primary_group_id="grp-1"
            )

            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "SOME_KEY"
            )

        # The Databricks fallback always fails because of the property setter issue
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_key_not_found_anywhere(self):
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC, patch(
            _PATCH_DS
        ) as MockDS:
            MockAKS.get_api_key_value = AsyncMock(return_value=None)
            MockUC.get_group_context.return_value = SimpleNamespace(
                primary_group_id="grp-1"
            )
            mock_ds_instance = AsyncMock()
            mock_ds_instance.get_databricks_config = AsyncMock(
                return_value=_make_config()
            )
            MockDS.return_value = mock_ds_instance

            with patch.object(
                DatabricksSecretsService,
                "validate_databricks_config",
                new_callable=AsyncMock,
                return_value=("https://example.com", "my-scope"),
            ), patch.object(
                DatabricksSecretsService,
                "get_databricks_secret_value",
                new_callable=AsyncMock,
                return_value="",
            ):
                result = await DatabricksSecretsService.setup_provider_api_key(
                    mock_db, "MISSING_KEY"
                )

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_outer_exception(self):
        mock_db = MagicMock()

        with patch(_PATCH_AKS, side_effect=RuntimeError("import error")):
            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "KEY"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_handles_user_context_exception(self):
        """When UserContext.get_group_context() raises, group_id becomes None."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC:
            MockAKS.get_api_key_value = AsyncMock(return_value="val")
            MockUC.get_group_context.side_effect = RuntimeError("no context")

            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "API_KEY"
            )

        assert result is True
        assert os.environ.get("API_KEY") == "val"
        os.environ.pop("API_KEY", None)

    @pytest.mark.asyncio
    async def test_databricks_fallback_exception_is_caught(self):
        """When validate_databricks_config raises, the exception is logged
        and the method continues (key not found anywhere -> False)."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC, patch(
            _PATCH_DS
        ) as MockDS:
            MockAKS.get_api_key_value = AsyncMock(return_value=None)
            MockUC.get_group_context.return_value = SimpleNamespace(
                primary_group_id="grp-1"
            )
            mock_ds_instance = AsyncMock()
            MockDS.return_value = mock_ds_instance

            with patch.object(
                DatabricksSecretsService,
                "validate_databricks_config",
                new_callable=AsyncMock,
                side_effect=ValueError("Databricks not configured"),
            ):
                result = await DatabricksSecretsService.setup_provider_api_key(
                    mock_db, "KEY"
                )

        assert result is False

    @pytest.mark.asyncio
    async def test_group_context_has_no_primary_group_id(self):
        """When group_context exists but has no primary_group_id attribute,
        group_id should be None."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC:
            MockAKS.get_api_key_value = AsyncMock(return_value="found-value")
            # group_context exists but lacks primary_group_id
            MockUC.get_group_context.return_value = SimpleNamespace(other_attr="x")

            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "MY_KEY"
            )

        assert result is True
        assert os.environ.get("MY_KEY") == "found-value"
        os.environ.pop("MY_KEY", None)

    @pytest.mark.asyncio
    async def test_group_context_returns_none(self):
        """When group_context returns None, group_id should be None."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS, patch(_PATCH_UC) as MockUC:
            MockAKS.get_api_key_value = AsyncMock(return_value="val2")
            MockUC.get_group_context.return_value = None

            result = await DatabricksSecretsService.setup_provider_api_key(
                mock_db, "ANOTHER_KEY"
            )

        assert result is True
        assert os.environ.get("ANOTHER_KEY") == "val2"
        os.environ.pop("ANOTHER_KEY", None)


# ===========================================================================
# Tests: _setup_provider_api_key_sync (staticmethod)
# ===========================================================================


class TestSetupProviderApiKeySync:
    """Tests for the static method _setup_provider_api_key_sync.

    This method does a local import of ApiKeysService and delegates to
    ApiKeysService.setup_provider_api_key_sync(). Since the module is
    already cached in sys.modules, we patch the class at its source.
    """

    def test_delegates_to_api_keys_service(self):
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.setup_provider_api_key_sync.return_value = True
            result = DatabricksSecretsService._setup_provider_api_key_sync(
                mock_db, "MY_KEY"
            )

        assert result is True
        MockAKS.setup_provider_api_key_sync.assert_called_once_with(mock_db, "MY_KEY")

    def test_returns_false_on_exception(self):
        """When ApiKeysService.setup_provider_api_key_sync raises, return False."""
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.setup_provider_api_key_sync.side_effect = RuntimeError("db error")
            result = DatabricksSecretsService._setup_provider_api_key_sync(
                mock_db, "KEY"
            )
        assert result is False

    def test_returns_false_when_api_keys_service_returns_false(self):
        mock_db = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.setup_provider_api_key_sync.return_value = False
            result = DatabricksSecretsService._setup_provider_api_key_sync(
                mock_db, "KEY"
            )
        assert result is False


# ===========================================================================
# Tests: get_personal_access_token
# ===========================================================================


class TestGetPersonalAccessToken:
    """Tests for get_personal_access_token."""

    @pytest.mark.asyncio
    async def test_returns_token_value(self):
        service = _make_service()
        mock_aks = AsyncMock()
        mock_aks.get_api_key_value = AsyncMock(return_value="pat-value")
        service.api_keys_service = mock_aks

        result = await service.get_personal_access_token()
        assert result == "pat-value"
        mock_aks.get_api_key_value.assert_awaited_once_with(
            "DATABRICKS_PERSONAL_ACCESS_TOKEN"
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_api_keys_service(self):
        service = _make_service()
        result = await service.get_personal_access_token()
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_when_key_not_found(self):
        service = _make_service()
        mock_aks = AsyncMock()
        mock_aks.get_api_key_value = AsyncMock(return_value=None)
        service.api_keys_service = mock_aks

        result = await service.get_personal_access_token()
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        service = _make_service()
        mock_aks = AsyncMock()
        mock_aks.get_api_key_value = AsyncMock(side_effect=RuntimeError("err"))
        service.api_keys_service = mock_aks

        result = await service.get_personal_access_token()
        assert result == ""


# ===========================================================================
# Tests: get_provider_api_key
# ===========================================================================


class TestGetProviderApiKey:
    """Tests for get_provider_api_key."""

    @pytest.mark.asyncio
    async def test_returns_key_value(self):
        service = _make_service(group_id="grp-1")
        service.api_keys_service = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.get_provider_api_key = AsyncMock(return_value="provider-key")
            result = await service.get_provider_api_key("openai")

        assert result == "provider-key"
        MockAKS.get_provider_api_key.assert_awaited_once_with(
            "openai", group_id="grp-1"
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_api_keys_service(self):
        service = _make_service()
        result = await service.get_provider_api_key("openai")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_when_key_not_found(self):
        service = _make_service(group_id="grp-1")
        service.api_keys_service = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.get_provider_api_key = AsyncMock(return_value=None)
            result = await service.get_provider_api_key("anthropic")

        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        service = _make_service(group_id="grp-1")
        service.api_keys_service = MagicMock()

        with patch(_PATCH_AKS) as MockAKS:
            MockAKS.get_provider_api_key = AsyncMock(
                side_effect=RuntimeError("boom")
            )
            result = await service.get_provider_api_key("deepseek")

        assert result == ""


# ===========================================================================
# Tests: get_all_databricks_tokens
# ===========================================================================


class TestGetAllDatabricksTokens:
    """Tests for get_all_databricks_tokens."""

    @pytest.mark.asyncio
    async def test_returns_all_tokens(self):
        service = _make_service()
        mock_aks = AsyncMock()

        async def mock_get(key):
            mapping = {
                "DATABRICKS_TOKEN": "tok1",
                "DATABRICKS_API_KEY": "tok2",
                "DATABRICKS_PERSONAL_ACCESS_TOKEN": "tok3",
            }
            return mapping.get(key)

        mock_aks.get_api_key_value = AsyncMock(side_effect=mock_get)
        service.api_keys_service = mock_aks

        result = await service.get_all_databricks_tokens()
        assert result == ["tok1", "tok2", "tok3"]

    @pytest.mark.asyncio
    async def test_returns_partial_when_some_missing(self):
        service = _make_service()
        mock_aks = AsyncMock()

        async def mock_get(key):
            if key == "DATABRICKS_TOKEN":
                return "tok1"
            return None

        mock_aks.get_api_key_value = AsyncMock(side_effect=mock_get)
        service.api_keys_service = mock_aks

        result = await service.get_all_databricks_tokens()
        assert result == ["tok1"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_api_keys_service(self):
        service = _make_service()
        result = await service.get_all_databricks_tokens()
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        service = _make_service()
        mock_aks = AsyncMock()
        mock_aks.get_api_key_value = AsyncMock(side_effect=RuntimeError("fail"))
        service.api_keys_service = mock_aks

        result = await service.get_all_databricks_tokens()
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_all_none(self):
        service = _make_service()
        mock_aks = AsyncMock()
        mock_aks.get_api_key_value = AsyncMock(return_value=None)
        service.api_keys_service = mock_aks

        result = await service.get_all_databricks_tokens()
        assert result == []


# ===========================================================================
# Tests: Edge cases and integration-style scenarios
# ===========================================================================


class TestEdgeCases:
    """Additional edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_get_secrets_empty_secrets_list(self):
        """API returns 200 with empty secrets list."""
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(200, json_data={"secrets": []})
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secrets("scope")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_secret_value_missing_value_key_in_response(self):
        """API returns 200 but JSON has no 'value' key."""
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(200, json_data={})
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secret_value("scope", "key")

        # result.get("value", "") returns "", base64.b64decode("") = b"" -> ""
        assert result == ""

    @pytest.mark.asyncio
    async def test_set_secret_calls_create_scope_with_correct_args(self):
        """Verify create_databricks_secret_scope is called with the right
        workspace_url and token before putting the secret."""
        service = _make_service()
        mock_ds = AsyncMock()
        cfg = _make_config(workspace_url="https://ws.example.com")
        mock_ds.get_databricks_config = AsyncMock(return_value=cfg)
        service._databricks_service = mock_ds
        service.create_databricks_secret_scope = AsyncMock(return_value=True)

        api_response = _mock_aiohttp_response(200)
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH,
            new_callable=AsyncMock,
            return_value=_make_auth_context(token="my-tok"),
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            await service.set_databricks_secret_value("my-scope", "my-key", "my-val")

        service.create_databricks_secret_scope.assert_awaited_once_with(
            "https://ws.example.com", "my-tok", "my-scope"
        )

    @pytest.mark.asyncio
    async def test_config_with_workspace_url_none_passes_empty_string(self):
        """validate_databricks_config returns '' when workspace_url is None."""
        service = _make_service()
        mock_ds = AsyncMock()
        cfg = _make_config(workspace_url=None)
        mock_ds.get_databricks_config = AsyncMock(return_value=cfg)
        service._databricks_service = mock_ds

        url, scope = await service.validate_databricks_config()
        assert url == ""

    def test_group_id_propagated_to_service(self):
        """Verify group_id is stored on the service for multi-tenant isolation."""
        service = _make_service(group_id="tenant-abc")
        assert service.group_id == "tenant-abc"

    @pytest.mark.asyncio
    async def test_get_secrets_no_secrets_key_in_response(self):
        """API returns 200 but JSON has no 'secrets' key."""
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(200, json_data={})
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            result = await service.get_databricks_secrets("scope")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_secrets_id_is_deterministic(self):
        """The 'id' field in returned secrets should be deterministic for the
        same scope:key combination."""
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(
            200, json_data={"secrets": [{"key": "STABLE_KEY"}]}
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            service.get_databricks_secret_value = AsyncMock(return_value="v")
            result = await service.get_databricks_secrets("my-scope")

        expected_id = hash("my-scope:STABLE_KEY") % 10000
        assert result[0]["id"] == expected_id

    @pytest.mark.asyncio
    async def test_get_secrets_description_always_empty(self):
        """Databricks doesn't store descriptions; verify the field is always empty."""
        service = _make_service()
        mock_ds = AsyncMock()
        mock_ds.get_databricks_config = AsyncMock(return_value=_make_config())
        service._databricks_service = mock_ds

        api_response = _mock_aiohttp_response(
            200, json_data={"secrets": [{"key": "K1"}]}
        )
        mock_session = _mock_aiohttp_session(api_response)

        with patch(
            _PATCH_AUTH, new_callable=AsyncMock, return_value=_make_auth_context()
        ), patch(_PATCH_AIOHTTP_SESSION, return_value=mock_session):
            service.get_databricks_secret_value = AsyncMock(return_value="x")
            result = await service.get_databricks_secrets("scope")

        assert result[0]["description"] == ""
