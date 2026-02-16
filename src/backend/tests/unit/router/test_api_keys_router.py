"""Unit tests for api_keys_router endpoints.

Tests all CRUD endpoints using direct async function calls with mocked
service dependencies. Permission checks verify that only admin and editor
roles can create, update, and delete API keys.
"""
import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime

from src.api.api_keys_router import (
    get_api_keys_metadata,
    create_api_key,
    update_api_key,
    delete_api_key,
    router,
)
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate
from src.utils.user_context import GroupContext


def gc(role="admin"):
    """Create a GroupContext with the given role."""
    return GroupContext(
        group_ids=["g1"],
        group_email="u@x.com",
        email_domain="x.com",
        user_role=role,
    )


def make_api_key(kid=1, name="test-key", value=""):
    """Create a mock API key response object."""
    return SimpleNamespace(
        id=kid,
        name=name,
        description="A test key",
        value=value,
    )


# ---------------------------------------------------------------------------
# GET /api-keys
# ---------------------------------------------------------------------------

class TestGetApiKeysMetadata:
    """Tests for get_api_keys_metadata endpoint."""

    @pytest.mark.asyncio
    async def test_returns_keys_list(self):
        svc = AsyncMock()
        keys = [make_api_key(1, "openai"), make_api_key(2, "anthropic")]
        svc.get_api_keys_metadata = AsyncMock(return_value=keys)

        result = await get_api_keys_metadata(service=svc)

        assert len(result) == 2
        assert result[0].name == "openai"
        assert result[1].name == "anthropic"

    @pytest.mark.asyncio
    async def test_returns_empty_list(self):
        svc = AsyncMock()
        svc.get_api_keys_metadata = AsyncMock(return_value=[])

        result = await get_api_keys_metadata(service=svc)

        assert result == []

    @pytest.mark.asyncio
    async def test_propagates_exception(self):
        svc = AsyncMock()
        svc.get_api_keys_metadata = AsyncMock(
            side_effect=RuntimeError("db error")
        )

        with pytest.raises(RuntimeError, match="db error"):
            await get_api_keys_metadata(service=svc)


# ---------------------------------------------------------------------------
# POST /api-keys
# ---------------------------------------------------------------------------

class TestCreateApiKey:
    """Tests for create_api_key endpoint."""

    @pytest.mark.asyncio
    async def test_create_success_as_admin(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)
        created = make_api_key(3, "new-key")
        svc.create_api_key = AsyncMock(return_value=created)

        data = ApiKeyCreate(name="new-key", value="sk-123")

        result = await create_api_key(
            api_key_data=data, group_context=gc("admin"), service=svc
        )

        assert result.name == "new-key"
        svc.find_by_name.assert_called_once_with("new-key")
        svc.create_api_key.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_success_as_editor(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)
        svc.create_api_key = AsyncMock(return_value=make_api_key())

        data = ApiKeyCreate(name="key", value="sk-123")

        result = await create_api_key(
            api_key_data=data, group_context=gc("editor"), service=svc
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_create_forbidden_for_operator(self):
        svc = AsyncMock()
        data = ApiKeyCreate(name="key", value="sk-123")

        with pytest.raises(ForbiddenError):
            await create_api_key(
                api_key_data=data, group_context=gc("operator"), service=svc
            )

        svc.find_by_name.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_duplicate_raises_bad_request(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key(1, "dup-key"))

        data = ApiKeyCreate(name="dup-key", value="sk-123")

        with pytest.raises(BadRequestError, match="already exists"):
            await create_api_key(
                api_key_data=data, group_context=gc("admin"), service=svc
            )

        svc.create_api_key.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_passes_email(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)
        svc.create_api_key = AsyncMock(return_value=make_api_key())

        data = ApiKeyCreate(name="key", value="sk-123")

        await create_api_key(
            api_key_data=data, group_context=gc("admin"), service=svc
        )

        call_kwargs = svc.create_api_key.call_args
        assert call_kwargs[1]["created_by_email"] == "u@x.com"

    @pytest.mark.asyncio
    async def test_create_propagates_service_error(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)
        svc.create_api_key = AsyncMock(side_effect=RuntimeError("db error"))

        data = ApiKeyCreate(name="key", value="sk-123")

        with pytest.raises(RuntimeError, match="db error"):
            await create_api_key(
                api_key_data=data, group_context=gc("admin"), service=svc
            )


# ---------------------------------------------------------------------------
# PUT /api-keys/{api_key_name}
# ---------------------------------------------------------------------------

class TestUpdateApiKey:
    """Tests for update_api_key endpoint."""

    @pytest.mark.asyncio
    async def test_update_success(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key(1, "mykey"))
        updated = make_api_key(1, "mykey")
        updated.description = "Updated"
        svc.update_api_key = AsyncMock(return_value=updated)

        data = ApiKeyUpdate(value="sk-new")

        result = await update_api_key(
            api_key_name="mykey",
            api_key_data=data,
            group_context=gc("admin"),
            service=svc,
        )

        assert result.name == "mykey"
        svc.update_api_key.assert_called_once_with("mykey", data)

    @pytest.mark.asyncio
    async def test_update_as_editor(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key())
        svc.update_api_key = AsyncMock(return_value=make_api_key())

        data = ApiKeyUpdate(value="sk-new")

        result = await update_api_key(
            api_key_name="key",
            api_key_data=data,
            group_context=gc("editor"),
            service=svc,
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_update_forbidden_for_operator(self):
        svc = AsyncMock()
        data = ApiKeyUpdate(value="sk-new")

        with pytest.raises(ForbiddenError):
            await update_api_key(
                api_key_name="key",
                api_key_data=data,
                group_context=gc("operator"),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_update_not_found(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)

        data = ApiKeyUpdate(value="sk-new")

        with pytest.raises(NotFoundError, match="not found"):
            await update_api_key(
                api_key_name="missing",
                api_key_data=data,
                group_context=gc("admin"),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_update_returns_none_raises_not_found(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key())
        svc.update_api_key = AsyncMock(return_value=None)

        data = ApiKeyUpdate(value="sk-new")

        with pytest.raises(NotFoundError, match="update failed"):
            await update_api_key(
                api_key_name="key",
                api_key_data=data,
                group_context=gc("admin"),
                service=svc,
            )


# ---------------------------------------------------------------------------
# DELETE /api-keys/{api_key_name}
# ---------------------------------------------------------------------------

class TestDeleteApiKey:
    """Tests for delete_api_key endpoint."""

    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key(1, "key"))
        svc.delete_api_key = AsyncMock(return_value=True)

        result = await delete_api_key(
            api_key_name="key",
            group_context=gc("admin"),
            service=svc,
        )

        # 204 No Content - function returns None
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_as_editor(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key())
        svc.delete_api_key = AsyncMock(return_value=True)

        result = await delete_api_key(
            api_key_name="key",
            group_context=gc("editor"),
            service=svc,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_delete_forbidden_for_operator(self):
        svc = AsyncMock()

        with pytest.raises(ForbiddenError):
            await delete_api_key(
                api_key_name="key",
                group_context=gc("operator"),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_delete_key_not_found(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError, match="not found"):
            await delete_api_key(
                api_key_name="missing",
                group_context=gc("admin"),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_delete_operation_returns_false(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key())
        svc.delete_api_key = AsyncMock(return_value=False)

        with pytest.raises(NotFoundError, match="not found"):
            await delete_api_key(
                api_key_name="key",
                group_context=gc("admin"),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_delete_propagates_service_error(self):
        svc = AsyncMock()
        svc.find_by_name = AsyncMock(return_value=make_api_key())
        svc.delete_api_key = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(RuntimeError, match="db error"):
            await delete_api_key(
                api_key_name="key",
                group_context=gc("admin"),
                service=svc,
            )


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_router_config(self):
        assert router.prefix == "/api-keys"
        assert "api-keys" in router.tags
        assert 404 in router.responses

    def test_router_has_expected_endpoints(self):
        route_paths = [route.path for route in router.routes]
        expected = [
            "/api-keys",
            "/api-keys/{api_key_name}",
        ]
        for path in expected:
            assert path in route_paths, f"Missing route: {path}"

    def test_expected_methods(self):
        methods_by_path = {}
        for route in router.routes:
            for method in route.methods:
                methods_by_path.setdefault(route.path, set()).add(method)

        assert "GET" in methods_by_path["/api-keys"]
        assert "POST" in methods_by_path["/api-keys"]
        assert "PUT" in methods_by_path["/api-keys/{api_key_name}"]
        assert "DELETE" in methods_by_path["/api-keys/{api_key_name}"]
