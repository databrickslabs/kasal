"""Unit tests for databricks_router endpoints.

Tests all endpoints using direct async function calls with mocked service
dependencies. Permission checks are tested by supplying GroupContext objects
with different role configurations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from datetime import datetime

from src.api.databricks_router import (
    set_databricks_config,
    get_databricks_config,
    check_personal_token_required,
    check_databricks_connection,
    get_databricks_environment,
    router,
)
from src.core.exceptions import ForbiddenError
from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse
from src.utils.user_context import GroupContext


def gc_admin():
    """Create a GroupContext with admin role (workspace admin)."""
    return GroupContext(
        group_ids=["g1"],
        group_email="admin@x.com",
        email_domain="x.com",
        user_role="admin",
    )


def gc_editor():
    """Create a GroupContext with editor role (not workspace admin)."""
    return GroupContext(
        group_ids=["g1"],
        group_email="editor@x.com",
        email_domain="x.com",
        user_role="editor",
    )


def gc_operator():
    """Create a GroupContext with operator role (not workspace admin)."""
    return GroupContext(
        group_ids=["g1"],
        group_email="operator@x.com",
        email_domain="x.com",
        user_role="operator",
    )


# ---------------------------------------------------------------------------
# POST /databricks/config
# ---------------------------------------------------------------------------

class TestSetDatabricksConfig:
    """Tests for set_databricks_config endpoint."""

    @pytest.mark.asyncio
    async def test_set_config_success_as_admin(self):
        svc = AsyncMock()
        config_response = {
            "status": "success",
            "message": "Configuration set successfully",
        }
        svc.set_databricks_config = AsyncMock(return_value=config_response)

        request = DatabricksConfigCreate(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            schema="sch",
            enabled=True,
        )

        result = await set_databricks_config(
            request=request,
            group_context=gc_admin(),
            service=svc,
        )

        assert result["status"] == "success"
        svc.set_databricks_config.assert_called_once()

    @pytest.mark.asyncio
    async def test_set_config_forbidden_for_editor(self):
        svc = AsyncMock()

        request = DatabricksConfigCreate(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            schema="sch",
            enabled=True,
        )

        with pytest.raises(ForbiddenError):
            await set_databricks_config(
                request=request,
                group_context=gc_editor(),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_set_config_forbidden_for_operator(self):
        svc = AsyncMock()

        request = DatabricksConfigCreate(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            schema="sch",
            enabled=True,
        )

        with pytest.raises(ForbiddenError):
            await set_databricks_config(
                request=request,
                group_context=gc_operator(),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_set_config_passes_email(self):
        svc = AsyncMock()
        svc.set_databricks_config = AsyncMock(return_value={"status": "ok"})

        request = DatabricksConfigCreate(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            schema="sch",
            enabled=True,
        )

        await set_databricks_config(
            request=request,
            group_context=gc_admin(),
            service=svc,
        )

        call_kwargs = svc.set_databricks_config.call_args
        assert call_kwargs[1]["created_by_email"] == "admin@x.com"

    @pytest.mark.asyncio
    async def test_set_config_propagates_service_error(self):
        svc = AsyncMock()
        svc.set_databricks_config = AsyncMock(
            side_effect=RuntimeError("config error")
        )

        request = DatabricksConfigCreate(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            schema="sch",
            enabled=True,
        )

        with pytest.raises(RuntimeError, match="config error"):
            await set_databricks_config(
                request=request,
                group_context=gc_admin(),
                service=svc,
            )


# ---------------------------------------------------------------------------
# GET /databricks/config
# ---------------------------------------------------------------------------

class TestGetDatabricksConfig:
    """Tests for get_databricks_config endpoint."""

    @pytest.mark.asyncio
    async def test_get_config_success(self):
        svc = AsyncMock()
        config = SimpleNamespace(
            workspace_url="https://example.com",
            warehouse_id="wh-1",
            catalog="cat",
            db_schema="sch",
            enabled=True,
        )
        svc.get_databricks_config = AsyncMock(return_value=config)

        result = await get_databricks_config(
            group_context=gc_admin(), service=svc
        )

        assert result.workspace_url == "https://example.com"

    @pytest.mark.asyncio
    async def test_get_config_returns_default_when_none(self):
        svc = AsyncMock()
        svc.get_databricks_config = AsyncMock(return_value=None)

        result = await get_databricks_config(
            group_context=gc_admin(), service=svc
        )

        assert isinstance(result, DatabricksConfigResponse)
        assert result.workspace_url == ""
        assert result.enabled is False

    @pytest.mark.asyncio
    async def test_get_config_forbidden_for_editor(self):
        svc = AsyncMock()

        with pytest.raises(ForbiddenError):
            await get_databricks_config(
                group_context=gc_editor(), service=svc
            )

    @pytest.mark.asyncio
    async def test_get_config_forbidden_for_operator(self):
        svc = AsyncMock()

        with pytest.raises(ForbiddenError):
            await get_databricks_config(
                group_context=gc_operator(), service=svc
            )


# ---------------------------------------------------------------------------
# GET /databricks/status/personal-token-required
# ---------------------------------------------------------------------------

class TestCheckPersonalTokenRequired:
    """Tests for check_personal_token_required endpoint."""

    @pytest.mark.asyncio
    async def test_token_required_true(self):
        svc = AsyncMock()
        svc.check_personal_token_required = AsyncMock(
            return_value={"personal_token_required": True, "message": "OAuth not configured"}
        )

        result = await check_personal_token_required(
            group_context=gc_admin(), service=svc
        )

        assert result["personal_token_required"] is True

    @pytest.mark.asyncio
    async def test_token_required_false(self):
        svc = AsyncMock()
        svc.check_personal_token_required = AsyncMock(
            return_value={"personal_token_required": False, "message": "OK"}
        )

        result = await check_personal_token_required(
            group_context=gc_admin(), service=svc
        )

        assert result["personal_token_required"] is False

    @pytest.mark.asyncio
    async def test_forbidden_for_non_admin(self):
        svc = AsyncMock()

        with pytest.raises(ForbiddenError):
            await check_personal_token_required(
                group_context=gc_editor(), service=svc
            )


# ---------------------------------------------------------------------------
# GET /databricks/connection
# ---------------------------------------------------------------------------

class TestCheckDatabricksConnection:
    """Tests for check_databricks_connection endpoint."""

    @pytest.mark.asyncio
    async def test_connection_success(self):
        svc = AsyncMock()
        svc.check_databricks_connection = AsyncMock(
            return_value={
                "connected": True,
                "workspace_url": "https://example.com",
            }
        )

        result = await check_databricks_connection(
            group_context=gc_admin(), service=svc
        )

        assert result["connected"] is True

    @pytest.mark.asyncio
    async def test_connection_failed(self):
        svc = AsyncMock()
        svc.check_databricks_connection = AsyncMock(
            return_value={"connected": False, "error": "Invalid credentials"}
        )

        result = await check_databricks_connection(
            group_context=gc_admin(), service=svc
        )

        assert result["connected"] is False

    @pytest.mark.asyncio
    async def test_forbidden_for_non_admin(self):
        svc = AsyncMock()

        with pytest.raises(ForbiddenError):
            await check_databricks_connection(
                group_context=gc_operator(), service=svc
            )


# ---------------------------------------------------------------------------
# GET /databricks/environment
# ---------------------------------------------------------------------------

class TestGetDatabricksEnvironment:
    """Tests for get_databricks_environment endpoint."""

    @pytest.mark.asyncio
    async def test_environment_success(self):
        mock_auth = MagicMock()
        mock_auth._workspace_host = "https://example.com"
        mock_auth._load_config = AsyncMock()

        mock_auth_context = MagicMock()
        mock_auth_context.auth_method = "pat"
        mock_auth_context.user_identity = "admin@x.com"

        with patch(
            "src.utils.databricks_auth._databricks_auth", mock_auth
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth_context,
        ):
            result = await get_databricks_environment(
                group_context=gc_admin()
            )

        assert result["databricks_host"] == "https://example.com"
        assert result["auth_method"] == "pat"
        assert result["user_identity"] == "admin@x.com"
        assert result["authenticated"] is True

    @pytest.mark.asyncio
    async def test_environment_no_auth(self):
        mock_auth = MagicMock()
        mock_auth._workspace_host = "https://example.com"
        mock_auth._load_config = AsyncMock()

        with patch(
            "src.utils.databricks_auth._databricks_auth", mock_auth
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await get_databricks_environment(
                group_context=gc_admin()
            )

        assert result["databricks_host"] == "https://example.com"
        assert result["auth_method"] is None
        assert result["authenticated"] is False

    @pytest.mark.asyncio
    async def test_environment_forbidden_for_non_admin(self):
        with pytest.raises(ForbiddenError):
            await get_databricks_environment(group_context=gc_editor())


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_router_config(self):
        assert router.prefix == "/databricks"
        assert "databricks" in router.tags

    def test_router_has_expected_endpoints(self):
        route_paths = [route.path for route in router.routes]
        expected = [
            "/databricks/config",
            "/databricks/status/personal-token-required",
            "/databricks/connection",
            "/databricks/environment",
        ]
        for path in expected:
            assert path in route_paths, f"Missing route: {path}"
