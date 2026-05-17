"""
Unit tests for the Group Tools API router.

Tests list_available_to_add, list_added, add_tool, set_enabled,
update_config, and remove_tool endpoints.  Admin-only endpoints are
verified for both the happy path and the forbidden path.

Note: the @require_admin() decorator raises fastapi.HTTPException(403)
(not a custom ForbiddenError), so we assert against that.
"""

import pytest
from types import SimpleNamespace
from datetime import datetime
from unittest.mock import AsyncMock
from fastapi import HTTPException

from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _admin_ctx(group_id="g1"):
    return GroupContext(
        group_ids=[group_id],
        group_email="admin@example.com",
        user_role="admin",
    )


def _user_ctx():
    return GroupContext(
        group_ids=["g1"],
        group_email="user@example.com",
        user_role="user",
    )


def _make_tool_response(i=1):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=i,
        tool_id=i,
        group_id="g1",
        enabled=True,
        config={},
        credentials_status="valid",
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------------------
# Tests – list_available_to_add
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_available_to_add_admin_success():
    """Admin can list globally available tools."""
    from src.api.group_tools_router import list_available_to_add
    from src.schemas.tool import ToolListResponse

    svc = AsyncMock()
    tool_list = ToolListResponse(tools=[], count=0)
    svc.list_available_to_add_for_group = AsyncMock(return_value=tool_list)

    result = await list_available_to_add(service=svc, group_context=_admin_ctx())
    assert result.count == 0
    svc.list_available_to_add_for_group.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_available_to_add_user_forbidden():
    """Non-admin users get 403 when listing available tools."""
    from src.api.group_tools_router import list_available_to_add

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await list_available_to_add(service=svc, group_context=_user_ctx())

    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Tests – list_added
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_added_admin_success():
    """Admin can list tools already in their group."""
    from src.api.group_tools_router import list_added
    from src.schemas.group_tool import GroupToolListResponse

    svc = AsyncMock()
    response = GroupToolListResponse(items=[], count=0)
    svc.list_added_for_group = AsyncMock(return_value=response)

    result = await list_added(service=svc, group_context=_admin_ctx())
    assert result.count == 0


@pytest.mark.asyncio
async def test_list_added_user_forbidden():
    """Non-admin users cannot list added tools."""
    from src.api.group_tools_router import list_added

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await list_added(service=svc, group_context=_user_ctx())

    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Tests – add_tool
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_tool_admin_success():
    """Admin can add a global tool to their group."""
    from src.api.group_tools_router import add_tool

    svc = AsyncMock()
    svc.add_tool_to_group = AsyncMock(return_value=_make_tool_response(5))

    result = await add_tool(tool_id=5, service=svc, group_context=_admin_ctx())
    assert result.id == 5
    svc.add_tool_to_group.assert_awaited_once_with(5, _admin_ctx())


@pytest.mark.asyncio
async def test_add_tool_user_forbidden():
    """Non-admin users cannot add tools to a group."""
    from src.api.group_tools_router import add_tool

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await add_tool(tool_id=1, service=svc, group_context=_user_ctx())

    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Tests – set_enabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_enabled_admin_success():
    """Admin can enable/disable a tool in their group."""
    from src.api.group_tools_router import set_enabled

    svc = AsyncMock()
    svc.set_group_tool_enabled = AsyncMock(return_value=_make_tool_response(3))

    result = await set_enabled(
        tool_id=3,
        payload={"enabled": True},
        service=svc,
        group_context=_admin_ctx(),
    )
    assert result.id == 3
    svc.set_group_tool_enabled.assert_awaited_once_with(3, True, _admin_ctx())


@pytest.mark.asyncio
async def test_set_enabled_missing_field_raises_bad_request():
    """set_enabled raises BadRequestError when 'enabled' is not in payload."""
    from src.api.group_tools_router import set_enabled
    from src.core.exceptions import BadRequestError

    svc = AsyncMock()
    with pytest.raises(BadRequestError) as exc_info:
        await set_enabled(
            tool_id=1,
            payload={"foo": "bar"},
            service=svc,
            group_context=_admin_ctx(),
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_set_enabled_non_bool_raises_bad_request():
    """set_enabled raises BadRequestError when 'enabled' is not a boolean."""
    from src.api.group_tools_router import set_enabled
    from src.core.exceptions import BadRequestError

    svc = AsyncMock()
    with pytest.raises(BadRequestError):
        await set_enabled(
            tool_id=1,
            payload={"enabled": "yes"},  # string, not bool
            service=svc,
            group_context=_admin_ctx(),
        )


@pytest.mark.asyncio
async def test_set_enabled_user_forbidden():
    """Non-admin users cannot toggle a tool's enabled state."""
    from src.api.group_tools_router import set_enabled

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await set_enabled(
            tool_id=1,
            payload={"enabled": True},
            service=svc,
            group_context=_user_ctx(),
        )

    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Tests – update_config
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_config_admin_success():
    """Admin can update group-scoped tool configuration."""
    from src.api.group_tools_router import update_config

    svc = AsyncMock()
    svc.update_group_tool_config = AsyncMock(return_value=_make_tool_response(7))

    new_cfg = {"timeout": 30, "retries": 2}
    result = await update_config(
        tool_id=7,
        config=new_cfg,
        service=svc,
        group_context=_admin_ctx(),
    )
    assert result.id == 7
    svc.update_group_tool_config.assert_awaited_once_with(7, new_cfg, _admin_ctx())


@pytest.mark.asyncio
async def test_update_config_user_forbidden():
    """Non-admin users cannot update tool configuration."""
    from src.api.group_tools_router import update_config

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await update_config(
            tool_id=1,
            config={},
            service=svc,
            group_context=_user_ctx(),
        )

    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Tests – remove_tool
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_tool_admin_success():
    """Admin can remove a tool from their group (returns None on success)."""
    from src.api.group_tools_router import remove_tool

    svc = AsyncMock()
    svc.remove_tool_from_group = AsyncMock(return_value=True)

    result = await remove_tool(tool_id=4, service=svc, group_context=_admin_ctx())
    assert result is None


@pytest.mark.asyncio
async def test_remove_tool_not_found_raises_404():
    """remove_tool raises NotFoundError when the mapping does not exist."""
    from src.api.group_tools_router import remove_tool
    from src.core.exceptions import NotFoundError

    svc = AsyncMock()
    svc.remove_tool_from_group = AsyncMock(return_value=False)

    with pytest.raises(NotFoundError) as exc_info:
        await remove_tool(tool_id=99, service=svc, group_context=_admin_ctx())

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_remove_tool_user_forbidden():
    """Non-admin users cannot remove tools from a group."""
    from src.api.group_tools_router import remove_tool

    svc = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await remove_tool(tool_id=1, service=svc, group_context=_user_ctx())

    assert exc_info.value.status_code == 403
