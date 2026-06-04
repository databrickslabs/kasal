"""
Unit tests for the Genie API router.

Tests get_genie_spaces, search_genie_spaces, get_genie_space_details,
execute_genie_query, and send_genie_message by calling handler functions
directly with mocked GenieService objects.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class GroupCtx:
    """Minimal GroupContext stand-in."""

    def __init__(self, group_ids=None, group_email="test@example.com"):
        self.group_ids = group_ids or ["grp-1"]
        self.group_email = group_email


def _make_request():
    return MagicMock()


def _make_spaces_response(count=2):
    from src.schemas.genie import GenieSpacesResponse, GenieSpace

    spaces = [
        GenieSpace(id=f"sp-{i}", name=f"Space {i}")
        for i in range(count)
    ]
    return GenieSpacesResponse(spaces=spaces, page_size=50, has_more=False)


def _patch_service(mock_instance):
    """Patch GenieService constructor to return mock_instance."""
    return patch("src.api.genie_router.GenieService", return_value=mock_instance)


def _patch_token(token=None):
    return patch("src.api.genie_router.extract_user_token_from_request", return_value=token)


def _patch_user_context():
    return patch("src.api.genie_router.UserContext.set_group_context")


# ---------------------------------------------------------------------------
# Tests – get_genie_spaces
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_genie_spaces_success():
    """get_genie_spaces returns spaces response from service."""
    from src.api.genie_router import get_genie_spaces

    mock_svc = AsyncMock()
    mock_svc.get_spaces = AsyncMock(return_value=_make_spaces_response(3))

    with _patch_service(mock_svc), _patch_token("tok"), _patch_user_context():
        result = await get_genie_spaces(
            request=_make_request(),
            page_token=None,
            page_size=50,
            group_context=GroupCtx(),
        )

    assert len(result.spaces) == 3
    mock_svc.get_spaces.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_genie_spaces_no_token():
    """get_genie_spaces works when no user token is present."""
    from src.api.genie_router import get_genie_spaces

    mock_svc = AsyncMock()
    mock_svc.get_spaces = AsyncMock(return_value=_make_spaces_response(0))

    with _patch_service(mock_svc), _patch_token(None), _patch_user_context():
        result = await get_genie_spaces(
            request=_make_request(),
            page_token=None,
            page_size=10,
            group_context=None,
        )

    assert result.spaces == []


@pytest.mark.asyncio
async def test_get_genie_spaces_page_size_capped_at_200():
    """page_size > 200 is capped to 200 before being passed to service."""
    from src.api.genie_router import get_genie_spaces
    from src.schemas.genie import GenieSpacesRequest

    mock_svc = AsyncMock()
    mock_svc.get_spaces = AsyncMock(return_value=_make_spaces_response(1))

    with _patch_service(mock_svc), _patch_token(None), _patch_user_context():
        await get_genie_spaces(
            request=_make_request(),
            page_token=None,
            page_size=500,
            group_context=GroupCtx(),
        )

    called_request: GenieSpacesRequest = mock_svc.get_spaces.call_args.args[0]
    assert called_request.page_size <= 200


# ---------------------------------------------------------------------------
# Tests – search_genie_spaces
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_genie_spaces_success():
    """search_genie_spaces forwards request to service and returns result."""
    from src.api.genie_router import search_genie_spaces
    from src.schemas.genie import GenieSpacesRequest

    mock_svc = AsyncMock()
    mock_svc.get_spaces = AsyncMock(return_value=_make_spaces_response(1))

    req_body = GenieSpacesRequest(search_query="sales", page_size=20)

    with _patch_service(mock_svc), _patch_token("t"), _patch_user_context():
        result = await search_genie_spaces(
            request=_make_request(),
            spaces_request=req_body,
            group_context=GroupCtx(),
        )

    assert len(result.spaces) == 1
    mock_svc.get_spaces.assert_awaited_once_with(req_body)


@pytest.mark.asyncio
async def test_search_genie_spaces_no_group_context():
    """search_genie_spaces works without group context (no UserContext update)."""
    from src.api.genie_router import search_genie_spaces
    from src.schemas.genie import GenieSpacesRequest

    mock_svc = AsyncMock()
    mock_svc.get_spaces = AsyncMock(return_value=_make_spaces_response(0))

    with _patch_service(mock_svc), _patch_token(None):
        result = await search_genie_spaces(
            request=_make_request(),
            spaces_request=GenieSpacesRequest(),
            group_context=None,
        )

    assert result.spaces == []


# ---------------------------------------------------------------------------
# Tests – get_genie_space_details
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_genie_space_details_success():
    """get_genie_space_details returns space when found."""
    from src.api.genie_router import get_genie_space_details
    from src.schemas.genie import GenieSpace

    mock_svc = AsyncMock()
    mock_svc.get_space_details = AsyncMock(
        return_value=GenieSpace(id="sp-42", name="Analytics")
    )

    with _patch_service(mock_svc), _patch_token("tok"):
        space = await get_genie_space_details(
            space_id="sp-42",
            request=_make_request(),
            group_context=None,
        )

    assert space.id == "sp-42"
    assert space.name == "Analytics"


@pytest.mark.asyncio
async def test_get_genie_space_details_not_found():
    """get_genie_space_details raises NotFoundError when service returns None."""
    from src.api.genie_router import get_genie_space_details
    from src.core.exceptions import NotFoundError

    mock_svc = AsyncMock()
    mock_svc.get_space_details = AsyncMock(return_value=None)

    with _patch_service(mock_svc), _patch_token(None):
        with pytest.raises(NotFoundError) as exc_info:
            await get_genie_space_details(
                space_id="missing",
                request=_make_request(),
                group_context=None,
            )

    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Tests – execute_genie_query
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_genie_query_success():
    """execute_genie_query calls service and returns response."""
    from src.api.genie_router import execute_genie_query
    from src.schemas.genie import GenieExecutionRequest, GenieExecutionResponse, GenieQueryStatus

    mock_svc = AsyncMock()
    mock_svc.execute_query = AsyncMock(
        return_value=GenieExecutionResponse(
            conversation_id="conv-1",
            message_id="msg-1",
            status=GenieQueryStatus.COMPLETED,
        )
    )

    exec_req = GenieExecutionRequest(space_id="sp-1", question="What are total sales?")

    with _patch_service(mock_svc), _patch_token("tok"):
        result = await execute_genie_query(
            request=_make_request(),
            execution_request=exec_req,
            group_context=None,
        )

    assert result.conversation_id == "conv-1"
    mock_svc.execute_query.assert_awaited_once_with(
        space_id="sp-1",
        question="What are total sales?",
        conversation_id=None,
        timeout=120,
    )


@pytest.mark.asyncio
async def test_execute_genie_query_uses_provided_timeout():
    """execute_genie_query uses custom timeout when provided."""
    from src.api.genie_router import execute_genie_query
    from src.schemas.genie import GenieExecutionRequest, GenieExecutionResponse, GenieQueryStatus

    mock_svc = AsyncMock()
    mock_svc.execute_query = AsyncMock(
        return_value=GenieExecutionResponse(
            conversation_id="c1",
            message_id="m1",
            status=GenieQueryStatus.COMPLETED,
        )
    )

    exec_req = GenieExecutionRequest(space_id="sp-1", question="Q?", timeout=60)

    with _patch_service(mock_svc), _patch_token(None):
        await execute_genie_query(
            request=_make_request(),
            execution_request=exec_req,
            group_context=None,
        )

    call_kwargs = mock_svc.execute_query.call_args.kwargs
    assert call_kwargs["timeout"] == 60


# ---------------------------------------------------------------------------
# Tests – send_genie_message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_genie_message_success():
    """send_genie_message returns response when service succeeds."""
    from src.api.genie_router import send_genie_message
    from src.schemas.genie import GenieSendMessageRequest, GenieSendMessageResponse, GenieMessageStatus

    mock_svc = AsyncMock()
    mock_svc.send_message = AsyncMock(
        return_value=GenieSendMessageResponse(
            conversation_id="cv-1",
            message_id="msg-77",
            status=GenieMessageStatus.RUNNING,
        )
    )

    msg_req = GenieSendMessageRequest(space_id="sp-1", message="Hello Genie")

    with _patch_service(mock_svc), _patch_token("t"):
        result = await send_genie_message(
            request=_make_request(),
            message_request=msg_req,
            group_context=None,
        )

    assert result.message_id == "msg-77"


@pytest.mark.asyncio
async def test_send_genie_message_raises_when_no_response():
    """send_genie_message raises KasalError when service returns None."""
    from src.api.genie_router import send_genie_message
    from src.schemas.genie import GenieSendMessageRequest
    from src.core.exceptions import KasalError

    mock_svc = AsyncMock()
    mock_svc.send_message = AsyncMock(return_value=None)

    msg_req = GenieSendMessageRequest(space_id="sp-1", message="Hi")

    with _patch_service(mock_svc), _patch_token(None):
        with pytest.raises(KasalError):
            await send_genie_message(
                request=_make_request(),
                message_request=msg_req,
                group_context=None,
            )
