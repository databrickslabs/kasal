"""
Direct-call tests for the named-session chat-history endpoints
(server-side chat-mode session storage).
"""
import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace

from src.api.chat_history_router import (
    create_named_session,
    list_named_sessions,
    rename_named_session,
    update_chat_message,
    save_chat_message,
)
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.chat_history import (
    ChatSessionCreateRequest,
    ChatSessionRenameRequest,
    SaveMessageRequest,
    UpdateMessageRequest,
)
from src.models.chat_session import ChatSession
from datetime import datetime


def _ctx(valid=True):
    return SimpleNamespace(
        is_valid=lambda: valid,
        group_email="dev@localhost",
        group_ids=["g1"],
        primary_group_id="g1",
    )


def _session_row(sid="s1", title="T"):
    return ChatSession(
        id=sid, title=title, user_id="dev@localhost", group_id="g1",
        created_at=datetime(2026, 6, 11), updated_at=datetime(2026, 6, 11),
    )


class TestCreateNamedSession:
    @pytest.mark.asyncio
    async def test_creates_with_user_from_context(self):
        svc = AsyncMock()
        svc.create_named_session = AsyncMock(return_value=_session_row())
        resp = await create_named_session(ChatSessionCreateRequest(title="T"), svc, _ctx())
        assert resp.id == "s1"
        kwargs = svc.create_named_session.await_args.kwargs
        assert kwargs["user_id"] == "dev@localhost"
        assert kwargs["title"] == "T"

    @pytest.mark.asyncio
    async def test_invalid_context_rejected(self):
        with pytest.raises(BadRequestError):
            await create_named_session(ChatSessionCreateRequest(), AsyncMock(), _ctx(valid=False))


class TestListNamedSessions:
    @pytest.mark.asyncio
    async def test_lists_for_user(self):
        svc = AsyncMock()
        svc.list_named_sessions = AsyncMock(return_value=[_session_row("a"), _session_row("b")])
        resp = await list_named_sessions(svc, _ctx())
        assert [s.id for s in resp] == ["a", "b"]


class TestRenameNamedSession:
    @pytest.mark.asyncio
    async def test_renames(self):
        svc = AsyncMock()
        svc.rename_named_session = AsyncMock(return_value=_session_row(title="Renamed"))
        resp = await rename_named_session("s1", ChatSessionRenameRequest(title="Renamed"), svc, _ctx())
        assert resp.title == "Renamed"

    @pytest.mark.asyncio
    async def test_not_found_raises(self):
        svc = AsyncMock()
        svc.rename_named_session = AsyncMock(return_value=None)
        with pytest.raises(NotFoundError):
            await rename_named_session("missing", ChatSessionRenameRequest(title="X"), svc, _ctx())


class TestUpdateChatMessage:
    @pytest.mark.asyncio
    async def test_updates(self):
        svc = AsyncMock()
        svc.update_message = AsyncMock(return_value={"id": "m1"})
        resp = await update_chat_message("m1", UpdateMessageRequest(content="new"), svc, _ctx())
        assert resp == {"id": "m1"}
        assert svc.update_message.await_args.kwargs["content"] == "new"

    @pytest.mark.asyncio
    async def test_not_found_raises(self):
        svc = AsyncMock()
        svc.update_message = AsyncMock(return_value=None)
        with pytest.raises(NotFoundError):
            await update_chat_message("missing", UpdateMessageRequest(content="x"), svc, _ctx())


class TestSaveMessageIdPassthrough:
    @pytest.mark.asyncio
    async def test_client_id_forwarded(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value={"id": "client-1"})
        req = SaveMessageRequest(session_id="s1", id="client-1", message_type="user", content="hi")
        await save_chat_message(req, svc, _ctx())
        assert svc.save_message.await_args.kwargs["message_id_override"] == "client-1"
