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
    get_session_preview,
    save_session_preview,
    delete_session_preview,
    get_session_running_job,
    set_session_running_job,
    clear_session_running_job,
)
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.chat_history import (
    ChatSessionCreateRequest,
    ChatSessionRenameRequest,
    SaveMessageRequest,
    SavePreviewRequest,
    SetRunningJobRequest,
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
        id=sid,
        title=title,
        user_id="dev@localhost",
        group_id="g1",
        created_at=datetime(2026, 6, 11),
        updated_at=datetime(2026, 6, 11),
    )


class TestCreateNamedSession:
    @pytest.mark.asyncio
    async def test_creates_with_user_from_context(self):
        svc = AsyncMock()
        svc.create_named_session = AsyncMock(return_value=_session_row())
        resp = await create_named_session(
            ChatSessionCreateRequest(title="T"), svc, _ctx()
        )
        assert resp.id == "s1"
        kwargs = svc.create_named_session.await_args.kwargs
        assert kwargs["user_id"] == "dev@localhost"
        assert kwargs["title"] == "T"

    @pytest.mark.asyncio
    async def test_invalid_context_rejected(self):
        with pytest.raises(BadRequestError):
            await create_named_session(
                ChatSessionCreateRequest(), AsyncMock(), _ctx(valid=False)
            )


class TestListNamedSessions:
    @pytest.mark.asyncio
    async def test_lists_for_user(self):
        svc = AsyncMock()
        svc.list_named_sessions = AsyncMock(
            return_value=[_session_row("a"), _session_row("b")]
        )
        resp = await list_named_sessions(svc, _ctx())
        assert [s.id for s in resp] == ["a", "b"]


class TestRenameNamedSession:
    @pytest.mark.asyncio
    async def test_renames(self):
        svc = AsyncMock()
        svc.rename_named_session = AsyncMock(return_value=_session_row(title="Renamed"))
        resp = await rename_named_session(
            "s1", ChatSessionRenameRequest(title="Renamed"), svc, _ctx()
        )
        assert resp.title == "Renamed"

    @pytest.mark.asyncio
    async def test_not_found_raises(self):
        svc = AsyncMock()
        svc.rename_named_session = AsyncMock(return_value=None)
        with pytest.raises(NotFoundError):
            await rename_named_session(
                "missing", ChatSessionRenameRequest(title="X"), svc, _ctx()
            )


class TestUpdateChatMessage:
    @pytest.mark.asyncio
    async def test_updates(self):
        svc = AsyncMock()
        svc.update_message = AsyncMock(return_value={"id": "m1"})
        resp = await update_chat_message(
            "m1", UpdateMessageRequest(content="new"), svc, _ctx()
        )
        assert resp == {"id": "m1"}
        assert svc.update_message.await_args.kwargs["content"] == "new"

    @pytest.mark.asyncio
    async def test_not_found_raises(self):
        svc = AsyncMock()
        svc.update_message = AsyncMock(return_value=None)
        with pytest.raises(NotFoundError):
            await update_chat_message(
                "missing", UpdateMessageRequest(content="x"), svc, _ctx()
            )


class TestSaveMessageIdPassthrough:
    @pytest.mark.asyncio
    async def test_client_id_forwarded(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value={"id": "client-1"})
        req = SaveMessageRequest(
            session_id="s1", id="client-1", message_type="user", content="hi"
        )
        await save_chat_message(req, svc, _ctx())
        assert svc.save_message.await_args.kwargs["message_id_override"] == "client-1"


class TestSessionPreview:
    @pytest.mark.asyncio
    async def test_get_returns_stored_preview(self):
        svc = AsyncMock()
        svc.get_preview = AsyncMock(
            return_value={"type": "ui", "data": "{}", "title": "T"}
        )
        resp = await get_session_preview("s1", svc, _ctx())
        assert resp.type == "ui" and resp.data == "{}" and resp.title == "T"

    @pytest.mark.asyncio
    async def test_get_returns_all_null_when_none(self):
        svc = AsyncMock()
        svc.get_preview = AsyncMock(return_value=None)
        resp = await get_session_preview("s1", svc, _ctx())
        assert resp.type is None and resp.data is None and resp.title is None

    @pytest.mark.asyncio
    async def test_save_forwards_fields(self):
        svc = AsyncMock()
        svc.set_preview = AsyncMock(return_value=True)
        await save_session_preview(
            "s1", SavePreviewRequest(type="ui", data="x", title="T"), svc, _ctx()
        )
        args = svc.set_preview.await_args.args
        assert args[0] == "s1" and args[1] == "ui" and args[2] == "x" and args[3] == "T"

    @pytest.mark.asyncio
    async def test_save_missing_session_raises(self):
        svc = AsyncMock()
        svc.set_preview = AsyncMock(return_value=False)
        with pytest.raises(NotFoundError):
            await save_session_preview(
                "missing", SavePreviewRequest(type="ui", data="x"), svc, _ctx()
            )

    @pytest.mark.asyncio
    async def test_delete_is_idempotent(self):
        svc = AsyncMock()
        svc.set_preview = AsyncMock(return_value=False)  # missing session → no raise
        await delete_session_preview("s1", svc, _ctx())
        # cleared by passing all-None
        assert svc.set_preview.await_args.args[1:4] == (None, None, None)

    @pytest.mark.asyncio
    async def test_invalid_context_rejected(self):
        with pytest.raises(BadRequestError):
            await get_session_preview("s1", AsyncMock(), _ctx(valid=False))


class TestRunningJobMarker:
    @pytest.mark.asyncio
    async def test_get_returns_job_id(self):
        svc = AsyncMock()
        svc.get_running_job = AsyncMock(return_value="job-9")
        resp = await get_session_running_job("s1", svc, _ctx())
        assert resp.job_id == "job-9"

    @pytest.mark.asyncio
    async def test_get_returns_null_when_none(self):
        svc = AsyncMock()
        svc.get_running_job = AsyncMock(return_value=None)
        resp = await get_session_running_job("s1", svc, _ctx())
        assert resp.job_id is None

    @pytest.mark.asyncio
    async def test_set_forwards_job_id(self):
        svc = AsyncMock()
        svc.set_running_job = AsyncMock(return_value=True)
        await set_session_running_job(
            "s1", SetRunningJobRequest(job_id="job-1"), svc, _ctx()
        )
        assert svc.set_running_job.await_args.args[:2] == ("s1", "job-1")

    @pytest.mark.asyncio
    async def test_set_missing_session_raises(self):
        svc = AsyncMock()
        svc.set_running_job = AsyncMock(return_value=False)
        with pytest.raises(NotFoundError):
            await set_session_running_job(
                "missing", SetRunningJobRequest(job_id="j"), svc, _ctx()
            )

    @pytest.mark.asyncio
    async def test_clear_passes_none_and_is_idempotent(self):
        svc = AsyncMock()
        svc.set_running_job = AsyncMock(return_value=False)
        await clear_session_running_job("s1", svc, _ctx())
        assert svc.set_running_job.await_args.args[:2] == ("s1", None)
