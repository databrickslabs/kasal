"""Comprehensive unit tests for ChatHistoryService.

Covers all public methods, branch paths (with/without group_context,
empty group_ids, None group_context), and error propagation from the
underlying repository layer.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from pydantic import ValidationError

from src.services.chat_history_service import ChatHistoryService
from src.schemas.chat_history import ChatHistoryResponse
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group_context(
    group_ids=None,
    group_email="user@example.com",
    primary_group_id="grp-1",
):
    """Create a GroupContext dataclass with sensible defaults for testing."""
    ctx = GroupContext(
        group_ids=group_ids or ["grp-1"],
        group_email=group_email,
    )
    return ctx


def _make_chat_history_row(
    id="msg-1",
    session_id="sess-1",
    user_id="user-1",
    message_type="user",
    content="hello",
    intent=None,
    confidence=None,
    generation_result=None,
    timestamp=None,
    group_id="grp-1",
    group_email="user@example.com",
):
    """Return a SimpleNamespace that looks like a ChatHistory ORM row.

    The ChatHistoryResponse schema has ``model_config = ConfigDict(from_attributes=True)``
    so it will call ``getattr`` on this namespace to validate.
    """
    return SimpleNamespace(
        id=id,
        session_id=session_id,
        user_id=user_id,
        message_type=message_type,
        content=content,
        intent=intent,
        confidence=confidence,
        generation_result=generation_result,
        timestamp=timestamp or datetime(2025, 1, 1, 12, 0, 0),
        group_id=group_id,
        group_email=group_email,
    )


def _build_service():
    """Instantiate ChatHistoryService with a mocked session and repository."""
    session = AsyncMock()
    with patch(
        "src.services.chat_history_service.ChatHistoryRepository"
    ) as RepoClass:
        repo_mock = AsyncMock()
        RepoClass.return_value = repo_mock
        service = ChatHistoryService(session)
    # service.repository is now the AsyncMock returned by RepoClass()
    return service, service.repository


# ========================================================================
# save_message
# ========================================================================


class TestSaveMessage:
    """Tests for ChatHistoryService.save_message."""

    @pytest.mark.asyncio
    async def test_save_message_basic(self):
        """Minimal call with required args returns a ChatHistoryResponse."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="sess-1",
            user_id="user-1",
            message_type="user",
            content="hi",
        )

        assert isinstance(result, ChatHistoryResponse)
        assert result.session_id == "sess-1"
        assert result.user_id == "user-1"
        assert result.message_type == "user"
        assert result.content == "hi"
        assert result.intent is None
        assert result.confidence is None
        assert result.generation_result is None
        assert result.group_id is None
        assert result.group_email is None
        repo.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_save_message_with_optional_fields(self):
        """All optional scalar fields are forwarded correctly."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())
        gen_result = {"agent": {"name": "Test"}}

        result = await svc.save_message(
            session_id="sess-2",
            user_id="user-2",
            message_type="assistant",
            content="response text",
            intent="generate_agent",
            confidence=0.95,
            generation_result=gen_result,
        )

        assert result.intent == "generate_agent"
        assert result.confidence == "0.95"
        assert result.generation_result == gen_result

    @pytest.mark.asyncio
    async def test_save_message_confidence_none_stays_none(self):
        """When confidence is None it should stay None (not be converted to string)."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="c",
            confidence=None,
        )

        assert result.confidence is None

    @pytest.mark.asyncio
    async def test_save_message_confidence_zero_converted(self):
        """confidence=0.0 is not None, so it should be stringified."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="c",
            confidence=0.0,
        )

        assert result.confidence == "0.0"

    @pytest.mark.asyncio
    async def test_save_message_with_group_context(self):
        """group_id and group_email are set when group_context is provided."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())
        ctx = _make_group_context(group_ids=["grp-42"], group_email="a@b.com")

        result = await svc.save_message(
            session_id="sess-1",
            user_id="user-1",
            message_type="user",
            content="msg",
            group_context=ctx,
        )

        assert result.group_id == "grp-42"
        assert result.group_email == "a@b.com"

    @pytest.mark.asyncio
    async def test_save_message_without_group_context(self):
        """When group_context is None, group fields are absent from data."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="sess-1",
            user_id="user-1",
            message_type="user",
            content="msg",
            group_context=None,
        )

        assert result.group_id is None
        assert result.group_email is None

    @pytest.mark.asyncio
    async def test_save_message_id_and_timestamp_generated(self):
        """A UUID id and a UTC timestamp are generated before persisting."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="sess-1",
            user_id="user-1",
            message_type="user",
            content="hello",
        )

        # id should be a 36-char UUID string
        assert len(result.id) == 36
        assert isinstance(result.timestamp, datetime)

    @pytest.mark.asyncio
    async def test_save_message_repository_create_called_with_correct_data(self):
        """Verify the exact dict passed to repository.create."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())
        ctx = _make_group_context(group_ids=["g1"], group_email="e@x.com")

        await svc.save_message(
            session_id="s1",
            user_id="u1",
            message_type="assistant",
            content="response",
            intent="greet",
            confidence=0.8,
            generation_result={"k": "v"},
            group_context=ctx,
        )

        call_args = repo.create.call_args
        data = call_args[0][0] if call_args[0] else call_args[1]
        assert data["session_id"] == "s1"
        assert data["user_id"] == "u1"
        assert data["message_type"] == "assistant"
        assert data["content"] == "response"
        assert data["intent"] == "greet"
        assert data["confidence"] == "0.8"
        assert data["generation_result"] == {"k": "v"}
        assert data["group_id"] == "g1"
        assert data["group_email"] == "e@x.com"
        # id and timestamp present
        assert "id" in data
        assert "timestamp" in data

    @pytest.mark.asyncio
    async def test_save_message_propagates_repository_error(self):
        """If repository.create raises, the error bubbles up."""
        svc, repo = _build_service()
        repo.create = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(RuntimeError, match="db error"):
            await svc.save_message(
                session_id="s",
                user_id="u",
                message_type="user",
                content="c",
            )

    @pytest.mark.asyncio
    async def test_save_message_execution_message_type(self):
        """The 'execution' message type is accepted by the schema."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="execution",
            content="run started",
        )

        assert result.message_type == "execution"

    @pytest.mark.asyncio
    async def test_save_message_trace_message_type(self):
        """The 'trace' message type is accepted by the schema."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="trace",
            content="trace data",
        )

        assert result.message_type == "trace"


# ========================================================================
# get_chat_session
# ========================================================================


class TestGetChatSession:
    """Tests for ChatHistoryService.get_chat_session."""

    @pytest.mark.asyncio
    async def test_returns_list_of_responses(self):
        """Happy path: repository rows are converted to ChatHistoryResponse."""
        svc, repo = _build_service()
        rows = [
            _make_chat_history_row(id="m1", content="a"),
            _make_chat_history_row(id="m2", content="b"),
        ]
        repo.get_by_session_and_group = AsyncMock(return_value=rows)
        ctx = _make_group_context()

        result = await svc.get_chat_session("sess-1", group_context=ctx)

        assert len(result) == 2
        assert all(isinstance(r, ChatHistoryResponse) for r in result)
        assert result[0].id == "m1"
        assert result[1].id == "m2"

    @pytest.mark.asyncio
    async def test_passes_pagination_to_repository(self):
        """page and per_page are forwarded."""
        svc, repo = _build_service()
        repo.get_by_session_and_group = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_chat_session("sess-1", page=3, per_page=10, group_context=ctx)

        repo.get_by_session_and_group.assert_awaited_once_with(
            session_id="sess-1",
            group_ids=ctx.group_ids,
            page=3,
            per_page=10,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_context(self):
        """Without group_context the service short-circuits to []."""
        svc, repo = _build_service()

        result = await svc.get_chat_session("sess-1", group_context=None)

        assert result == []
        repo.get_by_session_and_group.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_empty(self):
        """group_context with empty group_ids triggers early return."""
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.get_chat_session("sess-1", group_context=ctx)

        assert result == []
        repo.get_by_session_and_group.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_none(self):
        """group_context with group_ids=None triggers early return."""
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=None, group_email="u@x.com")

        result = await svc.get_chat_session("sess-1", group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_propagates_repository_error(self):
        """Repository errors propagate."""
        svc, repo = _build_service()
        repo.get_by_session_and_group = AsyncMock(side_effect=RuntimeError("boom"))
        ctx = _make_group_context()

        with pytest.raises(RuntimeError, match="boom"):
            await svc.get_chat_session("s", group_context=ctx)

    @pytest.mark.asyncio
    async def test_model_validate_called_on_each_row(self):
        """Each ORM row goes through ChatHistoryResponse.model_validate."""
        svc, repo = _build_service()
        row = _make_chat_history_row(
            id="x1",
            session_id="s1",
            user_id="u1",
            message_type="assistant",
            content="response",
            intent="greet",
            confidence="0.9",
            group_id="g1",
            group_email="e@e.com",
        )
        repo.get_by_session_and_group = AsyncMock(return_value=[row])
        ctx = _make_group_context()

        result = await svc.get_chat_session("s1", group_context=ctx)

        assert result[0].id == "x1"
        assert result[0].intent == "greet"
        assert result[0].confidence == "0.9"
        assert result[0].group_id == "g1"


# ========================================================================
# get_user_sessions
# ========================================================================


class TestGetUserSessions:
    """Tests for ChatHistoryService.get_user_sessions."""

    @pytest.mark.asyncio
    async def test_returns_list_of_responses(self):
        svc, repo = _build_service()
        rows = [_make_chat_history_row(id="m1")]
        repo.get_user_sessions = AsyncMock(return_value=rows)
        ctx = _make_group_context()

        result = await svc.get_user_sessions("user-1", group_context=ctx)

        assert len(result) == 1
        assert isinstance(result[0], ChatHistoryResponse)

    @pytest.mark.asyncio
    async def test_passes_pagination(self):
        svc, repo = _build_service()
        repo.get_user_sessions = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_user_sessions("u1", page=2, per_page=15, group_context=ctx)

        repo.get_user_sessions.assert_awaited_once_with(
            user_id="u1",
            group_ids=ctx.group_ids,
            page=2,
            per_page=15,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_context(self):
        svc, repo = _build_service()

        result = await svc.get_user_sessions("u1", group_context=None)

        assert result == []
        repo.get_user_sessions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_empty(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.get_user_sessions("u1", group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_none(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=None, group_email="u@x.com")

        result = await svc.get_user_sessions("u1", group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_default_pagination_values(self):
        """Defaults: page=0, per_page=20."""
        svc, repo = _build_service()
        repo.get_user_sessions = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_user_sessions("u1", group_context=ctx)

        repo.get_user_sessions.assert_awaited_once_with(
            user_id="u1",
            group_ids=ctx.group_ids,
            page=0,
            per_page=20,
        )

    @pytest.mark.asyncio
    async def test_propagates_repository_error(self):
        svc, repo = _build_service()
        repo.get_user_sessions = AsyncMock(side_effect=ValueError("bad"))
        ctx = _make_group_context()

        with pytest.raises(ValueError, match="bad"):
            await svc.get_user_sessions("u1", group_context=ctx)

    @pytest.mark.asyncio
    async def test_multiple_sessions_returned(self):
        """Multiple session rows are each converted to response DTOs."""
        svc, repo = _build_service()
        rows = [
            _make_chat_history_row(id="m1", session_id="s1"),
            _make_chat_history_row(id="m2", session_id="s2"),
            _make_chat_history_row(id="m3", session_id="s3"),
        ]
        repo.get_user_sessions = AsyncMock(return_value=rows)
        ctx = _make_group_context()

        result = await svc.get_user_sessions("u1", group_context=ctx)

        assert len(result) == 3
        assert [r.session_id for r in result] == ["s1", "s2", "s3"]


# ========================================================================
# get_group_sessions
# ========================================================================


class TestGetGroupSessions:
    """Tests for ChatHistoryService.get_group_sessions."""

    @pytest.mark.asyncio
    async def test_returns_repository_result_directly(self):
        """Result from repository is returned as-is (list of dicts)."""
        svc, repo = _build_service()
        expected = [
            {"session_id": "s1", "user_id": "u1", "latest_timestamp": datetime.utcnow(), "message_count": 3},
        ]
        repo.get_sessions_by_group = AsyncMock(return_value=expected)
        ctx = _make_group_context()

        result = await svc.get_group_sessions(group_context=ctx)

        assert result == expected

    @pytest.mark.asyncio
    async def test_passes_pagination_and_user_id(self):
        svc, repo = _build_service()
        repo.get_sessions_by_group = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_group_sessions(page=1, per_page=5, user_id="u1", group_context=ctx)

        repo.get_sessions_by_group.assert_awaited_once_with(
            group_ids=ctx.group_ids,
            user_id="u1",
            page=1,
            per_page=5,
        )

    @pytest.mark.asyncio
    async def test_user_id_defaults_to_none(self):
        svc, repo = _build_service()
        repo.get_sessions_by_group = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_group_sessions(group_context=ctx)

        repo.get_sessions_by_group.assert_awaited_once_with(
            group_ids=ctx.group_ids,
            user_id=None,
            page=0,
            per_page=20,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_context(self):
        svc, repo = _build_service()

        result = await svc.get_group_sessions(group_context=None)

        assert result == []
        repo.get_sessions_by_group.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_empty(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.get_group_sessions(group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_none(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=None, group_email="u@x.com")

        result = await svc.get_group_sessions(group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_propagates_repository_error(self):
        svc, repo = _build_service()
        repo.get_sessions_by_group = AsyncMock(side_effect=RuntimeError("fail"))
        ctx = _make_group_context()

        with pytest.raises(RuntimeError, match="fail"):
            await svc.get_group_sessions(group_context=ctx)

    @pytest.mark.asyncio
    async def test_returns_multiple_sessions(self):
        """Multiple session dicts are returned without transformation."""
        svc, repo = _build_service()
        expected = [
            {"session_id": "s1", "user_id": "u1", "latest_timestamp": datetime.utcnow(), "message_count": 5},
            {"session_id": "s2", "user_id": "u2", "latest_timestamp": datetime.utcnow(), "message_count": 2},
        ]
        repo.get_sessions_by_group = AsyncMock(return_value=expected)
        ctx = _make_group_context()

        result = await svc.get_group_sessions(group_context=ctx)

        assert len(result) == 2
        assert result[0]["session_id"] == "s1"
        assert result[1]["session_id"] == "s2"


# ========================================================================
# delete_session
# ========================================================================


class TestDeleteSession:
    """Tests for ChatHistoryService.delete_session."""

    @pytest.mark.asyncio
    async def test_returns_true_when_deleted(self):
        svc, repo = _build_service()
        repo.delete_session = AsyncMock(return_value=True)
        ctx = _make_group_context()

        result = await svc.delete_session("sess-1", group_context=ctx)

        assert result is True
        repo.delete_session.assert_awaited_once_with(
            session_id="sess-1",
            group_ids=ctx.group_ids,
        )

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self):
        svc, repo = _build_service()
        repo.delete_session = AsyncMock(return_value=False)
        ctx = _make_group_context()

        result = await svc.delete_session("sess-999", group_context=ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_group_context(self):
        svc, repo = _build_service()

        result = await svc.delete_session("sess-1", group_context=None)

        assert result is False
        repo.delete_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_false_when_group_ids_empty(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.delete_session("sess-1", group_context=ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_group_ids_none(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=None, group_email="u@x.com")

        result = await svc.delete_session("sess-1", group_context=ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_propagates_repository_error(self):
        svc, repo = _build_service()
        repo.delete_session = AsyncMock(side_effect=RuntimeError("db"))
        ctx = _make_group_context()

        with pytest.raises(RuntimeError, match="db"):
            await svc.delete_session("s1", group_context=ctx)


# ========================================================================
# count_session_messages
# ========================================================================


class TestCountSessionMessages:
    """Tests for ChatHistoryService.count_session_messages."""

    @pytest.mark.asyncio
    async def test_returns_count(self):
        svc, repo = _build_service()
        repo.count_messages_by_session = AsyncMock(return_value=42)
        ctx = _make_group_context()

        result = await svc.count_session_messages("sess-1", group_context=ctx)

        assert result == 42
        repo.count_messages_by_session.assert_awaited_once_with(
            session_id="sess-1",
            group_ids=ctx.group_ids,
        )

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_group_context(self):
        svc, repo = _build_service()

        result = await svc.count_session_messages("sess-1", group_context=None)

        assert result == 0
        repo.count_messages_by_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_zero_when_group_ids_empty(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.count_session_messages("sess-1", group_context=ctx)

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_group_ids_none(self):
        svc, repo = _build_service()
        ctx = GroupContext(group_ids=None, group_email="u@x.com")

        result = await svc.count_session_messages("sess-1", group_context=ctx)

        assert result == 0

    @pytest.mark.asyncio
    async def test_propagates_repository_error(self):
        svc, repo = _build_service()
        repo.count_messages_by_session = AsyncMock(side_effect=RuntimeError("err"))
        ctx = _make_group_context()

        with pytest.raises(RuntimeError, match="err"):
            await svc.count_session_messages("s1", group_context=ctx)

    @pytest.mark.asyncio
    async def test_returns_large_count(self):
        """Large counts are returned as-is from the repository."""
        svc, repo = _build_service()
        repo.count_messages_by_session = AsyncMock(return_value=999999)
        ctx = _make_group_context()

        result = await svc.count_session_messages("sess-1", group_context=ctx)

        assert result == 999999


# ========================================================================
# generate_session_id
# ========================================================================


class TestGenerateSessionId:
    """Tests for ChatHistoryService.generate_session_id (sync method)."""

    def test_returns_uuid_string(self):
        svc, _ = _build_service()
        sid = svc.generate_session_id()
        assert isinstance(sid, str)
        assert len(sid) == 36  # standard UUID format

    def test_returns_unique_ids(self):
        svc, _ = _build_service()
        ids = {svc.generate_session_id() for _ in range(100)}
        assert len(ids) == 100

    def test_uuid_format(self):
        """Output matches 8-4-4-4-12 hex pattern."""
        import re
        svc, _ = _build_service()
        sid = svc.generate_session_id()
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        assert re.match(pattern, sid)


# ========================================================================
# Constructor / initialization
# ========================================================================


class TestInit:
    """Tests for ChatHistoryService initialization."""

    def test_repository_is_created(self):
        """ChatHistoryRepository is instantiated with the session."""
        session = AsyncMock()
        with patch(
            "src.services.chat_history_service.ChatHistoryRepository"
        ) as RepoClass:
            repo_instance = AsyncMock()
            RepoClass.return_value = repo_instance
            svc = ChatHistoryService(session)

            RepoClass.assert_called_once_with(session)
            assert svc.repository is repo_instance

    def test_session_stored(self):
        """The session is stored via BaseService.__init__."""
        session = AsyncMock()
        with patch(
            "src.services.chat_history_service.ChatHistoryRepository"
        ):
            svc = ChatHistoryService(session)
            assert svc.session is session


# ========================================================================
# Edge cases / integration-like scenarios
# ========================================================================


class TestEdgeCases:
    """Additional edge-case and boundary tests."""

    @pytest.mark.asyncio
    async def test_save_message_empty_content_raises_validation_error(self):
        """Empty content raises ValidationError because ChatHistoryResponse
        inherits min_length=1 from the base schema on the content field.

        The service persists data first, then constructs the Pydantic DTO, so
        the validation error comes from the ChatHistoryResponse constructor.
        """
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        with pytest.raises(ValidationError):
            await svc.save_message(
                session_id="s",
                user_id="u",
                message_type="user",
                content="",
            )

    @pytest.mark.asyncio
    async def test_save_message_with_large_generation_result(self):
        """A large dict is passed through without modification."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())
        big = {f"key_{i}": f"value_{i}" for i in range(500)}

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="assistant",
            content="big",
            generation_result=big,
        )

        assert result.generation_result == big

    @pytest.mark.asyncio
    async def test_get_chat_session_with_multiple_group_ids(self):
        """Multiple group_ids are forwarded to the repository."""
        svc, repo = _build_service()
        repo.get_by_session_and_group = AsyncMock(return_value=[])
        ctx = _make_group_context(group_ids=["g1", "g2", "g3"])

        await svc.get_chat_session("s1", group_context=ctx)

        call_kwargs = repo.get_by_session_and_group.call_args[1]
        assert call_kwargs["group_ids"] == ["g1", "g2", "g3"]

    @pytest.mark.asyncio
    async def test_delete_session_with_multiple_group_ids(self):
        """Multiple group_ids are forwarded to delete_session."""
        svc, repo = _build_service()
        repo.delete_session = AsyncMock(return_value=True)
        ctx = _make_group_context(group_ids=["g1", "g2"])

        result = await svc.delete_session("s1", group_context=ctx)

        assert result is True
        repo.delete_session.assert_awaited_once_with(
            session_id="s1",
            group_ids=["g1", "g2"],
        )

    @pytest.mark.asyncio
    async def test_get_chat_session_default_pagination(self):
        """Defaults: page=0, per_page=50."""
        svc, repo = _build_service()
        repo.get_by_session_and_group = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_chat_session("s1", group_context=ctx)

        repo.get_by_session_and_group.assert_awaited_once_with(
            session_id="s1",
            group_ids=ctx.group_ids,
            page=0,
            per_page=50,
        )

    @pytest.mark.asyncio
    async def test_get_group_sessions_default_pagination(self):
        """Defaults: page=0, per_page=20."""
        svc, repo = _build_service()
        repo.get_sessions_by_group = AsyncMock(return_value=[])
        ctx = _make_group_context()

        await svc.get_group_sessions(group_context=ctx)

        repo.get_sessions_by_group.assert_awaited_once_with(
            group_ids=ctx.group_ids,
            user_id=None,
            page=0,
            per_page=20,
        )

    @pytest.mark.asyncio
    async def test_save_message_group_context_with_no_primary_group(self):
        """If group_context has empty group_ids, primary_group_id is None."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())
        ctx = GroupContext(group_ids=[], group_email="u@x.com")

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="c",
            group_context=ctx,
        )

        # primary_group_id returns None for empty list
        assert result.group_id is None
        assert result.group_email == "u@x.com"

    @pytest.mark.asyncio
    async def test_save_message_returns_consistent_id_and_timestamp(self):
        """The same id and timestamp in the returned DTO match what was persisted."""
        svc, repo = _build_service()
        persisted_data = None

        async def capture_create(data):
            nonlocal persisted_data
            persisted_data = dict(data)
            return MagicMock()

        repo.create = AsyncMock(side_effect=capture_create)

        result = await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="c",
        )

        assert persisted_data is not None
        assert result.id == persisted_data["id"]
        assert result.timestamp == persisted_data["timestamp"]

    @pytest.mark.asyncio
    async def test_get_user_sessions_empty_result(self):
        """Repository returning empty list results in empty response list."""
        svc, repo = _build_service()
        repo.get_user_sessions = AsyncMock(return_value=[])
        ctx = _make_group_context()

        result = await svc.get_user_sessions("u1", group_context=ctx)

        assert result == []

    @pytest.mark.asyncio
    async def test_count_session_messages_returns_zero_from_repo(self):
        """Repository returning 0 is correctly forwarded."""
        svc, repo = _build_service()
        repo.count_messages_by_session = AsyncMock(return_value=0)
        ctx = _make_group_context()

        result = await svc.count_session_messages("s1", group_context=ctx)

        assert result == 0

    @pytest.mark.asyncio
    async def test_save_message_invalid_message_type_raises_validation_error(self):
        """An invalid message_type raises ValidationError from the response schema."""
        svc, repo = _build_service()
        repo.create = AsyncMock(return_value=MagicMock())

        with pytest.raises(ValidationError):
            await svc.save_message(
                session_id="s",
                user_id="u",
                message_type="invalid_type",
                content="hello",
            )

    @pytest.mark.asyncio
    async def test_save_message_without_no_group_context_data_dict_has_no_group_keys(self):
        """When group_context is None, the dict passed to repository.create
        should NOT contain group_id or group_email keys."""
        svc, repo = _build_service()
        captured = {}

        async def capture(data):
            captured.update(data)
            return MagicMock()

        repo.create = AsyncMock(side_effect=capture)

        await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="text",
            group_context=None,
        )

        assert "group_id" not in captured
        assert "group_email" not in captured

    @pytest.mark.asyncio
    async def test_save_message_with_group_context_data_dict_has_group_keys(self):
        """When group_context is provided, the dict passed to repository.create
        should contain group_id and group_email keys."""
        svc, repo = _build_service()
        captured = {}

        async def capture(data):
            captured.update(data)
            return MagicMock()

        repo.create = AsyncMock(side_effect=capture)
        ctx = _make_group_context(group_ids=["gx"], group_email="a@b.com")

        await svc.save_message(
            session_id="s",
            user_id="u",
            message_type="user",
            content="text",
            group_context=ctx,
        )

        assert captured["group_id"] == "gx"
        assert captured["group_email"] == "a@b.com"
