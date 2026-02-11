"""Unit tests for chat_history_router endpoints.

Tests all endpoints using both direct async function calls and TestClient
with dependency overrides. Validates group context requirements, pagination,
and service delegation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.chat_history_router import (
    save_chat_message,
    get_chat_session_messages,
    get_user_chat_sessions,
    get_group_chat_sessions,
    delete_chat_session,
    create_new_chat_session,
    router,
    get_chat_history_service,
)
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.chat_history import SaveMessageRequest
from src.utils.user_context import GroupContext

from tests.unit.router.conftest import register_exception_handlers


def gc(valid=True, email="user@company.com"):
    """Create a GroupContext for testing."""
    if valid:
        return GroupContext(
            group_ids=["g1"],
            group_email=email,
            email_domain="company.com",
            user_role="admin",
        )
    return GroupContext()


def make_message(msg_id="msg-1", session_id="sess-1"):
    """Create a mock chat history message object."""
    return SimpleNamespace(
        id=msg_id,
        session_id=session_id,
        user_id="user@company.com",
        message_type="user",
        content="Test message",
        intent=None,
        confidence=None,
        generation_result=None,
        timestamp=datetime.now(timezone.utc),
        group_id="g1",
        group_email="user@company.com",
    )


def make_session_info(session_id="sess-1"):
    """Create a mock chat session info object."""
    return SimpleNamespace(
        session_id=session_id,
        user_id="user@company.com",
        latest_timestamp=datetime.now(timezone.utc),
        message_count=5,
    )


# ---------------------------------------------------------------------------
# POST /chat-history/messages
# ---------------------------------------------------------------------------

class TestSaveChatMessage:
    """Tests for save_chat_message endpoint."""

    @pytest.mark.asyncio
    async def test_save_success(self):
        svc = AsyncMock()
        msg = make_message()
        svc.save_message = AsyncMock(return_value=msg)

        request = SaveMessageRequest(
            session_id="sess-1",
            message_type="user",
            content="Hello",
        )

        result = await save_chat_message(
            message_request=request,
            service=svc,
            group_context=gc(),
        )

        assert result.id == "msg-1"
        svc.save_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_passes_user_id_from_context(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value=make_message())

        request = SaveMessageRequest(
            session_id="sess-1",
            message_type="user",
            content="Hello",
        )

        await save_chat_message(
            message_request=request,
            service=svc,
            group_context=gc(email="alice@company.com"),
        )

        call_kwargs = svc.save_message.call_args[1]
        assert call_kwargs["user_id"] == "alice@company.com"

    @pytest.mark.asyncio
    async def test_save_unknown_user_fallback(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value=make_message())

        request = SaveMessageRequest(
            session_id="sess-1",
            message_type="user",
            content="Hello",
        )

        ctx = GroupContext(
            group_ids=["g1"],
            group_email=None,
            email_domain="company.com",
        )

        await save_chat_message(
            message_request=request,
            service=svc,
            group_context=ctx,
        )

        call_kwargs = svc.save_message.call_args[1]
        assert call_kwargs["user_id"] == "unknown_user"

    @pytest.mark.asyncio
    async def test_save_invalid_group_context(self):
        svc = AsyncMock()
        request = SaveMessageRequest(
            session_id="sess-1",
            message_type="user",
            content="Hello",
        )

        with pytest.raises(BadRequestError, match="No valid group context"):
            await save_chat_message(
                message_request=request,
                service=svc,
                group_context=gc(valid=False),
            )

        svc.save_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_save_with_all_optional_fields(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value=make_message())

        request = SaveMessageRequest(
            session_id="sess-1",
            message_type="assistant",
            content="Response",
            intent="generate_agent",
            confidence=0.95,
            generation_result={"agent_id": "a1"},
        )

        await save_chat_message(
            message_request=request,
            service=svc,
            group_context=gc(),
        )

        call_kwargs = svc.save_message.call_args[1]
        assert call_kwargs["intent"] == "generate_agent"
        assert call_kwargs["confidence"] == 0.95
        assert call_kwargs["generation_result"] == {"agent_id": "a1"}


# ---------------------------------------------------------------------------
# GET /chat-history/sessions/{session_id}/messages
# ---------------------------------------------------------------------------

class TestGetChatSessionMessages:
    """Tests for get_chat_session_messages endpoint."""

    @pytest.mark.asyncio
    async def test_get_messages_success(self):
        svc = AsyncMock()
        svc.get_chat_session = AsyncMock(return_value=[make_message()])
        svc.count_session_messages = AsyncMock(return_value=1)

        result = await get_chat_session_messages(
            session_id="sess-1",
            service=svc,
            group_context=gc(),
            page=0,
            per_page=50,
        )

        assert result.session_id == "sess-1"
        assert result.total_messages == 1
        assert result.page == 0
        assert result.per_page == 50
        assert len(result.messages) == 1

    @pytest.mark.asyncio
    async def test_get_messages_with_pagination(self):
        svc = AsyncMock()
        svc.get_chat_session = AsyncMock(return_value=[])
        svc.count_session_messages = AsyncMock(return_value=100)

        result = await get_chat_session_messages(
            session_id="sess-2",
            service=svc,
            group_context=gc(),
            page=2,
            per_page=25,
        )

        assert result.page == 2
        assert result.per_page == 25
        assert result.total_messages == 100
        svc.get_chat_session.assert_called_once_with(
            session_id="sess-2",
            page=2,
            per_page=25,
            group_context=gc(),
        )

    @pytest.mark.asyncio
    async def test_get_messages_invalid_group_context(self):
        svc = AsyncMock()

        with pytest.raises(BadRequestError, match="No valid group context"):
            await get_chat_session_messages(
                session_id="sess-1",
                service=svc,
                group_context=gc(valid=False),
                page=0,
                per_page=50,
            )


# ---------------------------------------------------------------------------
# GET /chat-history/users/sessions
# ---------------------------------------------------------------------------

class TestGetUserChatSessions:
    """Tests for get_user_chat_sessions endpoint."""

    @pytest.mark.asyncio
    async def test_get_user_sessions_success(self):
        svc = AsyncMock()
        svc.get_user_sessions = AsyncMock(return_value=[make_message()])

        result = await get_user_chat_sessions(
            service=svc,
            group_context=gc(),
            page=0,
            per_page=20,
        )

        assert len(result) == 1
        svc.get_user_sessions.assert_called_once_with(
            user_id="user@company.com",
            page=0,
            per_page=20,
            group_context=gc(),
        )

    @pytest.mark.asyncio
    async def test_get_user_sessions_unknown_user_fallback(self):
        svc = AsyncMock()
        svc.get_user_sessions = AsyncMock(return_value=[])

        ctx = GroupContext(
            group_ids=["g1"],
            group_email=None,
            email_domain="company.com",
        )

        await get_user_chat_sessions(
            service=svc,
            group_context=ctx,
            page=0,
            per_page=20,
        )

        call_kwargs = svc.get_user_sessions.call_args[1]
        assert call_kwargs["user_id"] == "unknown_user"

    @pytest.mark.asyncio
    async def test_get_user_sessions_invalid_context(self):
        svc = AsyncMock()

        with pytest.raises(BadRequestError, match="No valid group context"):
            await get_user_chat_sessions(
                service=svc,
                group_context=gc(valid=False),
                page=0,
                per_page=20,
            )


# ---------------------------------------------------------------------------
# GET /chat-history/sessions
# ---------------------------------------------------------------------------

class TestGetGroupChatSessions:
    """Tests for get_group_chat_sessions endpoint."""

    @pytest.mark.asyncio
    async def test_get_group_sessions_success(self):
        svc = AsyncMock()
        sessions = [make_session_info("s1"), make_session_info("s2")]
        svc.get_group_sessions = AsyncMock(return_value=sessions)

        result = await get_group_chat_sessions(
            service=svc,
            group_context=gc(),
            page=0,
            per_page=20,
            user_id=None,
        )

        assert result.total_sessions == 2
        assert result.page == 0
        assert result.per_page == 20
        assert len(result.sessions) == 2

    @pytest.mark.asyncio
    async def test_get_group_sessions_with_user_filter(self):
        svc = AsyncMock()
        svc.get_group_sessions = AsyncMock(return_value=[make_session_info()])

        await get_group_chat_sessions(
            service=svc,
            group_context=gc(),
            page=0,
            per_page=20,
            user_id="alice@company.com",
        )

        svc.get_group_sessions.assert_called_once_with(
            page=0,
            per_page=20,
            user_id="alice@company.com",
            group_context=gc(),
        )

    @pytest.mark.asyncio
    async def test_get_group_sessions_empty(self):
        svc = AsyncMock()
        svc.get_group_sessions = AsyncMock(return_value=[])

        result = await get_group_chat_sessions(
            service=svc,
            group_context=gc(),
            page=0,
            per_page=20,
            user_id=None,
        )

        assert result.total_sessions == 0
        assert result.sessions == []

    @pytest.mark.asyncio
    async def test_get_group_sessions_invalid_context(self):
        svc = AsyncMock()

        with pytest.raises(BadRequestError, match="No valid group context"):
            await get_group_chat_sessions(
                service=svc,
                group_context=gc(valid=False),
                page=0,
                per_page=20,
                user_id=None,
            )


# ---------------------------------------------------------------------------
# DELETE /chat-history/sessions/{session_id}
# ---------------------------------------------------------------------------

class TestDeleteChatSession:
    """Tests for delete_chat_session endpoint."""

    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = AsyncMock()
        svc.delete_session = AsyncMock(return_value=True)

        result = await delete_chat_session(
            session_id="sess-1",
            service=svc,
            group_context=gc(),
        )

        assert result is None
        svc.delete_session.assert_called_once_with(
            session_id="sess-1", group_context=gc()
        )

    @pytest.mark.asyncio
    async def test_delete_not_found(self):
        svc = AsyncMock()
        svc.delete_session = AsyncMock(return_value=False)

        with pytest.raises(NotFoundError, match="Chat session not found"):
            await delete_chat_session(
                session_id="missing",
                service=svc,
                group_context=gc(),
            )

    @pytest.mark.asyncio
    async def test_delete_invalid_context(self):
        svc = AsyncMock()

        with pytest.raises(BadRequestError, match="No valid group context"):
            await delete_chat_session(
                session_id="sess-1",
                service=svc,
                group_context=gc(valid=False),
            )


# ---------------------------------------------------------------------------
# POST /chat-history/sessions/new
# ---------------------------------------------------------------------------

class TestCreateNewChatSession:
    """Tests for create_new_chat_session endpoint."""

    @pytest.mark.asyncio
    async def test_create_session_success(self):
        svc = MagicMock()
        svc.generate_session_id = MagicMock(return_value="new-sess-123")

        result = await create_new_chat_session(
            service=svc, group_context=gc()
        )

        assert result["session_id"] == "new-sess-123"
        svc.generate_session_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_session_invalid_context(self):
        svc = MagicMock()

        with pytest.raises(BadRequestError, match="No valid group context"):
            await create_new_chat_session(
                service=svc, group_context=gc(valid=False)
            )


# ---------------------------------------------------------------------------
# TestClient integration tests
# ---------------------------------------------------------------------------

class TestChatHistoryIntegration:
    """Integration tests using TestClient with dependency overrides."""

    @pytest.fixture
    def mock_service(self):
        svc = AsyncMock()
        svc.save_message = AsyncMock(return_value=make_message())
        svc.get_chat_session = AsyncMock(return_value=[make_message()])
        svc.count_session_messages = AsyncMock(return_value=1)
        svc.get_user_sessions = AsyncMock(return_value=[make_message()])
        svc.get_group_sessions = AsyncMock(return_value=[make_session_info()])
        svc.delete_session = AsyncMock(return_value=True)
        svc.generate_session_id = MagicMock(return_value="new-sess")
        return svc

    @pytest.fixture
    def client(self, mock_service):
        from src.core.dependencies import get_group_context

        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router)

        app.dependency_overrides[get_chat_history_service] = lambda: mock_service
        app.dependency_overrides[get_group_context] = lambda: gc()

        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def client_invalid_gc(self, mock_service):
        from src.core.dependencies import get_group_context

        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router)

        app.dependency_overrides[get_chat_history_service] = lambda: mock_service
        app.dependency_overrides[get_group_context] = lambda: gc(valid=False)

        return TestClient(app, raise_server_exceptions=False)

    def test_save_message_201(self, client):
        response = client.post(
            "/chat-history/messages",
            json={
                "session_id": "sess-1",
                "message_type": "user",
                "content": "Hello",
            },
        )
        assert response.status_code == 201

    def test_save_message_invalid_context_400(self, client_invalid_gc):
        response = client_invalid_gc.post(
            "/chat-history/messages",
            json={
                "session_id": "sess-1",
                "message_type": "user",
                "content": "Hello",
            },
        )
        assert response.status_code == 400

    def test_get_session_messages_200(self, client):
        response = client.get("/chat-history/sessions/sess-1/messages")
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == "sess-1"
        assert data["total_messages"] == 1

    def test_get_user_sessions_200(self, client):
        response = client.get("/chat-history/users/sessions")
        assert response.status_code == 200

    def test_get_group_sessions_200(self, client):
        response = client.get("/chat-history/sessions")
        assert response.status_code == 200
        data = response.json()
        assert "total_sessions" in data

    def test_delete_session_204(self, client):
        response = client.delete("/chat-history/sessions/sess-1")
        assert response.status_code == 204

    def test_delete_session_not_found_404(self, client, mock_service):
        mock_service.delete_session = AsyncMock(return_value=False)
        response = client.delete("/chat-history/sessions/missing")
        assert response.status_code == 404

    def test_create_new_session_201(self, client):
        response = client.post("/chat-history/sessions/new")
        assert response.status_code == 201
        assert response.json()["session_id"] == "new-sess"

    def test_validation_error_422(self, client):
        response = client.post(
            "/chat-history/messages",
            json={
                "session_id": "sess-1",
                "message_type": "invalid_type",
                "content": "x",
            },
        )
        assert response.status_code == 422

    def test_pagination_params(self, client):
        response = client.get(
            "/chat-history/sessions/sess-1/messages?page=2&per_page=25"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 2
        assert data["per_page"] == 25

    def test_pagination_validation_negative_page(self, client):
        response = client.get(
            "/chat-history/sessions/sess-1/messages?page=-1"
        )
        assert response.status_code == 422

    def test_pagination_validation_per_page_too_high(self, client):
        response = client.get(
            "/chat-history/sessions/sess-1/messages?per_page=200"
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_router_config(self):
        assert router.prefix == "/chat-history"
        assert "chat-history" in router.tags
        assert 404 in router.responses

    def test_router_has_expected_endpoints(self):
        route_paths = [route.path for route in router.routes]
        expected = [
            "/chat-history/messages",
            "/chat-history/sessions/{session_id}/messages",
            "/chat-history/users/sessions",
            "/chat-history/sessions",
            "/chat-history/sessions/{session_id}",
            "/chat-history/sessions/new",
        ]
        for path in expected:
            assert path in route_paths, f"Missing route: {path}"

    def test_expected_methods(self):
        methods_by_path = {}
        for route in router.routes:
            methods_by_path[route.path] = route.methods

        assert "POST" in methods_by_path["/chat-history/messages"]
        assert "GET" in methods_by_path["/chat-history/sessions/{session_id}/messages"]
        assert "GET" in methods_by_path["/chat-history/users/sessions"]
        assert "GET" in methods_by_path["/chat-history/sessions"]
        assert "DELETE" in methods_by_path["/chat-history/sessions/{session_id}"]
        assert "POST" in methods_by_path["/chat-history/sessions/new"]
