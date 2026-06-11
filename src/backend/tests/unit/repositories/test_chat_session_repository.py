"""
Unit tests for ChatSessionRepository (named chat-mode sessions).

Covers group-scoped CRUD, ordering, the empty-group short-circuits, and the
model's table shape (server-side replacement for browser IndexedDB storage).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.chat_session import ChatSession
from src.repositories.chat_session_repository import ChatSessionRepository


@pytest.fixture
def mock_session():
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()
    return session


@pytest.fixture
def repo(mock_session):
    return ChatSessionRepository(session=mock_session)


def _scalars_result(rows):
    result = MagicMock()
    result.scalars.return_value.first.return_value = rows[0] if rows else None
    result.scalars.return_value.all.return_value = rows
    return result


class TestModelShape:
    def test_table_and_columns(self):
        assert ChatSession.__tablename__ == "chat_sessions"
        cols = {c.name for c in ChatSession.__table__.columns}
        assert {"id", "title", "user_id", "created_at", "updated_at", "group_id", "group_email"} <= cols


class TestCreate:
    @pytest.mark.asyncio
    async def test_create_adds_and_flushes(self, repo, mock_session):
        record = await repo.create({
            "id": "s1", "title": "T", "user_id": "u@x.com", "group_id": "g1",
        })
        assert record.id == "s1"
        mock_session.add.assert_called_once()
        mock_session.flush.assert_awaited_once()


class TestGetByIdAndGroup:
    @pytest.mark.asyncio
    async def test_returns_record(self, repo, mock_session):
        row = ChatSession(id="s1", title="T", user_id="u", group_id="g1")
        mock_session.execute = AsyncMock(return_value=_scalars_result([row]))
        assert await repo.get_by_id_and_group("s1", ["g1"]) is row

    @pytest.mark.asyncio
    async def test_empty_groups_short_circuits(self, repo, mock_session):
        assert await repo.get_by_id_and_group("s1", []) is None
        mock_session.execute.assert_not_awaited()


class TestListByGroupAndUser:
    @pytest.mark.asyncio
    async def test_lists_rows(self, repo, mock_session):
        rows = [ChatSession(id="s1"), ChatSession(id="s2")]
        mock_session.execute = AsyncMock(return_value=_scalars_result(rows))
        result = await repo.list_by_group_and_user(["g1"], "u@x.com")
        assert result == rows
        # Query is ordered by updated_at desc and group+user scoped
        stmt = str(mock_session.execute.await_args.args[0])
        assert "ORDER BY chat_sessions.updated_at DESC" in stmt
        assert "chat_sessions.user_id" in stmt
        assert "chat_sessions.group_id IN" in stmt

    @pytest.mark.asyncio
    async def test_empty_groups_short_circuits(self, repo, mock_session):
        assert await repo.list_by_group_and_user([], "u") == []
        mock_session.execute.assert_not_awaited()


class TestUpdateTitle:
    @pytest.mark.asyncio
    async def test_updates_title_and_timestamp(self, repo, mock_session):
        row = ChatSession(id="s1", title="Old", user_id="u", group_id="g1")
        mock_session.execute = AsyncMock(return_value=_scalars_result([row]))
        result = await repo.update_title("s1", ["g1"], "New Title")
        assert result is row
        assert row.title == "New Title"
        assert row.updated_at is not None
        mock_session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_not_found_returns_none(self, repo, mock_session):
        mock_session.execute = AsyncMock(return_value=_scalars_result([]))
        assert await repo.update_title("missing", ["g1"], "X") is None


class TestTouchAndDelete:
    @pytest.mark.asyncio
    async def test_touch_issues_update(self, repo, mock_session):
        mock_session.execute = AsyncMock(return_value=MagicMock())
        await repo.touch("s1")
        stmt = str(mock_session.execute.await_args.args[0])
        assert "UPDATE chat_sessions" in stmt

    @pytest.mark.asyncio
    async def test_delete_returns_true_on_rows(self, repo, mock_session):
        mock_session.execute = AsyncMock(return_value=MagicMock(rowcount=1))
        assert await repo.delete_by_id_and_group("s1", ["g1"]) is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_no_rows(self, repo, mock_session):
        mock_session.execute = AsyncMock(return_value=MagicMock(rowcount=0))
        assert await repo.delete_by_id_and_group("s1", ["g1"]) is False

    @pytest.mark.asyncio
    async def test_delete_empty_groups_short_circuits(self, repo, mock_session):
        assert await repo.delete_by_id_and_group("s1", []) is False
        mock_session.execute.assert_not_awaited()
