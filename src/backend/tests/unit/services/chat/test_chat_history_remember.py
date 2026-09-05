"""The chat-history funnel remembers an answer once its row has settled.

A streamed answer is created with its first chunk and rewritten per chunk.
Remembering on the create stored "Assistant: # Lebanon Daily News Report —
September" and nothing more, for every streamed turn.
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.chat import history as history_module
from src.services.chat.history import ChatHistoryService
from src.utils.user_context import GroupContext

RUN = {"__chatmode": {"executionId": "run-1"}}


def _service():
    session = AsyncMock()
    with patch("src.services.chat.history.ChatHistoryRepository") as RepoClass:
        RepoClass.return_value = AsyncMock()
        svc = ChatHistoryService(session)
    svc.session_repository = AsyncMock()
    svc.repository.create = AsyncMock(return_value=MagicMock())
    svc.repository.save = AsyncMock()
    svc._last_user_message = AsyncMock(return_value="gather lebanese news")
    return svc


def _row(message_id, content, generation_result=RUN):
    return SimpleNamespace(
        id=message_id,
        session_id="s1",
        user_id="u1",
        message_type="assistant",
        content=content,
        intent=None,
        confidence=None,
        generation_result=generation_result,
        timestamp=datetime(2026, 9, 5, 12, 0, 0),
        group_id="g1",
        group_email="u@example.com",
    )


def _ctx():
    return GroupContext(group_ids=["g1"], group_email="u@example.com")


@pytest.fixture
def fast_settle(monkeypatch):
    monkeypatch.setattr(history_module, "EXCHANGE_SETTLE_SECONDS", 0.02)
    history_module._settling.clear()
    yield
    history_module._settling.clear()


@pytest.fixture
def memory_writes():
    remember = MagicMock()
    with (
        patch(
            "src.services.memory.run.crew_memory.build_session_memory",
            AsyncMock(return_value=object()),
        ),
        patch("src.services.memory.run.persist.remember_async", remember),
    ):
        yield remember


class TestAStreamedAnswerSettlesBeforeItIsRemembered:
    @pytest.mark.asyncio
    async def test_the_final_text_is_remembered_once(self, fast_settle, memory_writes):
        svc = _service()
        await svc.save_message(
            session_id="s1",
            user_id="u1",
            message_type="assistant",
            content="# Lebanon Daily News Report — September",
            generation_result=RUN,
            group_context=_ctx(),
            message_id_override="m1",
        )
        full = "# Lebanon Daily News Report — September 5, 2026\n\n## Latest: three strikes."
        svc.repository.get_by_id_and_group = AsyncMock(
            return_value=_row("m1", "partial")
        )
        await svc.update_message("m1", _ctx(), content=full)
        await asyncio.sleep(0.1)

        assert memory_writes.call_count == 1
        text = memory_writes.call_args.args[1]
        assert text.startswith("User: gather lebanese news")
        assert "Latest: three strikes." in text
        # Stamped with the run it belongs to, so the Run memory pane can show it.
        assert memory_writes.call_args.kwargs["metadata"] == {
            "session_id": "s1",
            "execution_id": "run-1",
        }
        # The question was read once, on the create; the per-chunk rewrites reuse it.
        assert svc._last_user_message.await_count == 1

    @pytest.mark.asyncio
    async def test_nothing_is_remembered_while_the_row_keeps_changing(
        self, fast_settle, memory_writes
    ):
        svc = _service()
        svc.repository.get_by_id_and_group = AsyncMock(return_value=_row("m2", ""))
        for i in range(4):
            await svc.update_message("m2", _ctx(), content=f"chunk {i}")
            await asyncio.sleep(0.005)
        assert memory_writes.call_count == 0
        await asyncio.sleep(0.1)
        assert memory_writes.call_count == 1
        assert "chunk 3" in memory_writes.call_args.args[1]

    @pytest.mark.asyncio
    async def test_user_rows_and_activity_cards_are_not_remembered(
        self, fast_settle, memory_writes
    ):
        svc = _service()
        await svc.save_message(
            session_id="s1",
            user_id="u1",
            message_type="user",
            content="gather lebanese news",
            group_context=_ctx(),
            message_id_override="u-1",
        )
        await svc.save_message(
            session_id="s1",
            user_id="u1",
            message_type="assistant",
            content="[ui-card]",
            group_context=_ctx(),
            message_id_override="c-1",
        )
        svc.repository.get_by_id_and_group = AsyncMock(
            return_value=_row("c-1", "[ui-card]")
        )
        await svc.update_message("c-1", _ctx(), intent="trace")  # no content → no clock
        await asyncio.sleep(0.1)
        assert memory_writes.call_count == 0
        assert history_module._settling == {}
