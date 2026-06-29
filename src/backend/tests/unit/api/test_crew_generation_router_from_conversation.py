"""Smoke tests for the answer-mode "Save to catalog" endpoint:
POST /crew/from-conversation -> CrewGenerationService.synthesize_crew_from_conversation.
"""
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.api.crew_generation_router import create_crew_from_conversation
from src.schemas.crew import CrewCreationResponse, CrewFromConversationRequest
from src.utils.user_context import GroupContext


def _gc():
    return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com")


@pytest.mark.asyncio
async def test_returns_synthesized_agents_and_tasks():
    """The endpoint hands the session id to the service and wraps the created
    entities in a CrewCreationResponse."""
    created = {
        "agents": [{"id": "a1", "name": "Researcher"}],
        "tasks": [{"id": "t1", "name": "Gather"}, {"id": "t2", "name": "Dashboard"}],
    }
    svc = Mock()
    svc.synthesize_crew_from_conversation = AsyncMock(return_value=created)

    with patch("src.api.crew_generation_router.CrewGenerationService", return_value=svc):
        resp = await create_crew_from_conversation(
            request=CrewFromConversationRequest(session_id="sess-1", model="m1"),
            group_context=_gc(),
            session=Mock(),
        )

    assert isinstance(resp, CrewCreationResponse)
    assert len(resp.agents) == 1
    assert len(resp.tasks) == 2
    svc.synthesize_crew_from_conversation.assert_awaited_once()
    kwargs = svc.synthesize_crew_from_conversation.await_args.kwargs
    assert kwargs["session_id"] == "sess-1"
    assert kwargs["model"] == "m1"


@pytest.mark.asyncio
async def test_empty_result_yields_empty_lists():
    """A service that returns nothing (no usable conversation handled upstream)
    still produces a well-formed response."""
    svc = Mock()
    svc.synthesize_crew_from_conversation = AsyncMock(return_value={})

    with patch("src.api.crew_generation_router.CrewGenerationService", return_value=svc):
        resp = await create_crew_from_conversation(
            request=CrewFromConversationRequest(session_id="sess-1"),
            group_context=_gc(),
            session=Mock(),
        )

    assert resp.agents == []
    assert resp.tasks == []
