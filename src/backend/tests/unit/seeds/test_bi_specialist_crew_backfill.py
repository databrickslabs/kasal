"""The BI-specialist crew seeder's self-heal: backfill canvas nodes onto an
existing crew that has none, without clobbering a crew the user has edited."""

import types
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.seeds import bi_specialist_crews as bsc

_CREW = {
    "id": "test-crew-slug",
    "name": "Test Crew",
    "agent_ids": ["a1"],
    "task_ids": ["t1"],
    "nodes": [
        {"id": "agent-a1", "type": "agentNode"},
        {"id": "task-t1", "type": "taskNode"},
    ],
    "edges": [{"id": "e1", "source": "agent-a1", "target": "task-t1"}],
}


def _session_with(existing):
    session = MagicMock()
    result = MagicMock()
    result.scalars.return_value.first.return_value = existing
    session.execute = AsyncMock(return_value=result)
    session.add = MagicMock()
    return session


@pytest.mark.asyncio
async def test_backfills_nodes_when_existing_crew_has_none():
    existing = types.SimpleNamespace(nodes=[], edges=[], agent_ids=[], task_ids=[])
    session = _session_with(existing)

    await bsc._seed_crew(session, _CREW)

    assert existing.nodes == _CREW["nodes"]
    assert existing.edges == _CREW["edges"]
    assert existing.agent_ids == ["a1"]
    assert existing.task_ids == ["t1"]
    session.add.assert_not_called()  # updated in place, not re-created


@pytest.mark.asyncio
async def test_does_not_clobber_existing_crew_that_has_nodes():
    user_nodes = [{"id": "agent-x", "type": "agentNode"}]
    existing = types.SimpleNamespace(
        nodes=user_nodes, edges=[], agent_ids=["x"], task_ids=["y"]
    )
    session = _session_with(existing)

    await bsc._seed_crew(session, _CREW)

    assert existing.nodes == user_nodes  # untouched — a user edit is sacred
    assert existing.agent_ids == ["x"]
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_creates_crew_when_absent():
    session = _session_with(None)

    await bsc._seed_crew(session, _CREW)

    session.add.assert_called_once()
    created = session.add.call_args.args[0]
    assert list(created.nodes) == _CREW["nodes"]
    assert list(created.edges) == _CREW["edges"]


_RENAMED = {**_CREW, "name": "1 · New Name", "previous_names": ["Old Seeded Name"]}


@pytest.mark.asyncio
async def test_renames_crew_still_carrying_old_seeded_name():
    existing = types.SimpleNamespace(
        name="Old Seeded Name",
        nodes=[{"id": "n"}],
        edges=[],
        agent_ids=["a1"],
        task_ids=["t1"],
    )
    await bsc._seed_crew(_session_with(existing), _RENAMED)
    assert existing.name == "1 · New Name"


@pytest.mark.asyncio
async def test_keeps_a_name_the_user_chose():
    existing = types.SimpleNamespace(
        name="My Sales Pipeline",
        nodes=[{"id": "n"}],
        edges=[],
        agent_ids=["a1"],
        task_ids=["t1"],
    )
    await bsc._seed_crew(_session_with(existing), _RENAMED)
    assert existing.name == "My Sales Pipeline"


def test_seeded_crew_names_are_unique_and_renames_cover_old_names():
    names = [e["crew"]["name"] for e in bsc.ALL_CREWS]
    assert len(names) == len(set(names))
    old = [n for e in bsc.ALL_CREWS for n in e["crew"].get("previous_names", [])]
    assert "UCMV — Generate Pipeline Config (API-Direct)" in old
    assert not set(old) & set(names)  # an old name must never be a current one
