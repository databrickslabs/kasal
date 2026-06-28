"""Regression tests for PERF-021/PERF-035: the CrewPreparation agent-UUID
lookup must fetch the group's agent table ONCE per preparation (not once per
crew agent) and must never dump the full table at INFO on a failed match."""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.paths.crew.crew_preparation import CrewPreparation


def _db_agent(uuid, role, name):
    return SimpleNamespace(id=uuid, role=role, name=name)


def _prep():
    with patch.object(CrewPreparation, "__init__", lambda self, *a, **k: None):
        prep = CrewPreparation.__new__(CrewPreparation)
    prep.config = {"group_id": "g1"}
    prep._group_agents_cache = None
    return prep


def _patch_session_and_service(agents):
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
    session_cm.__aexit__ = AsyncMock(return_value=None)

    service = MagicMock()
    service.find_by_group = AsyncMock(return_value=agents)

    return (
        patch("src.db.session.request_scoped_session", return_value=session_cm),
        patch("src.services.agent_service.AgentService", return_value=service),
        service,
    )


@pytest.mark.asyncio
async def test_group_agents_fetched_once_for_many_lookups():
    agents = [_db_agent(f"uuid-{i}", f"Role {i}", f"name-{i}") for i in range(5)]
    prep = _prep()
    p_session, p_service, service = _patch_session_and_service(agents)

    with p_session, p_service:
        for i in range(5):
            found = await prep._lookup_kasal_agent_uuid_via_service(
                {"role": f"Role {i}", "name": f"name-{i}"}, f"cfg-{i}"
            )
            assert found == f"uuid-{i}"

    service.find_by_group.assert_awaited_once()  # ONE fetch for 5 lookups


@pytest.mark.asyncio
async def test_failed_match_logs_bounded_summary_not_full_dump():
    agents = [_db_agent(f"uuid-{i}", f"Role {i}", f"name-{i}") for i in range(400)]
    prep = _prep()
    p_session, p_service, _ = _patch_session_and_service(agents)

    with p_session, p_service, patch(
        "src.engines.crewai.paths.crew.crew_preparation.logger"
    ) as mock_logger:
        found = await prep._lookup_kasal_agent_uuid_via_service(
            {"role": "Nonexistent", "name": "nope"}, "missing-id"
        )

    assert found is None
    all_messages = [
        str(c.args[0]) for c in mock_logger.info.call_args_list + mock_logger.warning.call_args_list
    ]
    dump_lines = [m for m in all_messages if "UUID=" in m]
    assert len(dump_lines) == 0, "full agent-table dump must not happen"
    warnings = [str(c.args[0]) for c in mock_logger.warning.call_args_list]
    assert any("400 group agents" in m for m in warnings)
    # The summary must be bounded — far fewer log calls than agents.
    assert mock_logger.info.call_count + mock_logger.warning.call_count < 10
