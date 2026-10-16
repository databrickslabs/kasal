"""The flow data-access bundle reads catalog rows only in the run's workspace.

The bundle's ``task``/``agent``/``crew`` entries are what the 17 read sites in
the flow subprocess call ``get`` on. They are bound to the run's group context
and read through ``catalog/run_lookup``; the subprocess end of this is
``test_flow_run_lookups_workspace_scoped.py``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.repositories.flow_repository import FlowRepository
from src.services.flow_builder.data_access import build_flow_data_access
from src.utils.user_context import GroupContext


def test_the_bundle_keeps_its_keys():
    bundle = build_flow_data_access(MagicMock(), GroupContext(group_ids=["ws-a"]))
    assert set(bundle) == {"flow", "task", "agent", "crew", "execution_history"}
    assert isinstance(bundle["flow"], FlowRepository)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind, read",
    [("agent", "agent_for_run"), ("task", "task_for_run"), ("crew", "crew_for_run")],
)
async def test_get_reads_through_the_run_scoped_lookup(kind, read):
    context = GroupContext(group_ids=["ws-a"])
    scoped = AsyncMock(return_value="row")
    with patch(f"src.services.catalog.run_lookup.{read}", scoped):
        bundle = build_flow_data_access(MagicMock(), context)
        assert await bundle[kind].get("some-id") == "row"
    service, entity_id, group_context, _log = scoped.await_args.args
    assert (entity_id, group_context) == ("some-id", context)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["agent", "task", "crew"])
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_a_run_with_no_group_ids_reads_nothing(kind, context):
    session = MagicMock()
    session.execute = AsyncMock(side_effect=AssertionError("no query expected"))
    bundle = build_flow_data_access(session, context)
    assert await bundle[kind].get("aaaaaaaa-0000-4000-8000-00000000000a") is None
    session.execute.assert_not_called()
