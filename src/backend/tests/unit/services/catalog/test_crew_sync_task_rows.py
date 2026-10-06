"""CrewService propagates task-node tool_configs to the TASK ROW on crew save.

The task row is the source of truth every read hydrates from. Without this, a
canvas edit saved via the crew (not the task editor) would update crew.nodes but
leave the task row — and every drill-through / reload — stale. So "save anywhere
propagates to the flow".
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.catalog.crews import CrewService

_TS = "src.services.catalog.tasks.TaskService"


def _crew(nodes, task_ids):
    return SimpleNamespace(task_ids=task_ids, nodes=nodes)


@pytest.mark.asyncio
async def test_writes_node_tool_configs_to_matching_task_row():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {
                "id": "task-t1",
                "type": "taskNode",
                "data": {
                    "taskId": "t1",
                    "tool_configs": {"UCMV": {"dataset_id": "DDD"}},
                },
            },
            {"id": "agent-a1", "type": "agentNode", "data": {"tool_configs": {"x": 1}}},
        ],
        task_ids=["t1"],
    )
    with patch(_TS) as ts:
        ts.return_value.update_with_group_check = AsyncMock()
        gc = MagicMock()
        await svc._sync_task_rows_from_nodes(crew, gc)
        call = ts.return_value.update_with_group_check.await_args

    assert call.args[0] == "t1"  # the task id
    assert call.args[1].tool_configs == {"UCMV": {"dataset_id": "DDD"}}
    assert call.args[2] is gc  # group-checked
    # agent node was not pushed to a task row (only one call, for the task node)
    assert ts.return_value.update_with_group_check.await_count == 1


@pytest.mark.asyncio
async def test_skips_task_ids_not_in_crew_task_ids():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {
                "id": "task-stale",
                "type": "taskNode",
                "data": {"taskId": "stale", "tool_configs": {"a": 1}},
            }
        ],
        task_ids=["t1"],  # 'stale' is not a current task
    )
    with patch(_TS) as ts:
        ts.return_value.update_with_group_check = AsyncMock()
        await svc._sync_task_rows_from_nodes(crew, MagicMock())
        ts.return_value.update_with_group_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_nodes_without_tool_configs():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[{"id": "task-t1", "type": "taskNode", "data": {"taskId": "t1"}}],
        task_ids=["t1"],
    )
    with patch(_TS) as ts:
        ts.return_value.update_with_group_check = AsyncMock()
        await svc._sync_task_rows_from_nodes(crew, MagicMock())
        ts.return_value.update_with_group_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_defensive_on_error_never_raises():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {
                "id": "task-t1",
                "type": "taskNode",
                "data": {"taskId": "t1", "tool_configs": {"a": 1}},
            }
        ],
        task_ids=["t1"],
    )
    with patch(_TS) as ts:
        ts.return_value.update_with_group_check = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        await svc._sync_task_rows_from_nodes(crew, MagicMock())  # must not raise


@pytest.mark.asyncio
async def test_none_crew_is_safe():
    svc = CrewService(session=MagicMock())
    await svc._sync_task_rows_from_nodes(None, MagicMock())  # must not raise
