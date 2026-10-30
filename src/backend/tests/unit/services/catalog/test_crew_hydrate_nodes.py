"""CrewService hydrates stale crew.nodes tool_configs from the live task row.

The Task editor writes tool_configs to the tasks table, but the crew's embedded
canvas snapshot is not resynced — so a reloaded canvas / flow drill-through showed
a stale dataset_id. On load we overlay each task node's tool_configs from its live
task row (in memory, display only) — and only from task rows of the crew's own
workspace, so an imported crew never pulls another tenant's tool_configs in.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.catalog.crews import CrewService

_TS = "src.services.catalog.tasks.TaskService"


WS = "ws-a"


def _crew(nodes, task_ids, group_id=WS):
    return SimpleNamespace(task_ids=task_ids, nodes=nodes, group_id=group_id)


@pytest.mark.asyncio
async def test_overlays_task_node_tool_configs_from_live_task_row():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {
                "id": "task-t1",
                "type": "taskNode",
                "data": {
                    "taskId": "t1",
                    "tool_configs": {"UCMV": {"dataset_id": "OLD63"}},
                },
            },
            {"id": "agent-a1", "type": "agentNode", "data": {}},
        ],
        task_ids=["t1"],
    )
    live = SimpleNamespace(tool_configs={"UCMV": {"dataset_id": "NEW"}}, group_id=WS)
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock(return_value=live)
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    assert out.nodes[0]["data"]["tool_configs"]["UCMV"]["dataset_id"] == "NEW"
    assert out.nodes[1]["data"] == {}  # agent node untouched


@pytest.mark.asyncio
async def test_derives_task_id_from_node_id_when_no_taskId():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[{"id": "task-t1", "type": "taskNode", "data": {"tool_configs": {}}}],
        task_ids=["t1"],
    )
    live = SimpleNamespace(tool_configs={"UCMV": {"dataset_id": "NEW"}}, group_id=WS)
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock(return_value=live)
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    assert out.nodes[0]["data"]["tool_configs"] == {"UCMV": {"dataset_id": "NEW"}}


@pytest.mark.asyncio
async def test_noop_when_task_row_has_no_tool_configs():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {
                "id": "task-t1",
                "type": "taskNode",
                "data": {
                    "taskId": "t1",
                    "tool_configs": {"UCMV": {"dataset_id": "OLD"}},
                },
            }
        ],
        task_ids=["t1"],
    )
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock(return_value=SimpleNamespace(tool_configs=None))
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    assert out.nodes[0]["data"]["tool_configs"]["UCMV"]["dataset_id"] == "OLD"


@pytest.mark.asyncio
async def test_defensive_on_lookup_error_never_raises():
    svc = CrewService(session=MagicMock())
    crew = _crew(
        nodes=[
            {"id": "task-t1", "type": "taskNode", "data": {"tool_configs": {"x": 1}}}
        ],
        task_ids=["t1"],
    )
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock(side_effect=RuntimeError("boom"))
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    assert out is crew  # swallowed; crew returned unchanged


@pytest.mark.asyncio
async def test_none_crew_is_safe():
    svc = CrewService(session=MagicMock())
    assert await svc._hydrate_task_nodes_from_tasks(None) is None


def _one_task_crew(group_id=WS):
    return _crew(
        nodes=[
            {
                "id": "task-t1",
                "type": "taskNode",
                "data": {"taskId": "t1", "tool_configs": {"Tool": {"key": "own"}}},
            }
        ],
        task_ids=["t1"],
        group_id=group_id,
    )


@pytest.mark.asyncio
async def test_a_task_row_of_another_workspace_is_never_overlaid():
    # An imported crew still names the source workspace's task id.
    svc = CrewService(session=MagicMock())
    crew = _one_task_crew()
    foreign = SimpleNamespace(
        tool_configs={"Tool": {"key": "other-ws-secret"}}, group_id="ws-b"
    )
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock(return_value=foreign)
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    assert out.nodes[0]["data"]["tool_configs"] == {"Tool": {"key": "own"}}


@pytest.mark.asyncio
async def test_a_crew_without_a_workspace_reads_no_task_rows():
    svc = CrewService(session=MagicMock())
    crew = _one_task_crew(group_id=None)
    with patch(_TS) as ts:
        ts.return_value.get = AsyncMock()
        out = await svc._hydrate_task_nodes_from_tasks(crew)

    ts.return_value.get.assert_not_called()
    assert out.nodes[0]["data"]["tool_configs"] == {"Tool": {"key": "own"}}
