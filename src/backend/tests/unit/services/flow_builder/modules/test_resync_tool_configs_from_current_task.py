"""Tests for resync_tool_configs_from_current_task + load_current_crew_tasks.

The whole-config version of the MCP drift: a flow's task IDs are frozen at save
time, but crew edits mint new task rows — so a flow can run a STALE task whose
tool_configs still holds an old warehouse_id / dataset_id / enable_reconciliation
while the crew is correct. This is the exact failure that reconciled a flow run
against a stale Power BI dataset and skipped reconciliation. These helpers re-sync
the LIVE config from the crew's current task.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.flow_builder.modules.stale_task_config import (
    load_current_crew_tasks,
    resync_tool_configs_from_current_task,
)

# Frozen snapshot the stale flow task carries: blank warehouse, OLD dataset.
_OLD = {
    "UCMVGeneratorTool": {"warehouse_id": "", "dataset_id": "360d8dc2"},
    "MCP_SERVERS": {"from": "node"},
}
# The crew's CURRENT task: filled warehouse, NEW dataset, reconciliation on.
_CUR = {
    "UCMVGeneratorTool": {
        "warehouse_id": "566a7f95",
        "dataset_id": "1bbd4feb",
        "enable_reconciliation": True,
    }
}


def test_stale_single_task_overlays_live_config():
    eff = {k: dict(v) for k, v in _OLD.items()}
    out = resync_tool_configs_from_current_task(
        eff, "stale", "Generate UCMV", [("cur", "Generate UCMV", _CUR)]
    )
    # Live warehouse / dataset / reconciliation win over the frozen snapshot.
    assert out["UCMVGeneratorTool"]["dataset_id"] == "1bbd4feb"
    assert out["UCMVGeneratorTool"]["warehouse_id"] == "566a7f95"
    assert out["UCMVGeneratorTool"]["enable_reconciliation"] is True
    # Node-only config the current task lacks is NOT dropped (update is additive).
    assert out["MCP_SERVERS"] == {"from": "node"}


def test_live_task_is_left_untouched():
    # The flow's task ID IS among the crew's current IDs → not stale → no overlay.
    eff = {"UCMVGeneratorTool": {"dataset_id": "360d8dc2"}}
    out = resync_tool_configs_from_current_task(
        eff, "cur", "Generate UCMV", [("cur", "Generate UCMV", _CUR)]
    )
    assert out["UCMVGeneratorTool"]["dataset_id"] == "360d8dc2"


def test_name_match_among_multiple_tasks():
    out = resync_tool_configs_from_current_task(
        {},
        "stale",
        "Generate UCMV",
        [("a", "Other", {"x": 1}), ("cur", "Generate UCMV", _CUR)],
    )
    assert out["UCMVGeneratorTool"]["dataset_id"] == "1bbd4feb"


def test_no_name_match_and_multiple_tasks_leaves_stale():
    # Ambiguous: several current tasks, none name-matching → don't guess, stay stale.
    eff = {"UCMVGeneratorTool": {"dataset_id": "360d8dc2"}}
    out = resync_tool_configs_from_current_task(
        eff, "stale", "Generate UCMV", [("a", "Other A", _CUR), ("b", "Other B", _CUR)]
    )
    assert out["UCMVGeneratorTool"]["dataset_id"] == "360d8dc2"


def test_empty_current_tasks_is_noop():
    assert resync_tool_configs_from_current_task({"a": 1}, "stale", "x", []) == {"a": 1}


def test_single_task_with_empty_config_does_not_overlay():
    eff = {"UCMVGeneratorTool": {"dataset_id": "360d8dc2"}}
    out = resync_tool_configs_from_current_task(
        eff, "stale", "Generate UCMV", [("cur", "Generate UCMV", {})]
    )
    assert out["UCMVGeneratorTool"]["dataset_id"] == "360d8dc2"


def _task(name, cfg):
    m = MagicMock()
    m.name = name
    m.tool_configs = cfg
    return m


@pytest.mark.asyncio
async def test_load_current_crew_tasks_builds_triples_and_skips_missing():
    crew = MagicMock()
    crew.task_ids = ["t1", "t2", "t3"]
    lookup = {"t1": _task("A", {"k": 1}), "t3": _task("C", {})}  # t2 missing → None
    repo = MagicMock()
    repo.get = AsyncMock(side_effect=lambda tid: lookup.get(tid))

    out = await load_current_crew_tasks(crew, repo)

    assert [(n, c) for _tid, n, c in out] == [("A", {"k": 1}), ("C", {})]


@pytest.mark.asyncio
async def test_load_current_crew_tasks_is_defensive_on_error():
    crew = MagicMock()
    crew.task_ids = ["bad"]
    repo = MagicMock()
    repo.get = AsyncMock(side_effect=RuntimeError("boom"))

    assert await load_current_crew_tasks(crew, repo) == []
