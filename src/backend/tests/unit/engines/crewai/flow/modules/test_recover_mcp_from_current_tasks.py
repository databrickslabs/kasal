"""Tests for recover_mcp_from_current_tasks.

A flow's startingPoint task IDs are captured at save time; crew edits mint new
task rows, so the flow can point at a stale task whose tool_configs lost the
MCP server the user added. This helper recovers MCP_SERVERS from the crew's
CURRENT task so the flow gets the same tools the crew does.
"""

from src.engines.crewai.paths.flow.modules.flow_processors import (
    recover_mcp_from_current_tasks,
)

MCP = {"MCP_SERVERS": {"servers": ["nemotemoyou"]}}


def test_recovers_when_single_current_task_has_mcp():
    # Flow references stale task 'stale' (empty); crew's current task 'cur' has MCP.
    out = recover_mcp_from_current_tasks(
        {}, "stale", "Gather", [("cur", "Gather", MCP)]
    )
    assert out["MCP_SERVERS"] == {"servers": ["nemotemoyou"]}


def test_recovers_by_name_match_when_multiple_tasks():
    out = recover_mcp_from_current_tasks(
        {},
        "stale",
        "Gather",
        [("other", "Send Email", {}), ("cur", "Gather", MCP)],
    )
    assert "MCP_SERVERS" in out


def test_no_name_match_and_multiple_tasks_does_not_recover():
    # Ambiguous: multiple current tasks, none name-matching → don't guess.
    out = recover_mcp_from_current_tasks(
        {},
        "stale",
        "Gather",
        [("a", "Other A", MCP), ("b", "Other B", {})],
    )
    assert "MCP_SERVERS" not in out


def test_keeps_existing_mcp_untouched():
    existing = {"MCP_SERVERS": {"servers": ["already"]}}
    out = recover_mcp_from_current_tasks(
        existing, "stale", "Gather", [("cur", "Gather", MCP)]
    )
    # Already present → not overwritten.
    assert out["MCP_SERVERS"] == {"servers": ["already"]}


def test_skips_the_same_stale_task_id():
    # The stale task itself (empty) must not be considered a recovery source.
    out = recover_mcp_from_current_tasks(
        {}, "stale", "Gather", [("stale", "Gather", {})]
    )
    assert "MCP_SERVERS" not in out


def test_no_current_tasks_is_noop():
    assert recover_mcp_from_current_tasks({}, "stale", "Gather", []) == {}
