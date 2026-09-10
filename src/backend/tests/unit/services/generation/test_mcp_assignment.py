import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.flow_builder.mcp_assignments import apply_flow_mcp_assignments
from src.services.generation.mcp_assignment import (
    assign_mcps_to_tasks,
    describe_selected_mcps,
)


@pytest.mark.asyncio
async def test_assigns_retrieval_and_presentation_separately_and_leaves_analysis_empty():
    tasks = {
        "fetch": {"scope": "Read orders"},
        "analyze": {"scope": "Analyze fetched orders"},
        "deck": {"scope": "Create slides"},
    }
    capabilities = [
        {
            "name": "postgres",
            "tools": [{"name": "query", "description": "Read tables"}],
        },
        {"name": "studio", "tools": [{"name": "deck", "description": "Create slides"}]},
    ]
    payload = {
        "assignments": [
            {"task_id": key, "servers": servers}
            for key, servers in [
                ("fetch", ["postgres"]),
                ("analyze", []),
                ("deck", ["studio"]),
            ]
        ]
    }
    with patch(
        "src.services.generation.mcp_assignment.LLMManager.completion",
        new_callable=AsyncMock,
        return_value=json.dumps(payload),
    ) as llm:
        result = await assign_mcps_to_tasks(tasks, capabilities, "model")
    assert result == {"fetch": ["postgres"], "analyze": [], "deck": ["studio"]}
    supplied = json.loads(llm.call_args.kwargs["messages"][1]["content"])
    assert supplied["selected_mcps"] == capabilities
    assert supplied["tasks"] == tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "assignments",
    [
        [{"task_id": "a", "servers": ["unselected"]}],
        [{"task_id": "other", "servers": ["selected"]}],
        [],
        [{"task_id": "a", "servers": []}, {"task_id": "a", "servers": []}],
    ],
)
async def test_invalid_assignments_fail_after_one_repair(assignments):
    with patch(
        "src.services.generation.mcp_assignment.LLMManager.completion",
        new_callable=AsyncMock,
        return_value=json.dumps({"assignments": assignments}),
    ) as llm:
        with pytest.raises(ValueError, match="Could not assign"):
            await assign_mcps_to_tasks({"a": {}}, [{"name": "selected"}], "m")
    assert llm.await_count == 2


@pytest.mark.asyncio
async def test_no_selection_does_not_discover_or_call_model():
    with patch(
        "src.services.generation.mcp_assignment.LLMManager.completion",
        new_callable=AsyncMock,
    ) as llm:
        assert await describe_selected_mcps([], None) == []
        assert await assign_mcps_to_tasks({"a": {}}, [], "m") == {}
        llm.assert_not_called()


@pytest.mark.asyncio
async def test_discovery_refuses_disabled_or_other_workspace_selection():
    session = AsyncMock()
    with (
        patch("src.db.session.routed_scoped_session", return_value=session),
        patch("src.services.mcp.mcp_client.service.MCPService") as service,
    ):
        service.return_value.get_all_servers_effective = AsyncMock(
            return_value=SimpleNamespace(servers=[SimpleNamespace(name="allowed")])
        )
        with pytest.raises(ValueError, match="no longer enabled"):
            await describe_selected_mcps(
                ["forbidden"],
                SimpleNamespace(primary_group_id="team", access_token="secret"),
            )
        service.return_value.get_servers_by_names_group_aware.assert_not_called()


@pytest.mark.asyncio
async def test_metadata_uses_runtime_auth_and_excludes_connection_secrets():
    group = SimpleNamespace(primary_group_id="team", access_token="user-secret")
    config = {
        "id": "1",
        "name": "orders",
        "api_key": "server-secret",
        "server_url": "https://example.com/mcp",
    }
    server = SimpleNamespace(name="orders", model_dump=lambda: config)
    with (
        patch("src.db.session.routed_scoped_session", return_value=AsyncMock()),
        patch("src.services.mcp.mcp_client.service.MCPService") as service,
        patch(
            "src.services.tools.mcp_integration.MCPIntegration._create_tools_for_server",
            new_callable=AsyncMock,
        ) as discover,
    ):
        service.return_value.get_all_servers_effective = AsyncMock(
            return_value=SimpleNamespace(servers=[server])
        )
        service.return_value.get_servers_by_names_group_aware = AsyncMock(
            return_value=[server]
        )
        discover.return_value = [
            SimpleNamespace(name="query", description="Read orders")
        ]
        result = await describe_selected_mcps(["orders"], group)
    assert result == [
        {"name": "orders", "tools": [{"name": "query", "description": "Read orders"}]}
    ]
    assert discover.call_args.kwargs == {
        "user_token": "user-secret",
        "group_id": "team",
    }
    assert "secret" not in json.dumps(result)


def test_flow_assignment_only_applies_to_matching_crew_task_and_preserves_catalog_configs():
    configs = {"other": {"key": "value"}, "MCP_SERVERS": {"servers": ["existing"]}}
    flow = {
        "nodes": [
            {
                "data": {
                    "crewId": "crew-a",
                    "mcpAssignments": {"task-a": ["postgres"], "task-b": ["studio"]},
                }
            }
        ]
    }
    result = apply_flow_mcp_assignments(configs, "task-a", "crew-a", flow)
    assert result["MCP_SERVERS"]["servers"] == ["existing", "postgres"]
    assert configs["MCP_SERVERS"]["servers"] == ["existing"]
    assert apply_flow_mcp_assignments(configs, "task-a", "crew-b", flow) is configs
    assert apply_flow_mcp_assignments(configs, "task-c", "crew-a", flow) is configs
