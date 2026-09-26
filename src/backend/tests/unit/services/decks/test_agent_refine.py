from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.schemas.deck import SlideRefineRequest
from src.services.decks import agent_refine


@pytest.mark.asyncio
async def test_slide_edit_uses_shared_agent_capabilities(monkeypatch):
    enabled = AsyncMock(
        return_value=SimpleNamespace(
            tools=[
                SimpleNamespace(title="SearchTool", id=1),
                SimpleNamespace(title="OtherTool", id=2),
            ]
        )
    )
    monkeypatch.setattr(
        agent_refine.ToolService, "get_enabled_tools_for_group", enabled
    )
    monkeypatch.setattr(
        agent_refine, "_system_prompt", AsyncMock(return_value="Return one slide")
    )
    launch = AsyncMock(return_value={"execution_id": "run-1"})
    monkeypatch.setattr(agent_refine.ExecutionService, "create_execution", launch)
    body = SlideRefineRequest(
        mode="refine",
        instruction="Search online and improve this slide",
        slide='<section class="slide">Original</section>',
        model="test-model",
        tools=["1", "DisabledTool"],
        mcp_servers=["browser"],
        agentbricks_endpoints=["researcher"],
        skills=["research"],
    )
    group_context = SimpleNamespace(primary_group_id="workspace-1")
    result = await agent_refine.start_slide_refinement(body, None, group_context)
    assert result == {"job_id": "run-1"}
    config = launch.call_args.kwargs["config"]
    assert config.execution_type == "agent"
    assert config.output_contract == "slide"
    assert config.model == "test-model"
    agent = next(iter(config.agents_yaml.values()))
    assert "SearchTool" in agent["tools"]
    assert "OtherTool" not in agent["tools"]
    assert "DisabledTool" not in agent["tools"]
    assert agent["tool_configs"]["MCP_SERVERS"] == {"servers": ["browser"]}
    assert agent["tool_configs"]["AgentBricksTool"]["endpointName"] == ["researcher"]
    assert agent["memory"] is False
    task = next(iter(config.tasks_yaml.values()))
    assert body.instruction in task["description"]
    assert body.slide in task["description"]
    # The whole context, as ToolService expects: a bare group id string made it
    # fall back to base tools and ignore the workspace's tool mappings.
    enabled.assert_awaited_once_with(group_context)


@pytest.mark.asyncio
async def test_refine_requires_current_slide():
    with pytest.raises(ValueError, match="slide to revise"):
        await agent_refine.start_slide_refinement(
            SlideRefineRequest(mode="refine", instruction="Improve"), None, None
        )
