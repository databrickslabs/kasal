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


async def _launched_config(monkeypatch, **fields):
    """Start a slide edit with its lookups stubbed; return the launched config."""
    monkeypatch.setattr(
        agent_refine.ToolService,
        "get_enabled_tools_for_group",
        AsyncMock(return_value=SimpleNamespace(tools=[])),
    )
    monkeypatch.setattr(
        agent_refine, "_system_prompt", AsyncMock(return_value="Return one slide")
    )
    launch = AsyncMock(return_value={"execution_id": "run-1"})
    monkeypatch.setattr(agent_refine.ExecutionService, "create_execution", launch)
    body = SlideRefineRequest(
        mode="refine",
        instruction="Tighten the copy",
        slide='<section class="slide">Original</section>',
        model="databricks-claude-opus-5",
        **fields,
    )
    group_context = SimpleNamespace(primary_group_id="workspace-1")
    await agent_refine.start_slide_refinement(body, None, group_context)
    return launch.call_args.kwargs["config"]


async def _agent_llm(monkeypatch, agent):
    """Build the editor's LLM as the light-agent run does, on a stub endpoint."""
    from src.services.execution.kernel import agent_builder

    def endpoint(model, group_id, temperature=None):
        return SimpleNamespace(
            model=model, max_tokens=200000, max_completion_tokens=None
        )

    engine = SimpleNamespace(name="test", build_llm=AsyncMock(side_effect=endpoint))
    monkeypatch.setattr(agent_builder, "active_harness", lambda: engine)
    return await agent_builder.build_agent_llm(agent, group_id="workspace-1")


@pytest.mark.asyncio
async def test_slide_edit_effort_reaches_the_editor_llm(monkeypatch):
    from src.core.llm.effort import resolve_effort

    config = await _launched_config(monkeypatch, execution_effort={"tier": "high"})
    profile = resolve_effort({"tier": "high"})
    # The chat's mechanism: the run carries it and the agent is stamped with it.
    assert config.inputs["execution_effort"] == {"tier": "high"}
    agent = next(iter(config.agents_yaml.values()))
    assert agent["execution_effort"] == {"tier": "high"}
    assert agent["execution_effort_override"] is True
    assert agent["max_iter"] == profile["max_iter"]
    assert agent["max_execution_time"] == profile["max_execution_time"]
    llm = await _agent_llm(monkeypatch, agent)
    assert llm.model == "databricks-claude-opus-5"
    assert llm.thinking_effort == "high"
    assert llm.reasoning_effort is None
    assert llm.max_tokens == profile["max_output_tokens"]


@pytest.mark.asyncio
async def test_slide_edit_without_effort_keeps_the_model_settings(monkeypatch):
    config = await _launched_config(monkeypatch)
    assert "execution_effort" not in config.inputs
    assert "resolved_effort" not in config.inputs
    agent = next(iter(config.agents_yaml.values()))
    assert "execution_effort" not in agent
    llm = await _agent_llm(monkeypatch, agent)
    assert llm.max_tokens == 200000
    assert not hasattr(llm, "thinking_effort")


@pytest.mark.asyncio
async def test_refine_requires_current_slide():
    with pytest.raises(ValueError, match="slide to revise"):
        await agent_refine.start_slide_refinement(
            SlideRefineRequest(mode="refine", instruction="Improve"), None, None
        )
