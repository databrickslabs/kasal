"""Skills that feed a tool parameter.

The failure this guards is silent in the worst way: a domain-context skill that
is attached to the agent, shows in the prompt, and never reaches the UCMV
generator's DAX→SQL call — the run succeeds with generic translations.
"""

from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.kernel import agent_skills
from src.services.execution.kernel.skill_tool_context import (
    AGENT_ATTR,
    apply_agent_tool_context,
    apply_tool_context,
    collect_tool_context,
)
from src.services.execution.kernel.task_builder import build_task_args
from src.services.tools.uc_metric_view_generator_tool import UCMetricViewGeneratorTool
from src.services.tools.ucmv_drift_monitor_tool import UCMVDriftMonitorTool


def _skill(
    name="cchbc-context",
    body="# CCHBC\nFiscal calendar 4-4-5.",
    param: Optional[str] = "domain_context",
):
    return SimpleNamespace(
        name=name,
        description="Domain context for the CCHBC model.",
        body=body,
        enabled=True,
        global_enabled=False,
        files=[],
        skill_metadata={"kasal-tool-param": param} if param else {},
    )


class TestCollect:
    def test_only_tagged_skills_feed_a_parameter(self):
        ctx = collect_tool_context([_skill(), _skill(name="pricing", param=None)])
        assert list(ctx) == ["domain_context"]
        assert "4-4-5" in ctx["domain_context"]
        assert "pricing" not in ctx["domain_context"]

    def test_several_skills_for_one_parameter_are_joined_in_order(self):
        ctx = collect_tool_context(
            [_skill(name="a", body="first"), _skill(name="b", body="second")]
        )
        text = ctx["domain_context"]
        assert text.index("first") < text.index("second")

    def test_a_skill_cannot_name_an_arbitrary_parameter(self):
        """A credential or warehouse id must not be settable from skill text."""
        assert collect_tool_context([_skill(param="client_secret")]) == {}

    def test_an_empty_body_feeds_nothing(self):
        assert collect_tool_context([_skill(body="   ")]) == {}


class TestApply:
    def test_the_generator_receives_the_skill_text(self):
        tool = UCMetricViewGeneratorTool()
        apply_tool_context([tool], {"domain_context": "skill text"})
        assert tool._default_config["domain_context"] == "skill text"

    def test_the_drift_monitor_receives_it_too(self):
        """It forwards domain_context to the generator it re-runs."""
        tool = UCMVDriftMonitorTool()
        apply_tool_context([tool], {"domain_context": "skill text"})
        assert tool._default_config["domain_context"] == "skill text"

    def test_an_inline_value_from_an_older_task_is_kept_after_the_skill(self):
        tool = UCMetricViewGeneratorTool(domain_context="inline notes")
        apply_tool_context([tool], {"domain_context": "skill text"})
        assert tool._default_config["domain_context"] == "skill text\n\ninline notes"

    def test_applying_twice_does_not_duplicate(self):
        """The same instance can be in the agent's AND the task's tool list."""
        tool = UCMetricViewGeneratorTool(domain_context="inline notes")
        for _ in range(2):
            apply_tool_context([tool], {"domain_context": "skill text"})
        assert tool._default_config["domain_context"] == "skill text\n\ninline notes"

    def test_tools_that_do_not_declare_the_parameter_are_left_alone(self):
        other = SimpleNamespace(name="Other", _default_config={})
        assert apply_tool_context([other], {"domain_context": "x"}) == 0
        assert other._default_config == {}

    def test_the_task_seam_reads_the_context_off_the_agent(self):
        agent = SimpleNamespace(role="r")
        object.__setattr__(agent, AGENT_ATTR, {"domain_context": "from agent"})
        tool = UCMetricViewGeneratorTool()
        assert apply_agent_tool_context([tool], agent) == 1
        assert tool._default_config["domain_context"] == "from agent"

    def test_an_agent_without_context_changes_nothing(self):
        tool = UCMetricViewGeneratorTool(domain_context="inline")
        assert apply_agent_tool_context([tool], SimpleNamespace(role="r")) == 0
        assert tool._default_config["domain_context"] == "inline"


def _resolved(skills):
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return (
        patch("src.db.session.get_isolated_db_session", return_value=session),
        patch(
            "src.services.skills.loader.resolve_for_agent",
            new=AsyncMock(return_value=skills),
        ),
    )


class TestWiring:
    @pytest.mark.asyncio
    async def test_inject_skills_feeds_the_agents_tools_and_reports_the_context(self):
        tool = UCMetricViewGeneratorTool()
        kwargs = {"backstory": "b", "tools": [tool]}
        context: dict = {}
        p_session, p_resolve = _resolved([_skill()])
        with p_session, p_resolve, patch.object(agent_skills, "_add_skill_tools"):
            await agent_skills.inject_skills(
                kwargs,
                {"skills": ["cchbc-context"]},
                group_id="acme",
                tool_context=context,
            )
        assert "4-4-5" in tool._default_config["domain_context"]
        assert "4-4-5" in context["domain_context"]

    @pytest.mark.asyncio
    async def test_a_task_tool_built_after_the_agent_is_fed_by_build_task_args(self):
        """The UCMV config lives on the TASK, so its tool instance is not one
        inject_skills ever saw."""
        agent = SimpleNamespace(role="r", tools=[])
        object.__setattr__(agent, AGENT_ATTR, {"domain_context": "from skill"})
        tool = UCMetricViewGeneratorTool(catalog="main")
        task_args = await build_task_args(
            {"name": "t", "description": "d", "expected_output": "e"}, agent, [tool]
        )
        assert task_args["tools"][0]._default_config["domain_context"] == "from skill"
