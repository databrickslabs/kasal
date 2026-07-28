"""Building the Remote Agent tool.

The regression this file exists for: ``_create_tool_impl`` looks the tool title
up in ``_tool_implementations`` and returns None when it is missing — BEFORE
reaching any of the per-tool branches. A branch added further down cannot rescue
a tool the map does not know, so the tool was silently unbuildable while looking
correct in the seed catalogue, in the picker and in the factory's own branch.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import src.services.a2a.a2a_client.agent_service  # noqa: F401  (patch target)
from src.services.tools.a2a_tool_builder import build_a2a_tools


def _plan(name="Researcher"):
    return {
        "name": name,
        "interface_url": "https://remote.example.com/a2a/v1",
        "api_key": None,
        "auth_type": "obo",
        "timeout_seconds": 300,
        "skills": [{"id": "research", "name": "Research", "description": "digs"}],
    }


def _service(rows, plan=None):
    class _Repo:
        async def list_enabled_for_group(self, group_ids):
            return rows

    class _Svc:
        def __init__(self, session):
            self.repository = _Repo()

        async def resolve_for_call(self, name, group_ids):
            return plan if plan is not None else _plan(name)

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return (
        patch("src.services.a2a.a2a_client.agent_service.A2AAgentService", _Svc),
        patch("src.db.session.get_isolated_db_session", return_value=session),
    )


class TestRegistration:
    def test_the_factory_knows_the_seeded_title(self):
        """The lookup is by TITLE and returns None for anything missing. Both
        the seeded title and the class name are registered because agent configs
        reference tools either way."""
        from src.services.tools.tool_factory import ToolFactory

        implementations = ToolFactory.__init__.__doc__ or ""
        del implementations  # the map is built in __init__; assert on an instance

        import inspect

        source = inspect.getsource(ToolFactory.__init__)
        assert '"Remote Agent"' in source
        assert '"A2AAgentTool"' in source


class TestBuilding:
    @pytest.mark.asyncio
    async def test_every_enabled_remote_becomes_a_tool(self):
        """An operator who attached one remote and ticked the tool means "let
        the agent use it" — making them also type its name is a configuration
        step that can only be got wrong."""
        rows = [SimpleNamespace(name="Researcher"), SimpleNamespace(name="Auditor")]
        p_svc, p_session = _service(rows)
        with p_svc, p_session:
            tools = await build_a2a_tools(group_ids=["acme"])

        assert [t.agent_name for t in tools] == ["Researcher", "Auditor"]

    @pytest.mark.asyncio
    async def test_a_named_subset_is_honoured(self):
        rows = [SimpleNamespace(name="Researcher"), SimpleNamespace(name="Auditor")]
        p_svc, p_session = _service(rows)
        with p_svc, p_session:
            tools = await build_a2a_tools(
                tool_config={"agent_name": "Auditor"}, group_ids=["acme"]
            )

        assert [t.agent_name for t in tools] == ["Auditor"]

    @pytest.mark.asyncio
    async def test_the_remotes_skills_reach_the_tool_description(self):
        """That description is what the calling model selects on."""
        p_svc, p_session = _service([SimpleNamespace(name="Researcher")])
        with p_svc, p_session:
            tools = await build_a2a_tools(group_ids=["acme"])

        assert "research" in tools[0].description

    @pytest.mark.asyncio
    async def test_no_group_context_builds_nothing(self):
        """Rather than reaching into every workspace's remotes."""
        assert await build_a2a_tools(group_ids=[]) == []

    @pytest.mark.asyncio
    async def test_nothing_configured_is_an_empty_list_not_an_error(self):
        """It leaves the agent without a capability it was never given, rather
        than with one that errors on first use."""
        p_svc, p_session = _service([])
        with p_svc, p_session:
            assert await build_a2a_tools(group_ids=["acme"]) == []

    @pytest.mark.asyncio
    async def test_the_obo_token_only_goes_to_obo_remotes(self):
        """Sending a user's Databricks token to a remote that authenticates its
        own way would leak it to a third party for nothing."""
        plan = _plan()
        plan["auth_type"] = "api_key"
        p_svc, p_session = _service([SimpleNamespace(name="Researcher")], plan=plan)
        with p_svc, p_session:
            tools = await build_a2a_tools(user_token="user-token", group_ids=["acme"])

        assert tools[0].user_token is None


class TestCrowding:
    """One tool per remote is what lets each description name that remote's
    skills. The cost is that leaving the selection empty in a workspace with
    many remotes hands the agent a wall of near-identical tools, which degrades
    its choice of every OTHER tool too."""

    @pytest.mark.asyncio
    async def test_an_unselected_workspace_is_capped_and_says_so(self, caplog):
        from src.services.tools import a2a_tool_builder

        rows = [SimpleNamespace(name=f"agent-{i}") for i in range(12)]
        p_svc, p_session = _service(rows)
        with p_svc, p_session, caplog.at_level("WARNING"):
            tools = await build_a2a_tools(group_ids=["acme"])

        assert len(tools) == a2a_tool_builder.MAX_UNSELECTED
        assert "Choose specific agents" in caplog.text

    @pytest.mark.asyncio
    async def test_an_explicit_selection_is_never_capped(self):
        """The operator said what they wanted; the cap exists for the case where
        nobody said anything."""
        from src.services.tools import a2a_tool_builder

        rows = [SimpleNamespace(name=f"agent-{i}") for i in range(12)]
        p_svc, p_session = _service(rows)
        with p_svc, p_session:
            tools = await build_a2a_tools(
                tool_config={"agent_names": [f"agent-{i}" for i in range(8)]},
                group_ids=["acme"],
            )

        assert len(tools) == 8 > a2a_tool_builder.MAX_UNSELECTED
