"""Skills reaching a built agent.

The proposal names this as the thing to test explicitly, because the failure
mode is silent: a skill list the builders do not copy is a field that shows as
configured in the UI and does nothing at run time.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.kernel import agent_skills


def _skill(name="pricing", global_enabled=False):
    return SimpleNamespace(
        name=name,
        description="How we price a deal.",
        body="# Steps",
        enabled=True,
        global_enabled=global_enabled,
        files=[],
    )


def _resolved(skills):
    """Patch the session and the resolver the kernel reaches for."""
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


class TestNames:
    def test_a_list_of_names_is_read(self):
        assert agent_skills.skill_names_of({"skills": ["a", "b"]}) == ["a", "b"]

    def test_a_list_of_objects_is_read(self):
        """The frontend sends names, generation sends rows — the untested shape
        is the one that breaks."""
        assert agent_skills.skill_names_of({"skills": [{"name": "a"}]}) == ["a"]

    def test_a_bare_string_is_read(self):
        assert agent_skills.skill_names_of({"skills": "a"}) == ["a"]

    def test_no_skills_is_empty_not_an_error(self):
        assert agent_skills.skill_names_of({}) == []


class TestInjection:
    @pytest.mark.asyncio
    async def test_the_block_lands_in_backstory_when_there_is_no_template(self):
        """CrewAI's default system prompt embeds backstory; a block written
        anywhere else never reaches the model."""
        kwargs = {"backstory": "You are careful."}
        p_session, p_resolve = _resolved([_skill()])
        with p_session, p_resolve:
            count = await agent_skills.inject_skills(
                kwargs, {"skills": ["pricing"]}, group_id="acme"
            )

        assert count == 1
        assert "You are careful." in kwargs["backstory"]
        assert "<available_skills>" in kwargs["backstory"]

    @pytest.mark.asyncio
    async def test_a_custom_system_template_wins(self):
        kwargs = {"backstory": "b", "system_template": "You are an agent."}
        p_session, p_resolve = _resolved([_skill()])
        with p_session, p_resolve:
            await agent_skills.inject_skills(
                kwargs, {"skills": ["pricing"]}, group_id="a"
            )

        assert "<available_skills>" in kwargs["system_template"]
        assert "<available_skills>" not in kwargs["backstory"]

    @pytest.mark.asyncio
    async def test_the_tools_are_equipped_alongside_the_block(self):
        """A list the model cannot act on is worse than no list — it spends
        tokens and produces nothing."""
        kwargs = {"backstory": "b", "tools": []}
        p_session, p_resolve = _resolved([_skill()])
        with p_session, p_resolve:
            await agent_skills.inject_skills(
                kwargs, {"skills": ["pricing"]}, group_id="a"
            )

        names = {t.name for t in kwargs["tools"]}
        assert names == {"load_skill", "read_skill_file"}

    @pytest.mark.asyncio
    async def test_tools_are_not_duplicated(self):
        """A duplicate tool name is how a tool-calling loop starts behaving
        unpredictably."""
        existing = SimpleNamespace(name="load_skill")
        kwargs = {"backstory": "b", "tools": [existing]}
        p_session, p_resolve = _resolved([_skill()])
        with p_session, p_resolve:
            await agent_skills.inject_skills(
                kwargs, {"skills": ["pricing"]}, group_id="a"
            )

        assert [t.name for t in kwargs["tools"]].count("load_skill") == 1

    @pytest.mark.asyncio
    async def test_an_agent_with_no_skills_is_left_untouched(self):
        """No block, no tools, no cost — which is most agents."""
        kwargs = {"backstory": "b", "tools": []}
        p_session, p_resolve = _resolved([])
        with p_session, p_resolve:
            count = await agent_skills.inject_skills(kwargs, {}, group_id="a")

        assert count == 0
        assert kwargs == {"backstory": "b", "tools": []}

    @pytest.mark.asyncio
    async def test_a_database_failure_costs_the_skills_not_the_run(self):
        with patch(
            "src.db.session.get_isolated_db_session", side_effect=RuntimeError("no db")
        ):
            count = await agent_skills.inject_skills(
                {"backstory": "b"}, {"skills": ["pricing"]}, group_id="a"
            )
        assert count == 0


class TestSpecPlumbing:
    """The field has to survive each path's normalisation.

    The flow path builds its spec from an explicit whitelist, so a field missing
    from that list is dropped silently — which for skills means an agent that
    looks configured and has none.
    """

    def test_the_flow_path_copies_skills_onto_the_spec(self):
        from src.services.flow_builder.modules.agent_adapter import AgentConfig

        agent_data = SimpleNamespace(
            role="r",
            goal="g",
            backstory="b",
            llm="model",
            skills=["pricing"],
        )
        spec = AgentConfig._agent_data_to_spec(agent_data)
        assert spec.get("skills") == ["pricing"]

    def test_the_crew_path_passes_its_config_through_as_the_spec(self):
        """It hands agent_config to build_agent directly, so anything on the
        config reaches the builder — including skills."""
        import inspect

        from src.services.agent_builder import agent_adapter

        source = inspect.getsource(agent_adapter.create_agent)
        assert "build_agent_with_tools(\n        agent_config," in source
