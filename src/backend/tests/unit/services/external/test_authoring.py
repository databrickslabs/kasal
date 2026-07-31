"""Creating a crew from outside the workspace.

Every test here corresponds to something that was WRONG and shipped: the crew
was not saved, then it was saved with no canvas, then the canvas had no task
dependencies, then the agents had no tools. Each was found by using the feature,
which is why they are pinned now.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.external import authoring
from src.services.external.identity import ExternalCaller
from src.services.external.permissions import ExternalPermissionError


class _Ctx:
    def __init__(self, role="admin"):
        self.group_ids = ["acme_corp"]
        self.group_email = "author@acme.com"
        self.access_token = "tok"
        self.user_role = role
        self.highest_role = role
        self.current_user = None

    @property
    def primary_group_id(self):
        return "acme_corp"


def _caller(role="admin"):
    return ExternalCaller(
        group_context=_Ctx(role), protocol="mcp", identifier="author@acme.com"
    )


_GENERATED = {
    "agents": [
        {"id": "a1", "name": "Researcher", "role": "R", "goal": "G", "tools": ["31"]},
        {"id": "a2", "name": "Summariser", "role": "S", "goal": "G2", "tools": []},
    ],
    "tasks": [
        {
            "id": "t1",
            "name": "Research",
            "agent_id": "a1",
            "context": [],
            "tools": ["31"],
        },
        {"id": "t2", "name": "Summarise", "agent_id": "a2", "context": ["Research"]},
    ],
}


def _generation(result=None):
    service = MagicMock()
    service.create_crew_complete = AsyncMock(return_value=result or _GENERATED)
    return (
        patch(
            "src.services.generation.crews.CrewGenerationService", return_value=service
        ),
        service,
    )


def _catalogue():
    service = MagicMock()
    service.create_with_group = AsyncMock(return_value=MagicMock(id="crew-1"))
    return (
        patch("src.services.catalog.crews.CrewService", return_value=service),
        service,
    )


def _tools(names=("SerperDevTool",)):
    service = MagicMock()
    service.get_enabled_tools_for_group = AsyncMock(
        return_value=MagicMock(tools=[MagicMock(title=n) for n in names])
    )
    return (
        patch("src.services.tools.tool_service.ToolService", return_value=service),
        service,
    )


class TestItProducesAWorkableCrew:
    @pytest.mark.asyncio
    async def test_saves_a_crew_row_not_just_agents_and_tasks(self):
        """create_crew_complete makes agents and tasks and NO Crew row — the
        browser assembles the crew afterwards. Without the second step the
        caller was told "created", nothing appeared in the catalogue, and the
        result could not be published because publication addresses a crew id."""
        p_gen, _ = _generation()
        p_cat, catalogue = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            result = await authoring.create_crew(_caller(), "do a thing")

        assert result["crew_id"] == "crew-1"
        catalogue.create_with_group.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_crew_carries_a_canvas(self):
        """The canvas renders FROM crew.nodes. Saved empty, the crew appeared in
        the catalogue and then opened to nothing — unopenable."""
        p_gen, _ = _generation()
        p_cat, catalogue = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "do a thing")

        saved = catalogue.create_with_group.await_args.args[0]
        assert [n.type for n in saved.nodes] == [
            "agentNode",
            "agentNode",
            "taskNode",
            "taskNode",
        ]

    @pytest.mark.asyncio
    async def test_tasks_are_wired_to_their_agent_and_to_each_other(self):
        """Agent->task alone left the run ORDER invisible: a summarise step
        looked unrelated to the research step feeding it."""
        p_gen, _ = _generation()
        p_cat, catalogue = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "do a thing")

        edges = {
            (e.source, e.target)
            for e in catalogue.create_with_group.await_args.args[0].edges
        }
        assert ("agent-a1", "task-t1") in edges
        assert ("agent-a2", "task-t2") in edges
        assert ("task-t1", "task-t2") in edges  # the dependency

    @pytest.mark.asyncio
    async def test_a_dependency_given_by_id_also_wires(self):
        """`context` carries ids in some paths and names in others. Matching one
        and dropping the other produces the same missing-edge symptom for half
        the crews, which reads as intermittent."""
        generated = {
            "agents": [{"id": "a1", "name": "R"}],
            "tasks": [
                {"id": "t1", "name": "First", "agent_id": "a1", "context": []},
                {"id": "t2", "name": "Second", "agent_id": "a1", "context": ["t1"]},
            ],
        }
        p_gen, _ = _generation(generated)
        p_cat, catalogue = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "x")

        edges = {
            (e.source, e.target)
            for e in catalogue.create_with_group.await_args.args[0].edges
        }
        assert ("task-t1", "task-t2") in edges


class TestToolAssignment:
    @pytest.mark.asyncio
    async def test_defaults_to_the_workspace_s_enabled_tools(self):
        """The planner assigns from the list it is GIVEN. Empty meant every
        agent came back with Tools: 0, so the same prompt typed into the canvas
        produced a more capable crew than the MCP one."""
        p_gen, generation = _generation()
        p_cat, _ = _catalogue()
        p_tools, _ = _tools(("SerperDevTool", "PerplexityTool"))
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "search the web")

        request = generation.create_crew_complete.await_args.args[0]
        assert request.tools == ["SerperDevTool", "PerplexityTool"]

    @pytest.mark.asyncio
    async def test_an_explicit_list_wins(self):
        p_gen, generation = _generation()
        p_cat, _ = _catalogue()
        p_tools, _ = _tools(("SerperDevTool",))
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "x", tools=["GenieTool"])

        assert generation.create_crew_complete.await_args.args[0].tools == ["GenieTool"]

    @pytest.mark.asyncio
    async def test_an_explicit_empty_list_means_none(self):
        """Distinct from omitting the argument, which takes the default."""
        p_gen, generation = _generation()
        p_cat, _ = _catalogue()
        p_tools, _ = _tools(("SerperDevTool",))
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(_caller(), "x", tools=[])

        assert generation.create_crew_complete.await_args.args[0].tools == []

    @pytest.mark.asyncio
    async def test_an_unreadable_tool_catalogue_does_not_fail_creation(self):
        """A crew with no tools is still a crew."""
        p_gen, generation = _generation()
        p_cat, _ = _catalogue()
        service = MagicMock()
        service.get_enabled_tools_for_group = AsyncMock(
            side_effect=RuntimeError("nope")
        )
        with (
            p_gen,
            p_cat,
            patch("src.services.tools.tool_service.ToolService", return_value=service),
        ):
            result = await authoring.create_crew(_caller(), "x")

        assert result["crew_id"] == "crew-1"
        assert generation.create_crew_complete.await_args.args[0].tools == []


class TestProcessAndReasoning:
    @pytest.mark.asyncio
    async def test_process_and_effort_reach_the_saved_crew(self):
        p_gen, _ = _generation()
        p_cat, catalogue = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            await authoring.create_crew(
                _caller(), "x", process="hierarchical", reasoning_effort="high"
            )

        saved = catalogue.create_with_group.await_args.args[0]
        assert saved.process == "hierarchical"
        assert saved.reasoning is True
        assert saved.reasoning_config == {"reasoning_effort": "high"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs", [{"process": "nonsense"}, {"reasoning_effort": "extreme"}]
    )
    async def test_invalid_values_are_refused_before_anything_is_generated(
        self, kwargs
    ):
        """Rather than saving a crew that fails later at kickoff for a reason the
        caller cannot see."""
        p_gen, generation = _generation()
        p_cat, _ = _catalogue()
        p_tools, _ = _tools()
        with p_gen, p_cat, p_tools:
            with pytest.raises(ValueError):
                await authoring.create_crew(_caller(), "x", **kwargs)

        generation.create_crew_complete.assert_not_awaited()


class TestPermission:
    @pytest.mark.asyncio
    async def test_an_operator_may_not_create(self):
        with (
            patch(
                "src.services.external.permissions.check_role_in_context",
                return_value=False,
            ),
            patch(
                "src.services.external.permissions.get_effective_role",
                return_value="operator",
            ),
        ):
            with pytest.raises(ExternalPermissionError):
                await authoring.create_crew(_caller("operator"), "x")

    @pytest.mark.asyncio
    async def test_the_check_happens_before_any_generation(self):
        """Generation costs a model call. Refusing after paying for it would be
        both slower and confusing."""
        p_gen, generation = _generation()
        with (
            p_gen,
            patch(
                "src.services.external.permissions.check_role_in_context",
                return_value=False,
            ),
            patch(
                "src.services.external.permissions.get_effective_role",
                return_value="operator",
            ),
        ):
            with pytest.raises(ExternalPermissionError):
                await authoring.create_crew(_caller("operator"), "x")

        generation.create_crew_complete.assert_not_awaited()


class TestItDoesNotPublish:
    @pytest.mark.asyncio
    async def test_creation_leaves_the_crew_unpublished(self):
        """Exposing a crew outside the workspace is a separate decision; doing
        both in one call would hide it from the workspace it affects."""
        p_gen, _ = _generation()
        p_cat, _ = _catalogue()
        p_tools, _ = _tools()
        with (
            p_gen,
            p_cat,
            p_tools,
            patch(
                "src.services.publications.publication.PublicationService"
            ) as publications,
        ):
            result = await authoring.create_crew(_caller(), "x")

        publications.return_value.publish.assert_not_called()
        assert "NOT reachable from outside" in result["note"]
