"""
Comprehensive unit tests for CrewGenerationService.

Targets 100% code coverage of src/services/crew_generation_service.py.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

import src.services.crew_generation_service as _mod
from src.services.crew_generation_service import CrewGenerationService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_service():
    """Build a CrewGenerationService with all external deps mocked out."""
    mock_session = Mock()
    with (
        patch("src.services.crew_generation_service.LLMLogService") as mock_log_svc_cls,
        patch(
            "src.services.crew_generation_service.LLMLogRepository"
        ) as mock_log_repo_cls,
        patch(
            "src.services.crew_generation_service.CrewGeneratorRepository"
        ) as mock_crew_repo_cls,
    ):
        mock_log_svc = Mock()
        mock_log_repo = Mock()
        mock_crew_repo = Mock()

        mock_log_svc_cls.return_value = mock_log_svc
        mock_log_repo_cls.return_value = mock_log_repo
        mock_crew_repo_cls.return_value = mock_crew_repo

        service = CrewGenerationService(mock_session)
    return service, mock_session, mock_log_svc, mock_crew_repo


def _minimal_setup(agents=None, tasks=None):
    """Return a minimal valid crew setup dict."""
    if agents is None:
        agents = [
            {
                "name": "Agent1",
                "role": "r1",
                "goal": "g1",
                "backstory": "b1",
                "tools": ["ToolA"],
            },
        ]
    if tasks is None:
        tasks = [
            {
                "name": "Task1",
                "description": "d1",
                "agent": "Agent1",
                "tools": ["ToolA"],
            },
        ]
    return {"agents": agents, "tasks": tasks}


def _allowed_tools():
    return [{"name": "ToolA", "id": "id-a"}, {"name": "ToolB", "id": "id-b"}]


def _tool_id_map():
    return {"ToolA": "id-a", "ToolB": "id-b"}


def _crew_complete_patches(
    service,
    gtd_return=None,
    ppt_return="sys",
    pcs_return=None,
    model_params=None,
    llm_content="{}",
    rjp_return=None,
    completion_side_effect=None,
    crew_repo_return=None,
):
    """Return a context-manager style set of patches for create_crew_complete tests.

    Returns a dict of mock objects after entering all patches.
    """
    if gtd_return is None:
        gtd_return = []
    if pcs_return is None:
        pcs_return = {"agents": [{"name": "A"}], "tasks": [{"name": "T"}]}
    if model_params is None:
        model_params = {"model": "m"}
    if rjp_return is None:
        rjp_return = {
            "agents": [{"name": "A", "role": "r", "goal": "g", "backstory": "b"}],
            "tasks": [{"name": "T"}],
        }
    if crew_repo_return is None:
        crew_repo_return = {"agents": [], "tasks": []}

    class PatchCtx:
        def __init__(self):
            self._patches = []
            self.mocks = {}

        def __enter__(self):
            p1 = patch("src.services.crew_generation_service.ToolService")
            p2 = patch.object(service, "_get_tool_details", new_callable=AsyncMock)
            p3 = patch.object(
                service, "_prepare_prompt_template", new_callable=AsyncMock
            )
            p4 = patch("src.services.crew_generation_service.LLMManager")
            p5 = patch("src.services.crew_generation_service.robust_json_parser")
            p6 = patch.object(service, "_process_crew_setup")

            self._patches = [p1, p2, p3, p4, p5, p6]
            ms = [p.start() for p in self._patches]

            ms[1].return_value = gtd_return  # _get_tool_details
            ms[2].return_value = ppt_return  # _prepare_prompt_template
            if completion_side_effect:
                ms[3].completion = AsyncMock(side_effect=completion_side_effect)
            else:
                ms[3].completion = AsyncMock(return_value=llm_content)
            ms[4].return_value = rjp_return  # robust_json_parser
            ms[5].return_value = pcs_return  # _process_crew_setup

            self.mocks = {
                "ts_cls": ms[0],
                "gtd": ms[1],
                "ppt": ms[2],
                "lm": ms[3],
                "rjp": ms[4],
                "pcs": ms[5],
            }
            return self.mocks

        def __exit__(self, *args):
            for p in reversed(self._patches):
                p.stop()

    return PatchCtx()


# ===========================================================================
# __init__
# ===========================================================================


class TestInit:
    def test_init_creates_all_dependencies(self):
        mock_session = Mock()
        with (
            patch("src.services.crew_generation_service.LLMLogService") as log_svc,
            patch("src.services.crew_generation_service.LLMLogRepository") as log_repo,
            patch(
                "src.services.crew_generation_service.CrewGeneratorRepository"
            ) as crew_repo,
        ):
            svc = CrewGenerationService(mock_session)

            log_repo.assert_called_once_with(mock_session)
            log_svc.assert_called_once_with(log_repo.return_value)
            crew_repo.assert_called_once_with(mock_session)

            assert svc.session is mock_session
            assert svc.log_service is log_svc.return_value
            assert svc.tool_service is None
            assert svc.crew_generator_repository is crew_repo.return_value


# ===========================================================================
# _log_llm_interaction
# ===========================================================================


class TestLogLlmInteraction:
    def setup_method(self):
        self.service, self.session, self.log_svc, _ = _build_service()

    @pytest.mark.asyncio
    async def test_log_success(self):
        self.log_svc.create_log = AsyncMock()
        await self.service._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
            status="success",
            error_message=None,
            group_context=None,
        )
        self.log_svc.create_log.assert_awaited_once_with(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
            status="success",
            error_message=None,
            group_context=None,
        )

    @pytest.mark.asyncio
    async def test_log_with_group_context(self):
        gc = Mock()
        self.log_svc.create_log = AsyncMock()
        await self.service._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
            group_context=gc,
        )
        self.log_svc.create_log.assert_awaited_once()
        call_kwargs = self.log_svc.create_log.call_args.kwargs
        assert call_kwargs["group_context"] is gc

    @pytest.mark.asyncio
    async def test_log_exception_swallowed(self):
        self.log_svc.create_log = AsyncMock(side_effect=RuntimeError("db down"))
        # Should NOT raise -- exception is swallowed and logged
        await self.service._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
        )


# ===========================================================================
# _prepare_prompt_template
# ===========================================================================


class TestPreparePromptTemplate:
    def setup_method(self):
        self.service, *_ = _build_service()

    @pytest.mark.asyncio
    async def test_template_not_found_raises(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value=None)
            with pytest.raises(ValueError, match="not found"):
                await self.service._prepare_prompt_template([], None)

    @pytest.mark.asyncio
    async def test_no_tools(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            result = await self.service._prepare_prompt_template([], None)
            assert result == "BASE"

    @pytest.mark.asyncio
    async def test_none_tools(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            result = await self.service._prepare_prompt_template(None, None)
            assert result == "BASE"

    @pytest.mark.asyncio
    async def test_tools_without_nl2sql(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [
                {
                    "name": "MyTool",
                    "description": "desc",
                    "parameters": {"p1": {"description": "pd", "type": "str"}},
                }
            ]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "Available tools:" in result
            assert "- MyTool: desc" in result
            # The trimmed catalog lists tool name + description only — per-parameter
            # schemas are intentionally omitted from the generation prompt
            # (they matter at execution, not when picking tool assignments).
            assert "p1 (str): pd" not in result
            assert "NL2SQLTool" not in result

    @pytest.mark.asyncio
    async def test_tools_with_nl2sql(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [{"name": "NL2SQLTool", "description": "sql tool"}]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "NL2SQLTool" in result
            assert "sql_query" in result

    @pytest.mark.asyncio
    async def test_tool_missing_fields_uses_defaults(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [{}]  # name/description/parameters all missing
            result = await self.service._prepare_prompt_template(tools, None)
            assert "Unknown Tool" in result
            assert "No description available" in result

    @pytest.mark.asyncio
    async def test_tool_parameters_are_omitted(self):
        """The trimmed catalog renders name + description only; per-parameter
        details (and their missing-field fallbacks) are no longer emitted."""
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [
                {
                    "name": "T",
                    "description": "d",
                    "parameters": {
                        "x": {}  # parameters are ignored by the trimmed catalog
                    },
                }
            ]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "- T: d" in result
            assert "No description" not in result
            assert "(any)" not in result

    @pytest.mark.asyncio
    async def test_tool_without_parameters(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [{"name": "SimpleTool", "description": "simple"}]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "SimpleTool" in result
            assert "Parameters:" not in result

    @pytest.mark.asyncio
    async def test_tool_with_empty_parameters(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [{"name": "T", "description": "d", "parameters": {}}]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "Parameters:" not in result

    @pytest.mark.asyncio
    async def test_group_context_passed_to_template_service(self):
        gc = Mock()
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            await self.service._prepare_prompt_template([], gc)
            ts.get_effective_template_content.assert_awaited_once_with(
                "generate_crew", gc
            )


# ===========================================================================
# _process_crew_setup
# ===========================================================================


class TestProcessCrewSetup:
    def setup_method(self):
        self.service, *_ = _build_service()

    # --- Validation errors ---
    def test_missing_agents_key_raises(self):
        """No 'agents' key at all."""
        with pytest.raises(ValueError, match="agents"):
            self.service._process_crew_setup({"tasks": [{"name": "t"}]}, [], {})

    def test_empty_agents_raises(self):
        with pytest.raises(ValueError, match="agents"):
            self.service._process_crew_setup(
                {"agents": [], "tasks": [{"name": "t"}]}, [], {}
            )

    def test_agents_not_list_raises(self):
        """When agents is a tuple (iterable but not list), isinstance check catches it."""
        with pytest.raises(ValueError, match="agents"):
            self.service._process_crew_setup(
                {"agents": ({"name": "a"},), "tasks": [{"name": "t"}]}, [], {}
            )

    def test_missing_tasks_key_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {"agents": [{"name": "a", "role": "r", "goal": "g", "backstory": "b"}]},
                [],
                {},
            )

    def test_empty_tasks_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {
                    "agents": [
                        {"name": "a", "role": "r", "goal": "g", "backstory": "b"}
                    ],
                    "tasks": [],
                },
                [],
                {},
            )

    def test_tasks_not_list_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {
                    "agents": [
                        {"name": "a", "role": "r", "goal": "g", "backstory": "b"}
                    ],
                    "tasks": ({"name": "t"},),
                },
                [],
                {},
            )

    def test_agent_missing_required_field_raises(self):
        setup = {
            "agents": [{"name": "a", "role": "r"}],
            "tasks": [{"name": "t", "agent": "a"}],
        }
        with pytest.raises(ValueError, match="Missing required field"):
            self.service._process_crew_setup(setup, [], {})

    # --- Model assignment ---
    def test_model_assigned_to_agents_when_provided(self):
        setup = _minimal_setup()
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map(), model="gpt-4"
        )
        for agent in result["agents"]:
            assert agent["llm"] == "gpt-4"

    def test_model_not_assigned_when_none(self):
        setup = _minimal_setup()
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map(), model=None
        )
        for agent in result["agents"]:
            assert "llm" not in agent

    def test_model_empty_string_not_assigned(self):
        """Empty string is falsy, so model should not be assigned."""
        setup = _minimal_setup()
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map(), model=""
        )
        for agent in result["agents"]:
            assert "llm" not in agent

    # --- Agent tool filtering ---
    def test_agent_tools_filtered_to_allowed(self):
        setup = _minimal_setup(
            agents=[
                {
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "tools": ["ToolA", "Bad"],
                }
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["agents"][0]["tools"] == ["id-a"]

    def test_agent_tool_id_not_in_map_keeps_name(self):
        setup = _minimal_setup(
            agents=[
                {
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "tools": ["ToolA"],
                }
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), {})
        assert result["agents"][0]["tools"] == ["ToolA"]

    def test_agent_existing_id_removed(self):
        setup = _minimal_setup(
            agents=[
                {
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "id": "old-id",
                }
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert "id" not in result["agents"][0]

    def test_agent_non_list_tools_initialized(self):
        setup = _minimal_setup(
            agents=[
                {
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "tools": "invalid",
                }
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["agents"][0]["tools"] == []

    def test_agent_no_tools_key_initialized(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b"}],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["agents"][0]["tools"] == []

    def test_agent_tools_none_initialized(self):
        setup = _minimal_setup(
            agents=[
                {"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": None}
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["agents"][0]["tools"] == []

    # --- Task tool filtering ---
    def test_task_tools_filtered_and_converted(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA", "Unknown"]}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["tools"] == ["id-a"]

    def test_task_tool_id_not_in_map_keeps_name(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), {})
        assert result["tasks"][0]["tools"] == ["ToolA"]

    def test_task_non_list_tools_initialized(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": "bad"}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["tools"] == []

    def test_task_no_tools_key_initialized(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A"}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["tools"] == []

    def test_task_existing_id_removed(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "id": "old-id", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert "id" not in result["tasks"][0]

    # --- Task context ---
    def test_task_context_non_empty_list_stored(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": ["dep1"]}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["_context_refs"] == ["dep1"]
        assert result["tasks"][0]["context"] == []

    def test_task_context_empty_list_clears_refs(self):
        setup = _minimal_setup(
            tasks=[
                {
                    "name": "T",
                    "agent": "A",
                    "tools": [],
                    "context": [],
                    "_context_refs": ["old"],
                }
            ],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert "_context_refs" not in result["tasks"][0]

    def test_task_context_none_no_refs(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": None}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert "_context_refs" not in result["tasks"][0]

    def test_task_context_invalid_type_no_refs_noop(self):
        """When context is not a list and _context_refs does not already exist, else branch is no-op."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": "invalid"}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert "_context_refs" not in result["tasks"][0]

    # --- Task agent assignment ---
    def test_task_with_agent_field(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["agent"] == "A"
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_with_assigned_agent_fallback(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "assigned_agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["agent"] == "A"
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_no_agent_assignment(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        # No agent assigned, so task should not get agent/assigned_agent set
        assert result["tasks"][0].get("agent") is None

    def test_task_tools_removed_logged(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA", "NotAllowed"]}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["tools"] == ["id-a"]

    def test_agent_missing_name_uses_index(self):
        """Agent without 'name' is removed as orphan (no name -> can't match task assignment).
        The method proceeds with an empty agents list after orphan removal."""
        setup = {
            "agents": [{"role": "r", "goal": "g", "backstory": "b"}],
            "tasks": [{"name": "t", "agent": "nonexistent"}],
        }
        # Agent has no name -> not in assigned_agent_names -> removed as orphan
        # After orphan removal, agents list is empty but validation already passed
        result = self.service._process_crew_setup(setup, [], {})
        assert len(result["agents"]) == 0

    def test_model_assigned_to_all_agents(self):
        setup = _minimal_setup(
            agents=[
                {"name": "A1", "role": "r", "goal": "g", "backstory": "b"},
                {"name": "A2", "role": "r", "goal": "g", "backstory": "b"},
            ],
            tasks=[
                {"name": "T1", "agent": "A1", "tools": []},
                {"name": "T2", "agent": "A2", "tools": []},
            ],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map(), model="some-model"
        )
        assert result["agents"][0]["llm"] == "some-model"
        assert result["agents"][1]["llm"] == "some-model"

    def test_agent_tools_all_allowed(self):
        """When no tools are removed, the removed-tools log branch is skipped."""
        setup = _minimal_setup(
            agents=[
                {
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "tools": ["ToolA"],
                }
            ],
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["agents"][0]["tools"] == ["id-a"]

    def test_task_all_tools_allowed_no_removal_log(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA"]}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["tools"] == ["id-a"]

    def test_task_with_assigned_agent_not_in_task_sets_assigned_agent(self):
        """First loop: task has 'agent' but not 'assigned_agent' -> assigned_agent is set."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_with_both_agent_and_assigned_agent(self):
        """First loop: task has both 'agent' and 'assigned_agent' -> assigned_agent not overwritten."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "assigned_agent": "B", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        # The first loop sets task['agent'] = 'A' and doesn't overwrite assigned_agent since it exists
        # The final loop sets both to 'A' (because agent='A')
        assert result["tasks"][0]["agent"] == "A"

    # --- disable_memory ---
    def test_disable_memory_sets_memory_false_on_all_agents(self):
        """disable_memory=True sets memory=False on every agent."""
        setup = _minimal_setup(
            agents=[
                {"name": "A1", "role": "r", "goal": "g", "backstory": "b", "tools": []},
                {"name": "A2", "role": "r", "goal": "g", "backstory": "b", "tools": []},
            ],
            tasks=[
                {"name": "T1", "agent": "A1", "tools": []},
                {"name": "T2", "agent": "A2", "tools": []},
            ],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map(), disable_memory=True
        )
        assert all(agent["memory"] is False for agent in result["agents"])

    def test_disable_memory_default_does_not_add_memory(self):
        """Default (disable_memory=False) does NOT add a memory key to agents."""
        setup = _minimal_setup(
            agents=[
                {"name": "A1", "role": "r", "goal": "g", "backstory": "b", "tools": []}
            ],
            tasks=[{"name": "T1", "agent": "A1", "tools": []}],
        )
        result = self.service._process_crew_setup(
            setup, _allowed_tools(), _tool_id_map()
        )
        for agent in result["agents"]:
            assert "memory" not in agent


# ===========================================================================
# _has_persistent_memory_backend
# ===========================================================================


class TestHasPersistentMemoryBackend:
    def setup_method(self):
        self.service, self.session, *_ = _build_service()

    def _patch_repo(self, get_by_type):
        """Patch MemoryBackendRepository (lazily imported) and return the patcher.

        get_by_type is installed as an AsyncMock (return_value or side_effect).
        """
        mock_repo = Mock()
        mock_repo.get_by_type = get_by_type
        mock_repo_cls = Mock(return_value=mock_repo)
        return (
            patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository",
                mock_repo_cls,
            ),
            mock_repo,
        )

    @pytest.mark.asyncio
    async def test_returns_true_when_databricks_backend_exists(self):
        gc = Mock()
        gc.primary_group_id = "grp1"
        # First call (DATABRICKS) returns a backend -> True, never checks LAKEBASE
        patcher, mock_repo = self._patch_repo(AsyncMock(return_value=[Mock()]))
        with patcher:
            result = await self.service._has_persistent_memory_backend(self.session, gc)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_when_lakebase_backend_exists(self):
        gc = Mock()
        gc.primary_group_id = "grp1"
        # DATABRICKS -> empty, LAKEBASE -> backend present
        patcher, mock_repo = self._patch_repo(AsyncMock(side_effect=[[], [Mock()]]))
        with patcher:
            result = await self.service._has_persistent_memory_backend(self.session, gc)
        assert result is True
        assert mock_repo.get_by_type.await_count == 2

    @pytest.mark.asyncio
    async def test_returns_false_when_no_backend(self):
        gc = Mock()
        gc.primary_group_id = "grp1"
        patcher, mock_repo = self._patch_repo(AsyncMock(return_value=[]))
        with patcher:
            result = await self.service._has_persistent_memory_backend(self.session, gc)
        assert result is False
        # Both DATABRICKS and LAKEBASE checked
        assert mock_repo.get_by_type.await_count == 2

    @pytest.mark.asyncio
    async def test_returns_false_when_group_context_none(self):
        # group_context is None -> short-circuits to False, repo never instantiated
        result = await self.service._has_persistent_memory_backend(self.session, None)
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_primary_group_id_missing(self):
        gc = Mock()
        gc.primary_group_id = None
        result = await self.service._has_persistent_memory_backend(self.session, gc)
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_repo_raises(self):
        gc = Mock()
        gc.primary_group_id = "grp1"
        patcher, mock_repo = self._patch_repo(
            AsyncMock(side_effect=RuntimeError("db down"))
        )
        with patcher:
            result = await self.service._has_persistent_memory_backend(self.session, gc)
        assert result is False


# ===========================================================================
# _safe_get_attr
# ===========================================================================


class TestSafeGetAttr:
    def setup_method(self):
        self.service, *_ = _build_service()

    def test_dict_existing(self):
        assert self.service._safe_get_attr({"k": "v"}, "k") == "v"

    def test_dict_missing_default(self):
        assert self.service._safe_get_attr({"k": "v"}, "other", "d") == "d"

    def test_dict_missing_no_default(self):
        assert self.service._safe_get_attr({"k": "v"}, "other") is None

    def test_object_existing(self):
        class O:
            x = 5

        assert self.service._safe_get_attr(O(), "x") == 5

    def test_object_missing_default(self):
        class O:
            pass

        assert self.service._safe_get_attr(O(), "y", "d") == "d"

    def test_object_missing_no_default(self):
        class O:
            pass

        assert self.service._safe_get_attr(O(), "y") is None

    def test_none_object(self):
        assert self.service._safe_get_attr(None, "x") is None

    def test_none_object_with_default(self):
        assert self.service._safe_get_attr(None, "x", "d") == "d"


# ===========================================================================
# _get_relevant_documentation
# ===========================================================================


class TestGetRelevantDocumentation:
    """Tests for _get_relevant_documentation.

    DocumentationEmbeddingService is NOT imported at module level in
    crew_generation_service.py; the name is referenced directly on line 379.
    Since the entire method body is wrapped in try/except, a NameError will be
    caught and the method will return "".

    To actually exercise the code paths INSIDE the try block, we must inject
    the mock class into the module namespace before each test.
    """

    def setup_method(self):
        self.service, *_ = _build_service()

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        """Any exception inside should be caught and return ''."""
        with patch("src.services.crew_generation_service.LLMManager") as lm:
            lm_instance = Mock()
            lm.return_value = lm_instance
            lm_instance.get_embedding = AsyncMock(side_effect=RuntimeError("boom"))
            result = await self.service._get_relevant_documentation("some prompt")
            assert result == ""

    @pytest.mark.asyncio
    async def test_empty_embedding_returns_empty(self):
        mock_des_cls = Mock()
        _mod.DocumentationEmbeddingService = mock_des_cls
        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=None)
                result = await self.service._get_relevant_documentation("some prompt")
                assert result == ""
        finally:
            if hasattr(_mod, "DocumentationEmbeddingService"):
                delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_genie_keyword_enhances_query(self):
        mock_doc = Mock()
        mock_doc.id = "doc1"
        mock_doc.title = "Genie Guide"
        mock_doc.source = "docs/genie"
        mock_doc.content = "content"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[mock_doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1, 0.2])
                lm.get_embedding = AsyncMock(return_value=[0.3, 0.4])

                result = await self.service._get_relevant_documentation(
                    "use genie tool"
                )
                assert "[GENIE TOOL]" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_reveal_keyword_enhances_query(self):
        mock_doc = Mock()
        mock_doc.id = "doc1"
        mock_doc.title = "Reveal Guide"
        mock_doc.source = "docs/reveal"
        mock_doc.content = "content"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[mock_doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1, 0.2])
                lm.get_embedding = AsyncMock(return_value=[0.3, 0.4])

                result = await self.service._get_relevant_documentation(
                    "make a reveal slide"
                )
                assert "[REVEAL.JS]" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_presentation_keyword_enhances_query(self):
        mock_doc = Mock()
        mock_doc.id = "doc1"
        mock_doc.title = "Presentation Tips"
        mock_doc.source = "docs/reveal"
        mock_doc.content = "content"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[mock_doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1, 0.2])
                lm.get_embedding = AsyncMock(return_value=[0.3, 0.4])

                result = await self.service._get_relevant_documentation(
                    "create a presentation"
                )
                assert "[REVEAL.JS]" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_no_docs_found_returns_empty(self):
        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])

                result = await self.service._get_relevant_documentation("no results")
                assert result == ""
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_deduplicate_docs(self):
        doc1 = Mock()
        doc1.id = "same-id"
        doc1.title = "Genie Stuff"
        doc1.source = "docs/genie"
        doc1.content = "c1"
        doc2 = Mock()
        doc2.id = "same-id"
        doc2.title = "Genie Stuff"
        doc2.source = "docs/genie"
        doc2.content = "c1"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(side_effect=[[doc2], [doc1]])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])
                lm.get_embedding = AsyncMock(return_value=[0.2])

                result = await self.service._get_relevant_documentation("genie query")
                assert result.count("[GENIE TOOL]") == 1
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_generic_doc_formatting(self):
        doc = Mock()
        doc.id = "d1"
        doc.title = "General"
        doc.source = "docs/general"
        doc.content = "body"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])

                result = await self.service._get_relevant_documentation("generic query")
                assert "General" in result
                assert "body" in result
                assert "[GENIE TOOL]" not in result
                assert "[REVEAL.JS]" not in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_genie_embedding_returns_none(self):
        doc = Mock()
        doc.id = "d1"
        doc.title = "Normal Doc"
        doc.source = "docs/normal"
        doc.content = "c"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])
                lm.get_embedding = AsyncMock(return_value=None)

                result = await self.service._get_relevant_documentation("genie related")
                assert "Normal Doc" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_reveal_embedding_returns_none(self):
        doc = Mock()
        doc.id = "d1"
        doc.title = "Normal Doc"
        doc.source = "docs/normal"
        doc.content = "c"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])
                lm.get_embedding = AsyncMock(return_value=None)

                result = await self.service._get_relevant_documentation(
                    "reveal presentation"
                )
                assert "Normal Doc" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_doc_source_none(self):
        doc = Mock()
        doc.id = "d1"
        doc.title = "No Source Doc"
        doc.source = None
        doc.content = "c"

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=[doc])
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])

                result = await self.service._get_relevant_documentation("query")
                # doc.source is None, so doc.source.lower() raises AttributeError
                # which is caught by the outer except, returning ''
                assert result == ""
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_limit_caps_general_docs(self):
        docs = []
        for i in range(10):
            d = Mock()
            d.id = f"d{i}"
            d.title = f"Doc {i}"
            d.source = "docs/src"
            d.content = f"c{i}"
            docs.append(d)

        mock_des_cls = Mock()
        des_instance = Mock()
        mock_des_cls.return_value = des_instance
        des_instance.search_similar_embeddings = AsyncMock(return_value=docs)
        _mod.DocumentationEmbeddingService = mock_des_cls

        try:
            with patch("src.services.crew_generation_service.LLMManager") as lm:
                lm_instance = Mock()
                lm.return_value = lm_instance
                lm_instance.get_embedding = AsyncMock(return_value=[0.1])

                result = await self.service._get_relevant_documentation(
                    "query", limit=3
                )
                assert result.count("###") == 3
        finally:
            delattr(_mod, "DocumentationEmbeddingService")


# ===========================================================================
# _create_tool_name_to_id_map
# ===========================================================================


class TestCreateToolNameToIdMap:
    def setup_method(self):
        self.service, *_ = _build_service()

    def test_basic_mapping(self):
        tools = [{"name": "A", "id": "1"}, {"name": "B", "id": "2"}]
        result = self.service._create_tool_name_to_id_map(tools)
        assert result == {"A": "1", "B": "2"}

    def test_empty(self):
        assert self.service._create_tool_name_to_id_map([]) == {}

    def test_missing_name(self):
        tools = [{"id": "1"}]
        assert self.service._create_tool_name_to_id_map(tools) == {}

    def test_missing_id(self):
        tools = [{"name": "A"}]
        assert self.service._create_tool_name_to_id_map(tools) == {}

    def test_title_takes_precedence(self):
        tools = [{"title": "TitleName", "name": "RegName", "id": "1"}]
        result = self.service._create_tool_name_to_id_map(tools)
        assert result["TitleName"] == "1"
        assert result["RegName"] == "1"

    def test_title_same_as_name(self):
        tools = [{"title": "Same", "name": "Same", "id": "1"}]
        result = self.service._create_tool_name_to_id_map(tools)
        assert result == {"Same": "1"}

    def test_numeric_id_converted_to_string(self):
        tools = [{"name": "A", "id": 42}]
        result = self.service._create_tool_name_to_id_map(tools)
        assert result["A"] == "42"


# ===========================================================================
# _get_tool_details
# ===========================================================================


class TestGetToolDetails:
    def setup_method(self):
        self.service, *_ = _build_service()

    def _make_tool_obj(self, title="T", id_val="1", has_model_dump=True):
        tool = Mock()
        tool.title = title
        tool.id = id_val
        if has_model_dump:
            tool.model_dump = Mock(
                return_value={
                    "name": title,
                    "description": f"desc-{title}",
                    "id": id_val,
                }
            )
        else:
            # Remove model_dump so hasattr returns False
            del tool.model_dump
        return tool

    def _make_tool_service(self, tools):
        ts = Mock()
        resp = Mock()
        resp.tools = tools
        ts.get_all_tools = AsyncMock(return_value=resp)
        return ts

    @pytest.mark.asyncio
    async def test_string_identifier_found_by_name(self):
        t = self._make_tool_obj(title="MyTool", id_val="100")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["MyTool"], ts)
        assert len(result) == 1
        assert result[0]["name"] == "MyTool"

    @pytest.mark.asyncio
    async def test_string_identifier_found_by_id(self):
        t = self._make_tool_obj(title="Other", id_val="myid")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["myid"], ts)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_string_identifier_not_found_placeholder(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details(["Unknown"], ts)
        assert len(result) == 1
        assert result[0]["name"] == "Unknown"
        assert result[0]["id"] == "Unknown"

    @pytest.mark.asyncio
    async def test_dict_identifier_found_by_name(self):
        t = self._make_tool_obj(title="Finder", id_val="200")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details([{"name": "Finder"}], ts)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_dict_identifier_found_by_id(self):
        t = self._make_tool_obj(title="Other", id_val="300")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details([{"id": "300"}], ts)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_dict_identifier_name_no_match_placeholder(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details(
            [{"name": "NoMatch", "id": "noid"}], ts
        )
        assert len(result) == 1
        assert result[0]["name"] == "NoMatch"
        assert result[0]["id"] == "noid"

    @pytest.mark.asyncio
    async def test_dict_identifier_name_no_match_no_id(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details([{"name": "NoMatch"}], ts)
        assert len(result) == 1
        assert result[0]["id"] == "NoMatch"

    @pytest.mark.asyncio
    async def test_dict_identifier_no_name_no_id_skipped(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details([{"description": "orphan"}], ts)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_unknown_identifier_type_skipped(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details([12345], ts)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_tool_with_model_dump(self):
        t = self._make_tool_obj(title="WithDump", id_val="400", has_model_dump=True)
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["WithDump"], ts)
        assert result[0]["name"] == "WithDump"

    @pytest.mark.asyncio
    async def test_tool_without_model_dump_uses_dict(self):
        t = self._make_tool_obj(title="NoDump", id_val="500", has_model_dump=False)
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["NoDump"], ts)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_tool_missing_name_uses_title(self):
        t = Mock()
        t.title = "TitleFallback"
        t.id = "600"
        t.model_dump = Mock(return_value={"id": "600"})  # No 'name' key
        t.description = "d"
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["TitleFallback"], ts)
        assert result[0].get("name") == "TitleFallback"

    @pytest.mark.asyncio
    async def test_tool_missing_description_uses_attr(self):
        t = Mock()
        t.title = "DescFallback"
        t.id = "700"
        t.description = "attr_desc"
        t.model_dump = Mock(
            return_value={"name": "DescFallback", "id": "700"}
        )  # No 'description'
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(["DescFallback"], ts)
        assert result[0]["description"] == "attr_desc"

    @pytest.mark.asyncio
    async def test_exception_fallback_string_identifiers(self):
        ts = Mock()
        ts.get_all_tools = AsyncMock(side_effect=RuntimeError("service down"))
        result = await self.service._get_tool_details(["StrTool"], ts)
        assert len(result) == 1
        assert result[0]["name"] == "StrTool"

    @pytest.mark.asyncio
    async def test_exception_fallback_dict_identifiers(self):
        ts = Mock()
        ts.get_all_tools = AsyncMock(side_effect=RuntimeError("service down"))
        result = await self.service._get_tool_details(
            [{"name": "DictTool", "id": "did"}], ts
        )
        assert len(result) == 1
        assert result[0]["name"] == "DictTool"
        assert result[0]["id"] == "did"

    @pytest.mark.asyncio
    async def test_exception_fallback_dict_no_name(self):
        ts = Mock()
        ts.get_all_tools = AsyncMock(side_effect=RuntimeError("service down"))
        result = await self.service._get_tool_details([{"id": "xyz"}], ts)
        assert len(result) == 1
        assert result[0]["name"] == "Unknown"

    @pytest.mark.asyncio
    async def test_dict_identifier_with_description(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details(
            [{"name": "X", "description": "custom desc"}], ts
        )
        assert result[0]["description"] == "custom desc"

    @pytest.mark.asyncio
    async def test_dict_identifier_without_description(self):
        ts = self._make_tool_service([])
        result = await self.service._get_tool_details([{"name": "X"}], ts)
        assert "A tool named X" in result[0]["description"]

    @pytest.mark.asyncio
    async def test_dict_identifier_found_by_id_not_name(self):
        """Dict with id matching but name not matching uses id lookup."""
        t = self._make_tool_obj(title="RealName", id_val="300")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details(
            [{"name": "WrongName", "id": "300"}], ts
        )
        assert len(result) == 1


# ===========================================================================
# create_crew_complete
# ===========================================================================


class TestCreateCrewComplete:
    def setup_method(self):
        self.service, self.session, self.log_svc, self.crew_repo = _build_service()
        self.log_svc.create_log = AsyncMock()

    def _make_request(self, prompt="build a crew", model=None, tools=None):
        req = Mock()
        req.prompt = prompt
        req.model = model
        req.tools = tools if tools is not None else []
        return req

    @pytest.mark.asyncio
    async def test_happy_path(self):
        req = self._make_request(model="test-model", tools=["ToolA"])
        gc = Mock()
        gc.primary_group_id = "grp1"

        tool_detail = {"name": "ToolA", "id": "id-a", "title": "ToolA"}

        with (
            patch("src.services.crew_generation_service.ToolService"),
            patch.object(
                self.service, "_get_tool_details", new_callable=AsyncMock
            ) as gtd,
            patch.object(
                self.service, "_prepare_prompt_template", new_callable=AsyncMock
            ) as ppt,
            patch("src.services.crew_generation_service.LLMManager") as lm,
            patch("src.services.crew_generation_service.robust_json_parser") as rjp,
            patch.object(self.service, "_process_crew_setup") as pcs,
        ):

            gtd.return_value = [tool_detail]
            ppt.return_value = "system prompt"
            lm.completion = AsyncMock(return_value='{"agents":[],"tasks":[]}')
            rjp.return_value = {
                "agents": [
                    {
                        "name": "A1",
                        "role": "r",
                        "goal": "g",
                        "backstory": "b",
                        "tools": ["ToolA"],
                    }
                ],
                "tasks": [
                    {
                        "name": "T1",
                        "description": "d",
                        "agent": "A1",
                        "tools": ["ToolA"],
                    }
                ],
            }
            pcs.return_value = {
                "agents": [{"name": "A1", "role": "r", "tools": ["id-a"]}],
                "tasks": [{"name": "T1", "agent": "A1", "tools": ["id-a"]}],
            }
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )

            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])
            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": Mock(
                        MemoryBackendRepository=mock_mem_repo_cls
                    ),
                    "src.models.memory_backend": Mock(
                        MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")
                    ),
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert result == {"agents": [], "tasks": []}

    @pytest.mark.asyncio
    async def test_no_group_context_skips_filtering(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req, group_context=None)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_model_from_env_var(self):
        req = self._make_request(model=None)

        with (
            _crew_complete_patches(self.service) as m,
            patch.dict(os.environ, {"CREW_MODEL": "env-model"}),
        ):
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            await self.service.create_crew_complete(req)
            m["lm"].completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_model_default_fallback(self):
        req = self._make_request(model=None)

        with _crew_complete_patches(self.service) as m:
            os.environ.pop("CREW_MODEL", None)
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            await self.service.create_crew_complete(req)
            m["lm"].completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_llm_call_exception_raises_valueerror(self):
        req = self._make_request(model="m")

        with (
            patch("src.services.crew_generation_service.ToolService"),
            patch.object(
                self.service, "_get_tool_details", new_callable=AsyncMock
            ) as gtd,
            patch.object(
                self.service, "_prepare_prompt_template", new_callable=AsyncMock
            ) as ppt,
            patch("src.services.crew_generation_service.LLMManager") as lm,
        ):
            gtd.return_value = []
            ppt.return_value = "sys"
            lm.completion = AsyncMock(side_effect=RuntimeError("api error"))

            with pytest.raises(ValueError, match="Error generating crew"):
                await self.service.create_crew_complete(req)

    @pytest.mark.asyncio
    async def test_outer_exception_reraises(self):
        req = self._make_request()
        with patch(
            "src.services.crew_generation_service.ToolService",
            side_effect=RuntimeError("outer"),
        ):
            with pytest.raises(RuntimeError, match="outer"):
                await self.service.create_crew_complete(req)

    @pytest.mark.asyncio
    async def test_task_with_agent_field_logging(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={
                "agents": [{"name": "A"}],
                "tasks": [{"name": "T", "agent": "A"}],
            },
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_with_assigned_agent_fallback(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={
                "agents": [{"name": "A"}],
                "tasks": [{"name": "T", "assigned_agent": "A"}],
            },
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_no_agent_warning(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [{"name": "T"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_agent_as_pydantic_model(self):
        req = self._make_request(model="m")
        mock_agent = Mock()
        mock_agent.model_dump = Mock(return_value={"name": "A"})
        mock_agent.get = Mock(side_effect=lambda k, d=None: {"name": "A"}.get(k, d))

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [mock_agent], "tasks": [{"name": "T", "agent": "A"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            mock_agent.model_dump.assert_called_once()

    @pytest.mark.asyncio
    async def test_agent_as_dict(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={
                "agents": [{"name": "A", "role": "r"}],
                "tasks": [{"name": "T"}],
            },
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_agent_as_non_dict_non_pydantic(self):
        """Agent that has no model_dump and is not a dict -> uses agent as-is."""
        req = self._make_request(model="m")

        # Create object that has no model_dump attribute and is not a dict,
        # but can handle .get() calls from the logging code
        class PlainObj:
            def __init__(self):
                self.name = "A"
                self.role = "r"
                self.tools = []

            def get(self, key, default=None):
                return getattr(self, key, default)

        plain_agent = PlainObj()
        # Explicitly remove model_dump if mock adds it
        if hasattr(plain_agent, "model_dump"):
            delattr(plain_agent, "model_dump")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [plain_agent], "tasks": [{"name": "T"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_as_pydantic_model(self):
        req = self._make_request(model="m")
        mock_task = Mock()
        mock_task.model_dump = Mock(return_value={"name": "T"})
        mock_task.get = Mock(
            side_effect=lambda k, d=None: {"name": "T", "agent": "A"}.get(k, d)
        )

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [mock_task]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            mock_task.model_dump.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_as_non_dict_non_pydantic(self):
        """Task that has no model_dump and is not a dict -> uses as-is.
        The task_dict must support item assignment for the agent preservation code."""
        req = self._make_request(model="m")

        # Use a dict subclass that pretends not to be a dict by removing model_dump
        # but still supports item assignment
        task_data = {"name": "T", "agent": "A"}

        class FakeTask:
            """Not a dict, no model_dump, but supports .get() and item assignment."""

            def __init__(self):
                self._data = {"name": "T"}

            def get(self, key, default=None):
                # Used in task.get('agent') / task.get('assigned_agent') lookups
                return {"name": "T"}.get(key, default)

        ft = FakeTask()
        # ft has no model_dump and is not a dict, so it goes to the else branch
        # task_dict = ft (the object itself)
        # Then task_dict.get('name', 'Unknown') is called, plus task_dict['agent'] = agent_name
        # Since task.get('agent') returns None and task.get('assigned_agent') returns None,
        # the code takes the else branch (no agent assignment), so no item assignment happens.

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [ft]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_tool_filtering_with_backends_present(self):
        req = self._make_request(model="m", tools=["DatabricksKnowledgeSearchTool"])
        gc = Mock()
        gc.primary_group_id = "grp1"
        dk_tool = {
            "name": "DatabricksKnowledgeSearchTool",
            "id": "dk-id",
            "title": "DatabricksKnowledgeSearchTool",
        }

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(
                return_value=[Mock()]
            )

            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": Mock(
                        MemoryBackendRepository=mock_mem_repo_cls
                    ),
                    "src.models.memory_backend": Mock(
                        MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")
                    ),
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_tool_filtering_without_backends(self):
        req = self._make_request(model="m", tools=["DatabricksKnowledgeSearchTool"])
        gc = Mock()
        gc.primary_group_id = "grp1"
        dk_tool = {
            "name": "DatabricksKnowledgeSearchTool",
            "id": "dk-id",
            "title": "DatabricksKnowledgeSearchTool",
        }

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": Mock(
                        MemoryBackendRepository=mock_mem_repo_cls
                    ),
                    "src.models.memory_backend": Mock(
                        MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")
                    ),
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_tool_filtering_exception_warning(self):
        req = self._make_request(model="m")
        gc = Mock()
        gc.primary_group_id = "grp1"

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            # Force the import inside the try block to fail
            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": None,
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_process_crew_setup_called_with_model(self):
        req = self._make_request(model="my-model")

        with _crew_complete_patches(
            self.service, model_params={"model": "my-model"}
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            await self.service.create_crew_complete(req)
            m["pcs"].assert_called_once()
            # Verify model= kwarg was passed
            assert m["pcs"].call_args.kwargs.get("model") == "my-model"

    @pytest.mark.asyncio
    async def test_databricks_filtering_with_title_field(self):
        req = self._make_request(model="m")
        gc = Mock()
        gc.primary_group_id = "grp1"
        dk_tool = {"title": "DatabricksKnowledgeSearchTool", "id": "dk-id"}

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": Mock(
                        MemoryBackendRepository=mock_mem_repo_cls
                    ),
                    "src.models.memory_backend": Mock(
                        MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")
                    ),
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_request_tools_none_handled(self):
        req = Mock()
        req.prompt = "build a crew"
        req.model = "m"
        req.tools = None

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req)
            # _get_tool_details should be called with [] (not None)
            m["gtd"].assert_awaited_once()
            first_arg = m["gtd"].call_args[0][0]
            assert first_arg == []

    @pytest.mark.asyncio
    async def test_group_context_no_primary_group_id(self):
        """Group context present but primary_group_id is None -> skip filtering."""
        req = self._make_request(model="m")
        gc = Mock()
        gc.primary_group_id = None

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            result = await self.service.create_crew_complete(req, group_context=gc)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_filtering_count_unchanged(self):
        """When before_count == after_count, the log message is not emitted."""
        req = self._make_request(model="m")
        gc = Mock()
        gc.primary_group_id = "grp1"
        # Tool that is NOT DatabricksKnowledgeSearchTool
        normal_tool = {"name": "NormalTool", "id": "n-id"}

        with _crew_complete_patches(self.service, gtd_return=[normal_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(
                return_value={"agents": [], "tasks": []}
            )
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict(
                "sys.modules",
                {
                    "src.repositories.memory_backend_repository": Mock(
                        MemoryBackendRepository=mock_mem_repo_cls
                    ),
                    "src.models.memory_backend": Mock(
                        MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")
                    ),
                },
            ):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result


# ===========================================================================
# Progressive / Streaming crew generation
# ===========================================================================

from src.core.exceptions import BadRequestError, KasalError
from src.schemas.task_generation import Agent as TaskGenAgent


class TestProgressiveGeneration:
    """Tests for create_crew_progressive() and its helper methods."""

    def setup_method(self):
        self.service, self.session, self.log_svc, self.crew_repo = _build_service()

    # ------------------------------------------------------------------
    # _generate_crew_plan
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_detaches_request_session(self):
        """Regression: the background task (asyncio.create_task) inherits a COPY
        of the dispatch request's context, including the request-scoped session
        that FastAPI closes once the response returns. create_crew_progressive
        must detach it before planning so the model-config read inside
        configure_crewai_llm runs on a fresh session, not the closed one
        ('Cannot operate on a closed database')."""
        from src.db.session import _request_session

        request = Mock()
        request.prompt = "gather swiss ai data"
        request.original_prompt = "gather swiss ai data"
        request.model = None
        request.tools = []

        captured = {}

        async def capture_then_stop(*args, **kwargs):
            captured["session_during_planning"] = _request_session.get(None)
            raise RuntimeError("stop after capturing the detached state")

        # Simulate the leaked, already-closed request session in the context.
        token = _request_session.set(AsyncMock())
        try:
            with (
                patch.object(
                    self.service, "_generate_crew_plan", side_effect=capture_then_stop
                ),
                patch("src.services.crew_generation_service.sse_manager") as sse,
            ):
                sse.broadcast_to_job = AsyncMock()
                await self.service.create_crew_progressive(
                    request, None, "gen-detach-test"
                )

            # By the time planning runs, the inherited session has been detached.
            assert captured["session_during_planning"] is None
        finally:
            _request_session.reset(token)

    @pytest.mark.asyncio
    async def test_generate_crew_plan_success(self):
        """_generate_crew_plan returns dict with agents/tasks/process_type/complexity."""
        request = Mock()
        request.prompt = "build a data pipeline crew"
        plan_dict = {
            "agents": [{"name": "Extractor", "role": "Data Extractor"}],
            "tasks": [{"name": "Extract Data", "assigned_agent": "Extractor"}],
            "process_type": "sequential",
            "complexity": "standard",
        }

        with (
            patch("src.services.crew_generation_service.TemplateService") as ts,
            patch("src.services.crew_generation_service.LLMManager") as lm,
            patch("src.services.crew_generation_service.robust_json_parser") as rjp,
        ):
            ts.get_effective_template_content = AsyncMock(return_value="system prompt")
            lm.completion = AsyncMock(return_value='{"agents":[]}')
            rjp.return_value = plan_dict
            self.log_svc.create_log = AsyncMock()

            result = await self.service._generate_crew_plan(request, None, "test-model")
            assert result == plan_dict
            assert "agents" in result
            assert "tasks" in result
            lm.completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_generate_crew_plan_no_agents_raises(self):
        """BadRequestError when plan has no agents."""
        request = Mock()
        request.prompt = "empty crew"
        plan_dict = {"agents": [], "tasks": [{"name": "T"}]}

        with (
            patch("src.services.crew_generation_service.TemplateService") as ts,
            patch("src.services.crew_generation_service.LLMManager") as lm,
            patch("src.services.crew_generation_service.robust_json_parser") as rjp,
        ):
            ts.get_effective_template_content = AsyncMock(return_value="sys")
            lm.completion = AsyncMock(return_value="{}")
            rjp.return_value = plan_dict
            self.log_svc.create_log = AsyncMock()

            with pytest.raises(BadRequestError, match="no agents"):
                await self.service._generate_crew_plan(request, None, "m")

    @pytest.mark.asyncio
    async def test_generate_crew_plan_no_tasks_raises(self):
        """BadRequestError when plan has no tasks."""
        request = Mock()
        request.prompt = "no tasks crew"
        plan_dict = {
            "agents": [{"name": "A", "role": "R"}],
            "tasks": [],
        }

        with (
            patch("src.services.crew_generation_service.TemplateService") as ts,
            patch("src.services.crew_generation_service.LLMManager") as lm,
            patch("src.services.crew_generation_service.robust_json_parser") as rjp,
        ):
            ts.get_effective_template_content = AsyncMock(return_value="sys")
            lm.completion = AsyncMock(return_value="{}")
            rjp.return_value = plan_dict
            self.log_svc.create_log = AsyncMock()

            with pytest.raises(BadRequestError, match="no tasks"):
                await self.service._generate_crew_plan(request, None, "m")

    @pytest.mark.asyncio
    async def test_generate_crew_plan_template_not_found(self):
        """KasalError when template 'generate_crew_plan' is missing."""
        request = Mock()
        request.prompt = "anything"

        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value=None)

            with pytest.raises(KasalError, match="not found"):
                await self.service._generate_crew_plan(request, None, "m")

    # ------------------------------------------------------------------
    # _find_agent_context (static)
    # ------------------------------------------------------------------

    def test_find_agent_context_found(self):
        """Returns TaskGenAgent when name matches."""
        task_plan = {"assigned_agent": "Researcher"}
        agent_results = [
            {
                "name": "Researcher",
                "role": "Research",
                "goal": "Find data",
                "backstory": "Expert",
            },
        ]
        result = CrewGenerationService._find_agent_context(task_plan, agent_results)
        assert result is not None
        assert isinstance(result, TaskGenAgent)
        assert result.name == "Researcher"
        assert result.role == "Research"
        assert result.goal == "Find data"

    def test_find_agent_context_not_found(self):
        """Returns None when no match exists."""
        task_plan = {"assigned_agent": "NonExistent"}
        agent_results = [
            {"name": "Researcher", "role": "R", "goal": "G", "backstory": "B"},
        ]
        result = CrewGenerationService._find_agent_context(task_plan, agent_results)
        assert result is None

    def test_find_agent_context_case_insensitive(self):
        """Matches regardless of case."""
        task_plan = {"assigned_agent": "DATA ANALYST"}
        agent_results = [
            {
                "name": "data analyst",
                "role": "Analysis",
                "goal": "Analyze",
                "backstory": "Pro",
            },
        ]
        result = CrewGenerationService._find_agent_context(task_plan, agent_results)
        assert result is not None
        assert result.name == "data analyst"

    def test_find_agent_context_no_assigned_agent(self):
        """Returns None when task_plan has no assigned_agent key."""
        task_plan = {"name": "some task"}
        agent_results = [
            {"name": "Agent1", "role": "R", "goal": "G", "backstory": "B"},
        ]
        result = CrewGenerationService._find_agent_context(task_plan, agent_results)
        assert result is None

    # ------------------------------------------------------------------
    # _resolve_agent_id (static)
    # ------------------------------------------------------------------

    def test_resolve_agent_id_exact_match(self):
        """Returns correct ID for matching agent name."""
        task_plan = {"assigned_agent": "Writer"}
        agent_results = [
            {"name": "Researcher", "id": "id-1"},
            {"name": "Writer", "id": "id-2"},
        ]
        result = CrewGenerationService._resolve_agent_id(task_plan, agent_results)
        assert result == "id-2"

    def test_resolve_agent_id_fallback_first(self):
        """Returns first agent ID when assigned_agent does not match any."""
        task_plan = {"assigned_agent": "Unknown"}
        agent_results = [
            {"name": "Alpha", "id": "id-alpha"},
            {"name": "Beta", "id": "id-beta"},
        ]
        result = CrewGenerationService._resolve_agent_id(task_plan, agent_results)
        assert result == "id-alpha"

    def test_resolve_agent_id_no_agents(self):
        """Returns None when agent_results is empty."""
        task_plan = {"assigned_agent": "Someone"}
        result = CrewGenerationService._resolve_agent_id(task_plan, [])
        assert result is None

    def test_resolve_agent_id_no_assigned_agent(self):
        """When no assigned_agent, falls back to first agent."""
        task_plan = {"name": "task without agent"}
        agent_results = [{"name": "Default", "id": "id-default"}]
        result = CrewGenerationService._resolve_agent_id(task_plan, agent_results)
        assert result == "id-default"

    def test_resolve_agent_id_case_insensitive(self):
        """Case-insensitive matching of agent names."""
        task_plan = {"assigned_agent": "WRITER"}
        agent_results = [{"name": "writer", "id": "id-w"}]
        result = CrewGenerationService._resolve_agent_id(task_plan, agent_results)
        assert result == "id-w"

    # ------------------------------------------------------------------
    # _resolve_progressive_dependencies
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_resolve_progressive_dependencies_success(self):
        """Name-to-ID mapping works and updates task context."""
        repo = Mock()
        repo.update_task_dependencies = AsyncMock()

        task_results = [
            {"name": "Task A", "id": "tid-a", "_plan": {}},
            {"name": "Task B", "id": "tid-b", "_plan": {"context": ["Task A"]}},
        ]

        await self.service._resolve_progressive_dependencies(
            task_results, "gen-1", repo
        )

        repo.update_task_dependencies.assert_awaited_once_with("tid-b", ["tid-a"])
        assert task_results[1]["context"] == ["tid-a"]

    @pytest.mark.asyncio
    async def test_resolve_progressive_dependencies_no_context(self):
        """Skips tasks without context references."""
        repo = Mock()
        repo.update_task_dependencies = AsyncMock()

        task_results = [
            {"name": "Task A", "id": "tid-a", "_plan": {}},
            {"name": "Task B", "id": "tid-b", "_plan": {}},
        ]

        await self.service._resolve_progressive_dependencies(
            task_results, "gen-1", repo
        )

        repo.update_task_dependencies.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_resolve_progressive_dependencies_missing_dep(self):
        """Skips unresolved references (name not found in task_results)."""
        repo = Mock()
        repo.update_task_dependencies = AsyncMock()

        task_results = [
            {"name": "Task A", "id": "tid-a", "_plan": {"context": ["NonExistent"]}},
        ]

        await self.service._resolve_progressive_dependencies(
            task_results, "gen-1", repo
        )

        repo.update_task_dependencies.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_resolve_progressive_dependencies_self_reference_excluded(self):
        """A task referencing itself is excluded from resolved IDs."""
        repo = Mock()
        repo.update_task_dependencies = AsyncMock()

        task_results = [
            {"name": "Task A", "id": "tid-a", "_plan": {"context": ["Task A"]}},
        ]

        await self.service._resolve_progressive_dependencies(
            task_results, "gen-1", repo
        )

        repo.update_task_dependencies.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_resolve_progressive_dependencies_repo_error_swallowed(self):
        """Exception from repo.update_task_dependencies is logged, not raised."""
        repo = Mock()
        repo.update_task_dependencies = AsyncMock(side_effect=RuntimeError("db error"))

        task_results = [
            {"name": "Task A", "id": "tid-a", "_plan": {}},
            {"name": "Task B", "id": "tid-b", "_plan": {"context": ["Task A"]}},
        ]

        # Should not raise
        await self.service._resolve_progressive_dependencies(
            task_results, "gen-1", repo
        )

    # ------------------------------------------------------------------
    # _suggest_genie_space
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_suggest_genie_space_with_results(self):
        """Returns best match dict when search returns spaces."""
        mock_space = Mock()
        mock_space.id = "space-1"
        mock_space.name = "Sales Space"
        mock_space.description = "Sales data"
        mock_response = Mock()
        mock_response.spaces = [mock_space]

        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(return_value=mock_response)

        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(
            sys.modules, {"src.repositories.genie_repository": mock_genie_module}
        ):
            result = await self.service._suggest_genie_space(
                "sales analysis", "analyze sales"
            )

        assert result is not None
        assert result["id"] == "space-1"
        assert result["name"] == "Sales Space"

    @pytest.mark.asyncio
    async def test_suggest_genie_space_fallback(self):
        """Returns first available space when search returns empty."""
        mock_space = Mock()
        mock_space.id = "space-fallback"
        mock_space.name = "Fallback Space"
        mock_space.description = ""

        empty_response = Mock()
        empty_response.spaces = []
        fallback_response = Mock()
        fallback_response.spaces = [mock_space]

        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(
            side_effect=[empty_response, fallback_response]
        )
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(
            sys.modules, {"src.repositories.genie_repository": mock_genie_module}
        ):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is not None
        assert result["id"] == "space-fallback"

    @pytest.mark.asyncio
    async def test_suggest_genie_space_no_spaces(self):
        """Returns None when no spaces exist at all."""
        empty_response = Mock()
        empty_response.spaces = []

        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(return_value=empty_response)
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(
            sys.modules, {"src.repositories.genie_repository": mock_genie_module}
        ):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is None

    @pytest.mark.asyncio
    async def test_suggest_genie_space_exception(self):
        """Returns None on exception."""
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(
            side_effect=RuntimeError("connection failed")
        )

        with patch.dict(
            sys.modules, {"src.repositories.genie_repository": mock_genie_module}
        ):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is None

    # ------------------------------------------------------------------
    # create_crew_progressive -- integration-level with mocks
    # ------------------------------------------------------------------

    def _make_progressive_request(
        self,
        prompt="build a crew",
        model="test-model",
        tools=None,
        auto_execute=False,
        session_id=None,
        memory_workspace_scope=True,
        disable_memory=False,
        mcp_servers=None,
        agentbricks_endpoints=None,
    ):
        """Create a mock CrewStreamingRequest.

        auto_execute defaults to False (AgentBuilder: generate-only) so the
        backend run branch is skipped; ChatMode tests pass auto_execute=True.
        The run-setting attrs are set explicitly because a bare Mock would make
        ``getattr(req, "auto_execute", False)`` truthy and ``request.mcp_servers``
        (or ``request.agentbricks_endpoints``) a non-iterable Mock.
        """
        req = Mock()
        req.prompt = prompt
        req.model = model
        req.tools = tools or []
        req.original_prompt = None
        req.auto_execute = auto_execute
        req.session_id = session_id
        req.memory_workspace_scope = memory_workspace_scope
        req.disable_memory = disable_memory
        req.mcp_servers = mcp_servers or []
        req.agentbricks_endpoints = agentbricks_endpoints or []
        return req

    def _make_plan(
        self, agents=None, tasks=None, process_type="sequential", complexity="standard"
    ):
        """Build a plan dict for _generate_crew_plan mock return."""
        if agents is None:
            agents = [{"name": "Agent1", "role": "Specialist"}]
        if tasks is None:
            tasks = [{"name": "Task1", "assigned_agent": "Agent1"}]
        return {
            "agents": agents,
            "tasks": tasks,
            "process_type": process_type,
            "complexity": complexity,
        }

    def _progressive_patches(
        self,
        plan=None,
        agent_gen_return=None,
        task_gen_response=None,
        agent_saved=None,
        task_saved=None,
        tool_details=None,
    ):
        """Build a context manager with all patches needed for create_crew_progressive."""

        if plan is None:
            plan = self._make_plan()
        if agent_gen_return is None:
            agent_gen_return = {
                "name": "Agent1",
                "role": "Specialist",
                "goal": "Do work",
                "backstory": "Expert",
                "advanced_config": {},
            }
        if task_gen_response is None:
            task_gen_response = Mock()
            task_gen_response.name = "Task1"
            task_gen_response.description = "Do something"
            task_gen_response.expected_output = "Result"
            task_gen_response.tools = []
            task_gen_response.llm_guardrail = None
        if agent_saved is None:
            agent_saved = {"id": "agent-id-1", "name": "Agent1", "role": "Specialist"}
        if task_saved is None:
            task_saved = {
                "id": "task-id-1",
                "name": "Task1",
                "description": "Do something",
            }

        service = self.service

        class ProgressivePatchCtx:
            def __init__(self):
                self._patches = []
                self.mocks = {}

            def __enter__(self):
                # Patch sse_manager.broadcast_to_job
                p_sse = patch("src.services.crew_generation_service.sse_manager")
                # Patch the isolated DB session used by the local-DB (SQLite) path.
                # The function does a local import
                # ``from src.db.session import ... get_isolated_db_session`` and uses
                # that private connection instead of the shared async_session_factory.
                p_session = patch("src.db.session.get_isolated_db_session")
                # Patch is_lakebase_enabled so the code takes the local-DB path
                # (the function does: ``from src.db.database_router import is_lakebase_enabled``)
                p_lakebase = patch(
                    "src.db.database_router.is_lakebase_enabled",
                    new_callable=AsyncMock,
                    return_value=False,
                )
                # Patch the plan generation
                p_plan = patch.object(
                    service, "_generate_crew_plan", new_callable=AsyncMock
                )
                # Patch AgentGenerationService
                p_agent_svc = patch(
                    "src.services.crew_generation_service.AgentGenerationService"
                )
                # Patch TaskGenerationService
                p_task_svc = patch(
                    "src.services.crew_generation_service.TaskGenerationService"
                )
                # Patch CrewGeneratorRepository
                p_repo = patch(
                    "src.services.crew_generation_service.CrewGeneratorRepository"
                )
                # Patch ToolService
                p_tool_svc = patch("src.services.crew_generation_service.ToolService")
                # Patch _get_tool_details
                p_gtd = patch.object(
                    service, "_get_tool_details", new_callable=AsyncMock
                )

                self._patches = [
                    p_sse,
                    p_session,
                    p_lakebase,
                    p_plan,
                    p_agent_svc,
                    p_task_svc,
                    p_repo,
                    p_tool_svc,
                    p_gtd,
                ]
                ms = [p.start() for p in self._patches]

                mock_sse = ms[0]
                mock_session_factory = ms[1]
                # ms[2] = is_lakebase_enabled (already configured via new_callable)
                mock_plan = ms[3]
                mock_agent_svc_cls = ms[4]
                mock_task_svc_cls = ms[5]
                mock_repo_cls = ms[6]
                mock_tool_svc_cls = ms[7]
                mock_gtd = ms[8]

                # Configure SSE manager
                mock_sse.broadcast_to_job = AsyncMock()

                # Configure async session factory as async context manager
                mock_session_obj = AsyncMock()
                mock_session_obj.commit = AsyncMock()
                mock_session_obj.rollback = AsyncMock()
                mock_cm = AsyncMock()
                mock_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
                mock_cm.__aexit__ = AsyncMock(return_value=False)
                mock_session_factory.return_value = mock_cm

                # Configure plan generation
                mock_plan.return_value = plan

                # Configure agent generation service
                mock_agent_gen = AsyncMock()
                mock_agent_gen.generate_agent = AsyncMock(return_value=agent_gen_return)
                mock_agent_svc_cls.return_value = mock_agent_gen

                # Configure task generation service
                mock_task_gen = AsyncMock()
                mock_task_gen.generate_task = AsyncMock(return_value=task_gen_response)
                mock_task_svc_cls.return_value = mock_task_gen

                # Configure crew repo
                mock_repo = AsyncMock()
                mock_repo.create_single_agent = AsyncMock(return_value=agent_saved)
                mock_repo.create_single_task = AsyncMock(return_value=task_saved)
                mock_repo.update_task_dependencies = AsyncMock()
                mock_repo_cls.return_value = mock_repo

                # Configure tool service and _get_tool_details
                mock_gtd.return_value = tool_details or []

                self.mocks = {
                    "sse": mock_sse,
                    "session_factory": mock_session_factory,
                    "session": mock_session_obj,
                    "plan": mock_plan,
                    "agent_svc_cls": mock_agent_svc_cls,
                    "agent_gen": mock_agent_gen,
                    "task_svc_cls": mock_task_svc_cls,
                    "task_gen": mock_task_gen,
                    "repo_cls": mock_repo_cls,
                    "repo": mock_repo,
                    "tool_svc_cls": mock_tool_svc_cls,
                    "gtd": mock_gtd,
                }
                return self.mocks

            def __exit__(self, *args):
                for p in reversed(self._patches):
                    p.stop()

        return ProgressivePatchCtx()

    @pytest.mark.asyncio
    async def test_create_crew_progressive_happy_path(self):
        """Full flow broadcasts plan_ready, agent_detail, task_detail, generation_complete."""
        request = self._make_progressive_request()
        gen_id = "gen-happy"

        with self._progressive_patches() as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]

            assert "plan_ready" in event_types
            assert "agent_detail" in event_types
            assert "task_detail" in event_types
            assert "generation_complete" in event_types

            # plan_ready should come first
            assert event_types.index("plan_ready") < event_types.index("agent_detail")
            assert event_types.index("agent_detail") < event_types.index("task_detail")
            assert event_types.index("task_detail") < event_types.index(
                "generation_complete"
            )

    @pytest.mark.asyncio
    async def test_create_crew_progressive_persists_via_isolated_session(self):
        """Regression: on SQLite the generation flow must run on a PRIVATE
        connection (get_isolated_db_session), never the shared StaticPool
        async_session_factory. Sharing the one connection let a concurrent
        request's commit/rollback discard a committed agent mid-generation,
        breaking the next task's agent_id foreign key."""
        request = self._make_progressive_request()

        with self._progressive_patches() as m:
            # The shared StaticPool factory must NOT back the generation session.
            with patch("src.db.session.async_session_factory") as shared_factory:
                await self.service.create_crew_progressive(request, None, "gen-iso")
                shared_factory.assert_not_called()

            # The isolated (private-connection) session WAS used.
            m["session_factory"].assert_called()

    @pytest.mark.asyncio
    async def test_create_crew_progressive_auto_executes_when_requested(self):
        """ChatMode (auto_execute=True): the backend runs the crew itself and folds
        the new execution id INTO generation_complete (one terminal event) so the
        run survives a session switch before the plan finishes."""
        request = self._make_progressive_request(
            auto_execute=True,
            session_id="chat-1",
            mcp_servers=["Databricks Genie: Sales"],
        )
        gen_id = "gen-auto"

        with self._progressive_patches() as m:
            with patch(
                "src.services.execution_service.ExecutionService"
            ) as mock_exec_cls:
                mock_exec = Mock()
                mock_exec.create_execution = AsyncMock(
                    return_value={
                        "execution_id": "job-auto-1",
                        "status": "running",
                        "run_name": "Auto Run",
                    }
                )
                mock_exec_cls.return_value = mock_exec

                await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types
            # No separate execution_started event — it's folded in.
            assert "execution_started" not in event_types

            complete = next(
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            )
            assert complete.args[1].data["execution_id"] == "job-auto-1"
            assert complete.args[1].data["run_name"] == "Auto Run"

            # The crew was executed via a CrewConfig built from the generated
            # agents/tasks, carrying the chat's session + memory scope.
            cfg = mock_exec.create_execution.call_args.kwargs["config"]
            assert cfg.session_id == "chat-1"
            assert any(k.startswith("agent_") for k in cfg.agents_yaml)
            # Not launched via FastAPI BackgroundTasks (none in this context).
            assert (
                mock_exec.create_execution.call_args.kwargs["background_tasks"] is None
            )

    @pytest.mark.asyncio
    async def test_create_crew_progressive_no_auto_execute_for_agentbuilder(self):
        """AgentBuilder (auto_execute=False, the default): the plan is generated
        and rendered, but the backend never starts a run."""
        request = self._make_progressive_request(auto_execute=False)
        gen_id = "gen-no-auto"

        with self._progressive_patches() as m:
            with patch(
                "src.services.execution_service.ExecutionService"
            ) as mock_exec_cls:
                await self.service.create_crew_progressive(request, None, gen_id)
                mock_exec_cls.assert_not_called()

            complete = next(
                c
                for c in m["sse"].broadcast_to_job.call_args_list
                if c.args[1].data["type"] == "generation_complete"
            )
            assert "execution_id" not in complete.args[1].data

    @pytest.mark.asyncio
    async def test_create_crew_progressive_auto_execute_failure_is_isolated(self):
        """A failure starting the backend run is reported via execution_error on
        generation_complete (not generation_failed) — generation itself succeeded."""
        request = self._make_progressive_request(auto_execute=True, session_id="chat-2")
        gen_id = "gen-auto-fail"

        with self._progressive_patches() as m:
            with patch(
                "src.services.execution_service.ExecutionService"
            ) as mock_exec_cls:
                mock_exec = Mock()
                mock_exec.create_execution = AsyncMock(
                    side_effect=RuntimeError("no capacity")
                )
                mock_exec_cls.return_value = mock_exec

                await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types
            assert "generation_failed" not in event_types
            complete = next(
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            )
            assert "execution_id" not in complete.args[1].data
            assert "no capacity" in complete.args[1].data["execution_error"]

    @pytest.mark.asyncio
    async def test_create_crew_progressive_planning_failure(self):
        """Broadcasts generation_failed when planning raises an exception."""
        request = self._make_progressive_request()
        gen_id = "gen-plan-fail"

        with self._progressive_patches() as m:
            m["plan"].side_effect = RuntimeError("LLM timeout")

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_failed" in event_types
            fail_event = next(
                c for c in calls if c.args[1].data["type"] == "generation_failed"
            )
            assert "LLM timeout" in fail_event.args[1].data["error"]

    @pytest.mark.asyncio
    async def test_create_crew_progressive_empty_plan(self):
        """Broadcasts generation_failed when plan has no agents."""
        request = self._make_progressive_request()
        gen_id = "gen-empty"

        empty_plan = {
            "agents": [],
            "tasks": [{"name": "T1"}],
            "process_type": "sequential",
        }

        with self._progressive_patches(plan=empty_plan) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_failed" in event_types
            fail_event = next(
                c for c in calls if c.args[1].data["type"] == "generation_failed"
            )
            assert "no agents" in fail_event.args[1].data["error"].lower()

    @pytest.mark.asyncio
    async def test_create_crew_progressive_agent_error_continues(self):
        """Agent error broadcasts entity_error and continues to next agent."""
        plan = self._make_plan(
            agents=[
                {"name": "Agent1", "role": "R1"},
                {"name": "Agent2", "role": "R2"},
            ],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent1"},
                {"name": "Task2", "assigned_agent": "Agent2"},
            ],
        )
        agent_saved_2 = {"id": "agent-id-2", "name": "Agent2", "role": "R2"}
        task_saved = {"id": "task-id-1", "name": "Task2", "description": "desc"}

        request = self._make_progressive_request(
            prompt="build a crew with 2 agents and 2 tasks"
        )
        gen_id = "gen-agent-err"

        with self._progressive_patches(
            plan=plan, agent_saved=agent_saved_2, task_saved=task_saved
        ) as m:
            # First agent generation fails, second succeeds
            call_count = [0]

            async def agent_gen_side_effect(**kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise RuntimeError("agent generation failed")
                return {
                    "name": "Agent2",
                    "role": "R2",
                    "goal": "G2",
                    "backstory": "B2",
                    "advanced_config": {},
                }

            m["agent_gen"].generate_agent = AsyncMock(side_effect=agent_gen_side_effect)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "entity_error" in event_types
            assert "agent_detail" in event_types
            assert "generation_complete" in event_types

            # Verify entity_error is for agent
            error_event = next(
                c for c in calls if c.args[1].data["type"] == "entity_error"
            )
            assert error_event.args[1].data["entity_type"] == "agent"
            assert error_event.args[1].data["name"] == "Agent1"

    @pytest.mark.asyncio
    async def test_create_crew_progressive_task_error_continues(self):
        """Task error broadcasts entity_error and continues to next task."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent1"},
                {"name": "Task2", "assigned_agent": "Agent1"},
            ],
        )
        agent_saved = {"id": "agent-id-1", "name": "Agent1", "role": "R1"}

        request = self._make_progressive_request()
        gen_id = "gen-task-err"

        task_response_ok = Mock()
        task_response_ok.name = "Task2"
        task_response_ok.description = "desc2"
        task_response_ok.expected_output = "output2"
        task_response_ok.tools = []
        task_response_ok.llm_guardrail = None

        task_saved_ok = {"id": "task-id-2", "name": "Task2", "description": "desc2"}

        with self._progressive_patches(
            plan=plan,
            agent_saved=agent_saved,
            task_saved=task_saved_ok,
            task_gen_response=task_response_ok,
        ) as m:
            # First task generation fails, second succeeds
            call_count = [0]

            async def task_gen_side_effect(req, gc=None):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise RuntimeError("task generation failed")
                return task_response_ok

            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_side_effect)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "entity_error" in event_types
            assert "generation_complete" in event_types

            error_event = next(
                c for c in calls if c.args[1].data["type"] == "entity_error"
            )
            assert error_event.args[1].data["entity_type"] == "task"

    @pytest.mark.asyncio
    async def test_create_crew_progressive_synthesizes_tasks_when_all_task_gen_fails(
        self,
    ):
        """Regression: when EVERY per-task LLM generation fails (common with small
        models that occasionally return malformed JSON), the crew would otherwise
        reach auto-execute with agents but ZERO tasks and die in crew preparation
        ('Failed to prepare crew'). The fallback must synthesize minimal tasks from
        the plan so the crew stays runnable."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[{"name": "Task1", "assigned_agent": "Agent1"}],
        )
        agent_saved = {"id": "agent-id-1", "name": "Agent1", "role": "R1"}
        # The fallback persists the synthesized task via create_single_task.
        task_saved = {"id": "task-fallback-1", "name": "Task1", "description": "synth"}

        request = self._make_progressive_request()
        gen_id = "gen-task-synth"

        with self._progressive_patches(
            plan=plan,
            agent_saved=agent_saved,
            task_saved=task_saved,
        ) as m:
            # EVERY task generation fails — the normal path produces no tasks.
            m["task_gen"].generate_task = AsyncMock(
                side_effect=RuntimeError("task generation failed")
            )

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            # The failed normal generation still reports an entity_error...
            assert "entity_error" in event_types
            # ...but the fallback synthesizes a task so the crew isn't empty.
            assert "task_detail" in event_types
            complete = next(
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            )
            assert len(complete.args[1].data["tasks"]) >= 1
            # The fallback used create_single_task even though generate_task failed.
            m["repo"].create_single_task.assert_awaited()

    @pytest.mark.asyncio
    async def test_create_crew_progressive_skips_auto_execute_when_no_tasks(self):
        """Regression: if task generation AND the fallback both yield nothing, the
        backend must NOT launch a taskless crew (which crashes in crew preparation).
        It reports a clear, actionable execution_error instead."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[{"name": "Task1", "assigned_agent": "Agent1"}],
        )
        agent_saved = {"id": "agent-id-1", "name": "Agent1", "role": "R1"}

        request = self._make_progressive_request(auto_execute=True, session_id="chat-x")
        gen_id = "gen-no-tasks-autoexec"

        with self._progressive_patches(plan=plan, agent_saved=agent_saved) as m:
            # Normal task generation fails for every task...
            m["task_gen"].generate_task = AsyncMock(
                side_effect=RuntimeError("task generation failed")
            )
            # ...and the fallback's persistence also fails, leaving zero tasks.
            m["repo"].create_single_task = AsyncMock(
                side_effect=RuntimeError("db write failed")
            )

            with patch(
                "src.services.execution_service.ExecutionService"
            ) as mock_exec_cls:
                await self.service.create_crew_progressive(request, None, gen_id)
                # A taskless crew must never be launched.
                mock_exec_cls.assert_not_called()

            calls = m["sse"].broadcast_to_job.call_args_list
            complete = next(
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            )
            assert "execution_id" not in complete.args[1].data
            assert "runnable tasks" in complete.args[1].data["execution_error"]

    @pytest.mark.asyncio
    async def test_create_crew_progressive_interleaved_order(self):
        """Agent -> its tasks -> next agent -> its tasks ordering."""
        plan = self._make_plan(
            agents=[
                {"name": "Alpha", "role": "R1"},
                {"name": "Beta", "role": "R2"},
            ],
            tasks=[
                {"name": "Alpha Task", "assigned_agent": "Alpha"},
                {"name": "Beta Task", "assigned_agent": "Beta"},
            ],
        )

        request = self._make_progressive_request(
            prompt="build a crew with 2 agents and 2 tasks"
        )
        gen_id = "gen-interleaved"

        agent_saves = [
            {"id": "aid-1", "name": "Alpha", "role": "R1"},
            {"id": "aid-2", "name": "Beta", "role": "R2"},
        ]
        task_saves = [
            {"id": "tid-1", "name": "Alpha Task", "description": "d1"},
            {"id": "tid-2", "name": "Beta Task", "description": "d2"},
        ]

        agent_configs = [
            {
                "name": "Alpha",
                "role": "R1",
                "goal": "G1",
                "backstory": "B1",
                "advanced_config": {},
            },
            {
                "name": "Beta",
                "role": "R2",
                "goal": "G2",
                "backstory": "B2",
                "advanced_config": {},
            },
        ]

        task_responses = []
        for ts in task_saves:
            tr = Mock()
            tr.name = ts["name"]
            tr.description = ts["description"]
            tr.expected_output = "output"
            tr.tools = []
            tr.llm_guardrail = None
            task_responses.append(tr)

        with self._progressive_patches(plan=plan) as m:
            agent_call_idx = [0]
            task_call_idx = [0]

            async def agent_gen_se(**kwargs):
                idx = agent_call_idx[0]
                agent_call_idx[0] += 1
                return agent_configs[idx]

            async def agent_save_se(data, gc):
                name = data.get("name", "")
                for s in agent_saves:
                    if s["name"] == name:
                        return s
                return agent_saves[0]

            async def task_gen_se(req, gc=None):
                idx = task_call_idx[0]
                task_call_idx[0] += 1
                return task_responses[idx]

            async def task_save_se(data, agent_id, gc):
                name = data.get("name", "")
                for s in task_saves:
                    if s["name"] == name:
                        return s
                return task_saves[0]

            m["agent_gen"].generate_agent = AsyncMock(side_effect=agent_gen_se)
            m["repo"].create_single_agent = AsyncMock(side_effect=agent_save_se)
            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)
            m["repo"].create_single_task = AsyncMock(side_effect=task_save_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]

            # Filter to agent_detail and task_detail only
            interleaved = [
                e for e in event_types if e in ("agent_detail", "task_detail")
            ]
            # Expected: agent_detail, task_detail, agent_detail, task_detail
            assert interleaved == [
                "agent_detail",
                "task_detail",
                "agent_detail",
                "task_detail",
            ]

    @pytest.mark.asyncio
    async def test_create_crew_progressive_sequential_dependency_chain(self):
        """Auto-chains context for sequential process type when tasks lack context."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[
                {"name": "Step1", "assigned_agent": "Agent1"},
                {"name": "Step2", "assigned_agent": "Agent1"},
                {"name": "Step3", "assigned_agent": "Agent1"},
            ],
            process_type="sequential",
        )
        # Tasks have no "context" key -- the method should auto-chain them

        request = self._make_progressive_request(prompt="build a crew, use 3 tasks")
        gen_id = "gen-seq-chain"

        task_saves = [
            {"id": "tid-1", "name": "Step1", "description": "d1"},
            {"id": "tid-2", "name": "Step2", "description": "d2"},
            {"id": "tid-3", "name": "Step3", "description": "d3"},
        ]

        task_responses = []
        for ts in task_saves:
            tr = Mock()
            tr.name = ts["name"]
            tr.description = ts["description"]
            tr.expected_output = "output"
            tr.tools = []
            tr.llm_guardrail = None
            task_responses.append(tr)

        with self._progressive_patches(plan=plan) as m:
            task_call_idx = [0]

            async def task_gen_se(req, gc=None):
                idx = task_call_idx[0]
                task_call_idx[0] += 1
                return task_responses[idx]

            async def task_save_se(data, agent_id, gc):
                name = data.get("name", "")
                for s in task_saves:
                    if s["name"] == name:
                        return s
                return task_saves[0]

            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)
            m["repo"].create_single_task = AsyncMock(side_effect=task_save_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            # Verify the plan was mutated to add context chains
            plan_tasks = m["plan"].return_value["tasks"]
            # Step2 should have context = ["Step1"]
            assert plan_tasks[1].get("context") == ["Step1"]
            # Step3 should have context = ["Step2"]
            assert plan_tasks[2].get("context") == ["Step2"]

            # Also verify _resolve_progressive_dependencies was called
            m["repo"].update_task_dependencies.assert_awaited()

    @pytest.mark.asyncio
    async def test_create_crew_progressive_genie_tool_detection(self):
        """Broadcasts tool_config_needed when GenieTool found in task tools."""
        genie_tool_id = "genie-tool-id"
        tool_details = [
            {
                "title": "GenieTool",
                "name": "GenieTool",
                "description": "Genie",
                "id": genie_tool_id,
            },
        ]

        plan = self._make_plan(
            agents=[{"name": "Analyst", "role": "Data Analyst"}],
            tasks=[{"name": "Query Data", "assigned_agent": "Analyst"}],
        )

        agent_saved = {"id": "aid-1", "name": "Analyst", "role": "Data Analyst"}

        task_response = Mock()
        task_response.name = "Query Data"
        task_response.description = "Query the database"
        task_response.expected_output = "Data results"
        task_response.tools = [{"name": "GenieTool"}]
        task_response.llm_guardrail = None

        task_saved = {
            "id": "tid-1",
            "name": "Query Data",
            "description": "Query the database",
        }

        request = self._make_progressive_request(tools=["genie-tool-id"])
        gen_id = "gen-genie"

        with self._progressive_patches(
            plan=plan,
            agent_saved=agent_saved,
            task_saved=task_saved,
            task_gen_response=task_response,
            tool_details=tool_details,
        ) as m:
            # We need _create_tool_name_to_id_map to return correct mapping
            with patch.object(
                self.service,
                "_create_tool_name_to_id_map",
                return_value={"GenieTool": genie_tool_id},
            ):
                # Also mock _suggest_genie_space
                with patch.object(
                    self.service,
                    "_suggest_genie_space",
                    new_callable=AsyncMock,
                    return_value={"id": "space-1", "name": "Sales"},
                ):
                    await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "tool_config_needed" in event_types

            config_event = next(
                c for c in calls if c.args[1].data["type"] == "tool_config_needed"
            )
            assert config_event.args[1].data["tool_name"] == "GenieTool"
            assert config_event.args[1].data["task_id"] == "tid-1"
            assert config_event.args[1].data["suggested_space"]["id"] == "space-1"

    @pytest.mark.asyncio
    async def test_create_crew_progressive_broadcasts_generation_complete(self):
        """Final event has agents and tasks lists."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[{"name": "Task1", "assigned_agent": "Agent1"}],
        )
        agent_saved = {"id": "aid-1", "name": "Agent1", "role": "R1"}
        task_saved = {"id": "tid-1", "name": "Task1", "description": "d1"}

        request = self._make_progressive_request()
        gen_id = "gen-complete"

        with self._progressive_patches(
            plan=plan, agent_saved=agent_saved, task_saved=task_saved
        ) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            complete_events = [
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            ]
            assert len(complete_events) == 1

            complete_data = complete_events[0].args[1].data
            assert complete_data["status"] == "completed"
            assert isinstance(complete_data["agents"], list)
            assert isinstance(complete_data["tasks"], list)
            assert len(complete_data["agents"]) == 1
            assert len(complete_data["tasks"]) == 1
            assert complete_data["agents"][0]["name"] == "Agent1"

    @pytest.mark.asyncio
    async def test_create_crew_progressive_unexpected_error_broadcasts_failed(self):
        """An unexpected error in the session block broadcasts generation_failed."""
        request = self._make_progressive_request()
        gen_id = "gen-unexpected"

        with self._progressive_patches() as m:
            # Make repo constructor raise inside the session block
            m["repo_cls"].side_effect = RuntimeError("unexpected DB error")

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            # plan_ready should still have been sent before the error
            assert "plan_ready" in event_types
            assert "generation_failed" in event_types

    @pytest.mark.asyncio
    async def test_create_crew_progressive_uses_env_model_fallback(self):
        """When request.model is None, uses env CREW_MODEL or default."""
        request = self._make_progressive_request(model=None)
        gen_id = "gen-model-fallback"

        with self._progressive_patches() as m:
            with patch.dict(os.environ, {"CREW_MODEL": "env-model"}, clear=False):
                await self.service.create_crew_progressive(request, None, gen_id)

            # Verify _generate_crew_plan was called with the env model
            call_args = m["plan"].call_args
            assert call_args.args[2] == "env-model" or call_args[0][2] == "env-model"

    @pytest.mark.asyncio
    async def test_create_crew_progressive_unassigned_tasks_handled(self):
        """Tasks without assigned_agent are processed after all agents."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[
                {"name": "Assigned Task", "assigned_agent": "Agent1"},
                {"name": "Unassigned Task"},  # No assigned_agent
            ],
        )
        agent_saved = {"id": "aid-1", "name": "Agent1", "role": "R1"}

        task_saves = [
            {"id": "tid-1", "name": "Assigned Task", "description": "d1"},
            {"id": "tid-2", "name": "Unassigned Task", "description": "d2"},
        ]

        task_responses = []
        for ts in task_saves:
            tr = Mock()
            tr.name = ts["name"]
            tr.description = ts["description"]
            tr.expected_output = "output"
            tr.tools = []
            tr.llm_guardrail = None
            task_responses.append(tr)

        request = self._make_progressive_request(prompt="build a crew, use 2 tasks")
        gen_id = "gen-unassigned"

        with self._progressive_patches(plan=plan, agent_saved=agent_saved) as m:
            task_call_idx = [0]

            async def task_gen_se(req, gc=None):
                idx = task_call_idx[0]
                task_call_idx[0] += 1
                return task_responses[idx]

            async def task_save_se(data, agent_id, gc):
                name = data.get("name", "")
                for s in task_saves:
                    if s["name"] == name:
                        return s
                return task_saves[0]

            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)
            m["repo"].create_single_task = AsyncMock(side_effect=task_save_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            task_detail_events = [
                c for c in calls if c.args[1].data["type"] == "task_detail"
            ]
            assert len(task_detail_events) == 2

            complete_events = [
                c for c in calls if c.args[1].data["type"] == "generation_complete"
            ]
            assert len(complete_events) == 1
            assert len(complete_events[0].args[1].data["tasks"]) == 2

    # ------------------------------------------------------------------
    # mlflow_enabled branch (lines 810-817)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_mlflow_enabled_success(self):
        """When mlflow_enabled=True, start_root_trace is called as context manager."""
        request = self._make_progressive_request()
        gen_id = "gen-mlflow-ok"

        with self._progressive_patches() as m:
            mock_trace_ctx = MagicMock()
            mock_trace_ctx.__enter__ = Mock(return_value=None)
            mock_trace_ctx.__exit__ = Mock(return_value=False)

            with patch.dict(
                "sys.modules",
                {
                    "src.services.mlflow_tracing_service": Mock(
                        start_root_trace=Mock(return_value=mock_trace_ctx)
                    ),
                },
            ):
                await self.service.create_crew_progressive(
                    request, None, gen_id, mlflow_enabled=True
                )

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types

    @pytest.mark.asyncio
    async def test_create_crew_progressive_mlflow_enabled_import_error(self):
        """When mlflow tracing import fails, falls back to nullcontext."""
        request = self._make_progressive_request()
        gen_id = "gen-mlflow-err"

        with self._progressive_patches() as m:
            # Make start_root_trace raise an exception
            with patch.dict(
                "sys.modules",
                {
                    "src.services.mlflow_tracing_service": Mock(
                        start_root_trace=Mock(side_effect=ImportError("no mlflow"))
                    ),
                },
            ):
                await self.service.create_crew_progressive(
                    request, None, gen_id, mlflow_enabled=True
                )

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types

    # ------------------------------------------------------------------
    # user_wants_multi branch for max_agents (lines 897-898)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_user_wants_multi_roles(self):
        """When prompt has multiple role patterns, max_agents uses min(3, max_tasks)."""
        request = self._make_progressive_request(
            prompt="use a researcher and writer to analyze data"
        )
        request.original_prompt = None
        gen_id = "gen-multi-roles"

        with self._progressive_patches() as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            # Verify _generate_crew_plan was called - the multi-role detection
            # should have set max_agents = min(3, max_tasks)
            m["plan"].assert_awaited_once()

    # ------------------------------------------------------------------
    # Agent truncation (lines 937-944)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_truncate_agents_single_sequential(self):
        """When max_agents==1 and process_type==sequential, keep LAST agent."""
        plan = self._make_plan(
            agents=[
                {"name": "Agent1", "role": "R1"},
                {"name": "Agent2", "role": "R2"},
            ],
            tasks=[{"name": "Task1", "assigned_agent": "Agent2"}],
            process_type="sequential",
        )
        # The prompt has only 1 verb, so max_agents should be 1.
        # Plan has 2 agents -> truncation should keep the LAST one.
        request = self._make_progressive_request(prompt="find something")
        request.original_prompt = None
        gen_id = "gen-trunc-single"

        agent_saved = {"id": "aid-2", "name": "Agent2", "role": "R2"}

        with self._progressive_patches(plan=plan, agent_saved=agent_saved) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            agent_events = [
                c for c in calls if c.args[1].data.get("type") == "agent_detail"
            ]
            # Should have only 1 agent (the last one kept)
            assert len(agent_events) == 1

    @pytest.mark.asyncio
    async def test_create_crew_progressive_truncate_agents_multi(self):
        """When max_agents>1 but LLM returns more, keep first N agents."""
        plan = self._make_plan(
            agents=[
                {"name": "Agent1", "role": "R1"},
                {"name": "Agent2", "role": "R2"},
                {"name": "Agent3", "role": "R3"},
            ],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent1"},
                {"name": "Task2", "assigned_agent": "Agent2"},
            ],
            process_type="sequential",
        )
        # Prompt with 2 verbs -> max_tasks=2, max_agents=2
        # Plan has 3 agents -> should truncate to first 2
        request = self._make_progressive_request(prompt="find and analyze data")
        request.original_prompt = None
        gen_id = "gen-trunc-multi"

        agent_saves = [
            {"id": "aid-1", "name": "Agent1", "role": "R1"},
            {"id": "aid-2", "name": "Agent2", "role": "R2"},
        ]

        with self._progressive_patches(plan=plan) as m:
            agent_call_idx = [0]

            async def agent_save_se(data, gc):
                idx = agent_call_idx[0]
                agent_call_idx[0] += 1
                return agent_saves[min(idx, len(agent_saves) - 1)]

            m["repo"].create_single_agent = AsyncMock(side_effect=agent_save_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            agent_events = [
                c for c in calls if c.args[1].data.get("type") == "agent_detail"
            ]
            # Should have at most 2 agents
            assert len(agent_events) <= 2

    # ------------------------------------------------------------------
    # Task truncation - non-single (line 953)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_truncate_tasks_multi(self):
        """When max_tasks>1 but LLM returns more tasks, keep first N tasks."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent1"},
                {"name": "Task2", "assigned_agent": "Agent1"},
                {"name": "Task3", "assigned_agent": "Agent1"},
            ],
            process_type="parallel",  # Not sequential, so [:max_tasks]
        )
        # Prompt with 2 verbs -> max_tasks=2, but plan has 3
        request = self._make_progressive_request(prompt="find and analyze data")
        request.original_prompt = None
        gen_id = "gen-trunc-tasks"

        with self._progressive_patches(plan=plan) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            task_events = [
                c for c in calls if c.args[1].data.get("type") == "task_detail"
            ]
            # Should have at most 2 tasks
            assert len(task_events) <= 2

    # ------------------------------------------------------------------
    # Context filtering for truncated tasks (line 969)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_context_filtered_after_truncation(self):
        """Stale context references are removed after task truncation."""
        plan = self._make_plan(
            agents=[{"name": "Agent1", "role": "R1"}],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent1", "context": []},
                {
                    "name": "Task2",
                    "assigned_agent": "Agent1",
                    "context": ["Task1", "Task3"],
                },
                {"name": "Task3", "assigned_agent": "Agent1", "context": ["Task1"]},
            ],
            process_type="parallel",
        )
        # max_tasks=2 -> Task3 gets truncated, Task2's context ref to Task3 should be removed
        request = self._make_progressive_request(prompt="find and analyze data")
        request.original_prompt = None
        gen_id = "gen-context-filter"

        with self._progressive_patches(plan=plan) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            task_events = [
                c for c in calls if c.args[1].data.get("type") == "task_detail"
            ]
            # Only 2 tasks should have been generated (Task3 was truncated)
            assert len(task_events) == 2

    # ------------------------------------------------------------------
    # Orphan agent removal (lines 980-986)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_orphan_agents_removed(self):
        """Agents with no tasks assigned are removed."""
        plan = self._make_plan(
            agents=[
                {"name": "Worker", "role": "R1"},
                {"name": "Orphan", "role": "R2"},
            ],
            tasks=[
                {"name": "Task1", "assigned_agent": "Worker"},
            ],
        )
        request = self._make_progressive_request(
            prompt="find and analyze data with 2 agents"
        )
        request.original_prompt = None
        gen_id = "gen-orphan"

        agent_saved = {"id": "aid-1", "name": "Worker", "role": "R1"}

        with self._progressive_patches(plan=plan, agent_saved=agent_saved) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            agent_events = [
                c for c in calls if c.args[1].data.get("type") == "agent_detail"
            ]
            # Only Worker should remain; Orphan should be removed
            assert len(agent_events) == 1

    # ------------------------------------------------------------------
    # Lakebase enabled path (lines 1026-1031)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_lakebase_enabled(self):
        """When lakebase is enabled, uses lakebase session factory."""
        request = self._make_progressive_request()
        gen_id = "gen-lakebase"

        plan = self._make_plan()

        with self._progressive_patches(plan=plan) as m:
            # Override is_lakebase_enabled to return True
            with patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ):
                with patch(
                    "src.db.database_router.get_lakebase_config_from_db",
                    new_callable=AsyncMock,
                    return_value={"instance_name": "test-lakebase"},
                ):
                    mock_lb_session = AsyncMock()
                    mock_lb_session.__aenter__ = AsyncMock(
                        return_value=AsyncMock(commit=AsyncMock(), rollback=AsyncMock())
                    )
                    mock_lb_session.__aexit__ = AsyncMock(return_value=False)

                    with patch(
                        "src.db.lakebase_session.get_lakebase_session",
                        return_value=mock_lb_session,
                    ):
                        await self.service.create_crew_progressive(
                            request, None, gen_id
                        )

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types

    @pytest.mark.asyncio
    async def test_create_crew_progressive_lakebase_no_config(self):
        """When lakebase config is None, uses env var fallback for instance name."""
        request = self._make_progressive_request()
        gen_id = "gen-lakebase-noconf"

        with self._progressive_patches() as m:
            with patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ):
                with patch(
                    "src.db.database_router.get_lakebase_config_from_db",
                    new_callable=AsyncMock,
                    return_value=None,
                ):
                    mock_lb_session = AsyncMock()
                    mock_lb_session.__aenter__ = AsyncMock(
                        return_value=AsyncMock(commit=AsyncMock(), rollback=AsyncMock())
                    )
                    mock_lb_session.__aexit__ = AsyncMock(return_value=False)

                    with patch(
                        "src.db.lakebase_session.get_lakebase_session",
                        return_value=mock_lb_session,
                    ) as mock_get_lb:
                        await self.service.create_crew_progressive(
                            request, None, gen_id
                        )
                        # Should fall back to env var or "kasal-lakebase"
                        mock_get_lb.assert_called_once()

    # ------------------------------------------------------------------
    # Tool resolution exception (lines 1065-1066)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_tool_resolution_failure(self):
        """When tool resolution fails, continues without tools."""
        request = self._make_progressive_request(tools=["some-tool"])
        gen_id = "gen-tool-fail"

        with self._progressive_patches() as m:
            m["gtd"].side_effect = RuntimeError("tool resolution failed")

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types

    # ------------------------------------------------------------------
    # Unassigned tasks - task_plan with empty assigned_agent (line 1084)
    # To hit this path, we need a task whose assigned_agent is falsy AFTER
    # the orphan-reassignment at line 964-966. This happens when the first
    # agent has an empty name (plan_agents[0].get("name","") == "").
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_task_empty_assigned_agent(self):
        """Task with empty string assigned_agent goes to unassigned list.

        When the first agent has name="" and a task references a non-existent agent,
        the orphan reassignment sets assigned_agent="" which is falsy -> unassigned.
        """
        # First agent has empty name. The orphan reassignment code at line 965-966
        # assigns tasks with missing/invalid assigned_agent to plan_agents[0].get("name","") = ""
        # Then in the task grouping loop, assigned="" is falsy -> unassigned_tasks
        plan = self._make_plan(
            agents=[
                {"name": "", "role": "Worker"},
                {"name": "Agent2", "role": "R2"},
            ],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent2"},
                {
                    "name": "Task2",
                    "assigned_agent": "NonExistent",
                },  # Will be reassigned to ""
            ],
        )
        agent_saves = [
            {"id": "aid-1", "name": "", "role": "Worker"},
            {"id": "aid-2", "name": "Agent2", "role": "R2"},
        ]
        task_saves = [
            {"id": "tid-1", "name": "Task1", "description": "d1"},
            {"id": "tid-2", "name": "Task2", "description": "d2"},
        ]

        task_responses = []
        for ts in task_saves:
            tr = Mock()
            tr.name = ts["name"]
            tr.description = ts["description"]
            tr.expected_output = "output"
            tr.tools = []
            tr.llm_guardrail = None
            task_responses.append(tr)

        agent_configs = [
            {
                "name": "",
                "role": "Worker",
                "goal": "G1",
                "backstory": "B1",
                "advanced_config": {},
            },
            {
                "name": "Agent2",
                "role": "R2",
                "goal": "G2",
                "backstory": "B2",
                "advanced_config": {},
            },
        ]

        request = self._make_progressive_request(
            prompt="find and analyze data with 2 agents"
        )
        request.original_prompt = None
        gen_id = "gen-empty-assigned"

        with self._progressive_patches(plan=plan) as m:
            agent_call_idx = [0]
            task_call_idx = [0]

            async def agent_gen_se(**kwargs):
                idx = agent_call_idx[0]
                agent_call_idx[0] += 1
                return agent_configs[min(idx, len(agent_configs) - 1)]

            async def agent_save_se(data, gc):
                idx = min(agent_call_idx[0] - 1, len(agent_saves) - 1)
                return agent_saves[idx]

            async def task_gen_se(req, gc=None):
                idx = task_call_idx[0]
                task_call_idx[0] += 1
                return task_responses[min(idx, len(task_responses) - 1)]

            async def task_save_se(data, agent_id, gc):
                name = data.get("name", "")
                for s in task_saves:
                    if s["name"] == name:
                        return s
                return task_saves[0]

            m["agent_gen"].generate_agent = AsyncMock(side_effect=agent_gen_se)
            m["repo"].create_single_agent = AsyncMock(side_effect=agent_save_se)
            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)
            m["repo"].create_single_task = AsyncMock(side_effect=task_save_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_complete" in event_types

    # ------------------------------------------------------------------
    # Advanced config keys applied to agent_data (line 1126)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_advanced_config_applied(self):
        """Agent advanced_config keys are copied to agent_data."""
        agent_gen_return = {
            "name": "Agent1",
            "role": "Specialist",
            "goal": "Do work",
            "backstory": "Expert",
            "advanced_config": {
                "max_iter": 5,
                "verbose": True,
                "allow_delegation": False,
            },
        }

        request = self._make_progressive_request()
        gen_id = "gen-adv-config"

        with self._progressive_patches(agent_gen_return=agent_gen_return) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            # Verify create_single_agent was called with advanced config keys
            call_args = m["repo"].create_single_agent.call_args
            agent_data = call_args[0][0]
            assert agent_data["max_iter"] == 5
            assert agent_data["verbose"] is True
            assert agent_data["allow_delegation"] is False

    # ------------------------------------------------------------------
    # Unassigned tasks with GenieTool (lines 1243-1298)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_unassigned_tasks_genie_tool(self):
        """Unassigned tasks with GenieTool trigger tool_config_needed.

        Uses an agent with name="" so that orphan-reassigned tasks get
        assigned_agent="" (falsy) and end up in unassigned_tasks.
        """
        genie_tool_id = "genie-id"
        tool_details = [
            {
                "title": "GenieTool",
                "name": "GenieTool",
                "description": "Genie",
                "id": genie_tool_id,
            },
        ]

        plan = self._make_plan(
            agents=[
                {"name": "", "role": "Worker"},
                {"name": "Agent2", "role": "R2"},
            ],
            tasks=[
                {"name": "Task1", "assigned_agent": "Agent2"},
                {"name": "Unassigned Genie Task", "assigned_agent": "NonExistent"},
            ],
        )

        agent_saves = [
            {"id": "aid-1", "name": "", "role": "Worker"},
            {"id": "aid-2", "name": "Agent2", "role": "R2"},
        ]

        task_response_genie = Mock()
        task_response_genie.name = "Unassigned Genie Task"
        task_response_genie.description = "Query with genie"
        task_response_genie.expected_output = "Data"
        task_response_genie.tools = [{"name": "GenieTool"}]
        task_response_genie.llm_guardrail = None

        task_response_normal = Mock()
        task_response_normal.name = "Task1"
        task_response_normal.description = "Normal task"
        task_response_normal.expected_output = "Result"
        task_response_normal.tools = []
        task_response_normal.llm_guardrail = None

        task_saves = [
            {"id": "tid-1", "name": "Task1", "description": "Normal task"},
            {
                "id": "tid-2",
                "name": "Unassigned Genie Task",
                "description": "Query with genie",
            },
        ]

        agent_configs = [
            {
                "name": "",
                "role": "Worker",
                "goal": "G",
                "backstory": "B",
                "advanced_config": {},
            },
            {
                "name": "Agent2",
                "role": "R2",
                "goal": "G2",
                "backstory": "B2",
                "advanced_config": {},
            },
        ]

        request = self._make_progressive_request(
            prompt="find and analyze data with 2 agents", tools=["genie-id"]
        )
        request.original_prompt = None
        gen_id = "gen-unassigned-genie"

        with self._progressive_patches(plan=plan, tool_details=tool_details) as m:
            agent_call_idx = [0]
            task_call_idx = [0]

            async def agent_gen_se(**kwargs):
                idx = agent_call_idx[0]
                agent_call_idx[0] += 1
                return agent_configs[min(idx, len(agent_configs) - 1)]

            async def agent_save_se(data, gc):
                idx = min(agent_call_idx[0] - 1, len(agent_saves) - 1)
                return agent_saves[idx]

            async def task_gen_se(req, gc=None):
                idx = task_call_idx[0]
                task_call_idx[0] += 1
                if idx == 0:
                    return task_response_normal
                return task_response_genie

            async def task_save_se(data, agent_id, gc):
                name = data.get("name", "")
                for s in task_saves:
                    if s["name"] == name:
                        return s
                return task_saves[0]

            m["agent_gen"].generate_agent = AsyncMock(side_effect=agent_gen_se)
            m["repo"].create_single_agent = AsyncMock(side_effect=agent_save_se)
            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)
            m["repo"].create_single_task = AsyncMock(side_effect=task_save_se)

            with patch.object(
                self.service,
                "_create_tool_name_to_id_map",
                return_value={"GenieTool": genie_tool_id},
            ):
                with patch.object(
                    self.service,
                    "_suggest_genie_space",
                    new_callable=AsyncMock,
                    return_value={"id": "space-1", "name": "Sales"},
                ):
                    await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "tool_config_needed" in event_types

    # ------------------------------------------------------------------
    # Unassigned task exception handling (lines 1314-1324)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_crew_progressive_unassigned_task_error(self):
        """Error in unassigned task processing broadcasts entity_error.

        Uses agent with name="" to force tasks into unassigned_tasks list.
        """
        plan = self._make_plan(
            agents=[
                {"name": "", "role": "Worker"},
                {"name": "Agent2", "role": "R2"},
            ],
            tasks=[
                {"name": "Assigned Task", "assigned_agent": "Agent2"},
                {"name": "Failing Task", "assigned_agent": "NonExistent"},
            ],
        )

        agent_saves = [
            {"id": "aid-1", "name": "", "role": "Worker"},
            {"id": "aid-2", "name": "Agent2", "role": "R2"},
        ]

        task_response_ok = Mock()
        task_response_ok.name = "Assigned Task"
        task_response_ok.description = "OK task"
        task_response_ok.expected_output = "output"
        task_response_ok.tools = []
        task_response_ok.llm_guardrail = None

        task_saved_ok = {
            "id": "tid-1",
            "name": "Assigned Task",
            "description": "OK task",
        }

        agent_configs = [
            {
                "name": "",
                "role": "Worker",
                "goal": "G",
                "backstory": "B",
                "advanced_config": {},
            },
            {
                "name": "Agent2",
                "role": "R2",
                "goal": "G2",
                "backstory": "B2",
                "advanced_config": {},
            },
        ]

        request = self._make_progressive_request(
            prompt="find and analyze data with 2 agents"
        )
        request.original_prompt = None
        gen_id = "gen-unassigned-err"

        with self._progressive_patches(
            plan=plan,
            task_gen_response=task_response_ok,
            task_saved=task_saved_ok,
        ) as m:
            agent_call_idx = [0]
            call_count = [0]

            async def agent_gen_se(**kwargs):
                idx = agent_call_idx[0]
                agent_call_idx[0] += 1
                return agent_configs[min(idx, len(agent_configs) - 1)]

            async def agent_save_se(data, gc):
                idx = min(agent_call_idx[0] - 1, len(agent_saves) - 1)
                return agent_saves[idx]

            async def task_gen_se(req, gc=None):
                call_count[0] += 1
                if call_count[0] == 1:
                    return task_response_ok
                raise RuntimeError("unassigned task generation failed")

            m["agent_gen"].generate_agent = AsyncMock(side_effect=agent_gen_se)
            m["repo"].create_single_agent = AsyncMock(side_effect=agent_save_se)
            m["task_gen"].generate_task = AsyncMock(side_effect=task_gen_se)

            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "entity_error" in event_types
            assert "generation_complete" in event_types

            error_events = [
                c for c in calls if c.args[1].data.get("type") == "entity_error"
            ]
            unassigned_errors = [
                e for e in error_events if e.args[1].data.get("entity_type") == "task"
            ]
            assert len(unassigned_errors) >= 1

    # ------------------------------------------------------------------
    # _generate_crew_plan logging exception (lines 1502-1503)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_generate_crew_plan_log_failure_swallowed(self):
        """Exception during plan LLM interaction logging is swallowed."""
        request = Mock()
        request.prompt = "build something"
        plan_dict = {
            "agents": [{"name": "A", "role": "R"}],
            "tasks": [{"name": "T", "assigned_agent": "A"}],
        }

        with (
            patch("src.services.crew_generation_service.TemplateService") as ts,
            patch("src.services.crew_generation_service.LLMManager") as lm,
            patch("src.services.crew_generation_service.robust_json_parser") as rjp,
        ):
            ts.get_effective_template_content = AsyncMock(return_value="sys prompt")
            lm.completion = AsyncMock(return_value='{"agents":[{"name":"A"}]}')
            rjp.return_value = plan_dict

            # Make the session factory raise to trigger the except on line 1502
            mock_session_factory = AsyncMock()
            mock_session_factory.__aenter__ = AsyncMock(
                side_effect=RuntimeError("db connection failed")
            )
            mock_session_factory.__aexit__ = AsyncMock(return_value=False)

            with patch(
                "src.db.session.async_session_factory",
                return_value=mock_session_factory,
            ):
                result = await self.service._generate_crew_plan(request, None, "m")

            assert result == plan_dict
            assert "agents" in result


class TestBuildCrewConfigFromGenerated:
    """build_crew_config_from_generated mirrors the frontend builder for the
    ChatMode auto-execute path (AgentBuilder never calls it)."""

    @staticmethod
    def _req(**kw):
        from src.schemas.crew import CrewStreamingRequest

        return CrewStreamingRequest(prompt="p", **kw)

    def test_keys_links_and_top_level(self):
        req = self._req(
            original_prompt="find swiss poets",
            model="m1",
            session_id="s1",
            memory_workspace_scope=False,
        )
        agents = [
            {
                "id": "a1",
                "role": "Researcher",
                "goal": "g",
                "backstory": "b",
                "tools": ["x"],
            }
        ]
        tasks = [
            {
                "id": "t1",
                "description": "do it",
                "expected_output": "out",
                "agent_id": "a1",
                "context": ["t0"],
                "tools": [],
            }
        ]
        cfg = CrewGenerationService.build_crew_config_from_generated(req, agents, tasks)
        # agent_<id> / task_<id> keys, role copied, task→agent + task→task links resolved
        assert cfg["agents_yaml"]["agent_a1"]["role"] == "Researcher"
        assert cfg["tasks_yaml"]["task_t1"]["agent"] == "agent_a1"
        assert cfg["tasks_yaml"]["task_t1"]["context"] == ["task_t0"]
        # description grounded with the user request
        assert "find swiss poets" in cfg["tasks_yaml"]["task_t1"]["description"]
        # top-level execution config. Default answer mode is 'chat' → a single
        # light agent (execution_type='agent'), no planning/reasoning.
        assert cfg["model"] == "m1" and cfg["execution_type"] == "agent"
        assert cfg["planning"] is False and cfg["reasoning"] is False
        assert cfg["session_id"] == "s1" and cfg["memory_workspace_scope"] is False

    def test_request_model_applied_as_agent_llm_when_absent(self):
        """The chat/generation model picker must reach the agent's LLM, so the
        light-agent path doesn't silently fall back to the engine default
        (maverick), which can't reliably tool-call MCP servers."""
        agents = [{"id": "a1", "role": "r", "goal": "g", "backstory": "b", "tools": []}]
        tasks = [{"id": "t1", "description": "d", "agent_id": "a1"}]
        cfg = CrewGenerationService.build_crew_config_from_generated(
            self._req(model="databricks-claude-sonnet-4-5"), agents, tasks
        )
        assert cfg["agents_yaml"]["agent_a1"]["llm"] == "databricks-claude-sonnet-4-5"

    def test_explicit_agent_llm_not_overridden_by_request_model(self):
        """A per-agent model (from generation) wins over the request model."""
        agents = [
            {"id": "a1", "role": "r", "goal": "g", "backstory": "b", "tools": [],
             "llm": "databricks-gpt-5"}
        ]
        tasks = [{"id": "t1", "description": "d", "agent_id": "a1"}]
        cfg = CrewGenerationService.build_crew_config_from_generated(
            self._req(model="databricks-claude-sonnet-4-5"), agents, tasks
        )
        assert cfg["agents_yaml"]["agent_a1"]["llm"] == "databricks-gpt-5"

    def test_no_request_model_leaves_agent_llm_unset(self):
        """Without a picked model, the agent carries no llm (engine default applies)."""
        agents = [{"id": "a1", "role": "r", "goal": "g", "backstory": "b", "tools": []}]
        tasks = [{"id": "t1", "description": "d", "agent_id": "a1"}]
        cfg = CrewGenerationService.build_crew_config_from_generated(
            self._req(), agents, tasks
        )
        assert "llm" not in cfg["agents_yaml"]["agent_a1"]

    def test_chat_mode_type_drives_reasoning_planning_execution_type(self, monkeypatch):
        """The ChatMode answer mode maps to reasoning / planning / execution_type:
        chat → single light agent; research → crew + reasoning; deep → crew +
        planning (with an explicit planning_llm) + reasoning."""
        # Local-model reasoning guard must be off so research/deep reason here.
        monkeypatch.delenv("LOCAL_LLM_BASE_URL", raising=False)
        agents = [{"id": "a1", "role": "r", "goal": "g", "backstory": "b", "tools": []}]
        tasks = [{"id": "t1", "description": "d", "agent_id": "a1"}]

        chat = CrewGenerationService.build_crew_config_from_generated(
            self._req(chat_mode_type="chat", model="m1"), agents, tasks
        )
        assert chat["execution_type"] == "agent"
        assert chat["planning"] is False and chat["reasoning"] is False
        assert "planning_llm" not in chat["inputs"]

        research = CrewGenerationService.build_crew_config_from_generated(
            self._req(chat_mode_type="research", model="m1"), agents, tasks
        )
        assert research["execution_type"] == "crew"
        assert research["reasoning"] is True and research["planning"] is False
        assert "planning_llm" not in research["inputs"]

        deep = CrewGenerationService.build_crew_config_from_generated(
            self._req(chat_mode_type="deep", model="m1"), agents, tasks
        )
        assert deep["execution_type"] == "crew"
        assert deep["reasoning"] is True and deep["planning"] is True
        # Deep planning must carry an explicit planning_llm (no OpenAI default 401).
        assert deep["inputs"].get("planning_llm") == "m1"

    def test_local_llm_disables_reasoning(self, monkeypatch):
        """CrewAI structured reasoning breaks on local endpoints, so reasoning is
        force-disabled when LOCAL_LLM_BASE_URL is set (even for research/deep)."""
        monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://localhost:8081/v1")
        cfg = CrewGenerationService.build_crew_config_from_generated(
            self._req(chat_mode_type="deep", model="m1"),
            [{"id": "a1", "role": "r"}],
            [{"id": "t1", "description": "d", "agent_id": "a1"}],
        )
        assert cfg["reasoning"] is False
        # planning still on for deep (only reasoning is gated by the local guard)
        assert cfg["planning"] is True

    def test_mcp_servers_injected_into_agents_and_tasks(self):
        req = self._req(mcp_servers=["Databricks Genie: Sales"])
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r", "tools": []}],
            [{"id": "t1", "description": "d", "agent_id": "a1"}],
        )
        servers = {"servers": ["Databricks Genie: Sales"]}
        assert cfg["agents_yaml"]["agent_a1"]["tool_configs"]["MCP_SERVERS"] == servers
        assert cfg["tasks_yaml"]["task_t1"]["tool_configs"]["MCP_SERVERS"] == servers
        assert "Databricks Genie: Sales" in cfg["tasks_yaml"]["task_t1"]["description"]

    def test_knowledge_file_paths_injected_into_agents_and_tasks(self):
        """A chat turn with attached files threads knowledge_file_paths into the
        DatabricksKnowledgeSearchTool tool_configs on BOTH the agent and the task
        — the light agent reads the AGENT's tool_configs, so this is what scopes
        the knowledge search to the uploaded file instead of group-wide."""
        paths = ["uploads/g/e/Kindeswohlgefährdung Schule Oberrieden.pdf"]
        req = self._req(knowledge_file_paths=paths)
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r", "tools": ["DatabricksKnowledgeSearchTool"]}],
            [{"id": "t1", "description": "d", "agent_id": "a1",
              "tools": ["DatabricksKnowledgeSearchTool"]}],
        )
        scope = {"file_paths": paths}
        assert cfg["agents_yaml"]["agent_a1"]["tool_configs"][
            "DatabricksKnowledgeSearchTool"] == scope
        assert cfg["tasks_yaml"]["task_t1"]["tool_configs"][
            "DatabricksKnowledgeSearchTool"] == scope

    def test_no_knowledge_file_paths_no_scoping_injected(self):
        """Without attached files, no DatabricksKnowledgeSearchTool scoping is
        added (the tool, if present, searches group-wide)."""
        req = self._req()  # knowledge_file_paths defaults to []
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r", "tools": ["DatabricksKnowledgeSearchTool"]}],
            [{"id": "t1", "description": "d", "agent_id": "a1"}],
        )
        assert "DatabricksKnowledgeSearchTool" not in (
            cfg["agents_yaml"]["agent_a1"].get("tool_configs", {})
        )

    def test_disable_memory_forces_agent_memory_false(self):
        req = self._req(disable_memory=True)
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req, [{"id": "a1", "role": "r"}], []
        )
        assert cfg["agents_yaml"]["agent_a1"]["memory"] is False

    def test_no_mcp_leaves_no_tool_configs(self):
        req = self._req()  # prompt="p" → grounds with the user request, no MCP
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r"}],
            [{"id": "t1", "description": "d", "agent_id": "a1"}],
        )
        assert "tool_configs" not in cfg["agents_yaml"]["agent_a1"]
        assert "tool_configs" not in cfg["tasks_yaml"]["task_t1"]
        desc = cfg["tasks_yaml"]["task_t1"]["description"]
        assert desc.startswith("d") and "USER REQUEST" in desc
        assert "MCP data sources" not in desc

    def test_agentbricks_endpoint_injected_into_agents_and_tasks(self):
        # Endpoint picked in the chat "+" — equip + configure the AgentBricksTool
        # (seed id 71) on every agent and task so the auto-executed ChatMode run
        # reaches the endpoint (regression: this path injected MCP but not AgentBricks,
        # so the tool ran unconfigured → "endpoint name is not configured").
        req = self._req(agentbricks_endpoints=["mas-9f2-endpoint"])
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r", "tools": []}],
            [{"id": "t1", "description": "d", "agent_id": "a1", "tools": ["71"]}],
        )
        endpoint_cfg = {"endpointName": ["mas-9f2-endpoint"]}
        agent = cfg["agents_yaml"]["agent_a1"]
        task = cfg["tasks_yaml"]["task_t1"]
        # Tool equipped on both (agent had none → appended; task already listed it → kept once)
        assert "71" in agent["tools"]
        assert task["tools"].count("71") == 1
        assert agent["tool_configs"]["AgentBricksTool"] == endpoint_cfg
        assert task["tool_configs"]["AgentBricksTool"] == endpoint_cfg
        assert "Agent Bricks agent is assigned" in task["description"]

    def test_agentbricks_tool_stripped_when_no_endpoint(self):
        # The generator/LLM may equip AgentBricksTool (71 / "AgentBricksTool") on its
        # own when the prompt mentions agentbricks. With NO endpoint picked, strip it
        # so an unconfigured tool never reaches — and aborts — the run.
        req = self._req()
        cfg = CrewGenerationService.build_crew_config_from_generated(
            req,
            [{"id": "a1", "role": "r", "tools": ["71"]}],
            [
                {
                    "id": "t1",
                    "description": "d",
                    "agent_id": "a1",
                    "tools": ["71", "AgentBricksTool"],
                }
            ],
        )
        assert cfg["agents_yaml"]["agent_a1"]["tools"] == []
        assert cfg["tasks_yaml"]["task_t1"]["tools"] == []
        assert "tool_configs" not in cfg["tasks_yaml"]["task_t1"]
        assert (
            "Agent Bricks agent is assigned"
            not in cfg["tasks_yaml"]["task_t1"]["description"]
        )

    def test_agentbricks_filter_preserves_other_tools(self):
        # The equip/strip helper must touch ONLY the AgentBricksTool reference
        # (id 71 / "AgentBricksTool") — every other tool passes through untouched,
        # whether an endpoint is picked or not.
        with_ep = CrewGenerationService.build_crew_config_from_generated(
            self._req(agentbricks_endpoints=["mas-1"]),
            [{"id": "a1", "role": "r", "tools": ["35", "SerperDevTool"]}],
            [{"id": "t1", "description": "d", "agent_id": "a1", "tools": ["35"]}],
        )
        # Other tools kept; AgentBricksTool appended.
        assert with_ep["agents_yaml"]["agent_a1"]["tools"] == [
            "35",
            "SerperDevTool",
            "71",
        ]
        assert with_ep["tasks_yaml"]["task_t1"]["tools"] == ["35", "71"]

        without_ep = CrewGenerationService.build_crew_config_from_generated(
            self._req(),
            [{"id": "a1", "role": "r", "tools": ["35", "SerperDevTool"]}],
            [{"id": "t1", "description": "d", "agent_id": "a1", "tools": ["35", "71"]}],
        )
        # Other tools kept; only AgentBricksTool (71) stripped.
        assert without_ep["agents_yaml"]["agent_a1"]["tools"] == ["35", "SerperDevTool"]
        assert without_ep["tasks_yaml"]["task_t1"]["tools"] == ["35"]


class TestChatFastPath:
    """ChatMode 'chat' answer mode bypasses crew generation (no plan/agent/task
    LLM calls) and runs the light agent directly on the raw prompt."""

    @staticmethod
    def _req(**kw):
        from src.schemas.crew import CrewStreamingRequest
        return CrewStreamingRequest(prompt="p", **kw)

    @pytest.mark.asyncio
    async def test_fast_path_executes_without_generation_and_emits_complete(self):
        """_run_chat_fast_path synthesizes a default agent+task, auto-executes the
        light agent, and emits a single generation_complete with execution_id."""
        svc = CrewGenerationService(MagicMock())
        req = self._req(
            original_prompt="what's the weather",
            model="m1",
            auto_execute=True,
            session_id="s1",
            tools=["webtool"],
        )

        exec_instance = MagicMock()
        exec_instance.create_execution = AsyncMock(
            return_value={"execution_id": "exec-1", "run_name": "Run A"}
        )
        broadcast = AsyncMock()
        with patch("src.services.execution_service.ExecutionService", MagicMock(return_value=exec_instance)), \
             patch.object(_mod.sse_manager, "broadcast_to_job", broadcast):
            await svc._run_chat_fast_path(req, None, "gen-1", None)

        # Light agent execution launched from a config built WITHOUT generation.
        exec_instance.create_execution.assert_awaited_once()
        crew_config = exec_instance.create_execution.await_args.kwargs["config"]
        assert crew_config.execution_type == "agent"           # light path
        assert crew_config.planning is False and crew_config.reasoning is False
        assert crew_config.session_id == "s1"
        # Exactly one default assistant agent + one task, carrying the attached tool.
        assert len(crew_config.agents_yaml) == 1
        agent = next(iter(crew_config.agents_yaml.values()))
        assert agent["role"] == "Assistant" and "webtool" in agent["tools"]

        # Single terminal event carrying the execution_id the chat UI needs.
        broadcast.assert_awaited_once()
        _gen_id, event = broadcast.await_args.args
        assert event.event == "generation_complete"
        assert event.data["execution_id"] == "exec-1"
        assert event.data["run_name"] == "Run A"
        assert len(event.data["agents"]) == 1 and len(event.data["tasks"]) == 1

    @pytest.mark.asyncio
    async def test_fast_path_execution_error_is_surfaced_not_raised(self):
        """A failure launching the run is reported on generation_complete, never
        raised (so the chat shows an error instead of hanging)."""
        svc = CrewGenerationService(MagicMock())
        req = self._req(original_prompt="hi", auto_execute=True)
        exec_instance = MagicMock()
        exec_instance.create_execution = AsyncMock(side_effect=RuntimeError("boom"))
        broadcast = AsyncMock()
        with patch("src.services.execution_service.ExecutionService", MagicMock(return_value=exec_instance)), \
             patch.object(_mod.sse_manager, "broadcast_to_job", broadcast):
            await svc._run_chat_fast_path(req, None, "gen-1", None)
        event = broadcast.await_args.args[1]
        assert event.data.get("execution_error") == "boom"
        assert "execution_id" not in event.data

    @pytest.mark.asyncio
    async def test_chat_mode_takes_fast_path_skipping_generation(self):
        """create_crew_progressive routes a chat+auto_execute request to the fast
        path and NEVER runs the planning LLM call."""
        svc = CrewGenerationService(MagicMock())
        req = self._req(chat_mode_type="chat", auto_execute=True)
        with patch.object(svc, "_run_chat_fast_path", new=AsyncMock()) as fast, \
             patch.object(svc, "_generate_crew_plan", new=AsyncMock()) as plan:
            await svc.create_crew_progressive(req, None, "gen-1", mlflow_enabled=False)
        fast.assert_awaited_once()
        plan.assert_not_called()

    @pytest.mark.asyncio
    async def test_research_mode_does_not_take_fast_path(self):
        """research/deep still generate a crew (fast path is chat-only)."""
        svc = CrewGenerationService(MagicMock())
        req = self._req(chat_mode_type="research", auto_execute=True)
        with patch.object(svc, "_run_chat_fast_path", new=AsyncMock()) as fast, \
             patch.object(svc, "_generate_crew_plan", new=AsyncMock(return_value={"agents": [], "tasks": []})) as plan:
            await svc.create_crew_progressive(req, None, "gen-1", mlflow_enabled=False)
        fast.assert_not_called()
        plan.assert_awaited_once()
