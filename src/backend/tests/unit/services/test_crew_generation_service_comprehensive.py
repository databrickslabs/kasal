"""
Comprehensive unit tests for CrewGenerationService.

Targets 100% code coverage of src/services/crew_generation_service.py.
"""
import json
import logging
import os
import sys
import traceback
import uuid
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, patch, AsyncMock, MagicMock, PropertyMock

import pytest

import src.services.crew_generation_service as _mod
from src.services.crew_generation_service import CrewGenerationService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_service():
    """Build a CrewGenerationService with all external deps mocked out."""
    mock_session = Mock()
    with patch("src.services.crew_generation_service.LLMLogService") as mock_log_svc_cls, \
         patch("src.services.crew_generation_service.LLMLogRepository") as mock_log_repo_cls, \
         patch("src.services.crew_generation_service.CrewGeneratorRepository") as mock_crew_repo_cls:
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
            {"name": "Agent1", "role": "r1", "goal": "g1", "backstory": "b1", "tools": ["ToolA"]},
        ]
    if tasks is None:
        tasks = [
            {"name": "Task1", "description": "d1", "agent": "Agent1", "tools": ["ToolA"]},
        ]
    return {"agents": agents, "tasks": tasks}


def _allowed_tools():
    return [{"name": "ToolA", "id": "id-a"}, {"name": "ToolB", "id": "id-b"}]


def _tool_id_map():
    return {"ToolA": "id-a", "ToolB": "id-b"}


def _crew_complete_patches(service, gtd_return=None, ppt_return="sys", pcs_return=None,
                           model_params=None, llm_content="{}", rjp_return=None,
                           completion_side_effect=None, crew_repo_return=None):
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
            p3 = patch.object(service, "_prepare_prompt_template", new_callable=AsyncMock)
            p4 = patch("src.services.crew_generation_service.LLMManager")
            p5 = patch("src.services.crew_generation_service.robust_json_parser")
            p6 = patch.object(service, "_process_crew_setup")

            self._patches = [p1, p2, p3, p4, p5, p6]
            ms = [p.start() for p in self._patches]

            ms[1].return_value = gtd_return          # _get_tool_details
            ms[2].return_value = ppt_return           # _prepare_prompt_template
            if completion_side_effect:
                ms[3].completion = AsyncMock(side_effect=completion_side_effect)
            else:
                ms[3].completion = AsyncMock(return_value=llm_content)
            ms[4].return_value = rjp_return           # robust_json_parser
            ms[5].return_value = pcs_return            # _process_crew_setup

            self.mocks = {
                "ts_cls": ms[0], "gtd": ms[1], "ppt": ms[2],
                "lm": ms[3], "rjp": ms[4], "pcs": ms[5],
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
        with patch("src.services.crew_generation_service.LLMLogService") as log_svc, \
             patch("src.services.crew_generation_service.LLMLogRepository") as log_repo, \
             patch("src.services.crew_generation_service.CrewGeneratorRepository") as crew_repo:
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
            endpoint="ep", prompt="p", response="r", model="m",
            status="success", error_message=None, group_context=None,
        )
        self.log_svc.create_log.assert_awaited_once_with(
            endpoint="ep", prompt="p", response="r", model="m",
            status="success", error_message=None, group_context=None,
        )

    @pytest.mark.asyncio
    async def test_log_with_group_context(self):
        gc = Mock()
        self.log_svc.create_log = AsyncMock()
        await self.service._log_llm_interaction(
            endpoint="ep", prompt="p", response="r", model="m",
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
            endpoint="ep", prompt="p", response="r", model="m",
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
            tools = [{"name": "MyTool", "description": "desc", "parameters": {
                "p1": {"description": "pd", "type": "str"}
            }}]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "Available tools:" in result
            assert "MyTool" in result
            assert "p1 (str): pd" in result
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
    async def test_tool_parameter_missing_fields(self):
        with patch("src.services.crew_generation_service.TemplateService") as ts:
            ts.get_effective_template_content = AsyncMock(return_value="BASE")
            tools = [{"name": "T", "description": "d", "parameters": {
                "x": {}  # missing description and type
            }}]
            result = await self.service._prepare_prompt_template(tools, None)
            assert "No description" in result
            assert "(any)" in result

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
            ts.get_effective_template_content.assert_awaited_once_with("generate_crew", gc)


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
            self.service._process_crew_setup({"agents": [], "tasks": [{"name": "t"}]}, [], {})

    def test_agents_not_list_raises(self):
        """When agents is a tuple (iterable but not list), isinstance check catches it."""
        with pytest.raises(ValueError, match="agents"):
            self.service._process_crew_setup({"agents": ({"name": "a"},), "tasks": [{"name": "t"}]}, [], {})

    def test_missing_tasks_key_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {"agents": [{"name": "a", "role": "r", "goal": "g", "backstory": "b"}]},
                [], {}
            )

    def test_empty_tasks_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {"agents": [{"name": "a", "role": "r", "goal": "g", "backstory": "b"}], "tasks": []},
                [], {}
            )

    def test_tasks_not_list_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            self.service._process_crew_setup(
                {"agents": [{"name": "a", "role": "r", "goal": "g", "backstory": "b"}], "tasks": ({"name": "t"},)},
                [], {}
            )

    def test_agent_missing_required_field_raises(self):
        setup = {"agents": [{"name": "a", "role": "r"}], "tasks": [{"name": "t"}]}
        with pytest.raises(ValueError, match="Missing required field"):
            self.service._process_crew_setup(setup, [], {})

    # --- Model assignment ---
    def test_model_assigned_to_agents_when_provided(self):
        setup = _minimal_setup()
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map(), model="gpt-4")
        for agent in result["agents"]:
            assert agent["llm"] == "gpt-4"

    def test_model_not_assigned_when_none(self):
        setup = _minimal_setup()
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map(), model=None)
        for agent in result["agents"]:
            assert "llm" not in agent

    def test_model_empty_string_not_assigned(self):
        """Empty string is falsy, so model should not be assigned."""
        setup = _minimal_setup()
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map(), model="")
        for agent in result["agents"]:
            assert "llm" not in agent

    # --- Agent tool filtering ---
    def test_agent_tools_filtered_to_allowed(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": ["ToolA", "Bad"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["agents"][0]["tools"] == ["id-a"]

    def test_agent_tool_id_not_in_map_keeps_name(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": ["ToolA"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), {})
        assert result["agents"][0]["tools"] == ["ToolA"]

    def test_agent_existing_id_removed(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "id": "old-id"}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert "id" not in result["agents"][0]

    def test_agent_non_list_tools_initialized(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": "invalid"}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["agents"][0]["tools"] == []

    def test_agent_no_tools_key_initialized(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b"}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["agents"][0]["tools"] == []

    def test_agent_tools_none_initialized(self):
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": None}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["agents"][0]["tools"] == []

    # --- Task tool filtering ---
    def test_task_tools_filtered_and_converted(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA", "Unknown"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
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
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["tools"] == []

    def test_task_no_tools_key_initialized(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A"}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["tools"] == []

    def test_task_existing_id_removed(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "id": "old-id", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert "id" not in result["tasks"][0]

    # --- Task context ---
    def test_task_context_non_empty_list_stored(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": ["dep1"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["_context_refs"] == ["dep1"]
        assert result["tasks"][0]["context"] == []

    def test_task_context_empty_list_clears_refs(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": [], "_context_refs": ["old"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert "_context_refs" not in result["tasks"][0]

    def test_task_context_none_no_refs(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": None}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert "_context_refs" not in result["tasks"][0]

    def test_task_context_invalid_type_no_refs_noop(self):
        """When context is not a list and _context_refs does not already exist, else branch is no-op."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": [], "context": "invalid"}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert "_context_refs" not in result["tasks"][0]

    # --- Task agent assignment ---
    def test_task_with_agent_field(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["agent"] == "A"
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_with_assigned_agent_fallback(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "assigned_agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["agent"] == "A"
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_no_agent_assignment(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        # No agent assigned, so task should not get agent/assigned_agent set
        assert result["tasks"][0].get("agent") is None

    def test_task_tools_removed_logged(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA", "NotAllowed"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["tools"] == ["id-a"]

    def test_agent_missing_name_uses_index(self):
        """Agent without 'name' falls back to Agent_{i} for logging, then fails validation on 'name'."""
        setup = {"agents": [{"role": "r", "goal": "g", "backstory": "b"}], "tasks": [{"name": "t"}]}
        with pytest.raises(ValueError, match="Missing required field 'name'"):
            self.service._process_crew_setup(setup, [], {})

    def test_model_assigned_to_all_agents(self):
        setup = _minimal_setup(
            agents=[
                {"name": "A1", "role": "r", "goal": "g", "backstory": "b"},
                {"name": "A2", "role": "r", "goal": "g", "backstory": "b"},
            ],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map(), model="some-model")
        assert result["agents"][0]["llm"] == "some-model"
        assert result["agents"][1]["llm"] == "some-model"

    def test_agent_tools_all_allowed(self):
        """When no tools are removed, the removed-tools log branch is skipped."""
        setup = _minimal_setup(
            agents=[{"name": "A", "role": "r", "goal": "g", "backstory": "b", "tools": ["ToolA"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["agents"][0]["tools"] == ["id-a"]

    def test_task_all_tools_allowed_no_removal_log(self):
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": ["ToolA"]}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["tools"] == ["id-a"]

    def test_task_with_assigned_agent_not_in_task_sets_assigned_agent(self):
        """First loop: task has 'agent' but not 'assigned_agent' -> assigned_agent is set."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        assert result["tasks"][0]["assigned_agent"] == "A"

    def test_task_with_both_agent_and_assigned_agent(self):
        """First loop: task has both 'agent' and 'assigned_agent' -> assigned_agent not overwritten."""
        setup = _minimal_setup(
            tasks=[{"name": "T", "agent": "A", "assigned_agent": "B", "tools": []}],
        )
        result = self.service._process_crew_setup(setup, _allowed_tools(), _tool_id_map())
        # The first loop sets task['agent'] = 'A' and doesn't overwrite assigned_agent since it exists
        # The final loop sets both to 'A' (because agent='A')
        assert result["tasks"][0]["agent"] == "A"


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

                result = await self.service._get_relevant_documentation("use genie tool")
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

                result = await self.service._get_relevant_documentation("make a reveal slide")
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

                result = await self.service._get_relevant_documentation("create a presentation")
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
        doc1 = Mock(); doc1.id = "same-id"; doc1.title = "Genie Stuff"; doc1.source = "docs/genie"; doc1.content = "c1"
        doc2 = Mock(); doc2.id = "same-id"; doc2.title = "Genie Stuff"; doc2.source = "docs/genie"; doc2.content = "c1"

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
        doc = Mock(); doc.id = "d1"; doc.title = "General"; doc.source = "docs/general"; doc.content = "body"

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
        doc = Mock(); doc.id = "d1"; doc.title = "Normal Doc"; doc.source = "docs/normal"; doc.content = "c"

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
        doc = Mock(); doc.id = "d1"; doc.title = "Normal Doc"; doc.source = "docs/normal"; doc.content = "c"

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

                result = await self.service._get_relevant_documentation("reveal presentation")
                assert "Normal Doc" in result
        finally:
            delattr(_mod, "DocumentationEmbeddingService")

    @pytest.mark.asyncio
    async def test_doc_source_none(self):
        doc = Mock(); doc.id = "d1"; doc.title = "No Source Doc"; doc.source = None; doc.content = "c"

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
            d = Mock(); d.id = f"d{i}"; d.title = f"Doc {i}"; d.source = "docs/src"; d.content = f"c{i}"
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

                result = await self.service._get_relevant_documentation("query", limit=3)
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
            tool.model_dump = Mock(return_value={"name": title, "description": f"desc-{title}", "id": id_val})
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
        result = await self.service._get_tool_details([{"name": "NoMatch", "id": "noid"}], ts)
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
        t.model_dump = Mock(return_value={"name": "DescFallback", "id": "700"})  # No 'description'
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
        result = await self.service._get_tool_details([{"name": "DictTool", "id": "did"}], ts)
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
        result = await self.service._get_tool_details(
            [{"name": "X"}], ts
        )
        assert "A tool named X" in result[0]["description"]

    @pytest.mark.asyncio
    async def test_dict_identifier_found_by_id_not_name(self):
        """Dict with id matching but name not matching uses id lookup."""
        t = self._make_tool_obj(title="RealName", id_val="300")
        ts = self._make_tool_service([t])
        result = await self.service._get_tool_details([{"name": "WrongName", "id": "300"}], ts)
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

        with patch("src.services.crew_generation_service.ToolService"), \
             patch.object(self.service, "_get_tool_details", new_callable=AsyncMock) as gtd, \
             patch.object(self.service, "_prepare_prompt_template", new_callable=AsyncMock) as ppt, \
             patch("src.services.crew_generation_service.LLMManager") as lm, \
             patch("src.services.crew_generation_service.robust_json_parser") as rjp, \
             patch.object(self.service, "_process_crew_setup") as pcs:

            gtd.return_value = [tool_detail]
            ppt.return_value = "system prompt"
            lm.completion = AsyncMock(return_value='{"agents":[],"tasks":[]}')
            rjp.return_value = {
                "agents": [{"name": "A1", "role": "r", "goal": "g", "backstory": "b", "tools": ["ToolA"]}],
                "tasks": [{"name": "T1", "description": "d", "agent": "A1", "tools": ["ToolA"]}],
            }
            pcs.return_value = {
                "agents": [{"name": "A1", "role": "r", "tools": ["id-a"]}],
                "tasks": [{"name": "T1", "agent": "A1", "tools": ["id-a"]}],
            }
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})

            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])
            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": Mock(MemoryBackendRepository=mock_mem_repo_cls),
                "src.models.memory_backend": Mock(MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")),
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert result == {"agents": [], "tasks": []}

    @pytest.mark.asyncio
    async def test_no_group_context_skips_filtering(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req, group_context=None)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_model_from_env_var(self):
        req = self._make_request(model=None)

        with _crew_complete_patches(self.service) as m, \
             patch.dict(os.environ, {"CREW_MODEL": "env-model"}):
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            await self.service.create_crew_complete(req)
            m["lm"].completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_model_default_fallback(self):
        req = self._make_request(model=None)

        with _crew_complete_patches(self.service) as m:
            os.environ.pop("CREW_MODEL", None)
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            await self.service.create_crew_complete(req)
            m["lm"].completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_llm_call_exception_raises_valueerror(self):
        req = self._make_request(model="m")

        with patch("src.services.crew_generation_service.ToolService"), \
             patch.object(self.service, "_get_tool_details", new_callable=AsyncMock) as gtd, \
             patch.object(self.service, "_prepare_prompt_template", new_callable=AsyncMock) as ppt, \
             patch("src.services.crew_generation_service.LLMManager") as lm:
            gtd.return_value = []
            ppt.return_value = "sys"
            lm.completion = AsyncMock(side_effect=RuntimeError("api error"))

            with pytest.raises(ValueError, match="Error generating crew"):
                await self.service.create_crew_complete(req)

    @pytest.mark.asyncio
    async def test_outer_exception_reraises(self):
        req = self._make_request()
        with patch("src.services.crew_generation_service.ToolService", side_effect=RuntimeError("outer")):
            with pytest.raises(RuntimeError, match="outer"):
                await self.service.create_crew_complete(req)

    @pytest.mark.asyncio
    async def test_task_with_agent_field_logging(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [{"name": "T", "agent": "A"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_with_assigned_agent_fallback(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [{"name": "T", "assigned_agent": "A"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_no_agent_warning(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [{"name": "T"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
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
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req)
            mock_agent.model_dump.assert_called_once()

    @pytest.mark.asyncio
    async def test_agent_as_dict(self):
        req = self._make_request(model="m")

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A", "role": "r"}], "tasks": [{"name": "T"}]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
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
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_task_as_pydantic_model(self):
        req = self._make_request(model="m")
        mock_task = Mock()
        mock_task.model_dump = Mock(return_value={"name": "T"})
        mock_task.get = Mock(side_effect=lambda k, d=None: {"name": "T", "agent": "A"}.get(k, d))

        with _crew_complete_patches(
            self.service,
            pcs_return={"agents": [{"name": "A"}], "tasks": [mock_task]},
        ) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
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
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_tool_filtering_with_backends_present(self):
        req = self._make_request(model="m", tools=["DatabricksKnowledgeSearchTool"])
        gc = Mock(); gc.primary_group_id = "grp1"
        dk_tool = {"name": "DatabricksKnowledgeSearchTool", "id": "dk-id", "title": "DatabricksKnowledgeSearchTool"}

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[Mock()])

            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": Mock(MemoryBackendRepository=mock_mem_repo_cls),
                "src.models.memory_backend": Mock(MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")),
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_tool_filtering_without_backends(self):
        req = self._make_request(model="m", tools=["DatabricksKnowledgeSearchTool"])
        gc = Mock(); gc.primary_group_id = "grp1"
        dk_tool = {"name": "DatabricksKnowledgeSearchTool", "id": "dk-id", "title": "DatabricksKnowledgeSearchTool"}

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": Mock(MemoryBackendRepository=mock_mem_repo_cls),
                "src.models.memory_backend": Mock(MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")),
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_tool_filtering_exception_warning(self):
        req = self._make_request(model="m")
        gc = Mock(); gc.primary_group_id = "grp1"

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            # Force the import inside the try block to fail
            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": None,
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_process_crew_setup_called_with_model(self):
        req = self._make_request(model="my-model")

        with _crew_complete_patches(self.service, model_params={"model": "my-model"}) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            await self.service.create_crew_complete(req)
            m["pcs"].assert_called_once()
            # Verify model= kwarg was passed
            assert m["pcs"].call_args.kwargs.get("model") == "my-model"

    @pytest.mark.asyncio
    async def test_databricks_filtering_with_title_field(self):
        req = self._make_request(model="m")
        gc = Mock(); gc.primary_group_id = "grp1"
        dk_tool = {"title": "DatabricksKnowledgeSearchTool", "id": "dk-id"}

        with _crew_complete_patches(self.service, gtd_return=[dk_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": Mock(MemoryBackendRepository=mock_mem_repo_cls),
                "src.models.memory_backend": Mock(MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")),
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result

    @pytest.mark.asyncio
    async def test_request_tools_none_handled(self):
        req = Mock()
        req.prompt = "build a crew"
        req.model = "m"
        req.tools = None

        with _crew_complete_patches(self.service) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
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
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            result = await self.service.create_crew_complete(req, group_context=gc)
            assert "agents" in result

    @pytest.mark.asyncio
    async def test_databricks_filtering_count_unchanged(self):
        """When before_count == after_count, the log message is not emitted."""
        req = self._make_request(model="m")
        gc = Mock(); gc.primary_group_id = "grp1"
        # Tool that is NOT DatabricksKnowledgeSearchTool
        normal_tool = {"name": "NormalTool", "id": "n-id"}

        with _crew_complete_patches(self.service, gtd_return=[normal_tool]) as m:
            self.crew_repo.create_crew_entities = AsyncMock(return_value={"agents": [], "tasks": []})
            mock_mem_repo_cls = Mock()
            mock_mem_repo_cls.return_value.get_by_type = AsyncMock(return_value=[])

            with patch.dict("sys.modules", {
                "src.repositories.memory_backend_repository": Mock(MemoryBackendRepository=mock_mem_repo_cls),
                "src.models.memory_backend": Mock(MemoryBackendTypeEnum=Mock(DATABRICKS="DATABRICKS")),
            }):
                result = await self.service.create_crew_complete(req, group_context=gc)
                assert "agents" in result
