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


# ===========================================================================
# Progressive / Streaming crew generation
# ===========================================================================

from src.core.exceptions import KasalError, BadRequestError
from src.schemas.task_generation import Agent as TaskGenAgent
from src.core.sse_manager import SSEEvent


class TestProgressiveGeneration:
    """Tests for create_crew_progressive() and its helper methods."""

    def setup_method(self):
        self.service, self.session, self.log_svc, self.crew_repo = _build_service()

    # ------------------------------------------------------------------
    # _generate_crew_plan
    # ------------------------------------------------------------------

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

        with patch("src.services.crew_generation_service.TemplateService") as ts, \
             patch("src.services.crew_generation_service.LLMManager") as lm, \
             patch("src.services.crew_generation_service.robust_json_parser") as rjp:
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

        with patch("src.services.crew_generation_service.TemplateService") as ts, \
             patch("src.services.crew_generation_service.LLMManager") as lm, \
             patch("src.services.crew_generation_service.robust_json_parser") as rjp:
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

        with patch("src.services.crew_generation_service.TemplateService") as ts, \
             patch("src.services.crew_generation_service.LLMManager") as lm, \
             patch("src.services.crew_generation_service.robust_json_parser") as rjp:
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
            {"name": "Researcher", "role": "Research", "goal": "Find data", "backstory": "Expert"},
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
            {"name": "data analyst", "role": "Analysis", "goal": "Analyze", "backstory": "Pro"},
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

        import sys
        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(return_value=mock_response)

        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(sys.modules, {"src.repositories.genie_repository": mock_genie_module}):
            result = await self.service._suggest_genie_space("sales analysis", "analyze sales")

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

        import sys
        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(
            side_effect=[empty_response, fallback_response]
        )
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(sys.modules, {"src.repositories.genie_repository": mock_genie_module}):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is not None
        assert result["id"] == "space-fallback"

    @pytest.mark.asyncio
    async def test_suggest_genie_space_no_spaces(self):
        """Returns None when no spaces exist at all."""
        empty_response = Mock()
        empty_response.spaces = []

        import sys
        mock_genie_repo = Mock()
        mock_genie_repo.get_spaces = AsyncMock(return_value=empty_response)
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(return_value=mock_genie_repo)

        with patch.dict(sys.modules, {"src.repositories.genie_repository": mock_genie_module}):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is None

    @pytest.mark.asyncio
    async def test_suggest_genie_space_exception(self):
        """Returns None on exception."""
        import sys
        mock_genie_module = Mock()
        mock_genie_module.GenieRepository = Mock(
            side_effect=RuntimeError("connection failed")
        )

        with patch.dict(sys.modules, {"src.repositories.genie_repository": mock_genie_module}):
            result = await self.service._suggest_genie_space("test", "desc")

        assert result is None

    # ------------------------------------------------------------------
    # create_crew_progressive -- integration-level with mocks
    # ------------------------------------------------------------------

    def _make_progressive_request(self, prompt="build a crew", model="test-model", tools=None):
        """Create a mock CrewStreamingRequest."""
        req = Mock()
        req.prompt = prompt
        req.model = model
        req.tools = tools or []
        req.original_prompt = None
        return req

    def _make_plan(self, agents=None, tasks=None, process_type="sequential", complexity="standard"):
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

    def _progressive_patches(self, plan=None, agent_gen_return=None, task_gen_response=None,
                             agent_saved=None, task_saved=None, tool_details=None):
        """Build a context manager with all patches needed for create_crew_progressive."""

        if plan is None:
            plan = self._make_plan()
        if agent_gen_return is None:
            agent_gen_return = {
                "name": "Agent1", "role": "Specialist",
                "goal": "Do work", "backstory": "Expert",
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
            task_saved = {"id": "task-id-1", "name": "Task1", "description": "Do something"}

        service = self.service

        class ProgressivePatchCtx:
            def __init__(self):
                self._patches = []
                self.mocks = {}

            def __enter__(self):
                # Patch sse_manager.broadcast_to_job
                p_sse = patch("src.services.crew_generation_service.sse_manager")
                # Patch async_session_factory at its source (the function
                # uses a local import: ``from src.db.session import async_session_factory``)
                p_session = patch("src.db.session.async_session_factory")
                # Patch is_lakebase_enabled so the code takes the local-DB path
                # (the function does: ``from src.db.database_router import is_lakebase_enabled``)
                p_lakebase = patch(
                    "src.db.database_router.is_lakebase_enabled",
                    new_callable=AsyncMock,
                    return_value=False,
                )
                # Patch the plan generation
                p_plan = patch.object(service, "_generate_crew_plan", new_callable=AsyncMock)
                # Patch AgentGenerationService
                p_agent_svc = patch("src.services.crew_generation_service.AgentGenerationService")
                # Patch TaskGenerationService
                p_task_svc = patch("src.services.crew_generation_service.TaskGenerationService")
                # Patch CrewGeneratorRepository
                p_repo = patch("src.services.crew_generation_service.CrewGeneratorRepository")
                # Patch ToolService
                p_tool_svc = patch("src.services.crew_generation_service.ToolService")
                # Patch _get_tool_details
                p_gtd = patch.object(service, "_get_tool_details", new_callable=AsyncMock)

                self._patches = [p_sse, p_session, p_lakebase, p_plan, p_agent_svc,
                                 p_task_svc, p_repo, p_tool_svc, p_gtd]
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
            assert event_types.index("task_detail") < event_types.index("generation_complete")

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
            fail_event = next(c for c in calls if c.args[1].data["type"] == "generation_failed")
            assert "LLM timeout" in fail_event.args[1].data["error"]

    @pytest.mark.asyncio
    async def test_create_crew_progressive_empty_plan(self):
        """Broadcasts generation_failed when plan has no agents."""
        request = self._make_progressive_request()
        gen_id = "gen-empty"

        empty_plan = {"agents": [], "tasks": [{"name": "T1"}], "process_type": "sequential"}

        with self._progressive_patches(plan=empty_plan) as m:
            await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "generation_failed" in event_types
            fail_event = next(c for c in calls if c.args[1].data["type"] == "generation_failed")
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
                {"name": "Task2", "assigned_agent": "Agent2"},
            ],
        )
        agent_saved_2 = {"id": "agent-id-2", "name": "Agent2", "role": "R2"}
        task_saved = {"id": "task-id-1", "name": "Task2", "description": "desc"}

        request = self._make_progressive_request(prompt="build a crew with 2 agents and 1 task")
        gen_id = "gen-agent-err"

        with self._progressive_patches(plan=plan, agent_saved=agent_saved_2, task_saved=task_saved) as m:
            # First agent generation fails, second succeeds
            call_count = [0]

            async def agent_gen_side_effect(**kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise RuntimeError("agent generation failed")
                return {
                    "name": "Agent2", "role": "R2",
                    "goal": "G2", "backstory": "B2",
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
            error_event = next(c for c in calls if c.args[1].data["type"] == "entity_error")
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

            error_event = next(c for c in calls if c.args[1].data["type"] == "entity_error")
            assert error_event.args[1].data["entity_type"] == "task"

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

        request = self._make_progressive_request(prompt="build a crew with 2 agents and 2 tasks")
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
            {"name": "Alpha", "role": "R1", "goal": "G1", "backstory": "B1", "advanced_config": {}},
            {"name": "Beta", "role": "R2", "goal": "G2", "backstory": "B2", "advanced_config": {}},
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
            interleaved = [e for e in event_types if e in ("agent_detail", "task_detail")]
            # Expected: agent_detail, task_detail, agent_detail, task_detail
            assert interleaved == ["agent_detail", "task_detail", "agent_detail", "task_detail"]

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
            {"title": "GenieTool", "name": "GenieTool", "description": "Genie", "id": genie_tool_id},
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

        task_saved = {"id": "tid-1", "name": "Query Data", "description": "Query the database"}

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
                self.service, "_create_tool_name_to_id_map",
                return_value={"GenieTool": genie_tool_id},
            ):
                # Also mock _suggest_genie_space
                with patch.object(
                    self.service, "_suggest_genie_space",
                    new_callable=AsyncMock,
                    return_value={"id": "space-1", "name": "Sales"},
                ):
                    await self.service.create_crew_progressive(request, None, gen_id)

            calls = m["sse"].broadcast_to_job.call_args_list
            event_types = [c.args[1].data["type"] for c in calls]
            assert "tool_config_needed" in event_types

            config_event = next(c for c in calls if c.args[1].data["type"] == "tool_config_needed")
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
