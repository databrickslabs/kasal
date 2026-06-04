"""
Comprehensive unit tests for src/engines/crewai/config_adapter.py

Targets: get_execution_logger, adapt_config, normalize_config, normalize_flow_config
Goal: push coverage from 17.7% to 50%+
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Helpers to build minimal CrewConfig objects without touching the DB
# ---------------------------------------------------------------------------

def _make_crew_config(
    agents_yaml=None,
    tasks_yaml=None,
    inputs=None,
    planning=False,
    reasoning=False,
    model=None,
    llm_provider=None,
):
    """Return a minimal CrewConfig-like object."""
    from src.schemas.execution import CrewConfig

    return CrewConfig(
        agents_yaml=agents_yaml or {},
        tasks_yaml=tasks_yaml or {},
        inputs=inputs or {},
        planning=planning,
        reasoning=reasoning,
        model=model,
        llm_provider=llm_provider,
    )


# ---------------------------------------------------------------------------
# Patch target: extract_crew_yaml_data is called by adapt_config
# ---------------------------------------------------------------------------
EXTRACT_PATCH = "src.engines.crewai.config_adapter.extract_crew_yaml_data"
LOGGER_MANAGER_PATCH = "src.engines.crewai.config_adapter.LoggerManager"


# ============================================================================
# get_execution_logger
# ============================================================================

class TestGetExecutionLogger:
    """Tests for get_execution_logger."""

    def test_returns_crew_logger_when_no_config(self):
        from src.engines.crewai.config_adapter import get_execution_logger
        mock_mgr = MagicMock()
        with patch(LOGGER_MANAGER_PATCH) as mock_cls:
            mock_cls.get_instance.return_value = mock_mgr
            result = get_execution_logger(config=None)
        assert result is mock_mgr.crew

    def test_returns_crew_logger_when_empty_dict(self):
        from src.engines.crewai.config_adapter import get_execution_logger
        mock_mgr = MagicMock()
        with patch(LOGGER_MANAGER_PATCH) as mock_cls:
            mock_cls.get_instance.return_value = mock_mgr
            result = get_execution_logger(config={})
        assert result is mock_mgr.crew

    def test_returns_flow_logger_for_dynamic_flow_config(self):
        from src.engines.crewai.config_adapter import get_execution_logger
        mock_mgr = MagicMock()
        with patch(LOGGER_MANAGER_PATCH) as mock_cls:
            mock_cls.get_instance.return_value = mock_mgr
            config = {
                "flow_config": {},
                "nodes": [],
                "edges": [],
            }
            result = get_execution_logger(config=config)
        assert result is mock_mgr.flow

    def test_returns_crew_logger_when_flow_keys_missing(self):
        from src.engines.crewai.config_adapter import get_execution_logger
        mock_mgr = MagicMock()
        with patch(LOGGER_MANAGER_PATCH) as mock_cls:
            mock_cls.get_instance.return_value = mock_mgr
            config = {"agents": [], "tasks": []}
            result = get_execution_logger(config=config)
        assert result is mock_mgr.crew

    def test_returns_crew_logger_for_partial_flow_keys(self):
        """flow_config present but nodes/edges missing → crew."""
        from src.engines.crewai.config_adapter import get_execution_logger
        mock_mgr = MagicMock()
        with patch(LOGGER_MANAGER_PATCH) as mock_cls:
            mock_cls.get_instance.return_value = mock_mgr
            result = get_execution_logger(config={"flow_config": {}, "nodes": []})
        assert result is mock_mgr.crew


# ============================================================================
# adapt_config
# ============================================================================

class TestAdaptConfig:
    """Tests for adapt_config."""

    @patch(EXTRACT_PATCH, return_value=({"agent1": {}}, {"task1": {}}))
    def test_basic_adapt_config(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(model="gpt-4o")
        result = adapt_config(cfg)

        assert result["model"] == "gpt-4o"
        assert result["agents"] == {"agent1": {}}
        assert result["tasks"] == {"task1": {}}
        assert result["crew"]["verbose"] is True
        assert result["crew"]["process"] == "sequential"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_tools_extracted_from_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"tools": ["tool_a", "tool_b"]})
        result = adapt_config(cfg)

        assert result["tools"] == ["tool_a", "tool_b"]

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_no_tools_when_inputs_empty(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={})
        result = adapt_config(cfg)

        assert result["tools"] == []

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_no_tools_when_inputs_is_none(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs=None)
        result = adapt_config(cfg)

        assert result["tools"] == []

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_planning_true_sets_flag(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(planning=True)
        result = adapt_config(cfg)

        assert result["crew"]["planning"] is True

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_reasoning_true_sets_flag(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(reasoning=True)
        result = adapt_config(cfg)

        assert result["crew"]["reasoning"] is True

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_planning_llm_in_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"planning_llm": "gpt-4"}, planning=True)
        result = adapt_config(cfg)

        assert result["crew"]["planning_llm"] == "gpt-4"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_reasoning_llm_in_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"reasoning_llm": "gpt-4"}, reasoning=True)
        result = adapt_config(cfg)

        assert result["crew"]["reasoning_llm"] == "gpt-4"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_memory_backend_config_preserved(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        memory_cfg = {"backend": "chroma"}
        cfg = _make_crew_config(inputs={"memory_backend_config": memory_cfg})
        result = adapt_config(cfg)

        assert result["memory_backend_config"] == memory_cfg

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_hierarchical_process_with_manager_llm(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"process": "hierarchical", "manager_llm": "gpt-4"})
        result = adapt_config(cfg)

        assert result["crew"]["process"] == "hierarchical"
        assert result["crew"]["manager_llm"] == "gpt-4"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_hierarchical_process_with_manager_agent(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"process": "hierarchical", "manager_agent": {"role": "boss"}})
        result = adapt_config(cfg)

        assert result["crew"]["manager_agent"] == {"role": "boss"}

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_max_rpm_from_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs={"max_rpm": 20})
        result = adapt_config(cfg)

        assert result["max_rpm"] == 20

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_max_rpm_defaults_to_10_when_no_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(inputs=None)
        result = adapt_config(cfg)

        assert result["max_rpm"] == 10

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_model_defaults_to_gpt4o_when_none(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(model=None)
        result = adapt_config(cfg)

        assert result["model"] == "gpt-4o"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_original_config_preserved(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(model="my-model", llm_provider="openai")
        result = adapt_config(cfg)

        assert result["original_config"]["model"] == "my-model"
        assert result["original_config"]["llm_provider"] == "openai"

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_planning_llm_absent_when_not_in_inputs(self, mock_extract):
        """When planning is True but no planning_llm in inputs, no planning_llm key in crew."""
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(planning=True, inputs=None)
        result = adapt_config(cfg)

        assert "planning_llm" not in result["crew"]

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_reasoning_llm_absent_when_not_in_inputs(self, mock_extract):
        from src.engines.crewai.config_adapter import adapt_config

        cfg = _make_crew_config(reasoning=True, inputs=None)
        result = adapt_config(cfg)

        assert "reasoning_llm" not in result["crew"]


# ============================================================================
# normalize_config
# ============================================================================

class TestNormalizeConfig:
    """Tests for normalize_config."""

    @patch(EXTRACT_PATCH, return_value=({}, {}))
    def test_crew_config_object_calls_adapt_config(self, mock_extract):
        from src.engines.crewai.config_adapter import normalize_config

        cfg = _make_crew_config(model="gpt-4")
        result = normalize_config(cfg)

        # Should return a dict (the output of adapt_config)
        assert isinstance(result, dict)
        assert "agents" in result

    def test_dict_passthrough(self):
        from src.engines.crewai.config_adapter import normalize_config

        raw = {"agents": [], "tasks": [], "crew": {}}
        result = normalize_config(raw)

        assert result is raw

    def test_arbitrary_dict_passthrough(self):
        from src.engines.crewai.config_adapter import normalize_config

        raw = {"key": "value", "nested": {"a": 1}}
        result = normalize_config(raw)

        assert result == raw


# ============================================================================
# normalize_flow_config
# ============================================================================

class TestNormalizeFlowConfig:
    """Tests for normalize_flow_config."""

    def _minimal_traditional(self):
        return {
            "agents": [
                {
                    "name": "agent1",
                    "role": "analyst",
                    "goal": "analyse",
                    "backstory": "expert",
                }
            ],
            "tasks": [
                {
                    "name": "task1",
                    "description": "do something",
                    "agent": "agent1",
                    "expected_output": "a report",
                }
            ],
            "flow": {
                "type": "sequential",
                "tasks": ["task1"],
            },
        }

    def test_dynamic_flow_passes_through_unchanged(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = {
            "flow_config": {"id": "flow_1"},
            "nodes": [{"id": "n1"}],
            "edges": [{"source": "n1", "target": "n2"}],
            "extra_key": "preserved",
        }
        result = normalize_flow_config(config)

        assert result is config
        assert result["extra_key"] == "preserved"

    def test_traditional_flow_normalizes_agents(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        result = normalize_flow_config(config)

        assert len(result["agents"]) == 1
        agent = result["agents"][0]
        assert agent["role"] == "analyst"
        assert agent["tools"] == []
        assert agent["allow_delegation"] is False
        assert agent["verbose"] is True

    def test_traditional_flow_normalizes_tasks(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        result = normalize_flow_config(config)

        assert len(result["tasks"]) == 1
        task = result["tasks"][0]
        assert task["name"] == "task1"
        assert task["async_execution"] is False
        assert task["markdown"] is False

    def test_traditional_flow_normalizes_flow_section(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        result = normalize_flow_config(config)

        assert result["flow"]["type"] == "sequential"
        assert result["flow"]["max_iterations"] == 10
        assert result["flow"]["timeout"] == 3600

    def test_traditional_flow_missing_section_raises(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = {"agents": [], "tasks": []}  # missing 'flow'
        with pytest.raises(ValueError, match="Missing required section 'flow'"):
            normalize_flow_config(config)

    def test_traditional_flow_missing_agents_raises(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = {"tasks": [], "flow": {}}  # missing 'agents'
        with pytest.raises(ValueError, match="Missing required section 'agents'"):
            normalize_flow_config(config)

    def test_traditional_flow_missing_tasks_raises(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = {"agents": [], "flow": {}}  # missing 'tasks'
        with pytest.raises(ValueError, match="Missing required section 'tasks'"):
            normalize_flow_config(config)

    def test_extra_keys_copied_to_normalized(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        config["custom_key"] = "custom_value"
        result = normalize_flow_config(config)

        assert result["custom_key"] == "custom_value"

    def test_agent_tool_configs_preserved(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        config["agents"][0]["tool_configs"] = {"MyTool": {"key": "val"}}
        result = normalize_flow_config(config)

        assert result["agents"][0]["tool_configs"] == {"MyTool": {"key": "val"}}

    def test_task_tool_configs_preserved(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        config["tasks"][0]["tool_configs"] = {"MyTool": {"k": "v"}}
        result = normalize_flow_config(config)

        assert result["tasks"][0]["tool_configs"] == {"MyTool": {"k": "v"}}

    def test_flow_defaults_applied(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = self._minimal_traditional()
        # Flow section has no optional keys
        result = normalize_flow_config(config)

        assert result["flow"]["parallel_tasks"] == []
        assert result["flow"]["conditional_tasks"] == {}
        assert result["flow"]["error_handling"] == {}

    def test_dynamic_flow_with_empty_nodes_and_edges(self):
        from src.engines.crewai.config_adapter import normalize_flow_config

        config = {"flow_config": {}, "nodes": [], "edges": []}
        result = normalize_flow_config(config)
        assert result is config
