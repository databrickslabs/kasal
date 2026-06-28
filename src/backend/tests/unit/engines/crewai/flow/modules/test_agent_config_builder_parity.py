"""Flow agent build now routes through the SAME shared builders the crew uses
(build_agent_kwargs + build_agent_llm). These tests prove the flow adapter maps
fields correctly and that flow builds its LLM the crew way (explicit group_id +
temperature via configure_crewai_llm), so the two paths can't diverge."""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.engines.crewai.paths.flow.modules.agent_adapter import AgentConfig
from src.engines.crewai.kernel.agent_builder import build_agent_kwargs


def _agent(**over):
    base = dict(role="R", goal="G", backstory="B", name="A")
    base.update(over)
    return SimpleNamespace(**base)


class TestAgentDataToSpec:
    def test_required_fields_and_defaults(self):
        spec = AgentConfig._agent_data_to_spec(_agent())
        assert spec["role"] == "R" and spec["goal"] == "G" and spec["backstory"] == "B"
        assert spec["allow_delegation"] is False
        # unset optional fields are absent → builder applies crew defaults
        assert "verbose" not in spec and "max_iter" not in spec

    def test_optional_fields_included_when_set(self):
        spec = AgentConfig._agent_data_to_spec(
            _agent(verbose=False, cache=True, max_retry_limit=5, max_iter=9, system_template="SYS")
        )
        assert spec["verbose"] is False
        assert spec["cache"] is True
        assert spec["max_retry_limit"] == 5
        assert spec["max_iter"] == 9
        assert spec["system_template"] == "SYS"


class TestFlowCrewAgentKwargsParity:
    def test_flow_spec_and_crew_dict_produce_identical_shared_kwargs(self):
        # Flow ORM object → spec; crew dict with the same values.
        agent_data = _agent(verbose=False, max_iter=4, allow_delegation=True)
        flow_spec = AgentConfig._agent_data_to_spec(agent_data)
        crew_dict = {
            "role": "R", "goal": "G", "backstory": "B",
            "verbose": False, "max_iter": 4, "allow_delegation": True,
        }
        flow_kwargs = build_agent_kwargs(flow_spec, [], "llm")
        crew_kwargs = build_agent_kwargs(crew_dict, [], "llm")
        assert flow_kwargs == crew_kwargs
        # And flow now honors verbose=False (was previously hardcoded True)
        assert flow_kwargs["verbose"] is False
        assert flow_kwargs["allow_code_execution"] is False  # security parity


class TestAgentDataToSpecLlm:
    """Flow's spec adapter feeds the LLM info to the shared builder so flow builds
    its LLM the crew way (configure_crewai_llm via build_agent_llm — covered in
    tests/unit/engines/crewai/common/test_agent_builder.py)."""

    def test_explicit_llm_and_temperature_in_spec(self):
        spec = AgentConfig._agent_data_to_spec(_agent(llm="my-model", temperature=40))
        assert spec["llm"] == "my-model"
        assert spec["temperature"] == 40

    def test_falls_back_to_model_when_no_llm(self):
        spec = AgentConfig._agent_data_to_spec(_agent(model="databricks-foo"))
        assert spec["llm"] == "databricks-foo"

    def test_no_llm_no_model_omits_llm(self):
        # No llm key → the shared builder applies its default_model
        # (databricks-llama-4-maverick for flow).
        spec = AgentConfig._agent_data_to_spec(_agent())
        assert "llm" not in spec
