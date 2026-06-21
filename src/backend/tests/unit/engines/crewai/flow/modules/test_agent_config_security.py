"""Flow agents must get the same security hardening + template handling as crew
agents (agent_helpers.py). Regression tests for the flow/crew alignment fix."""
from types import SimpleNamespace

from src.engines.crewai.flow.modules.agent_config import AgentConfig
from src.engines.crewai.helpers.agent_helpers import _build_security_preamble


class TestFlowAgentSecurityPreamble:
    def test_preamble_prepended_to_backstory_when_no_system_template(self):
        agent_data = SimpleNamespace(role="R", goal="G", backstory="original backstory", name="A")
        kwargs = AgentConfig._prepare_agent_kwargs(agent_data, tools=[], llm=None)

        assert kwargs["backstory"].startswith(_build_security_preamble())
        assert "original backstory" in kwargs["backstory"]
        # No custom system template was set, so none is added.
        assert "system_template" not in kwargs

    def test_preamble_prepended_to_system_template_with_correct_field_names(self):
        agent_data = SimpleNamespace(
            role="R", goal="G", backstory="bs", name="A", system_template="CUSTOM SYS"
        )
        kwargs = AgentConfig._prepare_agent_kwargs(agent_data, tools=[], llm=None)

        # Uses CrewAI's real field name, NOT the dropped 'system_prompt'.
        assert "system_prompt" not in kwargs
        assert kwargs["system_template"].startswith(_build_security_preamble())
        assert "CUSTOM SYS" in kwargs["system_template"]
        # A passthrough user template is supplied so CrewAI honors the custom one.
        assert kwargs["prompt_template"] == "{{ .Prompt }}"

    def test_response_template_uses_correct_field_name(self):
        agent_data = SimpleNamespace(
            role="R", goal="G", backstory="bs", name="A", response_template="FMT"
        )
        kwargs = AgentConfig._prepare_agent_kwargs(agent_data, tools=[], llm=None)

        assert kwargs.get("response_template") == "FMT"
        assert "format_prompt" not in kwargs  # the old dropped name
