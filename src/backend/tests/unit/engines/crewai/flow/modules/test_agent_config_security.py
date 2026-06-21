"""Flow agents must get the same security hardening + template handling as crew
agents — both now go through the shared common builders (build_agent_kwargs +
inject_security_preamble). Regression tests for the flow/crew alignment."""
from types import SimpleNamespace

from src.engines.crewai.flow.modules.agent_adapter import AgentConfig
from src.engines.crewai.common.agent_builder import build_agent_kwargs
from src.engines.crewai.common.agent_security import (
    _build_security_preamble,
    inject_security_preamble,
)


def _flow_agent_kwargs(agent_data):
    """Reproduce what configure_agent_and_tools feeds the shared builder:
    ORM object → spec → build_agent_kwargs → inject_security_preamble."""
    spec = AgentConfig._agent_data_to_spec(agent_data)
    kwargs = build_agent_kwargs(spec, [], None, label=getattr(agent_data, "name", "?"))
    inject_security_preamble(kwargs)
    return kwargs


class TestFlowAgentSecurityPreamble:
    def test_preamble_prepended_to_backstory_when_no_system_template(self):
        agent_data = SimpleNamespace(role="R", goal="G", backstory="original backstory", name="A")
        kwargs = _flow_agent_kwargs(agent_data)

        assert kwargs["backstory"].startswith(_build_security_preamble())
        assert "original backstory" in kwargs["backstory"]
        # No custom system template was set, so none is added.
        assert "system_template" not in kwargs

    def test_preamble_prepended_to_system_template_with_correct_field_names(self):
        agent_data = SimpleNamespace(
            role="R", goal="G", backstory="bs", name="A", system_template="CUSTOM SYS"
        )
        kwargs = _flow_agent_kwargs(agent_data)

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
        kwargs = _flow_agent_kwargs(agent_data)

        assert kwargs.get("response_template") == "FMT"
        assert "format_prompt" not in kwargs  # the old dropped name
