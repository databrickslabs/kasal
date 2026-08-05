"""Refused parameters are stripped before the request is built.

``resolve_llm_params`` has taken an ``unsupported`` filter since it was written,
and it was fed only ``ModelConfig.unsupported_params`` — a column that was NULL
for all 63 seeded models. So the filter ran on every call and removed nothing:
present, plumbed, and inert.

The consequence was not theoretical. `temperature` went to claude-opus-5 and came
back "Model global.anthropic.claude-opus-5 does not support the temperature
parameter" — a 400 that killed the run, on a parameter the catalogue seeds for
every model. There is no drop_params safety net on this path, so what is set IS
sent.

Hiding the control in the UI is not enough on its own: an agent saved before the
gating existed, or one whose model was swapped afterwards, still carries the
value. The filter is the layer that has to catch those, which is why the measured
registry is merged into it here rather than only consulted by the frontend.
"""

import pytest

from src.services.llm.manager import _refused_params


class TestMeasuredRefusalsReachTheFilter:
    def test_adaptive_claude_refuses_the_sampling_knobs(self):
        """The exact case that 400'd in production."""
        refused = _refused_params(
            {"key": "databricks-claude-opus-5"}, "databricks-claude-opus-5"
        )
        assert "temperature" in refused
        assert {"top_p", "frequency_penalty", "presence_penalty"} <= set(refused)

    def test_manual_claude_keeps_temperature(self):
        """Not a family-wide rule: sonnet-4-5 accepts temperature and top_p while
        rejecting the penalties. Over-filtering would silently change sampling for
        a model that was working."""
        refused = _refused_params(
            {"key": "databricks-claude-sonnet-4-5"}, "databricks-claude-sonnet-4-5"
        )
        assert "temperature" not in refused
        assert "top_p" not in refused
        assert "frequency_penalty" in refused

    def test_gpt5_refuses_stop_too(self):
        refused = _refused_params({"key": "databricks-gpt-5"}, "databricks-gpt-5")
        assert "stop" in refused

    @pytest.mark.parametrize(
        "model", ["databricks-gemini-3-1-pro", "databricks-llama-4-maverick"]
    )
    def test_a_model_that_refuses_nothing_filters_nothing(self, model):
        """Unset stays unset: these models send exactly what they sent before the
        registry existed."""
        assert _refused_params({"key": model}, model) == []


class TestDeclaredAndMeasuredAreUnioned:
    def test_a_declaration_alone_still_works(self):
        """The escape hatch for an endpoint nobody has measured."""
        assert _refused_params(
            {"key": "custom-model", "unsupported_params": ["top_k"]}, "custom-model"
        ) == ["top_k"]

    def test_declared_adds_to_measured(self):
        refused = _refused_params(
            {"key": "databricks-claude-opus-5", "unsupported_params": ["top_k"]},
            "databricks-claude-opus-5",
        )
        assert "top_k" in refused  # declared
        assert "temperature" in refused  # measured

    def test_a_declaration_cannot_cancel_a_measured_refusal(self):
        """Union, deliberately. A hand-written list can only ever ADD to what we
        measured — otherwise an out-of-date row could re-enable a parameter the
        endpoint is known to reject, and the 400 would come back."""
        refused = _refused_params(
            {"key": "databricks-claude-opus-5", "unsupported_params": []},
            "databricks-claude-opus-5",
        )
        assert "temperature" in refused

    def test_no_duplicates(self):
        refused = _refused_params(
            {"key": "databricks-gpt-5", "unsupported_params": ["temperature"]},
            "databricks-gpt-5",
        )
        assert len(refused) == len(set(refused))


class TestNameResolution:
    def test_the_served_name_is_preferred(self):
        """The catalogue key is a Kasal alias and can differ from the model the
        endpoint actually runs."""
        refused = _refused_params(
            {"key": "some-internal-alias"}, "global.anthropic.claude-opus-5"
        )
        assert "temperature" in refused

    def test_the_key_is_the_fallback(self):
        """When the served name carries no recognisable model (an AI-Gateway
        `system.ai.*` name, say), the key still identifies it."""
        refused = _refused_params(
            {"key": "databricks-claude-opus-5"}, "system.ai.something-opaque"
        )
        assert "temperature" in refused
