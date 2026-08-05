"""Per-model capability, and why it cannot be inferred.

Every value asserted here was MEASURED against a live Databricks workspace on
2026-08-05 by sending an invalid value and reading the enum the endpoint named in
its own error ("Supported values are: ...", "expected one of ..."). That method
matters: provider docs describe the DIRECT API, Databricks serving lags and
diverges, and the two disagreed. OpenAI's docs give ONE reasoning_effort scale;
the served models report FOUR, split per model. Trusting the docs would have
shipped a UI offering values that 400.

The rule these tests defend: capability is per MODEL, never per family. A regex
over a model name looks right and is wrong in both directions —
`claude-(opus|sonnet|haiku)-4-\\d` would send `thinking: {"type": "enabled"}` to
opus-4-7/4-8, which reject it, and a single effort list would send "minimal" to
gpt-5-1, which rejects that.
"""

import pytest

from src.core.llm.model_capabilities import (
    ReasoningStyle,
    accepts_param,
    allowed_efforts,
    model_capability,
    reasoning_style,
    refused_params,
)


class TestReasoningStyle:
    @pytest.mark.parametrize(
        "model,expected",
        [
            # Anthropic MANUAL: a token budget.
            ("databricks-claude-opus-4-1", ReasoningStyle.TOKEN_BUDGET),
            ("databricks-claude-opus-4-5", ReasoningStyle.TOKEN_BUDGET),
            ("databricks-claude-opus-4-6", ReasoningStyle.TOKEN_BUDGET),
            ("databricks-claude-sonnet-4-5", ReasoningStyle.TOKEN_BUDGET),
            ("databricks-claude-haiku-4-5", ReasoningStyle.TOKEN_BUDGET),
            # Anthropic ADAPTIVE. opus-4-7/4-8 land HERE despite the "4.x"
            # version — the split does not follow the version number, which is
            # exactly why a regex cannot express it.
            ("databricks-claude-opus-4-7", ReasoningStyle.ADAPTIVE_EFFORT),
            ("databricks-claude-opus-4-8", ReasoningStyle.ADAPTIVE_EFFORT),
            ("databricks-claude-opus-5", ReasoningStyle.ADAPTIVE_EFFORT),
            ("databricks-claude-fable-5", ReasoningStyle.ADAPTIVE_EFFORT),
            # reasoning_effort, no thinking block.
            ("databricks-gpt-5", ReasoningStyle.REASONING_EFFORT),
            ("databricks-gemini-3-1-pro", ReasoningStyle.REASONING_EFFORT),
            # Thinking arrives with nothing requested.
            ("databricks-inkling", ReasoningStyle.UNPROMPTED),
            ("databricks-kimi-k2-7-code", ReasoningStyle.UNPROMPTED),
        ],
    )
    def test_style(self, model, expected):
        assert reasoning_style(model) is expected

    @pytest.mark.parametrize(
        "model",
        [
            None,
            "",
            "databricks-llama-4-maverick",
            "databricks-gemma-3-12b",
            # 2.5 exposes nothing; only the 3.x line takes reasoning_effort.
            "databricks-gemini-2-5-flash",
        ],
    )
    def test_models_with_no_reasoning_surface(self, model):
        """None is the safe answer: the caller sends nothing and the model behaves
        exactly as it did before this registry existed."""
        assert reasoning_style(model) is None
        assert allowed_efforts(model) == ()


class TestAllowedEfforts:
    """FIVE distinct scales. Any single list is wrong for most of the catalogue."""

    @pytest.mark.parametrize(
        "model,expected",
        [
            # Endpoint: "expected one of `low`, `medium`, `high`, `xhigh`, `max`".
            ("databricks-claude-opus-5", ("low", "medium", "high", "xhigh", "max")),
            ("databricks-claude-fable-5", ("low", "medium", "high", "xhigh", "max")),
            # "Supported values are: 'minimal', 'low', 'medium', and 'high'."
            ("databricks-gpt-5", ("minimal", "low", "medium", "high")),
            ("databricks-gpt-5-mini", ("minimal", "low", "medium", "high")),
            ("databricks-gpt-5-nano", ("minimal", "low", "medium", "high")),
            # gpt-5-1 swaps "minimal" for "none" — same family, different enum.
            ("databricks-gpt-5-1", ("none", "low", "medium", "high")),
            # The 5-2/5-4/5-6 line adds "xhigh".
            ("databricks-gpt-5-2", ("none", "low", "medium", "high", "xhigh")),
            ("databricks-gpt-5-4-mini", ("none", "low", "medium", "high", "xhigh")),
            ("databricks-gpt-5-6-sol", ("none", "low", "medium", "high", "xhigh")),
            # Gemini rejects none/minimal/xhigh/max.
            ("databricks-gemini-3-1-pro", ("low", "medium", "high")),
        ],
    )
    def test_scale(self, model, expected):
        assert allowed_efforts(model) == expected

    def test_manual_models_have_no_effort_scale(self):
        """Their depth is the budget; an effort would be an unknown parameter."""
        assert allowed_efforts("databricks-claude-sonnet-4-5") == ()

    def test_the_scales_really_do_differ(self):
        """Guards the whole design: if these ever collapse to one list, the
        per-model lookup was pointless and someone has flattened it."""
        scales = {
            allowed_efforts(m)
            for m in (
                "databricks-claude-opus-5",
                "databricks-gpt-5",
                "databricks-gpt-5-1",
                "databricks-gpt-5-2",
                "databricks-gemini-3-1-pro",
            )
        }
        assert len(scales) == 5

    def test_supports_effort_is_case_and_space_tolerant(self):
        capability = model_capability("databricks-claude-opus-5")
        assert capability is not None
        assert capability.supports_effort(" HIGH ") is True
        assert capability.supports_effort("minimal") is False
        assert capability.supports_effort(None) is False


class TestRefusedParams:
    """Ordinary sampling knobs are refused per model too — the reason the Edit
    Model dialog offered `temperature` on a model that answers it with a 400."""

    def test_adaptive_claude_refuses_all_four(self):
        assert set(refused_params("databricks-claude-opus-5")) == {
            "temperature",
            "top_p",
            "frequency_penalty",
            "presence_penalty",
        }

    def test_manual_claude_accepts_temperature_but_not_penalties(self):
        """The case that proves refusals are not inferable from the family: same
        provider, same generation prefix, different answer."""
        assert accepts_param("databricks-claude-sonnet-4-5", "temperature") is True
        assert accepts_param("databricks-claude-sonnet-4-5", "top_p") is True
        assert (
            accepts_param("databricks-claude-sonnet-4-5", "frequency_penalty") is False
        )

    def test_gpt5_also_refuses_stop(self):
        assert "stop" in refused_params("databricks-gpt-5")

    @pytest.mark.parametrize(
        "model", ["databricks-gemini-3-1-pro", "databricks-llama-4-maverick", None]
    )
    def test_models_that_refuse_nothing(self, model):
        assert refused_params(model) == ()
        assert accepts_param(model, "temperature") is True


class TestNameMatching:
    @pytest.mark.parametrize(
        "model",
        [
            "databricks-claude-opus-5",  # Kasal key
            "global.anthropic.claude-opus-5",  # served name
            "openai/claude-opus-5",  # provider-prefixed
            "CLAUDE-OPUS-5",  # case
        ],
    )
    def test_the_shapes_a_model_name_arrives_in(self, model):
        assert reasoning_style(model) is ReasoningStyle.ADAPTIVE_EFFORT

    def test_specific_wins_over_generic(self):
        """ "gpt-5-1" contains "gpt-5". Matching the shorter fragment first would
        hand gpt-5-1 an enum that rejects its own values."""
        assert allowed_efforts("databricks-gpt-5-1") != allowed_efforts(
            "databricks-gpt-5"
        )
        assert "none" in allowed_efforts("databricks-gpt-5-1")
        assert "none" not in allowed_efforts("databricks-gpt-5")


class TestEveryEntryIsSourced:
    def test_no_entry_ships_without_evidence(self):
        """These are claims about someone else's API. An unsourced one is a guess,
        and guesses here were wrong twice before the registry existed."""
        for model in (
            "databricks-claude-opus-5",
            "databricks-claude-sonnet-4-5",
            "databricks-gpt-5",
            "databricks-gemini-3-1-pro",
            "databricks-inkling",
        ):
            capability = model_capability(model)
            assert capability is not None
            assert capability.source, model
            assert capability.evidence in ("measured", "documented"), model
            assert capability.note, model

    def test_gpt5_is_marked_as_not_returning_text(self):
        """It reasons and bills for it; the trace is simply not retrievable over
        chat completions. Recording that stops it reading as a bug."""
        capability = model_capability("databricks-gpt-5")
        assert capability is not None
        assert capability.returns_text is False
