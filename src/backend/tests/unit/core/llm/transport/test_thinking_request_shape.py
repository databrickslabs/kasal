"""What the transport actually ASKS for when it wants a model's thinking.

Anthropic returns thinking text only if the request opts in. ``display`` defaults
to ``"omitted"`` on Claude 5, Fable 5, Opus 4.7 and Opus 4.8, which returns a
thinking block with an EMPTY ``thinking`` field and a signature — indistinguishable
from a provider that withholds the trace unless you look at what was sent.

Two shapes, and mixing them up is a 400:

* MANUAL (Claude 4.1–4.6) — ``thinking: {"type": "enabled", "budget_tokens": N}``.
  The budget IS the depth, and ``max_tokens`` must exceed it.
* ADAPTIVE (Claude 4.7+, 5, Fable) — ``thinking: {"type": "adaptive"}``. A budget
  is REJECTED; depth is ``output_config: {"effort": ...}``, a SIBLING of
  ``thinking``, not a key inside it.

This file exists because ``_thinking_for`` had no tests, and two bugs lived in it:

1. It returned early unless ``thinking_budget_tokens`` was set — but that is the
   one field an adaptive model rejects, so its real knob (``thinking_effort``)
   was a silent no-op and NOTHING was sent.
2. No model config seeds a thinking default, so with (1) every adaptive Claude
   sent no ``thinking`` at all and the UI showed a redaction placeholder for a
   summary that was there for the asking.

Probed live 2026-08-05 (fable-5, opus-5): with no ``thinking`` field the summary
is ``text: ""`` + signature; with ``{"type":"adaptive","display":"summarized"}``
the same prompt returns real text. The model reasons either way — ``display``
only decides whether we can see work already being billed.
"""

import pytest

from src.core.llm.transport.completion import OpenAICompletion

ADAPTIVE = "databricks-claude-fable-5"
MANUAL = "databricks-claude-sonnet-4-5"
NOT_ANTHROPIC = "databricks-gpt-5"


def _shape(model: str, **kwargs):
    """``(thinking, params)`` for a model + config, from a fresh max_tokens."""
    llm = OpenAICompletion(model=model, **kwargs)
    params = {"max_tokens": 2000}
    return llm._thinking_for(params), params


class TestAdaptiveModels:
    def test_asks_for_the_summary_with_nothing_configured(self):
        """THE bug the user hit: an unconfigured adaptive Claude sent no thinking.

        It reasons regardless, so not asking buys nothing and costs the summary.
        """
        thinking, _ = _shape(ADAPTIVE)
        assert thinking == {"type": "adaptive", "display": "summarized"}

    def test_effort_alone_is_enough_to_configure_depth(self):
        """`thinking_effort` is the adaptive knob; a budget is rejected here."""
        thinking, params = _shape(ADAPTIVE, thinking_effort="high")
        assert thinking == {"type": "adaptive", "display": "summarized"}
        # A SIBLING of `thinking`, not nested inside it.
        assert params["extra_body"] == {"output_config": {"effort": "high"}}

    def test_a_budget_never_reaches_an_adaptive_model(self):
        """The endpoint rejects a budget on these; enable thinking, drop the number."""
        thinking, params = _shape(ADAPTIVE, thinking_budget_tokens=4096)
        assert thinking == {"type": "adaptive", "display": "summarized"}
        assert "budget_tokens" not in thinking
        # And it must not silently inflate max_tokens for a budget it never sends.
        assert params["max_tokens"] == 2000

    def test_an_effort_this_model_rejects_is_dropped_not_sent(self):
        """Scales differ per model; a value valid on one Anthropic model 400s another."""
        thinking, params = _shape(ADAPTIVE, thinking_effort="ludicrous")
        assert thinking == {"type": "adaptive", "display": "summarized"}
        assert params.get("extra_body") is None

    def test_effort_is_normalised(self):
        thinking, params = _shape(ADAPTIVE, thinking_effort="  HIGH  ")
        assert thinking is not None
        assert params["extra_body"] == {"output_config": {"effort": "high"}}


class TestManualModels:
    def test_a_budget_produces_the_enabled_shape(self):
        thinking, _ = _shape(MANUAL, thinking_budget_tokens=4096)
        assert thinking == {
            "type": "enabled",
            "budget_tokens": 4096,
            "display": "summarized",
        }

    def test_max_tokens_is_raised_to_clear_the_budget(self):
        """The endpoint enforces max_tokens > budget_tokens."""
        _, params = _shape(MANUAL, thinking_budget_tokens=4096)
        assert params["max_tokens"] > 4096

    def test_effort_alone_sends_nothing(self):
        """Here the budget IS the depth; effort is not part of this model's surface.

        Enabling thinking without a budget would be a 400, so stay out entirely.
        """
        thinking, params = _shape(MANUAL, thinking_effort="high")
        assert thinking is None
        assert params.get("extra_body") is None

    def test_no_config_means_no_thinking(self):
        """Unlike adaptive: here `thinking` genuinely enables a billed feature."""
        assert _shape(MANUAL)[0] is None


class TestNonAnthropicModels:
    @pytest.mark.parametrize(
        "kwargs", [{}, {"thinking_effort": "high"}, {"thinking_budget_tokens": 4096}]
    )
    def test_thinking_is_never_part_of_their_surface(self, kwargs):
        """`thinking` is Anthropic-specific — sending it elsewhere is an error."""
        thinking, params = _shape(NOT_ANTHROPIC, **kwargs)
        assert thinking is None
        assert params.get("extra_body") is None


class TestRedactionSentinelIsAFlagNotText:
    """``REDACTED_REASONING`` must never accumulate.

    Every delta of an encrypted stream reports it, and the accumulator appended
    each one, so the UI showed
    ``__kasal_reasoning_redacted____kasal_reasoning_redacted__…`` verbatim — the
    frontend tests the value for EQUALITY, so a repeat matched nothing and leaked.
    """

    def _llm(self):
        return OpenAICompletion(model=ADAPTIVE)

    def test_repeated_placeholders_collapse_to_one(self):
        from src.core.llm.transport.response_parsing import REDACTED_REASONING

        llm = self._llm()
        for _ in range(6):
            llm._add_reasoning(REDACTED_REASONING)
        assert llm._reasoning_text == REDACTED_REASONING

    def test_real_text_supersedes_the_placeholder(self):
        """A stream can report redacted early and produce a summary later."""
        from src.core.llm.transport.response_parsing import REDACTED_REASONING

        llm = self._llm()
        llm._add_reasoning(REDACTED_REASONING)
        llm._add_reasoning("first ")
        llm._add_reasoning("second")
        assert llm._reasoning_text == "first second"

    def test_real_text_still_accumulates_normally(self):
        llm = self._llm()
        for piece in ("a", "b", "c"):
            llm._add_reasoning(piece)
        assert llm._reasoning_text == "abc"

    def test_the_placeholder_never_appends_to_real_text(self):
        from src.core.llm.transport.response_parsing import REDACTED_REASONING

        llm = self._llm()
        llm._add_reasoning("actual reasoning")
        llm._add_reasoning(REDACTED_REASONING)
        assert llm._reasoning_text == "actual reasoning"
