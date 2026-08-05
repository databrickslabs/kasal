"""Reasoning models return ``content`` as typed BLOCKS, not a string.

Every caller in the transport assumed ``content`` was a ``str``. Six seeded
Databricks models do not agree, and the shapes were confirmed by probing the
live endpoints on 2026-08-05:

  1. a LIST with ``reasoning`` + ``text`` blocks — claude-fable-5, claude-opus-5
  2. a LIST with only ``text`` blocks — gemini-3-1-pro, gemini-3-1-flash-lite,
     gemini-3-5-flash, gemini-3-5-flash-lite
  3. a plain ``str`` plus a sibling ``reasoning_content`` field —
     databricks-inkling, kimi-k2-7-code

Two failures followed, and they had opposite symptoms. Streaming CRASHED: the
list reached ``LLMStreamChunkEvent(chunk=...)``, which declares ``chunk: str``,
so every run died with "1 validation error for LLMStreamChunkEvent / chunk /
Input should be a valid string". Non-streaming did NOT crash — it silently
returned the block list AS the answer, so the caller stored a base64
``signature`` (Anthropic) or ``thoughtSignature`` (Gemini) blob where the text
should be. The quiet one is the worse of the two.

Reasoning is split out rather than concatenated into the answer: it is the
model's private deliberation, and folding it in would put it into task output
and memory.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.events.types import LLMCallCompletedEvent, LLMReasoningChunkEvent
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.response_parsing import (
    REDACTED_REASONING,
    reasoning_was_redacted,
    split_content_blocks,
    split_message_content,
    text_content,
)

# ── The exact payloads the live endpoints returned ──────────────────────────

#: claude-fable-5, first streamed delta. `summary` is EMPTY — the opaque
#: `signature` carries the payload — so reasoning text is legitimately "".
FABLE_REASONING_DELTA = [
    {
        "type": "reasoning",
        "summary": [{"type": "summary_text", "text": "", "signature": "CAIS6wEK..."}],
    }
]

#: claude-fable-5 / claude-opus-5, non-streaming: reasoning THEN the answer.
FABLE_FULL = [
    {"type": "reasoning", "summary": [{"type": "summary_text", "text": "17*23=391"}]},
    {"type": "text", "text": "391"},
]

#: gemini-3-1-pro: a list, but text-only — no reasoning block at all.
GEMINI_TEXT_ONLY = [{"type": "text", "text": "391", "thoughtSignature": "AY89a190..."}]


class TestSplitContentBlocks:
    def test_a_plain_string_is_returned_unchanged(self):
        """The overwhelmingly common case must take an identity path."""
        assert split_content_blocks("hello") == ("hello", "")

    @pytest.mark.parametrize("empty", [None, ""])
    def test_absent_content_is_empty_not_an_error(self, empty):
        assert split_content_blocks(empty) == ("", "")

    def test_reasoning_and_text_are_separated(self):
        assert split_content_blocks(FABLE_FULL) == ("391", "17*23=391")

    def test_a_reasoning_only_delta_yields_no_answer_text(self):
        """Fable's first delta. Empty reasoning here is EXPECTED, not a failure."""
        assert split_content_blocks(FABLE_REASONING_DELTA) == ("", "")

    def test_a_text_only_list_is_still_unwrapped(self):
        """Gemini never sends reasoning, but it does send a list — which is why
        it hit the same bug despite having nothing to think about."""
        assert split_content_blocks(GEMINI_TEXT_ONLY) == ("391", "")

    def test_multiple_text_blocks_are_concatenated(self):
        blocks = [{"type": "text", "text": "39"}, {"type": "text", "text": "1"}]
        assert split_content_blocks(blocks) == ("391", "")

    def test_an_unknown_shape_is_stringified_rather_than_raised(self):
        """A weird answer beats a crashed run."""
        assert split_content_blocks(42) == ("42", "")

    def test_text_content_drops_the_reasoning(self):
        assert text_content(FABLE_FULL) == "391"


class TestSplitMessageContent:
    """All three conventions, through the one entry point the transport calls."""

    def test_sibling_reasoning_content_field_is_picked_up(self):
        """Convention 3 (inkling, kimi-k2-7-code): content is a normal string
        and the thinking sits beside it. This was dropped on the floor before."""
        message = SimpleNamespace(content="391", reasoning_content="17*23...")
        assert split_message_content(message) == ("391", "17*23...")

    def test_block_list_on_an_object_message(self):
        assert split_message_content(SimpleNamespace(content=FABLE_FULL)) == (
            "391",
            "17*23=391",
        )

    def test_a_dict_message_works_too(self):
        """Some OpenAI-compatible servers hand back plain dicts, not SDK objects."""
        assert split_message_content({"content": FABLE_FULL}) == ("391", "17*23=391")
        assert split_message_content({"content": "x", "reasoning_content": "why"}) == (
            "x",
            "why",
        )

    def test_a_message_with_neither_is_empty(self):
        assert split_message_content(SimpleNamespace(content=None)) == ("", "")


class TestRedactedReasoning:
    """ "The model did not reason" and "the provider hid the reasoning" are
    different facts, and the UI states them differently.

    Anthropic Claude on Databricks (fable-5, opus-5) returns a reasoning block
    whose summary is ``text: ""`` plus a ~456-char opaque ``signature``: thinking
    happened, encrypted. Reporting that as "no reasoning" made the UI imply these
    models don't think — and sent us hunting a UI bug that did not exist.
    """

    def test_an_empty_summary_with_a_signature_is_redacted(self):
        assert reasoning_was_redacted(FABLE_REASONING_DELTA) is True

    def test_real_summary_text_is_not_redacted(self):
        assert reasoning_was_redacted(FABLE_FULL) is False

    def test_no_reasoning_block_is_not_redacted(self):
        assert reasoning_was_redacted(GEMINI_TEXT_ONLY) is False
        assert reasoning_was_redacted("plain string") is False

    def test_the_sentinel_is_reported_instead_of_empty(self):
        """What the UI keys off to explain itself."""
        message = SimpleNamespace(
            content=[
                {
                    "type": "reasoning",
                    "summary": [
                        {"type": "summary_text", "text": "", "signature": "CAI"}
                    ],
                },
                {"type": "text", "text": "391"},
            ]
        )
        assert split_message_content(message) == ("391", REDACTED_REASONING)

    def test_real_reasoning_wins_over_the_sentinel(self):
        """A model that returns actual text must never be reported as redacted."""
        text, reasoning = split_message_content(SimpleNamespace(content=FABLE_FULL))
        assert reasoning == "17*23=391"
        assert reasoning != REDACTED_REASONING


# ── Through the transport ───────────────────────────────────────────────────


def _response(content, finish_reason="stop", reasoning_content=None):
    message = SimpleNamespace(content=content, tool_calls=None)
    if reasoning_content is not None:
        message.reasoning_content = reasoning_content
    return SimpleNamespace(
        choices=[SimpleNamespace(finish_reason=finish_reason, message=message)],
        usage=None,
    )


def _llm(**kw):
    """`client` is a lazy property with no setter — set the backing attribute."""
    llm = OpenAICompletion(model="test-model", **kw)
    object.__setattr__(llm, "_client", MagicMock())
    return llm


@pytest.fixture
def emitted():
    captured = []
    with (
        patch(
            "src.core.llm.transport.base.event_bus.emit",
            side_effect=lambda source, event: captured.append(event),
        ),
        patch(
            "src.core.llm.transport.completion.event_bus.emit",
            side_effect=lambda source, event: captured.append(event),
        ),
    ):
        yield captured


def _of(emitted, cls):
    return [e for e in emitted if isinstance(e, cls)]


class TestNonStreamingCall:
    def test_the_answer_is_a_string_not_the_block_list(self, emitted):
        """The quiet bug: this path never raised, it just returned the list — so
        a base64 signature blob was stored as the model's answer."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(FABLE_FULL)

        result = llm.call("hi")

        assert result == "391"
        assert isinstance(result, str)

    def test_gemini_text_only_list_is_unwrapped(self, emitted):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(GEMINI_TEXT_ONLY)

        assert llm.call("hi") == "391"

    def test_reasoning_reaches_the_completed_event(self, emitted):
        """Carried once per CALL, which is what the trace records."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(FABLE_FULL)

        llm.call("hi")

        assert _of(emitted, LLMCallCompletedEvent)[0].reasoning == "17*23=391"

    def test_reasoning_is_also_its_own_event(self, emitted):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(FABLE_FULL)

        llm.call("hi")

        assert [e.reasoning for e in _of(emitted, LLMReasoningChunkEvent)] == [
            "17*23=391"
        ]

    def test_sibling_reasoning_content_reaches_the_event(self, emitted):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(
            "391", reasoning_content="thinking"
        )

        llm.call("hi")

        assert _of(emitted, LLMCallCompletedEvent)[0].reasoning == "thinking"

    def test_a_model_without_reasoning_reports_none_not_empty_string(self, emitted):
        """Absence must not be invented into a value (same rule as
        finish_reason)."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response("plain")

        llm.call("hi")

        assert _of(emitted, LLMCallCompletedEvent)[0].reasoning is None
        assert _of(emitted, LLMReasoningChunkEvent) == []


def _chunk(content):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(delta=SimpleNamespace(content=content), finish_reason=None)
        ],
        usage=None,
    )


class TestStreamingCall:
    def test_a_reasoning_block_delta_does_not_crash(self, emitted):
        """THE reported failure: "1 validation error for LLMStreamChunkEvent /
        chunk / Input should be a valid string". Fable mixes a list delta into a
        stream of string deltas."""
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter(
            [_chunk(FABLE_REASONING_DELTA), _chunk("391"), _chunk("")]
        )

        assert llm.call("hi") == "391"

    def test_the_reasoning_block_stays_out_of_the_answer(self, emitted):
        """Accumulating it would put the model's thinking into the output."""
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter(
            [_chunk(FABLE_FULL), _chunk(" done")]
        )

        assert llm.call("hi") == "391 done"

    def test_chunk_events_carry_only_answer_text(self, emitted):
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter(
            [_chunk(FABLE_REASONING_DELTA), _chunk("391")]
        )

        llm.call("hi")

        from src.core.events.types import LLMStreamChunkEvent

        assert [e.chunk for e in _of(emitted, LLMStreamChunkEvent)] == ["391"]

    def test_streamed_reasoning_is_accumulated_onto_the_completed_event(self, emitted):
        """Per-delta reasoning events are too noisy for the trace, so the trace
        reads the accumulated value off the completed event instead."""
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter(
            [
                _chunk(
                    [
                        {
                            "type": "reasoning",
                            "summary": [{"type": "summary_text", "text": "step1 "}],
                        }
                    ]
                ),
                _chunk(
                    [
                        {
                            "type": "reasoning",
                            "summary": [{"type": "summary_text", "text": "step2"}],
                        }
                    ]
                ),
                _chunk("done"),
            ]
        )

        llm.call("hi")

        assert _of(emitted, LLMCallCompletedEvent)[0].reasoning == "step1 step2"

    def test_reasoning_does_not_leak_between_calls(self, emitted):
        """_reasoning_text is per-call state, like _finish_reason."""
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter([_chunk(FABLE_FULL)])
        llm.call("first")

        llm.client.chat.completions.create.return_value = iter([_chunk("plain")])
        llm.call("second")

        assert _of(emitted, LLMCallCompletedEvent)[-1].reasoning is None


class TestAnthropicExtendedThinking:
    """Claude takes a token BUDGET, not `reasoning_effort`.

    `ModelConfig.extended_thinking` was a dead column: stored, seeded, editable in
    the Edit Model dialog, and read by NOTHING when building an LLM — so the
    toggle silently did nothing. Wiring it revealed that Claude's thinking IS
    available after all, contradicting an earlier conclusion here that it was not:
    the first probe used `max_tokens` (3000) BELOW `budget_tokens` (10240) and the
    endpoint's 400 was misread as "unsupported".

    Verified live 2026-08-05 through this transport: sonnet-4-5 returns 1,600
    chars of reasoning with a budget and 0 without; haiku-4-5 returns 1,630.
    """

    def _params(self, model, budget=None, max_tokens=4000):
        # Constructed directly rather than via _llm(), which pins model=.
        llm = OpenAICompletion(
            model=model, thinking_budget_tokens=budget, max_tokens=max_tokens
        )
        object.__setattr__(llm, "_client", MagicMock())
        return llm._prepare_completion_params([{"role": "user", "content": "x"}], None)

    def test_no_budget_sends_nothing(self):
        """Off by default: thinking costs tokens and latency."""
        assert "extra_body" not in self._params("databricks-claude-sonnet-4-5")

    def test_claude_4x_gets_the_thinking_block(self):
        params = self._params(
            "databricks-claude-sonnet-4-5", budget=2000, max_tokens=16000
        )
        assert params["extra_body"]["thinking"] == {
            "type": "enabled",
            "budget_tokens": 2000,
            "display": "summarized",
        }

    def test_it_travels_in_extra_body_not_as_a_kwarg(self):
        """REGRESSION: as a top-level kwarg the OpenAI SDK raises
        "Completions.create() got an unexpected keyword argument 'thinking'"
        before any request is made. extra_body is the SDK's passthrough."""
        params = self._params(
            "databricks-claude-sonnet-4-5", budget=2000, max_tokens=16000
        )
        assert "thinking" not in params
        assert "thinking" in params["extra_body"]

    def test_max_tokens_is_raised_to_satisfy_the_endpoint(self):
        """The endpoint enforces `max_tokens > budget_tokens`. A budget above the
        configured cap is a fixable misconfiguration, not a reason to 400."""
        params = self._params(
            "databricks-claude-sonnet-4-5", budget=10240, max_tokens=4000
        )
        assert params["max_tokens"] > 10240

    def test_a_generous_cap_is_left_alone(self):
        params = self._params(
            "databricks-claude-sonnet-4-5", budget=2000, max_tokens=16000
        )
        assert params["max_tokens"] == 16000

    @pytest.mark.parametrize(
        "model",
        [
            # Non-Anthropic: `thinking` is not part of their surface at all.
            "databricks-gpt-5",
            "databricks-gemini-3-1-pro",
            "databricks-llama-4-maverick",
        ],
    )
    def test_non_anthropic_models_never_receive_a_thinking_block(self, model):
        assert "extra_body" not in self._params(model, budget=10240, max_tokens=16000)

    @pytest.mark.parametrize(
        "model",
        [
            "databricks-claude-opus-4-1",
            "databricks-claude-opus-4-5",
            "databricks-claude-opus-4-6",
            "databricks-claude-sonnet-4-5",
            "claude-sonnet-4-6",
            "databricks-claude-haiku-4-5",
            "databricks/claude-opus-4-5",
        ],
    )
    def test_manual_models_get_a_budget(self, model):
        params = self._params(model, budget=2000, max_tokens=16000)
        assert params["extra_body"]["thinking"] == {
            "type": "enabled",
            "budget_tokens": 2000,
            "display": "summarized",
        }

    @pytest.mark.parametrize(
        "model",
        [
            # Claude 4.7+ / 5 / Fable reject "enabled" and take no budget. They
            # are NOT "unsupported" — they need `adaptive`, and they DO return a
            # summary once `display` opts in (fable-5 255 chars, opus-5 1,629).
            "databricks-claude-opus-4-7",
            "databricks-claude-opus-4-8",
            "databricks-claude-opus-5",
            "databricks-claude-sonnet-5",
            "databricks-claude-fable-5",
        ],
    )
    def test_adaptive_models_get_adaptive_and_no_budget(self, model):
        thinking = self._params(model, budget=10240, max_tokens=16000)["extra_body"][
            "thinking"
        ]
        assert thinking == {"type": "adaptive", "display": "summarized"}
        assert "budget_tokens" not in thinking

    def test_display_summarized_is_always_requested(self):
        """REGRESSION — the single most costly omission in this whole area.

        `display` defaults to "omitted" on Claude 5/Fable/4.7/4.8, which returns
        thinking blocks with an EMPTY thinking field (only the encrypted
        signature). Without this key the response is indistinguishable from a
        provider that redacts its reasoning, and that is exactly what was
        concluded here — the UI shipped a message telling users Claude's thinking
        could not be shown at all. See
        platform.claude.com/docs/en/build-with-claude/thinking
        #controlling-thinking-display
        """
        for model in ("databricks-claude-sonnet-4-5", "databricks-claude-fable-5"):
            thinking = self._params(model, budget=2000, max_tokens=16000)["extra_body"][
                "thinking"
            ]
            assert thinking["display"] == "summarized", model
