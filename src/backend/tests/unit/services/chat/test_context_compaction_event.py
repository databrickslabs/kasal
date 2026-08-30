"""Context compaction must be visible, and must not fire against the wrong budget.

Two defects this guards:

1. ``_trim_conversation_to_window`` replaced the oldest tool results with a stub
   and emitted nothing — no log, no event, no span. An agent that silently lost
   the schema it had just read would re-query it and loop until the round budget
   ran out ("Tool-calling did not converge within N rounds"), with nothing in the
   trace explaining why.

2. An UNREGISTERED model silently gets DEFAULT_CONTEXT_WINDOW_SIZE (8192 → 6963
   after the 0.85 derate), which for a self-hosted model can be 4x too small, so
   the trim shreds context the agent still needs. ``src.services.llm.manager``
   registers every configured model at import and covers the common path, but a
   direct engine embedding — or a model set on an agent but absent from
   MODEL_CONFIGS — still lands on the 8192 default. The table stays authoritative
   whenever it KNOWS the model: an agent may claim a window the provider cannot
   honour, and trimming too late is a hard failure rather than a degraded one.
"""

import pytest

from src.core.events.bus import event_bus
from src.core.events.types import ContextCompactionEvent
from src.core.llm.transport.completion import OpenAICompletion


class _Agent:
    def __init__(self, window=None, respect=True):
        self.max_context_window_size = window
        self.respect_context_window = respect


# NOTE: importing ``src`` anywhere in the test session runs llm_manager's
# registration, which adds every MODEL_CONFIGS entry (including the vllm-hosted
# Qwen) to LLM_CONTEXT_WINDOW_SIZES. So a test for the UNKNOWN-model path must
# use a name that is genuinely absent, not merely absent from the seed file.
UNKNOWN_MODEL = "some-unregistered-selfhosted-model-v9"


def _llm(model=UNKNOWN_MODEL):
    return OpenAICompletion(model=model, api_key="x")


def _conversation(tool_messages=10, chars=4000):
    return [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "q"},
    ] + [
        {"role": "tool", "tool_call_id": f"t{i}", "content": "R" * chars}
        for i in range(tool_messages)
    ]


def _stub_count(conversation):
    return sum(
        1
        for m in conversation
        if str(m.get("content", "")).startswith("[earlier tool result")
    )


@pytest.fixture
def captured_events():
    events = []

    @event_bus.on(ContextCompactionEvent)
    def _capture(source, event):  # noqa: ARG001
        events.append(event)

    return events


def test_unknown_model_falls_back_to_the_agents_configured_window():
    llm = _llm()
    assert not llm._model_window_is_known()
    # Without an agent hint it is still the hardcoded default.
    assert llm._effective_context_window(_Agent(None)) == llm.get_context_window_size()
    # With one, the agent's number is the better estimate than 8192.
    assert llm._effective_context_window(_Agent(28672)) == int(28672 * 0.85)
    assert llm._effective_context_window(_Agent(28672)) > llm.get_context_window_size()


def test_a_known_model_keeps_the_table_window_even_if_the_agent_claims_more():
    """The table wins when it knows the model — honouring an over-claimed window
    would trim too late and turn a degraded run into a hard provider failure."""
    import src.services.llm.manager  # noqa: F401 — registers MODEL_CONFIGS windows

    llm = _llm("Qwen3-Coder-30B-A3B-Instruct")
    assert llm._model_window_is_known()
    assert (
        llm._effective_context_window(_Agent(999_999)) == llm.get_context_window_size()
    )


def test_configured_window_stops_the_needless_shredding():
    """~10k tokens of tool results blows the 8192 default but fits a 28672
    window — the difference is whole tool results destroyed."""
    llm = _llm()

    with_default = _conversation()
    llm._trim_conversation_to_window(with_default, _Agent(None))

    with_configured = _conversation()
    llm._trim_conversation_to_window(with_configured, _Agent(28672))

    assert _stub_count(with_default) > 0, "the default window still trims (unchanged)"
    assert _stub_count(with_configured) == 0, "a correctly-sized window drops nothing"


def test_compaction_emits_an_event_with_the_numbers(captured_events):
    llm = _llm()
    conversation = _conversation()

    llm._trim_conversation_to_window(conversation, _Agent(None))

    assert len(captured_events) == 1
    event = captured_events[0]
    assert event.type == "context_compaction"
    assert event.strategy == "tool_result_stub"
    assert event.model == UNKNOWN_MODEL
    assert event.tokens_before > event.tokens_after
    assert event.tokens_after <= event.window
    assert event.messages_compacted == _stub_count(conversation)
    assert "tool result" in (event.reason or "")


def test_no_event_when_nothing_is_dropped(captured_events):
    """A quiet run stays quiet — the row only appears when context was lost."""
    llm = _llm()
    conversation = _conversation(tool_messages=1, chars=10)

    llm._trim_conversation_to_window(conversation, _Agent(None))

    assert captured_events == []
    assert _stub_count(conversation) == 0


def test_respect_context_window_false_disables_compaction(captured_events):
    llm = _llm()
    conversation = _conversation()

    llm._trim_conversation_to_window(conversation, _Agent(None, respect=False))

    assert _stub_count(conversation) == 0
    assert captured_events == []


def test_event_is_registered_all_the_way_to_a_trace_row():
    """The bus event is worthless if the bridge or exporter drops it, so assert
    every registration rather than trusting the emit alone.

    The SUBSCRIPTION is the one that was missing. Mapping the event in
    _EVENT_SPAN_MAP looks like wiring it up, but the bridge only ever sees what
    it subscribes to in register(), and ContextCompactionEvent was absent from
    that list — so compaction emitted faithfully onto the bus and produced not a
    single trace row. The database had zero of them."""

    from src.services.otel_tracing.db_exporter import SPAN_NAME_MAP
    from src.services.otel_tracing.event_bridge import (
        _EVENT_CLASSES,
        _EVENT_SPAN_MAP,
    )

    span_name, event_type = _EVENT_SPAN_MAP["ContextCompactionEvent"]
    assert event_type == "context_compaction"
    assert SPAN_NAME_MAP[span_name] == "context_compaction"
    # Checked against the _EVENT_CLASSES data, not against the TEXT of
    # register()'s source. The source-scan version broke the moment the list was
    # hoisted out of the method into a module constant — it was asserting on
    # where the code happened to live, not on what it does.
    assert "ContextCompactionEvent" in {
        name for _module, name in _EVENT_CLASSES
    }, "mapped but not subscribed: the bridge never receives it"


# ---------------------------------------------------------------------------
# The budget the trim compares against, and the estimate it uses
# ---------------------------------------------------------------------------
#
# Two vLLM rejections in production, both EXACTLY one token over a 28,672
# window:
#
#   "You passed 20481 input tokens and requested 8192 output tokens"
#   "You passed 27734 input tokens and requested 939 output tokens"
#
# Neither was compacted first. Compaction compared against 0.85 x window
# (24,371) while the server would only accept window - max_tokens as a prompt,
# so a conversation between those numbers was too big to serve and too small to
# compact: every attempt was rejected, the agent retried at the same size, and
# the run looped until it failed.


def _sized_llm(model="gpt-4o", window=28672, max_tokens=8192):
    """An LLM whose window is known to the table, with an output reservation."""
    from src.core.llm.transport.constants import LLM_CONTEXT_WINDOW_SIZES

    LLM_CONTEXT_WINDOW_SIZES[model] = window
    return OpenAICompletion(model=model, api_key="x", max_tokens=max_tokens)


def test_input_budget_leaves_room_for_the_output_request():
    """What the server will accept as a prompt is window - max_tokens, not
    0.85 x window."""
    from src.core.llm.transport.context_window import _WINDOW_SAFETY_TOKENS

    llm = _sized_llm(window=28672, max_tokens=8192)

    budget = llm._input_budget()

    # Against the RAW window, which round-trips through the 0.85 derate and can
    # land a token low — conservative, which is the safe direction here.
    assert budget == llm._raw_context_window() - 8192 - _WINDOW_SAFETY_TOKENS
    assert budget < int(28672 * 0.85), "the old threshold was above what fits"


def test_input_budget_falls_back_when_no_output_is_reserved():
    """Nothing to subtract — the derated window stays the sane default."""
    llm = OpenAICompletion(model="gpt-4o", api_key="x")
    assert llm._input_budget() == llm._effective_context_window()


def test_the_prompt_that_was_rejected_is_now_compacted_first():
    """~20.5k tokens against a 28,672 window with 8,192 reserved: the exact
    shape of the first rejection."""
    llm = _sized_llm(window=28672, max_tokens=8192)
    conversation = _conversation(tool_messages=12, chars=5800)

    assert llm._estimate_tokens(conversation) > llm._input_budget()
    llm._trim_conversation_to_window(conversation)

    assert _stub_count(conversation) > 0, "must compact rather than overflow"
    assert llm._estimate_tokens(conversation) <= llm._input_budget()


def test_the_estimate_errs_high_rather_than_low():
    """chars/4 counted ~15% under what vLLM charged for tool JSON, and an
    under-estimate is a rejected request while an over-estimate only compacts
    slightly early."""
    llm = _sized_llm()
    text = "R" * 34000
    estimate = llm._estimate_tokens([{"role": "tool", "content": text}])

    assert estimate > 34000 // 4


def test_the_clamp_keeps_a_reserve_so_equality_cannot_tip_over():
    """Both failures were one token over: the maths was right up to the chat
    template's own scaffolding, which nothing counts client-side."""
    from src.core.llm.transport.context_window import _WINDOW_SAFETY_TOKENS

    llm = _sized_llm(window=28672, max_tokens=8192)
    messages = [{"role": "user", "content": "R" * 60000}]
    params = {"messages": messages, "max_tokens": 8192}

    llm._clamp_output_budget(params)

    estimate = llm._estimate_tokens(messages)
    assert estimate + params["max_tokens"] + _WINDOW_SAFETY_TOKENS <= 28672
