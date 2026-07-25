"""Context compaction must be visible, and must not fire against the wrong budget.

Two defects this guards:

1. ``_trim_conversation_to_window`` replaced the oldest tool results with a stub
   and emitted nothing — no log, no event, no span. An agent that silently lost
   the schema it had just read would re-query it and loop until the round budget
   ran out ("Tool-calling did not converge within N rounds"), with nothing in the
   trace explaining why.

2. An UNREGISTERED model silently gets DEFAULT_CONTEXT_WINDOW_SIZE (8192 → 6963
   after the 0.85 derate), which for a self-hosted model can be 4x too small, so
   the trim shreds context the agent still needs. ``src.core.llm_manager``
   registers every configured model at import and covers the common path, but a
   direct engine embedding — or a model set on an agent but absent from
   MODEL_CONFIGS — still lands on the 8192 default. The table stays authoritative
   whenever it KNOWS the model: an agent may claim a window the provider cannot
   honour, and trimming too late is a hard failure rather than a degraded one.
"""
import pytest

from kasal_engine.events.bus import crewai_event_bus
from kasal_engine.events.types import ContextCompactionEvent
from kasal_engine.llm.completion import OpenAICompletion


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

    @crewai_event_bus.on(ContextCompactionEvent)
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
    import src.core.llm_manager  # noqa: F401 — registers MODEL_CONFIGS windows

    llm = _llm("Qwen3-Coder-30B-A3B-Instruct")
    assert llm._model_window_is_known()
    assert llm._effective_context_window(_Agent(999_999)) == llm.get_context_window_size()


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
    both registrations rather than trusting the emit alone."""
    from src.services.otel_tracing.db_exporter import SPAN_NAME_MAP
    from src.services.otel_tracing.event_bridge import _EVENT_SPAN_MAP

    span_name, event_type = _EVENT_SPAN_MAP["ContextCompactionEvent"]
    assert event_type == "context_compaction"
    assert SPAN_NAME_MAP[span_name] == "context_compaction"
