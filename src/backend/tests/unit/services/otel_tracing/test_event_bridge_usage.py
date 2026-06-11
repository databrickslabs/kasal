"""Regression tests for LLM-013: the OTel event bridge must persist the
per-call token usage that CrewAI delivers on LLMCallCompletedEvent.usage.

Before the fix, the bridge read only ``event.total_tokens`` (which exists
solely on CrewKickoffCompletedEvent), so llm_response spans were persisted
with no token attributes and spend could not be attributed per
call/model/agent/task.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.otel_tracing.event_bridge import OTelEventBridge


def _bridge() -> OTelEventBridge:
    bridge = object.__new__(OTelEventBridge)
    bridge._current_crew_name = None
    return bridge


def _attributes_for(event) -> dict:
    span = MagicMock()
    _bridge()._set_extra_attributes(span, event)
    return {call.args[0]: call.args[1] for call in span.set_attribute.call_args_list}


def test_usage_dict_lands_on_span():
    event = SimpleNamespace(
        usage={"prompt_tokens": 4521, "completion_tokens": 238, "total_tokens": 4759},
    )

    attrs = _attributes_for(event)

    assert attrs["kasal.extra.prompt_tokens"] == 4521
    assert attrs["kasal.extra.completion_tokens"] == 238
    assert attrs["kasal.extra.total_tokens"] == 4759


def test_cached_prompt_tokens_included_when_present():
    event = SimpleNamespace(
        usage={"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110,
               "cached_prompt_tokens": 90},
    )

    attrs = _attributes_for(event)

    assert attrs["kasal.extra.cached_prompt_tokens"] == 90


def test_missing_usage_sets_no_token_attributes():
    attrs = _attributes_for(SimpleNamespace())

    assert not any(k.endswith("prompt_tokens") for k in attrs)


def test_none_usage_and_non_dict_usage_are_ignored():
    assert not any(
        k.endswith("_tokens") for k in _attributes_for(SimpleNamespace(usage=None))
    )
    assert not any(
        k.endswith("_tokens") for k in _attributes_for(SimpleNamespace(usage="oops"))
    )


def test_crew_aggregate_total_tokens_still_recorded():
    event = SimpleNamespace(total_tokens=12137808)

    attrs = _attributes_for(event)

    assert attrs["kasal.extra.total_tokens"] == 12137808
