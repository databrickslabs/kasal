"""Why the model stopped has to reach somebody.

The transport populates ``LLMCallCompletedEvent.finish_reason`` and judges
nothing — deliberately, and locked in by
``tests/unit/core/llm/transport/test_finish_reason.py``: no repetition
detector, no truncation exception, because deciding for the caller which
finish reasons are failures is a policy no other framework has.

That left the field arriving on the event and going no further. ``"length"``
— the endpoint stating the output allowance ran out mid-generation — reached
nothing at all, so execution 74be1413 stored an answer cut off mid-URL, marked
it COMPLETED, and recorded nowhere that it was a fragment.

Reporting is this layer's job. The bridge is the sole trace subscriber, so it
is the one place every path (chat, crew, flow) passes through.
"""

import logging
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


class TestFinishReasonReachesTheTrace:
    def test_length_lands_on_the_span(self):
        """The exporter captures every kasal.extra.* key, so this is what makes
        a truncated answer visible as truncated in the UI."""
        attrs = _attributes_for(SimpleNamespace(finish_reason="length"))

        assert attrs["kasal.extra.finish_reason"] == "length"

    def test_a_normal_stop_is_recorded_too(self):
        """Not only failures: 'stop' present is what makes 'length' absent
        meaningful rather than ambiguous."""
        attrs = _attributes_for(SimpleNamespace(finish_reason="stop"))

        assert attrs["kasal.extra.finish_reason"] == "stop"

    def test_an_endpoint_reporting_nothing_adds_no_attribute(self):
        """Absence is not truncation and must not be invented downstream."""
        attrs = _attributes_for(SimpleNamespace(finish_reason=None))

        assert "kasal.extra.finish_reason" not in attrs

    def test_an_event_without_the_field_is_not_an_error(self):
        """_set_extra_attributes runs for ~35 event types; most have no such
        field."""
        attrs = _attributes_for(SimpleNamespace())

        assert "kasal.extra.finish_reason" not in attrs


class TestTruncationIsAnnounced:
    def test_length_warns_with_the_detail_needed_to_act(self, caplog):
        with caplog.at_level(
            logging.WARNING, logger="src.services.otel_tracing.event_bridge"
        ):
            _attributes_for(
                SimpleNamespace(
                    finish_reason="length",
                    usage={"completion_tokens": 4096},
                    model="Qwen3-Coder-30B-A3B-Instruct",
                )
            )

        assert "TRUNCATED" in caplog.text
        # The token count and the model are what turn the line into an action:
        # 4096 against this model's max_output_tokens says the cap was hit
        # exactly, not approached.
        assert "4096" in caplog.text
        assert "Qwen3-Coder-30B-A3B-Instruct" in caplog.text

    def test_a_clean_stop_says_nothing(self, caplog):
        """A warning on every successful call is a warning nobody reads."""
        with caplog.at_level(
            logging.WARNING, logger="src.services.otel_tracing.event_bridge"
        ):
            _attributes_for(SimpleNamespace(finish_reason="stop", usage={}))

        assert "TRUNCATED" not in caplog.text

    def test_missing_usage_still_warns(self, caplog):
        """The truncation is the news; the token count is a detail. An event
        with no usage dict must not swallow the warning."""
        with caplog.at_level(
            logging.WARNING, logger="src.services.otel_tracing.event_bridge"
        ):
            _attributes_for(SimpleNamespace(finish_reason="length", model="m"))

        assert "TRUNCATED" in caplog.text

    def test_nothing_is_raised(self):
        """Reported, not raised — same contract the transport keeps. A
        truncated answer is sometimes exactly what the caller wanted."""
        _attributes_for(SimpleNamespace(finish_reason="length", usage={}))
