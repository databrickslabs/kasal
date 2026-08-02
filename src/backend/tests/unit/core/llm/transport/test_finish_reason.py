"""Why the model stopped is reported, not judged.

``LLMCallCompletedEvent.finish_reason`` has existed since the event was vendored
from CrewAI and was never populated, so ``"length"`` — the endpoint stating that
the output allowance ran out mid-generation — reached nothing. A truncated
fragment was stored as the answer with no record anywhere that it was one, which
is the hole two rounds of bespoke machinery were built to plug: first a KMP
periodicity detector over the text, then an exception on ``length``. Neither is
what anyone else does.

CrewAI extracts the field and puts it on this same event
(``events/types/llm_events.py:98``) and never compares it to a value — verified
against the cloned source: ``grep 'finish_reason ==' -> 0 hits``. LangChain
returns it in ``response_metadata``. Deciding for the caller which finish reasons
are failures is a policy Kasal invented and nobody else has.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.events.types import LLMCallCompletedEvent
from src.core.llm.transport.completion import OpenAICompletion


def _response(content: str, finish_reason: str):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason=finish_reason,
                message=SimpleNamespace(content=content, tool_calls=None),
            )
        ],
        usage=None,
    )


def _llm():
    """`client` is a lazy property with no setter — set the backing attribute."""
    llm = OpenAICompletion(model="test-model")
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _completed_events(emitted):
    return [e for e in emitted if isinstance(e, LLMCallCompletedEvent)]


@pytest.fixture
def emitted():
    captured = []
    with patch(
        "src.core.llm.transport.base.event_bus.emit",
        side_effect=lambda source, event: captured.append(event),
    ):
        yield captured


class TestFinishReasonReachesTheEvent:
    def test_a_truncated_call_reports_length(self, emitted):
        """The signal the whole incident turned on: the endpoint said the output
        allowance ran out, and nothing recorded it."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(
            "half a s", "length"
        )

        llm.call("hi")

        assert _completed_events(emitted)[0].finish_reason == "length"

    def test_a_normal_call_reports_stop(self, emitted):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response("done", "stop")

        llm.call("hi")

        assert _completed_events(emitted)[0].finish_reason == "stop"

    def test_a_truncated_call_still_returns_its_text(self, emitted):
        """Reported, not raised. The caller decides what a fragment is worth —
        no framework makes that decision inside the transport."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(
            "fragment", "length"
        )

        assert llm.call("hi") == "fragment"

    def test_an_endpoint_that_reports_nothing_is_not_invented(self, emitted):
        """Absence is not truncation, and must not be coerced into a value."""
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response("done", None)

        llm.call("hi")

        assert _completed_events(emitted)[0].finish_reason is None


class TestNoBespokeMachineryRemains:
    def test_the_transport_exports_no_repetition_helpers(self):
        """Verified against the cloned sources of CrewAI, LangChain, LangGraph
        and LiteLLM: none of them contains any equivalent."""
        import src.core.llm.transport as transport

        exported = dir(transport)
        for name in (
            "looping_unit",
            "RepetitionWatch",
            "check_response",
            "stop_on_loop",
        ):
            assert name not in exported

    def test_no_truncation_exception_exists(self):
        import src.core.llm.transport.exceptions as exceptions

        assert not hasattr(exceptions, "LLMRepetitionLoopError")
        assert not hasattr(exceptions, "LLMOutputTruncatedError")
