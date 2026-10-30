"""The model that actually answered, when a router picked it.

OpenRouter's Jev Router hands each request to a model of its choosing and names
it in the response's top-level ``model`` field. The transport reads that field
(non-streaming, streaming, Responses API) and reports it on
``LLMCallCompletedEvent.served_model`` — only when it names a DIFFERENT model,
so a provider echoing a dated id adds no noise.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.events.types import LLMCallCompletedEvent
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.served_model import served_model_if_different


class TestTheDiffersRule:
    @pytest.mark.parametrize(
        "requested, served",
        [
            ("claude-sonnet-4-5", "claude-sonnet-4-5-20250929"),
            ("anthropic/claude-sonnet-4.5", "anthropic/claude-sonnet-4-5"),
            ("databricks-claude-sonnet-4-5", "claude-sonnet-4-5-20250929"),
            ("gpt-4o", "GPT-4o-2024-08-06"),
            ("openai/gpt-4o", "gpt-4o"),
            ("meta-llama/llama-3-70b:free", "meta-llama/llama-3-70b"),
        ],
    )
    def test_the_same_model_decorated_is_not_reported(self, requested, served):
        assert served_model_if_different(requested, served) is None

    def test_a_router_pick_is_reported_verbatim(self):
        assert (
            served_model_if_different(
                "typesafe/jev-router", "anthropic/claude-opus-5-5"
            )
            == "anthropic/claude-opus-5-5"
        )

    @pytest.mark.parametrize("served", [None, "", "   "])
    def test_nothing_served_is_nothing_reported(self, served):
        assert served_model_if_different("jev-router", served) is None


def _llm(model="typesafe/jev-router", stream=False):
    llm = OpenAICompletion(model=model, stream=stream)
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _response(model):
    return SimpleNamespace(
        model=model,
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content="hello", tool_calls=None),
            )
        ],
        usage=None,
    )


def _chunk(text, model, finish_reason=None):
    delta = SimpleNamespace(content=text, tool_calls=None)
    return SimpleNamespace(
        model=model,
        usage=None,
        choices=[SimpleNamespace(delta=delta, finish_reason=finish_reason)],
    )


@pytest.fixture
def completed():
    captured = []
    with patch(
        "src.core.llm.transport.base.event_bus.emit",
        side_effect=lambda source, event: captured.append(event),
    ):
        yield lambda: [e for e in captured if isinstance(e, LLMCallCompletedEvent)]


class TestTheTransportReadsIt:
    def test_non_streaming(self, completed):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(
            "anthropic/claude-opus-5-5"
        )

        assert llm.call("hi") == "hello"

        assert completed()[0].served_model == "anthropic/claude-opus-5-5"
        assert llm.served_model == "anthropic/claude-opus-5-5"

    def test_streaming(self, completed):
        llm = _llm(stream=True)
        llm.client.chat.completions.create.return_value = iter(
            [
                _chunk("hel", "anthropic/claude-opus-5-5"),
                _chunk("lo", "anthropic/claude-opus-5-5", finish_reason="stop"),
            ]
        )

        assert llm.call("hi") == "hello"

        assert completed()[0].served_model == "anthropic/claude-opus-5-5"

    def test_responses_api(self, completed):
        llm = _llm()
        llm.api = "responses"
        llm.client.responses.create.return_value = SimpleNamespace(
            id="r1",
            model="openai/gpt-5",
            output=[],
            output_text="hello",
            usage=None,
        )

        assert llm.call("hi") == "hello"

        assert completed()[0].served_model == "openai/gpt-5"

    def test_a_provider_echoing_the_requested_model_reports_nothing(self, completed):
        llm = _llm(model="claude-sonnet-4-5")
        llm.client.chat.completions.create.return_value = _response(
            "claude-sonnet-4-5-20250929"
        )

        llm.call("hi")

        assert completed()[0].served_model is None

    def test_a_response_without_the_field_reports_nothing(self, completed):
        """MagicMock responses (and endpoints that omit it) must not leak a
        non-string into the event."""
        llm = _llm()
        response = MagicMock()
        response.choices[0].message = SimpleNamespace(content="x", tool_calls=None)
        response.choices[0].finish_reason = "stop"
        response.usage = None
        llm.client.chat.completions.create.return_value = response

        llm.call("hi")

        assert completed()[0].served_model is None

    def test_a_previous_calls_pick_does_not_leak_into_the_next(self, completed):
        llm = _llm()
        llm.client.chat.completions.create.return_value = _response(
            "anthropic/claude-opus-5-5"
        )
        llm.call("hi")
        llm.client.chat.completions.create.return_value = _response(None)

        llm.call("again")

        assert completed()[1].served_model is None
