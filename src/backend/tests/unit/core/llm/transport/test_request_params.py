"""What the app sets on an LLM must actually reach the request.

``BaseLLM`` is declared ``extra="allow"``, so a constructor kwarg the class does
not model was accepted, stored on the object, and then read by nothing: the
payload is built from the declared fields plus ``additional_params``, and an
extra was in neither. Every such kwarg was silently dropped.

The live casualty was the Databricks partner telemetry.
``LLMManager.configure_kasal_llm`` sets ``extra_headers`` with the Kasal
User-Agent that ``src/backend/CLAUDE.md`` requires on every Databricks API call
for usage tracking. It was set, it was stored, it was never sent, and nothing
anywhere failed — the only way to notice was to inspect the outgoing body.

Modelled on CrewAI's ``BaseLLM._validate_init_fields`` (base_llm.py:279-286),
which collects unknown kwargs into ``additional_params`` for exactly this
reason: one escape hatch that reaches the payload, rather than an attribute that
looks set and does nothing.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.llm.transport import LLM


def _llm(**kwargs):
    llm = LLM(model="databricks/some-model", **kwargs)
    client = MagicMock()
    client.chat.completions.create.return_value = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content="ok", tool_calls=None),
            )
        ],
        usage=None,
    )
    object.__setattr__(llm, "_client", client)
    return llm


USER_AGENT = {"User-Agent": "kasal-agent/1.0"}


class TestTelemetryHeaderReachesTheWire:
    def test_extra_headers_are_sent(self):
        """The regression that mattered: partner attribution on every
        Databricks call, mandated by backend/CLAUDE.md."""
        llm = _llm(extra_headers=USER_AGENT)

        llm.call("hi")

        sent = llm.client.chat.completions.create.call_args.kwargs
        assert sent["extra_headers"] == USER_AGENT

    def test_it_survives_alongside_declared_fields(self):
        """A declared field and an unknown kwarg in the same constructor call —
        the shape LLMManager actually builds."""
        llm = _llm(temperature=0.7, extra_headers=USER_AGENT)

        llm.call("hi")

        sent = llm.client.chat.completions.create.call_args.kwargs
        assert sent["temperature"] == 0.7
        assert sent["extra_headers"] == USER_AGENT


class TestUnknownKwargsBecomeRequestParams:
    def test_an_unknown_kwarg_is_collected_not_swallowed(self):
        llm = LLM(model="m", some_provider_knob=7)

        assert llm.additional_params["some_provider_knob"] == 7

    def test_extra_body_reaches_the_request(self):
        """How a vLLM-only sampler (repetition_penalty, top_k, min_p) travels:
        the OpenAI SDK strips unknown TOP-LEVEL kwargs client-side, so the
        server sees them only inside extra_body."""
        llm = _llm(extra_body={"repetition_penalty": 1.05, "top_k": 20})

        llm.call("hi")

        sent = llm.client.chat.completions.create.call_args.kwargs
        assert sent["extra_body"] == {"repetition_penalty": 1.05, "top_k": 20}

    def test_an_explicit_additional_params_still_works(self):
        llm = LLM(model="m", additional_params={"a": 1}, b=2)

        assert llm.additional_params == {"a": 1, "b": 2}

    def test_a_declared_field_is_not_collected(self):
        """Only the UNMODELLED ones. Declared fields keep their typing,
        validation and precedence."""
        llm = LLM(model="m", temperature=0.5)

        assert llm.temperature == 0.5
        assert "temperature" not in llm.additional_params

    def test_a_declared_field_wins_over_a_collected_one(self):
        """_prepare_completion_params applies declared fields AFTER merging
        additional_params, so collecting can only ever ADD a parameter."""
        llm = _llm(temperature=0.9, additional_params={"temperature": 0.1})

        llm.call("hi")

        assert llm.client.chat.completions.create.call_args.kwargs["temperature"] == 0.9


class TestNothingIsSentThatWasNotAskedFor:
    @pytest.mark.parametrize(
        "absent", ["top_p", "frequency_penalty", "presence_penalty", "stop"]
    )
    def test_an_unset_sampling_param_stays_off_the_wire(self, absent):
        """Unset means absent — the invariant every framework shares. A default
        that silently applied would change behaviour for every existing model."""
        llm = _llm()

        llm.call("hi")

        assert absent not in llm.client.chat.completions.create.call_args.kwargs
