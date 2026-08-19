"""The CrewAI LLM adapter forwards; it does not reimplement.

The property under test throughout: **a CrewAI run and a Kasal run make the same
request**. Every assertion here is really one assertion — that this class adds
nothing to the request path — because the moment it does, "did switching the
harness change the answer?" stops having an answer.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses.crewai.llm import build_kasal_backed_llm


@pytest.fixture
def inner():
    """A transport LLM shaped like what ``configure_kasal_llm`` returns."""
    return LLM(
        model="databricks/my-endpoint",
        temperature=0.3,
        api_key="dapi-SECRET-TOKEN",
        base_url="https://example.com",
    )


class TestIdentity:
    def test_it_is_a_crewai_llm(self, inner):
        """``crewai.Agent`` type-checks its ``llm``; anything else is refused."""
        from crewai.llms.base_llm import BaseLLM

        assert isinstance(build_kasal_backed_llm(inner), BaseLLM)

    def test_it_holds_the_transport_object_itself(self, inner):
        """Not a copy. A copy is how the two harnesses drift apart."""
        assert build_kasal_backed_llm(inner).inner is inner

    def test_the_model_and_sampling_settings_are_carried(self, inner):
        wrapped = build_kasal_backed_llm(inner)
        # The provider prefix is consumed by the transport's own validator, so
        # the wrapper reports what will actually be sent.
        assert wrapped.model == "my-endpoint"
        assert wrapped.provider == "databricks"
        assert wrapped.temperature == 0.3


class TestTheCredentialStaysInOnePlace:
    """Execution logs are downloadable from the UI. This is not a style point."""

    def test_the_api_key_is_not_copied_onto_the_wrapper(self, inner):
        assert build_kasal_backed_llm(inner).api_key != "dapi-SECRET-TOKEN"

    def test_the_repr_cannot_leak_it(self, inner):
        assert "SECRET" not in repr(build_kasal_backed_llm(inner))

    def test_the_transport_object_is_not_serialized_with_the_model(self, inner):
        """A declared field would put the credential-bearing object in a dump."""
        dumped = build_kasal_backed_llm(inner).model_dump()
        assert "_inner" not in dumped
        assert "SECRET" not in str(dumped)


class TestForwarding:
    def test_call_forwards_every_argument_in_order(self):
        """CrewAI and the transport agree on the signature; keep it that way.

        ``response_model`` is forwarded by KEYWORD, not positionally:
        ``DatabricksRetryLLM.call`` accepts it via ``**kwargs`` (not a named
        positional), so a 7th positional arg raised "takes from 2 to 7 positional
        arguments but 8 were given". See ``test_call_tolerates_kwargs_only_inner``.
        """
        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.call.return_value = "answer"
        wrapped = build_kasal_backed_llm(inner)

        tools = [{"type": "function"}]
        callbacks = ["cb"]
        functions = {"f": lambda: None}
        result = wrapped.call(
            "hello", tools, callbacks, functions, "task", "agent", "model"
        )

        assert result == "answer"
        inner.call.assert_called_once_with(
            "hello",
            tools,
            callbacks,
            functions,
            "task",
            "agent",
            response_model="model",
        )

    def test_call_tolerates_kwargs_only_inner(self):
        """Regression: a transport whose ``call`` takes ``response_model`` only via
        ``**kwargs`` (DatabricksRetryLLM's shape) must not raise a positional-arity
        TypeError. Forwarding ``response_model`` by keyword is what makes this hold."""
        captured = {}

        def inner_call(
            messages,
            tools=None,
            callbacks=None,
            available_functions=None,
            from_task=None,
            from_agent=None,
            **kwargs,
        ):
            captured.update(kwargs)
            return "ok"

        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.call = inner_call
        wrapped = build_kasal_backed_llm(inner)

        result = wrapped.call("hi", response_model="MySchema")
        assert result == "ok"
        assert captured.get("response_model") == "MySchema"

    @pytest.mark.asyncio
    async def test_acall_forwards_to_the_transport_s_own_acall(self):
        """Not to ``call`` in a thread — the transport already decides that."""
        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.acall = AsyncMock(return_value="async answer")
        wrapped = build_kasal_backed_llm(inner)

        assert await wrapped.acall("hello") == "async answer"
        inner.acall.assert_awaited_once()

    def test_an_error_is_not_swallowed(self):
        """Retry, fallback and context-limit handling live in the transport."""
        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.call.side_effect = RuntimeError("endpoint refused")
        with pytest.raises(RuntimeError, match="endpoint refused"):
            build_kasal_backed_llm(inner).call("hi")


class TestCrewAIChoosesNativeToolCalling:
    """The wrapper must let CrewAI use native tool calls, not a prose loop.

    This is the regression test for a real production failure. CrewAI probes
    the LLM with ``hasattr(llm, "supports_function_calling")`` and, finding
    nothing, silently falls back to a ReAct loop where the agent writes
    ``Action Input:`` as text. Every no-argument tool call then failed with
    "the Action Input is not a valid key, value dictionary", and the model
    retried with an invented ``{"dummy": ""}`` to get past the parser — on the
    Kasal harness the same tools worked, because the transport has always made
    native tool calls.

    Asserting on CrewAI's OWN predicate rather than on the method's return
    value: what matters is the decision CrewAI reaches, and a future release
    could reach it a different way.
    """

    def test_crewai_selects_native_tools_for_the_wrapper(self, inner):
        from crewai.utilities.agent_utils import check_native_tool_support

        wrapped = build_kasal_backed_llm(inner)
        assert check_native_tool_support(wrapped, original_tools=[object()]) is True

    def test_the_method_crewai_probes_for_actually_exists(self, inner):
        """`hasattr` is the whole check — an omission reads as "cannot"."""
        wrapped = build_kasal_backed_llm(inner)
        assert callable(getattr(wrapped, "supports_function_calling", None))

    def test_capability_answers_come_from_the_transport(self):
        """Not hardcoded here. The transport knows the model; this does not."""
        from src.core.llm.transport.llm import LLM

        # gpt-5 rejects stop words; the transport says so and the wrapper must
        # repeat it rather than assert a convenient default.
        for model in ("gpt-4o", "gpt-5"):
            transport = LLM(model=model)
            wrapped = build_kasal_backed_llm(transport)
            assert (
                wrapped.supports_stop_words() == transport.supports_stop_words()
            ), model
            assert (
                wrapped.supports_function_calling()
                == transport.supports_function_calling()
            ), model

    def test_a_transport_that_cannot_answer_still_gets_native_tools(self):
        """ "Unknown" must mean yes here, or tool calls degrade to prose."""
        from unittest.mock import MagicMock

        mute = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        del mute.supports_function_calling
        assert build_kasal_backed_llm(mute).supports_function_calling() is True


class TestToolCallsAreHandedToCrewAI:
    """CrewAI owns the tool loop; the transport must give it the decision.

    Regression test for the second production failure of this integration:
    ``ValueError: Invalid response from LLM call - None or empty``, raised from
    CrewAI's ``_validate_and_finalize_llm_response`` on every tool-using turn.

    The cause was a split of responsibility neither side stated. Kasal's
    transport normally runs the whole tool loop itself and returns final text.
    CrewAI's native path calls the LLM with ``available_functions=None`` because
    ITS executor executes tools — applying reflection prompts, iteration limits
    and its tool-failure policy between rounds. With tools present and no
    functions to call, the transport fell through its loop and returned ``""``:
    a pure tool-call response has no content. CrewAI could only report that as
    an empty LLM response, nowhere near the real cause.
    """

    @staticmethod
    def _response_with_a_tool_call():
        from types import SimpleNamespace

        function = SimpleNamespace(
            name="postgres_list_tables", arguments='{"schema":"public"}'
        )
        message = SimpleNamespace(
            content=None,
            tool_calls=[SimpleNamespace(id="call_1", function=function)],
            reasoning_content=None,
        )
        return SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="tool_calls")],
            usage=SimpleNamespace(
                total_tokens=10,
                prompt_tokens=5,
                completion_tokens=5,
                prompt_tokens_details=None,
                completion_tokens_details=None,
            ),
        )

    @pytest.fixture
    def stub_client(self):
        client = MagicMock()
        client.chat.completions.create.return_value = self._response_with_a_tool_call()
        return client

    TOOLS = [{"type": "function", "function": {"name": "postgres_list_tables"}}]
    MESSAGES = [{"role": "user", "content": "list the tables"}]

    def test_the_wrapper_returns_the_tool_calls(self, stub_client):
        transport = LLM(model="gpt-4o")
        with patch.object(type(transport), "client", property(lambda s: stub_client)):
            answer = build_kasal_backed_llm(transport).call(
                self.MESSAGES, self.TOOLS, None, None
            )
        assert isinstance(answer, list) and len(answer) == 1

    def test_crewai_recognises_and_can_read_them(self, stub_client):
        """Both halves matter, and only together.

        ``is_tool_call_list`` accepts a plain dict with a "function" key, so a
        list of dicts passes that check and is stored as pending work — but
        ``extract_tool_call_info`` reads ``.function.name`` by ATTRIBUTE, gets
        nothing from a dict, and the call is silently skipped. The agent would
        sit there having decided to call a tool that never runs.
        """
        from crewai.utilities.agent_utils import (
            extract_tool_call_info,
            is_tool_call_list,
        )

        transport = LLM(model="gpt-4o")
        with patch.object(type(transport), "client", property(lambda s: stub_client)):
            answer = build_kasal_backed_llm(transport).call(
                self.MESSAGES, self.TOOLS, None, None
            )

        assert is_tool_call_list(answer)
        assert extract_tool_call_info(answer[0]) == (
            "call_1",
            "postgres_list_tables",
            '{"schema":"public"}',
        )

    def test_without_delegation_the_transport_returns_empty(self, stub_client):
        """The exact failure, pinned so it cannot come back unnoticed."""
        transport = LLM(model="gpt-4o")
        transport.delegate_tool_calls = False
        with patch.object(type(transport), "client", property(lambda s: stub_client)):
            assert transport.call(self.MESSAGES, self.TOOLS, None, None) == ""

    def test_the_kasal_engine_still_runs_its_own_tool_loop(self, stub_client):
        """Delegation is OFF by default, so nothing about Kasal changes.

        Given `available_functions`, the transport executes the tool itself and
        never hands the decision back — which is what the Kasal runtime relies
        on, and what `wrap_tool` is built around.
        """
        transport = LLM(model="gpt-4o")
        assert transport.delegate_tool_calls is False

        ran = []

        def _tool(**kwargs):
            ran.append(kwargs)
            return "tables: a, b"

        # Second round returns text so the loop terminates.
        from types import SimpleNamespace

        finished = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content="done", tool_calls=[], reasoning_content=None
                    ),
                    finish_reason="stop",
                )
            ],
            usage=SimpleNamespace(
                total_tokens=1,
                prompt_tokens=1,
                completion_tokens=0,
                prompt_tokens_details=None,
                completion_tokens_details=None,
            ),
        )
        stub_client.chat.completions.create.side_effect = [
            self._response_with_a_tool_call(),
            finished,
        ]
        with patch.object(type(transport), "client", property(lambda s: stub_client)):
            answer = transport.call(
                self.MESSAGES,
                self.TOOLS,
                None,
                {"postgres_list_tables": _tool},
            )

        assert answer == "done"
        assert ran, "the transport must still execute tools for the Kasal harness"


class TestContextWindow:
    def test_it_answers_from_the_transport_not_crewai_s_model_table(self):
        """CrewAI trims against this.

        Its table knows neither a Databricks serving endpoint nor a model a user
        added to the catalogue. Too large and the endpoint refuses the request;
        too small and the conversation is silently amputated.
        """
        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.get_context_window_size.return_value = 131072
        assert build_kasal_backed_llm(inner).get_context_window_size() == 131072

    def test_a_real_transport_llm_agrees_with_its_wrapper(self):
        inner = LLM(model="gpt-4o")
        assert (
            build_kasal_backed_llm(inner).get_context_window_size()
            == inner.get_context_window_size()
        )

    def test_a_transport_that_cannot_answer_falls_back_rather_than_raising(self):
        inner = MagicMock(model="m", temperature=None, stop=[], provider="openai")
        inner.get_context_window_size.side_effect = RuntimeError("no idea")
        # CrewAI's own default; a wrong window beats a failed run.
        assert build_kasal_backed_llm(inner).get_context_window_size() > 0
