"""A response the model never finished must not pass as an answer.

``finish_reason == "length"`` is the protocol's own statement that the output
allowance ran out mid-generation, and it was ignored. What that produced: a run
whose stored "answer" was a fragment — and, when the model had stopped saying
anything new, 8KB of ``- SOC 1``, ``- SOC 2``, … up to the ceiling, persisted as
the result with nothing anywhere reporting a problem.

This replaced a hand-written repetition detector that inspected the text for a
repeating unit. It was the wrong mechanism: it could only see EXACT periodicity,
so an enumerating drift (``SOC 394``, ``SOC 395``) went straight through it,
while the endpoint had been reporting the real condition all along.
"""

from types import SimpleNamespace

import pytest

from src.core.llm.transport.completion import _check_truncation
from src.core.llm.transport.exceptions import (
    ExecutionBudgetExceededError,
    LLMOutputTruncatedError,
)


class TestFinishReason:
    def test_length_is_a_failure(self):
        with pytest.raises(LLMOutputTruncatedError):
            _check_truncation("some-model", "length", "half a sentence")

    def test_stop_passes(self):
        _check_truncation("some-model", "stop", "a complete answer")

    def test_tool_calls_pass(self):
        """The model stopped to call a tool. That is a normal end."""
        _check_truncation("some-model", "tool_calls", "")

    def test_a_missing_finish_reason_passes(self):
        """Not every endpoint reports one, and absence is not truncation."""
        _check_truncation("some-model", None, "an answer")

    def test_content_filter_passes(self):
        """A refusal is an answer about the request, not a fragment of one."""
        _check_truncation("some-model", "content_filter", "I cannot help with that.")


class TestWhatTheCallerGets:
    def test_the_partial_text_is_attached(self):
        """The fragment is the only salvageable thing; dropping it would make the
        failure less useful than the silent truncation it replaces."""
        with pytest.raises(LLMOutputTruncatedError) as caught:
            _check_truncation("some-model", "length", "the first half")

        assert caught.value.partial == "the first half"

    def test_the_message_names_the_model_and_the_size(self):
        with pytest.raises(LLMOutputTruncatedError) as caught:
            _check_truncation("qwen3-coder", "length", "x" * 8083)

        message = str(caught.value)
        assert "qwen3-coder" in message
        assert "8083" in message

    def test_it_is_a_budget_breach(self):
        """Callers that already degrade on a budget error do the right thing
        here with no change: the run stops, the partial is offered, and the
        standard path emits LLMCallFailedEvent."""
        assert issubclass(LLMOutputTruncatedError, ExecutionBudgetExceededError)

    def test_no_content_still_fails(self):
        """Truncated with nothing written is the worst case, not an exemption."""
        with pytest.raises(LLMOutputTruncatedError):
            _check_truncation("some-model", "length", None)


class TestTheDegenerateCaseThatStartedThis:
    def test_an_enumerating_loop_is_caught_by_the_finish_reason(self):
        """The exact output that got through the old detector.

        It has no repeating unit — every line differs by a number — so nothing
        that inspects the TEXT can see it. The endpoint reported `length`, which
        is all that was ever needed.
        """
        degenerate = "".join(f"- SOC {n}\n" for n in range(1, 457))

        with pytest.raises(LLMOutputTruncatedError) as caught:
            _check_truncation("qwen3-coder", "length", degenerate)

        assert caught.value.partial == degenerate


class TestResponseShape:
    def test_the_finish_reason_is_read_off_the_choice(self):
        """Pins where it comes from: choices[0].finish_reason, which is where
        every OpenAI-compatible endpoint puts it."""
        choice = SimpleNamespace(
            finish_reason="length", message=SimpleNamespace(content="fragment")
        )

        with pytest.raises(LLMOutputTruncatedError):
            _check_truncation("m", getattr(choice, "finish_reason", None), "fragment")
