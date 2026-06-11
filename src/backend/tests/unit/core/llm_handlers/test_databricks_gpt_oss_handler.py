"""Tests for DatabricksRetryLLM context-length error handling.

Regression suite for a critical bug: context-overflow errors were rewrapped
into a plain ``ValueError`` whose message contained no phrase from CrewAI's
``CONTEXT_LIMIT_ERRORS`` list, so ``respect_context_window`` summarization
never fired and the agent replayed the entire task up to max_retry_limit
times — deterministically overflowing again on each replay.

The contract: an overflow surfaced by the handler must (a) be CrewAI's
``LLMContextLengthExceededError`` and (b) carry a message CrewAI's own
phrase-matcher recognizes.
"""

import pytest
from unittest.mock import patch

from crewai.utilities.agent_utils import is_context_length_exceeded
from crewai.utilities.exceptions.context_window_exceeding_exception import (
    LLMContextLengthExceededError,
)

from src.core.llm_handlers.databricks_gpt_oss_handler import DatabricksRetryLLM


def _bare_handler() -> DatabricksRetryLLM:
    """Handler instance without running the heavy __init__ (no litellm setup)."""
    return object.__new__(DatabricksRetryLLM)


class TestContextLengthHint:
    @pytest.mark.parametrize(
        "error_str",
        [
            "prompt is too long: 2523462 tokens > 1000000 maximum",
            "maximum context length is 128000 tokens",
            "context_length_exceeded",
            "the context window is full",
            "too many tokens in request",
            "input is too long for this model",
            "request exceeds token limit",
            "expected a string with maximum length 400000",
        ],
    )
    def test_overflow_variants_are_detected(self, error_str):
        hint = _bare_handler()._context_length_hint(error_str)
        assert hint is not None

    @pytest.mark.parametrize(
        "error_str",
        [
            "connection reset by peer",
            "401 invalid access token",
            "rate limit exceeded",
            "internal server error",
        ],
    )
    def test_non_overflow_errors_return_none(self, error_str):
        assert _bare_handler()._context_length_hint(error_str) is None

    def test_hint_is_recognized_by_crewai_phrase_matcher(self):
        """The hint itself must match CONTEXT_LIMIT_ERRORS — CrewAI's recovery
        phrase-matches str(exception), not the exception type alone."""
        hint = _bare_handler()._context_length_hint("prompt is too long")
        assert hint is not None
        assert LLMContextLengthExceededError._is_context_limit_error(hint)


class TestCallRaisesRecognizableOverflow:
    def _make_handler(self) -> DatabricksRetryLLM:
        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler.litellm"
        ):
            return DatabricksRetryLLM(model="databricks/test-model")

    def test_call_raises_crewai_context_exception(self):
        handler = self._make_handler()
        overflow = Exception("prompt is too long: 2523462 tokens > 1000000 maximum")

        with patch(
            "crewai.LLM.call", side_effect=overflow
        ), pytest.raises(LLMContextLengthExceededError) as exc_info:
            handler.call(messages=[{"role": "user", "content": "hi"}])

        # CrewAI's own detector must accept the raised exception, otherwise
        # summarize-and-continue never fires and the task replays from scratch.
        assert is_context_length_exceeded(exc_info.value)
        assert exc_info.value.__cause__ is overflow

    def test_call_does_not_wrap_unrelated_errors(self):
        handler = self._make_handler()

        # Non-retryable, non-auth, non-overflow error must propagate unwrapped.
        with patch(
            "crewai.LLM.call", side_effect=RuntimeError("invalid request: bad parameter")
        ), pytest.raises(RuntimeError):
            handler.call(messages=[{"role": "user", "content": "hi"}])
