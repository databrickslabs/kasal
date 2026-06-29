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
            "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call", side_effect=overflow
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
            "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call", side_effect=RuntimeError("invalid request: bad parameter")
        ), pytest.raises(RuntimeError):
            handler.call(messages=[{"role": "user", "content": "hi"}])


class TestPlaceholderResponseRetry:
    """A bare "Calling tools." answer is the sanitization placeholder echoed
    back by the model (history mimicry after many tool turns) — never a real
    answer. It must be treated as empty and retried with a corrective nudge."""

    def _make_handler(self) -> DatabricksRetryLLM:
        with patch("src.core.llm_handlers.databricks_gpt_oss_handler.litellm"):
            handler = DatabricksRetryLLM(model="databricks/test-model")
        # No real backoff waits in tests.
        handler._get_backoff_time = lambda *a, **k: 0
        return handler

    def test_is_placeholder_response_detection(self):
        from src.core.llm_handlers.databricks_gpt_oss_handler import (
            _is_placeholder_response,
        )

        assert _is_placeholder_response("Calling tools.") is True
        assert _is_placeholder_response("  calling tools  ") is True
        assert _is_placeholder_response("Calling tools") is True
        assert _is_placeholder_response("Calling tools to fetch data…") is False
        assert _is_placeholder_response("A real answer.") is False
        assert _is_placeholder_response("") is False
        assert _is_placeholder_response(None) is False
        assert _is_placeholder_response(42) is False

    def test_append_placeholder_nudge_is_idempotent_and_safe(self):
        from src.core.llm_handlers.databricks_gpt_oss_handler import (
            _PLACEHOLDER_NUDGE,
            _append_placeholder_nudge,
        )

        messages = [{"role": "user", "content": "hi"}]
        _append_placeholder_nudge(messages)
        _append_placeholder_nudge(messages)
        assert len(messages) == 2
        assert messages[-1] == {"role": "user", "content": _PLACEHOLDER_NUDGE}
        _append_placeholder_nudge("not-a-list")  # no crash

    def test_call_retries_placeholder_and_returns_the_real_answer(self):
        from src.core.llm_handlers.databricks_gpt_oss_handler import (
            _PLACEHOLDER_NUDGE,
        )

        handler = self._make_handler()
        messages = [{"role": "user", "content": "list the tables"}]

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call",
            side_effect=["Calling tools.", "Here are the 12 tables…"],
        ) as mock_call, patch("time.sleep"):
            result = handler.call(messages=messages)

        assert result == "Here are the 12 tables…"
        assert mock_call.call_count == 2
        # The retry carried the corrective nudge so the model breaks the loop.
        retry_messages = mock_call.call_args_list[1].args[0]
        assert retry_messages[-1]["content"] == _PLACEHOLDER_NUDGE

    def test_call_gives_up_after_max_retries_of_placeholder(self):
        handler = self._make_handler()

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call",
            side_effect=["Calling tools."] * 10,
        ), patch("time.sleep"):
            result = handler.call(messages=[{"role": "user", "content": "hi"}])

        # Terminal behavior matches the empty-response path (empty string),
        # not a placeholder masquerading as an answer.
        assert result == ""


# ---------------------------------------------------------------------------
# Structured-output coercion (_coerce_to_response_model)
# ---------------------------------------------------------------------------
#
# CrewAI's long-term-memory consolidation / save-analysis pass response_model and
# then do `isinstance(resp, Model) or Model.model_validate(resp)`. litellm returns
# structured output as a JSON *string*, so model_validate(<str>) raised and CrewAI
# fell back ("Consolidation analysis failed, defaulting to insert"). The wrapper
# now parses the string into the model so the plan is actually used.

from pydantic import BaseModel  # noqa: E402


class _Plan(BaseModel):
    keep: bool
    note: str = ""


class TestCoerceToResponseModel:
    def test_parses_json_string_into_response_model(self):
        out = DatabricksRetryLLM._coerce_to_response_model(
            '{"keep": true, "note": "hi"}', {"response_model": _Plan}
        )
        assert isinstance(out, _Plan)
        assert out.keep is True and out.note == "hi"

    def test_strips_markdown_json_fence(self):
        fenced = "```json\n{\"keep\": false}\n```"
        out = DatabricksRetryLLM._coerce_to_response_model(fenced, {"response_model": _Plan})
        assert isinstance(out, _Plan)
        assert out.keep is False

    def test_no_response_model_returns_result_unchanged(self):
        out = DatabricksRetryLLM._coerce_to_response_model('{"keep": true}', {})
        assert out == '{"keep": true}'

    def test_non_string_result_returned_unchanged(self):
        plan = _Plan(keep=True)
        out = DatabricksRetryLLM._coerce_to_response_model(plan, {"response_model": _Plan})
        assert out is plan

    def test_invalid_json_falls_back_to_original_string(self):
        # Not valid for the model → return the original string so the caller's
        # own fallback still applies (behaviour identical to before the fix).
        bad = "not json at all"
        out = DatabricksRetryLLM._coerce_to_response_model(bad, {"response_model": _Plan})
        assert out == bad


class TestSupportsNativeStructuredOutput:
    """DatabricksRetryLLM enforces output_pydantic (response_format json_schema)
    for all Databricks chat models except gemini, so the converter keeps the real
    schema instead of the unenforced output_json downgrade."""

    def _make(self, model: str) -> DatabricksRetryLLM:
        # Use the "databricks/<model>" prefix llm_manager actually passes; without
        # it CrewAI's LLM factory dispatches to a provider completion class instead
        # of keeping DatabricksRetryLLM.
        with patch("src.core.llm_handlers.databricks_gpt_oss_handler.litellm"):
            return DatabricksRetryLLM(model=model)

    def test_claude_supports_native(self):
        assert self._make("databricks/databricks-claude-opus-4-8").supports_native_structured_output() is True

    def test_llama_supports_native(self):
        assert self._make("databricks/llama-4-maverick").supports_native_structured_output() is True

    def test_gpt_oss_supports_native(self):
        assert self._make("databricks/databricks-gpt-oss-120b").supports_native_structured_output() is True

    def test_gemini_excluded(self):
        assert self._make("databricks/databricks-gemini-2-5-pro").supports_native_structured_output() is False
