"""Context-overflow detection must be one list, shared with the engine.

A missed phrase is not cosmetic: the run hard-fails where it could have compacted
and continued, so a phrase learned in one place has to reach every consumer.
"""

from kasal_engine.llm import CONTEXT_LIMIT_ERRORS
from src.core.llm.context_limits import (
    _KASAL_CONTEXT_LIMIT_PHRASES,
    extend_engine_context_limit_phrases,
    is_context_limit_error,
)


class TestSharedPhraseList:
    def test_kasal_phrases_are_merged_into_the_engine_list(self):
        for phrase in _KASAL_CONTEXT_LIMIT_PHRASES:
            assert phrase in CONTEXT_LIMIT_ERRORS

    def test_extension_is_idempotent(self):
        before = len(CONTEXT_LIMIT_ERRORS)
        extend_engine_context_limit_phrases()
        extend_engine_context_limit_phrases()
        assert len(CONTEXT_LIMIT_ERRORS) == before

    def test_engine_builtins_survive_the_extension(self):
        assert "context length exceeded" in CONTEXT_LIMIT_ERRORS
        assert "too many tokens" in CONTEXT_LIMIT_ERRORS


class TestIsContextLimitError:
    def test_vllm_phrasing(self):
        assert is_context_limit_error(
            "This model's maximum context length is only 28672 tokens. "
            "Please reduce the length of the input."
        )

    def test_anthropic_on_databricks_phrasing(self):
        assert is_context_limit_error("BadRequestError: prompt is too long: 210000 tokens")

    def test_openai_phrasing(self):
        assert is_context_limit_error("context_length_exceeded: maximum context length")

    def test_matching_is_case_insensitive(self):
        assert is_context_limit_error("MAXIMUM CONTEXT LENGTH exceeded")

    def test_unrelated_errors_do_not_match(self):
        assert not is_context_limit_error("rate limit exceeded")
        assert not is_context_limit_error("401 Unauthorized")
        assert not is_context_limit_error("")


class TestHandlerUsesTheSharedList:
    def test_context_length_hint_matches_a_kasal_phrase(self):
        """The hint used to match a private tuple; it now shares the list, so a
        vLLM-phrased overflow is recognised by the Databricks wrapper too."""
        from unittest.mock import patch

        with patch("src.core.llm.handlers.databricks_retry_llm.litellm"):
            from src.core.llm.handlers.databricks_retry_llm import DatabricksRetryLLM

            llm = DatabricksRetryLLM(model="databricks/x", api_key="k")

        hint = llm._context_length_hint("please reduce the length of the input")
        assert hint is not None
        assert hint.lower().startswith("context length exceeded")
        assert llm._context_length_hint("some unrelated failure") is None
