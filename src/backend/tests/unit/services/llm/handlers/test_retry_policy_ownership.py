"""Exactly one layer may retry a Databricks call.

The engine hands ``max_retries`` to the OpenAI SDK client, which retries with its
own backoff *underneath* DatabricksRetryLLM's loop — invisible to it. At the
engine default of 2 a rate-limited call became (2+1) x 5 = 15 HTTP attempts, each
outer attempt first paying the SDK's backoff and then ours.
"""

from unittest.mock import patch

from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM


def _llm(**kwargs):
    with patch("src.services.llm.handlers.databricks_retry_llm.litellm"):
        return DatabricksRetryLLM(
            model="databricks/databricks-claude-sonnet-4-6", api_key="k", **kwargs
        )


class TestRetryPolicyOwnership:
    def test_sdk_retries_are_off_by_default(self):
        assert _llm().max_retries == 0

    def test_wrapper_still_owns_a_retry_budget(self):
        llm = _llm()
        assert llm.MAX_RETRIES == 3
        assert llm.RATE_LIMIT_MAX_RETRIES == 5
        assert llm._get_max_retries(is_rate_limit=True) == llm.RATE_LIMIT_MAX_RETRIES
        assert llm._get_max_retries(is_rate_limit=False) == llm.MAX_RETRIES

    def test_total_attempts_are_no_longer_multiplied(self):
        llm = _llm()
        worst_case = (llm.max_retries + 1) * llm.RATE_LIMIT_MAX_RETRIES
        assert worst_case == llm.RATE_LIMIT_MAX_RETRIES == 5

    def test_explicit_max_retries_still_wins(self):
        """A caller that deliberately wants SDK-level retries keeps them."""
        assert _llm(max_retries=4).max_retries == 4

    def test_the_errors_the_sdk_would_have_retried_are_covered_here(self):
        llm = _llm()
        for error in (
            "timeout",
            "connection reset",
            "429 too many requests",
            "503 service unavailable",
            "502 bad gateway",
        ):
            assert llm._is_retryable_error(error), error
