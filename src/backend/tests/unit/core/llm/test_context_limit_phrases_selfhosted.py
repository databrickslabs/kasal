"""The overflow has to be RECOGNISED, or nothing can react to it.

Recovery is phrase-matched against the error text: `databricks_retry_llm`
asks `_context_length_hint(error_str)` and, on a match, raises
LLMContextLengthExceededError so summarisation can fire. No match means the
handler logs "Non-retryable error" and the 400 kills the run — which is what
happened, because none of the known phrases appear in this server's wording.
"""

import pytest

from src.core.llm.context_limits import is_context_limit_error

# The real 400, verbatim from crew.log.
SELF_HOSTED_400 = (
    "Error code: 400 - {'error': {'code': 400, 'message': 'request (139516 "
    "tokens) exceeds the available context size (131072 tokens), try "
    "increasing it', 'type': 'exceed_context_size_error', 'n_prompt_tokens': "
    "139516, 'n_ctx': 131072}}"
)


def test_the_wording_that_killed_the_run_is_recognised():
    assert is_context_limit_error(SELF_HOSTED_400)


def test_the_error_TYPE_alone_is_enough():
    """Listed as well as the prose: the type survives a reworded message."""
    assert is_context_limit_error("{'type': 'exceed_context_size_error'}")


@pytest.mark.parametrize(
    "message",
    [
        "This model's maximum context length is 8192 tokens",
        "prompt is too long: 210000 tokens > 200000 maximum",
        "exceeds maximum allowed content length",
    ],
)
def test_the_wordings_that_already_worked_still_do(message):
    assert is_context_limit_error(message)


@pytest.mark.parametrize(
    "message",
    [
        "429 Too Many Requests",
        "connection reset by peer",
        "invalid api key",
        "",
    ],
)
def test_unrelated_failures_are_not_mistaken_for_overflow(message):
    """A false positive here would raise LLMContextLengthExceededError for a
    rate limit and send the agent compacting instead of retrying."""
    assert not is_context_limit_error(message)
