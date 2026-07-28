"""One answer to "is this error a context-window overflow?".

The knowledge was split three ways: the engine ships ``CONTEXT_LIMIT_ERRORS``
(used by ``is_context_length_exceeded`` and ``LLMContextLengthExceededError``),
``llm_manager`` appended vLLM's phrasing to that list at import, and
``DatabricksRetryLLM._context_length_hint`` matched its own private tuple of
markers. A phrase learned in one place did not help the other two — and the
consequence is asymmetric: a missed phrase means a run HARD-FAILS where it could
have compacted and continued.

So the engine's list stays the single source of truth and is extended here, once,
with the phrasings kasal's endpoints actually emit. Extending rather than
replacing keeps the engine's own recovery paths (which read that list) in sync
with ours for free.
"""

import logging
from typing import Final

from src.core.llm.transport import CONTEXT_LIMIT_ERRORS

logger = logging.getLogger(__name__)

# Phrasings the engine's built-in list does not carry, all observed in kasal:
#   - vLLM / OpenAI-compatible self-hosted servers say "... the model's context
#     length is only N tokens ... maximum input length ... Please reduce the
#     length of the input", matching NONE of the built-ins. Without these a crew
#     whose sequential-task context outgrows the window fails outright instead of
#     summarizing — the output clamp cannot help once the INPUT alone is too big.
#   - Anthropic-on-Databricks says "prompt is too long".
#   - Databricks model serving says "exceeds maximum allowed content length",
#     and puts "requestsize" in the error envelope. These were patched into the
#     engine list from inside the crew subprocess; that block referenced
#     crewAI's context_window_exceeding_exception, which stopped existing when
#     crewAI was removed, so it raised NameError into its own except and printed
#     a warning on every run. The phrases went with it — none of the others here
#     match Databricks' wording, so a Databricks overflow HARD-FAILED instead of
#     compacting. They belong here, where every consumer sees them.
#   - Assorted providers phrase it as "maximum context" / "context window" /
#     "tokens > " without the exact built-in wording.
_KASAL_CONTEXT_LIMIT_PHRASES: Final[tuple[str, ...]] = (
    "maximum input length",
    "please reduce the length of the input",
    "the model's context length is only",
    "prompt is too long",
    "exceeds maximum allowed content length",
    "maximum allowed content length",
    "requestsize",
    "maximum context",
    "context window",
    "context_length",
    "tokens > ",
)


def extend_engine_context_limit_phrases() -> None:
    """Merge kasal's phrasings into the engine list. Idempotent."""
    added = [p for p in _KASAL_CONTEXT_LIMIT_PHRASES if p not in CONTEXT_LIMIT_ERRORS]
    CONTEXT_LIMIT_ERRORS.extend(added)
    if added:
        logger.info(
            "Extended engine CONTEXT_LIMIT_ERRORS with %d kasal phrases", len(added)
        )


def is_context_limit_error(error_str: str) -> bool:
    """True when ``error_str`` reads like a context-window overflow.

    Takes an already-stringified error so callers that have only a message (and
    have often already lowercased it) do not have to fabricate an exception.
    """
    if not error_str:
        return False
    lowered = error_str.lower()
    return any(phrase.lower() in lowered for phrase in CONTEXT_LIMIT_ERRORS)


# The engine list is a module-level singleton, so extending it at import time
# means every consumer — engine recovery paths included — sees the same phrases.
extend_engine_context_limit_phrases()
