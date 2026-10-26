"""Compaction the SERVER forces, when the estimate said the prompt fit.

The proactive trim in ``completion`` (``_trim_conversation_to_window``) decides
from a chars-per-token guess. The guess can be badly low: a run died carrying
240,735 characters of tool results the estimator priced at 70,804 tokens and
the server counted at 128,975 — JSON-escaped Cyrillic search results, where
``\\u0412`` is six characters and about five tokens for one letter. Until this
module existed the server's rejection was terminal. ``LLM.call`` re-raised it as
``LLMContextLengthExceededError`` and the executor declined to replay a turn
that had already run tools — correctly, since a retry of the identical prompt
would only repeat their side effects. Nothing stood between "the estimate was
wrong" and "the run failed".

The rejection is cheap (the server counts tokens and refuses before inference)
and it carries the one number the estimator lacked: how big the prompt REALLY
is. So the reactive path stubs the oldest tool results — the same lossy
operation the proactive trim performs — sized by the server's own count, and
retries the same round. No tool is re-executed: their results are already in
the conversation.

Pure helpers only. The ``OpenAICompletion`` methods that use them own the
event, the log line and the per-instance calibration.
"""

import re
from collections.abc import Callable
from typing import Any, Final

#: What replaces a compacted tool result. Shared by the proactive trim and the
#: reactive path so a result is never stubbed twice and the trace reads the same.
TOOL_RESULT_STUB: Final = "[earlier tool result trimmed to fit the context window]"

#: How many times one round may be rejected and retried behind compaction.
#: Every retry stubs at least one more tool result, so the loop ends on its own
#: once nothing is left to stub; the cap is for a server that keeps refusing for
#: a reason the phrase list mistook for a context overflow.
MAX_REJECTIONS_PER_ROUND: Final = 3

#: Largest ratio of "what the server counted" to "what the estimate said" that
#: is believed. The measured worst case was 2.4 (one fully JSON-escaped Cyrillic
#: page); anything beyond this is more likely a stray number in the message than
#: a tokenizer, and calibrating on it would shred the context for the rest of
#: the run. Above the cap the caller halves instead.
MAX_ESTIMATE_CORRECTION: Final = 5.0

# Token counts as providers print them: "133502", "133,502". Four digits or
# more — the small numbers in these messages are HTTP codes and max_tokens, and
# a count under 1,000 could not have overflowed any window. The lookarounds
# refuse digits glued to a word or a dot, so "req_1234567890" and "2.32.0" do
# not count.
_TOKEN_COUNT_RE = re.compile(r"(?<![\w.])(?:\d{1,3}(?:,\d{3})+|\d{4,})(?![\w.])")


def reported_prompt_tokens(error_message: str) -> int | None:
    """The prompt size the server measured, read out of its rejection.

    Every provider words the refusal differently, and every one of them puts
    the count in the message:

        llama.cpp  request (133502 tokens) exceeds the available context size
                   (131072 tokens)
        vLLM       maximum context length is 131072 tokens. However, you
                   requested 139516 tokens (131324 in the messages, 8192 in
                   the completion)
        OpenAI     This model's maximum context length is 128000 tokens.
                   However, your messages resulted in 133502 tokens
        Anthropic  prompt is too long: 213462 tokens > 200000 maximum

    The request was refused for being LARGER than the limit, so the largest
    number in the message is what was sent. vLLM's largest includes the
    completion allowance; that over-reads the prompt by ``max_tokens`` and
    compacts slightly more than necessary, which is the safe direction.
    None when the message carries no count — the caller halves instead.
    """
    counts = [
        int(match.group().replace(",", ""))
        for match in _TOKEN_COUNT_RE.finditer(error_message or "")
    ]
    return max(counts) if counts else None


def is_tool_result(message: dict[str, Any]) -> bool:
    """Chat Completions ``tool`` message or Responses ``function_call_output``."""
    return (
        message.get("role") == "tool" or message.get("type") == "function_call_output"
    )


def stub_oldest_tool_results(
    conversation: list[dict[str, Any]],
    estimated_tokens: Callable[[], int],
    target: int,
) -> int:
    """Replace tool results, oldest first, until ``estimated_tokens()`` is at
    most ``target``. Returns how many were replaced.

    Always replaces at least one when any remain — callers that want "only if
    over budget" check before calling. Never touches the system prompt, user
    messages or the tool_call structure (pairing must survive). Results already
    stubbed are skipped, so a second call eats into newer results instead of
    spinning on the same one.
    """
    compacted = 0
    for message in conversation:
        if not is_tool_result(message):
            continue
        key = "content" if message.get("role") == "tool" else "output"
        if message.get(key) == TOOL_RESULT_STUB:
            continue
        message[key] = TOOL_RESULT_STUB
        compacted += 1
        if estimated_tokens() <= target:
            break
    return compacted
