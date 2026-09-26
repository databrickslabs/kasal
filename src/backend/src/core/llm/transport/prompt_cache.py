"""Anthropic prompt caching for the agent tool loop.

Every tool round resends the system prompt, the tool schemas and the whole
conversation so far. Claude caches a prompt prefix only where a request marks
it with ``cache_control: {"type": "ephemeral"}``; unmarked, every round is billed
at full input price and prefilled from scratch. This module decides where the
markers go, per endpoint, and never lets one reach an endpoint that rejects it.

Placement (Anthropic's limit is 4 breakpoints per request; markers the caller
already set count against it):

1. **The stable prefix** — the last system block. Tools render before system,
   so one marker there caches tool definitions and system prompt together. With
   no system prompt, the native path marks the last tool definition instead.
2. **The rolling tail** — the end of the conversation as sent. Next round's
   prompt starts with this one's, so its marker becomes a read point (the API
   looks back up to 20 blocks for the previous entry).
3. **CrewAI's hints** — ``crewai.llms.cache.mark_cache_breakpoint`` flags the
   system prompt and the initial task prompt with a top-level
   ``cache_breakpoint`` key. For Claude it is translated into a marker on that
   user message (the system hint is already covered by 1); for every other
   endpoint it is removed, because OpenAI-compatible servers 400 on the
   unknown field.

Endpoints:

* ``"anthropic"`` — the native Messages API (``AnthropicClient``). The chat
  messages keep their hints here and ``anthropic_messages.message_params``
  places markers on the NATIVE request, where a tool result is a real block.
* ``"databricks"`` — Databricks-hosted Claude over the OpenAI-compatible chat
  API. Databricks documents ``cache_control`` on text, image and reasoning
  content items and on ``tool_calls`` entries, for Claude only. A ``tool``
  message has no documented field, so a tail that ends in tool results is
  marked on the preceding assistant's last tool call.
* ``None`` — everything else: hints stripped, nothing added.

Minimum cacheable prefix is model-dependent (512–4096 tokens); a shorter
prefix is not an error, the marker is simply ignored by the server.
"""

from __future__ import annotations

from typing import Any, Literal

CacheMode = Literal["anthropic", "databricks"]

HINT_KEY = "cache_breakpoint"
MAX_BREAKPOINTS = 4
EPHEMERAL: dict[str, str] = {"type": "ephemeral"}

_SYSTEM_ROLES = ("system", "developer")
# Anthropic: thinking blocks cannot carry cache_control.
_UNMARKABLE_NATIVE = ("thinking", "redacted_thinking")


def cache_mode(provider: str | None, model: str | None, api: str) -> CacheMode | None:
    """Which caching dialect this endpoint speaks, or None for "send no markers".

    Databricks is matched on the endpoint name: a Claude serving endpoint is
    named ``databricks-claude-*`` (or carries ``claude`` in a custom name).
    Anything that cannot be identified as Claude gets no markers — a marker on a
    non-Claude endpoint is a 400, while a missing one only costs money.
    """
    if provider == "anthropic":
        return "anthropic"
    if api != "completions":
        return None
    if provider == "databricks" and "claude" in str(model or "").lower():
        return "databricks"
    return None


def without_hints(messages: list[Any]) -> list[Any]:
    """Remove CrewAI's internal cache hint without mutating its conversation.

    OpenAI-compatible endpoints reject this top-level field. Preserve all
    other fields, including Responses reasoning/tool items and content blocks.
    """
    return [
        (
            {key: value for key, value in message.items() if key != HINT_KEY}
            if isinstance(message, dict) and HINT_KEY in message
            else message
        )
        for message in messages
    ]


def chat_messages_for(mode: CacheMode | None, messages: list[Any]) -> list[Any]:
    """The Chat Completions ``messages`` to send for this endpoint."""
    if mode == "anthropic":
        # The native translator reads the hints and builds its own dicts, so
        # nothing with a top-level hint ever reaches the wire.
        return list(messages)
    if mode == "databricks":
        return mark_chat_messages(messages)
    return without_hints(messages)


# ── OpenAI-compatible chat shape (Databricks-hosted Claude) ────────────────


def _has_marker(value: Any) -> bool:
    return isinstance(value, dict) and "cache_control" in value


def _count_chat_markers(messages: list[Any]) -> int:
    count = 0
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, list):
            count += sum(1 for block in content if _has_marker(block))
        count += sum(1 for call in message.get("tool_calls") or [] if _has_marker(call))
    return count


def _mark_chat_content(message: dict[str, Any]) -> dict[str, Any] | None:
    """``message`` with its last text/image item marked, or None if it has none."""
    content = message.get("content")
    if isinstance(content, str):
        if not content.strip():
            return None
        block = {"type": "text", "text": content, "cache_control": dict(EPHEMERAL)}
        return {**message, "content": [block]}
    if not isinstance(content, list):
        return None
    for i in range(len(content) - 1, -1, -1):
        block = content[i]
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "text" and not str(block.get("text") or "").strip():
            continue
        if kind not in ("text", "image_url"):
            continue
        if _has_marker(block):
            return message
        blocks = list(content)
        blocks[i] = {**block, "cache_control": dict(EPHEMERAL)}
        return {**message, "content": blocks}
    return None


def _mark_chat_tool_call(message: dict[str, Any]) -> dict[str, Any] | None:
    calls = message.get("tool_calls")
    if not isinstance(calls, list) or not calls or not isinstance(calls[-1], dict):
        return None
    if _has_marker(calls[-1]):
        return message
    marked = list(calls)
    marked[-1] = {**calls[-1], "cache_control": dict(EPHEMERAL)}
    return {**message, "tool_calls": marked}


def _chat_tail_target(messages: list[Any]) -> tuple[int, str] | None:
    """Where the rolling breakpoint goes: ``(index, "content"|"tool_call")``."""
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        if role in _SYSTEM_ROLES:
            return None  # nothing after the system prompt; its own marker covers it
        if role == "tool":
            continue  # undocumented on Databricks — mark the call that produced it
        if role == "assistant" and message.get("tool_calls"):
            return index, "tool_call"
        return index, "content"
    return None


def mark_chat_messages(messages: list[Any]) -> list[Any]:
    """Hints translated into ``cache_control`` for Databricks-hosted Claude.

    Returns a new list; only the messages that gain a marker are copied, so the
    caller's conversation is never mutated.
    """
    hinted = [
        i
        for i, m in enumerate(messages)
        if isinstance(m, dict) and m.get(HINT_KEY) and m.get("role") == "user"
    ]
    out = without_hints(messages)
    budget = MAX_BREAKPOINTS - _count_chat_markers(out)

    targets: list[tuple[int, str]] = []
    system = [
        i
        for i, m in enumerate(out)
        if isinstance(m, dict) and m.get("role") in _SYSTEM_ROLES
    ]
    if system:
        targets.append((system[-1], "content"))
    tail = _chat_tail_target(out)
    if tail is not None:
        targets.append(tail)
    targets.extend((i, "content") for i in reversed(hinted))

    seen: set[tuple[int, str]] = set()
    for index, kind in targets:
        if budget <= 0:
            break
        if (index, kind) in seen:
            continue
        seen.add((index, kind))
        message = out[index]
        marked = (
            _mark_chat_tool_call(message)
            if kind == "tool_call"
            else _mark_chat_content(message)
        )
        if marked is None or marked is message:
            continue  # nothing markable, or the caller already marked it
        out[index] = marked
        budget -= 1
    return out


# ── Native Messages API shape ──────────────────────────────────────────────


def _count_native_markers(request: dict[str, Any]) -> int:
    count = sum(1 for block in request.get("system") or [] if _has_marker(block))
    count += sum(1 for tool in request.get("tools") or [] if _has_marker(tool))
    for message in request.get("messages") or []:
        count += sum(1 for block in message["content"] if _has_marker(block))
    return count


def _native_markable(block: Any) -> bool:
    if not isinstance(block, dict) or block.get("type") in _UNMARKABLE_NATIVE:
        return False
    return not (block.get("type") == "text" and not str(block.get("text") or ""))


def _mark_native_last(blocks: list[Any], upto: int | None = None) -> bool | None:
    """Mark the last markable block at or before ``upto``, in place on the list.

    The block itself is copied — it may be a replayed signed block the client
    keeps for later rounds. Returns True when a marker was added, False when the
    block was already marked, None when nothing could carry one.
    """
    end = len(blocks) - 1 if upto is None else min(upto, len(blocks) - 1)
    for i in range(end, -1, -1):
        block = blocks[i]
        if not _native_markable(block):
            continue
        if _has_marker(block):
            return False
        blocks[i] = {**block, "cache_control": dict(EPHEMERAL)}
        return True
    return None


def mark_native_request(
    request: dict[str, Any], hinted: list[tuple[int, int]] | None = None
) -> None:
    """Place breakpoints on a native Messages API request, in place.

    ``hinted`` lists ``(message index, last block index)`` for each user message
    CrewAI flagged, recorded while the conversation was translated (Anthropic
    coalesces consecutive same-role turns, so chat positions do not survive).
    Every list and block touched here was built by the translator; blocks are
    copied before marking.
    """
    budget = MAX_BREAKPOINTS - _count_native_markers(request)
    if budget <= 0:
        return
    system = request.get("system")
    tools = request.get("tools")
    messages = request.get("messages") or []

    def spend(result: bool | None) -> None:
        nonlocal budget
        if result:
            budget -= 1

    if system:
        spend(_mark_native_last(system))
    elif tools:
        spend(_mark_native_last(tools))
    if budget > 0 and messages:
        spend(_mark_native_last(messages[-1]["content"]))
    for message_index, block_index in reversed(hinted or []):
        if budget <= 0:
            break
        spend(_mark_native_last(messages[message_index]["content"], block_index))
