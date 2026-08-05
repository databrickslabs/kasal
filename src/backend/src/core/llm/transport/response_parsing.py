"""Pulling the structured bits out of a provider response.

Pure functions over the OpenAI SDK's response objects — token counts, tool
calls, reasoning items, built-in tool records. No client, no state, no events;
every one is ``response -> plain dict/list``.

Extracted from ``completion.py``, which is over the file-size ceiling. The
methods stay on ``OpenAICompletion`` as one-line delegates because they are
part of a real subclass contract: ``DatabricksResponsesLLM`` calls
``self._extract_*`` and its tests patch those names. Moving the BODIES out
shrinks the module without touching that surface.

Both response shapes are handled here because both arrive at the same call
sites: a Chat Completions object carries ``choices[0].message``, a Responses
object carries a flat ``output`` list. ``function_calls`` reads either.
"""

from typing import Any

#: Sentinel used as the reasoning value when the model DID reason but the
#: provider withheld the text (Anthropic on Databricks encrypts it into an
#: opaque `signature`). Carried through the event → trace → UI so the reasoning
#: panel can explain itself instead of rendering nothing, which reads as "this
#: model does not reason" and is a different, wrong claim. The UI matches on this
#: exact string, so do not reword it without updating
#: frontend Common/ReasoningPanel.tsx.
REDACTED_REASONING = "__kasal_reasoning_redacted__"

#: Responses-API item types that are a provider-side tool the model ran itself,
#: as opposed to a function call handed back for us to execute.
BUILTIN_TOOL_CALL_TYPES = (
    "web_search_call",
    "file_search_call",
    "code_interpreter_call",
    "computer_call",
)


def chat_token_usage(response: Any) -> dict[str, Any] | None:
    """Token counts from a Chat Completions response, or None if absent."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    details = getattr(usage, "prompt_tokens_details", None)
    return {
        "prompt_tokens": getattr(usage, "prompt_tokens", 0),
        "completion_tokens": getattr(usage, "completion_tokens", 0),
        "total_tokens": getattr(usage, "total_tokens", 0),
        "cached_prompt_tokens": (
            getattr(details, "cached_tokens", 0) if details else 0
        ),
    }


def responses_token_usage(response: Any) -> dict[str, Any] | None:
    """Token counts from a Responses API response, or None if absent."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    return {
        "prompt_tokens": getattr(usage, "input_tokens", 0),
        "completion_tokens": getattr(usage, "output_tokens", 0),
        "total_tokens": getattr(usage, "total_tokens", 0),
    }


def function_calls(response: Any) -> list[dict[str, Any]]:
    """Normalized tool calls from a chat completion or a Responses object."""
    calls: list[dict[str, Any]] = []
    choices = getattr(response, "choices", None)
    if choices:
        tool_calls = getattr(choices[0].message, "tool_calls", None) or []
        for call in tool_calls:
            function = getattr(call, "function", None)
            if function is not None:
                calls.append(
                    {
                        "id": call.id,
                        "name": function.name,
                        "arguments": function.arguments,
                    }
                )
        return calls
    for item in getattr(response, "output", None) or []:
        if getattr(item, "type", None) == "function_call":
            calls.append(
                {
                    "id": getattr(item, "call_id", None) or getattr(item, "id", None),
                    "name": item.name,
                    "arguments": item.arguments,
                }
            )
    return calls


def reasoning_items(response: Any) -> list[Any]:
    """The raw reasoning items, for chaining them into the next request."""
    return [
        item
        for item in (getattr(response, "output", None) or [])
        if getattr(item, "type", None) == "reasoning"
    ]


def _block_field(block: Any, key: str) -> Any:
    """Read ``key`` off a content block that may be a dict OR an SDK object."""
    if isinstance(block, dict):
        return block.get(key)
    return getattr(block, key, None)


def split_content_blocks(content: Any) -> tuple[str, str]:
    """Split Chat-Completions ``content`` into ``(text, reasoning_text)``.

    Most endpoints put a plain string here. Anthropic-style reasoning models on
    Databricks (Claude Fable 5, verified against the live endpoint) instead send
    a LIST of typed blocks, and mix the two within a single stream::

        delta 1  content=[{"type": "reasoning", "summary": [...]}]   <- list!
        delta 2  content="OK"                                       <- str
        delta 3  content=""

    Callers assumed ``str`` unconditionally, so the list reached
    ``LLMStreamChunkEvent(chunk=...)`` — declared ``chunk: str`` — and every run
    died with "1 validation error for LLMStreamChunkEvent / chunk / Input should
    be a valid string". The non-streaming path had the matching bug: the whole
    block list became the answer text.

    Returns the concatenated ``type == "text"`` parts as the answer, and the
    reasoning summary text separately so a caller can surface it as its own
    event instead of smuggling it into the output. A ``str`` input is returned
    unchanged with empty reasoning, so non-reasoning models take an identity
    path.
    """
    if content is None:
        return "", ""
    if isinstance(content, str):
        return content, ""
    if not isinstance(content, (list, tuple)):
        # Unknown shape — stringify rather than raise; a weird answer beats a
        # crashed run, and the caller only ever needs text out of this.
        return str(content), ""

    text_parts: list[str] = []
    reasoning_parts: list[str] = []
    for block in content:
        btype = _block_field(block, "type")
        if btype == "text":
            piece = _block_field(block, "text")
            if piece:
                text_parts.append(str(piece))
        elif btype == "reasoning":
            # summary is itself a list of {"type": "summary_text", "text": ...}.
            # Anthropic on Databricks sends it with text="" and a populated
            # `signature` — the model DID think, the provider just encrypted the
            # trace. That is the REDACTED case, reported by
            # `reasoning_was_redacted`; it is not a parse failure and not the
            # same as a model that never reasons.
            for summary in _block_field(block, "summary") or []:
                piece = _block_field(summary, "text")
                if piece:
                    reasoning_parts.append(str(piece))
        elif btype is None and isinstance(block, str):
            # Defensive: a bare string inside the list.
            text_parts.append(block)
    return "".join(text_parts), "".join(reasoning_parts)


def reasoning_was_redacted(content: Any) -> bool:
    """True when the model reasoned but the provider withheld the text.

    Anthropic Claude on Databricks (fable-5, opus-5) returns a ``reasoning``
    block whose ``summary`` carries ``text: ""`` plus a long opaque
    ``signature`` — proof that thinking happened, encrypted. Distinguishing this
    from "this model does not reason" is what lets the UI explain an empty
    reasoning panel instead of silently showing nothing.

    Usually FIXABLE by asking correctly, and the earlier claim here that it was
    not was measured wrong. Re-probed live 2026-08-05 against fable-5 and opus-5:
    with no ``thinking`` field the summary is ``text: ""`` + signature, but with
    ``thinking:{"type":"adaptive","display":"summarized"}`` the same question
    returns real summary text ("That's a classic one—the answer is 9."). The
    default is ``display: "omitted"``, so omitting the field is what produced the
    empty summary — not a provider that withholds it.

    So treat a true result as "we did not ask for the summary, or this model has
    none to give", and check the REQUEST before concluding the text is
    unavailable. ``reasoning_effort`` is still rejected outright, and
    ``thinking:{"type":"enabled"}`` is still rejected in favour of ``"adaptive"``.
    """
    if not isinstance(content, (list, tuple)):
        return False
    for block in content:
        if _block_field(block, "type") != "reasoning":
            continue
        for summary in _block_field(block, "summary") or []:
            if not _block_field(summary, "text") and _block_field(summary, "signature"):
                return True
    return False


def text_content(content: Any) -> str:
    """The answer text from Chat-Completions ``content`` (see
    :func:`split_content_blocks`); reasoning blocks are dropped."""
    return split_content_blocks(content)[0]


def split_message_content(message: Any) -> tuple[str, str]:
    """``(text, reasoning)`` from a Chat-Completions message OR a stream delta.

    Providers disagree on how to expose reasoning, so this handles all THREE
    shapes seen across the seeded Databricks catalogue (probed live 2026-08-05):

    1. ``content`` is a LIST with ``reasoning`` + ``text`` blocks —
       claude-fable-5, claude-opus-5.
    2. ``content`` is a LIST with only ``text`` blocks — the gemini-3.x family
       (3-1-pro, 3-1-flash-lite, 3-5-flash, 3-5-flash-lite). No reasoning, but
       still a list, so it hit the same crash.
    3. ``content`` is a plain ``str`` and the reasoning sits in a SIBLING
       ``reasoning_content`` field — databricks-inkling, kimi-k2-7-code. Their
       reasoning was previously dropped on the floor.

    Everything else is a plain string with no reasoning and takes the identity
    path. Handling this in one function means a model that starts returning
    reasoning needs no code change.
    """
    content = _block_field(message, "content")
    text, reasoning = split_content_blocks(content)
    # Convention 3: sibling field, alongside a normal string content.
    sibling = _block_field(message, "reasoning_content")
    if sibling:
        reasoning = f"{reasoning}{sibling}" if reasoning else str(sibling)
    if not reasoning and reasoning_was_redacted(content):
        # The model thought but the provider encrypted the trace. Say so, rather
        # than leaving the UI to imply the model did no reasoning at all.
        return text, REDACTED_REASONING
    return text, reasoning


def builtin_tool_outputs(response: Any) -> list[dict[str, Any]]:
    """Records of provider-side tools the model ran on its own."""
    return [
        {
            "id": getattr(item, "id", None),
            "status": getattr(item, "status", None),
            "type": getattr(item, "type", ""),
        }
        for item in (getattr(response, "output", None) or [])
        if getattr(item, "type", "") in BUILTIN_TOOL_CALL_TYPES
    ]
