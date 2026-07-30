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
