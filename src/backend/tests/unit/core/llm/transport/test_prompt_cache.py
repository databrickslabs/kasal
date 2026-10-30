"""Claude prompt-cache breakpoints, asserted on the bytes each client sends.

Two transport paths serve Claude:

* Databricks-hosted Claude over the OpenAI-compatible chat API (OpenAI SDK).
* The native Messages API (``AnthropicClient`` over the Anthropic SDK).

Every other endpoint must keep receiving neither ``cache_control`` nor
CrewAI's ``cache_breakpoint`` hint (see ``test_cache_breakpoint.py``).
"""

import json
from copy import deepcopy

import httpx
import httpx2
import pytest
from anthropic import Anthropic
from openai import OpenAI

from src.core.llm.transport import LLM
from src.core.llm.transport.prompt_cache import (
    EPHEMERAL,
    cache_mode,
    mark_chat_messages,
)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup",
            "description": "Look something up",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
        },
    }
]


def _conversation():
    """The shape CrewAI's executor builds: hinted system + task, then a tool round."""
    return [
        {
            "role": "system",
            "content": "You are a careful analyst.",
            "cache_breakpoint": True,
        },
        {"role": "user", "content": "Task: find X", "cache_breakpoint": True},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": '{"q": "X"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_1", "content": "X is 42"},
    ]


def _walk(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def _markers(payload):
    return [node for node in _walk(payload) if "cache_control" in node]


def _hints(payload):
    return [node for node in _walk(payload) if "cache_breakpoint" in node]


# ── Databricks-hosted Claude (OpenAI-compatible chat) ────────────────────────


def _chat_body(usage):
    return {
        "id": "chat_1",
        "object": "chat.completion",
        "created": 0,
        "model": "databricks-claude-sonnet-4-5",
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": "ok"},
            }
        ],
        "usage": usage,
    }


def _run_chat(model, messages, tools=None, usage=None, use_async=False):
    sent = []

    def respond(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json=_chat_body(usage))

    llm = LLM(model=model, api_key="test-key", base_url="https://llm.invalid/v1")
    client = OpenAI(
        api_key="test-key",
        base_url="https://llm.invalid/v1",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    )
    object.__setattr__(llm, "_client", client)
    return llm, sent, respond


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
async def test_databricks_claude_gets_breakpoints_on_the_wire(use_async):
    messages = _conversation()
    original = deepcopy(messages)
    usage = {
        "prompt_tokens": 1200,
        "completion_tokens": 3,
        "total_tokens": 1203,
        "cache_read_input_tokens": 1000,
        "cache_creation_input_tokens": 150,
    }
    llm, sent, _ = _run_chat(
        "databricks/databricks-claude-sonnet-4-5", messages, usage=usage
    )
    answer = (
        await llm.acall(messages, tools=TOOLS)
        if use_async
        else llm.call(messages, tools=TOOLS)
    )

    assert answer == "ok"
    (payload,) = sent
    sent_messages = payload["messages"]
    # System prompt: string content becomes one marked text item.
    assert sent_messages[0]["content"] == [
        {
            "type": "text",
            "text": "You are a careful analyst.",
            "cache_control": EPHEMERAL,
        }
    ]
    # CrewAI's task hint is translated, not dropped.
    assert sent_messages[1]["content"] == [
        {"type": "text", "text": "Task: find X", "cache_control": EPHEMERAL}
    ]
    # Rolling tail: the conversation ends in a tool result, which has no
    # documented cache_control field on Databricks — the call that produced it
    # carries the marker instead.
    assert sent_messages[2]["tool_calls"][0]["cache_control"] == EPHEMERAL
    assert sent_messages[3] == {
        "role": "tool",
        "tool_call_id": "call_1",
        "content": "X is 42",
    }
    # Tool definitions are covered by the system marker (tools render first).
    assert not _markers(payload["tools"])
    assert len(_markers(payload)) == 3
    assert not _hints(payload)
    assert messages == original  # the caller's conversation is untouched

    metrics = llm.get_usage_metrics()
    assert metrics["cached_prompt_tokens"] == 1000
    assert metrics["cache_creation_tokens"] == 150


def test_databricks_claude_marks_a_trailing_user_turn():
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": [{"type": "text", "text": "hello"}]},
    ]
    llm, sent, _ = _run_chat("databricks/databricks-claude-opus-4-6", messages)
    llm.call(messages)
    (payload,) = sent
    assert payload["messages"][1]["content"] == [
        {"type": "text", "text": "hello", "cache_control": EPHEMERAL}
    ]
    assert len(_markers(payload)) == 2


@pytest.mark.parametrize(
    "model",
    [
        "databricks/databricks-meta-llama-3-3-70b-instruct",
        "databricks/databricks-gpt-oss-120b",
        "openai/claude-lookalike",  # not a Databricks Claude endpoint: no markers
    ],
)
def test_non_claude_chat_endpoints_get_neither_markers_nor_hints(model):
    messages = _conversation()
    llm, sent, _ = _run_chat(model, messages)
    llm.call(messages, tools=TOOLS)
    (payload,) = sent
    assert not _markers(payload)
    assert not _hints(payload)
    assert payload["messages"][0]["content"] == "You are a careful analyst."


def test_databricks_claude_respects_the_four_breakpoint_limit():
    marked = {"type": "text", "text": "doc", "cache_control": EPHEMERAL}
    messages = [
        {"role": "system", "content": [dict(marked), dict(marked), dict(marked)]},
        {"role": "user", "content": "q", "cache_breakpoint": True},
        {"role": "assistant", "content": "a"},
        {"role": "user", "content": "follow-up"},
    ]
    llm, sent, _ = _run_chat("databricks/databricks-claude-sonnet-4-5", messages)
    llm.call(messages)
    (payload,) = sent
    assert len(_markers(payload)) == 4
    # The system prompt was already marked by the caller; the one free slot
    # goes to the rolling tail ahead of the older hinted turn.
    assert payload["messages"][3]["content"][0]["cache_control"] == EPHEMERAL
    assert payload["messages"][1]["content"] == "q"
    assert not _hints(payload)


def test_responses_api_never_carries_markers():
    assert cache_mode("databricks", "databricks-claude-sonnet-4-5", "responses") is None


def test_mark_chat_messages_skips_empty_content():
    out = mark_chat_messages(
        [{"role": "system", "content": ""}, {"role": "user", "content": "  "}]
    )
    assert not _markers(out)


# ── Native Messages API (AnthropicClient) ────────────────────────────────────


def _native_llm(model="anthropic/claude-sonnet-4-5"):
    sent = []

    def respond(request):
        sent.append(json.loads(request.content))
        return httpx2.Response(
            200,
            json={
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude-sonnet-4-5",
                "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {
                    "input_tokens": 20,
                    "output_tokens": 3,
                    "cache_read_input_tokens": 900,
                    "cache_creation_input_tokens": 80,
                },
            },
        )

    llm = LLM(model=model, api_key="test-key", base_url="https://llm.invalid")
    llm.client._native = Anthropic(
        api_key="test-key",
        base_url="https://llm.invalid",
        max_retries=0,
        http_client=httpx2.Client(transport=httpx2.MockTransport(respond)),
    )
    return llm, sent


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
async def test_native_anthropic_gets_breakpoints_on_the_wire(use_async):
    messages = _conversation()
    original = deepcopy(messages)
    llm, sent = _native_llm()
    answer = (
        await llm.acall(messages, tools=TOOLS)
        if use_async
        else llm.call(messages, tools=TOOLS)
    )

    assert answer == "ok"
    (payload,) = sent
    assert payload["system"] == [
        {
            "type": "text",
            "text": "You are a careful analyst.",
            "cache_control": EPHEMERAL,
        }
    ]
    assert "cache_control" not in payload["tools"][0]
    first, second, third = payload["messages"]
    # CrewAI's hinted task prompt.
    assert first["content"][-1]["cache_control"] == EPHEMERAL
    assert "cache_control" not in second["content"][-1]
    # Rolling tail on the native tool_result block itself.
    assert third["content"][-1]["type"] == "tool_result"
    assert third["content"][-1]["cache_control"] == EPHEMERAL
    assert len(_markers(payload)) == 3
    assert not _hints(payload)
    assert messages == original

    metrics = llm.get_usage_metrics()
    assert metrics["prompt_tokens"] == 20 + 900 + 80
    assert metrics["cached_prompt_tokens"] == 900
    assert metrics["cache_creation_tokens"] == 80


def test_native_without_system_marks_the_last_tool():
    llm, sent = _native_llm()
    llm.call([{"role": "user", "content": "hi"}], tools=TOOLS)
    (payload,) = sent
    assert payload["tools"][-1]["cache_control"] == EPHEMERAL
    assert payload["messages"][0]["content"][-1]["cache_control"] == EPHEMERAL
    assert len(_markers(payload)) == 2


def test_native_replayed_signed_blocks_are_not_mutated():
    """A marker on a replayed assistant turn must not leak into the client's
    cache of signed blocks, which later rounds replay verbatim."""
    llm, sent = _native_llm()
    signed = [
        {"type": "thinking", "thinking": "", "signature": "sig"},
        {"type": "tool_use", "id": "call_1", "name": "lookup", "input": {"q": "X"}},
    ]
    llm.client._signed_blocks[("call_1",)] = signed
    snapshot = deepcopy(signed)
    llm.call(
        [
            {"role": "user", "content": "go"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": '{"q": "X"}'},
                    }
                ],
            },
        ]
    )
    (payload,) = sent
    tail = payload["messages"][-1]["content"]
    assert "cache_control" not in tail[0]  # thinking blocks cannot be marked
    assert tail[1]["cache_control"] == EPHEMERAL
    assert signed == snapshot
