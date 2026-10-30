"""CrewAI cache hints must not leak into OpenAI-compatible HTTP requests."""

import json
from copy import deepcopy

import httpx
import pytest
from openai import OpenAI

from src.core.llm.transport.completion import OpenAICompletion


@pytest.mark.parametrize("api", ["responses", "completions"])
@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
async def test_cache_hints_are_removed_at_the_http_boundary(api, use_async):
    messages = [
        {"role": "system", "content": "Be helpful", "cache_breakpoint": True},
        {"role": "developer", "content": "Be concise", "cache_breakpoint": False},
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text" if api == "responses" else "text",
                    "text": "Explain cache_breakpoint",
                }
            ],
            "cache_breakpoint": None,
        },
    ]
    if api == "responses":
        messages.extend(
            [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": "Checking",
                    "phase": "commentary",
                    "cache_breakpoint": True,
                },
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": [],
                    "encrypted_content": "opaque",
                },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": '{"cache_breakpoint": true}',
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": '{"cache_breakpoint": true}',
                },
            ]
        )
    else:
        messages.extend(
            [
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": "{}"},
                        }
                    ],
                    "cache_breakpoint": True,
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "found"},
            ]
        )
    original = deepcopy(messages)
    expected = deepcopy(messages)
    for item in expected:
        item.pop("cache_breakpoint", None)
    requests = []

    def respond(request):
        requests.append(request)
        payload = json.loads(request.content)
        field = "input" if api == "responses" else "messages"
        assert payload[field] == expected
        if api == "responses":
            body = {
                "id": "resp_1",
                "object": "response",
                "created_at": 0,
                "model": "gpt-4o",
                "status": "completed",
                "output": [
                    {
                        "id": "msg_1",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {"type": "output_text", "text": "ok", "annotations": []}
                        ],
                    }
                ],
            }
        else:
            body = {
                "id": "chat_1",
                "object": "chat.completion",
                "created": 0,
                "model": "gpt-4o",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
            }
        return httpx.Response(200, json=body)

    with OpenAI(
        api_key="test-key",
        base_url="https://llm.invalid/v1",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as client:
        llm = OpenAICompletion(model="gpt-4o", api=api)
        object.__setattr__(llm, "_client", client)
        answer = await llm.acall(messages) if use_async else llm.call(messages)

    assert answer == "ok"
    assert len(requests) == 1
    assert requests[0].url.path == (
        "/v1/responses" if api == "responses" else "/v1/chat/completions"
    )
    assert messages == original
