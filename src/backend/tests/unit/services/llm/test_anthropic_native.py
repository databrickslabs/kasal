"""Native Claude wire protocol, signed thinking replay and streaming tool rounds."""

import json
from unittest.mock import AsyncMock, patch

import httpx2
import pytest
from anthropic import Anthropic

from src.seeds.model_configs import DEFAULT_MODELS
from src.services.llm.manager import LLMManager


def sse_message(model, blocks, stop):
    events = [
        (
            "message_start",
            {
                "message": {
                    "id": "msg_test",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 8, "output_tokens": 0},
                }
            },
        )
    ]
    for i, block in enumerate(blocks):
        events.append(("content_block_start", {"index": i, "content_block": block}))
        events.append(("content_block_stop", {"index": i}))
    events.extend(
        [
            (
                "message_delta",
                {
                    "delta": {"stop_reason": stop, "stop_sequence": None},
                    "usage": {"output_tokens": 4},
                },
            ),
            ("message_stop", {}),
        ]
    )
    return "".join(
        f"event: {kind}\ndata: {json.dumps({'type': kind, **data})}\n\n"
        for kind, data in events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model,budget",
    [
        ("claude-haiku-4-5", 0),
        ("claude-haiku-4-5", 2048),
        ("claude-opus-5-5", 0),
        ("claude-fable-5-1", 0),
        ("claude-sonnet-5", 0),
    ],
)
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("override", [None, "https://claude.example.com/v1"])
async def test_native_messages_thinking_and_tools(
    monkeypatch, model, budget, stream, override
):
    monkeypatch.delenv("ANTHROPIC_API_BASE", raising=False)
    monkeypatch.setenv("OPENAI_BASE_URL", "https://wrong-provider.example.com/v1")
    if override:
        monkeypatch.setenv("ANTHROPIC_API_BASE", override)
    service = AsyncMock()
    service.get_model_config.return_value = DEFAULT_MODELS[model]
    requests = []
    signed = {
        "type": "thinking",
        "thinking": "Check a source first.",
        "signature": "test-signature",
    }

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            blocks = [
                signed,
                {"type": "tool_use", "id": "lookup-1", "name": "lookup", "input": {}},
            ]
            stop = "tool_use"
        else:
            blocks = [{"type": "text", "text": "Updated slide"}]
            stop = "end_turn"
        if json.loads(request.content).get("stream"):
            # Send actual deltas for text and thinking, as the API does.
            payload = sse_message(model, blocks, stop)
            for i, block in enumerate(blocks):
                field = {"text": "text", "thinking": "thinking"}.get(block["type"])
                if field:
                    value = block[field]
                    payload = payload.replace(
                        json.dumps(block), json.dumps({**block, field: ""})
                    )
                    marker = f'event: content_block_stop\ndata: {{"type": "content_block_stop", "index": {i}}}'
                    delta = {
                        "type": "content_block_delta",
                        "index": i,
                        "delta": {"type": f"{field}_delta", field: value},
                    }
                    payload = payload.replace(
                        marker,
                        f"event: content_block_delta\ndata: {json.dumps(delta)}\n\n{marker}",
                    )
            return httpx2.Response(
                200, text=payload, headers={"content-type": "text/event-stream"}
            )
        return httpx2.Response(
            200,
            json={
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": blocks,
                "stop_reason": stop,
                "stop_sequence": None,
                "usage": {"input_tokens": 8, "output_tokens": 4},
            },
        )

    with httpx2.Client(transport=httpx2.MockTransport(respond)) as client:
        with (
            patch("src.db.session.routed_scoped_session", return_value=AsyncMock()),
            patch("src.services.llm.manager.ModelConfigService", return_value=service),
            patch(
                "src.services.llm.manager.ApiKeysService.get_provider_api_key",
                new_callable=AsyncMock,
                return_value="anthropic-test-key",
            ),
            patch(
                "anthropic.Anthropic",
                side_effect=lambda **kw: Anthropic(http_client=client, **kw),
            ),
        ):
            llm = await LLMManager.configure_kasal_llm(model, "workspace", None)
            llm.stream = stream
            llm.thinking_budget_tokens = budget
            answer = llm.call(
                [
                    {"role": "system", "content": "Be accurate"},
                    {"role": "user", "content": "Improve this slide"},
                ],
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "parameters": {"type": "object", "properties": {}},
                        },
                    }
                ],
                available_functions={"lookup": lambda: "Verified content"},
            )
    assert answer == "Updated slide"
    assert len(requests) == 2
    expected = (override or "https://api.anthropic.com").removesuffix(
        "/v1"
    ) + "/v1/messages"
    for request in requests:
        assert str(request.url) == expected
        assert request.headers["x-api-key"] == "anthropic-test-key"
        body = json.loads(request.content)
        assert body["model"] == model
        if "haiku" in model and not budget:
            assert "thinking" not in body
            assert body["temperature"] == 0.7
        else:
            assert body["thinking"]["type"] == (
                "enabled" if "haiku" in model else "adaptive"
            )
            assert "temperature" not in body
        assert body["tools"][0]["input_schema"]["type"] == "object"
        assert "response_format" not in body
    followup = json.loads(requests[1].content)
    assert followup["messages"][1]["content"][0] == signed
    assert followup["messages"][2]["content"][0] == {
        "type": "tool_result",
        "tool_use_id": "lookup-1",
        "content": "Verified content",
    }
    assert "Check a source first." in llm._reasoning_text
