"""Verify the real manager/transport boundary, not just a mocked LLM constructor."""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from openai import OpenAI

from src.core.llm.transport import LLM
from src.seeds.model_configs import DEFAULT_MODELS
from src.services.llm.manager import LLMManager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model",
    [
        "gemini-3.8-flash",
        "gemini-3.5-flash-lite",
    ],
)
@pytest.mark.parametrize("override", [None, "https://claude.example.com/v1"])
async def test_direct_requests_use_their_endpoint_key_and_bare_model(
    monkeypatch, model, override
):
    provider = DEFAULT_MODELS[model]["provider"]
    endpoint_env = f"{provider.upper()}_API_BASE"
    monkeypatch.delenv(endpoint_env, raising=False)
    monkeypatch.setenv("OPENAI_BASE_URL", "https://openai.example.com/v1")
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-provider-test-key")
    if override:
        monkeypatch.setenv(endpoint_env, override)
    session = AsyncMock()
    service = AsyncMock()
    service.get_model_config.return_value = DEFAULT_MODELS[model]
    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "lookup-1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{}"},
                    }
                ],
            }
            reason = "tool_calls"
        else:
            message = {"role": "assistant", "content": "Updated slide"}
            reason = "stop"
        return httpx.Response(
            200,
            json={
                "id": "test-response",
                "object": "chat.completion",
                "created": 1,
                "model": model,
                "choices": [{"index": 0, "message": message, "finish_reason": reason}],
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 4,
                    "total_tokens": 12,
                },
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(respond))
    try:
        with (
            patch("src.db.session.routed_scoped_session", return_value=session),
            patch("src.services.llm.manager.ModelConfigService", return_value=service),
            patch(
                "src.services.llm.manager.ApiKeysService.get_provider_api_key",
                new_callable=AsyncMock,
                return_value=f"{provider}-test-key",
            ) as key_lookup,
            patch(
                "openai.OpenAI",
                side_effect=lambda **kw: OpenAI(http_client=client, **kw),
            ),
        ):
            llm = await LLMManager.configure_kasal_llm(model, "workspace", None)
            result = llm.call(
                "Improve this slide using lookup",
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
        assert result == "Updated slide"
        key_lookup.assert_awaited_once_with(provider, group_id="workspace")
        assert llm.provider == provider
        assert len(requests) == 2
        base = {
            "anthropic": "https://api.anthropic.com/v1",
            "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
        }[provider]
        expected = (override or base) + "/chat/completions"
        for request in requests:
            assert str(request.url) == expected
            assert request.headers["authorization"] == f"Bearer {provider}-test-key"
            body = json.loads(request.content)
            assert body["model"] == model
            if provider == "anthropic" and model != "claude-haiku-4-5":
                assert "temperature" not in body
        assert any(
            m["role"] == "tool" for m in json.loads(requests[1].content)["messages"]
        )
    finally:
        client.close()


@pytest.mark.parametrize("provider", [None, "anthropic"])
def test_anthropic_never_defaults_to_openai_endpoint(provider):
    with pytest.raises(ValueError, match="explicit API endpoint") as error:
        LLM(model="anthropic/claude-sonnet-5", provider=provider, api_key="test-key")
    assert "test-key" not in str(error.value)


def test_anthropic_never_inherits_openai_key(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-provider-test-key")
    with pytest.raises(ValueError, match="its own API key"):
        LLM(model="anthropic/claude-sonnet-5", api_base="https://claude.example.com/v1")


def test_explicit_provider_also_strips_anthropic_prefix():
    llm = LLM(
        model="anthropic/claude-sonnet-5",
        provider="anthropic",
        api_key="test-key",
        api_base="https://claude.example.com/v1",
    )
    assert llm.model == "claude-sonnet-5"


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["gpt-6-astra", "gpt-6-sol", "gpt-6-luna"])
async def test_gpt6_uses_responses_with_reasoning_and_tools(model, monkeypatch):
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    service = AsyncMock()
    service.get_model_config.return_value = DEFAULT_MODELS[model]
    requests = []
    round_outputs = []

    def respond(request):
        requests.append(request)
        if len(requests) <= 2:
            number = len(requests)
            output = [
                {
                    "type": "reasoning",
                    "id": f"rs_{number}",
                    "summary": [],
                    "encrypted_content": f"opaque-reasoning-{number}",
                },
                {
                    "type": "function_call",
                    "id": f"fc_{number}",
                    "call_id": f"call_{number}",
                    "name": "lookup",
                    "arguments": "{}",
                    "status": "completed",
                },
            ]
            round_outputs.append(output)
            return httpx.Response(
                200,
                json={
                    "id": f"resp_{number}",
                    "object": "response",
                    "created_at": 1,
                    "status": "completed",
                    "model": model,
                    "output": output,
                    "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
                },
            )
        # Validate the conversation at the wire boundary, as the server does.
        inputs = json.loads(request.content)["input"]
        for output in round_outputs:
            call = output[1]
            assert call in inputs, "Tool result has no matching model function_call"
            index = inputs.index(call)
            assert inputs[index - 1] == output[0], "Reasoning must accompany the call"
            assert inputs[index + 1] == {
                "type": "function_call_output",
                "call_id": call["call_id"],
                "output": "ok",
            }
        return httpx.Response(
            200,
            json={
                "id": "resp_test",
                "object": "response",
                "created_at": 1,
                "status": "completed",
                "model": model,
                "output": [
                    {
                        "type": "message",
                        "id": "msg_test",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {"type": "output_text", "text": "Done", "annotations": []}
                        ],
                    }
                ],
                "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with (
            patch("src.db.session.routed_scoped_session", return_value=AsyncMock()),
            patch("src.services.llm.manager.ModelConfigService", return_value=service),
            patch(
                "src.services.llm.manager.ApiKeysService.get_provider_api_key",
                new_callable=AsyncMock,
                return_value="openai-test-key",
            ),
            patch(
                "openai.OpenAI",
                side_effect=lambda **kw: OpenAI(http_client=client, **kw),
            ),
        ):
            llm = await LLMManager.configure_kasal_llm(model, "workspace", None)
            assert llm.api == "responses"
            llm.reasoning_effort = "medium"
            assert (
                llm.call(
                    "Hello",
                    tools=[
                        {
                            "type": "function",
                            "function": {
                                "name": "lookup",
                                "parameters": {"type": "object", "properties": {}},
                            },
                        }
                    ],
                    available_functions={"lookup": lambda: "ok"},
                )
                == "Done"
            )
    body = json.loads(requests[0].content)
    assert str(requests[0].url) == "https://api.openai.com/v1/responses"
    assert body["model"] == model
    assert body["reasoning"]["effort"] == "medium"
    assert body["tools"][0]["type"] == "function"
    assert "temperature" not in body

    assert len(requests) == 3
