"""LLMManager.completion must actually SEND the extra headers it is given.

It set `llm.extra_headers = ...` after construction; the transport builds the
request from its declared fields plus `additional_params`, so the attribute was
stored and never sent — the Kasal User-Agent telemetry never reached the wire.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.llm.transport.completion import OpenAICompletion
from src.services.llm.manager import LLMManager


@pytest.mark.asyncio
async def test_extra_headers_reach_the_request() -> None:
    llm = OpenAICompletion(
        model="test-model", api_key="k", base_url="https://example.com"
    )
    create = MagicMock()
    create.return_value.choices = [
        MagicMock(
            message=MagicMock(content="hi", tool_calls=None), finish_reason="stop"
        )
    ]
    create.return_value.usage = None
    llm._client = MagicMock()
    llm._client.chat.completions.create = create

    with (
        patch.object(LLMManager, "_get_group_id_from_context", return_value="g1"),
        patch.object(LLMManager, "configure_kasal_llm", AsyncMock(return_value=llm)),
    ):
        await LLMManager.completion(
            messages=[{"role": "user", "content": "x"}],
            model="test-model",
            extra_headers={"User-Agent": "kasal-test"},
        )

    assert create.call_args.kwargs["extra_headers"]["User-Agent"] == "kasal-test"
