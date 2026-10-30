"""Slide refine falls back to the engine default model.

It passed an optional `model` straight into LLMManager.completion, so a request
without one failed with "Model configuration not found: None". The other
generators already default to DEFAULT_ENGINE_MODEL.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.decks import slide_refine
from src.utils.model_config import DEFAULT_ENGINE_MODEL


@pytest.mark.asyncio
async def test_ask_without_a_model_uses_the_engine_default() -> None:
    completion = AsyncMock(return_value=("", DEFAULT_ENGINE_MODEL))
    with patch.object(slide_refine.LLMManager, "completion", completion):
        try:
            await slide_refine._ask([{"role": "user", "content": "x"}], None)
        except Exception:  # parsing an empty reply may fail; the call is the point
            pass
    assert completion.await_args.kwargs["model"] == DEFAULT_ENGINE_MODEL
