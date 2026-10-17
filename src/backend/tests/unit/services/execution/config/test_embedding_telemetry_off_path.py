"""A Databricks embedding call must not wait for its usage telemetry.

The memory embedder used to send its logfood GET inline, with ``requests`` and a
5 s timeout, after every embedding batch and before returning the vectors. That
request is telemetry: it goes to the usage worker in ``core/llm/usage_telemetry``
and the embedding returns as soon as the vectors are in.
"""

import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.config.embedder_config_builder import EmbedderConfigBuilder


async def _databricks_embedder():
    config = {
        "agents": [
            {
                "role": "a",
                "embedder_config": {
                    "provider": "databricks",
                    "config": {"model": "databricks-gte-large-en"},
                },
            }
        ]
    }
    builder = EmbedderConfigBuilder(config, user_token="user-tok")
    with (
        patch(
            "src.utils.databricks_auth.get_databricks_auth_headers",
            new_callable=AsyncMock,
            return_value=({"Authorization": "Bearer t"}, None),
        ),
        patch.object(
            builder,
            "_get_databricks_endpoint",
            new_callable=AsyncMock,
            return_value="https://example.com",
        ),
    ):
        _, embedder, _ = await builder.configure_embedder({})
    assert embedder is not None
    return embedder


def _embedding_response():
    response = MagicMock(status_code=200)
    response.json.return_value = {
        "data": [{"embedding": [0.1, 0.2, 0.3]}],
        "usage": {"prompt_tokens": 3, "total_tokens": 3},
    }
    return response


@pytest.mark.asyncio
async def test_embedding_returns_while_the_telemetry_request_is_stuck():
    embedder = await _databricks_embedder()
    started = threading.Event()
    release = threading.Event()

    def _stuck_send(**kwargs):
        started.set()
        release.wait(3)  # a workspace API that does not answer

    with (
        patch("requests.post", return_value=_embedding_response()),
        patch("src.utils.telemetry.send_logfood_telemetry_sync", _stuck_send),
    ):
        try:
            began = time.monotonic()
            vectors = embedder(["hello"])
            elapsed = time.monotonic() - began
        finally:
            release.set()
        # Still sent — by the usage worker, not by the embedding call.
        assert started.wait(5)

    assert vectors == [[0.1, 0.2, 0.3]]
    assert elapsed < 1.0, f"embedding waited {elapsed:.2f}s for telemetry"
