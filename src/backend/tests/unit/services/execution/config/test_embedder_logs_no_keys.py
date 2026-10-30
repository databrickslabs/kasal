"""Configuring an embedder logs its provider and key names, never the API key.

These lines go to crew.log and from there into the run's execution logs.
"""

import logging
from unittest.mock import AsyncMock, patch

import pytest

from src.core.logger import LoggerManager
from src.services.execution.config.embedder_config_builder import EmbedderConfigBuilder

KEY = "sk-provider-key-value-123"


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


@pytest.fixture
def crew_log():
    logger = LoggerManager.get_instance().crew
    handler, level = _Capture(), logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    yield handler.lines
    logger.removeHandler(handler)
    logger.setLevel(level)


@pytest.mark.parametrize(
    "embedder",
    [
        {"provider": "openai", "config": {"model": "text-embedding-3-large"}},
        {"provider": "google", "config": {"model": "text-embedding-004"}},
        {"provider": "cohere", "config": {"api_key": KEY, "model": "m"}},
        {"config": {"api_key": KEY}},  # invalid: no provider
    ],
)
async def test_no_api_key_reaches_the_log(crew_log, embedder):
    builder = EmbedderConfigBuilder(
        {"group_id": "ws", "agents": [{"embedder_config": embedder}]}
    )
    with patch(
        "src.services.settings.api_keys.ApiKeysService.get_provider_api_key",
        AsyncMock(return_value=KEY),
    ):
        crew_kwargs, _, _ = await builder.configure_embedder({})
    assert crew_log, "expected the configuration to be logged"
    assert not [line for line in crew_log if KEY in line]
    if embedder.get("provider") in ("openai", "google"):
        assert crew_kwargs["embedder"]["config"]["api_key"] == KEY  # still used
        assert any("config_keys=['api_key', 'model']" in line for line in crew_log)
