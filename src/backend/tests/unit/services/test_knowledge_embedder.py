"""Unit tests for the shared knowledge embedder resolver."""
from unittest.mock import AsyncMock, patch

import pytest

from src.services import knowledge_embedder
from src.services.knowledge_embedder import resolve_knowledge_embedder_config

_GET_AUTH = "src.utils.databricks_auth.get_auth_context"


@pytest.mark.asyncio
async def test_resolves_databricks_when_auth_available():
    with patch(_GET_AUTH, new_callable=AsyncMock, return_value=object()):
        cfg = await resolve_knowledge_embedder_config(user_token="tok", group_id="g1")
    assert cfg["provider"] == "databricks"
    assert cfg["config"]["model"] == "databricks-gte-large-en"


@pytest.mark.asyncio
async def test_falls_back_to_ollama_when_no_auth():
    with patch(_GET_AUTH, new_callable=AsyncMock, return_value=None):
        cfg = await resolve_knowledge_embedder_config(group_id="g1")
    assert cfg["provider"] == "ollama"
    # Local fallback defaults to nomic-embed-text (matches the memory embedder;
    # SQLite stores vectors as TEXT so the 768-dim width is fine in local dev).
    assert cfg["config"]["model"] == "nomic-embed-text"
    assert cfg["config"]["url"].startswith("http")


@pytest.mark.asyncio
async def test_falls_back_to_ollama_when_auth_probe_raises():
    with patch(_GET_AUTH, new_callable=AsyncMock, side_effect=RuntimeError("boom")):
        cfg = await resolve_knowledge_embedder_config()
    assert cfg["provider"] == "ollama"


@pytest.mark.asyncio
async def test_ollama_model_is_configurable():
    with patch.object(knowledge_embedder, "KNOWLEDGE_OLLAMA_EMBED_MODEL", "custom-1024"):
        with patch(_GET_AUTH, new_callable=AsyncMock, return_value=None):
            cfg = await resolve_knowledge_embedder_config()
    assert cfg["config"]["model"] == "custom-1024"
