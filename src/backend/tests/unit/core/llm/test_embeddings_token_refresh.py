"""The 401 retry in Databricks embeddings must actually refresh the token.

It called an undefined ``get_databricks_auth_headers()`` — never imported, here
or in llm_manager where the code used to live — so the retry raised NameError,
the surrounding ``except Exception`` swallowed it, and every expired token turned
into a silent ``None``. Nothing failed loudly, so nothing pointed at it.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.llm import embeddings


class _Response:
    def __init__(self, status, payload=None):
        self.status = status
        self._payload = payload or {}

    async def json(self):
        return self._payload

    async def text(self):
        return "error body"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    """Answers 401 first, then whatever comes next in ``responses``."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.headers_seen = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.headers_seen.append(dict(headers or {}))
        return self._responses.pop(0)


@pytest.fixture
def databricks_auth():
    auth = MagicMock()
    auth.token = "expired-token"
    auth.workspace_url = "https://example.com"
    auth.auth_method = "OBO"
    auth.get_headers.return_value = {"Authorization": "Bearer refreshed-token"}
    return auth


@pytest.mark.asyncio
async def test_401_refreshes_auth_and_retries(databricks_auth):
    session = _Session([
        _Response(401),
        _Response(200, {"data": [{"embedding": [0.1, 0.2, 0.3]}]}),
    ])

    @asynccontextmanager
    async def _shared_session():
        yield session

    with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock,
               return_value=databricks_auth), \
         patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"), \
         patch("src.utils.aiohttp_session.shared_client_session", _shared_session), \
         patch.object(embeddings, "_get_group_id_from_context", return_value="group-1"), \
         patch.object(embeddings.DatabricksURLUtils, "construct_llm_base_url",
                      return_value="https://example.com/serving-endpoints"), \
         patch.object(embeddings.DatabricksURLUtils, "extract_workspace_from_endpoint",
                      return_value="https://example.com"), \
         patch.object(embeddings.DatabricksURLUtils, "construct_embeddings_url",
                      return_value=("https://example.com/embeddings", "m")):
        result = await embeddings.get_embedding("hello")

    assert result == [0.1, 0.2, 0.3], "the retry after refresh must return the embedding"
    assert len(session.headers_seen) == 2, "a second request must actually be made"
    assert session.headers_seen[1]["Authorization"] == "Bearer refreshed-token"


@pytest.mark.asyncio
async def test_401_without_usable_auth_gives_up_quietly(databricks_auth):
    """No refreshed credential: return None rather than raising."""
    session = _Session([_Response(401)])

    @asynccontextmanager
    async def _shared_session():
        yield session

    auth_calls = [databricks_auth, None]

    async def _auth(*args, **kwargs):
        return auth_calls.pop(0)

    with patch("src.utils.databricks_auth.get_auth_context", _auth), \
         patch("src.utils.user_context.UserContext.get_user_token", return_value="tok"), \
         patch("src.utils.aiohttp_session.shared_client_session", _shared_session), \
         patch.object(embeddings, "_get_group_id_from_context", return_value="group-1"), \
         patch.object(embeddings.DatabricksURLUtils, "construct_llm_base_url",
                      return_value="https://example.com/serving-endpoints"), \
         patch.object(embeddings.DatabricksURLUtils, "extract_workspace_from_endpoint",
                      return_value="https://example.com"), \
         patch.object(embeddings.DatabricksURLUtils, "construct_embeddings_url",
                      return_value=("https://example.com/embeddings", "m")):
        result = await embeddings.get_embedding("hello")

    assert result is None
