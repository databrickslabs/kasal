"""Two workspaces in one process each get THEIR OWN third-party key.

Regression for the cross-tenant key bleed: ToolFactory used to pre-load
SERPER/PERPLEXITY/OPENAI keys into ``os.environ`` and the tools read the env
before the database, so whichever workspace initialised last supplied the key
for everyone. Keys now travel explicitly, from the workspace's ApiKeysService to
the tool instance, and the process environment is never touched.
"""

import os
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.tools import serper_search
from src.services.tools.image_generation import ImageGenerationTool
from src.services.tools.perplexity_tool import PerplexitySearchTool
from src.services.tools.serper_search import SerperDevTool
from src.services.tools.tool_factory import ToolFactory

KEYS = {
    "ws-a": {
        "SERPER_API_KEY": "serper-A",
        "PERPLEXITY_API_KEY": "pplx-A",
        "OPENAI_API_KEY": "openai-A",
    },
    "ws-b": {
        "SERPER_API_KEY": "serper-B",
        "PERPLEXITY_API_KEY": "pplx-B",
        "OPENAI_API_KEY": "openai-B",
    },
}
_SECRET_ENV = ("SERPER_API_KEY", "PERPLEXITY_API_KEY", "OPENAI_API_KEY")


def _keys_service(group_id: str) -> MagicMock:
    """An ApiKeysService stand-in scoped to one workspace."""

    async def find_by_name(name):
        value = KEYS[group_id].get(name)
        return MagicMock(encrypted_value=value) if value else None

    svc = MagicMock()
    svc.group_id = group_id
    svc.find_by_name = AsyncMock(side_effect=find_by_name)
    return svc


def _factory(group_id: str) -> ToolFactory:
    f = ToolFactory({"group_id": group_id}, api_keys_service=_keys_service(group_id))
    for title in ("SerperDevTool", "PerplexityTool", "Image Generation Tool"):
        info = MagicMock(title=title, id=hash(title) % 1000, config={})
        f._available_tools[title] = info
    return f


@pytest.fixture(autouse=True)
def _clean_env_and_plain_decrypt(monkeypatch):
    for name in _SECRET_ENV:
        monkeypatch.delenv(name, raising=False)
    with patch(
        "src.services.tools.tool_factory.EncryptionUtils.decrypt_value",
        side_effect=lambda v: v,
    ):
        yield


def test_interleaved_workspaces_each_build_tools_with_their_own_keys():
    fa, fb = _factory("ws-a"), _factory("ws-b")

    serper_a = fa.create_tool("SerperDevTool")
    serper_b = fb.create_tool("SerperDevTool")
    pplx_b = fb.create_tool("PerplexityTool")
    pplx_a = fa.create_tool("PerplexityTool")
    img_a = fa.create_tool("Image Generation Tool")
    img_b = fb.create_tool("Image Generation Tool")
    serper_a2 = fa.create_tool("SerperDevTool")

    assert isinstance(serper_a, SerperDevTool)
    assert (serper_a.api_key, serper_b.api_key, serper_a2.api_key) == (
        "serper-A",
        "serper-B",
        "serper-A",
    )
    assert isinstance(pplx_a, PerplexitySearchTool)
    assert (pplx_a._api_key, pplx_b._api_key) == ("pplx-A", "pplx-B")
    assert isinstance(img_a, ImageGenerationTool)
    assert (img_a.api_key, img_b.api_key) == ("openai-A", "openai-B")
    # And nothing was parked in the environment every workspace shares.
    for name in _SECRET_ENV:
        assert name not in os.environ


def test_concurrent_searches_send_each_workspaces_own_key():
    """Requests running at the same time never swap keys."""
    tools = {
        g: SerperDevTool(api_key=KEYS[g]["SERPER_API_KEY"]) for g in ("ws-a", "ws-b")
    }
    sent: list[tuple[str, str]] = []
    barrier = threading.Barrier(2)

    def fake_http_json(url, payload, headers, timeout):
        barrier.wait(timeout=5)  # both requests in flight at once
        sent.append((payload["q"], headers["X-API-KEY"]))
        return {}

    with patch.object(serper_search, "_http_json", side_effect=fake_http_json):
        threads = [
            threading.Thread(target=tools[g]._make_api_request, args=(g, "search"))
            for g in ("ws-a", "ws-b")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

    assert sorted(sent) == [("ws-a", "serper-A"), ("ws-b", "serper-B")]


def test_a_key_left_in_the_environment_is_never_used(monkeypatch):
    """Even if something put a key in os.environ, a workspace without one fails."""
    monkeypatch.setenv("SERPER_API_KEY", "someone-elses-key")
    tool = SerperDevTool()
    with pytest.raises(ValueError, match="No Serper API key"):
        tool._make_api_request("q", "search")

    monkeypatch.setenv("PERPLEXITY_API_KEY", "someone-elses-key")
    with pytest.raises(ValueError, match="Perplexity API key is required"):
        PerplexitySearchTool()

    monkeypatch.setenv("OPENAI_API_KEY", "someone-elses-key")
    assert "No API key configured" in ImageGenerationTool()._run(
        image_description="a cat"
    )
