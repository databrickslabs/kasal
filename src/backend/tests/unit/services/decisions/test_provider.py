import httpx
import pytest

from src.services.decisions import provider


@pytest.mark.asyncio
async def test_transport_uses_bearer_and_structured_decisions(monkeypatch):
    monkeypatch.setenv("JEV_API_BASE", "https://example.com")
    seen = []

    def handle(request):
        seen.append(request)
        return httpx.Response(200, json={"answers": {}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    monkeypatch.setattr(provider.httpx, "AsyncClient", lambda **kwargs: client)
    assert await provider.evaluate("key", {"request": "hello"}, {}) == {"answers": {}}
    assert str(seen[0].url) == "https://example.com/v1/systemone"
    assert seen[0].headers["Authorization"] == "Bearer key"
    assert b'"model":"jev-1.13.0"' in seen[0].content


@pytest.mark.asyncio
async def test_transport_rejects_insecure_endpoint(monkeypatch):
    monkeypatch.setenv("JEV_API_BASE", "http://example.com")
    with pytest.raises(ValueError, match="HTTPS"):
        await provider.evaluate("key", {}, {})
