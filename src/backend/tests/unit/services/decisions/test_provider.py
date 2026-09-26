import httpx
import pytest

from src.services.decisions import provider
from src.services.settings import engine_settings


@pytest.mark.asyncio
async def test_transport_uses_bearer_and_structured_decisions(monkeypatch):
    monkeypatch.setitem(
        engine_settings._snapshot, engine_settings.JEV_API_BASE, "https://example.com/"
    )
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
    monkeypatch.setitem(
        engine_settings._snapshot, engine_settings.JEV_API_BASE, "http://example.com"
    )
    with pytest.raises(ValueError, match="HTTPS"):
        await provider.evaluate("key", {}, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["", "   "])
async def test_no_built_in_endpoint(monkeypatch, value):
    """Unset means unconfigured: there is no default third-party URL."""
    monkeypatch.setitem(engine_settings._snapshot, engine_settings.JEV_API_BASE, value)
    assert provider.api_base() is None
    assert provider.is_configured() is False
    with pytest.raises(ValueError, match="not configured"):
        await provider.evaluate("key", {}, {})


def test_source_has_no_hard_coded_endpoint():
    import inspect

    source = inspect.getsource(provider)
    assert "https://api." not in source
    assert "os.environ" not in source
