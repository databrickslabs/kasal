"""Security headers on what ``src/entrypoint.py`` serves: the SPA page too.

``build_app`` puts ``SPAMiddleware`` outermost, and it answers every page route
(``/``, ``/workflows``, ...) itself. The security headers used to be added only
by the inner ``SecurityHeadersMiddleware``, so the HTML document, the one
response a Content-Security-Policy acts on, carried none of them.

These tests run the real composition: ``build_app()`` over the real
``src.main.app``, against a throwaway frontend build, and read the policy off
the responses rather than off a constant.
"""

import importlib
import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

APP_ROOT = Path(__file__).resolve().parents[3]
API_PATH = "/api/v1/__no_such_route"


def _directives(policy: str) -> dict[str, list[str]]:
    parsed = {}
    for part in policy.split(";"):
        tokens = part.split()
        if tokens:
            parsed[tokens[0]] = tokens[1:]
    return parsed


@pytest.fixture
def entrypoint():
    spec = importlib.util.spec_from_file_location(
        "kasal_entrypoint_under_test", APP_ROOT / "entrypoint.py"
    )
    module = importlib.util.module_from_spec(spec)
    # Its import reconfigures root logging; keep that out of the test run.
    with patch("src.config.logging.configure_early_logging"):
        spec.loader.exec_module(module)
    yield module
    sys.modules.pop("kasal_entrypoint_under_test", None)


@pytest.fixture
def served(tmp_path, monkeypatch, entrypoint):
    """build_app() over a private copy of the real app's routes and middleware."""
    (tmp_path / "index.html").write_text("<!doctype html><title>Kasal</title>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "index-abc.js").write_text("export {};")
    monkeypatch.setenv("FRONTEND_STATIC_DIR", str(tmp_path))

    # The subject is entrypoint.py (beside the other test_entrypoint_* files);
    # src.main is only the app it wraps. build_app mounts, routes and wraps that
    # shared app object, so give it copies and nothing leaks into other tests.
    app = importlib.import_module("src.main").app
    monkeypatch.setattr(app, "user_middleware", list(app.user_middleware))
    monkeypatch.setattr(app, "middleware_stack", None)
    monkeypatch.setattr(app.router, "routes", list(app.router.routes))
    return entrypoint.build_app()


async def _get(app, path: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        return await client.get(path)


@pytest.fixture
async def policy(served) -> dict[str, list[str]]:
    """The CSP as served, parsed into directive -> sources."""
    response = await _get(served, "/")
    values = response.headers.get_list("content-security-policy")
    assert len(values) == 1, values
    return _directives(values[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/", "/workflows", "/runs/abc/details"])
async def test_the_spa_document_carries_the_security_headers(served, path):
    page = await _get(served, path)
    api = await _get(served, API_PATH)
    assert page.status_code == 200
    assert page.headers["content-type"].startswith("text/html")
    csp = page.headers.get_list("content-security-policy")
    assert len(csp) == 1, csp
    assert csp == api.headers.get_list("content-security-policy")
    assert page.headers["x-content-type-options"] == "nosniff"
    assert page.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    # Kasal is embedded cross-origin by the Databricks workspace, so the page
    # is not framing-restricted (no X-Frame-Options, no frame-ancestors).
    assert "x-frame-options" not in page.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/assets/index-abc.js", API_PATH])
async def test_static_assets_and_api_responses_still_carry_them(served, path):
    page = await _get(served, "/")
    response = await _get(served, path)
    assert response.headers.get_list(
        "content-security-policy"
    ) == page.headers.get_list("content-security-policy")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "SAMEORIGIN"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


@pytest.mark.asyncio
async def test_the_health_check_reaches_the_api_not_the_page(served):
    response = await _get(served, "/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
    assert response.headers["x-frame-options"] == "SAMEORIGIN"


def test_the_spa_middleware_is_registered_once(served, entrypoint):
    spa = [m for m in served.user_middleware if m.cls is entrypoint.SPAMiddleware]
    assert len(spa) == 1


@pytest.mark.asyncio
async def test_images_load_only_from_the_app_and_its_map_tiles(policy):
    """Remote images in model output must not load by themselves: img-src
    names no scheme-wide or wildcard source, only what the app itself needs."""
    assert sorted(policy["img-src"]) == sorted(
        [
            "'self'",
            "data:",
            "blob:",
            "https://tile.openstreetmap.org",
            "https://*.tile.openstreetmap.org",
        ]
    )


@pytest.mark.asyncio
async def test_the_policy_allows_what_the_app_loads_and_nothing_wider(policy):
    assert policy["default-src"] == ["'self'"]
    # Inline: the sandboxed srcdoc previews inherit this policy and run their
    # own inline scripts. wasm: the run-PDF layout engine.
    assert sorted(policy["script-src"]) == sorted(
        ["'self'", "'unsafe-inline'", "'wasm-unsafe-eval'"]
    )
    assert sorted(policy["style-src"]) == sorted(["'self'", "'unsafe-inline'"])
    assert sorted(policy["font-src"]) == sorted(["'self'", "data:"])
    assert sorted(policy["connect-src"]) == sorted(
        [
            "'self'",
            "data:",
            "https://fonts.gstatic.com",
            "https://login.microsoftonline.com",
        ]
    )
    assert policy["frame-src"] == ["'self'"]
    assert policy["object-src"] == ["'none'"]
    assert policy["base-uri"] == ["'self'"]
    assert policy["form-action"] == ["'self'"]
    assert "frame-ancestors" not in policy
    for sources in policy.values():
        assert not {"*", "https:", "http:", "'unsafe-eval'"} & set(sources)
