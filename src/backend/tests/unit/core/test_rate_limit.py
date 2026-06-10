"""
Unit tests for src.core.rate_limit.RateLimitMiddleware (pure-ASGI rate limiter).

Two layers:
  * Pure logic (path exemption, identity keying, env toggle, guarded/disabled
    pass-through) — runs everywhere, no optional deps.
  * Active enforcement (429 on exceed, per-identity isolation) — guarded by
    ``importorskip("limits")`` so it runs in CI (where uv sync installs the
    ``limits`` package) and skips cleanly in a venv that hasn't synced it yet.
"""
import os
from unittest.mock import patch

import pytest

from src.core.rate_limit import RateLimitMiddleware, _rate_limit_enabled


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #
class RecordingApp:
    """Downstream ASGI app that records how many times it was invoked."""

    def __init__(self):
        self.calls = 0

    async def __call__(self, scope, receive, send):
        self.calls += 1
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def _scope(path, headers=None, client=("1.2.3.4", 12345)):
    return {"type": "http", "path": path, "headers": headers or [], "client": client}


async def _invoke(mw, scope):
    """Run the middleware against a scope; return (status, sent_messages)."""
    sent = []

    async def send(msg):
        sent.append(msg)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    await mw(scope, receive, send)
    status = next((m["status"] for m in sent if m["type"] == "http.response.start"), None)
    return status, sent


# --------------------------------------------------------------------------- #
#  Pure logic — no optional deps required
# --------------------------------------------------------------------------- #
class TestShouldLimit:
    @pytest.mark.parametrize("path", [
        "/api/v1/agents",
        "/api/v1/executions",
        "/api/generate",
    ])
    def test_api_paths_are_limited(self, path):
        assert RateLimitMiddleware._should_limit(path) is True

    @pytest.mark.parametrize("path", [
        "/",                         # SPA root
        "/assets/main.js",           # static asset
        "/static/index.css",
        "/docs/guide.md",
        "/health",                   # non-API health
        "/favicon.ico",
    ])
    def test_non_api_paths_are_exempt(self, path):
        assert RateLimitMiddleware._should_limit(path) is False

    @pytest.mark.parametrize("path", [
        "/api/v1/sse/executions/job-1/stream",   # SSE (contains /sse/)
        "/api/v1/runs/abc/stream",               # ends with /stream
        "/api/v1/health",                        # health under /api
        "/api/v1/healthcheck",
        "/api/v1/healthz",
    ])
    def test_sse_and_health_under_api_are_exempt(self, path):
        assert RateLimitMiddleware._should_limit(path) is False


class TestIdentity:
    def test_prefers_forwarded_email(self):
        scope = _scope("/api/x", headers=[(b"x-forwarded-email", b"alice@example.com")])
        assert RateLimitMiddleware._identity(scope) == "user:alice@example.com"

    def test_auth_request_email_fallback(self):
        scope = _scope("/api/x", headers=[(b"x-auth-request-email", b"bob@example.com")])
        assert RateLimitMiddleware._identity(scope) == "user:bob@example.com"

    def test_falls_back_to_client_ip(self):
        scope = _scope("/api/x", headers=[], client=("10.0.0.5", 999))
        assert RateLimitMiddleware._identity(scope) == "ip:10.0.0.5"

    def test_unknown_when_no_client(self):
        scope = _scope("/api/x", headers=[], client=None)
        assert RateLimitMiddleware._identity(scope) == "ip:unknown"

    def test_two_users_get_distinct_keys(self):
        a = RateLimitMiddleware._identity(_scope("/api/x", [(b"x-forwarded-email", b"a@x.com")]))
        b = RateLimitMiddleware._identity(_scope("/api/x", [(b"x-forwarded-email", b"b@x.com")]))
        assert a != b


class TestEnabledToggle:
    def test_default_enabled(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RATE_LIMIT_ENABLED", None)
            assert _rate_limit_enabled() is True

    @pytest.mark.parametrize("val", ["false", "0", "no", "off", "OFF", "False"])
    def test_disabled_values(self, val):
        with patch.dict(os.environ, {"RATE_LIMIT_ENABLED": val}):
            assert _rate_limit_enabled() is False

    @pytest.mark.parametrize("val", ["true", "1", "yes", "on"])
    def test_enabled_values(self, val):
        with patch.dict(os.environ, {"RATE_LIMIT_ENABLED": val}):
            assert _rate_limit_enabled() is True


class TestDisabledPassThrough:
    @pytest.mark.asyncio
    async def test_disabled_via_env_passes_through(self):
        app = RecordingApp()
        with patch.dict(os.environ, {"RATE_LIMIT_ENABLED": "false"}):
            mw = RateLimitMiddleware(app)
        assert mw._active is False
        # Even a limited API path is forwarded untouched.
        status, _ = await _invoke(mw, _scope("/api/v1/agents"))
        assert status == 200
        assert app.calls == 1

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through(self):
        app = RecordingApp()
        with patch.dict(os.environ, {"RATE_LIMIT_ENABLED": "false"}):
            mw = RateLimitMiddleware(app)

        async def send(msg):
            pass

        async def receive():
            return {"type": "lifespan.startup"}

        await mw({"type": "lifespan"}, receive, send)
        assert app.calls == 1  # forwarded


# --------------------------------------------------------------------------- #
#  Active enforcement — requires the optional `limits` package
# --------------------------------------------------------------------------- #
def _active_mw(app, limit="3/minute"):
    """Construct an ACTIVE middleware with a tiny in-memory limit."""
    with patch.dict(os.environ, {
        "RATE_LIMIT_ENABLED": "true",
        "RATE_LIMIT_DEFAULT": limit,
        "RATE_LIMIT_STORAGE_URI": "memory://",
    }):
        return RateLimitMiddleware(app)


class TestActiveEnforcement:
    def setup_method(self):
        pytest.importorskip("limits")

    @pytest.mark.asyncio
    async def test_allows_up_to_limit_then_429(self):
        app = RecordingApp()
        mw = _active_mw(app, "3/minute")
        assert mw._active is True

        scope = _scope("/api/v1/agents", [(b"x-forwarded-email", b"u@x.com")])
        statuses = [(await _invoke(mw, scope))[0] for _ in range(5)]
        assert statuses == [200, 200, 200, 429, 429]
        assert app.calls == 3  # blocked requests never reach the app

    @pytest.mark.asyncio
    async def test_per_identity_isolation(self):
        app = RecordingApp()
        mw = _active_mw(app, "2/minute")

        a = _scope("/api/v1/agents", [(b"x-forwarded-email", b"a@x.com")])
        b = _scope("/api/v1/agents", [(b"x-forwarded-email", b"b@x.com")])
        # Exhaust user A
        assert (await _invoke(mw, a))[0] == 200
        assert (await _invoke(mw, a))[0] == 200
        assert (await _invoke(mw, a))[0] == 429
        # User B has an independent budget
        assert (await _invoke(mw, b))[0] == 200

    @pytest.mark.asyncio
    async def test_sse_never_limited(self):
        app = RecordingApp()
        mw = _active_mw(app, "2/minute")
        sse = _scope("/api/v1/sse/x/stream", [(b"x-forwarded-email", b"u@x.com")])
        statuses = [(await _invoke(mw, sse))[0] for _ in range(6)]
        assert statuses == [200] * 6  # exempt path is never throttled

    @pytest.mark.asyncio
    async def test_429_response_shape(self):
        app = RecordingApp()
        mw = _active_mw(app, "1/minute")
        scope = _scope("/api/v1/agents", [(b"x-forwarded-email", b"u@x.com")])
        await _invoke(mw, scope)               # consume the single token
        status, sent = await _invoke(mw, scope)  # this one is blocked

        assert status == 429
        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k: v for k, v in start["headers"]}
        assert headers[b"content-type"] == b"application/json"
        assert headers[b"retry-after"] == b"60"
        body = next(m for m in sent if m["type"] == "http.response.body")["body"]
        assert b"Rate limit exceeded" in body
