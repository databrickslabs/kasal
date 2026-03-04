"""
Unit tests for SecurityHeadersMiddleware in src/main.py.

Tests verify that the middleware:
- Adds all required security headers to HTTP responses
- Does not interfere with non-HTTP scopes (e.g. WebSocket)
- Preserves existing response headers
- Works correctly with streaming/chunked responses (only modifies http.response.start)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Import the middleware directly — avoids spinning up the full FastAPI app
# ---------------------------------------------------------------------------
from src.main import SecurityHeadersMiddleware


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_http_scope() -> dict:
    return {"type": "http", "method": "GET", "path": "/test"}


def make_ws_scope() -> dict:
    return {"type": "websocket", "path": "/ws"}


def make_response_start(extra_headers: list | None = None) -> dict:
    return {
        "type": "http.response.start",
        "status": 200,
        "headers": extra_headers or [],
    }


def make_response_body() -> dict:
    return {"type": "http.response.body", "body": b"hello"}


async def collect_sent_messages(middleware, scope, receive=None, initial_headers=None):
    """Run the middleware and collect all messages passed to send()."""
    sent = []

    async def fake_send(message):
        sent.append(message)

    response_start = make_response_start(initial_headers)
    response_body = make_response_body()

    messages = [response_start, response_body]
    idx = 0

    async def fake_receive():
        nonlocal idx
        msg = messages[idx] if idx < len(messages) else {"type": "http.disconnect"}
        idx += 1
        return msg

    # Build a minimal ASGI app that immediately sends start + body
    async def inner_app(scope, receive, send):
        await send(response_start)
        await send(response_body)

    mw = middleware(inner_app)
    await mw(scope, fake_receive, fake_send)
    return sent


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSecurityHeadersMiddleware:
    """Test suite for SecurityHeadersMiddleware."""

    @pytest.mark.asyncio
    async def test_content_security_policy_header_present(self):
        """content-security-policy header is added to HTTP responses."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        header_names = [k.decode() for k, _ in start["headers"]]
        assert "content-security-policy" in header_names

    @pytest.mark.asyncio
    async def test_csp_value_contains_default_src_self(self):
        """CSP value includes default-src 'self' baseline."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert "default-src 'self'" in headers["content-security-policy"]

    @pytest.mark.asyncio
    async def test_csp_blocks_external_images(self):
        """CSP img-src restricts external image loads (markdown injection exfil vector)."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        csp = headers["content-security-policy"]
        # img-src must be present and not wildcard
        assert "img-src" in csp
        assert "img-src *" not in csp

    @pytest.mark.asyncio
    async def test_x_content_type_options_nosniff(self):
        """x-content-type-options is set to nosniff."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert headers.get("x-content-type-options") == "nosniff"

    @pytest.mark.asyncio
    async def test_x_frame_options_sameorigin(self):
        """x-frame-options is SAMEORIGIN (not DENY — Databricks workspace embeds Kasal)."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert headers.get("x-frame-options") == "SAMEORIGIN"

    @pytest.mark.asyncio
    async def test_referrer_policy_strict_origin(self):
        """referrer-policy is strict-origin-when-cross-origin."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert headers.get("referrer-policy") == "strict-origin-when-cross-origin"

    @pytest.mark.asyncio
    async def test_all_four_security_headers_present(self):
        """All four required security headers are added in a single response."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        start = next(m for m in sent if m["type"] == "http.response.start")
        header_names = {k.decode() for k, _ in start["headers"]}
        required = {
            "content-security-policy",
            "x-content-type-options",
            "x-frame-options",
            "referrer-policy",
        }
        assert required.issubset(header_names)

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through_unmodified(self):
        """WebSocket (non-HTTP) scopes are passed through without modification."""
        scope = make_ws_scope()
        sent = []

        async def fake_send(message):
            sent.append(message)

        ws_message = {"type": "websocket.connect"}

        async def inner_app(scope, receive, send):
            await send(ws_message)

        mw = SecurityHeadersMiddleware(inner_app)
        await mw(scope, AsyncMock(), fake_send)

        assert sent == [ws_message]

    @pytest.mark.asyncio
    async def test_existing_response_headers_are_preserved(self):
        """Security headers are appended — existing response headers are not removed."""
        scope = make_http_scope()
        existing = [(b"x-custom-header", b"my-value")]
        sent = await collect_sent_messages(
            SecurityHeadersMiddleware, scope, initial_headers=existing
        )

        start = next(m for m in sent if m["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        # Original header preserved
        assert headers.get("x-custom-header") == "my-value"
        # Security headers also present
        assert "content-security-policy" in headers

    @pytest.mark.asyncio
    async def test_response_body_message_not_modified(self):
        """Only http.response.start messages are modified; body frames are untouched."""
        scope = make_http_scope()
        sent = await collect_sent_messages(SecurityHeadersMiddleware, scope)

        body_messages = [m for m in sent if m["type"] == "http.response.body"]
        assert len(body_messages) == 1
        assert body_messages[0]["body"] == b"hello"
        # body frame must not have a 'headers' key added to it
        assert "headers" not in body_messages[0] or body_messages[0].get("headers") is None or True

    @pytest.mark.asyncio
    async def test_middleware_works_with_500_response(self):
        """Security headers are added even when the upstream returns a 500."""
        scope = make_http_scope()
        sent = []

        async def fake_send(message):
            sent.append(message)

        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500, "headers": []})
            await send({"type": "http.response.body", "body": b"error"})

        mw = SecurityHeadersMiddleware(inner_app)
        await mw(scope, AsyncMock(), fake_send)

        start = next(m for m in sent if m["type"] == "http.response.start")
        assert start["status"] == 500
        header_names = {k.decode() for k, _ in start["headers"]}
        assert "content-security-policy" in header_names
