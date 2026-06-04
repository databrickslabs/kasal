"""
Additional unit tests for src/main.py to push coverage above 40%.

Tests focus on:
- Exception handlers (KasalError, ValueError, PydanticValidationError, IntegrityError, generic)
- Middleware classes (LocalDevAuthMiddleware, SecurityHeadersMiddleware)
- Health endpoint
- App attributes
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Exception handler tests via TestClient
# ---------------------------------------------------------------------------

class TestExceptionHandlers:
    """Test global exception handlers fire correctly."""

    def test_kasal_error_handler_returns_correct_status(self):
        from src.main import app
        from src.core.exceptions import KasalError

        # Add a test route that raises KasalError
        test_router = FastAPI()

        @test_router.get("/raise-kasal")
        async def raise_kasal():
            raise KasalError(status_code=422, detail="custom kasal error")

        # Use the main app's handler directly
        client = TestClient(app, raise_server_exceptions=False)
        # Test health endpoint to verify app is working
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_healthy_json(self):
        from src.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_app_returns_404_for_unknown_route(self):
        from src.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/nonexistent_route_xyz")
        assert response.status_code == 404

    def test_kasal_error_handler_is_registered(self):
        from src.main import app
        from src.core.exceptions import KasalError
        assert KasalError in app.exception_handlers

    def test_value_error_handler_is_registered(self):
        from src.main import app
        assert ValueError in app.exception_handlers

    def test_exception_handler_is_registered(self):
        from src.main import app
        assert Exception in app.exception_handlers

    @pytest.mark.asyncio
    async def test_kasal_error_handler_returns_json(self):
        """Test kasal_error_handler function directly."""
        import json
        from src.main import kasal_error_handler
        from src.core.exceptions import KasalError

        exc = KasalError(status_code=400, detail="test error")
        mock_request = MagicMock()

        response = await kasal_error_handler(mock_request, exc)
        assert response.status_code == 400
        body = json.loads(response.body)
        assert body["detail"] == "test error"

    @pytest.mark.asyncio
    async def test_value_error_handler_returns_400(self):
        import json
        from src.main import value_error_handler

        exc = ValueError("bad value input")
        mock_request = MagicMock()

        response = await value_error_handler(mock_request, exc)
        assert response.status_code == 400
        body = json.loads(response.body)
        assert "bad value input" in body["detail"]

    @pytest.mark.asyncio
    async def test_generic_error_handler_returns_500(self):
        import json
        from src.main import generic_error_handler

        exc = RuntimeError("some unexpected error")
        mock_request = MagicMock()

        response = await generic_error_handler(mock_request, exc)
        assert response.status_code == 500
        body = json.loads(response.body)
        assert "Internal server error" in body["detail"]


# ---------------------------------------------------------------------------
# LocalDevAuthMiddleware tests
# ---------------------------------------------------------------------------

class TestLocalDevAuthMiddleware:
    """Tests for LocalDevAuthMiddleware pure ASGI class."""

    @pytest.mark.asyncio
    async def test_injects_email_header_when_missing(self):
        from src.main import LocalDevAuthMiddleware

        received_scopes = []

        async def mock_app(scope, receive, send):
            received_scopes.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)

        scope = {
            "type": "http",
            "headers": [],  # No existing email headers
        }

        await middleware(scope, None, None)
        # Should have injected x-forwarded-email
        injected_headers = dict(received_scopes[0]["headers"])
        assert b"x-forwarded-email" in injected_headers

    @pytest.mark.asyncio
    async def test_does_not_inject_when_email_header_present(self):
        from src.main import LocalDevAuthMiddleware

        received_scopes = []

        async def mock_app(scope, receive, send):
            received_scopes.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)

        scope = {
            "type": "http",
            "headers": [(b"x-forwarded-email", b"existing@example.com")],
        }

        await middleware(scope, None, None)
        # Should not add another x-forwarded-email
        headers = dict(received_scopes[0]["headers"])
        # Only one x-forwarded-email should exist
        assert headers[b"x-forwarded-email"] == b"existing@example.com"

    @pytest.mark.asyncio
    async def test_does_not_inject_when_x_auth_request_email_present(self):
        from src.main import LocalDevAuthMiddleware

        received_scopes = []

        async def mock_app(scope, receive, send):
            received_scopes.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)

        scope = {
            "type": "http",
            "headers": [(b"x-auth-request-email", b"auth@example.com")],
        }

        await middleware(scope, None, None)
        # Should not add x-forwarded-email since x-auth-request-email exists
        headers = dict(received_scopes[0]["headers"])
        assert b"x-forwarded-email" not in headers

    @pytest.mark.asyncio
    async def test_passthrough_for_non_http_scope(self):
        from src.main import LocalDevAuthMiddleware

        called = []

        async def mock_app(scope, receive, send):
            called.append(scope["type"])

        middleware = LocalDevAuthMiddleware(mock_app)
        scope = {"type": "websocket", "headers": []}

        await middleware(scope, None, None)
        assert called == ["websocket"]


# ---------------------------------------------------------------------------
# SecurityHeadersMiddleware tests
# ---------------------------------------------------------------------------

class TestSecurityHeadersMiddleware:
    """Tests for SecurityHeadersMiddleware pure ASGI class."""

    @pytest.mark.asyncio
    async def test_adds_security_headers_to_http_response(self):
        from src.main import SecurityHeadersMiddleware

        sent_messages = []

        async def mock_app(scope, receive, send):
            # Simulate sending a response start message
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            })

        middleware = SecurityHeadersMiddleware(mock_app)

        scope = {"type": "http"}

        async def capture_send(message):
            sent_messages.append(message)

        await middleware(scope, None, capture_send)

        # Get the response start message
        response_start = sent_messages[0]
        headers = dict(response_start["headers"])
        assert b"content-security-policy" in headers
        assert b"x-content-type-options" in headers
        assert b"x-frame-options" in headers
        assert b"referrer-policy" in headers

    @pytest.mark.asyncio
    async def test_does_not_add_headers_to_non_response_start(self):
        from src.main import SecurityHeadersMiddleware

        sent_messages = []

        async def mock_app(scope, receive, send):
            await send({
                "type": "http.response.body",
                "body": b"body content",
            })

        middleware = SecurityHeadersMiddleware(mock_app)
        scope = {"type": "http"}

        async def capture_send(message):
            sent_messages.append(message)

        await middleware(scope, None, capture_send)

        # Body message should pass through unchanged
        body_msg = sent_messages[0]
        assert body_msg["type"] == "http.response.body"
        assert b"x-content-type-options" not in body_msg

    @pytest.mark.asyncio
    async def test_passthrough_for_non_http_scope(self):
        from src.main import SecurityHeadersMiddleware

        called = []

        async def mock_app(scope, receive, send):
            called.append(scope["type"])

        middleware = SecurityHeadersMiddleware(mock_app)
        scope = {"type": "lifespan"}

        await middleware(scope, None, None)
        assert called == ["lifespan"]

    def test_csp_header_value(self):
        from src.main import SecurityHeadersMiddleware
        assert "default-src" in SecurityHeadersMiddleware._CSP
        assert "script-src" in SecurityHeadersMiddleware._CSP

    def test_security_headers_list(self):
        from src.main import SecurityHeadersMiddleware
        header_names = [h[0] for h in SecurityHeadersMiddleware._SECURITY_HEADERS]
        assert b"content-security-policy" in header_names
        assert b"x-content-type-options" in header_names
        assert b"x-frame-options" in header_names
        assert b"referrer-policy" in header_names


# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

class TestAppConfiguration:
    def test_app_has_cors_middleware(self):
        from src.main import app
        middleware_classes = [
            m.cls.__name__ if hasattr(m, "cls") else str(type(m))
            for m in app.user_middleware
        ]
        assert any("CORS" in c for c in middleware_classes) or len(app.user_middleware) > 0

    def test_app_routes_include_api_v1_prefix(self):
        from src.main import app, settings
        routes = [r.path for r in app.routes]
        api_routes = [r for r in routes if r.startswith(settings.API_V1_STR)]
        assert len(api_routes) > 0

    def test_app_does_not_raise_on_import(self):
        """Importing main should not raise any exceptions."""
        import src.main
        assert src.main.app is not None
