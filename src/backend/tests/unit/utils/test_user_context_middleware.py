"""
Unit tests for UserContextMiddleware — the pure ASGI middleware that extracts
user/group context from HTTP headers without buffering StreamingResponse bodies.

Covers:
- HTTP scope: context extraction and passthrough
- Non-HTTP scope passthrough (websocket, lifespan)
- Error handling clears context
- Group context extraction failure handling
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.utils.user_context import (
    UserContextMiddleware,
    UserContext,
    GroupContext,
)


def _make_scope(scope_type="http", headers=None):
    """Build a minimal ASGI scope dict."""
    hdrs = []
    if headers:
        for k, v in headers.items():
            hdrs.append((k.lower().encode(), v.encode()))
    return {
        "type": scope_type,
        "method": "GET",
        "path": "/test",
        "query_string": b"",
        "headers": hdrs,
    }


class TestUserContextMiddlewareCall:
    """Test UserContextMiddleware.__call__ ASGI dispatch."""

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self):
        """Non-http scope types (websocket, lifespan) pass directly to inner app."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(scope_type="websocket")
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        inner_app.assert_awaited_once_with(scope, receive, send)

    @pytest.mark.asyncio
    async def test_http_scope_calls_inner_app(self):
        """HTTP scope extracts context and calls inner app."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(scope_type="http")
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock, return_value=None), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}):
            await middleware(scope, receive, send)

        inner_app.assert_awaited_once_with(scope, receive, send)

    @pytest.mark.asyncio
    async def test_http_scope_sets_group_context(self):
        """When group context is extracted, it is set via UserContext."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        mock_gc = GroupContext(
            group_ids=["grp-1"],
            group_email="user@example.com",
            email_domain="example.com",
        )

        scope = _make_scope(
            scope_type="http",
            headers={"x-forwarded-email": "user@example.com"},
        )
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock, return_value=mock_gc) as mock_extract, \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}), \
             patch.object(UserContext, 'set_group_context') as mock_set_gc, \
             patch.object(UserContext, 'clear_context'):
            await middleware(scope, receive, send)

        mock_set_gc.assert_called_once_with(mock_gc)

    @pytest.mark.asyncio
    async def test_http_scope_sets_user_token(self):
        """When access_token is in user context, it is set via UserContext."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(
            scope_type="http",
            headers={"x-forwarded-access-token": "test-token"},
        )
        receive = AsyncMock()
        send = AsyncMock()

        user_ctx = {"access_token": "test-token", "method": "GET", "url": "/test"}

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock, return_value=None), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value=user_ctx), \
             patch.object(UserContext, 'set_user_context') as mock_set_uc, \
             patch.object(UserContext, 'set_user_token') as mock_set_token, \
             patch.object(UserContext, 'clear_context'):
            await middleware(scope, receive, send)

        mock_set_uc.assert_called_once_with(user_ctx)
        mock_set_token.assert_called_once_with("test-token")

    @pytest.mark.asyncio
    async def test_error_in_inner_app_clears_context(self):
        """If the inner app raises, context is cleared and app still called."""
        inner_app = AsyncMock(side_effect=RuntimeError("app error"))
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(scope_type="http")
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock, return_value=None), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}), \
             patch.object(UserContext, 'clear_context') as mock_clear:
            # The middleware catches the error and calls app again
            # But since both calls raise, we need to handle that
            try:
                await middleware(scope, receive, send)
            except RuntimeError:
                pass

        # clear_context should be called in the finally block
        mock_clear.assert_called()

    @pytest.mark.asyncio
    async def test_group_context_extraction_greenlet_error(self):
        """Greenlet/async errors during group extraction are handled gracefully."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(
            scope_type="http",
            headers={"x-forwarded-email": "user@example.com"},
        )
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock,
                    side_effect=RuntimeError("greenlet_spawn has not been called")), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}), \
             patch.object(UserContext, 'clear_context'):
            await middleware(scope, receive, send)

        # Should still call inner app despite group extraction failure
        inner_app.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_group_context_extraction_generic_error(self):
        """Generic errors during group extraction are logged and handled."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(
            scope_type="http",
            headers={"x-forwarded-email": "user@example.com"},
        )
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock,
                    side_effect=ValueError("some error")), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}), \
             patch.object(UserContext, 'clear_context'):
            await middleware(scope, receive, send)

        inner_app.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_context_cleared_in_finally(self):
        """UserContext.clear_context is always called in the finally block."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = _make_scope(scope_type="http")
        receive = AsyncMock()
        send = AsyncMock()

        with patch('src.utils.user_context.extract_group_context_from_request',
                    new_callable=AsyncMock, return_value=None), \
             patch('src.utils.user_context.extract_user_context_from_request',
                    return_value={}), \
             patch.object(UserContext, 'clear_context') as mock_clear:
            await middleware(scope, receive, send)

        mock_clear.assert_called()

    @pytest.mark.asyncio
    async def test_lifespan_scope_passthrough(self):
        """Lifespan scope passes directly through without context extraction."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)

        scope = {"type": "lifespan"}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        inner_app.assert_awaited_once_with(scope, receive, send)
