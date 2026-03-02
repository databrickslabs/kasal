"""
Unit tests for get_group_context in dependencies.py.

Covers the broadened `except Exception` handler that returns an empty
GroupContext instead of crashing with 500 when group resolution fails.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import Request

from src.core.dependencies import get_group_context
from src.utils.user_context import GroupContext


def _make_request(state_attrs=None):
    """Build a minimal mock Request with a clean state."""
    request = MagicMock(spec=Request)
    request.state = MagicMock()
    # Ensure no cached group context
    if hasattr(request.state, '_group_context_cache'):
        del request.state._group_context_cache
    else:
        request.state._group_context_cache = None
        type(request.state).__dict__  # force attribute existence check
    # Use a fresh SimpleNamespace-like state
    request.state = type('State', (), {})()
    return request


class TestGetGroupContextExceptionHandling:
    """Test the broadened except Exception in get_group_context."""

    @pytest.mark.asyncio
    async def test_generic_exception_returns_empty_context(self):
        """When GroupContext.from_email raises a generic Exception, return empty GroupContext."""
        request = _make_request()

        with patch.object(
            GroupContext, 'from_email',
            new_callable=AsyncMock,
            side_effect=RuntimeError("Database connection lost"),
        ):
            result = await get_group_context(
                request=request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user=None,
                x_auth_request_access_token=None,
                x_group_id=None,
                x_group_domain=None,
            )

        assert isinstance(result, GroupContext)
        assert result.group_ids is None
        assert result.group_email is None

    @pytest.mark.asyncio
    async def test_value_error_raises_http_403(self):
        """ValueError from GroupContext.from_email still raises HTTPException(403)."""
        from fastapi import HTTPException

        request = _make_request()

        with patch.object(
            GroupContext, 'from_email',
            new_callable=AsyncMock,
            side_effect=ValueError("Access denied: User does not have access to group X"),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await get_group_context(
                    request=request,
                    x_forwarded_email=None,
                    x_forwarded_access_token=None,
                    x_auth_request_email="user@example.com",
                    x_auth_request_user=None,
                    x_auth_request_access_token=None,
                    x_group_id=None,
                    x_group_domain=None,
                )

            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_no_email_returns_empty_context(self):
        """When no email headers are present, return empty GroupContext."""
        request = _make_request()

        result = await get_group_context(
            request=request,
            x_forwarded_email=None,
            x_forwarded_access_token=None,
            x_auth_request_email=None,
            x_auth_request_user=None,
            x_auth_request_access_token=None,
            x_group_id=None,
            x_group_domain=None,
        )

        assert isinstance(result, GroupContext)
        assert result.group_ids is None

    @pytest.mark.asyncio
    async def test_successful_email_returns_valid_context(self):
        """When GroupContext.from_email succeeds, return the context and cache it."""
        request = _make_request()

        mock_context = GroupContext(
            group_ids=["group-1"],
            group_email="user@example.com",
            email_domain="example.com",
        )

        with patch.object(
            GroupContext, 'from_email',
            new_callable=AsyncMock,
            return_value=mock_context,
        ):
            result = await get_group_context(
                request=request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user=None,
                x_auth_request_access_token=None,
                x_group_id=None,
                x_group_domain=None,
            )

        assert result is mock_context
        # Check it was cached on request.state
        assert hasattr(request.state, '_group_context_cache')

    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached_context(self):
        """Second call with same email returns cached GroupContext."""
        request = _make_request()

        mock_context = GroupContext(
            group_ids=["group-1"],
            group_email="user@example.com",
        )

        # Pre-populate the cache
        cache_key = "group_context:user@example.com:None"
        request.state._group_context_cache = {cache_key: mock_context}

        result = await get_group_context(
            request=request,
            x_forwarded_email=None,
            x_forwarded_access_token=None,
            x_auth_request_email="user@example.com",
            x_auth_request_user=None,
            x_auth_request_access_token=None,
            x_group_id=None,
            x_group_domain=None,
        )

        # Should return cached value without calling from_email
        assert result is mock_context
