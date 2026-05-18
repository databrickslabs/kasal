"""
Comprehensive unit tests for src/dependencies/admin_auth.py

Targets: _create_user_from_forwarded_email, get_current_user_from_email,
         require_authenticated_user, get_authenticated_user, get_admin_user

Goal: push coverage from 17.7% to 50%+
"""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException


# ============================================================================
# Helpers
# ============================================================================

def _make_group_context(email=None, user_role=None, highest_role=None, access_token=None):
    ctx = MagicMock()
    ctx.group_email = email
    ctx.user_role = user_role
    ctx.highest_role = highest_role
    ctx.access_token = access_token
    return ctx


def _make_user(email="user@example.com", is_system_admin=False, role="regular"):
    from unittest.mock import MagicMock
    user = MagicMock()
    user.email = email
    user.is_system_admin = is_system_admin
    user.role = role
    return user


# ============================================================================
# _create_user_from_forwarded_email
# ============================================================================

class TestCreateUserFromForwardedEmail:

    @pytest.mark.asyncio
    async def test_handles_exception_and_returns_none(self):
        from src.dependencies.admin_auth import _create_user_from_forwarded_email

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=Exception("DB error"))
        mock_session.rollback = AsyncMock()

        result = await _create_user_from_forwarded_email(mock_session, "bad@example.com")

        assert result is None
        assert mock_session.rollback.called

    @pytest.mark.asyncio
    async def test_returns_existing_user_when_found(self):
        """Test that existing user is returned directly with updated last_login."""
        from src.dependencies.admin_auth import _create_user_from_forwarded_email
        from src.models.user import User

        # Use a real User instance (no hashed_password needed since it's not in the model)
        mock_session = AsyncMock()
        existing = MagicMock(spec=User)
        existing.email = "exist@example.com"

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        result = await _create_user_from_forwarded_email(mock_session, "exist@example.com")

        assert result is existing


# ============================================================================
# get_current_user_from_email
# ============================================================================

class TestGetCurrentUserFromEmail:

    @pytest.mark.asyncio
    async def test_returns_none_when_no_email(self):
        from src.dependencies.admin_auth import get_current_user_from_email

        session = AsyncMock()
        ctx = _make_group_context(email=None)
        result = await get_current_user_from_email(session, ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_calls_user_service_and_returns_user(self):
        from src.dependencies.admin_auth import get_current_user_from_email

        session = AsyncMock()
        ctx = _make_group_context(email="user@example.com")
        expected_user = _make_user("user@example.com")

        mock_service = AsyncMock()
        mock_service.get_or_create_user_by_email = AsyncMock(return_value=expected_user)

        with patch("src.services.user_service.UserService", return_value=mock_service):
            result = await get_current_user_from_email(session, ctx)

        assert result is expected_user


# ============================================================================
# require_authenticated_user
# ============================================================================

class TestRequireAuthenticatedUser:

    @pytest.mark.asyncio
    async def test_raises_401_when_no_email(self):
        from src.dependencies.admin_auth import require_authenticated_user

        session = AsyncMock()
        ctx = _make_group_context(email=None)

        with pytest.raises(HTTPException) as exc_info:
            await require_authenticated_user(session, ctx)

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_returns_user_when_get_current_user_succeeds(self):
        from src.dependencies.admin_auth import require_authenticated_user

        session = AsyncMock()
        ctx = _make_group_context(email="user@example.com")
        user = _make_user("user@example.com")

        with patch("src.dependencies.admin_auth.get_current_user_from_email", return_value=user):
            result = await require_authenticated_user(session, ctx)

        assert result is user

    @pytest.mark.asyncio
    async def test_auto_creates_user_when_not_found(self):
        from src.dependencies.admin_auth import require_authenticated_user

        session = AsyncMock()
        ctx = _make_group_context(email="new@example.com")
        created_user = _make_user("new@example.com")

        with (
            patch("src.dependencies.admin_auth.get_current_user_from_email", return_value=None),
            patch("src.dependencies.admin_auth._create_user_from_forwarded_email", return_value=created_user),
        ):
            result = await require_authenticated_user(session, ctx)

        assert result is created_user

    @pytest.mark.asyncio
    async def test_raises_401_when_user_creation_fails(self):
        from src.dependencies.admin_auth import require_authenticated_user

        session = AsyncMock()
        ctx = _make_group_context(email="fail@example.com")

        with (
            patch("src.dependencies.admin_auth.get_current_user_from_email", return_value=None),
            patch("src.dependencies.admin_auth._create_user_from_forwarded_email", return_value=None),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await require_authenticated_user(session, ctx)

        assert exc_info.value.status_code == 401


# ============================================================================
# get_authenticated_user
# ============================================================================

class TestGetAuthenticatedUser:

    @pytest.mark.asyncio
    async def test_delegates_to_require_authenticated_user(self):
        from src.dependencies.admin_auth import get_authenticated_user

        session = AsyncMock()
        ctx = _make_group_context(email="u@e.com")
        user = _make_user("u@e.com")

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user) as mock_req:
            result = await get_authenticated_user(session, ctx)

        assert result is user
        mock_req.assert_called_once_with(session, ctx)


# ============================================================================
# get_admin_user
# ============================================================================

class TestGetAdminUser:

    @pytest.mark.asyncio
    async def test_system_admin_passes(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="admin@example.com")
        user = _make_user("admin@example.com", is_system_admin=True)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            result = await get_admin_user(session, ctx)

        assert result is user

    @pytest.mark.asyncio
    async def test_group_admin_via_highest_role_passes(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="gadmin@example.com", highest_role="admin")
        user = _make_user("gadmin@example.com", is_system_admin=False)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            result = await get_admin_user(session, ctx)

        assert result is user

    @pytest.mark.asyncio
    async def test_group_admin_via_user_role_passes(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="grp@example.com", user_role="admin")
        # highest_role not set (None)
        ctx.highest_role = None
        user = _make_user("grp@example.com", is_system_admin=False)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            result = await get_admin_user(session, ctx)

        assert result is user

    @pytest.mark.asyncio
    async def test_regular_user_raises_403(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="regular@example.com", user_role="member")
        ctx.highest_role = None
        user = _make_user("regular@example.com", is_system_admin=False)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            with pytest.raises(HTTPException) as exc_info:
                await get_admin_user(session, ctx)

        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_no_role_at_all_raises_403(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="norole@example.com", user_role=None)
        ctx.highest_role = None
        user = _make_user("norole@example.com", is_system_admin=False)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            with pytest.raises(HTTPException) as exc_info:
                await get_admin_user(session, ctx)

        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_highest_role_case_insensitive(self):
        from src.dependencies.admin_auth import get_admin_user

        session = AsyncMock()
        ctx = _make_group_context(email="upper@example.com", highest_role="ADMIN")
        user = _make_user("upper@example.com", is_system_admin=False)

        with patch("src.dependencies.admin_auth.require_authenticated_user", return_value=user):
            result = await get_admin_user(session, ctx)

        assert result is user
