"""Forwarded-identity fallback provisioning preserves role and transaction rules."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.models.enums import UserRole


def _make_user(email="user@example.com", is_system_admin=False, role="regular"):
    from unittest.mock import MagicMock

    user = MagicMock()
    user.email = email
    user.is_system_admin = is_system_admin
    user.role = role
    return user


def _make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    return session


class TestCreateUserFromForwardedEmail:
    @pytest.mark.asyncio
    async def test_handles_exception_and_returns_none(self):
        from src.services.groups.forwarded_identity import (
            get_or_create_forwarded_user as _create_user_from_forwarded_email,
        )

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=Exception("DB error"))
        mock_session.rollback = AsyncMock()

        result = await _create_user_from_forwarded_email(
            mock_session, "bad@example.com"
        )

        assert result is None
        assert mock_session.rollback.called

    @pytest.mark.asyncio
    async def test_returns_existing_user_when_found(self):
        """Test that existing user is returned directly with updated last_login."""
        from src.services.groups.forwarded_identity import (
            get_or_create_forwarded_user as _create_user_from_forwarded_email,
        )
        from src.models.user import User

        # Use a real User instance (no hashed_password needed since it's not in the model)
        mock_session = AsyncMock()
        existing = MagicMock(spec=User)
        existing.email = "exist@example.com"

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        result = await _create_user_from_forwarded_email(
            mock_session, "exist@example.com"
        )

        assert result is existing


class TestCreateUserFromForwardedEmailBranches:
    @pytest.mark.asyncio
    async def test_create_user_existing_user(self):
        """Test returning existing user from X-Forwarded-Email."""
        from src.services.groups.forwarded_identity import (
            get_or_create_forwarded_user as _create_user_from_forwarded_email,
        )

        session = _make_session()
        existing_user = _make_user("test@example.com")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing_user
        session.execute = AsyncMock(return_value=mock_result)

        result = await _create_user_from_forwarded_email(session, "test@example.com")
        assert result is existing_user
        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_user_new_unique_username(self):
        from src.services.groups.forwarded_identity import get_or_create_forwarded_user

        session = _make_session()
        missing = MagicMock()
        missing.scalars.return_value.first.return_value = None
        username_result = MagicMock()
        username_result.scalars.return_value.first.return_value = None
        session.execute.side_effect = [missing, username_result]
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            result = await get_or_create_forwarded_user(session, "newuser@example.com")
        assert result.email == "newuser@example.com"
        assert result.username == "newuser"
        assert result.role == UserRole.REGULAR
        session.add.assert_called_once_with(result)
        session.flush.assert_awaited_once()
        session.commit.assert_awaited_once()
        session.refresh.assert_awaited_once_with(result)
        session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_user_duplicate_username(self):
        from src.services.groups.forwarded_identity import get_or_create_forwarded_user

        session = _make_session()
        missing = MagicMock()
        missing.scalars.return_value.first.return_value = None
        username_result = MagicMock()
        username_result.scalars.return_value.first.return_value = MagicMock()
        session.execute.side_effect = [missing, username_result]
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            result = await get_or_create_forwarded_user(
                session, "newuser@mycompany.com"
            )
        assert result.email == "newuser@mycompany.com"
        assert result.username == "newuser_mycompany"
        assert result.role == UserRole.REGULAR
        session.add.assert_called_once_with(result)
        session.flush.assert_awaited_once()
        session.commit.assert_awaited_once()
        session.refresh.assert_awaited_once_with(result)
        session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_user_admin_email_in_dev(self):
        from src.services.groups.forwarded_identity import get_or_create_forwarded_user

        session = _make_session()
        missing = MagicMock()
        missing.scalars.return_value.first.return_value = None
        username_result = MagicMock()
        username_result.scalars.return_value.first.return_value = None
        session.execute.side_effect = [missing, username_result]
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            result = await get_or_create_forwarded_user(session, "admin@localhost")
        assert result.email == "admin@localhost"
        assert result.username == "admin"
        assert result.role == UserRole.ADMIN
        session.add.assert_called_once_with(result)
        session.flush.assert_awaited_once()
        session.commit.assert_awaited_once()
        session.refresh.assert_awaited_once_with(result)
        session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_user_exception_returns_none(self):
        """Test exception handling returns None."""
        from src.services.groups.forwarded_identity import (
            get_or_create_forwarded_user as _create_user_from_forwarded_email,
        )

        session = _make_session()
        session.execute = AsyncMock(side_effect=Exception("DB error"))

        result = await _create_user_from_forwarded_email(session, "user@example.com")
        assert result is None
        session.rollback.assert_called_once()
