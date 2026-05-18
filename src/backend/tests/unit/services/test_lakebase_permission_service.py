"""
Unit tests for services/lakebase_permission_service.py

Tests for LakebasePermissionService methods:
- _quote_pg_role validation
- grant_schema_permissions_async
- grant_schema_permissions_sync
- grant_default_privileges_async
- grant_default_privileges_sync
- grant_all_permissions_async
- grant_all_permissions_sync
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.lakebase_permission_service import (
    LakebasePermissionService,
    _quote_pg_role,
)


# ---- _quote_pg_role ----

def test_quote_pg_role_email():
    result = _quote_pg_role("admin@example.com")
    assert result == '"admin@example.com"'


def test_quote_pg_role_uuid():
    result = _quote_pg_role("550e8400-e29b-41d4-a716-446655440000")
    assert '"550e8400-e29b-41d4-a716-446655440000"' == result


def test_quote_pg_role_invalid_raises():
    with pytest.raises(ValueError, match="Invalid PostgreSQL role"):
        _quote_pg_role("not-valid-!!!")


def test_quote_pg_role_empty_raises():
    with pytest.raises(ValueError):
        _quote_pg_role("")


def test_quote_pg_role_none_raises():
    with pytest.raises((ValueError, TypeError)):
        _quote_pg_role(None)


def test_quote_pg_role_email_with_dots():
    result = _quote_pg_role("first.last@company.org")
    assert '"first.last@company.org"' == result


# ---- LakebasePermissionService initialization ----

def test_init():
    svc = LakebasePermissionService()
    assert svc is not None


# ---- grant_schema_permissions_async ----

@pytest.mark.asyncio
async def test_grant_schema_permissions_async_success():
    """Test successful async schema permission grant."""
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    # Set up async context manager
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    await svc.grant_schema_permissions_async(mock_engine, "admin@example.com")

    assert mock_conn.execute.call_count == 2  # kasal and public schemas


@pytest.mark.asyncio
async def test_grant_schema_permissions_async_exception_logged_not_raised():
    """Test that permission errors are caught and logged, not raised."""
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(side_effect=Exception("Permission denied"))
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    # Should not raise
    await svc.grant_schema_permissions_async(mock_engine, "admin@example.com")


@pytest.mark.asyncio
async def test_grant_schema_permissions_async_invalid_email_logs():
    """Test that invalid email is caught and logged, not raised."""
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    # The ValueError from _quote_pg_role is caught internally and logged
    # Should not raise
    await svc.grant_schema_permissions_async(mock_engine, "invalid!!!")


# ---- grant_schema_permissions_sync ----

def test_grant_schema_permissions_sync_success():
    """Test successful sync schema permission grant."""
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock()

    svc.grant_schema_permissions_sync(mock_conn, "admin@example.com")

    assert mock_conn.execute.call_count == 2  # kasal and public schemas


def test_grant_schema_permissions_sync_exception_logged():
    """Test that permission errors are caught and logged, not raised."""
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock(side_effect=Exception("Permission denied"))

    # Should not raise
    svc.grant_schema_permissions_sync(mock_conn, "admin@example.com")


def test_grant_schema_permissions_sync_invalid_email():
    """Test that invalid email is caught and logged, not raised."""
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    # The ValueError from _quote_pg_role is caught internally and logged
    # Should not raise
    svc.grant_schema_permissions_sync(mock_conn, "invalid!!!")


# ---- grant_default_privileges_async ----

@pytest.mark.asyncio
async def test_grant_default_privileges_async_success():
    """Test successful async default privileges grant."""
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    await svc.grant_default_privileges_async(mock_engine, "admin@example.com")

    assert mock_conn.execute.call_count == 2  # tables and sequences


@pytest.mark.asyncio
async def test_grant_default_privileges_async_exception_logged():
    """Test that privilege errors are caught and logged."""
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(side_effect=Exception("Privilege denied"))
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    # Should not raise
    await svc.grant_default_privileges_async(mock_engine, "admin@example.com")


# ---- grant_default_privileges_sync ----

def test_grant_default_privileges_sync_success():
    """Test successful sync default privileges grant."""
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock()

    svc.grant_default_privileges_sync(mock_conn, "admin@example.com")

    assert mock_conn.execute.call_count == 2  # tables and sequences


def test_grant_default_privileges_sync_exception_logged():
    """Test that privilege errors are caught and logged."""
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock(side_effect=Exception("Privilege denied"))

    # Should not raise
    svc.grant_default_privileges_sync(mock_conn, "admin@example.com")


# ---- grant_all_permissions_async ----

@pytest.mark.asyncio
async def test_grant_all_permissions_async():
    """Test grant_all_permissions_async calls both sub-methods."""
    svc = LakebasePermissionService()
    svc.grant_schema_permissions_async = AsyncMock()
    svc.grant_default_privileges_async = AsyncMock()

    mock_engine = MagicMock()
    await svc.grant_all_permissions_async(mock_engine, "admin@example.com")

    svc.grant_schema_permissions_async.assert_awaited_once_with(mock_engine, "admin@example.com")
    svc.grant_default_privileges_async.assert_awaited_once_with(mock_engine, "admin@example.com")


# ---- grant_all_permissions_sync ----

def test_grant_all_permissions_sync():
    """Test grant_all_permissions_sync calls both sub-methods."""
    svc = LakebasePermissionService()
    svc.grant_schema_permissions_sync = MagicMock()
    svc.grant_default_privileges_sync = MagicMock()

    mock_conn = MagicMock()
    svc.grant_all_permissions_sync(mock_conn, "admin@example.com")

    svc.grant_schema_permissions_sync.assert_called_once_with(mock_conn, "admin@example.com")
    svc.grant_default_privileges_sync.assert_called_once_with(mock_conn, "admin@example.com")
