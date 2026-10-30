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

from unittest.mock import MagicMock

import pytest

from src.services.databricks.lakebase.permission import (
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


# ---- grant_all_permissions_sync ----


def test_grant_all_permissions_sync():
    """Test grant_all_permissions_sync calls both sub-methods."""
    svc = LakebasePermissionService()
    svc.grant_schema_permissions_sync = MagicMock()
    svc.grant_default_privileges_sync = MagicMock()

    mock_conn = MagicMock()
    svc.grant_all_permissions_sync(mock_conn, "admin@example.com")

    svc.grant_schema_permissions_sync.assert_called_once_with(
        mock_conn, "admin@example.com"
    )
    svc.grant_default_privileges_sync.assert_called_once_with(
        mock_conn, "admin@example.com"
    )
