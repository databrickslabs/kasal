"""
Coverage tests for services/lakebase_permission_service.py
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.services.lakebase_permission_service import (
    LakebasePermissionService,
    _quote_pg_role,
)


# ---- _quote_pg_role ----

def test_quote_pg_role_valid_email():
    assert _quote_pg_role("admin@example.com") == '"admin@example.com"'


def test_quote_pg_role_valid_uuid():
    uid = "550e8400-e29b-41d4-a716-446655440000"
    assert _quote_pg_role(uid) == f'"{uid}"'


def test_quote_pg_role_invalid_raises():
    with pytest.raises(ValueError):
        _quote_pg_role("not@valid!")


def test_quote_pg_role_empty_raises():
    with pytest.raises(ValueError):
        _quote_pg_role("")


def test_quote_pg_role_none_raises():
    with pytest.raises((ValueError, TypeError)):
        _quote_pg_role(None)


# ---- Service initialization ----

def test_init():
    svc = LakebasePermissionService()
    assert svc is not None


# ---- grant_schema_permissions_async ----

@pytest.mark.asyncio
async def test_grant_schema_permissions_async_success():
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_conn = AsyncMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    await svc.grant_schema_permissions_async(mock_engine, "admin@example.com")
    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_grant_schema_permissions_async_error_not_raised():
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(side_effect=Exception("denied"))
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin
    # Should not raise
    await svc.grant_schema_permissions_async(mock_engine, "admin@example.com")


@pytest.mark.asyncio
async def test_grant_schema_permissions_async_invalid_email():
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    # ValueError from _quote_pg_role is caught internally
    await svc.grant_schema_permissions_async(mock_engine, "invalid!!!")


# ---- grant_schema_permissions_sync ----

def test_grant_schema_permissions_sync_success():
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    svc.grant_schema_permissions_sync(mock_conn, "admin@example.com")
    assert mock_conn.execute.call_count == 2


def test_grant_schema_permissions_sync_error_not_raised():
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("denied")
    # Should not raise
    svc.grant_schema_permissions_sync(mock_conn, "admin@example.com")


def test_grant_schema_permissions_sync_invalid_email():
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    # ValueError from _quote_pg_role is caught internally
    svc.grant_schema_permissions_sync(mock_conn, "invalid!!!")


# ---- grant_default_privileges_async ----

@pytest.mark.asyncio
async def test_grant_default_privileges_async_success():
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_conn = AsyncMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin

    await svc.grant_default_privileges_async(mock_engine, "admin@example.com")
    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_grant_default_privileges_async_error_not_raised():
    svc = LakebasePermissionService()
    mock_engine = MagicMock()
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(side_effect=Exception("denied"))
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin.return_value = mock_begin
    # Should not raise
    await svc.grant_default_privileges_async(mock_engine, "admin@example.com")


# ---- grant_default_privileges_sync ----

def test_grant_default_privileges_sync_success():
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    svc.grant_default_privileges_sync(mock_conn, "admin@example.com")
    assert mock_conn.execute.call_count == 2


def test_grant_default_privileges_sync_error_not_raised():
    svc = LakebasePermissionService()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("denied")
    # Should not raise
    svc.grant_default_privileges_sync(mock_conn, "admin@example.com")


# ---- grant_all_permissions_async ----

@pytest.mark.asyncio
async def test_grant_all_permissions_async():
    svc = LakebasePermissionService()
    svc.grant_schema_permissions_async = AsyncMock()
    svc.grant_default_privileges_async = AsyncMock()

    mock_engine = MagicMock()
    await svc.grant_all_permissions_async(mock_engine, "admin@example.com")

    svc.grant_schema_permissions_async.assert_awaited_once_with(mock_engine, "admin@example.com")
    svc.grant_default_privileges_async.assert_awaited_once_with(mock_engine, "admin@example.com")


# ---- grant_all_permissions_sync ----

def test_grant_all_permissions_sync():
    svc = LakebasePermissionService()
    svc.grant_schema_permissions_sync = MagicMock()
    svc.grant_default_privileges_sync = MagicMock()

    mock_conn = MagicMock()
    svc.grant_all_permissions_sync(mock_conn, "admin@example.com")

    svc.grant_schema_permissions_sync.assert_called_once_with(mock_conn, "admin@example.com")
    svc.grant_default_privileges_sync.assert_called_once_with(mock_conn, "admin@example.com")
