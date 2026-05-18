"""
Coverage tests for services/memory_config_service.py
Covers missing lines: get_active_config branches
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.schemas.memory_backend import MemoryBackendType


VALID_DATABRICKS_CONFIG = {
    "workspace_url": "https://example.databricks.com",
    "endpoint_name": "my_vs_endpoint",
    "short_term_index": "catalog.schema.short_term",
}

VALID_LAKEBASE_CONFIG = {
    "instance_name": "my_lakebase_instance",
}


def make_backend(
    name="test",
    is_active=True,
    is_default=False,
    backend_type=MemoryBackendType.DATABRICKS,
    databricks_config=None,
    lakebase_config=None,
):
    b = MagicMock()
    b.name = name
    b.is_active = is_active
    b.is_default = is_default
    b.backend_type = backend_type
    b.databricks_config = databricks_config
    b.lakebase_config = lakebase_config
    b.updated_at = datetime.now(timezone.utc)
    b.enable_short_term = True
    b.enable_long_term = True
    b.enable_entity = True
    b.enable_relationship_retrieval = False
    b.custom_config = None
    return b


def make_service():
    from src.services.memory_config_service import MemoryConfigService
    svc = MemoryConfigService(session=AsyncMock())
    svc.repository = AsyncMock()
    return svc


# ---- No group_id, no backends ----

@pytest.mark.asyncio
async def test_no_group_id_no_backends_returns_none():
    svc = make_service()
    svc.repository.get_all = AsyncMock(return_value=[])
    result = await svc.get_active_config(group_id=None)
    assert result is None


# ---- group_id but no active backends ----

@pytest.mark.asyncio
async def test_group_id_inactive_backend_returns_none():
    svc = make_service()
    svc.repository.get_by_group_id = AsyncMock(return_value=[
        make_backend(is_active=False)
    ])
    result = await svc.get_active_config(group_id="g1")
    assert result is None


@pytest.mark.asyncio
async def test_group_id_empty_backends_returns_none():
    svc = make_service()
    svc.repository.get_by_group_id = AsyncMock(return_value=[])
    result = await svc.get_active_config(group_id="g1")
    assert result is None


# ---- group_id with DATABRICKS backend with databricks_config ----

@pytest.mark.asyncio
async def test_group_id_databricks_with_config():
    svc = make_service()
    backend = make_backend(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=VALID_DATABRICKS_CONFIG.copy(),
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id="g1")
    assert result is not None
    assert result.backend_type == MemoryBackendType.DATABRICKS
    assert result.databricks_config is not None


# ---- group_id with LAKEBASE backend with lakebase_config ----

@pytest.mark.asyncio
async def test_group_id_lakebase_with_config():
    svc = make_service()
    backend = make_backend(
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=VALID_LAKEBASE_CONFIG.copy(),
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id="g1")
    assert result is not None
    assert result.backend_type == MemoryBackendType.LAKEBASE


# ---- group_id with DEFAULT/other backend (no specific config) ----

@pytest.mark.asyncio
async def test_group_id_default_backend_no_config():
    svc = make_service()
    backend = make_backend(
        backend_type=MemoryBackendType.DEFAULT,
        databricks_config=None,
        lakebase_config=None,
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id="g1")
    assert result is not None
    assert result.backend_type == MemoryBackendType.DEFAULT


# ---- group_id with DATABRICKS, no workspace_url, auth succeeds ----

@pytest.mark.asyncio
async def test_group_id_databricks_no_workspace_url_auth_succeeds():
    svc = make_service()
    config = {
        "endpoint_name": "my_endpoint",
        "short_term_index": "catalog.schema.idx",
        # No workspace_url
    }
    backend = make_backend(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=config,
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[backend])

    mock_auth = MagicMock()
    mock_auth.workspace_url = "https://auto.databricks.com"
    mock_auth.auth_method = "oauth"

    # asyncio is a local import inside the function, patch it via builtins
    with patch('asyncio.run', return_value=mock_auth):
        result = await svc.get_active_config(group_id="g1")
    assert result is not None


# ---- group_id with DATABRICKS, no workspace_url, auth fails ----

@pytest.mark.asyncio
async def test_group_id_databricks_no_workspace_url_auth_fails():
    svc = make_service()
    config = {
        "endpoint_name": "my_endpoint",
        "short_term_index": "catalog.schema.idx",
        # No workspace_url
    }
    backend = make_backend(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=config,
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[backend])

    with patch('asyncio.run', side_effect=Exception("auth error")):
        result = await svc.get_active_config(group_id="g1")
    assert result is not None


# ---- group_id, prefer DATABRICKS when multiple active ----

@pytest.mark.asyncio
async def test_group_id_prefers_databricks_over_default():
    svc = make_service()
    default_backend = make_backend(
        backend_type=MemoryBackendType.DEFAULT,
        databricks_config=None,
    )
    db_backend = make_backend(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=VALID_DATABRICKS_CONFIG.copy(),
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[default_backend, db_backend])
    result = await svc.get_active_config(group_id="g1")
    assert result is not None
    assert result.backend_type == MemoryBackendType.DATABRICKS


# ---- group_id, prefer LAKEBASE when no DATABRICKS ----

@pytest.mark.asyncio
async def test_group_id_prefers_lakebase_over_default():
    svc = make_service()
    default_backend = make_backend(
        backend_type=MemoryBackendType.DEFAULT,
        databricks_config=None,
    )
    lb_backend = make_backend(
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=VALID_LAKEBASE_CONFIG.copy(),
    )
    svc.repository.get_by_group_id = AsyncMock(return_value=[default_backend, lb_backend])
    result = await svc.get_active_config(group_id="g1")
    assert result is not None
    assert result.backend_type == MemoryBackendType.LAKEBASE


# ---- System-level default (no group_id): active+default DATABRICKS ----

@pytest.mark.asyncio
async def test_no_group_id_default_databricks():
    svc = make_service()
    backend = make_backend(
        is_active=True,
        is_default=True,
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=VALID_DATABRICKS_CONFIG.copy(),
    )
    svc.repository.get_all = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id=None)
    assert result is not None
    assert result.backend_type == MemoryBackendType.DATABRICKS


# ---- System-level default: active+default LAKEBASE ----

@pytest.mark.asyncio
async def test_no_group_id_default_lakebase():
    svc = make_service()
    backend = make_backend(
        is_active=True,
        is_default=True,
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=VALID_LAKEBASE_CONFIG.copy(),
    )
    svc.repository.get_all = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id=None)
    assert result is not None
    assert result.backend_type == MemoryBackendType.LAKEBASE


# ---- System-level default: active=False or is_default=False ----

@pytest.mark.asyncio
async def test_no_group_id_inactive_default_returns_none():
    svc = make_service()
    backend = make_backend(is_active=False, is_default=True)
    svc.repository.get_all = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id=None)
    assert result is None


@pytest.mark.asyncio
async def test_no_group_id_not_default_returns_none():
    svc = make_service()
    backend = make_backend(is_active=True, is_default=False)
    svc.repository.get_all = AsyncMock(return_value=[backend])
    result = await svc.get_active_config(group_id=None)
    assert result is None


# ---- System-level default: DATABRICKS, no workspace_url ----

@pytest.mark.asyncio
async def test_no_group_id_databricks_no_workspace_url_auth_succeeds():
    svc = make_service()
    config = {
        "endpoint_name": "my_endpoint",
        "short_term_index": "catalog.schema.idx",
    }
    backend = make_backend(
        is_active=True,
        is_default=True,
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=config,
    )
    svc.repository.get_all = AsyncMock(return_value=[backend])

    mock_auth = MagicMock()
    mock_auth.workspace_url = "https://system.databricks.com"
    mock_auth.auth_method = "pat"

    with patch('asyncio.run', return_value=mock_auth):
        result = await svc.get_active_config(group_id=None)
    assert result is not None


@pytest.mark.asyncio
async def test_no_group_id_databricks_no_workspace_url_auth_fails():
    svc = make_service()
    config = {
        "endpoint_name": "my_endpoint",
        "short_term_index": "catalog.schema.idx",
    }
    backend = make_backend(
        is_active=True,
        is_default=True,
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=config,
    )
    svc.repository.get_all = AsyncMock(return_value=[backend])

    with patch('asyncio.run', side_effect=Exception("auth error")):
        result = await svc.get_active_config(group_id=None)
    assert result is not None
