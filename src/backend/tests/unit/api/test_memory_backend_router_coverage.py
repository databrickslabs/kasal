"""
Coverage tests for src/api/memory_backend_router.py
Uses lazy imports to avoid collection errors with --cov.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
import importlib


class AdminCtx:
    def __init__(self, is_admin=True, is_system_admin=False):
        self.user_role = "admin" if is_admin else "viewer"
        self.current_user = SimpleNamespace(
            is_system_admin=is_system_admin,
            is_personal_workspace_manager=is_admin,
        )
        self.primary_group_id = "user_alice_example_com"
        self.group_ids = ["user_alice_example_com"]
        self.group_email = "alice@example.com"
        self.access_token = "tok"


def get_m():
    return importlib.import_module("src.api.memory_backend_router")


# ─── get_memory_backend_service ───────────────────────────────────────────────

def test_get_memory_backend_service():
    m = get_m()
    fake_session = MagicMock()
    with patch("src.api.memory_backend_router.MemoryBackendService") as MockSvc:
        MockSvc.return_value = MagicMock()
        m.get_memory_backend_service(session=fake_session)
        MockSvc.assert_called_once_with(fake_session)


# ─── get_workspace_url ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_workspace_url():
    m = get_m()
    svc = AsyncMock()
    svc.get_workspace_url = AsyncMock(return_value={"workspace_url": "https://db.com"})
    ctx = AdminCtx()
    result = await m.get_workspace_url(service=svc, group_context=ctx)
    assert result["workspace_url"] == "https://db.com"


# ─── test_lakebase_connection ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_lakebase_connection_success():
    m = get_m()
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(return_value={"success": True})
    ctx = AdminCtx()
    result = await m.test_lakebase_connection(group_context=ctx, service=svc, request=None)
    assert result["success"] is True


@pytest.mark.asyncio
async def test_lakebase_connection_exception():
    m = get_m()
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(side_effect=Exception("conn failed"))
    ctx = AdminCtx()
    result = await m.test_lakebase_connection(group_context=ctx, service=svc, request=None)
    assert result["success"] is False


# ─── get_memory_configs ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_memory_configs():
    m = get_m()
    svc = AsyncMock()
    svc.get_all = AsyncMock(return_value=[])
    ctx = AdminCtx()
    result = await m.get_memory_configs(service=svc, group_context=ctx, request=None)
    assert isinstance(result, list)


# ─── get_memory_config_by_id ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_memory_config_by_id_not_found():
    m = get_m()
    svc = AsyncMock()
    svc.get_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    with pytest.raises(NotFoundError):
        await m.get_memory_config_by_id(backend_id="999", service=svc, group_context=ctx)


# ─── create_memory_config ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_memory_config_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_create_memory_config_success():
    m = get_m()
    svc = AsyncMock()
    created = MagicMock()
    svc.create_memory_backend = AsyncMock(return_value=created)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch("src.api.memory_backend_router.MemoryBackendResponse") as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await m.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── update_memory_config ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_memory_config_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.update_memory_config(
            backend_id=1, update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_not_found():
    m = get_m()
    svc = AsyncMock()
    svc.update_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await m.update_memory_config(
            backend_id="999", update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_success():
    m = get_m()
    svc = AsyncMock()
    updated = MagicMock()
    svc.update_memory_backend = AsyncMock(return_value=updated)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch("src.api.memory_backend_router.MemoryBackendResponse") as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await m.update_memory_config(
            backend_id="1", update_data=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── delete_memory_config ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_memory_config_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.delete_memory_config(backend_id="1", service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_delete_memory_config_not_found():
    m = get_m()
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await m.delete_memory_config(backend_id="999", service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_delete_memory_config_success():
    m = get_m()
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await m.delete_memory_config(backend_id="1", service=svc, group_context=ctx)
    assert result["success"] is True


# ─── set_default_memory_config ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_set_default_memory_config_not_found():
    m = get_m()
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await m.set_default_memory_config(
            backend_id="999", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_set_default_memory_config_success():
    m = get_m()
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await m.set_default_memory_config(
        backend_id="1", service=svc, group_context=ctx
    )
    assert result["success"] is True


# ─── get_default_memory_config ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_default_memory_config_none():
    m = get_m()
    svc = AsyncMock()
    svc.get_default_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    result = await m.get_default_memory_config(service=svc, group_context=ctx)
    assert result is None


# ─── get_memory_stats ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_memory_stats():
    m = get_m()
    svc = AsyncMock()
    svc.get_memory_stats = AsyncMock(return_value={"total": 100})
    ctx = AdminCtx()
    result = await m.get_memory_stats(crew_id=None, service=svc, group_context=ctx)
    assert result["total"] == 100


# ─── validate_memory_config ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_validate_memory_config_databricks_valid():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType, DatabricksMemoryConfig
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=DatabricksMemoryConfig(memory_index="catalog.schema.memory_index", 
            workspace_url="https://example.com",
            endpoint_name="my-endpoint",
            short_term_index="st_idx",
            long_term_index="lt_idx",
            entity_index="ent_idx",
        )
    )
    result = await m.validate_memory_config(config=config, service=svc, group_context=ctx)
    assert result["valid"] is True


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_no_config():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=None
    )
    result = await m.validate_memory_config(config=config, service=svc, group_context=ctx)
    assert result["valid"] is False


# ─── get_databricks_indexes ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_databricks_indexes():
    m = get_m()
    svc = AsyncMock()
    svc.get_available_indexes = AsyncMock(return_value={"indexes": ["idx1"]})
    ctx = AdminCtx()
    from src.schemas.memory_backend import DatabricksMemoryConfig
    config = DatabricksMemoryConfig(memory_index="catalog.schema.memory_index", 
        workspace_url="https://example.com",
        endpoint_name="my-endpoint",
        short_term_index="st_idx",
    )
    result = await m.get_databricks_indexes(config=config, request=None, service=svc, group_context=ctx)
    assert result is not None


# ─── initialize_lakebase_tables ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_initialize_lakebase_tables_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.initialize_lakebase_tables(request={}, service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_initialize_lakebase_tables_success():
    m = get_m()
    svc = AsyncMock()
    svc.initialize_lakebase_tables = AsyncMock(return_value={"success": True})
    ctx = AdminCtx(is_admin=True)
    result = await m.initialize_lakebase_tables(
        request={"instance_name": "my-lakebase", "embedding_dimension": 768},
        service=svc,
        group_context=ctx
    )
    assert result["success"] is True


# ─── get_lakebase_table_stats ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_lakebase_table_stats():
    m = get_m()
    svc = AsyncMock()
    svc.get_lakebase_table_stats = AsyncMock(return_value={"tables": []})
    ctx = AdminCtx()
    result = await m.get_lakebase_table_stats(service=svc, group_context=ctx)
    assert "tables" in result


# ─── get_lakebase_table_data ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_lakebase_table_data():
    m = get_m()
    svc = AsyncMock()
    svc.get_lakebase_table_data = AsyncMock(return_value={"documents": []})
    ctx = AdminCtx()
    result = await m.get_lakebase_table_data(
        service=svc, group_context=ctx, table_name="crew_short_term_memory", limit=50
    )
    assert "documents" in result


# ─── get_lakebase_entity_data ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_lakebase_entity_data():
    m = get_m()
    svc = AsyncMock()
    svc.get_lakebase_entity_data = AsyncMock(return_value={"entities": []})
    ctx = AdminCtx()
    result = await m.get_lakebase_entity_data(
        service=svc, group_context=ctx
    )
    assert "entities" in result


# ─── save_lakebase_config ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_lakebase_config_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.save_lakebase_config(
            request=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_save_lakebase_config_success():
    m = get_m()
    svc = AsyncMock()
    svc.save_lakebase_config = AsyncMock(return_value={"saved": True})
    ctx = AdminCtx(is_admin=True)
    result = await m.save_lakebase_config(
        request=MagicMock(), service=svc, group_context=ctx
    )
    assert result is not None


# ─── one_click_databricks_setup ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_one_click_databricks_setup_forbidden():
    m = get_m()
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await m.one_click_databricks_setup(
            request=MagicMock(), req=None, service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_one_click_databricks_setup_success():
    m = get_m()
    svc = AsyncMock()
    svc.one_click_databricks_setup = AsyncMock(return_value={"success": True, "endpoints": [], "indexes": []})
    ctx = AdminCtx(is_admin=True)
    result = await m.one_click_databricks_setup(
        request={"workspace_url": "https://example.com", "catalog": "cat", "schema": "sch"},
        req=None, service=svc, group_context=ctx
    )
    assert result is not None
