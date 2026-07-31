"""
Coverage tests for the src/api/memory_backend package.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.memory_backend import (
    configs_router,
    dependencies,
    lakebase_router,
    records_router,
)
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError


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


# ─── get_memory_backend_service ───────────────────────────────────────────────


def test_get_memory_backend_service():
    fake_session = MagicMock()
    with patch("src.api.memory_backend.dependencies.MemoryBackendService") as MockSvc:
        MockSvc.return_value = MagicMock()
        dependencies.get_memory_backend_service(session=fake_session)
        MockSvc.assert_called_once_with(fake_session)


# ─── test_lakebase_connection ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lakebase_connection_success():
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(return_value={"success": True})
    ctx = AdminCtx()
    result = await lakebase_router.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert result["success"] is True


@pytest.mark.asyncio
async def test_lakebase_connection_exception():
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(side_effect=Exception("conn failed"))
    ctx = AdminCtx()
    result = await lakebase_router.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert result["success"] is False


# ─── get_memory_configs ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_configs():
    svc = AsyncMock()
    svc.get_all = AsyncMock(return_value=[])
    ctx = AdminCtx()
    result = await configs_router.get_memory_configs(
        service=svc, group_context=ctx, request=None
    )
    assert isinstance(result, list)


# ─── get_memory_config_by_id ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_config_by_id_not_found():
    svc = AsyncMock()
    svc.get_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    with pytest.raises(NotFoundError):
        await configs_router.get_memory_config_by_id(
            backend_id="999", service=svc, group_context=ctx
        )


# ─── create_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_create_memory_config_success():
    svc = AsyncMock()
    created = MagicMock()
    svc.create_memory_backend = AsyncMock(return_value=created)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch(
        "src.api.memory_backend.configs_router.MemoryBackendResponse"
    ) as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await configs_router.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── update_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.update_memory_config(
            backend_id=1, update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_not_found():
    svc = AsyncMock()
    svc.update_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.update_memory_config(
            backend_id="999", update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_success():
    svc = AsyncMock()
    updated = MagicMock()
    svc.update_memory_backend = AsyncMock(return_value=updated)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch(
        "src.api.memory_backend.configs_router.MemoryBackendResponse"
    ) as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await configs_router.update_memory_config(
            backend_id="1", update_data=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── delete_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.delete_memory_config(
            backend_id="1", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_delete_memory_config_not_found():
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.delete_memory_config(
            backend_id="999", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_delete_memory_config_success():
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await configs_router.delete_memory_config(
        backend_id="1", service=svc, group_context=ctx
    )
    assert result["success"] is True


# ─── set_default_memory_config ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_default_memory_config_not_found():
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.set_default_memory_config(
            backend_id="999", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_set_default_memory_config_success():
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await configs_router.set_default_memory_config(
        backend_id="1", service=svc, group_context=ctx
    )
    assert result["success"] is True


# ─── get_default_memory_config ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_default_memory_config_none():
    svc = AsyncMock()
    svc.get_default_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    result = await configs_router.get_default_memory_config(
        service=svc, group_context=ctx
    )
    assert result is None


# ─── get_memory_stats ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_stats():
    svc = AsyncMock()
    svc.get_memory_stats = AsyncMock(return_value={"total": 100})
    ctx = AdminCtx()
    result = await records_router.get_memory_stats(
        crew_id=None, service=svc, group_context=ctx
    )
    assert result["total"] == 100


# ─── validate_memory_config ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_valid():
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import (
        DatabricksMemoryConfig,
        MemoryBackendConfig,
        MemoryBackendType,
    )

    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=DatabricksMemoryConfig(
            memory_index="catalog.schema.memory_index",
            workspace_url="https://example.com",
            endpoint_name="my-endpoint",
            short_term_index="st_idx",
            long_term_index="lt_idx",
            entity_index="ent_idx",
        ),
    )
    result = await configs_router.validate_memory_config(
        config=config, service=svc, group_context=ctx
    )
    assert result["valid"] is True


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_no_config():
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS, databricks_config=None
    )
    result = await configs_router.validate_memory_config(
        config=config, service=svc, group_context=ctx
    )
    assert result["valid"] is False


# ─── initialize_lakebase_tables ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_initialize_lakebase_tables_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await lakebase_router.initialize_lakebase_tables(
            request={}, service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_initialize_lakebase_tables_success():
    svc = AsyncMock()
    svc.initialize_lakebase_tables = AsyncMock(return_value={"success": True})
    ctx = AdminCtx(is_admin=True)
    result = await lakebase_router.initialize_lakebase_tables(
        request={"instance_name": "my-lakebase", "embedding_dimension": 768},
        service=svc,
        group_context=ctx,
    )
    assert result["success"] is True


# ─── get_lakebase_table_stats ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_table_stats():
    svc = AsyncMock()
    svc.get_lakebase_table_stats = AsyncMock(return_value={"tables": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_table_stats(
        service=svc, group_context=ctx
    )
    assert "tables" in result


# ─── get_lakebase_table_data ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_table_data():
    svc = AsyncMock()
    svc.get_lakebase_table_data = AsyncMock(return_value={"documents": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_table_data(
        service=svc, group_context=ctx, table_name="crew_short_term_memory", limit=50
    )
    assert "documents" in result


# ─── get_lakebase_entity_data ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_entity_data():
    svc = AsyncMock()
    svc.get_lakebase_entity_data = AsyncMock(return_value={"entities": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_entity_data(
        service=svc, group_context=ctx
    )
    assert "entities" in result


# ─── save_lakebase_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_lakebase_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await lakebase_router.save_lakebase_config(
            request=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_save_lakebase_config_success():
    svc = AsyncMock()
    svc.save_lakebase_config = AsyncMock(return_value={"saved": True})
    ctx = AdminCtx(is_admin=True)
    result = await lakebase_router.save_lakebase_config(
        request=MagicMock(), service=svc, group_context=ctx
    )
    assert result is not None
