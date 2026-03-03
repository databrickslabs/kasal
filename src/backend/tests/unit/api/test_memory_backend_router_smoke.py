import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.api.memory_backend_router import (
    get_workspace_url,
    validate_memory_config,
    get_databricks_indexes,
    create_databricks_index,
    get_lakebase_table_data,
    get_lakebase_entity_data,
)
import importlib
from src.schemas.memory_backend import MemoryBackendConfig, DatabricksMemoryConfig, MemoryBackendType


class Ctx:
    def __init__(self, user_role='admin', primary_group_id='g1'):
        self.user_role = user_role
        self.primary_group_id = primary_group_id


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_errors_and_valid():
    svc = AsyncMock()
    ctx = Ctx()
    # Missing required fields -> errors
    cfg = MemoryBackendConfig(backend_type=MemoryBackendType.DATABRICKS,
                              databricks_config=DatabricksMemoryConfig(endpoint_name='ep', short_term_index=''))
    out = await validate_memory_config(config=cfg, service=svc, group_context=ctx)
    assert out['valid'] is False
    assert any('Short-term memory index' in e or 'required' in e for e in out['errors'])

    # Valid minimal config
    cfg2 = MemoryBackendConfig(backend_type=MemoryBackendType.DATABRICKS,
                               databricks_config=DatabricksMemoryConfig(endpoint_name='ep', short_term_index='c.s.i', embedding_dimension=1024))
    out2 = await validate_memory_config(config=cfg2, service=svc, group_context=ctx)
    assert out2['valid'] is True


@pytest.mark.asyncio
async def test_get_workspace_url_and_indexes_and_connection():
    ctx = Ctx()
    svc = AsyncMock()
    # workspace url
    svc.get_workspace_url = AsyncMock(return_value={"workspace_url": "https://x"})
    ws = await get_workspace_url(service=svc, group_context=ctx)
    assert ws["workspace_url"] == "https://x"

    # test connection
    with patch('src.api.memory_backend_router.extract_user_token_from_request', return_value='tok'):
        svc.test_databricks_connection = AsyncMock(return_value={"success": True})
        cfg = DatabricksMemoryConfig(endpoint_name='ep', short_term_index='c.s.i', embedding_dimension=1024)
        m = importlib.import_module('src.api.memory_backend_router')
        out = await m.test_databricks_connection(config=cfg, request=None, group_context=ctx, service=svc)
        assert out["success"] is True

        # indexes
        svc.get_databricks_indexes = AsyncMock(return_value={"indexes": ["i1"]})
        out2 = await get_databricks_indexes(config=cfg, request=None, group_context=ctx, service=svc)
        assert out2["indexes"] == ["i1"]


@pytest.mark.asyncio
async def test_create_databricks_index_validations_and_success():
    ctx = Ctx()
    svc = AsyncMock()

    # Missing required params -> 400
    with pytest.raises(Exception):
        await create_databricks_index(request={"config": {"endpoint_name": "ep", "short_term_index": "c.s.i"}}, req=None, group_context=ctx, service=svc)

    # Valid path
    req = {
        "config": {"endpoint_name": "ep", "short_term_index": "c.s.i", "embedding_dimension": 1024},
        "index_type": "short_term",
        "catalog": "c",
        "schema": "s",
        "table_name": "t",
        "primary_key": "id"
    }
    with patch('src.api.memory_backend_router.extract_user_token_from_request', return_value='tok'):
        svc.create_databricks_index = AsyncMock(return_value={"success": True})
        out = await create_databricks_index(request=req, req=None, group_context=ctx, service=svc)
        assert out["success"] is True


@pytest.mark.asyncio
async def test_get_lakebase_table_data():
    """Test the GET /lakebase/table-data endpoint."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.get_lakebase_table_data = AsyncMock(return_value={
        "success": True,
        "documents": [{"id": "d1", "text": "hello", "agent": "researcher"}],
        "total": 1,
    })

    result = await get_lakebase_table_data(
        group_context=ctx,
        service=svc,
        table_name="crew_short_term_memory",
        limit=50,
        instance_name=None,
    )
    assert result["success"] is True
    assert result["total"] == 1
    assert result["documents"][0]["id"] == "d1"
    svc.get_lakebase_table_data.assert_awaited_once_with(
        table_name="crew_short_term_memory",
        limit=50,
        instance_name=None,
    )


@pytest.mark.asyncio
async def test_get_lakebase_table_data_with_instance():
    """Test the GET /lakebase/table-data endpoint with instance name."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.get_lakebase_table_data = AsyncMock(return_value={
        "success": True,
        "documents": [],
        "total": 0,
    })

    result = await get_lakebase_table_data(
        group_context=ctx,
        service=svc,
        table_name="crew_long_term_memory",
        limit=10,
        instance_name="kasal-lakebase1",
    )
    assert result["success"] is True
    svc.get_lakebase_table_data.assert_awaited_once_with(
        table_name="crew_long_term_memory",
        limit=10,
        instance_name="kasal-lakebase1",
    )


@pytest.mark.asyncio
async def test_get_lakebase_entity_data():
    """Test the GET /lakebase/entity-data endpoint."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.get_lakebase_entity_data = AsyncMock(return_value={
        "entities": [{"id": "e1", "name": "Alice", "type": "person"}],
        "relationships": [{"source": "e1", "target": "e2", "type": "knows"}],
    })

    result = await get_lakebase_entity_data(
        group_context=ctx,
        service=svc,
        entity_table="crew_entity_memory",
        limit=200,
        instance_name=None,
    )
    assert len(result["entities"]) == 1
    assert result["entities"][0]["name"] == "Alice"
    assert len(result["relationships"]) == 1
    svc.get_lakebase_entity_data.assert_awaited_once_with(
        entity_table="crew_entity_memory",
        limit=200,
        instance_name=None,
    )


@pytest.mark.asyncio
async def test_get_lakebase_entity_data_with_custom_params():
    """Test the GET /lakebase/entity-data endpoint with custom parameters."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.get_lakebase_entity_data = AsyncMock(return_value={
        "entities": [],
        "relationships": [],
    })

    result = await get_lakebase_entity_data(
        group_context=ctx,
        service=svc,
        entity_table="crew_entity_memory",
        limit=50,
        instance_name="kasal-lakebase1",
    )
    assert result["entities"] == []
    assert result["relationships"] == []

