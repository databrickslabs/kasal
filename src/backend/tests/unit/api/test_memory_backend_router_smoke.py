import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.api.memory_backend_router import (
    get_workspace_url,
    validate_memory_config,
    get_databricks_indexes,
    create_databricks_index,
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

