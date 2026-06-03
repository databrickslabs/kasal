"""
Coverage tests for src/engines/crewai/memory/memory_backend_factory.py

Updated for the app-modes refactoring:
- create_unified_storage() replaces per-type create_memory_backends()
- create_memory_backends() is a legacy shim returning {"unified": backend} or {}
- _validate_databricks_index() (singular) validates one index
- DatabricksMemoryConfig uses memory_index field
- LakebaseMemoryConfig uses memory_table field
- create_embedder_wrapper() was removed; no replacement needed

These tests cover the same logical branches as before but against the new API.
"""
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.memory.memory_backend_factory import (
    MemoryBackendFactory,
    DatabricksIndexValidationError,
)
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
)


# ─── DatabricksIndexValidationError ──────────────────────────────────────────

def test_databricks_index_validation_error_attrs():
    result = {
        "error_type": "missing_index",
        "missing_indexes": ["catalog.schema.mem"],
        "provisioning_indexes": [],
    }
    exc = DatabricksIndexValidationError("index missing", result)
    assert str(exc) == "index missing"
    assert exc.error_type == "missing_index"
    assert exc.missing_indexes == ["catalog.schema.mem"]
    assert exc.provisioning_indexes == []
    assert exc.validation_result is result


def test_databricks_index_validation_error_defaults():
    exc = DatabricksIndexValidationError("msg", {})
    assert exc.error_type == "unknown"
    assert exc.missing_indexes == []
    assert exc.provisioning_indexes == []


# ─── _validate_databricks_index (singular) ────────────────────────────────────

@pytest.mark.asyncio
async def test_validate_databricks_index_skips_no_workspace_url():
    # No workspace_url → returns without error (skips validation)
    await MemoryBackendFactory._validate_databricks_index(
        workspace_url=None,
        endpoint_name="ep",
        index_name="catalog.schema.mem",
        user_token=None,
        group_id=None,
    )


@pytest.mark.asyncio
async def test_validate_databricks_index_skips_no_endpoint_name():
    # No endpoint_name → returns without error (skips validation)
    await MemoryBackendFactory._validate_databricks_index(
        workspace_url="https://example.com",
        endpoint_name="",
        index_name="catalog.schema.mem",
        user_token=None,
        group_id=None,
    )


@pytest.mark.asyncio
async def test_validate_databricks_index_skips_no_index_name():
    # No index_name → returns without error (skips validation)
    await MemoryBackendFactory._validate_databricks_index(
        workspace_url="https://example.com",
        endpoint_name="ep",
        index_name="",
        user_token=None,
        group_id=None,
    )


def _make_repo_module(describe_result=None, describe_side_effect=None):
    """Create a mock module for DatabricksVectorIndexRepository."""
    mock_repo = AsyncMock()
    if describe_side_effect:
        mock_repo.describe_index.side_effect = describe_side_effect
    else:
        mock_repo.describe_index.return_value = describe_result or {}
    mock_module = MagicMock()
    mock_module.DatabricksVectorIndexRepository = MagicMock(return_value=mock_repo)
    return mock_module, mock_repo


@pytest.mark.asyncio
async def test_validate_databricks_index_passes_when_online():
    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "ONLINE", "ready": True}},
    })

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        # Should not raise
        await MemoryBackendFactory._validate_databricks_index(
            workspace_url="https://example.com",
            endpoint_name="ep",
            index_name="catalog.schema.mem",
            user_token=None,
            group_id=None,
        )


@pytest.mark.asyncio
async def test_validate_databricks_index_raises_missing():
    mock_module, _ = _make_repo_module(describe_result={
        "success": False,
        "message": "index not found",
    })

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError) as exc_info:
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )
    assert exc_info.value.error_type == "missing_index"


@pytest.mark.asyncio
async def test_validate_databricks_index_raises_provisioning():
    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "PROVISIONING", "ready": False}},
    })

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError) as exc_info:
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )
    assert exc_info.value.error_type == "provisioning_indexes"


@pytest.mark.asyncio
async def test_validate_databricks_index_raises_unexpected_state():
    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "FAILED", "ready": False}},
    })

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError) as exc_info:
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )
    assert exc_info.value.error_type == "unexpected_state"


@pytest.mark.asyncio
async def test_validate_databricks_index_raises_on_describe_exception():
    mock_module, _ = _make_repo_module(describe_side_effect=RuntimeError("connection timeout"))

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError) as exc_info:
            await MemoryBackendFactory._validate_databricks_index(
                workspace_url="https://example.com",
                endpoint_name="ep",
                index_name="catalog.schema.mem",
                user_token=None,
                group_id=None,
            )
    assert exc_info.value.error_type == "describe_failed"


# ─── create_memory_backends - DEFAULT backend ─────────────────────────────────

@pytest.mark.asyncio
async def test_create_memory_backends_default_type():
    config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
    result = await MemoryBackendFactory.create_memory_backends(
        config=config, crew_id="test_crew_uuid"
    )
    assert result == {}


@pytest.mark.asyncio
async def test_create_memory_backends_unsupported_type():
    config = MagicMock()
    config.backend_type = "UNKNOWN_TYPE"
    result = await MemoryBackendFactory.create_memory_backends(
        config=config, crew_id="test_crew_uuid"
    )
    assert result == {}


@pytest.mark.asyncio
async def test_create_memory_backends_databricks_no_config():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=None,
    )
    with pytest.raises(ValueError, match="Databricks configuration is required"):
        await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew_uuid"
        )


@pytest.mark.asyncio
async def test_create_memory_backends_databricks_missing_index_raises():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=DatabricksMemoryConfig(
            endpoint_name="ep",
            memory_index="catalog.schema.mem",
            workspace_url="https://example.com",
        ),
    )

    mock_module, _ = _make_repo_module(describe_result={"success": False, "message": "not found"})
    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_uuid"
            )


@pytest.mark.asyncio
async def test_create_memory_backends_databricks_provisioning_raises():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=DatabricksMemoryConfig(
            endpoint_name="ep",
            memory_index="catalog.schema.mem",
            workspace_url="https://example.com",
        ),
    )

    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "PROVISIONING", "ready": False}},
    })
    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        with pytest.raises(DatabricksIndexValidationError):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="grp_crew_uuid"
            )


# ─── create_memory_backends - LAKEBASE backend ───────────────────────────────

@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_no_config():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=None,
    )
    with pytest.raises(ValueError, match="Lakebase configuration is required"):
        await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew_uuid"
        )


@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_returns_unified():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
    )
    mock_backend = MagicMock()
    mock_lakebase_module = MagicMock()
    mock_lakebase_module.LakebaseStorageBackend = MagicMock(return_value=mock_backend)

    with patch.dict(sys.modules, {
        "src.engines.crewai.memory.lakebase_storage_backend": mock_lakebase_module,
    }):
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="g1_crew_abc123"
        )

    assert "unified" in result
    assert result["unified"] is mock_backend


@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_with_job_id():
    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=LakebaseMemoryConfig(memory_table="crew_memory"),
    )
    captured_kwargs = {}

    def capture(**kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    mock_lakebase_module = MagicMock()
    mock_lakebase_module.LakebaseStorageBackend = MagicMock(side_effect=capture)

    with patch.dict(sys.modules, {
        "src.engines.crewai.memory.lakebase_storage_backend": mock_lakebase_module,
    }):
        await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="g1_crew_abc123", job_id="job-456"
        )

    assert captured_kwargs.get("session_id") == "job-456"
