"""
Coverage tests for src/engines/crewai/memory/memory_backend_factory.py
Targets the DatabricksIndexValidationError class and key branches in
create_memory_backends and _validate_databricks_indexes.
"""
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
)


# ─── DatabricksIndexValidationError ──────────────────────────────────────────

def test_databricks_index_validation_error_attrs():
    result = {
        "error_type": "missing_indexes",
        "missing_indexes": ["short_term: idx1 (not found)"],
        "provisioning_indexes": [],
    }
    exc = DatabricksIndexValidationError("index missing", result)
    assert str(exc) == "index missing"
    assert exc.error_type == "missing_indexes"
    assert exc.missing_indexes == ["short_term: idx1 (not found)"]
    assert exc.provisioning_indexes == []
    assert exc.validation_result is result


def test_databricks_index_validation_error_defaults():
    exc = DatabricksIndexValidationError("msg", {})
    assert exc.error_type == "unknown"
    assert exc.missing_indexes == []
    assert exc.provisioning_indexes == []


# ─── _validate_databricks_indexes ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_validate_databricks_indexes_no_config():
    config = MagicMock()
    config.databricks_config = None
    result = await MemoryBackendFactory._validate_databricks_indexes(config)
    assert result == (True, [], [], [])


def _make_repo_module(describe_result=None, describe_side_effect=None):
    """Create a mock module for DatabricksVectorIndexRepository."""
    import sys
    mock_repo = AsyncMock()
    if describe_side_effect:
        mock_repo.describe_index.side_effect = describe_side_effect
    else:
        mock_repo.describe_index.return_value = describe_result or {}
    mock_module = MagicMock()
    mock_module.DatabricksVectorIndexRepository = MagicMock(return_value=mock_repo)
    return mock_module, mock_repo


@pytest.mark.asyncio
async def test_validate_databricks_indexes_all_ready():
    import sys
    mock_module, mock_repo = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "ONLINE", "ready": True}},
    })

    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = "lt_idx"
    db_config.entity_index = "ent_idx"

    config = MagicMock()
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = True
    config.enable_entity = True

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is True
    assert len(valid) == 3
    assert missing == []
    assert provisioning == []


@pytest.mark.asyncio
async def test_validate_databricks_indexes_provisioning():
    import sys
    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "PROVISIONING", "ready": False}},
    })

    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = None
    db_config.entity_index = None

    config = MagicMock()
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is False
    assert len(provisioning) == 1


@pytest.mark.asyncio
async def test_validate_databricks_indexes_missing():
    import sys
    mock_module, _ = _make_repo_module(describe_result={
        "success": False,
        "error": "not found",
    })

    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = None
    db_config.entity_index = None

    config = MagicMock()
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is False
    assert len(missing) == 1


@pytest.mark.asyncio
async def test_validate_databricks_indexes_unknown_state():
    import sys
    mock_module, _ = _make_repo_module(describe_result={
        "success": True,
        "description": {"status": {"state": "ERROR", "ready": False}},
    })

    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = None
    db_config.entity_index = None

    config = MagicMock()
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is False
    assert len(provisioning) == 1


@pytest.mark.asyncio
async def test_validate_databricks_indexes_exception_per_index():
    import sys
    mock_module, _ = _make_repo_module(describe_side_effect=Exception("timeout"))

    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = None
    db_config.entity_index = None

    config = MagicMock()
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": mock_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is False
    assert len(missing) == 1


@pytest.mark.asyncio
async def test_validate_databricks_indexes_repository_import_error():
    import sys
    # Remove the module from sys.modules to force import error simulation
    config = MagicMock()
    config.databricks_config = MagicMock()
    config.enable_short_term = True
    config.databricks_config.short_term_index = "idx"

    broken_module = MagicMock()
    broken_module.DatabricksVectorIndexRepository = MagicMock(side_effect=Exception("import error"))

    with patch.dict(sys.modules, {
        "src.repositories.databricks_vector_index_repository": broken_module
    }):
        all_valid, valid, missing, provisioning = await MemoryBackendFactory._validate_databricks_indexes(config)

    assert all_valid is False
    assert len(missing) == 1


# ─── create_memory_backends - DEFAULT backend ─────────────────────────────────

@pytest.mark.asyncio
async def test_create_memory_backends_default_type():
    import sys
    config = MagicMock()
    config.backend_type = MemoryBackendType.DEFAULT

    mock_crewai_memory = MagicMock()
    mock_crewai_memory.ShortTermMemory = MagicMock()
    mock_crewai_memory.LongTermMemory = MagicMock()
    mock_crewai_memory.EntityMemory = MagicMock()

    with patch.dict(sys.modules, {
        "crewai.memory": mock_crewai_memory,
        "crewai.memory.storage.ltm_sqlite_storage": MagicMock(),
        "crewai.memory.storage.rag_storage": MagicMock(),
    }):
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
    config = MagicMock()
    config.backend_type = MemoryBackendType.DATABRICKS
    config.databricks_config = None

    with pytest.raises(ValueError, match="Databricks configuration is required"):
        await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew_uuid"
        )


@pytest.mark.asyncio
async def test_create_memory_backends_databricks_missing_indexes_raises():
    db_config = MagicMock()
    db_config.workspace_url = "https://example.com"
    db_config.endpoint_name = "ep"
    db_config.short_term_index = "st_idx"
    db_config.long_term_index = None
    db_config.entity_index = None

    config = MagicMock()
    config.backend_type = MemoryBackendType.DATABRICKS
    config.databricks_config = db_config
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    with patch.object(
        MemoryBackendFactory, "_validate_databricks_indexes",
        new=AsyncMock(return_value=(False, [], ["short_term: st_idx (not found)"], []))
    ):
        with pytest.raises(DatabricksIndexValidationError):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew_uuid"
            )


@pytest.mark.asyncio
async def test_create_memory_backends_databricks_provisioning_raises():
    config = MagicMock()
    config.backend_type = MemoryBackendType.DATABRICKS
    config.databricks_config = MagicMock()

    with patch.object(
        MemoryBackendFactory, "_validate_databricks_indexes",
        new=AsyncMock(return_value=(False, [], [], ["short_term: st_idx (state: PROVISIONING)"]))
    ):
        with pytest.raises(DatabricksIndexValidationError):
            await MemoryBackendFactory.create_memory_backends(
                config=config, crew_id="test_crew_uuid"
            )


# ─── create_memory_backends - LAKEBASE backend ───────────────────────────────

@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_no_config():
    config = MagicMock()
    config.backend_type = MemoryBackendType.LAKEBASE
    config.lakebase_config = None

    with pytest.raises(ValueError, match="Lakebase configuration is required"):
        await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="test_crew_uuid"
        )


@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_short_term_only():
    import sys
    lakebase_cfg = MagicMock()
    lakebase_cfg.short_term_table = "short_term_table"
    lakebase_cfg.long_term_table = "long_term_table"
    lakebase_cfg.entity_table = "entity_table"
    lakebase_cfg.embedding_dimension = 1024
    lakebase_cfg.instance_name = "my-lakebase"

    config = MagicMock()
    config.backend_type = MemoryBackendType.LAKEBASE
    config.lakebase_config = lakebase_cfg
    config.enable_short_term = True
    config.enable_long_term = False
    config.enable_entity = False

    mock_storage = MagicMock()
    mock_wrapper = MagicMock()

    mock_lakebase_module = MagicMock()
    mock_lakebase_module.LakebasePgVectorStorage = MagicMock(return_value=mock_storage)
    mock_wrapper_module = MagicMock()
    mock_wrapper_module.CrewAILakebaseWrapper = MagicMock(return_value=mock_wrapper)

    with patch.dict(sys.modules, {
        "src.engines.crewai.memory.lakebase_pgvector_storage": mock_lakebase_module,
        "src.engines.crewai.memory.crewai_lakebase_wrapper": mock_wrapper_module,
    }):
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="g1_crew_abc123"
        )

    assert "short_term" in result


@pytest.mark.asyncio
async def test_create_memory_backends_lakebase_all_memory():
    import sys
    lakebase_cfg = MagicMock()
    lakebase_cfg.short_term_table = "st"
    lakebase_cfg.long_term_table = "lt"
    lakebase_cfg.entity_table = "ent"
    lakebase_cfg.embedding_dimension = 768
    lakebase_cfg.instance_name = "lb"

    config = MagicMock()
    config.backend_type = MemoryBackendType.LAKEBASE
    config.lakebase_config = lakebase_cfg
    config.enable_short_term = True
    config.enable_long_term = True
    config.enable_entity = True

    mock_wrapper = MagicMock()

    mock_lakebase_module = MagicMock()
    mock_lakebase_module.LakebasePgVectorStorage = MagicMock()
    mock_wrapper_module = MagicMock()
    mock_wrapper_module.CrewAILakebaseWrapper = MagicMock(return_value=mock_wrapper)

    with patch.dict(sys.modules, {
        "src.engines.crewai.memory.lakebase_pgvector_storage": mock_lakebase_module,
        "src.engines.crewai.memory.crewai_lakebase_wrapper": mock_wrapper_module,
    }):
        result = await MemoryBackendFactory.create_memory_backends(
            config=config, crew_id="g1_crew_abc123", job_id="job-123"
        )

    assert "short_term" in result
    assert "long_term" in result
    assert "entity" in result


# ─── create_embedder_wrapper ─────────────────────────────────────────────────

def test_create_embedder_wrapper_callable_embedder():
    mock_embedder = MagicMock(return_value=[[0.1, 0.2, 0.3]])
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)

    wrapper.embed_and_store("some text", metadata={"key": "val"})
    mock_storage.save.assert_called_once()


def test_create_embedder_wrapper_embed_method():
    mock_embedder = MagicMock(spec=["embed"])
    mock_embedder.embed.return_value = [0.1, 0.2]
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    wrapper.embed_and_store("text")
    mock_storage.save.assert_called_once()


def test_create_embedder_wrapper_no_interface():
    # Object with no __call__ (not callable) and no embed method
    class NoInterface:
        def some_other_method(self):
            pass

    mock_embedder = NoInterface()
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    wrapper.embed_and_store("text")  # Should not raise, just log error
    mock_storage.save.assert_not_called()


def test_create_embedder_wrapper_search_callable():
    mock_embedder = MagicMock(return_value=[[0.1, 0.2]])
    mock_storage = MagicMock()
    mock_storage.search.return_value = ["result1"]

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    results = wrapper.search("query", limit=5)
    assert results == ["result1"]


def test_create_embedder_wrapper_search_embed_method():
    mock_embedder = MagicMock(spec=["embed"])
    mock_embedder.embed.return_value = [0.5, 0.6]
    mock_storage = MagicMock()
    mock_storage.search.return_value = []

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    results = wrapper.search("query")
    assert results == []


def test_create_embedder_wrapper_search_no_interface():
    class NoInterface:
        def some_other_method(self):
            pass

    mock_embedder = NoInterface()
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    results = wrapper.search("query")
    assert results == []


def test_create_embedder_wrapper_reset():
    mock_embedder = MagicMock()
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    wrapper.reset()
    mock_storage.reset.assert_called_once()


def test_create_embedder_wrapper_embed_and_store_exception():
    mock_embedder = MagicMock(side_effect=Exception("embed error"))
    mock_storage = MagicMock()

    wrapper = MemoryBackendFactory.create_embedder_wrapper(mock_embedder, mock_storage)
    wrapper.embed_and_store("text")  # Should not raise
    mock_storage.save.assert_not_called()
