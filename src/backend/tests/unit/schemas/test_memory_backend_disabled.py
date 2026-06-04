"""
Unit tests for memory backend schema configuration.

Updated for app-modes: The concept of "disabled configuration" via enable_short_term/
enable_long_term/enable_entity flags has been removed. Now memory is either:
- DEFAULT backend (CrewAI uses its built-in LanceDB)
- DATABRICKS backend (Databricks Vector Search, unified index)
- LAKEBASE backend (pgvector, unified table)

The system falls back to DEFAULT when no active backend is configured.
"""
import pytest
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig,
    LakebaseMemoryConfig,
    CognitiveMemoryConfig,
)


class TestMemoryBackendConfigSchema:
    """Test MemoryBackendConfig Pydantic schema validation."""

    def test_default_backend_config(self):
        """Default backend requires no extra configuration."""
        config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        assert config.backend_type == MemoryBackendType.DEFAULT
        assert config.databricks_config is None
        assert config.lakebase_config is None
        assert config.cognitive_config is None
        assert config.custom_config is None

    def test_databricks_backend_config_with_required_fields(self):
        """Databricks backend requires endpoint_name and memory_index."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="my-endpoint",
                memory_index="catalog.schema.unified",
            ),
        )
        assert config.backend_type == MemoryBackendType.DATABRICKS
        assert config.databricks_config is not None
        assert config.databricks_config.endpoint_name == "my-endpoint"
        assert config.databricks_config.memory_index == "catalog.schema.unified"

    def test_lakebase_backend_config(self):
        """Lakebase backend requires memory_table."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.LAKEBASE,
            lakebase_config=LakebaseMemoryConfig(
                memory_table="crew_memory",
            ),
        )
        assert config.backend_type == MemoryBackendType.LAKEBASE
        assert config.lakebase_config is not None
        assert config.lakebase_config.memory_table == "crew_memory"

    def test_cognitive_config_is_optional(self):
        """cognitive_config is optional in all backend types."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="ep",
                memory_index="cat.sch.unified",
            ),
            cognitive_config=CognitiveMemoryConfig(
                semantic_weight=0.5,
                recency_weight=0.3,
                importance_weight=0.2,
            ),
        )
        assert config.cognitive_config is not None
        assert config.cognitive_config.semantic_weight == 0.5

    def test_default_backend_is_system_fallback(self):
        """DEFAULT backend signals the system to use CrewAI's built-in LanceDB."""
        config = MemoryBackendConfig()  # defaults to DEFAULT
        assert config.backend_type == MemoryBackendType.DEFAULT
        # System uses this to determine "no custom backend configured"
        is_using_default = config.backend_type == MemoryBackendType.DEFAULT
        assert is_using_default is True

    def test_databricks_config_requires_memory_index(self):
        """DatabricksMemoryConfig raises ValidationError without memory_index."""
        with pytest.raises(Exception):  # pydantic ValidationError
            DatabricksMemoryConfig(endpoint_name="ep")  # memory_index missing

    def test_databricks_config_workspace_url_optional(self):
        """workspace_url is optional (can be sourced from env/OBO)."""
        config = DatabricksMemoryConfig(
            endpoint_name="ep",
            memory_index="cat.sch.unified",
            # workspace_url omitted
        )
        assert config.workspace_url is None

    def test_databricks_config_with_all_auth_fields(self):
        """DatabricksMemoryConfig accepts all authentication options."""
        config = DatabricksMemoryConfig(
            endpoint_name="ep",
            memory_index="cat.sch.unified",
            workspace_url="https://example.databricks.com",
            auth_type="pat",
            personal_access_token="dapi-test-token",
        )
        assert config.personal_access_token == "dapi-test-token"
        assert config.auth_type == "pat"

    def test_lakebase_memory_table_default(self):
        """LakebaseMemoryConfig has 'crew_memory' as default table name."""
        config = LakebaseMemoryConfig()
        assert config.memory_table == "crew_memory"

    def test_memory_backend_type_values(self):
        """MemoryBackendType has the expected string values."""
        assert MemoryBackendType.DEFAULT == "default"
        assert MemoryBackendType.DATABRICKS == "databricks"
        assert MemoryBackendType.LAKEBASE == "lakebase"

    def test_config_custom_config_is_dict(self):
        """custom_config accepts arbitrary key-value pairs."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            custom_config={"custom_key": "custom_value", "number": 42},
        )
        assert config.custom_config is not None
        assert config.custom_config["custom_key"] == "custom_value"
        assert config.custom_config["number"] == 42
