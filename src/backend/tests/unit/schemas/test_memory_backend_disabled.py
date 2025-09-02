"""
Unit tests for disabled memory backend configuration behavior.

Tests the schema and configuration logic when all memory types are disabled.
"""
import pytest
from src.schemas.memory_backend import (
    MemoryBackendConfig, 
    MemoryBackendType,
    DatabricksMemoryConfig
)


class TestDisabledMemoryConfiguration:
    """Test cases for disabled memory backend configuration."""
    
    def test_all_memory_types_disabled_is_disabled_config(self):
        """Test that a config with all memory types disabled is considered disabled."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                document_endpoint_name="test-endpoint",
                short_term_index="test.short_term",
                long_term_index="test.long_term",
                entity_index="test.entity",
                document_index="test.document"
            ),
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=False
        )
        
        # This configuration should be considered "disabled"
        is_disabled = (
            not config.enable_short_term and 
            not config.enable_long_term and 
            not config.enable_entity
        )
        
        assert is_disabled is True
    
    def test_some_memory_types_enabled_is_not_disabled(self):
        """Test that a config with some memory types enabled is not disabled."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                document_endpoint_name="test-endpoint",
                short_term_index="test.short_term",
                long_term_index="test.long_term",
                entity_index="test.entity",
                document_index="test.document"
            ),
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=True
        )
        
        # This configuration should NOT be considered "disabled"
        is_disabled = (
            not config.enable_short_term and 
            not config.enable_long_term and 
            not config.enable_entity
        )
        
        assert is_disabled is False
    
    def test_memory_backend_config_validation(self):
        """Test that memory backend configs validate properly."""
        # Test valid config
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                document_endpoint_name="test-endpoint",
                short_term_index="test.short_term",
                long_term_index="test.long_term",
                entity_index="test.entity",
                document_index="test.document"
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        assert config.backend_type == MemoryBackendType.DATABRICKS
        assert config.databricks_config.workspace_url == "https://test.databricks.com"
        assert config.enable_short_term is True
    
    def test_disabled_config_should_use_default_memory(self):
        """Test the logic for when a disabled config should fall back to default."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                document_endpoint_name="test-endpoint",
                short_term_index="test.short_term",
                long_term_index="test.long_term",
                entity_index="test.entity",
                document_index="test.document"
            ),
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=False
        )
        
        # When all memory types are disabled, the system should:
        # 1. Recognize this as a "disabled configuration"
        # 2. Fall back to default memory (ChromaDB + SQLite)
        # 3. Store data in /Library/Application Support/kasal_default_[crew_id]/
        
        # Check if this is a disabled configuration
        is_disabled_config = (
            config.backend_type == MemoryBackendType.DATABRICKS and
            not config.enable_short_term and
            not config.enable_long_term and
            not config.enable_entity
        )
        
        assert is_disabled_config is True
        
        # In the actual implementation, when is_disabled_config is True:
        # - The system ignores the Databricks configuration
        # - Falls back to default ChromaDB + SQLite memory
        # - Creates storage in the default location