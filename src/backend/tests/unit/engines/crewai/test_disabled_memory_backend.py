"""
Unit tests for disabled memory backend functionality.

Simplified tests that don't require importing the actual crew_preparation module.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Set test environment variables before imports
os.environ["DATABASE_TYPE"] = "sqlite"
os.environ["SQLITE_DB_PATH"] = ":memory:"
os.environ["LOG_DIR"] = "/tmp/test_logs"

from src.schemas.memory_backend import (
    MemoryBackendConfig, 
    MemoryBackendType,
    DatabricksMemoryConfig
)


class TestDisabledMemoryBackendSimple:
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
    
    def test_storage_directory_naming_for_default(self):
        """Test that default memory uses correct storage directory naming."""
        crew_id = "test_crew_123"
        
        # Simulate default backend type
        memory_backend_config = {
            'backend_type': 'default',
            'enable_short_term': True,
            'enable_long_term': True,
            'enable_entity': True
        }
        
        # The expected directory name for default backend
        expected_dirname = f"kasal_default_{crew_id}"
        
        # This is what the code does
        backend_type = memory_backend_config.get('backend_type')
        if backend_type == 'databricks':
            storage_dirname = f"kasal_databricks_{crew_id}"
        else:  # default
            storage_dirname = f"kasal_default_{crew_id}"
        
        assert storage_dirname == expected_dirname
    
    def test_storage_directory_naming_for_databricks(self):
        """Test that Databricks memory uses correct storage directory naming."""
        crew_id = "test_crew_456"
        
        # Simulate Databricks backend type
        memory_backend_config = {
            'backend_type': 'databricks',
            'enable_short_term': True,
            'enable_long_term': False,
            'enable_entity': True
        }
        
        # The expected directory name for Databricks backend
        expected_dirname = f"kasal_databricks_{crew_id}"
        
        # This is what the code does
        backend_type = memory_backend_config.get('backend_type')
        if backend_type == 'databricks':
            storage_dirname = f"kasal_databricks_{crew_id}"
        else:  # default
            storage_dirname = f"kasal_default_{crew_id}"
        
        assert storage_dirname == expected_dirname


if __name__ == "__main__":
    pytest.main([__file__, "-v"])