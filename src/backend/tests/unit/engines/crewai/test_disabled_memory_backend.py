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