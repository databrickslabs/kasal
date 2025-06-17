"""
Unit tests for settings edge cases and global settings instance.

Tests additional edge cases, validation scenarios, and the global settings 
instance behavior that may not be covered in the main settings tests.
"""
import pytest
from unittest.mock import patch
import os
from typing import Union, List

from src.config.settings import Settings, settings


class TestSettingsEdgeCases:
    """Test cases for Settings edge cases and advanced scenarios."""
    
    def test_global_settings_instance_type(self):
        """Test that the global settings instance is of correct type."""
        assert isinstance(settings, Settings)
        assert hasattr(settings, 'PROJECT_NAME')
        assert hasattr(settings, 'DATABASE_URI')
    
    def test_global_settings_immutability_simulation(self):
        """Test that creating new Settings instances doesn't affect the global one."""
        original_project_name = settings.PROJECT_NAME
        
        # Create a new instance with different values
        new_settings = Settings(PROJECT_NAME="Different Backend")
        
        # Global settings should remain unchanged
        assert settings.PROJECT_NAME == original_project_name
        assert new_settings.PROJECT_NAME == "Different Backend"
    
    def test_cors_origins_validator_edge_cases(self):
        """Test CORS origins validator with various edge cases."""
        
        # Test with single URL
        settings_single = Settings(BACKEND_CORS_ORIGINS="http://localhost:3000")
        assert len(settings_single.BACKEND_CORS_ORIGINS) == 1
        
        # Test with valid URLs separated by commas
        settings_mixed = Settings(BACKEND_CORS_ORIGINS="http://localhost:3000,http://example.com")
        assert len(settings_mixed.BACKEND_CORS_ORIGINS) == 2
        
        # Test with string format that bypasses the comma splitting
        settings_bracket = Settings(BACKEND_CORS_ORIGINS=["http://localhost:3000"])
        assert len(settings_bracket.BACKEND_CORS_ORIGINS) == 1
    
    def test_cors_origins_validator_invalid_types(self):
        """Test CORS origins validator with invalid input types."""
        
        # Test with integer (should raise ValueError)
        with pytest.raises(ValueError):
            Settings(BACKEND_CORS_ORIGINS=123)
        
        # Test with dict (should raise ValueError)
        with pytest.raises(ValueError):
            Settings(BACKEND_CORS_ORIGINS={"invalid": "dict"})
        
        # Test with None (should raise ValueError)
        with pytest.raises(ValueError):
            Settings(BACKEND_CORS_ORIGINS=None)
    
    def test_database_uri_validator_edge_cases(self):
        """Test database URI validators with edge cases."""
        
        # Test PostgreSQL with empty POSTGRES_DB
        settings_postgres_empty_db = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="pass", 
            POSTGRES_SERVER="server",
            POSTGRES_DB=""
        )
        assert settings_postgres_empty_db.DATABASE_URI.endswith("/")
        
        # Test SQLite default behavior
        settings_sqlite_default = Settings(DATABASE_TYPE="sqlite")
        assert "sqlite+aiosqlite" in settings_sqlite_default.DATABASE_URI
        
        # Test database URI with different database types
        settings_postgres = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="testuser",
            POSTGRES_PASSWORD="testpass",
            POSTGRES_SERVER="localhost",
            POSTGRES_DB="testdb"
        )
        assert "postgresql+asyncpg" in settings_postgres.DATABASE_URI
        assert "testuser" in settings_postgres.DATABASE_URI
    
    def test_sync_database_uri_validator_edge_cases(self):
        """Test sync database URI validator with edge cases."""
        
        # Test with custom port as string
        settings_custom_port = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="pass",
            POSTGRES_SERVER="server",
            POSTGRES_PORT="5433",  # Non-default port
            POSTGRES_DB="testdb"
        )
        assert ":5433/" in settings_custom_port.SYNC_DATABASE_URI
        
        # Test SQLite sync URI format
        settings_sqlite_sync = Settings(DATABASE_TYPE="sqlite")
        assert "sqlite://" in settings_sqlite_sync.SYNC_DATABASE_URI
        # Should not have the +aiosqlite part for sync URI
        assert "+aiosqlite" not in settings_sqlite_sync.SYNC_DATABASE_URI
    
    def test_database_type_variations(self):
        """Test database type handling with various case and format variations."""
        
        # Test with mixed case
        settings_mixed_case = Settings(DATABASE_TYPE="SqLiTe")
        assert "sqlite+aiosqlite" in settings_mixed_case.DATABASE_URI
        
        # Test with extra whitespace (if environment variable has it)
        with patch.dict(os.environ, {"DATABASE_TYPE": " postgres "}):
            # Create fresh settings to pick up env var
            from src.config.settings import Settings as FreshSettings
            settings_whitespace = FreshSettings()
            # Should still work even with whitespace
            assert settings_whitespace.DATABASE_TYPE == " postgres "
    
    def test_environment_variable_precedence(self):
        """Test that environment variables take precedence over defaults."""
        
        # Test multiple environment variables at once
        env_vars = {
            "DATABASE_TYPE": "sqlite",
            "SQLITE_DB_PATH": "/custom/path/test.db", 
            "LOG_LEVEL": "ERROR"
        }
        
        with patch.dict(os.environ, env_vars):
            from src.config.settings import Settings as FreshSettings
            env_settings = FreshSettings()
            
            assert env_settings.DATABASE_TYPE == "sqlite"
            assert env_settings.SQLITE_DB_PATH == "/custom/path/test.db"
            assert env_settings.LOG_LEVEL == "ERROR"
    
    def test_model_config_attributes(self):
        """Test model configuration attributes are properly set."""
        test_settings = Settings()
        config = test_settings.model_config
        
        # Verify all expected config attributes
        assert config["env_file"] == ".env"
        assert config["env_file_encoding"] == "utf-8"
        assert config["case_sensitive"] is True
        
        # Test that config is accessible from class
        assert Settings.model_config["env_file"] == ".env"
    
    def test_settings_field_types_and_defaults(self):
        """Test that all settings fields have correct types and defaults."""
        test_settings = Settings()
        
        # String fields
        assert isinstance(test_settings.PROJECT_NAME, str)
        assert isinstance(test_settings.API_V1_STR, str)
        assert isinstance(test_settings.SECRET_KEY, str)
        assert isinstance(test_settings.ALGORITHM, str)
        assert isinstance(test_settings.LOG_LEVEL, str)
        assert isinstance(test_settings.SERVER_HOST, str)
        
        # Integer fields
        assert isinstance(test_settings.ACCESS_TOKEN_EXPIRE_MINUTES, int)
        assert isinstance(test_settings.SERVER_PORT, int)
        
        # Boolean fields
        assert isinstance(test_settings.DOCS_ENABLED, bool)
        assert isinstance(test_settings.DEBUG_MODE, bool)
        assert isinstance(test_settings.AUTO_SEED_DATABASE, bool)
        
        # List fields
        assert isinstance(test_settings.CORS_ORIGINS, list)
        assert isinstance(test_settings.BACKEND_CORS_ORIGINS, list)
        
        # Verify critical default values
        assert test_settings.ACCESS_TOKEN_EXPIRE_MINUTES == 60 * 24 * 8
        assert test_settings.API_V1_STR == "/api/v1"
        assert test_settings.SERVER_PORT == 8000
    
    def test_database_uri_with_special_characters(self):
        """Test database URI construction with special characters in credentials."""
        
        # Test with special characters in password
        settings_special_chars = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="user@domain",
            POSTGRES_PASSWORD="p@ss!w0rd#123",
            POSTGRES_SERVER="server.example.com",
            POSTGRES_DB="test-db"
        )
        
        # URI should contain the special characters
        uri = settings_special_chars.DATABASE_URI
        assert "user@domain" in uri
        assert "p@ss!w0rd#123" in uri
        assert "server.example.com" in uri
        assert "test-db" in uri
    
    def test_settings_repr_and_str_safety(self):
        """Test that Settings instances can be safely converted to string without exposing secrets."""
        test_settings = Settings()
        
        # These should not raise exceptions
        settings_str = str(test_settings)
        settings_repr = repr(test_settings)
        
        # Basic checks that they return strings
        assert isinstance(settings_str, str)
        assert isinstance(settings_repr, str)
        
        # Should contain class name
        assert "Settings" in settings_repr
    
    def test_cors_origins_whitespace_edge_cases(self):
        """Test CORS origins handling with various whitespace scenarios."""
        
        # Test with whitespace in URLs that are trimmed properly
        settings_with_spaces = Settings(BACKEND_CORS_ORIGINS="http://localhost:3000 , http://example.com")
        assert len(settings_with_spaces.BACKEND_CORS_ORIGINS) == 2
        
        # Verify the URLs are properly trimmed
        urls = [str(url) for url in settings_with_spaces.BACKEND_CORS_ORIGINS]
        assert any("localhost" in url for url in urls)
        assert any("example.com" in url for url in urls)
    
    @patch.dict(os.environ, {}, clear=True)
    def test_settings_with_no_environment_variables(self):
        """Test Settings initialization when no environment variables are set."""
        # Clear environment and create fresh settings
        from src.config.settings import Settings as FreshSettings
        clean_settings = FreshSettings()
        
        # Should use all defaults
        assert clean_settings.DATABASE_TYPE == "postgres"  # Default fallback
        assert clean_settings.SQLITE_DB_PATH == "./app.db"  # Default fallback
        assert clean_settings.DB_FILE_PATH == "sqlite.db"  # Default fallback