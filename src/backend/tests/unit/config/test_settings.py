"""
Unit tests for settings module.

Tests the configuration settings and validators.
"""
import pytest
from unittest.mock import patch
import os

from src.config.settings import Settings


class TestSettings:
    """Test cases for Settings configuration."""
    
    def test_default_settings(self):
        """Test default settings initialization."""
        settings = Settings()
        
        assert settings.PROJECT_NAME == "Modern Backend"
        assert settings.PROJECT_DESCRIPTION == "A modern backend API for the Kasal application"
        assert settings.VERSION == "0.1.0"
        assert settings.API_V1_STR == "/api/v1"
        assert settings.CORS_ORIGINS == ["http://localhost:3000", "http://127.0.0.1:3000"]
        assert settings.DATABASE_TYPE == os.getenv("DATABASE_TYPE", "postgres")
        assert settings.DOCS_ENABLED is True
        assert settings.LOG_LEVEL == "DEBUG"  # Check the actual default from environment
        assert settings.SERVER_HOST == "0.0.0.0"
        assert settings.SERVER_PORT == 8000
        assert settings.DEBUG_MODE is False
        assert settings.AUTO_SEED_DATABASE is True
    
    def test_cors_origins_from_string(self):
        """Test CORS origins parsing from comma-separated string."""
        settings = Settings(BACKEND_CORS_ORIGINS="http://localhost,http://localhost:3000")
        
        assert len(settings.BACKEND_CORS_ORIGINS) == 2
        assert "http://localhost" in str(settings.BACKEND_CORS_ORIGINS[0])
        assert "http://localhost:3000" in str(settings.BACKEND_CORS_ORIGINS[1])
    
    def test_cors_origins_from_list(self):
        """Test CORS origins when provided as list."""
        origins = ["http://localhost", "http://localhost:3000"]
        settings = Settings(BACKEND_CORS_ORIGINS=origins)
        
        assert len(settings.BACKEND_CORS_ORIGINS) == 2
    
    def test_cors_origins_invalid_format(self):
        """Test CORS origins with invalid format."""
        with pytest.raises(ValueError):
            Settings(BACKEND_CORS_ORIGINS=123)  # Invalid type
    
    def test_database_uri_postgres_default(self):
        """Test database URI assembly for PostgreSQL."""
        settings = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="testuser",
            POSTGRES_PASSWORD="testpass",
            POSTGRES_SERVER="testserver",
            POSTGRES_PORT="5432",
            POSTGRES_DB="testdb"
        )
        
        expected_uri = "postgresql+asyncpg://testuser:testpass@testserver:5432/testdb"
        assert settings.DATABASE_URI == expected_uri
    
    def test_database_uri_sqlite(self):
        """Test database URI assembly for SQLite."""
        settings = Settings(
            DATABASE_TYPE="sqlite"
        )
        
        # It uses the default SQLITE_DB_PATH which is "./app.db"
        expected_uri = "sqlite+aiosqlite:///./app.db"
        assert settings.DATABASE_URI == expected_uri
    
    def test_database_uri_custom_string(self):
        """Test database URI when provided as custom string."""
        custom_uri = "postgresql://custom:uri@host:5432/db"
        settings = Settings(DATABASE_URI=custom_uri)
        
        assert settings.DATABASE_URI == custom_uri
    
    def test_sync_database_uri_postgres(self):
        """Test sync database URI assembly for PostgreSQL."""
        settings = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="testuser",
            POSTGRES_PASSWORD="testpass",
            POSTGRES_SERVER="testserver",
            POSTGRES_PORT="5432",
            POSTGRES_DB="testdb"
        )
        
        expected_uri = "postgresql+asyncpg://testuser:testpass@testserver:5432/testdb"
        assert settings.SYNC_DATABASE_URI == expected_uri
    
    def test_sync_database_uri_sqlite(self):
        """Test sync database URI assembly for SQLite."""
        settings = Settings(
            DATABASE_TYPE="sqlite"
        )
        
        # It uses the default SQLITE_DB_PATH which is "./app.db"
        expected_uri = "sqlite:///./app.db"
        assert settings.SYNC_DATABASE_URI == expected_uri
    
    def test_sync_database_uri_custom_string(self):
        """Test sync database URI when provided as custom string."""
        custom_uri = "postgresql://sync:uri@host:5432/db"
        settings = Settings(SYNC_DATABASE_URI=custom_uri)
        
        assert settings.SYNC_DATABASE_URI == custom_uri
    
    def test_database_uri_empty_db_name(self):
        """Test database URI with empty database name."""
        settings = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="pass",
            POSTGRES_SERVER="server",
            POSTGRES_DB=""
        )
        
        # Should still construct valid URI with empty DB
        assert "postgresql+asyncpg://user:pass@server:5432/" in settings.DATABASE_URI
    
    def test_cors_origins_whitespace_handling(self):
        """Test CORS origins with whitespace handling."""
        settings = Settings(BACKEND_CORS_ORIGINS="http://localhost ,  http://localhost:3000  ")
        
        # Should strip whitespace
        assert len(settings.BACKEND_CORS_ORIGINS) == 2
        # Check that URLs are properly parsed despite whitespace
        assert any("localhost" in str(url) for url in settings.BACKEND_CORS_ORIGINS)
    
    def test_env_file_loading(self):
        """Test that settings can load from .env file."""
        # This test verifies the model_config is properly set
        settings = Settings()
        config = settings.model_config
        
        assert config.get("env_file") == ".env"
        assert config.get("env_file_encoding") == "utf-8"
        assert config.get("case_sensitive") is True
    
    def test_database_type_case_insensitive(self):
        """Test database type is case insensitive."""
        settings_upper = Settings(DATABASE_TYPE="SQLITE")
        settings_lower = Settings(DATABASE_TYPE="sqlite")
        
        assert "sqlite+aiosqlite" in settings_upper.DATABASE_URI
        assert "sqlite+aiosqlite" in settings_lower.DATABASE_URI
    
    def test_postgres_default_port(self):
        """Test PostgreSQL uses default port when not specified."""
        settings = Settings(
            DATABASE_TYPE="postgres",
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="pass",
            POSTGRES_SERVER="server",
            POSTGRES_DB="db"
            # POSTGRES_PORT is not set, will use default "5432"
        )
        
        # Should use default port 5432
        assert ":5432/" in settings.DATABASE_URI
    
    def test_environment_variable_override(self):
        """Test that environment variables override defaults."""
        with patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}):
            # Create a new Settings instance to pick up the env var
            from src.config.settings import Settings as FreshSettings
            settings = FreshSettings()
            
            assert settings.DATABASE_TYPE == "sqlite"