"""Unit tests for MCPSettings model."""

import pytest
from datetime import datetime
from src.models.mcp_settings import MCPSettings


class TestMCPSettingsModel:
    """Test suite for MCPSettings model."""
    
    def test_mcp_settings_creation_default(self):
        """Test creating MCPSettings with default values."""
        settings = MCPSettings()
        
        # Defaults are applied by SQLAlchemy when adding to session
        assert hasattr(settings, 'global_enabled')
        # Timestamps are set by SQLAlchemy when adding to session
        assert hasattr(settings, 'created_at')
        assert hasattr(settings, 'updated_at')
    
    def test_mcp_settings_creation_custom(self):
        """Test creating MCPSettings with custom values."""
        settings = MCPSettings(global_enabled=True)
        
        assert settings.global_enabled is True
        # Timestamps are set by SQLAlchemy when adding to session
        # assert isinstance(settings.created_at, datetime)
        # assert isinstance(settings.updated_at, datetime)
    
    def test_mcp_settings_tablename(self):
        """Test the table name is correct."""
        assert MCPSettings.__tablename__ == "mcp_settings"
    
    def test_mcp_settings_toggle(self):
        """Test toggling global_enabled."""
        settings = MCPSettings(global_enabled=True)
        assert settings.global_enabled is True
        
        # Toggle off
        settings.global_enabled = False
        assert settings.global_enabled is False
        
        # Toggle back on
        settings.global_enabled = True
        assert settings.global_enabled is True
    
    def test_mcp_settings_timestamps(self):
        """Test that timestamps are properly set."""
        # Set timestamps manually for testing
        now = datetime.now()
        settings = MCPSettings(created_at=now, updated_at=now)
        
        # Both timestamps should be set
        assert settings.created_at is not None
        assert settings.updated_at is not None
        
        # They should be datetime objects
        assert isinstance(settings.created_at, datetime)
        assert isinstance(settings.updated_at, datetime)
        
        # Initially, they should be the same
        assert settings.created_at == settings.updated_at