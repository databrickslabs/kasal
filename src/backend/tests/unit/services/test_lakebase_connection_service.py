"""
Unit tests for services/lakebase_connection_service.py
"""

import pytest
from unittest.mock import patch
from src.services.lakebase_connection_service import LakebaseConnectionService


class TestLakebaseConnectionService:
    """Tests for LakebaseConnectionService"""

    @pytest.fixture
    def service(self):
        """Create LakebaseConnectionService instance for testing"""
        return LakebaseConnectionService(user_token="fake-token", user_email="test@example.com")

    def test_lakebaseconnectionservice_initialization(self, service):
        """Test LakebaseConnectionService initializes correctly"""
        assert service.user_token == "fake-token"
        assert service.user_email == "test@example.com"
        assert service._workspace_client is None

    def test_get_spn_username_with_env(self, service):
        """Test get_spn_username returns DATABRICKS_CLIENT_ID from env"""
        with patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "test-client-id"}):
            result = service.get_spn_username()
            assert result == "test-client-id"

    def test_get_spn_username_without_env(self, service):
        """Test get_spn_username returns None when env var not set"""
        with patch.dict("os.environ", {}, clear=True):
            result = service.get_spn_username()
            assert result is None
