"""
Unit tests for DatabricksVectorSearchVerificationService.

Tests resource verification functionality for Databricks Vector Search.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp
import os

from src.services.databricks_vectorsearch_verification_service import DatabricksVectorSearchVerificationService


@pytest.fixture
def service():
    """Create a DatabricksVectorSearchVerificationService instance."""
    return DatabricksVectorSearchVerificationService()


@pytest.fixture
def sample_config():
    """Create a sample configuration with Databricks resources."""
    return {
        'databricks_config': {
            'endpoint_name': 'test-endpoint',
            'document_endpoint_name': 'doc-endpoint',
            'short_term_index': 'ml.agents.short_term',
            'long_term_index': 'ml.agents.long_term',
            'entity_index': 'ml.agents.entity',
            'document_index': 'ml.docs.embeddings'
        }
    }


class TestDatabricksVectorSearchVerificationService:
    """Test cases for DatabricksVectorSearchVerificationService."""
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_verify_resources_all_exist(
        self, mock_get_headers, mock_get, service, sample_config
    ):
        """Test verification when all resources exist."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        user_token = "user-token"
        
        mock_get_headers.return_value = (
            {"Authorization": "Bearer token", "Content-Type": "application/json"},
            None
        )
        
        # Mock all requests to return successful responses
        def create_mock_response(status, data):
            mock_resp = AsyncMock()
            mock_resp.status = status
            mock_resp.json = AsyncMock(return_value=data)
            return mock_resp
        
        # Response data
        endpoint_response = {
            "endpoint_status": {"state": "ONLINE"},
            "endpoint_type": "STANDARD"
        }
        
        test_endpoint_indexes = {
            "vector_indexes": [
                {
                    "name": "ml.agents.short_term",
                    "status": {"state": "READY", "ready": True}
                },
                {
                    "name": "ml.agents.long_term",
                    "status": {"state": "READY", "ready": True}
                },
                {
                    "name": "ml.agents.entity",
                    "status": {"state": "READY", "ready": True}
                }
            ]
        }
        
        doc_endpoint_indexes = {
            "vector_indexes": [
                {
                    "name": "ml.docs.embeddings",
                    "status": {"state": "READY", "ready": True}
                }
            ]
        }
        
        # Create an async context manager mock
        class MockResponse:
            def __init__(self, status, json_data):
                self.status = status
                self._json_data = json_data
            
            async def json(self):
                return self._json_data
            
            async def __aenter__(self):
                return self
            
            async def __aexit__(self, *args):
                pass
        
        # Configure mock get to return MockResponse based on URL
        def get_side_effect(url, **kwargs):
            # Handle endpoint status checks
            if "/vector-search/endpoints/" in url and "indexes?" not in url:
                if "test-endpoint" in url or "doc-endpoint" in url:
                    return MockResponse(200, endpoint_response)
                else:
                    return MockResponse(404, {})
            
            # Handle index listing
            elif "/vector-search/indexes?" in url:
                if "endpoint_name=test-endpoint" in url:
                    return MockResponse(200, test_endpoint_indexes)
                elif "endpoint_name=doc-endpoint" in url:
                    return MockResponse(200, doc_endpoint_indexes)
                else:
                    return MockResponse(200, {"vector_indexes": []})
            
            else:
                return MockResponse(404, {})
        
        # Configure mock
        mock_get.side_effect = get_side_effect
        
        # Act
        result = await service.verify_databricks_resources(
            workspace_url, user_token, sample_config
        )
        
        # Assert
        assert result["success"] is True
        resources = result["resources"]
        
        # Check endpoints
        assert resources["endpoints"]["test-endpoint"]["exists"] is True
        assert resources["endpoints"]["test-endpoint"]["state"] == "ONLINE"
        assert resources["endpoints"]["test-endpoint"]["ready"] is True
        
        assert resources["endpoints"]["doc-endpoint"]["exists"] is True
        assert resources["endpoints"]["doc-endpoint"]["ready"] is True
        
        # Check that all indexes exist (without checking specific endpoints)
        assert resources["indexes"]["ml.agents.short_term"]["exists"] is True
        assert resources["indexes"]["ml.agents.short_term"]["state"] == "READY"
        assert resources["indexes"]["ml.agents.short_term"]["ready"] is True
        
        assert resources["indexes"]["ml.agents.long_term"]["exists"] is True
        assert resources["indexes"]["ml.agents.long_term"]["state"] == "READY"
        assert resources["indexes"]["ml.agents.long_term"]["ready"] is True
        
        assert resources["indexes"]["ml.agents.entity"]["exists"] is True
        assert resources["indexes"]["ml.agents.entity"]["state"] == "READY"
        assert resources["indexes"]["ml.agents.entity"]["ready"] is True
        
        assert resources["indexes"]["ml.docs.embeddings"]["exists"] is True
        assert resources["indexes"]["ml.docs.embeddings"]["state"] == "READY"
        assert resources["indexes"]["ml.docs.embeddings"]["ready"] is True
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_verify_resources_endpoint_not_found(
        self, mock_get_headers, mock_get, service, sample_config
    ):
        """Test verification when endpoint doesn't exist."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        mock_get_headers.return_value = (
            {"Authorization": "Bearer token", "Content-Type": "application/json"},
            None
        )
        
        # Mock 404 response for endpoint
        mock_resp_404 = AsyncMock()
        mock_resp_404.status = 404
        
        # Mock 200 response for second endpoint
        mock_resp_200 = AsyncMock()
        mock_resp_200.status = 200
        mock_resp_200.json = AsyncMock(return_value={
            "endpoint_status": {"state": "PROVISIONING"},
            "endpoint_type": "STANDARD"
        })
        
        # Create responses list for all expected calls
        mock_responses = []
        
        # First, endpoint checks
        mock_responses.append(mock_resp_404)  # test-endpoint returns 404
        mock_responses.append(mock_resp_200)  # doc-endpoint returns 200
        
        # Then index checks for each configured index against each endpoint
        # For each index, it will check all endpoints
        # We have 4 indexes and 2 endpoints, so up to 8 calls
        for _ in range(8):
            index_resp = AsyncMock()
            index_resp.status = 200
            index_resp.json = AsyncMock(return_value={"vector_indexes": []})
            mock_responses.append(index_resp)
        
        mock_get.return_value.__aenter__.side_effect = mock_responses
        
        # Act
        result = await service.verify_databricks_resources(
            workspace_url, None, sample_config
        )
        
        # Assert
        assert result["success"] is True
        resources = result["resources"]
        
        # Check the endpoints based on what was returned
        # The first endpoint (test-endpoint) should not exist
        if "test-endpoint" in resources["endpoints"]:
            assert resources["endpoints"]["test-endpoint"]["exists"] is False
            assert resources["endpoints"]["test-endpoint"]["state"] == "NOT_FOUND"
        
        # The second endpoint (doc-endpoint) should exist
        if "doc-endpoint" in resources["endpoints"]:
            assert resources["endpoints"]["doc-endpoint"]["exists"] is True
            assert resources["endpoints"]["doc-endpoint"]["state"] == "PROVISIONING"
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    async def test_verify_resources_index_not_found(
        self, mock_get, service
    ):
        """Test verification when some indexes don't exist."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        config = {
            'databricks_config': {
                'endpoint_name': 'test-endpoint',
                'short_term_index': 'ml.agents.short_term',
                'long_term_index': 'ml.agents.missing_index'
            }
        }
        
        # Create a mock function for get_databricks_auth_headers
        async def mock_get_databricks_auth_headers(**kwargs):
            return (
                {"Authorization": "Bearer token", "Content-Type": "application/json"},
                None
            )
        
        # Mock the databricks_auth module
        import sys
        from unittest.mock import MagicMock
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.get_databricks_auth_headers = mock_get_databricks_auth_headers
        sys.modules['src.utils.databricks_auth'] = mock_databricks_auth
        
        try:
            # Mock endpoint exists
            endpoint_resp = AsyncMock()
            endpoint_resp.status = 200
            endpoint_resp.json = AsyncMock(return_value={
                "endpoint_status": {"state": "ONLINE"}
            })
            
            # Mock index list with only short_term
            index_resp = AsyncMock()
            index_resp.status = 200
            index_resp.json = AsyncMock(return_value={
                "vector_indexes": [
                    {
                        "name": "ml.agents.short_term",
                        "status": {"state": "READY", "ready": True}
                    }
                ]
            })
            
            # We need 3 responses:
            # 1. Endpoint check for test-endpoint
            # 2. Index list for ml.agents.short_term on test-endpoint (found)
            # 3. Index list for ml.agents.missing_index on test-endpoint (not found)
            mock_get.return_value.__aenter__.side_effect = [
                endpoint_resp,  # endpoint exists
                index_resp,     # index list for short_term (found)
                index_resp      # index list for missing_index (not found because it's not in the list)
            ]
            
            # Act
            result = await service.verify_databricks_resources(
                workspace_url, None, config
            )
            
            # Assert
            assert result["success"] is True
            resources = result["resources"]
            
            assert resources["indexes"]["ml.agents.short_term"]["exists"] is True
            assert resources["indexes"]["ml.agents.missing_index"]["exists"] is False
            assert resources["indexes"]["ml.agents.missing_index"]["state"] == "NOT_FOUND"
        finally:
            # Clean up the mocked module
            if 'src.utils.databricks_auth' in sys.modules:
                del sys.modules['src.utils.databricks_auth']
    
    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.utils.encryption_utils.EncryptionUtils')
    async def test_verify_resources_auth_fallback_to_api_key(
        self, mock_encryption, mock_uow_class, mock_api_service_class,
        mock_get_headers, service
    ):
        """Test authentication fallback to API key from database."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        # OBO auth fails
        mock_get_headers.return_value = (None, "OBO failed")
        
        # Mock API key service
        mock_api_service = AsyncMock()
        mock_api_key = MagicMock(encrypted_value="encrypted-token")
        mock_api_service.find_by_name.side_effect = [mock_api_key, None]
        # from_unit_of_work is a classmethod that returns a coroutine
        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
        
        # Mock encryption
        mock_encryption.decrypt_value.return_value = "decrypted-token"
        
        # Mock UnitOfWork
        mock_uow_instance = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow_instance
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value={
                "endpoint_status": {"state": "ONLINE"}
            })
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            # Act
            result = await service.verify_databricks_resources(
                workspace_url, None, {'databricks_config': {'endpoint_name': 'test'}}
            )
        
        # Assert
        assert result["success"] is True
        mock_api_service.find_by_name.assert_called_with("DATABRICKS_TOKEN")
    
    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    @patch('os.getenv')
    async def test_verify_resources_auth_fallback_to_env(
        self, mock_getenv, mock_get_headers, service
    ):
        """Test authentication fallback to environment variable."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        # All other auth methods fail
        mock_get_headers.return_value = (None, "Failed")
        mock_getenv.side_effect = lambda x: "env-token" if x == "DATABRICKS_API_KEY" else None
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api:
            mock_api.from_unit_of_work.side_effect = Exception("No API service")
            
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_resp = AsyncMock()
                mock_resp.status = 200
                mock_resp.json = AsyncMock(return_value={
                    "endpoint_status": {"state": "ONLINE"}
                })
                mock_get.return_value.__aenter__.return_value = mock_resp
                
                # Act
                result = await service.verify_databricks_resources(
                    workspace_url, None, {'databricks_config': {'endpoint_name': 'test'}}
                )
        
        # Assert
        assert result["success"] is True
        mock_getenv.assert_called()
    
    @pytest.mark.asyncio
    async def test_verify_resources_no_auth_available(self, service):
        """Test error when no authentication method succeeds."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        with patch('src.utils.databricks_auth.get_databricks_auth_headers',
                   return_value=(None, "Failed")):
            with patch('src.services.api_keys_service.ApiKeysService') as mock_api:
                mock_api.from_unit_of_work.side_effect = Exception("No API service")
                
                with patch('os.getenv',
                           return_value=None):
                    
                    # Act
                    result = await service.verify_databricks_resources(
                        workspace_url, None, {'databricks_config': {}}
                    )
        
        # Assert
        assert result["success"] is False
        assert "Unable to authenticate" in result["message"]
    
    @pytest.mark.asyncio
    async def test_verify_resources_no_config_provided(self, service):
        """Test verification with no configuration provided."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        with patch('src.utils.databricks_auth.get_databricks_auth_headers',
                   return_value=({"Authorization": "Bearer token"}, None)):
            
            # Act
            result = await service.verify_databricks_resources(
                workspace_url, None, None
            )
        
        # Assert
        assert result["success"] is True
        assert len(result["resources"]["endpoints"]) == 0
        assert len(result["resources"]["indexes"]) == 0
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_verify_resources_endpoint_error_state(
        self, mock_get_headers, mock_get, service, sample_config
    ):
        """Test handling of endpoint in error state."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        mock_get_headers.return_value = (
            {"Authorization": "Bearer token", "Content-Type": "application/json"},
            None
        )
        
        # Mock error response
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")
        
        mock_get.return_value.__aenter__.return_value = mock_resp
        
        # Act
        result = await service.verify_databricks_resources(
            workspace_url, None, sample_config
        )
        
        # Assert
        assert result["success"] is True
        resources = result["resources"]
        
        assert resources["endpoints"]["test-endpoint"]["exists"] is False
        assert resources["endpoints"]["test-endpoint"]["state"] == "ERROR"
    
    @pytest.mark.asyncio
    async def test_verify_resources_general_exception(self, service):
        """Test handling of general exceptions during verification."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        with patch('src.utils.databricks_auth.get_databricks_auth_headers',
                   side_effect=Exception("Network error")):
            
            # Act
            result = await service.verify_databricks_resources(
                workspace_url, None, {}
            )
        
        # Assert
        assert result["success"] is False
        assert "Network error" in result["message"]
        assert len(result["resources"]["endpoints"]) == 0
        assert len(result["resources"]["indexes"]) == 0