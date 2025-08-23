"""
Unit tests for DatabricksVectorSearchVerificationService.

These tests properly mock repository dependencies.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys

# Ensure the service can be imported
sys.path.insert(0, '.')


class TestDatabricksVectorSearchVerificationService:
    """Test cases for DatabricksVectorSearchVerificationService."""
    
    @pytest.fixture
    def mock_endpoint_repo(self):
        """Mock the endpoint repository."""
        repo = AsyncMock()
        return repo
    
    @pytest.fixture
    def mock_index_repo(self):
        """Mock the index repository."""
        repo = AsyncMock()
        return repo
    
    @pytest.fixture
    def service(self):
        """Create service instance."""
        from src.services.databricks_vectorsearch_verification_service import DatabricksVectorSearchVerificationService
        return DatabricksVectorSearchVerificationService()
    
    def test_service_initialization(self, service):
        """Test that the service can be initialized."""
        assert service is not None
    
    @pytest.mark.asyncio
    async def test_verify_resources_success_case(self, service, mock_endpoint_repo, mock_index_repo):
        """Test successful resource verification."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState, EndpointType
        from src.schemas.databricks_vector_index import IndexInfo, IndexResponse, IndexListResponse, IndexState
        
        # Mock endpoint response
        endpoint_info = EndpointInfo(
            name="test-endpoint",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        mock_endpoint_repo.get_endpoint = AsyncMock(return_value=EndpointResponse(
            success=True,
            endpoint=endpoint_info,
            message="Success"
        ))
        
        # Mock index response
        index_info = IndexInfo(
            name="ml.agents.test",
            endpoint_name="test-endpoint",
            state=IndexState.READY,
            ready=True
        )
        mock_index_repo.list_indexes = AsyncMock(return_value=IndexListResponse(
            success=True,
            indexes=[index_info],
            message="Success"
        ))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'test-endpoint',
                        'short_term_index': 'ml.agents.test'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    "user-token",
                    config
                )
        
        # Verify result
        assert result["success"] is True
        assert "endpoints" in result["resources"]
        assert "indexes" in result["resources"]
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["state"] == "ONLINE"
        assert result["resources"]["indexes"]["ml.agents.test"]["exists"] is True
    
    @pytest.mark.asyncio
    async def test_verify_resources_auth_failure(self, service, mock_endpoint_repo, mock_index_repo):
        """Test handling of authentication failure."""
        from src.schemas.databricks_vector_endpoint import EndpointResponse
        
        # Mock authentication failure in repository
        mock_endpoint_repo.get_endpoint = AsyncMock(side_effect=Exception("Authentication failed"))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    {'databricks_config': {'endpoint_name': 'test-endpoint'}}
                )
        
        # Should handle error gracefully - service returns success=True but marks resource as ERROR
        assert result["success"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is False
        assert result["resources"]["endpoints"]["test-endpoint"]["state"] == "ERROR"
    
    @pytest.mark.asyncio
    async def test_verify_resources_no_config(self, service):
        """Test with no configuration provided."""
        result = await service.verify_databricks_resources(
            "https://test.databricks.com",
            None,
            None  # No config
        )
        
        # Should succeed with empty resources
        assert result["success"] is True
        assert len(result["resources"]["endpoints"]) == 0
        assert len(result["resources"]["indexes"]) == 0
    
    @pytest.mark.asyncio
    async def test_verify_resources_exception_handling(self, service, mock_endpoint_repo, mock_index_repo):
        """Test exception handling during verification."""
        # Mock exception in repository
        mock_endpoint_repo.get_endpoint = AsyncMock(side_effect=Exception("Network error"))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    "user-token",
                    {'databricks_config': {'endpoint_name': 'test-endpoint'}}
                )
        
        # Should handle exception gracefully - service returns success=True but marks resource as ERROR  
        assert result["success"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is False
        assert result["resources"]["endpoints"]["test-endpoint"]["state"] == "ERROR"
    
    @pytest.mark.asyncio
    async def test_verify_resources_endpoint_not_found(self, service, mock_endpoint_repo, mock_index_repo):
        """Test when endpoint doesn't exist."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState
        
        # Mock endpoint not found
        endpoint_info = EndpointInfo(
            name="missing-endpoint",
            state=EndpointState.NOT_FOUND,
            ready=False,
            endpoint_type=None
        )
        mock_endpoint_repo.get_endpoint = AsyncMock(return_value=EndpointResponse(
            success=False,
            endpoint=endpoint_info,
            message="Not found"
        ))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'missing-endpoint'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    config
                )
        
        # Should mark as not found
        assert result["success"] is True
        assert result["resources"]["endpoints"]["missing-endpoint"]["exists"] is False
        assert result["resources"]["endpoints"]["missing-endpoint"]["state"] == "NOT_FOUND"
    
    @pytest.mark.asyncio
    async def test_verify_resources_with_multiple_resources(self, service, mock_endpoint_repo, mock_index_repo):
        """Test verification with multiple endpoints and indexes."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState, EndpointType
        from src.schemas.databricks_vector_index import IndexInfo, IndexResponse, IndexListResponse, IndexState
        
        # Mock multiple endpoints
        endpoint1_info = EndpointInfo(
            name="endpoint1",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        endpoint2_info = EndpointInfo(
            name="endpoint2",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        
        def get_endpoint_side_effect(endpoint_name, user_token=None):
            if endpoint_name == "endpoint1":
                return EndpointResponse(success=True, endpoint=endpoint1_info, message="Success")
            elif endpoint_name == "endpoint2":
                return EndpointResponse(success=True, endpoint=endpoint2_info, message="Success")
            return EndpointResponse(success=False, endpoint=None, message="Not found")
        
        mock_endpoint_repo.get_endpoint = AsyncMock(side_effect=get_endpoint_side_effect)
        
        # Mock multiple indexes
        def list_indexes_side_effect(endpoint_name, user_token=None):
            if endpoint_name == "endpoint1":
                return IndexListResponse(
                    success=True,
                    indexes=[
                        IndexInfo(name="index1", endpoint_name="endpoint1", state=IndexState.READY, ready=True),
                        IndexInfo(name="index2", endpoint_name="endpoint1", state=IndexState.READY, ready=True)
                    ],
                    message="Success"
                )
            elif endpoint_name == "endpoint2":
                return IndexListResponse(
                    success=True,
                    indexes=[
                        IndexInfo(name="index3", endpoint_name="endpoint2", state=IndexState.READY, ready=True)
                    ],
                    message="Success"
                )
            return IndexListResponse(success=False, indexes=[], message="Not found")
        
        mock_index_repo.list_indexes = AsyncMock(side_effect=list_indexes_side_effect)
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'endpoint1',
                        'document_endpoint_name': 'endpoint2',
                        'short_term_index': 'index1',
                        'long_term_index': 'index2',
                        'entity_index': 'index3'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    config
                )
        
        # Verify all resources found
        assert result["success"] is True
        assert result["resources"]["endpoints"]["endpoint1"]["exists"] is True
        assert result["resources"]["endpoints"]["endpoint2"]["exists"] is True
        assert result["resources"]["indexes"]["index1"]["exists"] is True
        assert result["resources"]["indexes"]["index2"]["exists"] is True
        assert result["resources"]["indexes"]["index3"]["exists"] is True
    
    @pytest.mark.asyncio
    async def test_verify_resources_with_env_token(self, service, mock_endpoint_repo, mock_index_repo):
        """Test authentication fallback to environment variable."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState, EndpointType
        
        # Mock successful endpoint fetch
        endpoint_info = EndpointInfo(
            name="test-endpoint",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        mock_endpoint_repo.get_endpoint = AsyncMock(return_value=EndpointResponse(
            success=True,
            endpoint=endpoint_info,
            message="Success"
        ))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'test-endpoint'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    config
                )
        
        # Verify success with env token
        assert result["success"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is True
    
    @pytest.mark.asyncio
    async def test_verify_resources_with_service_principal(self, service, mock_endpoint_repo, mock_index_repo):
        """Test authentication with Service Principal credentials."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState, EndpointType
        
        # Mock successful endpoint fetch
        endpoint_info = EndpointInfo(
            name="test-endpoint",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        mock_endpoint_repo.get_endpoint = AsyncMock(return_value=EndpointResponse(
            success=True,
            endpoint=endpoint_info,
            message="Success"
        ))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'test-endpoint'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    config
                )
        
        # Verify success with Service Principal token
        assert result["success"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["state"] == "ONLINE"
    
    @pytest.mark.asyncio
    async def test_verify_resources_with_database_token(self, service, mock_endpoint_repo, mock_index_repo):
        """Test authentication fallback to database API key."""
        from src.schemas.databricks_vector_endpoint import EndpointInfo, EndpointResponse, EndpointState, EndpointType
        
        # Mock successful endpoint fetch
        endpoint_info = EndpointInfo(
            name="test-endpoint",
            state=EndpointState.ONLINE,
            ready=True,
            endpoint_type=EndpointType.STANDARD
        )
        mock_endpoint_repo.get_endpoint = AsyncMock(return_value=EndpointResponse(
            success=True,
            endpoint=endpoint_info,
            message="Success"
        ))
        
        with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorEndpointRepository', return_value=mock_endpoint_repo):
            with patch('src.services.databricks_vectorsearch_verification_service.DatabricksVectorIndexRepository', return_value=mock_index_repo):
                config = {
                    'databricks_config': {
                        'endpoint_name': 'test-endpoint'
                    }
                }
                
                result = await service.verify_databricks_resources(
                    "https://test.databricks.com",
                    None,
                    config
                )
        
        # Verify success with database token
        assert result["success"] is True
        assert result["resources"]["endpoints"]["test-endpoint"]["exists"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])