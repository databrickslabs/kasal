"""
Memory backend service - facade for all memory backend operations.

This module acts as a facade that delegates to specialized services for different operations.
"""
from typing import List, Optional, Dict, Any, Tuple

from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendConfig, 
    MemoryBackendCreate,
    MemoryBackendUpdate,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork

# Import specialized services
from src.services.memory_backend_base_service import MemoryBackendBaseService
from src.services.memory_config_service import MemoryConfigService
from src.services.databricks_connection_service import DatabricksConnectionService
from src.services.databricks_index_service import DatabricksIndexService
from src.services.databricks_vectorsearch_setup_service import DatabricksVectorSearchSetupService
from src.services.databricks_vectorsearch_verification_service import DatabricksVectorSearchVerificationService

logger = LoggerManager.get_instance().system


class MemoryBackendService:
    """
    Facade service for managing memory backend configurations.
    
    This service delegates to specialized services for different operations:
    - Base CRUD operations -> MemoryBackendBaseService
    - Configuration retrieval -> MemoryConfigService
    - Databricks connections -> DatabricksConnectionService
    - Index operations -> DatabricksIndexService
    - Setup operations -> DatabricksVectorSearchSetupService
    - Verification -> DatabricksVectorSearchVerificationService
    """
    
    def __init__(self, uow: UnitOfWork):
        """
        Initialize the service with all sub-services.
        
        Args:
            uow: Unit of Work instance
        """
        self.uow = uow
        
        # Initialize sub-services
        self._base_service = MemoryBackendBaseService(uow)
        self._config_service = MemoryConfigService(uow)
        self._connection_service = DatabricksConnectionService(uow)
        self._index_service = DatabricksIndexService()
        self._setup_service = DatabricksVectorSearchSetupService(uow)
        self._verification_service = DatabricksVectorSearchVerificationService()
    
    # ===== Base CRUD Operations (delegated to MemoryBackendBaseService) =====
    
    async def create_memory_backend(
        self, 
        group_id: str,
        config: MemoryBackendCreate
    ) -> MemoryBackend:
        """Create a new memory backend configuration."""
        return await self._base_service.create_memory_backend(group_id, config)
    
    async def get_memory_backends(self, group_id: str) -> List[MemoryBackend]:
        """Get all memory backend configurations for a group."""
        return await self._base_service.get_memory_backends(group_id)
    
    async def get_memory_backend(self, group_id: str, backend_id: str) -> Optional[MemoryBackend]:
        """Get a specific memory backend configuration."""
        return await self._base_service.get_memory_backend(group_id, backend_id)
    
    async def get_default_memory_backend(self, group_id: str) -> Optional[MemoryBackend]:
        """Get the default memory backend for a group."""
        return await self._base_service.get_default_memory_backend(group_id)
    
    async def update_memory_backend(
        self,
        group_id: str,
        backend_id: str,
        update_data: MemoryBackendUpdate
    ) -> Optional[MemoryBackend]:
        """Update a memory backend configuration."""
        return await self._base_service.update_memory_backend(group_id, backend_id, update_data)
    
    async def delete_memory_backend(self, group_id: str, backend_id: str) -> bool:
        """Delete a memory backend configuration."""
        return await self._base_service.delete_memory_backend(group_id, backend_id)
    
    async def set_default_backend(self, group_id: str, backend_id: str) -> bool:
        """Set a memory backend as default."""
        return await self._base_service.set_default_backend(group_id, backend_id)
    
    async def get_memory_stats(self, group_id: str, crew_id: str) -> Dict[str, Any]:
        """Get memory usage statistics for a crew."""
        return await self._base_service.get_memory_stats(group_id, crew_id)
    
    async def delete_all_and_create_disabled(self, group_id: str) -> Dict[str, Any]:
        """Delete all memory backend configurations for a group and create a disabled one."""
        return await self._base_service.delete_all_and_create_disabled(group_id)
    
    async def delete_disabled_configurations(self, group_id: str) -> int:
        """Delete all disabled (DEFAULT type) configurations for a group."""
        return await self._base_service.delete_disabled_configurations(group_id)
    
    # ===== Configuration Management (delegated to MemoryConfigService) =====
    
    async def get_active_config(self, group_id: str = None) -> Optional[MemoryBackendConfig]:
        """Get the active memory backend configuration."""
        return await self._config_service.get_active_config(group_id)
    
    # ===== Databricks Connection Operations (delegated to DatabricksConnectionService) =====
    
    async def test_databricks_connection(
        self,
        config: DatabricksMemoryConfig,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test connection to Databricks Vector Search."""
        return await self._connection_service.test_databricks_connection(config, user_token)
    
    async def get_databricks_endpoint_status(
        self,
        workspace_url: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get the status of a Databricks Vector Search endpoint."""
        return await self._connection_service.get_databricks_endpoint_status(
            workspace_url, endpoint_name, user_token
        )
    
    async def _get_databricks_auth_token(
        self,
        workspace_url: str,
        user_token: Optional[str] = None
    ) -> Tuple[str, str]:
        """Get Databricks authentication token with proper fallback."""
        return await self._connection_service.get_databricks_auth_token(
            workspace_url, user_token
        )
    
    # ===== Databricks Index Operations (delegated to DatabricksIndexService) =====
    
    async def create_databricks_index(
        self,
        config: DatabricksMemoryConfig,
        index_type: str,
        catalog: str,
        schema: str,
        table_name: str,
        primary_key: str = "id",
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a Databricks Vector Search index."""
        return await self._index_service.create_databricks_index(
            config, index_type, catalog, schema, table_name, primary_key, user_token
        )
    
    async def get_databricks_indexes(
        self,
        config: DatabricksMemoryConfig,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get available Databricks Vector Search indexes for an endpoint."""
        return await self._index_service.get_databricks_indexes(config, user_token)
    
    async def delete_databricks_index(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Delete a Databricks Vector Search index."""
        return await self._index_service.delete_databricks_index(
            workspace_url, index_name, endpoint_name, user_token
        )
    
    async def delete_databricks_endpoint(
        self,
        workspace_url: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Delete a Databricks Vector Search endpoint."""
        return await self._index_service.delete_databricks_endpoint(
            workspace_url, endpoint_name, user_token
        )
    
    async def get_index_info(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get information about a Databricks Vector Search index including document count."""
        return await self._index_service.get_index_info(
            workspace_url, index_name, endpoint_name, user_token
        )
    
    async def empty_index(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        index_type: str,
        embedding_dimension: int,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Empty a Databricks Vector Search index by deleting all vectors."""
        return await self._index_service.empty_index(
            workspace_url, index_name, endpoint_name, index_type, 
            embedding_dimension, user_token
        )
    
    async def get_index_documents(
        self,
        workspace_url: str,
        endpoint_name: str,
        index_name: str,
        index_type: Optional[str] = None,
        embedding_dimension: int = 1024,
        limit: int = 30,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get documents from a Databricks Vector Search index."""
        return await self._index_service.get_index_documents(
            workspace_url, endpoint_name, index_name, index_type, embedding_dimension, limit, user_token
        )
    
    async def search_vectors(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        query_embedding: List[float],
        memory_type: str,
        k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        user_token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors in a Databricks Vector Search index.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            query_embedding: Query vector for similarity search
            memory_type: Type of memory ("short_term", "long_term", "entity", "document")
            k: Number of results to return
            filters: Optional filters to apply
            user_token: Optional user access token for OBO authentication
            
        Returns:
            List of search results
        """
        try:
            # Delegate to the index service for vector search operations
            return await self._index_service.search_vectors(
                workspace_url, index_name, endpoint_name, query_embedding, 
                memory_type, k, filters, user_token
            )
        except Exception as e:
            logger.error(f"Failed to search vectors in {index_name}: {e}")
            return []
    
    # ===== Databricks Setup Operations (delegated to DatabricksVectorSearchSetupService) =====
    
    async def one_click_databricks_setup(
        self,
        workspace_url: str,
        catalog: str = "ml",
        schema: str = "agents",
        embedding_dimension: int = 1024,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """One-click setup for Databricks Vector Search memory backend."""
        return await self._setup_service.one_click_databricks_setup(
            workspace_url, catalog, schema, embedding_dimension, user_token, group_id
        )
    
    # ===== Databricks Verification Operations (delegated to DatabricksVectorSearchVerificationService) =====
    
    async def verify_databricks_resources(
        self,
        workspace_url: str,
        user_token: Optional[str] = None,
        config: Optional['MemoryBackend'] = None
    ) -> Dict[str, Any]:
        """Verify which Databricks resources actually exist."""
        # Convert MemoryBackend to dict if needed
        config_dict = None
        if config:
            config_dict = {
                'databricks_config': config.databricks_config
            }
        return await self._verification_service.verify_databricks_resources(
            workspace_url, user_token, config_dict
        )
    
    async def get_workspace_url(self) -> Dict[str, Any]:
        """
        Get the Databricks workspace URL from environment variables.
        Checks in order: DATABRICKS_HOST (for Databricks Apps), DATABRICKS_WORKSPACE_URL.
        
        Returns:
            Dict with workspace_url and source, or None values if not found
        """
        import os
        
        # Check for DATABRICKS_HOST first (Databricks Apps environment)
        databricks_host = os.getenv("DATABRICKS_HOST")
        if databricks_host:
            # Ensure it has https:// prefix
            if not databricks_host.startswith("http"):
                databricks_host = f"https://{databricks_host}"
            logger.info(f"Detected workspace URL from DATABRICKS_HOST: {databricks_host}")
            return {
                "workspace_url": databricks_host,
                "source": "DATABRICKS_HOST",
                "detected": True
            }
        
        # Check for DATABRICKS_WORKSPACE_URL (alternative env var for local dev)
        workspace_url = os.getenv("DATABRICKS_WORKSPACE_URL")
        if workspace_url:
            # Ensure it has https:// prefix
            if not workspace_url.startswith("http"):
                workspace_url = f"https://{workspace_url}"
            logger.info(f"Detected workspace URL from DATABRICKS_WORKSPACE_URL: {workspace_url}")
            return {
                "workspace_url": workspace_url,
                "source": "DATABRICKS_WORKSPACE_URL",
                "detected": True
            }
        
        # No workspace URL found in environment
        logger.info("No workspace URL detected in environment variables")
        return {
            "workspace_url": None,
            "source": None,
            "detected": False
        }