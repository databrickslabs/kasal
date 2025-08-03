"""
Databricks Vector Search setup service for automated configuration.

This module handles one-click setup and automated configuration of Databricks Vector Search.
"""
from typing import Dict, Any, Optional
from datetime import datetime
import random
import string
import asyncio

from src.schemas.memory_backend import DatabricksMemoryConfig, MemoryBackendType, MemoryBackendCreate
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork
from src.services.memory_backend_base_service import MemoryBackendBaseService

logger = LoggerManager.get_instance().system


class DatabricksVectorSearchSetupService:
    """Service for automated Databricks Vector Search setup."""
    
    def __init__(self, uow: UnitOfWork = None):
        """
        Initialize the service.
        
        Args:
            uow: Unit of Work instance (optional)
        """
        self.uow = uow
    
    async def one_click_databricks_setup(
        self,
        workspace_url: str,
        catalog: str = "ml",
        schema: str = "agents",
        embedding_dimension: int = 1024,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        One-click setup for Databricks Vector Search memory backend.
        Creates all necessary endpoints and indexes automatically.
        
        Args:
            workspace_url: Databricks workspace URL
            catalog: Catalog name (default: ml)
            schema: Schema name (default: agents)
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Setup result with created resources
        """
        try:
            from databricks.vector_search.client import VectorSearchClient
            
            # Generate unique suffix for naming
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            unique_id = f"{timestamp}_{random_suffix}"
            
            # Get authenticated client
            client = None
            if user_token:
                try:
                    from src.utils.databricks_auth import get_databricks_auth_headers
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=workspace_url,
                                personal_access_token=token
                            )
                except Exception:
                    pass
            
            if not client:
                # Try API key service or default auth
                client = VectorSearchClient(workspace_url=workspace_url)
            
            results = {
                "success": True,
                "message": "Databricks Vector Search setup completed successfully",
                "endpoints": {},
                "indexes": {},
                "config": None
            }
            
            # Step 1: Create memory endpoint (Direct Access)
            memory_endpoint_name = f"kasal_memory_{unique_id}"
            try:
                client.create_endpoint(
                    name=memory_endpoint_name,
                    endpoint_type="STANDARD"  # Direct Access capable
                )
                results["endpoints"]["memory"] = {
                    "name": memory_endpoint_name,
                    "type": "Direct Access",
                    "status": "created"
                }
            except Exception as e:
                if "already exists" in str(e).lower():
                    results["endpoints"]["memory"] = {
                        "name": memory_endpoint_name,
                        "type": "Direct Access",
                        "status": "already_exists"
                    }
                else:
                    raise
            
            # Step 2: Create document endpoint (Direct Access)
            doc_endpoint_name = f"kasal_docs_{unique_id}"
            try:
                client.create_endpoint(
                    name=doc_endpoint_name,
                    endpoint_type="STANDARD"  # Direct Access capable
                )
                results["endpoints"]["document"] = {
                    "name": doc_endpoint_name,
                    "type": "Direct Access",
                    "status": "created"
                }
            except Exception as e:
                if "already exists" in str(e).lower():
                    results["endpoints"]["document"] = {
                        "name": doc_endpoint_name,
                        "type": "Direct Access",
                        "status": "already_exists"
                    }
                else:
                    # Continue without document endpoint
                    results["endpoints"]["document"] = {
                        "name": doc_endpoint_name,
                        "error": str(e)
                    }
            
            # Wait for endpoints to be ready (simplified - in production, poll status)
            await asyncio.sleep(5)
            
            # Step 3: Create all memory indexes
            index_configs = [
                ("short_term", f"short_term_memory_{unique_id}", memory_endpoint_name),
                ("long_term", f"long_term_memory_{unique_id}", memory_endpoint_name),
                ("entity", f"entity_memory_{unique_id}", memory_endpoint_name),
            ]
            
            # Add document index if document endpoint was created successfully
            if doc_endpoint_name and "error" not in results["endpoints"].get("document", {}):
                # For document indexes, create on the document endpoint (storage optimized)
                # This creates a Direct Access index for flexible CRUD operations
                index_configs.append(
                    ("document", f"document_embeddings_{unique_id}", doc_endpoint_name)
                )
            
            for index_type, table_name, endpoint_name in index_configs:
                index_name = f"{catalog}.{schema}.{table_name}"
                
                # Define schema based on type
                if index_type == "short_term":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string",
                        "agent_id": "string",
                        "content": "string",
                        "embedding": "array<float>",
                        "metadata": "string",
                        "timestamp": "string",
                        "score": "float"
                    }
                elif index_type == "long_term":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string",
                        "agent_id": "string",
                        "content": "string",
                        "embedding": "array<float>",
                        "metadata": "string",
                        "timestamp": "string",
                        "importance": "float"
                    }
                elif index_type == "entity":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string",
                        "agent_id": "string",
                        "entity_type": "string",
                        "entity_name": "string",
                        "embedding": "array<float>",
                        "attributes": "string",
                        "relationships": "string",
                        "timestamp": "string"
                    }
                elif index_type == "document":
                    schema_def = {
                        "id": "string",
                        "source": "string",
                        "title": "string",
                        "content": "string",
                        "embedding": "array<float>",
                        "doc_metadata": "string",
                        "created_at": "string",
                        "updated_at": "string"
                    }
                
                try:
                    client.create_direct_access_index(
                        endpoint_name=endpoint_name,
                        index_name=index_name,
                        primary_key="id",
                        embedding_dimension=embedding_dimension,
                        embedding_vector_column="embedding",
                        schema=schema_def
                    )
                    results["indexes"][index_type] = {
                        "name": index_name,
                        "status": "created"
                    }
                except Exception as e:
                    if "already exists" in str(e).lower():
                        results["indexes"][index_type] = {
                            "name": index_name,
                            "status": "already_exists"
                        }
                    else:
                        results["indexes"][index_type] = {
                            "name": index_name,
                            "error": str(e)
                        }
            
            # Step 4: Create memory backend configuration
            config = DatabricksMemoryConfig(
                endpoint_name=memory_endpoint_name,
                document_endpoint_name=doc_endpoint_name if "error" not in results["endpoints"]["document"] else None,
                short_term_index=results["indexes"].get("short_term", {}).get("name", ""),
                long_term_index=results["indexes"].get("long_term", {}).get("name", ""),
                entity_index=results["indexes"].get("entity", {}).get("name", ""),
                document_index=results["indexes"].get("document", {}).get("name", "") if "document" in results["indexes"] else None,
                workspace_url=workspace_url,
                embedding_dimension=embedding_dimension,
                catalog=catalog,
                schema=schema
            )
            
            results["config"] = config.model_dump()
            results["catalog"] = catalog
            results["schema"] = schema
            
            # Save the configuration if group_id is provided
            if group_id and self.uow:
                try:
                    # First, delete all existing configurations for this group
                    logger.info(f"Deleting all existing memory backend configurations for group {group_id}")
                    try:
                        # Get all configurations for the group
                        repo = self.uow.memory_backend_repository
                        existing_configs = await repo.get_by_group_id(group_id)
                        delete_count = len(existing_configs)
                        
                        # Delete each configuration
                        for config_to_delete in existing_configs:
                            await repo.delete(config_to_delete.id)
                        
                        await self.uow.commit()
                        logger.info(f"Deleted {delete_count} existing memory backend configurations")
                    except Exception as delete_error:
                        logger.error(f"Error deleting existing configurations: {delete_error}")
                        # Continue anyway - we still want to save the new configuration
                    
                    # Save the configuration for the group
                    # Create the memory backend configuration
                    backend_config_data = {
                        "name": f"Databricks Setup {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
                        "description": "Auto-generated Databricks Vector Search configuration",
                        "backend_type": MemoryBackendType.DATABRICKS,
                        "databricks_config": config,
                        "enable_short_term": True,
                        "enable_long_term": True,
                        "enable_entity": True
                    }
                    
                    # Create MemoryBackendCreate schema instance
                    backend_config = MemoryBackendCreate(**backend_config_data)
                    
                    # Save using the existing method
                    base_service = MemoryBackendBaseService(self.uow)
                    saved_backend = await base_service.create_memory_backend(group_id, backend_config)
                    results["backend_id"] = saved_backend.id
                    results["message"] = "Setup completed and configuration saved"
                except Exception as save_error:
                    logger.error(f"Failed to save configuration: {save_error}")
                    if "foreign key constraint" in str(save_error).lower():
                        results["warning"] = "Setup completed but configuration not saved. Please ensure you are logged in."
                        results["info"] = "The Databricks resources were created successfully. You can configure them manually in your agent or crew settings."
                    else:
                        results["warning"] = f"Setup completed but failed to save configuration: {str(save_error)}"
            else:
                results["info"] = "Databricks resources created successfully. Please log in to save the configuration."
            
            return results
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Setup failed: {str(e)}",
                "error": str(e)
            }