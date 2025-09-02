"""
Databricks Vector Search setup service for automated configuration.

This module handles one-click setup and automated configuration of Databricks Vector Search.
"""
from typing import Dict, Any, Optional
from datetime import datetime
import random
import string
import asyncio
import os

from src.schemas.memory_backend import DatabricksMemoryConfig, MemoryBackendType, MemoryBackendCreate
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
from src.schemas.databricks_vector_endpoint import EndpointCreate, EndpointType, EndpointState
from src.schemas.databricks_vector_index import IndexCreate
from src.repositories.databricks_vector_endpoint_repository import DatabricksVectorEndpointRepository
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
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
            # Debug logging for troubleshooting
            logger.info(f"[DEBUG] Starting one-click setup with workspace_url: {workspace_url}")
            logger.info(f"[DEBUG] User token present: {bool(user_token)}")
            if user_token:
                logger.info(f"[DEBUG] User token length: {len(user_token)}")
            
            # Generate unique suffix for naming
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            unique_id = f"{timestamp}_{random_suffix}"
            
            # Create repositories
            endpoint_repo = DatabricksVectorEndpointRepository(workspace_url)
            index_repo = DatabricksVectorIndexRepository(workspace_url)
            
            results = {
                "success": True,
                "message": "Databricks Vector Search setup completed successfully",
                "endpoints": {},
                "indexes": {},
                "config": None
            }
            
            # Step 1: Create memory endpoint (Direct Access)
            memory_endpoint_name = f"kasal_memory_{unique_id}"
            endpoint_request = EndpointCreate(
                name=memory_endpoint_name,
                endpoint_type=EndpointType.STANDARD
            )
            
            endpoint_response = await endpoint_repo.create_endpoint(endpoint_request, user_token)
            
            if endpoint_response.success:
                results["endpoints"]["memory"] = {
                    "name": memory_endpoint_name,
                    "type": "Direct Access",
                    "status": "created" if "already exists" not in endpoint_response.message else "already_exists"
                }
            else:
                raise Exception(f"Failed to create memory endpoint: {endpoint_response.message}")
            
            # Step 2: Create document endpoint (Direct Access)
            doc_endpoint_name = f"kasal_docs_{unique_id}"
            doc_endpoint_request = EndpointCreate(
                name=doc_endpoint_name,
                endpoint_type=EndpointType.STANDARD
            )
            
            doc_endpoint_response = await endpoint_repo.create_endpoint(doc_endpoint_request, user_token)
            
            if doc_endpoint_response.success:
                results["endpoints"]["document"] = {
                    "name": doc_endpoint_name,
                    "type": "Direct Access",
                    "status": "created" if "already exists" not in doc_endpoint_response.message else "already_exists"
                }
            else:
                # Continue without document endpoint
                results["endpoints"]["document"] = {
                    "name": doc_endpoint_name,
                    "error": doc_endpoint_response.error or doc_endpoint_response.message
                }
            
            # Step 3: Create indexes with retry logic
            # Indexes can be created while endpoints are provisioning, but the backing tables
            # might take a moment to be created. We'll retry if we get "table does not exist" errors.
            
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
            
            async def create_index_with_retry(index_type: str, table_name: str, endpoint_name: str, max_retries: int = 3) -> Dict[str, Any]:
                """Create an index with retry logic for table creation delays."""
                index_name = f"{catalog}.{schema}.{table_name}"
                
                # Get schema from centralized definition
                schema_def = DatabricksIndexSchemas.get_schema(index_type)
                if not schema_def:
                    logger.error(f"Unknown index type: {index_type}")
                    return {"name": index_name, "error": f"Unknown index type: {index_type}"}
                
                for attempt in range(max_retries):
                    try:
                        index_request = IndexCreate(
                            name=index_name,
                            endpoint_name=endpoint_name,
                            primary_key="id",
                            embedding_dimension=embedding_dimension,
                            embedding_vector_column="embedding",
                            schema=schema_def
                        )
                        
                        index_response = await index_repo.create_index(index_request, user_token)
                        
                        if index_response.success:
                            logger.info(f"Successfully created index {index_name}")
                            return {
                                "name": index_name,
                                "status": "created"
                            }
                        else:
                            # Check if it's a table doesn't exist error
                            error_msg = str(index_response.message)
                            if "does not exist" in error_msg.lower() and "table" in error_msg.lower():
                                if attempt < max_retries - 1:
                                    wait_time = (attempt + 1) * 10  # 10, 20, 30 seconds
                                    logger.info(f"Table for {index_name} not ready yet, retrying in {wait_time} seconds (attempt {attempt + 1}/{max_retries})")
                                    await asyncio.sleep(wait_time)
                                    continue
                            raise Exception(f"Failed to create index: {index_response.message}")
                    except Exception as e:
                        error_str = str(e)
                        if "already exists" in error_str.lower():
                            logger.info(f"Index {index_name} already exists")
                            return {
                                "name": index_name,
                                "status": "already_exists"
                            }
                        elif "does not exist" in error_str.lower() and attempt < max_retries - 1:
                            # Table/catalog doesn't exist yet, retry
                            wait_time = (attempt + 1) * 10  # 10, 20, 30 seconds
                            logger.info(f"Resource for {index_name} not ready yet, retrying in {wait_time} seconds (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Failed to create index {index_name}: {e}")
                            return {
                                "name": index_name,
                                "error": str(e)
                            }
                
                # If we get here, we've exhausted retries
                return {
                    "name": index_name,
                    "error": f"Failed after {max_retries} attempts - resources may still be provisioning"
                }
            
            # Create indexes with retry logic
            for index_type, table_name, endpoint_name in index_configs:
                result = await create_index_with_retry(index_type, table_name, endpoint_name)
                results["indexes"][index_type] = result
            
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
                    # First, check if there are existing configurations
                    repo = self.uow.memory_backend_repository
                    existing_configs = await repo.get_by_group_id(group_id)
                    
                    # If there are existing configs, we need to handle them properly
                    if existing_configs:
                        logger.info(f"Found {len(existing_configs)} existing memory backend configurations for group {group_id}")
                        
                        # Check if all existing configs are disabled (DEFAULT type)
                        all_disabled = all(
                            existing_config.backend_type == MemoryBackendType.DEFAULT 
                            for existing_config in existing_configs
                        )
                        
                        if all_disabled:
                            # If all configs are disabled, we can safely delete them
                            logger.info("All existing configurations are disabled, deleting them")
                            for config_to_delete in existing_configs:
                                await repo.delete(config_to_delete.id)
                            await self.uow.commit()
                            logger.info(f"Deleted {len(existing_configs)} disabled configurations")
                        else:
                            # If there are non-disabled configs, we should not delete them automatically
                            # Instead, we'll just add the new configuration
                            logger.info("Found active configurations, will add new Databricks configuration alongside them")
                    
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