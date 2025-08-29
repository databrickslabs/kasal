"""
Factory for creating memory storage backends.

This module provides a factory pattern for creating different memory storage
backends based on configuration.
"""
import logging
from typing import Optional, Dict, Any

from src.core.logger import LoggerManager
from src.schemas.memory_backend import MemoryBackendType, MemoryBackendConfig

logger = LoggerManager.get_instance().crew


class MemoryBackendFactory:
    """Factory for creating memory storage backends."""
    
    @staticmethod
    async def create_memory_backends(
        config: MemoryBackendConfig,
        crew_id: str,
        embedder: Optional[Any] = None,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create memory storage backends based on configuration.
        
        Args:
            config: Memory backend configuration
            crew_id: Unique identifier for the crew
            embedder: Optional embedder to use for generating embeddings
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Dictionary with memory type keys and storage instances
        """
        memory_backends = {}
        
        if config.backend_type == MemoryBackendType.DATABRICKS:
            # Create Databricks Vector Storage backends
            if not config.databricks_config:
                raise ValueError("Databricks configuration is required for Databricks backend")
            
            try:
                from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
                from src.engines.crewai.memory.crewai_databricks_wrapper import CrewAIDatabricksWrapper
                
                # Create short-term memory storage
                if config.enable_short_term and config.databricks_config.short_term_index:
                    logger.info(f"Creating Databricks short-term memory storage for crew {crew_id}")
                    
                    # Build kwargs dynamically to avoid passing None values
                    storage_kwargs = {
                        "endpoint_name": config.databricks_config.endpoint_name,
                        "index_name": config.databricks_config.short_term_index,
                        "crew_id": crew_id,
                        "memory_type": "short_term",
                        "embedding_dimension": config.databricks_config.embedding_dimension or 1024,
                        "workspace_url": config.databricks_config.workspace_url,
                    }
                    
                    # Only add auth parameters if they have values
                    # This allows DatabricksVectorStorage to check env vars when these are None
                    if config.databricks_config.personal_access_token:
                        storage_kwargs["personal_access_token"] = config.databricks_config.personal_access_token
                    if config.databricks_config.service_principal_client_id:
                        storage_kwargs["service_principal_client_id"] = config.databricks_config.service_principal_client_id
                    if config.databricks_config.service_principal_client_secret:
                        storage_kwargs["service_principal_client_secret"] = config.databricks_config.service_principal_client_secret
                    if user_token:
                        storage_kwargs["user_token"] = user_token
                    
                    short_term_storage = DatabricksVectorStorage(**storage_kwargs)
                    # Wrap with CrewAI-compatible wrapper
                    memory_backends['short_term'] = CrewAIDatabricksWrapper(
                        short_term_storage, 
                        embedder, 
                        enable_relationship_retrieval=False  # Not applicable for short-term memory
                    )
                
                # Create long-term memory storage
                if config.enable_long_term and config.databricks_config.long_term_index:
                    logger.info(f"Creating Databricks long-term memory storage for crew {crew_id}")
                    
                    # Build kwargs dynamically to avoid passing None values
                    storage_kwargs = {
                        "endpoint_name": config.databricks_config.endpoint_name,
                        "index_name": config.databricks_config.long_term_index,
                        "crew_id": crew_id,
                        "memory_type": "long_term",
                        "embedding_dimension": config.databricks_config.embedding_dimension or 1024,
                        "workspace_url": config.databricks_config.workspace_url,
                    }
                    
                    # Only add auth parameters if they have values
                    if config.databricks_config.personal_access_token:
                        storage_kwargs["personal_access_token"] = config.databricks_config.personal_access_token
                    if config.databricks_config.service_principal_client_id:
                        storage_kwargs["service_principal_client_id"] = config.databricks_config.service_principal_client_id
                    if config.databricks_config.service_principal_client_secret:
                        storage_kwargs["service_principal_client_secret"] = config.databricks_config.service_principal_client_secret
                    if user_token:
                        storage_kwargs["user_token"] = user_token
                    
                    long_term_storage = DatabricksVectorStorage(**storage_kwargs)
                    # Wrap with CrewAI-compatible wrapper
                    memory_backends['long_term'] = CrewAIDatabricksWrapper(
                        long_term_storage, 
                        embedder, 
                        enable_relationship_retrieval=False  # Not applicable for long-term memory
                    )
                
                # Create entity memory storage
                if config.enable_entity and config.databricks_config.entity_index:
                    logger.info(f"Creating Databricks entity memory storage for crew {crew_id}")
                    
                    # Build kwargs dynamically to avoid passing None values
                    storage_kwargs = {
                        "endpoint_name": config.databricks_config.endpoint_name,
                        "index_name": config.databricks_config.entity_index,
                        "crew_id": crew_id,
                        "memory_type": "entity",
                        "embedding_dimension": config.databricks_config.embedding_dimension or 1024,
                        "workspace_url": config.databricks_config.workspace_url,
                    }
                    
                    # Only add auth parameters if they have values
                    if config.databricks_config.personal_access_token:
                        storage_kwargs["personal_access_token"] = config.databricks_config.personal_access_token
                    if config.databricks_config.service_principal_client_id:
                        storage_kwargs["service_principal_client_id"] = config.databricks_config.service_principal_client_id
                    if config.databricks_config.service_principal_client_secret:
                        storage_kwargs["service_principal_client_secret"] = config.databricks_config.service_principal_client_secret
                    if user_token:
                        storage_kwargs["user_token"] = user_token
                    
                    entity_storage = DatabricksVectorStorage(**storage_kwargs)
                    # Wrap with CrewAI-compatible wrapper, passing relationship retrieval configuration
                    enable_relationship_retrieval = config.enable_relationship_retrieval
                    logger.info(f"MemoryBackendFactory: config.enable_relationship_retrieval = {enable_relationship_retrieval}")
                    memory_backends['entity'] = CrewAIDatabricksWrapper(
                        entity_storage, 
                        embedder, 
                        enable_relationship_retrieval=enable_relationship_retrieval
                    )
                    
            except ImportError as e:
                logger.error(f"Failed to import Databricks storage: {e}")
                raise
            except Exception as e:
                logger.error(f"Failed to create Databricks memory backends: {e}")
                raise
                
        elif config.backend_type == MemoryBackendType.DEFAULT:
            # Create default memory backends using CrewAI's built-in storage
            logger.info(f"Creating default CrewAI memory backends (ChromaDB + SQLite) for crew {crew_id}")
            
            try:
                from crewai.memory import ShortTermMemory, LongTermMemory, EntityMemory
                from crewai.memory.storage.rag_storage import RAGStorage
                from crewai.memory.storage.ltm_sqlite_storage import LTMSQLiteStorage
                
                # Create memory backends based on configuration
                # Important: We don't instantiate the memory classes here, just return the configuration
                # The crew_preparation.py will instantiate them with the proper parameters
                
                # For default backend, we return empty dict and let CrewAI handle the initialization
                # This ensures that CrewAI's default memory initialization works properly
                # with the embedder configuration we've already set up
                logger.info(f"Default memory backend will use ChromaDB for short-term/entity and SQLite for long-term")
                logger.info(f"Storage will be created in: kasal_default_{crew_id}")
                
                # Return empty dict to signal that default CrewAI memory should be used
                # The crew_preparation.py will handle this by setting memory=True and letting
                # CrewAI initialize its own memory with our configured embedder
                return {}
                
            except ImportError as e:
                logger.error(f"Failed to import CrewAI memory classes for default backend: {e}")
                raise
        else:
            logger.warning(f"Unsupported memory backend type: {config.backend_type}")
            return {}
        
        return memory_backends
    
    @staticmethod
    def create_embedder_wrapper(embedder: Any, storage: Any):
        """
        Create a wrapper that combines embedder and storage for CrewAI integration.
        
        Args:
            embedder: The embedder function/object
            storage: The storage backend
            
        Returns:
            A wrapper object that CrewAI can use
        """
        class EmbedderStorageWrapper:
            """Wrapper that combines embedding and storage functionality."""
            
            def __init__(self, embedder, storage):
                self.embedder = embedder
                self.storage = storage
            
            def embed_and_store(self, text: str, metadata: Optional[Dict] = None, agent: Optional[str] = None):
                """Embed text and store in vector database."""
                try:
                    # Generate embedding
                    if hasattr(self.embedder, '__call__'):
                        # If embedder is a function
                        embedding = self.embedder([text])[0]
                    elif hasattr(self.embedder, 'embed'):
                        # If embedder has embed method
                        embedding = self.embedder.embed(text)
                    else:
                        logger.error("Embedder does not have a callable interface")
                        return
                    
                    # Store with embedding
                    value = {
                        'data': text,
                        'embedding': embedding
                    }
                    self.storage.save(value, metadata=metadata, agent=agent)
                    
                except Exception as e:
                    logger.error(f"Error in embed_and_store: {e}")
            
            def search(self, query: str, limit: int = 3, **kwargs):
                """Search for similar content."""
                try:
                    # Generate query embedding
                    if hasattr(self.embedder, '__call__'):
                        query_embedding = self.embedder([query])[0]
                    elif hasattr(self.embedder, 'embed'):
                        query_embedding = self.embedder.embed(query)
                    else:
                        logger.error("Embedder does not have a callable interface")
                        return []
                    
                    # Search with embedding
                    return self.storage.search({'embedding': query_embedding}, limit=limit, **kwargs)
                    
                except Exception as e:
                    logger.error(f"Error in search: {e}")
                    return []
            
            def reset(self):
                """Reset the storage."""
                self.storage.reset()
        
        return EmbedderStorageWrapper(embedder, storage)