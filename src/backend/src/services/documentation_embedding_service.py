from typing import Dict, List, Optional, Any
import logging
import traceback
import uuid
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.schemas.memory_backend import MemoryBackendType
from src.core.logger import LoggerManager
from src.repositories.memory_backend_repository import MemoryBackendRepository
from src.services.embedding_queue_service import embedding_queue

# Configure logging
logger = LoggerManager.get_instance().documentation_embedding

class DocumentationEmbeddingService:
    """Service for handling documentation embedding operations."""

    def __init__(self, session: Optional[AsyncSession] = None):
        """Initialize service with optional database session."""
        self.session = session
        self._databricks_storage = None
        self._memory_config = None
        self._checked_config = False
    
    async def _check_databricks_config(self) -> bool:
        """Check if Databricks is configured for documentation storage."""
        # Return cached result if already checked
        if self._checked_config:
            return bool(self._memory_config and self._memory_config.backend_type == MemoryBackendType.DATABRICKS)
        
        self._checked_config = True
        
        try:
            # Documentation is global, so find ANY active Databricks configuration
            from src.schemas.memory_backend import MemoryBackendConfig
            from src.models.memory_backend import MemoryBackend

            # Use the injected session or get a new one
            if self.session:
                repository = MemoryBackendRepository(self.session)
                all_backends = await repository.get_all()
            else:
                from src.db.session import async_session_factory
                async with async_session_factory() as session:
                    repository = MemoryBackendRepository(session)
                    all_backends = await repository.get_all()
            
            # Filter active Databricks backends and sort by created_at descending
            databricks_backends = [
                b for b in all_backends 
                if b.is_active and b.backend_type == MemoryBackendType.DATABRICKS
            ]
            
            if databricks_backends:
                # Sort by created_at descending and take the first (most recent)
                databricks_backends.sort(key=lambda x: x.created_at, reverse=True)
                backend = databricks_backends[0]
                
                # Convert backend model to config schema
                self._memory_config = MemoryBackendConfig(
                    backend_type=backend.backend_type,
                    databricks_config=backend.databricks_config,
                    enable_short_term=backend.enable_short_term,
                    enable_long_term=backend.enable_long_term,
                    enable_entity=backend.enable_entity,
                    custom_config=backend.custom_config
                )
                logger.info(f"Found latest Databricks configuration for documentation storage (from group: {backend.group_id}, created: {backend.created_at})")
                return True
            
            self._memory_config = None
            return False
        except Exception as e:
            logger.warning(f"Failed to check Databricks configuration: {e}")
            self._memory_config = None
            return False
    
    async def _get_databricks_storage(self, user_token: Optional[str] = None):
        """Get or create Databricks storage instance.
        
        Args:
            user_token: Optional user access token for OBO authentication
        """
        if self._databricks_storage:
            # Update the user token on the cached instance for OBO authentication
            # This ensures each request uses the correct user token
            if user_token:
                self._databricks_storage.user_token = user_token
            return self._databricks_storage
            
        if not await self._check_databricks_config():
            return None
            
        try:
            from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
            from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
            
            # Get databricks config first to handle both dict and object forms
            db_config = self._memory_config.databricks_config
            
            # Check if databricks_config is None
            if not db_config:
                logger.warning("Databricks configuration is None, cannot initialize storage")
                return None
            
            # Use document index if configured, otherwise use a dedicated documentation index
            if hasattr(db_config, 'document_index'):
                index_name = db_config.document_index
            elif isinstance(db_config, dict):
                index_name = db_config.get('document_index')
            else:
                index_name = None
            
            if not index_name:
                # Create a default documentation index name
                if hasattr(db_config, 'short_term_index'):
                    short_term_index = db_config.short_term_index
                elif isinstance(db_config, dict):
                    short_term_index = db_config.get('short_term_index', '')
                else:
                    short_term_index = ''
                
                if short_term_index:
                    index_name = short_term_index.rsplit('.', 1)[0] + '.documentation_embeddings'
                else:
                    index_name = 'documentation_embeddings'
                logger.info(f"No document index configured, using: {index_name}")
            
            # Extract configuration values - handle both dict and object forms
            if hasattr(db_config, 'endpoint_name'):
                # It's an object
                # Use document_endpoint_name if available, otherwise fall back to endpoint_name
                endpoint_name = getattr(db_config, 'document_endpoint_name', None) or db_config.endpoint_name
                workspace_url = db_config.workspace_url
                embedding_dimension = db_config.embedding_dimension or 1024
                personal_access_token = db_config.personal_access_token
                service_principal_client_id = db_config.service_principal_client_id
                service_principal_client_secret = db_config.service_principal_client_secret
            elif isinstance(db_config, dict):
                # It's a dictionary
                # Use document_endpoint_name if available, otherwise fall back to endpoint_name
                endpoint_name = db_config.get('document_endpoint_name') or db_config.get('endpoint_name')
                workspace_url = db_config.get('workspace_url')
                embedding_dimension = db_config.get('embedding_dimension', 1024)
                personal_access_token = db_config.get('personal_access_token')
                service_principal_client_id = db_config.get('service_principal_client_id')
                service_principal_client_secret = db_config.get('service_principal_client_secret')
            else:
                logger.error(f"Unexpected databricks_config type: {type(db_config)}")
                return None
            
            logger.info(f"Checking if index is ready before initializing Databricks storage - endpoint: {endpoint_name}, index: {index_name}")
            
            # Use DatabricksIndexService to wait for index readiness with retries
            from src.services.databricks_index_service import DatabricksIndexService
            index_service = DatabricksIndexService(workspace_url)
            
            # Wait for index to be ready (with shorter timeout for documentation embedding)
            # 60 seconds should be enough for most cases, but allows for some waiting
            readiness_result = await index_service.wait_for_index_ready(
                workspace_url=workspace_url,
                index_name=index_name,
                endpoint_name=endpoint_name,
                max_wait_seconds=60,  # Wait up to 1 minute
                check_interval_seconds=5,  # Check every 5 seconds
                user_token=user_token  # Pass the user token for authentication
            )
            
            if not readiness_result.get("ready"):
                message = readiness_result.get("message", "Index not ready")
                attempts = readiness_result.get("attempts", 0)
                elapsed_time = readiness_result.get("elapsed_time", 0)
                
                logger.info(f"Index {index_name} not ready after {attempts} attempts ({elapsed_time:.1f}s): {message}")
                logger.info("Skipping Databricks storage initialization - will retry on next embedding attempt")
                return None
            
            logger.info(f"Index {index_name} is ready after {readiness_result.get('attempts', 0)} attempts ({readiness_result.get('elapsed_time', 0):.1f}s)")
            
            # Only create DatabricksVectorStorage if index is ready
            logger.info(f"Index is ready, initializing Databricks storage with endpoint: {endpoint_name}, index: {index_name}")
            
            self._databricks_storage = DatabricksVectorStorage(
                endpoint_name=endpoint_name,
                index_name=index_name,
                crew_id="documentation",  # Static crew ID for documentation
                memory_type="document",
                embedding_dimension=embedding_dimension,
                workspace_url=workspace_url,
                personal_access_token=personal_access_token,
                service_principal_client_id=service_principal_client_id,
                service_principal_client_secret=service_principal_client_secret,
                user_token=user_token  # Pass OBO token for Databricks Apps authentication
            )
            
            logger.info(f"Successfully initialized Databricks storage for documentation with endpoint: {endpoint_name}, index: {index_name}")
            return self._databricks_storage
            
        except Exception as e:
            logger.error(f"Failed to initialize Databricks storage: {e}")
            return None
    
    async def create_documentation_embedding(
        self,
        doc_embedding: DocumentationEmbeddingCreate,
        user_token: Optional[str] = None
    ) -> DocumentationEmbedding:
        """Create a new documentation embedding.

        Args:
            doc_embedding: The documentation embedding to create
            user_token: Optional user access token for OBO authentication
        """
        # Check if we should use Databricks
        databricks_storage = await self._get_databricks_storage(user_token=user_token)

        # Determine if we're in local development mode (SQLite or local PostgreSQL)
        import os
        database_type = os.getenv("DATABASE_TYPE", "postgres").lower()
        is_local_dev = (
            database_type == "sqlite" or
            (database_type == "postgres" and os.getenv("POSTGRES_SERVER", "localhost") == "localhost")
        )

        if databricks_storage:
            try:
                # Create a unique ID for the document
                doc_id = str(uuid.uuid4())
                
                # Prepare metadata
                metadata = doc_embedding.doc_metadata or {}
                metadata.update({
                    'source': doc_embedding.source,
                    'title': doc_embedding.title,
                    'created_at': datetime.utcnow().isoformat()
                })
                
                # Save to Databricks
                logger.info(f"Attempting to save to Databricks index: {databricks_storage.index_name}")
                logger.info(f"Document ID: {doc_id}, Content length: {len(doc_embedding.content)}, Embedding dimensions: {len(doc_embedding.embedding)}")
                
                # DatabricksVectorStorage.save() expects a single 'data' dict parameter
                # that includes content, embedding, and metadata
                data = {
                    'content': doc_embedding.content,
                    'embedding': doc_embedding.embedding,
                    'metadata': metadata,
                    'context': {
                        'query_text': doc_embedding.title or '',
                        'session_id': str(doc_id),
                        'interaction_sequence': 0
                    }
                }
                
                # Save using the correct method signature
                await databricks_storage.save(data)
                
                logger.info(f"Successfully saved documentation embedding to Databricks with ID: {doc_id} in index: {databricks_storage.index_name}")
                
                # Verify the document was saved by getting stats
                try:
                    stats = await databricks_storage.get_stats()
                    logger.info(f"Current index stats after save: {stats}")
                except Exception as e:
                    logger.error(f"Error getting stats after save: {e}")
                
                # Return a DocumentationEmbedding object for consistency
                return DocumentationEmbedding(
                    id=doc_id,  # Using string ID from Databricks
                    source=doc_embedding.source,
                    title=doc_embedding.title,
                    content=doc_embedding.content,
                    doc_metadata=metadata,
                    embedding=doc_embedding.embedding,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
            except Exception as e:
                error_str = str(e)
                logger.error(f"Failed to save to Databricks: {e}")
                logger.error(f"Exception type: {type(e).__name__}")
                logger.error(f"Exception details: {error_str}")
                
                # Check if it's a "not ready" error
                if "not ready" in error_str.lower():
                    logger.warning("Databricks Vector Search index is not ready yet. Documentation will be seeded when the index becomes available.")
                    # Return a placeholder object to indicate partial success
                    return DocumentationEmbedding(
                        id="pending-" + str(uuid.uuid4()),
                        source=doc_embedding.source,
                        title=doc_embedding.title,
                        content=doc_embedding.content,
                        doc_metadata=doc_embedding.doc_metadata or {},
                        embedding=doc_embedding.embedding,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Re-raise the exception for other errors
                raise

        # Only use database storage in local development (SQLite or local PostgreSQL)
        # Skip database storage entirely when:
        # 1. Databricks/Lakebase is configured (even if it failed above)
        # 2. Production PostgreSQL is being used
        if not is_local_dev:
            # In production or with Databricks configured, don't store in database
            logger.info("Skipping database storage (not in local development mode)")
            # Return a placeholder object to indicate the operation was handled
            return DocumentationEmbedding(
                id="skip-db-" + str(uuid.uuid4()),
                source=doc_embedding.source,
                title=doc_embedding.title,
                content=doc_embedding.content,
                doc_metadata=doc_embedding.doc_metadata or {},
                embedding=doc_embedding.embedding,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )

        # Use traditional database storage through repository only in local development
        if not self.session:
            raise ValueError("Session is required for database operations")

        # Use batching service for SQLite to reduce lock contention
        import os
        if database_type == "sqlite":
            logger.info("Using embedding queue service for batch processing")
            # Add to queue for batch processing
            await embedding_queue.add_embedding(
                source=doc_embedding.source,
                title=doc_embedding.title,
                content=doc_embedding.content,
                embedding=doc_embedding.embedding,
                doc_metadata=doc_embedding.doc_metadata
            )
            # Return a placeholder immediately to avoid blocking
            return DocumentationEmbedding(
                id="queued-" + str(uuid.uuid4()),
                source=doc_embedding.source,
                title=doc_embedding.title,
                content=doc_embedding.content,
                doc_metadata=doc_embedding.doc_metadata or {},
                embedding=doc_embedding.embedding,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        else:
            # Create the embedding in the database directly for PostgreSQL
            from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
            repository = DocumentationEmbeddingRepository(self.session)
            return await repository.create(doc_embedding)
    
    async def get_documentation_embedding(
        self, 
        embedding_id: int
    ) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_by_id(embedding_id)
    
    async def get_documentation_embeddings(
        self, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_all(skip, limit)
    
    async def update_documentation_embedding(
        self, 
        embedding_id: int, 
        update_data: Dict[str, Any]
    ) -> Optional[DocumentationEmbedding]:
        """Update a documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.update(embedding_id, update_data)
    
    async def delete_documentation_embedding(
        self, 
        embedding_id: int
    ) -> bool:
        """Delete a documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.delete(embedding_id)
    
    async def search_similar_embeddings(
        self,
        query_embedding: List[float],
        limit: int = 5
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity.
        
        This method automatically detects the storage backend and uses the appropriate
        similarity search implementation:
        - Databricks: Uses Vector Search API
        - PostgreSQL: Uses pgvector extension with <=> operator
        - SQLite: Uses pure SQL implementation with JSON functions
        
        Args:
            query_embedding: The embedding vector to search for
            limit: Maximum number of results to return
            db: Database session (can be AsyncSession or Session)
            
        Returns:
            List of DocumentationEmbedding objects sorted by similarity
        """
        try:
            # Check if we should use Databricks
            databricks_storage = await self._get_databricks_storage()
            if databricks_storage:
                try:
                    # Search in Databricks - call with correct method signature
                    # DatabricksVectorStorage.search expects (query_embedding, k, filters)
                    results = await databricks_storage.search(
                        query_embedding,  # First positional argument
                        k=limit  # Named argument for number of results
                    )
                    
                    # Convert results to DocumentationEmbedding objects
                    similar_docs = []
                    for result in results:
                        # Extract metadata
                        metadata = result.get('metadata', {})
                        
                        doc = DocumentationEmbedding(
                            id=result.get('id', ''),
                            source=metadata.get('source', ''),
                            title=metadata.get('title', ''),
                            content=result.get('content', ''),
                            doc_metadata=metadata,
                            embedding=[],  # Don't return embeddings in search results
                            created_at=datetime.fromisoformat(metadata.get('created_at', datetime.utcnow().isoformat())),
                            updated_at=datetime.fromisoformat(metadata.get('updated_at', metadata.get('created_at', datetime.utcnow().isoformat())))
                        )
                        similar_docs.append(doc)
                    
                    logger.info(f"Found {len(similar_docs)} similar documents in Databricks")
                    return similar_docs
                    
                except Exception as e:
                    logger.error(f"Failed to search in Databricks, falling back to database: {e}")
                    # Fall back to database search
            
            # Traditional database search
            if not self.session:
                logger.warning("No session provided to search_similar_embeddings")
                return []

            logger.debug(f"Session type: {type(self.session)}")

            # Use the repository method for similarity search
            from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
            repository = DocumentationEmbeddingRepository(self.session)

            logger.info("Using repository for similarity search")
            return await repository.search_similar(query_embedding, limit)
                
        except Exception as e:
            logger.error(f"Error in search_similar_embeddings: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return []
    
    async def search_by_source(
        self,
        source: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by source."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.search_by_source(source, skip, limit)
    
    async def search_by_title(
        self,
        title: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by title."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.search_by_title(title, skip, limit)
    
    async def get_recent_embeddings(
        self,
        limit: int = 10
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_recent(limit) 