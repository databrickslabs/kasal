import asyncio
import logging
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.services.knowledge.embedding_queue import embedding_queue

# Configure logging
logger = LoggerManager.get_instance().documentation_embedding


class DocumentationEmbeddingService:
    """Service for handling documentation embedding operations."""

    def __init__(self, session: Optional[AsyncSession] = None):
        """Initialize service with optional database session."""
        self.session = session
        self._memory_config = None
        self._checked_config = False

    async def create_documentation_embedding(
        self,
        doc_embedding: DocumentationEmbeddingCreate,
        user_token: Optional[str] = None,
    ) -> DocumentationEmbedding:
        """Create a new documentation embedding.

        Args:
            doc_embedding: The documentation embedding to create
            user_token: Optional user access token for OBO authentication
        """
        # Document embeddings are stored in the application database's pgvector
        # table (Lakebase pgvector in production, SQLite locally). Databricks Vector
        # Search is no longer used for document embeddings.
        if not self.session:
            raise ValueError("Session is required for database operations")

        import os

        database_type = os.getenv("DATABASE_TYPE", "postgres").lower()

        # Use the batching queue for SQLite to reduce write-lock contention.
        if database_type == "sqlite":
            logger.info("Using embedding queue service for batch processing")
            await embedding_queue.add_embedding(
                source=doc_embedding.source,
                title=doc_embedding.title,
                content=doc_embedding.content,
                embedding=doc_embedding.embedding,
                doc_metadata=doc_embedding.doc_metadata,
                group_id=getattr(doc_embedding, "group_id", None),
                file_path=getattr(doc_embedding, "file_path", None),
            )
            # Return a placeholder immediately to avoid blocking
            return DocumentationEmbedding(
                id="queued-" + str(uuid.uuid4()),
                source=doc_embedding.source,
                title=doc_embedding.title,
                content=doc_embedding.content,
                doc_metadata=doc_embedding.doc_metadata or {},
                group_id=getattr(doc_embedding, "group_id", None),
                file_path=getattr(doc_embedding, "file_path", None),
                embedding=doc_embedding.embedding,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

        # PostgreSQL / Lakebase pgvector — store directly via the repository.
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.create(doc_embedding)

    async def get_documentation_embedding(
        self, embedding_id: int
    ) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_by_id(embedding_id)

    async def get_documentation_embeddings(
        self, skip: int = 0, limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_all(skip, limit)

    async def update_documentation_embedding(
        self, embedding_id: int, update_data: Dict[str, Any]
    ) -> Optional[DocumentationEmbedding]:
        """Update a documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.update(embedding_id, update_data)

    async def delete_documentation_embedding(self, embedding_id: int) -> bool:
        """Delete a documentation embedding by ID."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.delete(embedding_id)

    async def search_similar_embeddings(
        self,
        query_embedding: List[float],
        limit: int = 5,
        group_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity against the app
        database's pgvector table (Lakebase pgvector in production, SQLite locally).

        Scoping:
        - group_id is None  -> built-in CrewAI documentation only (group_id IS NULL).
        - group_id is set    -> that workspace's uploaded knowledge, optionally
          narrowed to file_paths (the crew's knowledge sources).

        Args:
            query_embedding: The embedding vector to search for
            limit: Maximum number of results to return
            group_id: Workspace scope (None = built-in docs)
            file_paths: Optional knowledge-file filter (only with group_id)

        Returns:
            List of DocumentationEmbedding objects sorted by similarity
        """
        try:
            # Similarity search runs against the app DB's pgvector table
            # (Lakebase pgvector in production, SQLite locally). Databricks Vector
            # Search is no longer used for document embeddings.
            if not self.session:
                logger.warning("No session provided to search_similar_embeddings")
                return []

            logger.debug(f"Session type: {type(self.session)}")

            # Use the repository method for pgvector similarity search
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )

            repository = DocumentationEmbeddingRepository(self.session)

            logger.info("Using pgvector repository for similarity search")
            return await repository.search_similar(
                query_embedding, limit, group_id=group_id, file_paths=file_paths
            )

        except Exception as e:
            logger.error(f"Error in search_similar_embeddings: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return []

    async def search_by_source(
        self, source: str, skip: int = 0, limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by source."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.search_by_source(source, skip, limit)

    async def search_by_title(
        self, title: str, skip: int = 0, limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by title."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.search_by_title(title, skip, limit)

    async def get_recent_embeddings(
        self, limit: int = 10
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        if not self.session:
            raise ValueError("Session is required for database operations")
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        repository = DocumentationEmbeddingRepository(self.session)
        return await repository.get_recent(limit)
