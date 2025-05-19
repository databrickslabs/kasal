from typing import Dict, List, Optional, Any, Union
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
import traceback

from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from sqlalchemy import text, select

# Configure logging
logger = logging.getLogger(__name__)

class DocumentationEmbeddingService:
    """Service for handling documentation embedding operations."""
    
    def __init__(self, repository: DocumentationEmbeddingRepository = None):
        self.repository = repository or DocumentationEmbeddingRepository()
    
    async def create_documentation_embedding(
        self, 
        doc_embedding: DocumentationEmbeddingCreate,
        db=None
    ) -> DocumentationEmbedding:
        """Create a new documentation embedding."""
        return await self.repository.create(db, doc_embedding)
    
    def get_documentation_embedding(
        self, 
        embedding_id: int,
        db=None
    ) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        return self.repository.get_by_id(db, embedding_id)
    
    def get_documentation_embeddings(
        self, 
        skip: int = 0, 
        limit: int = 100,
        db=None
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        return self.repository.get_all(db, skip, limit)
    
    def update_documentation_embedding(
        self, 
        embedding_id: int, 
        update_data: Dict[str, Any],
        db=None
    ) -> Optional[DocumentationEmbedding]:
        """Update a documentation embedding by ID."""
        return self.repository.update(db, embedding_id, update_data)
    
    def delete_documentation_embedding(
        self, 
        embedding_id: int,
        db=None
    ) -> bool:
        """Delete a documentation embedding by ID."""
        return self.repository.delete(db, embedding_id)
    
    async def search_similar_embeddings(
        self,
        query_embedding: List[float],
        limit: int = 5,
        db=None
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity.
        
        This method supports both sync and async sessions.
        
        Args:
            query_embedding: The embedding vector to search for
            limit: Maximum number of results to return
            db: Database session (can be AsyncSession or Session)
            
        Returns:
            List of DocumentationEmbedding objects sorted by similarity
        """
        try:
            if not db:
                logger.warning("No database session provided to search_similar_embeddings")
                return []
                
            # Check if we're using an AsyncSession
            if isinstance(db, AsyncSession):
                logger.info("Using AsyncSession for similarity search")
                
                try:
                    # First approach: Use a more compatible approach with SQLAlchemy ORM
                    base_query = select(DocumentationEmbedding)
                    query = base_query.order_by(text("embedding <=> :embedding")).limit(limit)
                    result = await db.execute(query, {"embedding": query_embedding})
                    similar_docs = result.scalars().all()
                    
                    logger.info(f"Found {len(similar_docs)} similar documents with SQLAlchemy ORM approach")
                    return similar_docs
                except Exception as orm_error:
                    # Log the error but try the fallback approach
                    logger.warning(f"Error with SQLAlchemy ORM approach: {str(orm_error)}")
                    logger.warning("Trying fallback with raw SQL approach")
                    
                    # Fallback: Use completely raw SQL
                    try:
                        # Properly format the embedding array for PostgreSQL
                        embedding_str = str(query_embedding).replace('[', '{').replace(']', '}')
                        
                        # Raw SQL query with direct embedding array notation
                        raw_query = text(f"""
                            SELECT id, source, title, content, doc_metadata, created_at, updated_at
                            FROM documentation_embeddings
                            ORDER BY embedding <=> '{embedding_str}'::vector
                            LIMIT {limit}
                        """)
                        
                        result = await db.execute(raw_query)
                        rows = result.all()
                        
                        # Map rows to DocumentationEmbedding objects
                        similar_docs = []
                        for row in rows:
                            doc = DocumentationEmbedding(
                                id=row.id,
                                source=row.source,
                                title=row.title,
                                content=row.content,
                                doc_metadata=row.doc_metadata,
                                created_at=row.created_at,
                                updated_at=row.updated_at,
                                # We don't have the embedding in the result, but we don't need it
                                embedding=[]
                            )
                            similar_docs.append(doc)
                        
                        logger.info(f"Found {len(similar_docs)} similar documents with raw SQL approach")
                        return similar_docs
                    except Exception as raw_sql_error:
                        logger.error(f"Error with raw SQL approach: {str(raw_sql_error)}")
                        raise
            else:
                # Fall back to synchronous version
                logger.info("Using synchronous Session for similarity search")
                return self.repository.search_similar(db, query_embedding, limit)
        except Exception as e:
            logger.error(f"Error in search_similar_embeddings: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return []
    
    def search_by_source(
        self,
        source: str,
        skip: int = 0,
        limit: int = 100,
        db=None
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by source."""
        return self.repository.search_by_source(db, source, skip, limit)
    
    def search_by_title(
        self,
        title: str,
        skip: int = 0,
        limit: int = 100,
        db=None
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by title."""
        return self.repository.search_by_title(db, title, skip, limit)
    
    def get_recent_embeddings(
        self,
        limit: int = 10,
        db=None
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        return self.repository.get_recent(db, limit) 