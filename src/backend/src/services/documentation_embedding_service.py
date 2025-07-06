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
    
    def _get_database_type(self, db) -> str:
        """Detect the database type from the session."""
        try:
            if hasattr(db, 'bind') and db.bind:
                dialect_name = db.bind.dialect.name.lower()
                return dialect_name
            elif hasattr(db, 'get_bind') and db.get_bind():
                dialect_name = db.get_bind().dialect.name.lower()
                return dialect_name
            else:
                # Fallback: try to detect from settings
                from src.config.settings import settings
                return settings.DATABASE_TYPE.lower()
        except Exception as e:
            logger.warning(f"Could not detect database type, defaulting to postgres: {e}")
            return "postgres"
    
    async def _sqlite_cosine_similarity_search(
        self,
        query_embedding: List[float],
        limit: int,
        db
    ) -> List[DocumentationEmbedding]:
        """
        Pure SQL implementation of cosine similarity for SQLite.
        
        This implementation uses JSON functions and mathematical operations
        to calculate cosine similarity without requiring pgvector extension.
        """
        try:
            import json
            query_json = json.dumps(query_embedding)
            
            # Pure SQL cosine similarity calculation
            # Formula: cosine_similarity = dot_product / (norm_a * norm_b)
            similarity_query = text("""
                WITH vector_calculations AS (
                    SELECT 
                        id,
                        source,
                        title,
                        content,
                        doc_metadata,
                        created_at,
                        updated_at,
                        embedding,
                        -- Parse JSON and calculate dot product with query vector
                        (
                            SELECT SUM(
                                CAST(d.value AS REAL) * CAST(q.value AS REAL)
                            )
                            FROM json_each(embedding) d, json_each(:query_vector) q
                            WHERE d.key = q.key
                        ) AS dot_product,
                        -- Calculate norm of document vector
                        (
                            SELECT SQRT(SUM(
                                CAST(value AS REAL) * CAST(value AS REAL)
                            ))
                            FROM json_each(embedding)
                        ) AS doc_norm,
                        -- Query vector norm (calculated once)
                        (
                            SELECT SQRT(SUM(
                                CAST(value AS REAL) * CAST(value AS REAL)
                            ))
                            FROM json_each(:query_vector)
                        ) AS query_norm
                    FROM documentation_embeddings
                    WHERE embedding IS NOT NULL
                )
                SELECT 
                    id, source, title, content, doc_metadata, created_at, updated_at,
                    -- Calculate cosine similarity
                    CASE 
                        WHEN doc_norm > 0 AND query_norm > 0 
                        THEN dot_product / (doc_norm * query_norm)
                        ELSE 0 
                    END AS similarity
                FROM vector_calculations
                WHERE similarity > 0
                ORDER BY similarity DESC
                LIMIT :limit_val
            """)
            
            result = await db.execute(similarity_query, {
                "query_vector": query_json,
                "limit_val": limit
            })
            rows = result.all()
            
            # Convert rows to DocumentationEmbedding objects
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
                    embedding=[]  # Don't need to return the embedding
                )
                similar_docs.append(doc)
            
            logger.info(f"SQLite cosine similarity found {len(similar_docs)} similar documents")
            return similar_docs
            
        except Exception as e:
            logger.error(f"Error in SQLite cosine similarity search: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return []
    
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
        
        This method automatically detects the database type and uses the appropriate
        similarity search implementation:
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
            if not db:
                logger.warning("No database session provided to search_similar_embeddings")
                return []
            
            # Detect database type
            db_type = self._get_database_type(db)
            logger.info(f"Detected database type: {db_type}")
            
            # Use appropriate similarity search based on database type
            if db_type == "sqlite":
                logger.info("Using SQLite pure SQL cosine similarity implementation")
                return await self._sqlite_cosine_similarity_search(query_embedding, limit, db)
            else:
                # PostgreSQL implementation with pgvector
                logger.info("Using PostgreSQL pgvector implementation")
                return await self._postgres_vector_similarity_search(query_embedding, limit, db)
                
        except Exception as e:
            logger.error(f"Error in search_similar_embeddings: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return []
    
    async def _postgres_vector_similarity_search(
        self,
        query_embedding: List[float],
        limit: int,
        db
    ) -> List[DocumentationEmbedding]:
        """
        PostgreSQL implementation using pgvector extension.
        """
        # Check if we're using an AsyncSession
        if isinstance(db, AsyncSession):
            logger.info("Using AsyncSession for PostgreSQL similarity search")
            
            try:
                # First approach: Use SQLAlchemy ORM with properly formatted vector
                # Format the embedding as a vector string for PostgreSQL
                embedding_str = f"[{','.join(str(x) for x in query_embedding)}]"
                base_query = select(DocumentationEmbedding)
                query = base_query.order_by(text("embedding <=> :embedding")).limit(limit)
                result = await db.execute(query, {"embedding": embedding_str})
                similar_docs = result.scalars().all()
                
                logger.info(f"Found {len(similar_docs)} similar documents with SQLAlchemy ORM approach")
                return similar_docs
            except Exception as orm_error:
                # Log the error but try the fallback approach
                logger.warning(f"Error with SQLAlchemy ORM approach: {str(orm_error)}")
                logger.warning("Trying fallback with raw SQL approach")
                
                # Fallback: Use completely raw SQL
                try:
                    # Properly format the embedding array for PostgreSQL vector type
                    embedding_str = f"[{','.join(str(x) for x in query_embedding)}]"
                    
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