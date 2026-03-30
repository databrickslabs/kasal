from typing import Dict, List, Optional, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.core.base_repository import BaseRepository


class DocumentationEmbeddingRepository(BaseRepository[DocumentationEmbedding]):
    """Repository for managing documentation embeddings in the database."""
    
    def __init__(self, db: Union[AsyncSession, Session]):
        """Initialize repository with database session."""
        super().__init__(DocumentationEmbedding, db)
        self.db = db

    async def create(
        self, 
        doc_embedding: DocumentationEmbeddingCreate
    ) -> DocumentationEmbedding:
        """Create a new documentation embedding in the database."""
        db_embedding = DocumentationEmbedding(
            source=doc_embedding.source,
            title=doc_embedding.title,
            content=doc_embedding.content,
            embedding=doc_embedding.embedding,
            doc_metadata=doc_embedding.doc_metadata
        )
        self.db.add(db_embedding)
        await self.db.flush()  # Flush to get the ID but don't commit
        return db_embedding

    async def get_by_id(self, embedding_id: int) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(DocumentationEmbedding).where(DocumentationEmbedding.id == embedding_id)
            )
            return result.scalar_one_or_none()
        else:
            return self.db.query(DocumentationEmbedding).filter(DocumentationEmbedding.id == embedding_id).first()

    async def get_all(
        self,
        skip: int = 0, 
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(DocumentationEmbedding).offset(skip).limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(DocumentationEmbedding).offset(skip).limit(limit).all()

    async def update(
        self,
        embedding_id: int, 
        update_data: Dict[str, Any]
    ) -> Optional[DocumentationEmbedding]:
        """Update a documentation embedding by ID with the provided data."""
        db_embedding = await self.get_by_id(embedding_id)
        if db_embedding:
            for key, value in update_data.items():
                setattr(db_embedding, key, value)
            await self.db.flush()
        return db_embedding

    async def delete(self, embedding_id: int) -> bool:
        """Delete a documentation embedding by ID."""
        db_embedding = await self.get_by_id(embedding_id)
        if db_embedding:
            await self.db.delete(db_embedding)
            # Don't commit here, let UnitOfWork handle it
            return True
        return False

    async def search_similar(
        self,
        query_embedding: List[float],
        limit: int = 5
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity.
        Handles both PostgreSQL with pgvector and SQLite.
        """
        if isinstance(self.db, AsyncSession):
            # Detect database type
            db_type = await self._get_database_type()
            
            if db_type == "sqlite":
                return await self._search_similar_sqlite(query_embedding, limit)
            else:
                return await self._search_similar_postgres(query_embedding, limit)
        else:
            # Sync version for backwards compatibility
            return self.db.query(DocumentationEmbedding).order_by(
                DocumentationEmbedding.embedding.cosine_distance(query_embedding)
            ).limit(limit).all()
        
    async def search_by_source(
        self,
        source: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by source."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(DocumentationEmbedding)
                .where(DocumentationEmbedding.source.contains(source))
                .offset(skip)
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(DocumentationEmbedding).filter(
                DocumentationEmbedding.source.contains(source)
            ).offset(skip).limit(limit).all()
        
    async def search_by_title(
        self,
        title: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by title."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(DocumentationEmbedding)
                .where(DocumentationEmbedding.title.contains(title))
                .offset(skip)
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(DocumentationEmbedding).filter(
                DocumentationEmbedding.title.contains(title)
            ).offset(skip).limit(limit).all()
        
    async def get_recent(
        self,
        limit: int = 10
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(DocumentationEmbedding)
                .order_by(desc(DocumentationEmbedding.created_at))
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(DocumentationEmbedding).order_by(
                desc(DocumentationEmbedding.created_at)
            ).limit(limit).all()
    
    async def _get_database_type(self) -> str:
        """Detect the database type from the session."""
        try:
            if hasattr(self.db, 'bind') and self.db.bind:
                dialect_name = self.db.bind.dialect.name.lower()
                return dialect_name
            elif hasattr(self.db, 'get_bind'):
                bind = await self.db.get_bind()
                if bind:
                    dialect_name = bind.dialect.name.lower()
                    return dialect_name
            # Fallback: try to detect from settings
            from src.config.settings import settings
            return settings.DATABASE_TYPE.lower()
        except Exception as e:
            # Default to postgres if detection fails
            return "postgresql"
    
    async def _search_similar_sqlite(
        self,
        query_embedding: List[float],
        limit: int
    ) -> List[DocumentationEmbedding]:
        """SQLite implementation of similarity search using JSON functions."""
        import json
        from sqlalchemy import text
        
        query_json = json.dumps(query_embedding)
        
        # Pure SQL cosine similarity calculation
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
        
        result = await self.db.execute(similarity_query, {
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
        
        return similar_docs
    
    async def _search_similar_postgres(
        self,
        query_embedding: List[float],
        limit: int
    ) -> List[DocumentationEmbedding]:
        """PostgreSQL implementation using pgvector extension."""
        from sqlalchemy import text
        
        # Format the embedding as a vector string for PostgreSQL
        embedding_str = f"[{','.join(str(x) for x in query_embedding)}]"
        base_query = select(DocumentationEmbedding)
        query = base_query.order_by(text("embedding <=> :embedding")).limit(limit)
        result = await self.db.execute(query, {"embedding": embedding_str})
        return result.scalars().all() 