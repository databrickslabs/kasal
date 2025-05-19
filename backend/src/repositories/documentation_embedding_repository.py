from typing import Dict, List, Optional, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate


class DocumentationEmbeddingRepository:
    """Repository for managing documentation embeddings in the database."""

    @staticmethod
    async def create(
        db: AsyncSession, 
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
        db.add(db_embedding)
        await db.commit()
        await db.refresh(db_embedding)
        return db_embedding

    @staticmethod
    def get_by_id(db: Session, embedding_id: int) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        return db.query(DocumentationEmbedding).filter(DocumentationEmbedding.id == embedding_id).first()

    @staticmethod
    def get_all(
        db: Session, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        return db.query(DocumentationEmbedding).offset(skip).limit(limit).all()

    @staticmethod
    def update(
        db: Session, 
        embedding_id: int, 
        update_data: Dict[str, Any]
    ) -> Optional[DocumentationEmbedding]:
        """Update a documentation embedding by ID with the provided data."""
        db_embedding = DocumentationEmbeddingRepository.get_by_id(db, embedding_id)
        if db_embedding:
            for key, value in update_data.items():
                setattr(db_embedding, key, value)
            db.commit()
            db.refresh(db_embedding)
        return db_embedding

    @staticmethod
    def delete(db: Session, embedding_id: int) -> bool:
        """Delete a documentation embedding by ID."""
        db_embedding = DocumentationEmbeddingRepository.get_by_id(db, embedding_id)
        if db_embedding:
            db.delete(db_embedding)
            db.commit()
            return True
        return False

    @staticmethod
    def search_similar(
        db: Session,
        query_embedding: List[float],
        limit: int = 5
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity.
        Requires PostgreSQL with pgvector extension.
        """
        # Using pgvector's cosine_distance operator for similarity search
        # This requires the pgvector extension to be installed
        return db.query(DocumentationEmbedding).order_by(
            DocumentationEmbedding.embedding.cosine_distance(query_embedding)
        ).limit(limit).all()
        
    @staticmethod
    def search_by_source(
        db: Session,
        source: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by source."""
        return db.query(DocumentationEmbedding).filter(
            DocumentationEmbedding.source.contains(source)
        ).offset(skip).limit(limit).all()
        
    @staticmethod
    def search_by_title(
        db: Session,
        title: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Search for documentation embeddings by title."""
        return db.query(DocumentationEmbedding).filter(
            DocumentationEmbedding.title.contains(title)
        ).offset(skip).limit(limit).all()
        
    @staticmethod
    def get_recent(
        db: Session,
        limit: int = 10
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        return db.query(DocumentationEmbedding).order_by(
            desc(DocumentationEmbedding.created_at)
        ).limit(limit).all() 