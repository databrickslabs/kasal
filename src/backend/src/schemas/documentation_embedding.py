from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict


class DocumentationEmbeddingBase(BaseModel):
    """Base schema for documentation embeddings."""
    source: str
    title: str
    content: str
    embedding: List[float]
    doc_metadata: Optional[Dict] = None


class DocumentationEmbeddingCreate(DocumentationEmbeddingBase):
    """Schema for creating documentation embeddings."""
    pass


class DocumentationEmbedding(DocumentationEmbeddingBase):
    """Schema for fetching documentation embeddings."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class DocumentationEmbeddingSearch(BaseModel):
    """Schema for searching documentation embeddings."""
    query_embedding: List[float]
    limit: Optional[int] = 5