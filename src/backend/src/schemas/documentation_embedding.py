from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel


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

    class Config:
        from_attributes = True 