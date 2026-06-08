from sqlalchemy import Column, Integer, String, Text, JSON, DateTime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func
from sqlalchemy import text
from sqlalchemy.types import TypeDecorator, UserDefinedType

from src.db.base import Base


# Define a custom type for pgvector with SQLite fallback
class Vector(UserDefinedType):
    def __init__(self, dim=1024):
        self.dim = dim

    def get_col_spec(self, **kw):
        # Use vector type for PostgreSQL, TEXT for SQLite
        if hasattr(self, 'dialect') and 'sqlite' in str(self.dialect).lower():
            return "TEXT"
        return f"vector({self.dim})"
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            
            # For SQLite: store as JSON string
            if 'sqlite' in dialect.name.lower():
                if isinstance(value, list):
                    import json
                    return json.dumps(value)
                return value
            
            # For PostgreSQL: convert to vector format
            if isinstance(value, list):
                return f"[{','.join(str(x) for x in value)}]"
            return value
        return process
    
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
                
            # For SQLite: parse JSON string back to list
            if 'sqlite' in dialect.name.lower():
                if isinstance(value, str):
                    try:
                        import json
                        return json.loads(value)
                    except:
                        return value
            return value
        return process


class DocumentationEmbedding(Base):
    """Model representing documentation embeddings for CrewAI docs."""
    
    __tablename__ = "documentation_embeddings"
    
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True, nullable=False)
    title = Column(String, index=True, nullable=False)
    content = Column(Text, nullable=False)
    embedding = Column(Vector(1024), nullable=False)
    doc_metadata = Column(JSON, nullable=True)
    # Multi-tenant knowledge scoping: uploaded knowledge files live in this same
    # pgvector table (Lakebase). Built-in CrewAI docs leave these NULL.
    group_id = Column(String(100), index=True, nullable=True)  # workspace isolation
    file_path = Column(String, index=True, nullable=True)  # source knowledge file
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"DocumentationEmbedding(id={self.id}, source={self.source}, title={self.title})"


class KnowledgeEmbedding(Base):
    """Embeddings for user-uploaded knowledge files (RAG).

    Same column layout as DocumentationEmbedding but a separate table so uploaded
    knowledge is created and owned by the app principal on Lakebase — the legacy
    documentation_embeddings table is owned by another role and can't be altered.
    group_id (workspace isolation) and file_path (the crew's knowledge source)
    are always populated here.
    """

    __tablename__ = "knowledge_embeddings"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True, nullable=False)
    title = Column(String, index=True, nullable=False)
    content = Column(Text, nullable=False)
    embedding = Column(Vector(1024), nullable=False)
    doc_metadata = Column(JSON, nullable=True)
    group_id = Column(String(100), index=True, nullable=True)  # workspace isolation
    file_path = Column(String, index=True, nullable=True)  # source knowledge file
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"KnowledgeEmbedding(id={self.id}, group_id={self.group_id}, file_path={self.file_path})"