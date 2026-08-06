from sqlalchemy import JSON, Column, DateTime, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator, UserDefinedType

from src.db.base import Base


# Define a custom type for pgvector with SQLite fallback
class Vector(UserDefinedType):
    #: The type's only state is ``dim``, which is immutable per column, so it is
    #: safe in a statement cache key. Without this SQLAlchemy refuses to cache any
    #: statement touching an embedding column and warns on every one — a real cost
    #: on the similarity queries, which run per prompt.
    cache_ok = True

    def __init__(self, dim=1024):
        self.dim = dim

    def get_col_spec(self, **kw):
        # Use vector type for PostgreSQL, TEXT for SQLite
        if hasattr(self, "dialect") and "sqlite" in str(self.dialect).lower():
            return "TEXT"
        return f"vector({self.dim})"

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None

            # For SQLite: store as JSON string
            if "sqlite" in dialect.name.lower():
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
            if "sqlite" in dialect.name.lower():
                if isinstance(value, str):
                    try:
                        import json

                        return json.loads(value)
                    except:
                        return value
            return value

        return process

    class comparator_factory(UserDefinedType.Comparator):
        """Expose pgvector's distance operators on this hand-rolled type.

        This is NOT the pgvector library's ``Vector`` — it is a local
        ``UserDefinedType`` so the same model works on SQLite (where the column is
        TEXT holding JSON). Being hand-rolled, it shipped without the comparator
        methods callers assume, so ``WorkflowRecipe.embedding.cosine_distance(...)``
        raised::

            Neither 'InstrumentedAttribute' object nor 'Comparator' object
            associated with WorkflowRecipe.embedding has an attribute
            'cosine_distance'

        The recipe repository catches that and skips, so exemplar lookup silently
        degraded to no exemplars on every Postgres/Lakebase run — a feature quietly
        off rather than a visible failure.

        ``<=>`` is cosine distance, ``<->`` L2, ``<#>`` negative inner product.
        PostgreSQL only; on SQLite the repository takes its own Python-side path
        (``_find_similar_sqlite``) and never reaches these.
        """

        def _distance(self, other, operator: str):
            from sqlalchemy import Float, cast, literal

            # Two things are load-bearing here:
            #
            # 1. Bind through THIS type, so bind_processor formats the list as
            #    '[a,b,c]' instead of sending a raw ARRAY.
            # 2. CAST it explicitly. Without the cast the driver sends the string
            #    as `unknown` and PostgreSQL cannot resolve the operator:
            #      operator does not exist: public.vector <=> unknown
            #    A compile-only test does not catch that — the SQL looks right and
            #    only the live server rejects it.
            vector_param = cast(literal(other, self.expr.type), self.expr.type)
            return self.op(operator, return_type=Float)(vector_param)

        def cosine_distance(self, other):
            return self._distance(other, "<=>")

        def l2_distance(self, other):
            return self._distance(other, "<->")

        def max_inner_product(self, other):
            return self._distance(other, "<#>")


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
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

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
    # Uploader email — per-user isolation of uploaded knowledge within a group
    # (NULL on legacy rows, treated as group-shared).
    created_by = Column(String(255), index=True, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self):
        return f"KnowledgeEmbedding(id={self.id}, group_id={self.group_id}, file_path={self.file_path})"
