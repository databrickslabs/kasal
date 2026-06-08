from typing import Dict, List, Optional, Any, Union, Type
from sqlalchemy.orm import Session
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.core.base_repository import BaseRepository


class DocumentationEmbeddingRepository(BaseRepository[DocumentationEmbedding]):
    """Repository for managing documentation / knowledge embeddings.

    Model-agnostic: defaults to the built-in ``DocumentationEmbedding`` table,
    but the knowledge-upload path passes ``model=KnowledgeEmbedding`` so uploaded
    knowledge lives in its own ``knowledge_embeddings`` table (created and owned
    by the app principal on Lakebase, avoiding the legacy documentation_embeddings
    table's ownership constraints). Both tables share the same column layout.
    """

    def __init__(
        self,
        db: Union[AsyncSession, Session],
        model: Type[DocumentationEmbedding] = DocumentationEmbedding,
    ):
        """Initialize repository with database session and target model."""
        super().__init__(model, db)
        self.db = db
        self._model = model

    async def create(
        self,
        doc_embedding: DocumentationEmbeddingCreate
    ) -> DocumentationEmbedding:
        """Create a new documentation embedding in the database."""
        db_embedding = self._model(
            source=doc_embedding.source,
            title=doc_embedding.title,
            content=doc_embedding.content,
            embedding=doc_embedding.embedding,
            doc_metadata=doc_embedding.doc_metadata,
            group_id=getattr(doc_embedding, "group_id", None),
            file_path=getattr(doc_embedding, "file_path", None),
        )
        self.db.add(db_embedding)
        await self.db.flush()  # Flush to get the ID but don't commit
        return db_embedding

    async def bulk_create(
        self,
        items: List[DocumentationEmbeddingCreate],
    ) -> List[DocumentationEmbedding]:
        """Insert many embeddings in one transaction on the current session.

        Used by knowledge-file ingest: all chunk rows are written through the
        request's own session (one flush), instead of the per-row SQLite queue
        which uses a separate session and deadlocks against an open upload
        transaction on SQLite's single-writer lock.
        """
        objs = [
            self._model(
                source=i.source,
                title=i.title,
                content=i.content,
                embedding=i.embedding,
                doc_metadata=i.doc_metadata,
                group_id=getattr(i, "group_id", None),
                file_path=getattr(i, "file_path", None),
            )
            for i in items
        ]
        self.db.add_all(objs)
        if isinstance(self.db, AsyncSession):
            await self.db.flush()
        else:
            self.db.flush()
        return objs

    async def get_by_id(self, embedding_id: int) -> Optional[DocumentationEmbedding]:
        """Get a specific documentation embedding by ID."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(self._model).where(self._model.id == embedding_id)
            )
            return result.scalar_one_or_none()
        else:
            return self.db.query(self._model).filter(self._model.id == embedding_id).first()

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 100
    ) -> List[DocumentationEmbedding]:
        """Get a list of documentation embeddings with pagination."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(self._model).offset(skip).limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(self._model).offset(skip).limit(limit).all()

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

    async def delete_by_file(
        self,
        group_id: str,
        execution_id: str,
        filename: str,
    ) -> int:
        """Delete all chunk rows for one uploaded knowledge file.

        Matches within a workspace (group_id) on the stored full file_path, which
        always contains ``/<execution_id>/`` and ends with the filename. Returns
        the number of rows deleted. autoescape neutralizes LIKE wildcards in the
        user-supplied filename/execution_id.
        """
        from sqlalchemy import delete as sa_delete

        conditions = [
            self._model.group_id == group_id,
            self._model.file_path.contains(f"/{execution_id}/", autoescape=True),
            self._model.file_path.contains(filename, autoescape=True),
        ]
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(sa_delete(self._model).where(*conditions))
            return result.rowcount or 0
        else:
            deleted = self.db.query(self._model).filter(*conditions).delete(
                synchronize_session=False
            )
            return deleted or 0

    async def search_similar(
        self,
        query_embedding: List[float],
        limit: int = 5,
        group_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
    ) -> List[DocumentationEmbedding]:
        """
        Search for similar embeddings using cosine similarity.
        Handles both PostgreSQL with pgvector and SQLite.

        Scoping:
        - group_id is None  -> only rows with group_id IS NULL (built-in docs).
        - group_id is set    -> only that workspace's rows, optionally narrowed
          to file_paths (the crew's knowledge sources).
        """
        if isinstance(self.db, AsyncSession):
            # Detect database type
            db_type = await self._get_database_type()

            if db_type == "sqlite":
                return await self._search_similar_sqlite(query_embedding, limit, group_id, file_paths)
            else:
                return await self._search_similar_postgres(query_embedding, limit, group_id, file_paths)
        else:
            # Sync version for backwards compatibility
            query = self.db.query(self._model)
            if group_id is None:
                query = query.filter(self._model.group_id.is_(None))
            else:
                query = query.filter(self._model.group_id == group_id)
                if file_paths:
                    query = query.filter(self._model.file_path.in_(file_paths))
            return query.order_by(
                self._model.embedding.cosine_distance(query_embedding)
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
                select(self._model)
                .where(self._model.source.contains(source))
                .offset(skip)
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(self._model).filter(
                self._model.source.contains(source)
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
                select(self._model)
                .where(self._model.title.contains(title))
                .offset(skip)
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(self._model).filter(
                self._model.title.contains(title)
            ).offset(skip).limit(limit).all()

    async def get_recent(
        self,
        limit: int = 10
    ) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(self._model)
                .order_by(desc(self._model.created_at))
                .limit(limit)
            )
            return result.scalars().all()
        else:
            return self.db.query(self._model).order_by(
                desc(self._model.created_at)
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
        limit: int,
        group_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
    ) -> List[DocumentationEmbedding]:
        """SQLite implementation of similarity search using JSON functions."""
        import json
        from sqlalchemy import text

        query_json = json.dumps(query_embedding)
        table = self._model.__tablename__

        # Scope the candidate rows the same way as the pgvector path. group_id
        # is bound as a parameter; file_paths is applied as a Python post-filter
        # below (SQLite is dev-only, so exact-limit semantics matter less here).
        params: Dict[str, Any] = {"query_vector": query_json, "limit_val": limit}
        if group_id is None:
            scope_clause = "AND group_id IS NULL"
        else:
            scope_clause = "AND group_id = :group_id_val"
            params["group_id_val"] = group_id

        # Pure SQL cosine similarity calculation
        similarity_query = text(f"""
            WITH vector_calculations AS (
                SELECT
                    id,
                    source,
                    title,
                    content,
                    doc_metadata,
                    group_id,
                    file_path,
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
                FROM {table}
                WHERE embedding IS NOT NULL
                {scope_clause}
            )
            SELECT
                id, source, title, content, doc_metadata, group_id, file_path,
                created_at, updated_at,
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

        result = await self.db.execute(similarity_query, params)
        rows = result.all()

        # Convert rows to model objects
        similar_docs = []
        for row in rows:
            if file_paths and getattr(row, "file_path", None) not in file_paths:
                continue
            doc = self._model(
                id=row.id,
                source=row.source,
                title=row.title,
                content=row.content,
                doc_metadata=row.doc_metadata,
                group_id=row.group_id,
                file_path=row.file_path,
                created_at=row.created_at,
                updated_at=row.updated_at,
                embedding=[]  # Don't need to return the embedding
            )
            similar_docs.append(doc)

        return similar_docs

    async def _search_similar_postgres(
        self,
        query_embedding: List[float],
        limit: int,
        group_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
    ) -> List[DocumentationEmbedding]:
        """PostgreSQL implementation using pgvector extension."""
        from sqlalchemy import text

        # Format the embedding as a vector string for PostgreSQL
        embedding_str = f"[{','.join(str(x) for x in query_embedding)}]"
        query = select(self._model)
        if group_id is None:
            # Built-in docs only (uploaded knowledge always carries a group_id).
            query = query.where(self._model.group_id.is_(None))
        else:
            query = query.where(self._model.group_id == group_id)
            if file_paths:
                query = query.where(self._model.file_path.in_(file_paths))
        query = query.order_by(text("embedding <=> :embedding")).limit(limit)
        result = await self.db.execute(query, {"embedding": embedding_str})
        return result.scalars().all()
