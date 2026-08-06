from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate

#: Where a similarity search stashes each row's score.
#:
#: The search returns ORM instances and both callers already consume that shape,
#: so the score rides along on the instance rather than changing every signature
#: to a (row, score) tuple. Transient by nature — never persisted.
SIMILARITY_ATTR = "_kasal_similarity"


def _attach_similarity(row: Any, similarity: Optional[float]) -> None:
    """Record how close this row was to the query, when it is known."""
    if similarity is None:
        return
    try:
        setattr(row, SIMILARITY_ATTR, float(similarity))
    except Exception:  # noqa: BLE001 — a score is never worth failing a search
        pass


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

    def _owner_kwargs(self, item: DocumentationEmbeddingCreate) -> dict:
        """created_by only exists on models that carry the column (e.g.
        KnowledgeEmbedding) — never pass it to the legacy docs model."""
        if hasattr(self._model, "created_by"):
            return {"created_by": getattr(item, "created_by", None)}
        return {}

    async def create(
        self, doc_embedding: DocumentationEmbeddingCreate
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
            **self._owner_kwargs(doc_embedding),
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
                **self._owner_kwargs(i),
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
            return (
                self.db.query(self._model)
                .filter(self._model.id == embedding_id)
                .first()
            )

    async def get_all(
        self, skip: int = 0, limit: int = 100
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
        self, embedding_id: int, update_data: Dict[str, Any]
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
        created_by: Optional[str] = None,
    ) -> int:
        """Delete all chunk rows for one uploaded knowledge file.

        Matches within a workspace (group_id) on the stored full file_path, which
        always contains ``/<execution_id>/`` and ends with the filename. When
        ``created_by`` is given, only rows uploaded by THAT user (or legacy rows
        with no uploader) are deleted — a user must not delete another user's
        knowledge. Returns the number of rows deleted. autoescape neutralizes
        LIKE wildcards in the user-supplied filename/execution_id.
        """
        from sqlalchemy import delete as sa_delete
        from sqlalchemy import or_

        conditions = [
            self._model.group_id == group_id,
            self._model.file_path.contains(f"/{execution_id}/", autoescape=True),
            self._model.file_path.contains(filename, autoescape=True),
        ]
        if created_by and hasattr(self._model, "created_by"):
            conditions.append(
                or_(
                    self._model.created_by.is_(None),
                    self._model.created_by == created_by,
                )
            )
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(sa_delete(self._model).where(*conditions))
            return result.rowcount or 0
        else:
            deleted = (
                self.db.query(self._model)
                .filter(*conditions)
                .delete(synchronize_session=False)
            )
            return deleted or 0

    async def find_content_hash(
        self,
        group_id: str,
        file_path: str,
        created_by: Optional[str] = None,
    ) -> Optional[str]:
        """The content hash stored for a file, or None.

        Every chunk of a file carries the same hash in ``doc_metadata``, so one
        row answers "is this exact content already embedded?". Scoped to the
        group and, when given, the uploader — reading another user's hash would
        leak the fact that they uploaded a particular document.
        """
        from sqlalchemy import or_, select

        conditions = [
            self._model.group_id == group_id,
            self._model.file_path == file_path,
        ]
        if created_by and hasattr(self._model, "created_by"):
            conditions.append(
                or_(
                    self._model.created_by.is_(None),
                    self._model.created_by == created_by,
                )
            )
        result = await self.db.execute(
            select(self._model.doc_metadata).where(*conditions).limit(1)
        )
        metadata = result.scalars().first()
        if isinstance(metadata, dict):
            value = metadata.get("content_hash")
            return str(value) if value else None
        return None

    async def delete_by_path(
        self,
        group_id: str,
        file_path: str,
        created_by: Optional[str] = None,
    ) -> int:
        """Delete every chunk stored under an exact ``file_path``.

        Distinct from ``delete_by_file``, which matches on execution id and
        filename with LIKE. This is the exact-identity delete used when a file
        is replaced or detached, so it cannot catch a same-named file uploaded
        into a different session.
        """
        from sqlalchemy import delete as sa_delete
        from sqlalchemy import or_

        conditions = [
            self._model.group_id == group_id,
            self._model.file_path == file_path,
        ]
        if created_by and hasattr(self._model, "created_by"):
            conditions.append(
                or_(
                    self._model.created_by.is_(None),
                    self._model.created_by == created_by,
                )
            )
        result = await self.db.execute(sa_delete(self._model).where(*conditions))
        return int(getattr(result, "rowcount", 0) or 0)

    async def delete_expired(self, group_id: str, cutoff: datetime) -> int:
        """Delete rows for ``group_id`` older than ``cutoff``; returns rows deleted.

        Backs the knowledge TTL purge. Scoped to a single group even though the
        table is shared across tenants — one group's purge must never delete
        another group's rows (defense in depth alongside search's own TTL/
        group filtering). Does not commit; the caller controls the transaction
        because the session may be an app session or a Lakebase session with
        different commit semantics.
        """
        from sqlalchemy import delete as sa_delete

        result = await self.db.execute(
            sa_delete(self._model).where(
                self._model.group_id == group_id,
                self._model.created_at < cutoff,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0)

    async def delete_expired_all(self, cutoff: datetime) -> int:
        """Delete rows older than ``cutoff`` in EVERY group. Returns rows deleted.

        Deliberately unscoped, unlike ``delete_expired`` next door, and that is
        the difference between the two: this backs the system-wide TTL sweep,
        which has no user and no group — it runs on a timer to make the
        retention window true of the database and not only of search results.

        Retention is the one thing that must not be per-tenant here: a workspace
        nobody has uploaded to in a month is exactly the one whose expired rows
        would otherwise sit forever.
        """
        from sqlalchemy import delete as sa_delete

        result = await self.db.execute(
            sa_delete(self._model).where(self._model.created_at < cutoff)
        )
        return int(getattr(result, "rowcount", 0) or 0)

    async def bulk_insert_raw(self, rows: List[Dict[str, Any]]) -> None:
        """Bulk-insert plain-dict embedding rows via a single INSERT statement.

        Used by the background embedding queue, which batches raw dicts (not
        ``DocumentationEmbeddingCreate``) to avoid building one ORM instance per
        row before a potentially large batch insert. Does not commit — the
        caller owns the transaction (the queue opens its own short-lived
        session per flush).
        """
        from sqlalchemy import insert as sa_insert

        if not rows:
            return
        await self.db.execute(sa_insert(self._model).values(rows))

    async def insert_raw(self, row: Dict[str, Any]) -> DocumentationEmbedding:
        """Insert one plain-dict embedding row and return the new instance.

        Used by the embedding queue's per-item retry path after a batch insert
        fails. Builds the model straight from the dict (rather than going
        through ``create()``/``DocumentationEmbeddingCreate``) so the row's own
        ``created_at`` (set when it was queued) is preserved instead of falling
        back to the column's server default. Does not commit.
        """
        db_embedding = self._model(**row)
        self.db.add(db_embedding)
        return db_embedding

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
                return await self._search_similar_sqlite(
                    query_embedding, limit, group_id, file_paths
                )
            else:
                return await self._search_similar_postgres(
                    query_embedding, limit, group_id, file_paths
                )
        else:
            # Sync version for backwards compatibility
            query = self.db.query(self._model)
            if group_id is None:
                query = query.filter(self._model.group_id.is_(None))
            else:
                query = query.filter(self._model.group_id == group_id)
                if file_paths:
                    query = query.filter(self._model.file_path.in_(file_paths))
            return (
                query.order_by(self._model.embedding.cosine_distance(query_embedding))
                .limit(limit)
                .all()
            )

    async def search_by_source(
        self, source: str, skip: int = 0, limit: int = 100
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
            return (
                self.db.query(self._model)
                .filter(self._model.source.contains(source))
                .offset(skip)
                .limit(limit)
                .all()
            )

    async def search_by_title(
        self, title: str, skip: int = 0, limit: int = 100
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
            return (
                self.db.query(self._model)
                .filter(self._model.title.contains(title))
                .offset(skip)
                .limit(limit)
                .all()
            )

    async def get_recent(self, limit: int = 10) -> List[DocumentationEmbedding]:
        """Get most recently created documentation embeddings."""
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(self._model).order_by(desc(self._model.created_at)).limit(limit)
            )
            return result.scalars().all()
        else:
            return (
                self.db.query(self._model)
                .order_by(desc(self._model.created_at))
                .limit(limit)
                .all()
            )

    async def _get_database_type(self) -> str:
        """Detect the database type from the session."""
        try:
            if hasattr(self.db, "bind") and self.db.bind:
                dialect_name = self.db.bind.dialect.name.lower()
                return dialect_name
            elif hasattr(self.db, "get_bind"):
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
        """SQLite implementation: fetch the scoped rows and rank by cosine
        similarity in Python.

        The previous pure-SQL version computed the dot product with a
        json_each(embedding) × json_each(:query) join PER ROW (~1M joined rows
        per chunk at 1024 dims) — ~30 seconds for a few hundred chunks, which
        blew the knowledge tool's 30s timeout ("Error searching knowledge
        base"). A few hundred 1024-dim vectors rank in milliseconds in Python.
        Returning live model instances also keeps every column (created_by
        included) for the per-user filters upstream.
        """
        import json as _json
        import math

        query = select(self._model)
        if group_id is None:
            # Built-in docs only (uploaded knowledge always carries a group_id).
            query = query.where(self._model.group_id.is_(None))
        else:
            query = query.where(self._model.group_id == group_id)
            if file_paths:
                query = query.where(self._model.file_path.in_(file_paths))
        result = await self.db.execute(query)
        rows = result.scalars().all()

        q = [float(x) for x in query_embedding]
        q_norm = math.sqrt(sum(x * x for x in q)) or 1.0

        scored = []
        for row in rows:
            emb = row.embedding
            if isinstance(emb, (str, bytes)):
                try:
                    emb = _json.loads(emb)
                except (TypeError, ValueError):
                    continue
            if not emb:
                continue
            dot = 0.0
            norm_sq = 0.0
            for d, qv in zip(emb, q):
                d = float(d)
                dot += d * qv
                norm_sq += d * d
            if norm_sq <= 0:
                continue
            similarity = dot / (math.sqrt(norm_sq) * q_norm)
            if similarity > 0:
                scored.append((similarity, row))

        # Sort on the similarity only (model instances are not comparable).
        scored.sort(key=lambda pair: pair[0], reverse=True)
        top = scored[:limit]
        for similarity, row in top:
            _attach_similarity(row, similarity)
        return [row for _, row in top]

    async def list_group_file_paths(self, group_id: str) -> List[str]:
        """Return the distinct stored ``file_path`` values for a group.

        Used to resolve a requested file name (often a bare basename) to the
        actual stored full path(s) before a scoped similarity search, so the
        ranking can be narrowed to ONLY the requested file's chunks.
        """
        if isinstance(self.db, AsyncSession):
            result = await self.db.execute(
                select(self._model.file_path)
                .where(self._model.group_id == group_id)
                .distinct()
            )
            return [r for (r,) in result.all() if r]
        # Sync fallback
        return [
            r
            for (r,) in self.db.query(self._model.file_path)
            .filter(self._model.group_id == group_id)
            .distinct()
            .all()
            if r
        ]

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
        query = select(self._model)
        if group_id is None:
            # Built-in docs only (uploaded knowledge always carries a group_id).
            query = query.where(self._model.group_id.is_(None))
        else:
            query = query.where(self._model.group_id == group_id)
            if file_paths:
                query = query.where(self._model.file_path.in_(file_paths))
        # Built by the Vector type's comparator, NOT hand-written SQL. The
        # previous version was `literal_column("embedding <=> (:embedding)::vector")`
        # with the value passed via execute(..., {"embedding": ...}). That mixes
        # paramstyles: SQLAlchemy renders the rest of the statement as asyncpg's
        # `$1`/`$2` positional params and leaves the literal_column's `:embedding`
        # untouched, so PostgreSQL received a stray colon and rejected the whole
        # query — "syntax error at or near \":\"". Every knowledge search then
        # failed, and the tool reported "nothing in the knowledge base came close"
        # rather than an error, so it read as an empty index instead of a broken
        # query.
        #
        # cosine_distance() handles both things the hand-written SQL was trying to
        # do: it formats the list through the type's bind_processor and casts to
        # ``vector`` (the operator is ``vector <=> vector``; an uncast bind arrives
        # as text or unknown and does not resolve).
        #
        # The distance is SELECTed, not merely ordered by: ordering alone tells the
        # caller which chunk ranked first but not whether ANY of them are close, so
        # twenty unrelated chunks were handed to an agent as "relevant results" and
        # it re-queried forever looking for an answer they did not contain.
        distance = self._model.embedding.cosine_distance(query_embedding).label(
            "distance"
        )
        query = query.add_columns(distance).order_by(distance).limit(limit)
        result = await self.db.execute(query)
        rows = []
        for row, dist in result.all():
            # pgvector's <=> is cosine DISTANCE; similarity is its complement.
            _attach_similarity(row, 1.0 - float(dist) if dist is not None else None)
            rows.append(row)
        return rows
