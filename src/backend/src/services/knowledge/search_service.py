"""
Knowledge Search Service

Handles searching knowledge files in vector storage.
Separated from DatabricksKnowledgeService for clean architecture.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.documentation_embedding_repository import SIMILARITY_ATTR

logger = logging.getLogger(__name__)


class KnowledgeSearchService:
    """Service for searching knowledge files in vector storage."""

    def __init__(self, session: AsyncSession, group_id: str):
        """
        Initialize the Knowledge Search Service.

        Args:
            session: Database session
            group_id: Group ID for tenant isolation
        """
        self.session = session
        self.group_id = group_id
        self._memory_backend_service = None

    async def search(
        self,
        query: str,
        execution_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
        agent_id: Optional[str] = None,
        limit: int = 5,
        user_token: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for knowledge in the Databricks Vector Index.

        Args:
            query: The search query
            execution_id: Optional execution ID for scoping
            file_paths: Optional list of file paths to filter search
            agent_id: Optional agent ID for access control filtering
            limit: Maximum number of results to return
            user_token: Optional user token for OBO authentication
            created_by: Requesting user's email — when set, results are
                isolated to chunks THIS user uploaded (rows without an
                uploader, e.g. legacy ones, stay group-shared)

        Returns:
            List of search results with content and metadata
        """
        logger.info(
            f"Knowledge search: query='{query}', group={self.group_id}, agent={agent_id}, user={created_by}, limit={limit}"
        )
        logger.info(f"File paths parameter: {file_paths}")

        try:
            from src.services.knowledge.documentation_embedding import (
                DocumentationEmbeddingService,
            )

            # Generate the query embedding with the SAME embedder used at ingest
            # time (resolved through the shared resolver: Databricks in prod, local
            # Ollama in dev — both 1024 dims) so it matches the stored vectors in
            # the documentation_embeddings pgvector table.
            from src.services.knowledge.embedder import (
                resolve_knowledge_embedder_config,
            )
            from src.services.llm.manager import LLMManager

            embedder_config = await resolve_knowledge_embedder_config(
                user_token=user_token, group_id=self.group_id
            )
            try:
                query_embedding = await LLMManager.get_embedding(
                    query, embedder_config=embedder_config
                )
                if not query_embedding:
                    logger.error("Failed to generate query embedding")
                    return []
            except Exception as embed_error:
                logger.error(f"Error generating embedding: {embed_error}")
                return []

            # Uploaded knowledge lives in pgvector, scoped by group_id (tenant
            # isolation) and optionally file_paths. When the active memory backend
            # is Lakebase, that's the Lakebase memory instance; otherwise the app
            # DB. Read from the same place ingest writes.
            from src.models.documentation_embedding import KnowledgeEmbedding
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.services.knowledge.embedding_session import (
                knowledge_embedding_session,
            )

            formatted_results = []
            try:
                async with knowledge_embedding_session(
                    self.session, self.group_id, user_token
                ) as (search_session, _is_lakebase):
                    repo = DocumentationEmbeddingRepository(
                        search_session, model=KnowledgeEmbedding
                    )
                    # Compare basenames under NFC normalization: an uploaded path
                    # from a macOS filesystem is NFD (decomposed — "ä" = a + ¨),
                    # while the same name arriving via JSON/HTTP may be NFC, so a
                    # raw string compare silently fails for accented filenames
                    # (e.g. "Kindeswohlgefährdung").
                    import unicodedata

                    def _basename_nfc(path: Optional[str]) -> str:
                        return unicodedata.normalize(
                            "NFC", (path or "").rsplit("/", 1)[-1]
                        )

                    wanted = (
                        {_basename_nfc(fp) for fp in file_paths if fp}
                        if file_paths
                        else set()
                    )

                    if wanted:
                        # A specific file was requested: rank ONLY within THAT
                        # file's chunks. We resolve the requested name(s) to the
                        # actual stored full path(s) in this group (matched by
                        # basename — robust to bare-name vs full-path differences),
                        # then run the similarity search scoped to those paths.
                        #
                        # Why not rank group-wide and filter after: top-k is applied
                        # by the DB BEFORE any post-filter, so a DIFFERENT, more
                        # query-similar document (e.g. an English "presentation" deck
                        # vs a German PDF) can fill the entire top-k and crowd the
                        # requested file out — leaving nothing after the filter, so
                        # the agent answers from NO source and hallucinates. Scoping
                        # the ranking guarantees the requested file's best chunks are
                        # what we return.
                        #
                        # If the requested file isn't in the store (e.g. its
                        # embedding failed), return EMPTY — never substitute the
                        # group's other files (serving a different document is worse
                        # than returning nothing).
                        group_paths = await repo.list_group_file_paths(self.group_id)
                        scoped_paths = [
                            p for p in group_paths if _basename_nfc(p) in wanted
                        ]
                        if not scoped_paths:
                            logger.warning(
                                f"[KNOWLEDGE-SEARCH] Requested file(s) {sorted(wanted)} "
                                f"not in store for group={self.group_id}; returning no "
                                f"results (not substituting other files)."
                            )
                            rows = []
                        else:
                            rows = (
                                await repo.search_similar(
                                    query_embedding,
                                    limit=limit,
                                    group_id=self.group_id,
                                    file_paths=scoped_paths,
                                )
                                or []
                            )
                    else:
                        # No specific file requested: rank across all of the group's
                        # uploaded chunks.
                        rows = (
                            await repo.search_similar(
                                query_embedding,
                                limit=limit,
                                group_id=self.group_id,
                                file_paths=None,
                            )
                            or []
                        )
                    total_group_rows = len(rows)

                    # Per-user isolation: only chunks uploaded by the requesting
                    # user are returned (rows without an uploader — legacy or
                    # built-in — stay group-shared).
                    if created_by:
                        rows = [
                            r
                            for r in rows
                            if (owner := getattr(r, "created_by", None)) is None
                            or owner == created_by
                        ]

                    # TTL: expired chunks are excluded immediately, even before
                    # the next upload-time purge sweeps them out of the table.
                    from src.services.knowledge.embedding_service import (
                        KNOWLEDGE_TTL_DAYS,
                    )

                    if KNOWLEDGE_TTL_DAYS > 0:
                        from datetime import datetime, timedelta, timezone

                        cutoff = datetime.now(timezone.utc) - timedelta(
                            days=KNOWLEDGE_TTL_DAYS
                        )

                        def _fresh(r) -> bool:
                            created = getattr(r, "created_at", None)
                            if not created:
                                return True
                            if created.tzinfo is None:
                                created = created.replace(tzinfo=timezone.utc)
                            return created >= cutoff

                        rows = [r for r in rows if _fresh(r)]

                    logger.info(
                        f"[KNOWLEDGE-SEARCH] table=knowledge_embeddings lakebase={_is_lakebase} "
                        f"group={self.group_id} requested={file_paths} "
                        f"group_rows={total_group_rows} -> {len(rows)} rows"
                    )
                    # Also emit as a span: the subprocess's logger.info above
                    # does NOT reach the OTel logs table, but spans DO reach
                    # otel_spans — so this is what makes a deployed run's search
                    # routing + row counts observable (lakebase store vs empty
                    # app-DB fallback, group_rows before per-user/TTL filters).
                    from src.services.knowledge.embedding_session import (
                        emit_knowledge_span,
                    )

                    emit_knowledge_span(
                        "knowledge_search",
                        {
                            "group_id": self.group_id,
                            "lakebase": bool(_is_lakebase),
                            "group_rows": total_group_rows,
                            "returned_rows": len(rows),
                            "created_by": created_by,
                        },
                    )
                    # Map rows to dicts INSIDE the session context: a Lakebase
                    # session commits/expires on exit, so attributes must be read
                    # before the context closes (else DetachedInstanceError).
                    for row in rows or []:
                        try:
                            metadata = getattr(row, "doc_metadata", None) or {}
                            source = (
                                getattr(row, "file_path", None)
                                or getattr(row, "source", "")
                                or ""
                            )
                            formatted_results.append(
                                {
                                    "content": getattr(row, "content", "") or "",
                                    "metadata": {
                                        "source": source,
                                        "title": getattr(row, "title", "") or "",
                                        "chunk_index": metadata.get("chunk_index", 0),
                                        # The similarity the search itself computed
                                        # (SIMILARITY_ATTR), falling back to any score
                                        # already on the chunk's metadata. This used to
                                        # be hardcoded to whatever metadata carried —
                                        # nothing — so every result reported 0.000 and
                                        # an agent could not tell a match from noise.
                                        "score": float(
                                            getattr(row, SIMILARITY_ATTR, None)
                                            or metadata.get("score", 0.0)
                                            or 0.0
                                        ),
                                        "group_id": getattr(row, "group_id", None)
                                        or self.group_id,
                                        "execution_id": execution_id,
                                    },
                                }
                            )
                        except Exception as fmt_err:
                            logger.error(f"Error formatting result: {fmt_err}")
                            continue
            except Exception as search_error:
                logger.error(f"Search failed: {search_error}", exc_info=True)
                # Surface the real error as a span: this runs in the crew
                # subprocess whose logger.error does NOT reach the OTel logs
                # table, so a swallowed pgvector/permission error (e.g. the
                # "vector <=> text" cast bug) is otherwise invisible. Spans DO
                # reach otel_spans.
                from src.services.knowledge.embedding_session import emit_knowledge_span

                emit_knowledge_span(
                    "knowledge_search_error",
                    {
                        "group_id": self.group_id,
                        "error": f"{type(search_error).__name__}: {search_error}"[:400],
                    },
                )
                return []

            logger.info(f"Found {len(formatted_results)} results")
            return formatted_results

        except Exception as e:
            logger.error(f"Error searching knowledge: {e}", exc_info=True)
            return []
