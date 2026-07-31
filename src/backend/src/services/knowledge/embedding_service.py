"""
Knowledge Embedding Service

Handles embedding of knowledge files into vector storage.
Separated from DatabricksKnowledgeService for clean architecture.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# TTL for uploaded knowledge embeddings: rows older than this are purged and
# excluded from search, so the store never accumulates stale uploads. 0 or a
# negative value disables the TTL.
#
# Seven days, not thirty. These are attachments — someone drops a PDF to ask a
# question about it — not a curated corpus. A month is longer than the intent,
# and the shorter the window the smaller the answer to "what user data do you
# still hold". Raise it with the env var where a workspace really does treat
# uploads as a library.
#
# Enforced in three places, deliberately: search excludes expired rows so expiry
# takes effect the moment it passes; ``purge_expired`` runs before each upload;
# and a daily sweep (``main.py``) removes rows from disk even in a workspace
# where nobody uploads again — without it, "we keep files for 7 days" would be
# true of search results and false of the database.
KNOWLEDGE_TTL_DAYS = int(os.getenv("KNOWLEDGE_TTL_DAYS", "7"))


class KnowledgeEmbeddingService:
    """Service for embedding knowledge files into vector storage."""

    def __init__(self, session: AsyncSession, group_id: str):
        """
        Initialize the Knowledge Embedding Service.

        Args:
            session: Database session (for future use if needed)
            group_id: Group ID for tenant isolation
        """
        self.session = session
        self.group_id = group_id
        self._memory_backend_service = None

    async def embed_file(
        self,
        file_path: str,
        file_content: str,
        execution_id: str,
        agent_ids: Optional[List[str]] = None,
        user_token: Optional[str] = None,
        created_by: Optional[str] = None,
        content_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Embed a file's content into vector storage.

        Args:
            file_path: Full path to the file
            file_content: Content of the file to embed
            execution_id: Execution ID for scoping
            agent_ids: Optional list of agent IDs for access control
            user_token: Optional user token for OBO authentication
            created_by: Uploader email — stamped on every chunk so search can
                isolate knowledge per user within the group

        Returns:
            Embedding result with status and metadata
        """
        try:
            logger.info(f"[EMBEDDING] Starting embedding for file: {file_path}")

            if agent_ids:
                logger.info(f"[EMBEDDING] Agent IDs for access control: {agent_ids}")
            else:
                logger.warning("[EMBEDDING] No agent_ids provided")

            # Chunk the content with context enrichment
            chunks = await self._chunk_with_context(file_content, file_path)
            if not chunks:
                logger.warning(f"[EMBEDDING] No chunks generated for file: {file_path}")
                return {"status": "skipped", "reason": "No chunks generated"}

            logger.info(f"[EMBEDDING] Generated {len(chunks)} context-enriched chunks")

            # Knowledge embeddings are stored in the application's pgvector
            # documentation_embeddings table (Lakebase pgvector in production,
            # SQLite locally), scoped by group_id (tenant isolation) and file_path
            # (so the search tool can narrow to a crew's knowledge sources). The
            # raw file still lives in the Databricks Volume; only the vector is
            # stored here. This replaces the Databricks Vector Search index.
            from src.models.documentation_embedding import KnowledgeEmbedding
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
            from src.services.llm.manager import LLMManager

            filename = file_path.split("/")[-1]

            # Embed ALL chunks in batches with a single auth resolution. Doing this
            # per-chunk previously re-opened a DB session + re-resolved the PAT for
            # every chunk (243 lookups for a 243-chunk file) and issued one HTTP
            # round-trip each — the dominant cost. The embedder is resolved through
            # the shared resolver (Databricks in prod, local Ollama in dev) — the
            # SAME resolver the search side uses, so query/stored vectors always
            # match (both 1024-dim).
            from src.services.knowledge.embedder import (
                resolve_knowledge_embedder_config,
            )

            embedder_config = await resolve_knowledge_embedder_config(
                user_token=user_token, group_id=self.group_id
            )
            chunk_texts = [c["content"] for c in chunks]
            embeddings = await LLMManager.get_embeddings(
                chunk_texts, embedder_config=embedder_config
            )

            creates: List[DocumentationEmbeddingCreate] = []
            for i, chunk_data in enumerate(chunks):
                chunk_content = chunk_data["content"]
                raw_content = chunk_data.get("raw_content", chunk_content)
                section = chunk_data.get("section", f"Section {i+1}")
                document_summary = chunk_data.get("document_summary", "")

                embedding = embeddings[i] if i < len(embeddings) else None
                if not embedding:
                    logger.warning(
                        f"[EMBEDDING] No embedding produced for chunk {i}; skipping"
                    )
                    continue

                # Prepare metadata (kept in the doc_metadata JSON column)
                metadata = {
                    "source": file_path,
                    "filename": filename,
                    "execution_id": execution_id,
                    "group_id": self.group_id,
                    "agent_ids": agent_ids or [],
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "section": section,
                    "parent_document_id": f"{self.group_id}:{execution_id}:{filename}",
                    "document_summary": document_summary,
                    "raw_content": raw_content,
                    "file_path": file_path,
                    "created_by": created_by,
                    "created_at": datetime.utcnow().isoformat(),
                    "type": "knowledge_source",
                    "content_type": self._detect_content_type(filename),
                    # Identity of the CONTENT, so a re-upload of the same file
                    # can be recognised and skipped. Stored in metadata rather
                    # than a column: it is read once per upload, by file_path,
                    # and a new column on a table that exists in every deployed
                    # install is a migration for something a JSON field answers.
                    "content_hash": content_hash,
                }

                creates.append(
                    DocumentationEmbeddingCreate(
                        source=file_path,
                        title=f"{filename} ({section})",
                        content=chunk_content,
                        embedding=embedding,
                        doc_metadata=metadata,
                        group_id=self.group_id,
                        file_path=file_path,
                        created_by=created_by,
                    )
                )

            # Store all chunk rows in ONE bulk insert. When the active memory
            # backend is Lakebase, this writes to the Lakebase memory instance
            # (kasal.documentation_embeddings) — the same pgvector instance as
            # crew memory — otherwise to the app DB. We bulk-insert on a single
            # session (never the SQLite per-row queue, whose separate session
            # deadlocks against this upload's open transaction).
            from src.services.knowledge.embedding_session import (
                ensure_lakebase_doc_table,
                knowledge_embedding_session,
            )

            embedded_chunks = len(creates)
            if creates:
                async with knowledge_embedding_session(
                    self.session, self.group_id, user_token
                ) as (store_session, is_lakebase):
                    repository = DocumentationEmbeddingRepository(
                        store_session, model=KnowledgeEmbedding
                    )
                    try:
                        if is_lakebase:
                            # Self-heal the Lakebase table (add embedding/group_id/
                            # file_path columns if it pre-dates the pgvector schema)
                            # before inserting.
                            await ensure_lakebase_doc_table(store_session)
                        await repository.bulk_create(creates)
                        if not is_lakebase:
                            # Lakebase sessions commit on context exit; the app
                            # session must be committed explicitly.
                            await store_session.commit()
                        logger.info(
                            f"[EMBEDDING] Stored {embedded_chunks} chunks in knowledge_embeddings "
                            f"(lakebase={is_lakebase}, group={self.group_id}, file={file_path})"
                        )
                    except Exception as store_error:
                        logger.error(
                            f"[EMBEDDING] Failed to store knowledge embeddings: {store_error}"
                        )
                        if not is_lakebase:
                            await store_session.rollback()
                        raise

            logger.info(
                f"[EMBEDDING] Successfully embedded {embedded_chunks}/{len(chunks)} chunks"
            )

            if embedded_chunks == 0:
                # Every chunk was parsed but none embedded — the embedding model
                # returned nothing (no Databricks auth and/or local Ollama model
                # unreachable). Report this as an error instead of a misleading
                # "success" so the upload surfaces the failure.
                provider = embedder_config.get("provider")
                model = embedder_config.get("config", {}).get("model")
                logger.error(
                    f"[EMBEDDING] Embedded 0/{len(chunks)} chunks for {file_path} — "
                    f"embedding model unavailable (provider={provider}, model={model})"
                )
                return {
                    "status": "error",
                    "chunks_processed": len(chunks),
                    "chunks_embedded": 0,
                    "index_name": "knowledge_embeddings (pgvector)",
                    "error": "embedding_model_unavailable",
                    "message": (
                        f"Parsed {len(chunks)} chunks but embedded 0 — the embedding "
                        f"model is unavailable (provider={provider}, model={model})."
                    ),
                }

            return {
                "status": "success",
                "chunks_processed": len(chunks),
                "chunks_embedded": embedded_chunks,
                "index_name": "knowledge_embeddings (pgvector)",
                "message": f"Successfully embedded {embedded_chunks} chunks from {file_path}",
            }

        except Exception as e:
            logger.error(
                f"[EMBEDDING] Error embedding file {file_path}: {e}", exc_info=True
            )
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to embed file {file_path}",
            }

    async def file_already_embedded(
        self,
        file_path: str,
        content_hash: str,
        created_by: Optional[str] = None,
        user_token: Optional[str] = None,
    ) -> bool:
        """True when this exact content is already stored under this path.

        Compares the hash carried in ``doc_metadata`` on any one existing chunk
        — every chunk of a file carries the same one, so a single row answers
        it. A file with no stored hash (uploaded before this existed) reads as
        "not embedded", so the next upload replaces it and starts carrying one.

        Never raises: a failure here must not block an upload. The cost of
        guessing wrong is a duplicate, which is what happened before anyway.
        """
        if not content_hash:
            return False
        try:
            from src.models.documentation_embedding import KnowledgeEmbedding
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.services.knowledge.embedding_session import (
                knowledge_embedding_session,
            )

            async with knowledge_embedding_session(
                self.session, self.group_id, user_token
            ) as (store_session, _is_lakebase):
                repository = DocumentationEmbeddingRepository(
                    store_session, model=KnowledgeEmbedding
                )
                stored = await repository.find_content_hash(
                    group_id=self.group_id,
                    file_path=file_path,
                    created_by=created_by,
                )
            return bool(stored) and stored == content_hash
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not check whether %s is embedded: %s", file_path, exc)
            return False

    async def forget_file(
        self,
        file_path: str,
        created_by: Optional[str] = None,
        user_token: Optional[str] = None,
    ) -> int:
        """Remove every chunk stored under ``file_path``. Returns rows deleted.

        Used when a file's content CHANGED (replace, not append) and when a user
        detaches a file. Scoped to the group and, when supplied, the uploader —
        one user must never delete another's knowledge.
        """
        try:
            from src.models.documentation_embedding import KnowledgeEmbedding
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.services.knowledge.embedding_session import (
                knowledge_embedding_session,
            )

            async with knowledge_embedding_session(
                self.session, self.group_id, user_token
            ) as (store_session, _is_lakebase):
                repository = DocumentationEmbeddingRepository(
                    store_session, model=KnowledgeEmbedding
                )
                deleted = await repository.delete_by_path(
                    group_id=self.group_id,
                    file_path=file_path,
                    created_by=created_by,
                )
                await store_session.commit()
            return deleted
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not remove chunks for %s: %s", file_path, exc)
            return 0

    async def purge_expired(self, user_token: Optional[str] = None) -> int:
        """Delete knowledge embeddings past the TTL (KNOWLEDGE_TTL_DAYS).

        Runs opportunistically before each upload so the table never
        accumulates stale uploads — there is no scheduler dependency. Search
        additionally excludes expired rows, so expiry takes effect even
        between uploads. Failures never block an upload (non-fatal).

        Returns:
            Number of purged chunk rows.
        """
        if KNOWLEDGE_TTL_DAYS <= 0:
            return 0
        from src.models.documentation_embedding import KnowledgeEmbedding
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )
        from src.services.knowledge.embedding_session import knowledge_embedding_session

        cutoff = datetime.utcnow() - timedelta(days=KNOWLEDGE_TTL_DAYS)
        try:
            async with knowledge_embedding_session(
                self.session, self.group_id, user_token
            ) as (store_session, is_lakebase):
                # Scoped to THIS group: the table is shared across tenants and
                # one tenant's request must never run DML on another's rows
                # (defense in depth — expired rows are filtered from search
                # regardless, and each group sweeps its own on upload).
                repository = DocumentationEmbeddingRepository(
                    store_session, model=KnowledgeEmbedding
                )
                try:
                    purged = await repository.delete_expired(self.group_id, cutoff)
                except Exception:
                    # CRITICAL: on the app-DB path store_session IS self.session
                    # — the SAME session the subsequent embed insert reuses. A
                    # failed DELETE leaves the transaction ABORTED; without this
                    # rollback Postgres rejects every following statement with
                    # InFailedSQLTransactionError, so the real purge error surfaces
                    # only as a decoy on the bulk INSERT. Roll back here so the
                    # session is clean and the embed can proceed (or fail with its
                    # OWN error). The Lakebase path rolls back in its own session
                    # context, so scope this to the app-DB path.
                    if not is_lakebase:
                        await store_session.rollback()
                    raise
                if not is_lakebase:
                    await store_session.commit()
                if purged:
                    logger.info(
                        f"[EMBEDDING] TTL purge removed {purged} knowledge chunks "
                        f"older than {KNOWLEDGE_TTL_DAYS} days"
                    )
                return purged
        except Exception as e:
            # Real cause is logged at error level (it was previously masked by the
            # aborted-transaction decoy on the next statement). Still non-fatal:
            # a purge failure must never block an upload.
            logger.error(
                f"[EMBEDDING] TTL purge failed (non-fatal, upload continues): "
                f"{type(e).__name__}: {e}"
            )
            return 0

    async def _chunk_with_context(
        self, content: str, file_path: str, chunk_size: int = 1000, overlap: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Chunk content with context enrichment.

        Args:
            content: Content to chunk
            file_path: File path for logging
            chunk_size: Size of each chunk
            overlap: Overlap between chunks

        Returns:
            List of chunk dictionaries with metadata
        """
        try:
            # Generate document summary first
            filename = file_path.split("/")[-1]
            document_summary = await self._generate_document_summary(content, filename)

            # Split content into chunks
            chunks = []
            start = 0
            content_length = len(content)
            chunk_index = 0

            while start < content_length:
                end = min(start + chunk_size, content_length)
                chunk_text = content[start:end]

                # Determine section based on position
                position_pct = (start / content_length) * 100
                if position_pct < 25:
                    section = "Introduction"
                elif position_pct < 50:
                    section = "Early Content"
                elif position_pct < 75:
                    section = "Middle Content"
                else:
                    section = "Later Content"

                # Create contextual chunk
                contextual_content = self._create_contextual_chunk(
                    chunk_text=chunk_text,
                    document_summary=document_summary,
                    filename=filename,
                    section=section,
                    chunk_index=chunk_index,
                )

                chunks.append(
                    {
                        "content": contextual_content,
                        "raw_content": chunk_text,
                        "section": section,
                        "document_summary": document_summary,
                        "chunk_index": chunk_index,
                    }
                )

                chunk_index += 1
                start = end - overlap if end < content_length else content_length

            logger.info(f"[CHUNKING] Created {len(chunks)} context-enriched chunks")
            return chunks

        except Exception as e:
            logger.error(f"[CHUNKING] Error in _chunk_with_context: {e}")
            return []

    def _create_contextual_chunk(
        self,
        chunk_text: str,
        document_summary: str,
        filename: str,
        section: str,
        chunk_index: int,
    ) -> str:
        """
        Create a context-enriched chunk for better embedding.

        Args:
            chunk_text: Raw chunk text
            document_summary: Summary of the entire document
            filename: Name of the file
            section: Section identifier
            chunk_index: Index of this chunk

        Returns:
            Context-enriched chunk text
        """
        context_prefix = f"Document: {filename}\n"
        context_prefix += f"Summary: {document_summary}\n"
        context_prefix += f"Section: {section} (Part {chunk_index + 1})\n"
        context_prefix += "---\n"

        return context_prefix + chunk_text

    async def _generate_document_summary(self, content: str, filename: str) -> str:
        """
        Generate a summary of the document for context.

        Args:
            content: Full document content
            filename: Name of the file

        Returns:
            Document summary
        """
        try:
            # Simple summary: first 200 characters + file type
            content_type = self._detect_content_type(filename)
            preview = content[:200].strip()
            if len(content) > 200:
                preview += "..."

            return f"{content_type} file containing: {preview}"

        except Exception as e:
            logger.error(f"[SUMMARY] Error generating summary: {e}")
            return f"{filename} content"

    def _detect_content_type(self, filename: str) -> str:
        """
        Detect content type from filename.

        Args:
            filename: Name of the file

        Returns:
            Content type string
        """
        ext = filename.lower().split(".")[-1] if "." in filename else "unknown"
        content_types = {
            "pdf": "PDF Document",
            "txt": "Text Document",
            "md": "Markdown Document",
            "doc": "Word Document",
            "docx": "Word Document",
            "csv": "CSV Data",
            "json": "JSON Data",
            "xml": "XML Data",
            "html": "HTML Document",
            "py": "Python Code",
            "js": "JavaScript Code",
            "ts": "TypeScript Code",
        }
        return content_types.get(ext, "Document")
