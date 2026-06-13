"""
Knowledge Search Service

Handles searching knowledge files in vector storage.
Separated from DatabricksKnowledgeService for clean architecture.
"""
from typing import Dict, Any, List, Optional
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

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
        logger.info(f"Knowledge search: query='{query}', group={self.group_id}, agent={agent_id}, user={created_by}, limit={limit}")
        logger.info(f"File paths parameter: {file_paths}")

        try:
            from src.core.llm_manager import LLMManager
            from src.services.documentation_embedding_service import DocumentationEmbeddingService

            # Generate the query embedding with the same model used at ingest
            # time (databricks-gte-large-en, 1024 dims) so it matches the stored
            # vectors in the documentation_embeddings pgvector table.
            try:
                query_embedding = await LLMManager.get_embedding(query, model="databricks-gte-large-en")
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
            from src.services.knowledge_embedding_session import knowledge_embedding_session
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.models.documentation_embedding import KnowledgeEmbedding
            formatted_results = []
            try:
                async with knowledge_embedding_session(
                    self.session, self.group_id, user_token
                ) as (search_session, _is_lakebase):
                    repo = DocumentationEmbeddingRepository(search_session, model=KnowledgeEmbedding)
                    # Search GROUP-WIDE (no SQL file_path filter): the tool often
                    # passes a bare filename ("test.txt") while the stored path is
                    # the full "/Volumes/.../test.txt", so an exact-path filter
                    # would wrongly match nothing. We soft-filter by basename below
                    # and fall back to all group rows if nothing matches.
                    rows = await repo.search_similar(
                        query_embedding,
                        limit=limit,
                        group_id=self.group_id,
                        file_paths=None,
                    ) or []
                    total_group_rows = len(rows)

                    if file_paths:
                        wanted = {fp.rsplit('/', 1)[-1] for fp in file_paths if fp}
                        narrowed = [
                            r for r in rows
                            if ((getattr(r, 'file_path', None) or '').rsplit('/', 1)[-1] in wanted)
                        ]
                        # Only narrow when at least one file matched; otherwise keep
                        # the group-wide results (filename mismatch shouldn't blank it).
                        if narrowed:
                            rows = narrowed

                    # Per-user isolation: only chunks uploaded by the requesting
                    # user are returned (rows without an uploader — legacy or
                    # built-in — stay group-shared).
                    if created_by:
                        rows = [
                            r for r in rows
                            if (owner := getattr(r, 'created_by', None)) is None
                            or owner == created_by
                        ]

                    # TTL: expired chunks are excluded immediately, even before
                    # the next upload-time purge sweeps them out of the table.
                    from src.services.knowledge_embedding_service import KNOWLEDGE_TTL_DAYS
                    if KNOWLEDGE_TTL_DAYS > 0:
                        from datetime import datetime, timedelta, timezone

                        cutoff = datetime.now(timezone.utc) - timedelta(days=KNOWLEDGE_TTL_DAYS)

                        def _fresh(r) -> bool:
                            created = getattr(r, 'created_at', None)
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
                    from src.services.knowledge_embedding_session import emit_knowledge_span
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
                            source = getattr(row, "file_path", None) or getattr(row, "source", "") or ""
                            formatted_results.append({
                                "content": getattr(row, "content", "") or "",
                                "metadata": {
                                    "source": source,
                                    "title": getattr(row, "title", "") or "",
                                    "chunk_index": metadata.get("chunk_index", 0),
                                    # pgvector orders by distance but does not return a
                                    # similarity score column; results are already ranked.
                                    "score": float(metadata.get("score", 0.0) or 0.0),
                                    "group_id": getattr(row, "group_id", None) or self.group_id,
                                    "execution_id": execution_id,
                                },
                            })
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
                from src.services.knowledge_embedding_session import emit_knowledge_span
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

    async def _resolve_file_paths(
        self,
        file_paths: List[str],
        index_repo,
        document_index: str,
        endpoint_name: str,
        query_embedding: List[float],
        search_columns: List[str],
        user_token: Optional[str] = None
    ) -> Optional[List[str]]:
        """
        Resolve filenames to full volume paths by querying the index.

        If file_paths contains full paths (starting with /Volumes), returns them as-is.
        If file_paths contains just filenames, queries the index to find matching full paths.

        Args:
            file_paths: List of file paths or filenames to resolve
            index_repo: Repository for index operations
            document_index: Index name
            endpoint_name: Endpoint name
            query_embedding: Query embedding vector
            search_columns: Columns to retrieve
            user_token: Optional user token for OBO authentication

        Returns:
            List of resolved full paths, or None if resolution fails
        """
        # Check if all paths are already full paths
        if all(path.startswith("/Volumes") for path in file_paths):
            logger.info(f"File paths are already full paths: {file_paths}")
            return file_paths

        # Extract filenames that need resolution
        filenames_to_resolve = []
        full_paths = []

        for path in file_paths:
            if path.startswith("/Volumes"):
                full_paths.append(path)
            else:
                # Extract filename from path (in case it has directory separators)
                filename = path.split("/")[-1]
                filenames_to_resolve.append(filename)

        # If no filenames need resolution, return full paths
        if not filenames_to_resolve:
            return full_paths if full_paths else None

        # Query index to find all sources for this group
        try:
            logger.info(f"Resolving filenames to full paths: {filenames_to_resolve}")

            # Query without source filter to get all files for this group
            all_sources_results = await asyncio.wait_for(
                index_repo.similarity_search(
                    index_name=document_index,
                    endpoint_name=endpoint_name,
                    query_vector=query_embedding,
                    columns=search_columns,
                    filters={"group_id": self.group_id},  # Only filter by group_id
                    num_results=100,  # Get more results to find all unique files
                    user_token=user_token
                ),
                timeout=5
            )

            # Repository returns {'success': bool, 'results': {...}, 'message': str}
            if not all_sources_results or not all_sources_results.get('success'):
                logger.warning(f"No results from index to resolve filenames: {all_sources_results.get('message') if all_sources_results else 'None'}")
                return None

            # Extract the actual results from the repository response
            results = all_sources_results.get('results', {})
            if not results:
                logger.warning("No 'results' key in repository response")
                return None

            # Extract unique source paths from results
            from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
            positions = DatabricksIndexSchemas.get_column_positions("document")
            source_position = positions["source"]

            # The 'results' key contains the search response with 'result' -> 'data_array'
            data_array = results.get('result', {}).get('data_array', [])

            unique_sources = set()
            for result in data_array:
                if len(result) > source_position:
                    source = result[source_position]
                    if source:
                        unique_sources.add(source)

            logger.info(f"Found {len(unique_sources)} unique sources in index for group {self.group_id}")

            # Match filenames to full paths
            resolved = []
            for filename in filenames_to_resolve:
                matched = False
                for source_path in unique_sources:
                    source_filename = source_path.split("/")[-1]
                    if source_filename == filename:
                        resolved.append(source_path)
                        logger.info(f"Resolved '{filename}' to '{source_path}'")
                        matched = True
                        break

                if not matched:
                    logger.warning(f"Could not resolve filename '{filename}' to any indexed path")

            # Combine resolved paths with any full paths that were already provided
            all_resolved = full_paths + resolved

            return all_resolved if all_resolved else None

        except Exception as e:
            logger.error(f"Error resolving file paths: {e}")
            return None

    async def _get_vector_storage(self, user_token: Optional[str] = None):
        """
        Get vector storage instance.

        Args:
            user_token: Optional user token for OBO authentication

        Returns:
            DatabricksVectorStorage instance or None
        """
        try:
            from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
            from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

            # Lazy initialization of memory backend service
            if self._memory_backend_service is None:
                from src.services.memory_backend_service import MemoryBackendService
                self._memory_backend_service = MemoryBackendService(self.session)

            # Get memory backends for this group
            group_backends = await self._memory_backend_service.get_memory_backends(self.group_id)

            # Filter active Databricks backends
            databricks_backends = [
                b for b in group_backends
                if b.is_active and b.backend_type == MemoryBackendType.DATABRICKS
            ]

            if not databricks_backends:
                logger.warning("No active Databricks memory backend found")
                return None

            # Use most recent backend
            databricks_backends.sort(key=lambda x: x.created_at, reverse=True)
            backend = databricks_backends[0]

            # Convert to config
            memory_config = MemoryBackendConfig(
                backend_type=backend.backend_type,
                databricks_config=backend.databricks_config,
                cognitive_config=backend.cognitive_config,
                custom_config=backend.custom_config,
            )

            db_config = memory_config.databricks_config
            if not db_config:
                logger.warning("Databricks configuration is None")
                return None

            # Get document index
            document_index = None
            if hasattr(db_config, 'document_index'):
                document_index = db_config.document_index
            elif isinstance(db_config, dict):
                document_index = db_config.get('document_index')

            if not document_index:
                logger.warning("Document index not configured")
                return None

            # Extract configuration
            if hasattr(db_config, 'endpoint_name'):
                endpoint_name = getattr(db_config, 'document_endpoint_name', None) or db_config.endpoint_name
                workspace_url = db_config.workspace_url
                embedding_dimension = db_config.embedding_dimension or 1024
                personal_access_token = db_config.personal_access_token
                service_principal_client_id = db_config.service_principal_client_id
                service_principal_client_secret = db_config.service_principal_client_secret
            elif isinstance(db_config, dict):
                endpoint_name = db_config.get('document_endpoint_name') or db_config.get('endpoint_name')
                workspace_url = db_config.get('workspace_url')
                embedding_dimension = db_config.get('embedding_dimension', 1024)
                personal_access_token = db_config.get('personal_access_token')
                service_principal_client_id = db_config.get('service_principal_client_id')
                service_principal_client_secret = db_config.get('service_principal_client_secret')
            else:
                logger.error(f"Unexpected databricks_config type: {type(db_config)}")
                return None

            # Create storage instance
            storage = DatabricksVectorStorage(
                endpoint_name=endpoint_name,
                index_name=document_index,
                crew_id="knowledge_files",
                memory_type="document",
                embedding_dimension=embedding_dimension,
                workspace_url=workspace_url,
                personal_access_token=personal_access_token,
                service_principal_client_id=service_principal_client_id,
                service_principal_client_secret=service_principal_client_secret,
                user_token=user_token
            )

            return storage

        except Exception as e:
            logger.error(f"Error creating vector storage: {e}", exc_info=True)
            return None
