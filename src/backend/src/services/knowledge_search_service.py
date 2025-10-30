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
        user_token: Optional[str] = None
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

        Returns:
            List of search results with content and metadata
        """
        logger.info(f"Knowledge search: query='{query}', group={self.group_id}, agent={agent_id}, limit={limit}")
        logger.info(f"File paths parameter: {file_paths}")

        try:
            # Get vector storage configuration
            vector_storage = await self._get_vector_storage(user_token)
            if not vector_storage:
                logger.warning("Vector storage not configured")
                return []

            document_index = vector_storage.index_name
            endpoint_name = vector_storage.endpoint_name

            # Import dependencies
            from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
            from src.core.llm_manager import LLMManager

            # Get schema and generate query embedding
            search_columns = DatabricksIndexSchemas.get_search_columns("document")

            try:
                query_embedding = await LLMManager.get_embedding(query, model="databricks-gte-large-en")
                if not query_embedding:
                    logger.error("Failed to generate query embedding")
                    return []
            except Exception as embed_error:
                logger.error(f"Error generating embedding: {embed_error}")
                return []

            index_repo = vector_storage.repository

            # Quick readiness check
            try:
                index_info = await asyncio.wait_for(
                    index_repo.get_index(index_name=document_index, endpoint_name=endpoint_name, user_token=user_token),
                    timeout=3
                )
                ready = False
                try:
                    if hasattr(index_info, 'success'):
                        ready = bool(getattr(index_info, 'success', False) and getattr(getattr(index_info, 'index', None), 'ready', False))
                    elif isinstance(index_info, dict):
                        status = index_info.get('status') or {}
                        ready = bool(status.get('ready') or index_info.get('ready'))
                except Exception:
                    ready = False
                if not ready:
                    logger.info("Databricks index not ready (provisioning)")
                    return []
            except Exception as e:
                logger.info(f"Skipping search due to provisioning: {e}")
                return []

            # Build search filters
            filters = {"group_id": self.group_id}

            if file_paths:
                logger.info(f"[SEARCH SERVICE v2] About to resolve file_paths: {file_paths}")
                # Resolve filenames to full paths if needed
                resolved_paths = await self._resolve_file_paths(
                    file_paths,
                    index_repo,
                    document_index,
                    endpoint_name,
                    query_embedding,
                    search_columns,
                    user_token
                )

                if resolved_paths:
                    # Databricks Vector Search standard endpoint syntax:
                    # Single value: {"source": "path"} or Multiple values: {"source": ["path1", "path2"]}
                    # The list format works for both single and multiple files
                    filters["source"] = resolved_paths
                    logger.info(f"Resolved file paths for filtering: {resolved_paths}")
                else:
                    logger.warning(f"Could not resolve any paths from: {file_paths}")
                    # Don't add source filter - will search all files for this group
                    pass

            # DISABLED: agent_ids filter with $contains doesn't work correctly in Databricks Vector Search
            # Tested with direct API calls - returns 0 results even when agent_ids match
            # See: /tmp/test_agent_ids_filter.py for proof
            # Access control is still maintained via:
            #   - group_id filter (tenant isolation)
            #   - source filter (file-level access, path includes group_id)
            # TODO: Re-enable when Databricks fixes the $contains operator for array fields
            # if agent_id:
            #     filters["agent_ids"] = {"$contains": agent_id}

            # Perform similarity search
            try:
                search_results = await asyncio.wait_for(
                    index_repo.similarity_search(
                        index_name=document_index,
                        endpoint_name=endpoint_name,
                        query_vector=query_embedding,
                        columns=search_columns,
                        filters=filters,
                        num_results=limit,
                        user_token=user_token
                    ),
                    timeout=10
                )
            except asyncio.TimeoutError:
                logger.warning("Search timed out")
                return []
            except Exception as search_error:
                logger.error(f"Search failed: {search_error}")
                return []

            # Extract and format results
            data_array = search_results.get('results', {}).get('result', {}).get('data_array', [])
            if not data_array:
                logger.warning("No results found")
                return []

            positions = DatabricksIndexSchemas.get_column_positions("document")
            formatted_results = []

            for idx, result in enumerate(data_array):
                try:
                    content = result[positions['content']] if 'content' in positions and len(result) > positions['content'] else ""
                    source = result[positions['source']] if 'source' in positions and len(result) > positions['source'] else ""
                    title = result[positions['title']] if 'title' in positions and len(result) > positions['title'] else ""
                    chunk_index = result[positions['chunk_index']] if 'chunk_index' in positions and len(result) > positions['chunk_index'] else 0
                    score = result[-1] if len(result) > len(positions) else 0.0

                    formatted_results.append({
                        "content": content,
                        "metadata": {
                            "source": source,
                            "title": title,
                            "chunk_index": chunk_index,
                            "score": score,
                            "group_id": self.group_id,
                            "execution_id": execution_id
                        }
                    })

                except Exception as e:
                    logger.error(f"Error formatting result: {e}")
                    continue

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
                enable_short_term=backend.enable_short_term,
                enable_long_term=backend.enable_long_term,
                enable_entity=backend.enable_entity,
                custom_config=backend.custom_config
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
