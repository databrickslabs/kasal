"""
Knowledge Embedding Service

Handles embedding of knowledge files into vector storage.
Separated from DatabricksKnowledgeService for clean architecture.
"""
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


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
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Embed a file's content into vector storage.

        Args:
            file_path: Full path to the file
            file_content: Content of the file to embed
            execution_id: Execution ID for scoping
            agent_ids: Optional list of agent IDs for access control
            user_token: Optional user token for OBO authentication

        Returns:
            Embedding result with status and metadata
        """
        try:
            logger.info(f"[EMBEDDING] Starting embedding for file: {file_path}")

            if agent_ids:
                logger.info(f"[EMBEDDING] Agent IDs for access control: {agent_ids}")
            else:
                logger.warning(f"[EMBEDDING] No agent_ids provided")

            # Chunk the content with context enrichment
            chunks = await self._chunk_with_context(file_content, file_path)
            if not chunks:
                logger.warning(f"[EMBEDDING] No chunks generated for file: {file_path}")
                return {"status": "skipped", "reason": "No chunks generated"}

            logger.info(f"[EMBEDDING] Generated {len(chunks)} context-enriched chunks")

            # Get vector storage
            vector_storage = await self._get_vector_storage(user_token)
            if not vector_storage:
                logger.warning(f"[EMBEDDING] Vector search not configured")
                return {"status": "skipped", "reason": "Vector search not configured"}

            logger.info(f"[EMBEDDING] Using vector index: {vector_storage.index_name}")

            # Process each chunk
            embedded_chunks = 0
            filename = file_path.split('/')[-1]

            for i, chunk_data in enumerate(chunks):
                try:
                    chunk_content = chunk_data['content']
                    raw_content = chunk_data.get('raw_content', chunk_content)
                    section = chunk_data.get('section', f'Section {i+1}')
                    document_summary = chunk_data.get('document_summary', '')

                    # Prepare metadata
                    metadata = {
                        'source': file_path,
                        'filename': filename,
                        'execution_id': execution_id,
                        'group_id': self.group_id,
                        'chunk_index': i,
                        'total_chunks': len(chunks),
                        'section': section,
                        'parent_document_id': f"{self.group_id}:{execution_id}:{filename}",
                        'document_summary': document_summary,
                        'file_path': file_path,
                        'created_at': datetime.utcnow().isoformat(),
                        'type': 'knowledge_source',
                        'content_type': self._detect_content_type(filename)
                    }

                    # Convert agent_ids to JSON string for storage
                    import json
                    agent_ids_json = json.dumps(agent_ids) if agent_ids else json.dumps([])

                    data = {
                        'content': chunk_content,
                        'agent_ids': agent_ids_json,
                        'group_id': self.group_id,
                        'metadata': metadata,
                        'context': {
                            'query_text': f"Knowledge file: {filename}",
                            'session_id': execution_id,
                            'interaction_sequence': i
                        }
                    }

                    # Save to vector storage
                    await vector_storage.save(data)
                    embedded_chunks += 1

                    logger.info(f"[EMBEDDING] Embedded chunk {i+1}/{len(chunks)} for file: {file_path}")

                except Exception as chunk_error:
                    logger.error(f"[EMBEDDING] Error embedding chunk {i}: {chunk_error}")
                    continue

            logger.info(f"[EMBEDDING] Successfully embedded {embedded_chunks}/{len(chunks)} chunks")

            return {
                "status": "success",
                "chunks_processed": len(chunks),
                "chunks_embedded": embedded_chunks,
                "index_name": vector_storage.index_name,
                "message": f"Successfully embedded {embedded_chunks} chunks from {file_path}"
            }

        except Exception as e:
            logger.error(f"[EMBEDDING] Error embedding file {file_path}: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to embed file {file_path}"
            }

    async def _chunk_with_context(
        self,
        content: str,
        file_path: str,
        chunk_size: int = 1000,
        overlap: int = 200
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
            filename = file_path.split('/')[-1]
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
                    chunk_index=chunk_index
                )

                chunks.append({
                    'content': contextual_content,
                    'raw_content': chunk_text,
                    'section': section,
                    'document_summary': document_summary,
                    'chunk_index': chunk_index
                })

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
        chunk_index: int
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
        ext = filename.lower().split('.')[-1] if '.' in filename else 'unknown'
        content_types = {
            'pdf': 'PDF Document',
            'txt': 'Text Document',
            'md': 'Markdown Document',
            'doc': 'Word Document',
            'docx': 'Word Document',
            'csv': 'CSV Data',
            'json': 'JSON Data',
            'xml': 'XML Data',
            'html': 'HTML Document',
            'py': 'Python Code',
            'js': 'JavaScript Code',
            'ts': 'TypeScript Code'
        }
        return content_types.get(ext, 'Document')

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
