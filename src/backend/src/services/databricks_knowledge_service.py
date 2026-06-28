"""
Databricks Knowledge Source Service
"""

import asyncio
import io
import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import UploadFile

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import KasalError, UnprocessableEntityError
from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.repositories.databricks_volume_repository import DatabricksVolumeRepository

logger = logging.getLogger(__name__)

# SECURITY: cap knowledge-file uploads so a single request cannot buffer an
# unbounded amount of data into memory (DoS). Enforced via Content-Length up
# front when available, with a post-read backstop.
MAX_KNOWLEDGE_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB


class DatabricksKnowledgeService:
    """Service for managing knowledge files in Databricks Volumes."""

    def __init__(
        self,
        session: AsyncSession,
        group_id: str,
        created_by_email: Optional[str] = None,
        user_token: Optional[str] = None,
    ):
        """
        Initialize the Databricks Knowledge Service.

        Args:
            session: Database session
            group_id: Group ID for tenant isolation
            created_by_email: Email of the user
            user_token: Optional user token for OBO authentication
        """
        self.session = session
        self.repository = DatabricksConfigRepository(session)
        self.volume_repository = DatabricksVolumeRepository(
            user_token=user_token, group_id=group_id
        )
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.user_token = user_token

        # Initialize specialized services (proper separation of concerns)
        from src.services.knowledge_embedding_service import KnowledgeEmbeddingService
        from src.services.knowledge_search_service import KnowledgeSearchService

        self.embedding_service = KnowledgeEmbeddingService(session, group_id)
        self.search_service = KnowledgeSearchService(session, group_id)

    async def upload_knowledge_file(
        self,
        file: UploadFile,
        execution_id: str,
        group_id: str,
        volume_config: Optional[Dict[str, Any]] = None,
        agent_ids: Optional[List[str]] = None,
        user_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest an uploaded knowledge file: stage it in a local temp file,
        embed its content into the local knowledge store (knowledge_embeddings
        — SQLite locally, Lakebase pgvector when deployed) stamped with the
        uploading user, then delete the temp file. No Databricks Volume is
        involved and no raw upload is retained.

        Args:
            file: The uploaded file
            execution_id: Execution ID for scoping
            group_id: Group ID for tenant isolation
            volume_config: Legacy volume configuration (only selected_agents
                is still read; the volume itself is no longer used for uploads)
            agent_ids: Optional list of agent IDs that can access this knowledge source
            user_token: Optional user token for OBO authentication

        Returns:
            Upload response with the logical file path and embedding metadata
        """
        logger.info("=" * 60)
        logger.info("STARTING KNOWLEDGE FILE UPLOAD")

        # CRITICAL DEBUG: Log what agent_ids we receive in the service
        logger.info(
            f"[SERVICE] 🔍 AGENT_IDS RECEIVED: {agent_ids} (type: {type(agent_ids)}, length: {len(agent_ids) if agent_ids else 0})"
        )

        if agent_ids:
            logger.info(f"[SERVICE] ✅ Agent IDs detected: {agent_ids}")
        else:
            logger.warning(
                f"[SERVICE] ⚠️ No agent_ids provided - this will result in null agent_ids in vector index!"
            )

        logger.info(f"File: {file.filename}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"Volume Config: {volume_config}")
        logger.info("=" * 60)

        try:
            # SECURITY: reject oversized uploads up front (via Content-Length when
            # available) so we never buffer an unbounded body into memory.
            _max = MAX_KNOWLEDGE_UPLOAD_BYTES
            declared_size = getattr(file, "size", None)
            if isinstance(declared_size, int) and declared_size > _max:
                raise UnprocessableEntityError(
                    detail=f"File exceeds the {_max // (1024 * 1024)} MB upload limit"
                )

            # Read file content
            content = await file.read()
            file_size = len(content)
            # Backstop in case Content-Length was absent or understated.
            if file_size > _max:
                raise UnprocessableEntityError(
                    detail=f"File exceeds the {_max // (1024 * 1024)} MB upload limit"
                )
            logger.info(f"File size: {file_size} bytes ({file_size/1024:.2f} KB)")

            # TTL sweep: purge expired knowledge BEFORE adding more, so the
            # embedding store never bloats (KNOWLEDGE_TTL_DAYS; non-fatal).
            await self.embedding_service.purge_expired(user_token=user_token)

            # Stage the raw upload in a LOCAL TEMP FILE — its content is
            # embedded into the local knowledge store (knowledge_embeddings:
            # SQLite locally, Lakebase pgvector when deployed), stamped with
            # the uploading user for per-user isolation, and the temp file is
            # deleted as soon as the embedding attempt finishes. No Databricks
            # Volume is required and no raw upload is retained anywhere.
            safe_name = os.path.basename(file.filename or "upload.txt") or "upload.txt"
            tmp_fd, tmp_path = tempfile.mkstemp(
                prefix="kasal-knowledge-", suffix=f"-{safe_name}"
            )
            logger.info(f"[UPLOAD] Staged upload in temp file: {tmp_path}")
            try:
                with os.fdopen(tmp_fd, "wb") as staged:
                    staged.write(content)
                with open(tmp_path, "rb") as staged:
                    raw_bytes = staged.read()

                extraction = self._extract_text_content(safe_name, raw_bytes)
                if extraction.get("status") != "success":
                    raise UnprocessableEntityError(
                        detail=extraction.get(
                            "message", f"Could not extract text from '{safe_name}'"
                        )
                    )

                # Logical path — the stable identity of the embedded chunks.
                # The crew's tool_configs carry it and the search tool
                # soft-filters by its basename; no physical file lives here.
                file_path = f"uploads/{group_id}/{execution_id}/{safe_name}"

                logger.info(f"[UPLOAD] Starting embedding for file: {file_path}")
                embedding_result = await self.embedding_service.embed_file(
                    file_path=file_path,
                    file_content=extraction.get("content", ""),
                    execution_id=execution_id,
                    agent_ids=agent_ids,
                    user_token=user_token,
                    created_by=self.created_by_email,
                )
            finally:
                # The raw upload never outlives the embedding attempt.
                try:
                    os.unlink(tmp_path)
                    logger.info(f"[UPLOAD] Deleted temp file: {tmp_path}")
                except OSError:
                    pass

            if embedding_result.get("status") != "success":
                # Embedding IS the upload now — nothing was persisted, so the
                # caller (and the UI) must see a real failure.
                raise UnprocessableEntityError(
                    detail=(
                        f"Failed to embed '{safe_name}': "
                        f"{embedding_result.get('message') or embedding_result.get('reason') or 'unknown error'}"
                    )
                )

            logger.info(
                f"[UPLOAD] Embedding completed with result: {embedding_result.get('status')}"
            )

            # Ensure DatabricksKnowledgeSearchTool (id 36) is in this workspace's
            # tool catalog (group_tools), so it shows in the task tool picker AND
            # the crew can resolve it. The frontend only toggles the GLOBAL tool
            # flag; without a group_tools mapping, get_enabled_tools_for_group
            # excludes it and the agent gets "Tool not found in available tools".
            await self._ensure_knowledge_tool_in_group(group_id)

            response = {
                "status": "success",
                "path": file_path,
                "filename": safe_name,
                "size": file_size,
                "execution_id": execution_id,
                "group_id": group_id,
                "created_by": self.created_by_email,
                "uploaded_at": datetime.now().isoformat(),
                "selected_agents": (volume_config or {}).get("selected_agents", []),
                "embedding_result": embedding_result,
                "upload_method": "temp_embed",
                "message": (
                    f"File {safe_name} embedded successfully "
                    f"({embedding_result.get('chunks_embedded', 0)} chunks); "
                    "the temporary upload was deleted"
                ),
            }

            logger.info(f"Returning unified response: {response}")
            return response

        except KasalError:
            # Already carries an HTTP status + actionable detail — let the global
            # handler map it so the client (and the UI) see the real error.
            raise
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"ERROR in upload_knowledge_file: {str(e)}")
            logger.error("=" * 60, exc_info=True)
            raise KasalError(detail=f"Knowledge upload failed: {e}") from e

    # Tool id of DatabricksKnowledgeSearchTool (see src/seeds/tools.py).
    _KNOWLEDGE_SEARCH_TOOL_ID = 36

    async def _ensure_knowledge_tool_in_group(self, group_id: str) -> None:
        """Register DatabricksKnowledgeSearchTool in the workspace's tool catalog.

        Crews resolve tools via get_enabled_tools_for_group, which requires a
        group_tools mapping — not just the global tools.enabled flag. Without
        this, an uploaded knowledge file's task references a tool the crew can't
        instantiate ("Tool not found in available tools"). Idempotent (upsert);
        non-fatal so a catalog hiccup never fails the upload.
        """
        if not group_id:
            return
        try:
            from src.services.group_tool_service import GroupToolService
            from src.utils.user_context import GroupContext

            group_context = GroupContext(group_ids=[group_id])
            service = GroupToolService(self.session)
            await service.add_tool_to_group(
                self._KNOWLEDGE_SEARCH_TOOL_ID,
                group_context,
                defaults={"enabled": True},
            )
            # Guarantee the mapping is enabled even if it pre-existed disabled.
            await service.set_group_tool_enabled(
                self._KNOWLEDGE_SEARCH_TOOL_ID, True, group_context
            )
            try:
                await self.session.commit()
            except Exception:
                pass
            logger.info(
                f"Ensured DatabricksKnowledgeSearchTool in tool catalog for group {group_id}"
            )
        except Exception as e:
            logger.warning(
                f"Could not register knowledge search tool for group {group_id}: {e}"
            )

    def _extract_text_content(self, filename: str, content: Any) -> Dict[str, Any]:
        """Extract embeddable text from raw file content (bytes or str).

        PDFs go through pdfminer.six text extraction; everything else is
        decoded as UTF-8 (lossy fallback). Shared by the temp-file upload
        path and the legacy volume read path so both extract identically.

        Returns:
            {"status": "success", "content": str} or {"status": "error", "message": str}
        """
        lower_name = (filename or "").lower()
        if lower_name.endswith(".pdf"):
            logger.info("Detected PDF file, extracting text content")
            try:
                # pdfminer.six (MIT-licensed) — pure-Python PDF text extraction.
                from pdfminer.high_level import extract_text

                raw = content if isinstance(content, bytes) else str(content).encode("utf-8")
                text_content = extract_text(io.BytesIO(raw)) or ""
            except ImportError:
                # Fail loudly instead of embedding a placeholder as "success":
                # otherwise the table gets one junk chunk and the real content
                # is silently dropped.
                logger.error("pdfminer.six not installed — cannot extract PDF text")
                return {
                    "status": "error",
                    "message": (
                        "PDF text extraction requires the 'pdfminer.six' package "
                        "(MIT). Install it (pip install pdfminer.six) and re-upload."
                    ),
                }
            except Exception as pdf_error:
                logger.error(f"Error extracting PDF text: {pdf_error}", exc_info=True)
                return {
                    "status": "error",
                    "message": f"Could not extract text from PDF '{filename}': {pdf_error}",
                }

            if not text_content.strip():
                logger.warning(f"No extractable text found in PDF: {filename}")
                return {
                    "status": "error",
                    "message": (
                        f"No extractable text found in '{filename}'. It may be a "
                        "scanned / image-only PDF that needs OCR."
                    ),
                }

            logger.info(f"Extracted {len(text_content)} chars from PDF")
            return {"status": "success", "content": text_content}

        # For non-PDF files, decode as text
        if isinstance(content, bytes):
            try:
                content = content.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning(f"Failed to decode {filename} as UTF-8")
                content = content.decode("utf-8", errors="ignore")
        return {"status": "success", "content": content}

    async def read_knowledge_file(
        self, file_path: str, group_id: str, user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Read a knowledge file from Databricks Volume using repository pattern.

        Args:
            file_path: Full path to the file (e.g., /Volumes/catalog/schema/volume/path/file.ext)
            group_id: Group ID for tenant isolation
            user_token: Optional user token for OBO authentication

        Returns:
            File content and metadata
        """
        logger.info("=" * 60)
        logger.info("READING KNOWLEDGE FILE VIA REPOSITORY")
        logger.info(f"File path: {file_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"User token provided: {bool(user_token)}")
        logger.info("=" * 60)

        try:
            # Parse the volume path to extract catalog, schema, volume
            # Expected format: /Volumes/catalog/schema/volume/path/to/file.ext
            if not file_path.startswith("/Volumes/"):
                return {
                    "status": "error",
                    "message": f"Invalid volume path format: {file_path}",
                    "path": file_path,
                }

            path_parts = file_path.split("/")
            # path_parts = ['', 'Volumes', 'catalog', 'schema', 'volume', 'path', 'to', 'file.ext']
            if len(path_parts) < 6:
                return {
                    "status": "error",
                    "message": f"Invalid volume path structure: {file_path}",
                    "path": file_path,
                }

            catalog = path_parts[2]
            schema = path_parts[3]
            volume = path_parts[4]
            # Relative path within volume: path/to/file.ext
            relative_path = "/".join(path_parts[5:])

            logger.info(
                f"Parsed path - Catalog: {catalog}, Schema: {schema}, Volume: {volume}"
            )
            logger.info(f"Relative path: {relative_path}")

            # Use volume repository to download the file
            download_result = await self.volume_repository.download_file_from_volume(
                catalog=catalog,
                schema=schema,
                volume_name=volume,
                file_name=relative_path,
            )

            if not download_result.get("success"):
                error_msg = download_result.get("error", "Unknown error")
                logger.error(f"Failed to download file: {error_msg}")
                return {"status": "error", "message": error_msg, "path": file_path}

            # Get the file content
            content = download_result.get("content")
            if content is None:
                return {
                    "status": "error",
                    "message": "No content returned from download",
                    "path": file_path,
                }

            # Extract text (PDF via pdfminer, otherwise UTF-8 decode) — shared
            # with the temp-file upload path.
            filename = file_path.split("/")[-1].lower()
            extraction = self._extract_text_content(filename, content)
            if extraction.get("status") != "success":
                return {
                    "status": "error",
                    "message": extraction.get("message", "Could not extract text"),
                    "path": file_path,
                }
            content = extraction.get("content", "")

            logger.info(f"Successfully read file: {len(content)} characters")

            return {
                "status": "success",
                "path": file_path,
                "content": content,
                "size": len(content),
                "filename": filename,
            }

        except Exception as e:
            logger.error(f"Error reading knowledge file: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "path": file_path}

    async def browse_volume_files(
        self,
        volume_path: str,
        group_id: str,
        execution_id: Optional[str] = None,
        user_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Browse files in a Databricks volume directory using repository pattern.

        Args:
            volume_path: Volume path in format "catalog.schema.volume" or full path "/Volumes/catalog/schema/volume/path"
            group_id: Group ID for tenant isolation
            execution_id: Optional execution ID for scoping
            user_token: Optional user token for OBO authentication

        Returns:
            List of files and directories with metadata
        """
        logger.info("=" * 60)
        logger.info("BROWSING VOLUME FILES VIA REPOSITORY")
        logger.info(f"Volume path: {volume_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info("=" * 60)

        try:
            # Parse volume path
            if volume_path.startswith("/Volumes/"):
                # Full path format: /Volumes/catalog/schema/volume/optional/path
                path_parts = volume_path.split("/")
                if len(path_parts) < 5:
                    return {
                        "success": False,
                        "error": f"Invalid volume path structure: {volume_path}",
                    }
                catalog = path_parts[2]
                schema = path_parts[3]
                volume = path_parts[4]
                # Optional subdirectory path
                subpath = "/".join(path_parts[5:]) if len(path_parts) > 5 else ""
            else:
                # Dot notation format: catalog.schema.volume
                parts = volume_path.split(".")
                if len(parts) != 3:
                    return {
                        "success": False,
                        "error": f"Invalid volume path format: {volume_path}. Expected 'catalog.schema.volume'",
                    }
                catalog, schema, volume = parts
                # Add group_id and execution_id to path if provided
                subpath_parts = [group_id]
                if execution_id:
                    subpath_parts.append(execution_id)
                subpath = "/".join(subpath_parts)

            logger.info(
                f"Parsed - Catalog: {catalog}, Schema: {schema}, Volume: {volume}"
            )
            logger.info(f"Subpath: {subpath}")

            # Use repository to list volume contents
            list_result = await self.volume_repository.list_volume_contents(
                catalog=catalog, schema=schema, volume_name=volume, path=subpath
            )

            if not list_result.get("success"):
                error_msg = list_result.get("error", "Unknown error")
                logger.error(f"Failed to list volume contents: {error_msg}")
                return {"success": False, "error": error_msg}

            # Process the files list
            files = list_result.get("files", [])

            # Get workspace URL for generating Databricks URLs
            try:
                from src.utils.databricks_auth import get_auth_context

                auth = await get_auth_context()
                workspace_url = auth.workspace_url if auth else None
            except Exception:
                workspace_url = None

            # Format the response
            formatted_files = []
            for file_info in files:
                file_entry = {
                    "name": file_info.get("name"),
                    "path": file_info.get("path"),
                    "type": file_info.get("type", "file"),  # file or directory
                    "size": file_info.get("size", 0),
                    "modified_at": file_info.get("modified_at"),
                }

                # Add Databricks URL if we have workspace URL
                if workspace_url and file_entry["path"]:
                    file_entry["databricks_url"] = (
                        f"{workspace_url}/explore/data{file_entry['path']}"
                    )

                formatted_files.append(file_entry)

            logger.info(f"Successfully listed {len(formatted_files)} files/directories")

            return {
                "success": True,
                "files": formatted_files,
                "volume_path": f"{catalog}.{schema}.{volume}",
                "full_path": f"/Volumes/{catalog}/{schema}/{volume}/{subpath}".rstrip(
                    "/"
                ),
                "count": len(formatted_files),
            }

        except Exception as e:
            logger.error(f"Error browsing volume files: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    async def register_volume_file(
        self, execution_id: str, file_path: str, group_id: str
    ) -> Dict[str, Any]:
        """
        Register an existing Databricks Volume file for use as knowledge source.

        Args:
            execution_id: Execution ID for scoping
            file_path: Full path to the file in Databricks Volume
            group_id: Group ID for tenant isolation

        Returns:
            Registration confirmation with file metadata
        """
        try:
            # Get file name from path
            filename = os.path.basename(file_path)

            # For now, simulate successful registration
            logger.info(f"Simulating registration of file {filename} from volume")

            # Return registration metadata
            return {
                "status": "success",
                "path": file_path,
                "filename": filename,
                "execution_id": execution_id,
                "group_id": group_id,
                "registered_at": datetime.now().isoformat(),
                "source": "volume",
                "message": f"File {filename} registered successfully from Databricks Volume",
            }

        except Exception as e:
            logger.error(f"Error registering volume file: {e}")
            raise

    def _get_file_type(self, filename: str) -> str:
        """
        Determine file type from extension.

        Args:
            filename: Name of the file

        Returns:
            File type string
        """
        ext = os.path.splitext(filename)[1].lower()
        type_map = {
            ".pdf": "pdf",
            ".txt": "text",
            ".md": "markdown",
            ".json": "json",
            ".csv": "csv",
            ".doc": "word",
            ".docx": "word",
            ".py": "python",
            ".js": "javascript",
            ".ts": "typescript",
            ".yaml": "yaml",
            ".yml": "yaml",
            ".xml": "xml",
            ".html": "html",
        }
        return type_map.get(ext, "file")

    async def list_knowledge_files(
        self, execution_id: str, group_id: str
    ) -> List[Dict[str, Any]]:
        """
        List all knowledge files for a specific execution.

        Args:
            execution_id: Execution ID to list files for
            group_id: Group ID for tenant isolation

        Returns:
            List of files with metadata
        """
        try:
            # For now, return an empty list since we're simulating uploads
            logger.info(
                f"Listing knowledge files for execution {execution_id}, group {group_id}"
            )

            # In a real implementation, this would list files from Databricks Volume
            # For now, return empty list to avoid hanging
            return []

        except Exception as e:
            logger.error(f"Error listing knowledge files: {e}")
            return []

    async def delete_knowledge_file(
        self,
        execution_id: str,
        group_id: str,
        filename: str,
        user_token: Optional[str] = None,
    ) -> bool:
        """
        Delete a knowledge file from Databricks Volume using repository pattern.

        Args:
            execution_id: Execution ID of the file
            group_id: Group ID for tenant isolation
            filename: Name of the file to delete
            user_token: Optional user token for OBO authentication

        Returns:
            True if deletion was successful
        """
        try:
            # Uploads are no longer persisted anywhere (temp file deleted after
            # embedding), so "deleting a knowledge file" means deleting its
            # EMBEDDINGS from the knowledge store (Lakebase pgvector when
            # active, else the app DB) so the search tool no longer retrieves
            # its chunks.
            from src.models.documentation_embedding import KnowledgeEmbedding
            from src.repositories.documentation_embedding_repository import (
                DocumentationEmbeddingRepository,
            )
            from src.services.knowledge_embedding_session import (
                ensure_lakebase_doc_table,
                knowledge_embedding_session,
            )

            async with knowledge_embedding_session(
                self.session, group_id, user_token
            ) as (store_session, is_lakebase):
                doc_repo = DocumentationEmbeddingRepository(
                    store_session, model=KnowledgeEmbedding
                )
                if is_lakebase:
                    await ensure_lakebase_doc_table(store_session)
                deleted_rows = await doc_repo.delete_by_file(
                    group_id,
                    execution_id,
                    filename,
                    # A user only deletes their OWN uploads (legacy rows with
                    # no uploader stay group-deletable).
                    created_by=self.created_by_email,
                )
                if not is_lakebase:
                    await store_session.commit()
            logger.info(
                f"Removed {deleted_rows} embedding rows for {filename} (execution {execution_id})"
            )
            return True

        except Exception as e:
            logger.error(f"Error deleting knowledge file: {e}", exc_info=True)
            return False

    async def _resolve_filenames_to_paths(
        self, filenames: List[str], user_token: Optional[str] = None
    ) -> Optional[List[str]]:
        """
        Resolve filenames to full volume paths by querying the index.

        Args:
            filenames: List of filenames to resolve
            user_token: Optional user token for OBO authentication

        Returns:
            List of resolved full paths, or original filenames if resolution fails
        """
        try:
            # Get vector storage configuration
            vector_storage = await self.search_service._get_vector_storage(user_token)
            if not vector_storage:
                logger.warning(
                    "[DK SERVICE] Vector storage not configured for resolution"
                )
                return filenames

            document_index = vector_storage.index_name
            endpoint_name = vector_storage.endpoint_name
            index_repo = vector_storage.repository

            # Generate a dummy query embedding for fetching sources, using the
            # shared knowledge embedder so it matches the ingest/search model.
            from src.core.llm_manager import LLMManager
            from src.services.knowledge_embedder import resolve_knowledge_embedder_config

            embedder_config = await resolve_knowledge_embedder_config(
                user_token=user_token, group_id=self.group_id
            )
            dummy_embedding = await LLMManager.get_embedding(
                "dummy", embedder_config=embedder_config
            )
            if not dummy_embedding:
                logger.warning(
                    "[DK SERVICE] Failed to generate embedding for resolution"
                )
                return filenames

            # Get search columns
            from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

            search_columns = DatabricksIndexSchemas.get_search_columns("document")

            # Query index to get all sources for this group
            logger.info(
                f"[DK SERVICE] Querying index to find sources for group {self.group_id}"
            )
            logger.info(
                f"[DK SERVICE] Using filters: {{'group_id': '{self.group_id}'}}"
            )

            try:
                all_sources_results = await asyncio.wait_for(
                    index_repo.similarity_search(
                        index_name=document_index,
                        endpoint_name=endpoint_name,
                        query_vector=dummy_embedding,
                        columns=search_columns,
                        filters={"group_id": self.group_id},
                        num_results=100,
                        user_token=user_token,
                    ),
                    timeout=10,  # Increased timeout
                )
                logger.info(f"[DK SERVICE] Query completed, checking results...")
            except asyncio.TimeoutError:
                logger.error("[DK SERVICE] Query timed out after 10 seconds")
                return filenames
            except Exception as query_error:
                logger.error(f"[DK SERVICE] Query failed with error: {query_error}")
                return filenames

            if not all_sources_results:
                logger.warning("[DK SERVICE] all_sources_results is None or empty")
                return filenames

            # Repository returns {'success': bool, 'results': {...}, 'message': str}
            if not all_sources_results.get("success"):
                logger.warning(
                    f"[DK SERVICE] Query failed: {all_sources_results.get('message')}"
                )
                return filenames

            results = all_sources_results.get("results", {})
            if not results:
                logger.warning("[DK SERVICE] No 'results' in response")
                return filenames

            # The 'results' key contains the actual search response with 'result' -> 'data_array'
            data_array = results.get("result", {}).get("data_array", [])
            logger.info(f"[DK SERVICE] Got data_array with {len(data_array)} items")

            if len(data_array) == 0:
                logger.warning(
                    "[DK SERVICE] data_array is empty - no results from index"
                )
                return filenames

            # Extract unique source paths
            positions = DatabricksIndexSchemas.get_column_positions("document")
            source_position = positions["source"]

            unique_sources = set()
            for result in data_array:
                if len(result) > source_position:
                    source = result[source_position]
                    if source:
                        unique_sources.add(source)

            logger.info(
                f"[DK SERVICE] Found {len(unique_sources)} unique sources in index"
            )

            # Match filenames to full paths
            resolved = []
            for filename in filenames:
                matched = False
                for source_path in unique_sources:
                    source_filename = source_path.split("/")[-1]
                    if source_filename == filename:
                        resolved.append(source_path)
                        logger.info(
                            f"[DK SERVICE] Resolved '{filename}' to '{source_path}'"
                        )
                        matched = True
                        break

                if not matched:
                    logger.warning(
                        f"[DK SERVICE] Could not resolve '{filename}', keeping as-is"
                    )
                    resolved.append(filename)

            return resolved

        except Exception as e:
            logger.error(f"[DK SERVICE] Error resolving filenames: {e}")
            return filenames

    async def search_knowledge(
        self,
        query: str,
        group_id: str,
        execution_id: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
        agent_id: Optional[str] = None,
        limit: int = 5,
        user_token: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for knowledge in the Databricks Vector Index.

        This method delegates to KnowledgeSearchService following clean architecture pattern.

        Args:
            query: The search query
            group_id: Group ID for tenant isolation
            execution_id: Optional execution ID for scoping
            file_paths: Optional list of file paths to filter search
            agent_id: Optional agent ID for access control filtering
            limit: Maximum number of results to return
            user_token: Optional user token for OBO authentication

        Returns:
            List of search results with content and metadata
        """
        # No path resolution needed: uploads use logical paths
        # (uploads/{group}/{execution}/{name}) and the search service
        # soft-filters by basename, so bare filenames and full paths both work.

        # Delegate to search service (proper separation of concerns)
        result = await self.search_service.search(
            query=query,
            execution_id=execution_id,
            file_paths=file_paths,
            agent_id=agent_id,
            limit=limit,
            user_token=user_token,
            # Per-user isolation: prefer the explicit caller identity, falling
            # back to the service's own (API-context) user.
            created_by=created_by or self.created_by_email,
        )

        return result
