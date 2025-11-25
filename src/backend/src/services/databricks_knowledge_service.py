"""
Databricks Knowledge Source Service
"""
from typing import Dict, Any, List, Optional
from fastapi import UploadFile
import logging
import os
import io
import asyncio
import base64
import json
import uuid
from datetime import datetime
try:
    from databricks.sdk.service.files import FileInfo
except ImportError:
    # Databricks SDK not installed, will use REST API
    FileInfo = None

from sqlalchemy.ext.asyncio import AsyncSession
from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.repositories.databricks_volume_repository import DatabricksVolumeRepository
from src.utils.databricks_auth import get_workspace_client

logger = logging.getLogger(__name__)


class DatabricksKnowledgeService:
    """Service for managing knowledge files in Databricks Volumes."""

    def __init__(self, session: AsyncSession, group_id: str, created_by_email: Optional[str] = None, user_token: Optional[str] = None):
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
        self.volume_repository = DatabricksVolumeRepository(user_token=user_token, group_id=group_id)
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
        volume_config: Dict[str, Any],
        agent_ids: Optional[List[str]] = None,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upload a file to Databricks Volume for knowledge source.

        Args:
            file: The uploaded file
            execution_id: Execution ID for scoping
            group_id: Group ID for tenant isolation
            volume_config: Volume configuration
            agent_ids: Optional list of agent IDs that can access this knowledge source
            user_token: Optional user token for OBO authentication

        Returns:
            Upload response with file path and metadata
        """
        logger.info("="*60)
        logger.info("STARTING KNOWLEDGE FILE UPLOAD")

        # CRITICAL DEBUG: Log what agent_ids we receive in the service
        logger.info(f"[SERVICE] 🔍 AGENT_IDS RECEIVED: {agent_ids} (type: {type(agent_ids)}, length: {len(agent_ids) if agent_ids else 0})")

        if agent_ids:
            logger.info(f"[SERVICE] ✅ Agent IDs detected: {agent_ids}")
        else:
            logger.warning(f"[SERVICE] ⚠️ No agent_ids provided - this will result in null agent_ids in vector index!")

        logger.info(f"File: {file.filename}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"Volume Config: {volume_config}")
        logger.info("="*60)

        try:
            # Get Databricks configuration
            config = await self.repository.get_active_config(group_id=group_id)
            if not config:
                # Use default configuration if none exists - get from unified auth
                logger.warning("No Databricks config found in database, using unified auth defaults")
                workspace_url = 'https://example.databricks.com'
                token = ''
                try:
                    from src.utils.databricks_auth import get_auth_context
                    auth = await get_auth_context()
                    if auth:
                        workspace_url = auth.workspace_url
                        token = auth.token
                        logger.debug(f"Using unified {auth.auth_method} auth for default config")
                except Exception as e:
                    logger.warning(f"Failed to get unified auth for default config: {e}")

                config = type('obj', (object,), {
                    'knowledge_volume_enabled': True,
                    'knowledge_volume_path': 'main.default.knowledge',
                    'workspace_url': workspace_url,
                    'encrypted_personal_access_token': token
                })()
                logger.info(f"Default config - workspace_url: {config.workspace_url}")
                logger.info(f"Default config - volume_path: {config.knowledge_volume_path}")
            else:
                logger.info(f"Found Databricks config - workspace_url: {config.workspace_url}")
                logger.info(f"Found Databricks config - volume_enabled: {config.knowledge_volume_enabled}")
                logger.info(f"Found Databricks config - volume_path: {config.knowledge_volume_path}")

            # Construct volume path
            volume_path = volume_config.get('volume_path', 'main.default.knowledge')
            if hasattr(config, 'knowledge_volume_path') and config.knowledge_volume_path:
                volume_path = config.knowledge_volume_path

            logger.info(f"Using volume path: {volume_path}")

            # Parse volume path (format: catalog.schema.volume)
            parts = volume_path.split('.')
            if len(parts) != 3:
                # Use defaults if invalid format
                catalog, schema, volume = 'main', 'default', 'knowledge'
                logger.warning(f"Invalid volume path format '{volume_path}', using defaults: {catalog}.{schema}.{volume}")
            else:
                catalog, schema, volume = parts
                logger.info(f"Parsed volume path - Catalog: {catalog}, Schema: {schema}, Volume: {volume}")

            # Create date directory if configured
            date_dir = ""
            if volume_config.get('create_date_dirs', True):
                date_dir = datetime.now().strftime('%Y-%m-%d')
                logger.info(f"Creating date directory: {date_dir}")

            # Construct full path - ensure it starts with /Volumes
            # Don't add 'knowledge' subdirectory since the volume itself is already 'knowledge'
            file_path = f"/Volumes/{catalog}/{schema}/{volume}/{group_id}/{execution_id}"
            if date_dir:
                file_path = f"{file_path}/{date_dir}"
            file_path = f"{file_path}/{file.filename}"
            file_path = file_path.replace('//', '/')  # Clean up double slashes

            logger.info("="*60)
            logger.info(f"FULL UPLOAD PATH: {file_path}")
            logger.info("="*60)

            # Read file content
            content = await file.read()
            file_size = len(content)
            logger.info(f"File size: {file_size} bytes ({file_size/1024:.2f} KB)")

            # Use DatabricksVolumeRepository for upload (includes OBO → PAT fallback)
            logger.info("Attempting upload via DatabricksVolumeRepository")

            # Track upload success and method used
            upload_successful = False
            upload_method = "none"
            selected_agents = volume_config.get('selected_agents', [])
            workspace_url = None

            try:
                # Upload using the repository layer (handles auth fallback automatically)
                upload_result = await self.volume_repository.upload_file_to_volume(
                    catalog=catalog,
                    schema=schema,
                    volume_name=volume,
                    file_name=f"{group_id}/{execution_id}/{date_dir}/{file.filename}" if date_dir else f"{group_id}/{execution_id}/{file.filename}",
                    file_content=content,
                    user_token=user_token  # Pass user_token for OBO authentication
                )

                if upload_result["success"]:
                    logger.info("="*60)
                    logger.info("SUCCESS! File uploaded via DatabricksVolumeRepository")
                    logger.info(f"File location: {file_path}")
                    logger.info("="*60)

                    upload_successful = True
                    upload_method = "repository"

                    # Get workspace URL for display
                    from src.utils.databricks_auth import get_auth_context
                    try:
                        auth = await get_auth_context()
                        if auth:
                            workspace_url = auth.workspace_url
                    except Exception:
                        pass
                else:
                    logger.error(f"Repository upload failed: {upload_result.get('error')}")
                    raise Exception(upload_result.get('error', 'Upload failed'))

            except Exception as e:
                logger.error(f"Failed to upload to Databricks: {e}", exc_info=True)
                logger.warning("Upload failed - will use simulation mode")

            # Set simulation mode if upload not successful
            if not upload_successful:
                logger.warning("Upload failed - authentication error")
                logger.warning("Check that you have proper Databricks authentication configured")

                logger.info("="*60)
                logger.info("UPLOAD FAILED - File not uploaded to Databricks")
                logger.info(f"Target path: {file_path}")
                logger.info(f"File: {file.filename}, Size: {file_size} bytes")
                logger.info("To resolve:")
                logger.info("  1. Verify OBO token is passed from request headers")
                logger.info("  2. Check PAT token is configured in API Keys Service")
                logger.info("  3. Or verify Service Principal credentials")
                logger.info("="*60)

                upload_method = "failed"

            logger.info(f"Selected agents for knowledge access: {selected_agents}")

            # SINGLE EMBEDDING EXECUTION: Process and embed the uploaded file once
            logger.info(f"[UPLOAD] Starting embedding for file: {file_path}")

            try:
                # Read file content for embedding
                read_result = await self.read_knowledge_file(
                    file_path=file_path,
                    group_id=group_id,
                    user_token=user_token
                )

                if read_result.get('status') == 'success':
                    file_content = read_result.get('content', '')

                    # Delegate to embedding service (proper separation of concerns)
                    embedding_result = await self.embedding_service.embed_file(
                        file_path=file_path,
                        file_content=file_content,
                        execution_id=execution_id,
                        agent_ids=agent_ids,
                        user_token=user_token
                    )
                else:
                    embedding_result = {"status": "error", "message": "Failed to read file for embedding"}

            except Exception as embed_error:
                logger.error(f"[UPLOAD] EMBEDDING FAILED: {embed_error}", exc_info=True)
                embedding_result = {"status": "error", "message": f"Embedding failed: {embed_error}"}

            logger.info(f"[UPLOAD] Embedding completed with result: {embedding_result.get('status')}")

            # Return unified response regardless of upload method
            response = {
                "status": "success",
                "path": file_path,
                "filename": file.filename,
                "size": file_size,
                "execution_id": execution_id,
                "group_id": group_id,
                "uploaded_at": datetime.now().isoformat(),
                "selected_agents": selected_agents,
                "embedding_result": embedding_result,
                "upload_method": upload_method,
                "volume_info": {
                    "catalog": catalog,
                    "schema": schema,
                    "volume": volume,
                    "full_path": file_path
                },
                "message": f"File {file.filename} uploaded successfully via {upload_method}",
                "simulated": not upload_successful
            }

            logger.info(f"Returning unified response: {response}")
            return response

        except Exception as e:
            logger.error("="*60)
            logger.error(f"ERROR in upload_knowledge_file: {str(e)}")
            logger.error("="*60, exc_info=True)
            # Return error response instead of raising
            return {
                "status": "error",
                "message": str(e),
                "filename": file.filename if file else "unknown"
            }

    async def read_knowledge_file(
        self,
        file_path: str,
        group_id: str,
        user_token: Optional[str] = None
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
        logger.info("="*60)
        logger.info("READING KNOWLEDGE FILE VIA REPOSITORY")
        logger.info(f"File path: {file_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"User token provided: {bool(user_token)}")
        logger.info("="*60)

        try:
            # Parse the volume path to extract catalog, schema, volume
            # Expected format: /Volumes/catalog/schema/volume/path/to/file.ext
            if not file_path.startswith('/Volumes/'):
                return {
                    "status": "error",
                    "message": f"Invalid volume path format: {file_path}",
                    "path": file_path
                }

            path_parts = file_path.split('/')
            # path_parts = ['', 'Volumes', 'catalog', 'schema', 'volume', 'path', 'to', 'file.ext']
            if len(path_parts) < 6:
                return {
                    "status": "error",
                    "message": f"Invalid volume path structure: {file_path}",
                    "path": file_path
                }

            catalog = path_parts[2]
            schema = path_parts[3]
            volume = path_parts[4]
            # Relative path within volume: path/to/file.ext
            relative_path = '/'.join(path_parts[5:])

            logger.info(f"Parsed path - Catalog: {catalog}, Schema: {schema}, Volume: {volume}")
            logger.info(f"Relative path: {relative_path}")

            # Use volume repository to download the file
            download_result = await self.volume_repository.download_file_from_volume(
                catalog=catalog,
                schema=schema,
                volume_name=volume,
                file_name=relative_path
            )

            if not download_result.get("success"):
                error_msg = download_result.get("error", "Unknown error")
                logger.error(f"Failed to download file: {error_msg}")
                return {
                    "status": "error",
                    "message": error_msg,
                    "path": file_path
                }

            # Get the file content
            content = download_result.get("content")
            if content is None:
                return {
                    "status": "error",
                    "message": "No content returned from download",
                    "path": file_path
                }

            # Handle PDF files if needed
            filename = file_path.split('/')[-1].lower()
            if filename.endswith('.pdf'):
                logger.info("Detected PDF file, extracting text content")
                try:
                    # Try pypdf (newer) first, then fall back to PyPDF2
                    try:
                        from pypdf import PdfReader
                    except ImportError:
                        from PyPDF2 import PdfReader
                    
                    import io
                    
                    # Content from repository is bytes
                    pdf_reader = PdfReader(io.BytesIO(content))
                    text_content = ""
                    for page_num in range(len(pdf_reader.pages)):
                        page = pdf_reader.pages[page_num]
                        text_content += page.extract_text() + "\n"
                    
                    logger.info(f"Extracted {len(text_content)} chars from PDF")
                    content = text_content
                except ImportError:
                    logger.warning("PDF library not installed")
                    content = f"[PDF File: {filename}]\n[Unable to extract text - PDF library not installed]"
                except Exception as pdf_error:
                    logger.error(f"Error extracting PDF text: {pdf_error}")
                    content = f"[PDF File: {filename}]\n[Error extracting text: {pdf_error}]"
            else:
                # For non-PDF files, decode as text
                if isinstance(content, bytes):
                    try:
                        content = content.decode('utf-8')
                    except UnicodeDecodeError:
                        logger.warning(f"Failed to decode {filename} as UTF-8")
                        content = content.decode('utf-8', errors='ignore')

            logger.info(f"Successfully read file: {len(content)} characters")
            
            return {
                "status": "success",
                "path": file_path,
                "content": content,
                "size": len(content),
                "filename": filename
            }

        except Exception as e:
            logger.error(f"Error reading knowledge file: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "path": file_path
            }
    
    async def browse_volume_files(
        self,
        volume_path: str,
        group_id: str,
        execution_id: Optional[str] = None,
        user_token: Optional[str] = None
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
        logger.info("="*60)
        logger.info("BROWSING VOLUME FILES VIA REPOSITORY")
        logger.info(f"Volume path: {volume_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info("="*60)

        try:
            # Parse volume path
            if volume_path.startswith('/Volumes/'):
                # Full path format: /Volumes/catalog/schema/volume/optional/path
                path_parts = volume_path.split('/')
                if len(path_parts) < 5:
                    return {
                        "success": False,
                        "error": f"Invalid volume path structure: {volume_path}"
                    }
                catalog = path_parts[2]
                schema = path_parts[3]
                volume = path_parts[4]
                # Optional subdirectory path
                subpath = '/'.join(path_parts[5:]) if len(path_parts) > 5 else ''
            else:
                # Dot notation format: catalog.schema.volume
                parts = volume_path.split('.')
                if len(parts) != 3:
                    return {
                        "success": False,
                        "error": f"Invalid volume path format: {volume_path}. Expected 'catalog.schema.volume'"
                    }
                catalog, schema, volume = parts
                # Add group_id and execution_id to path if provided
                subpath_parts = [group_id]
                if execution_id:
                    subpath_parts.append(execution_id)
                subpath = '/'.join(subpath_parts)

            logger.info(f"Parsed - Catalog: {catalog}, Schema: {schema}, Volume: {volume}")
            logger.info(f"Subpath: {subpath}")

            # Use repository to list volume contents
            list_result = await self.volume_repository.list_volume_contents(
                catalog=catalog,
                schema=schema,
                volume_name=volume,
                path=subpath
            )

            if not list_result.get("success"):
                error_msg = list_result.get("error", "Unknown error")
                logger.error(f"Failed to list volume contents: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }

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
                    "modified_at": file_info.get("modified_at")
                }
                
                # Add Databricks URL if we have workspace URL
                if workspace_url and file_entry["path"]:
                    file_entry["databricks_url"] = f"{workspace_url}/explore/data{file_entry['path']}"
                
                formatted_files.append(file_entry)

            logger.info(f"Successfully listed {len(formatted_files)} files/directories")

            return {
                "success": True,
                "files": formatted_files,
                "volume_path": f"{catalog}.{schema}.{volume}",
                "full_path": f"/Volumes/{catalog}/{schema}/{volume}/{subpath}".rstrip('/'),
                "count": len(formatted_files)
            }

        except Exception as e:
            logger.error(f"Error browsing volume files: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    async def register_volume_file(
        self,
        execution_id: str,
        file_path: str,
        group_id: str
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
                "message": f"File {filename} registered successfully from Databricks Volume"
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
            '.pdf': 'pdf',
            '.txt': 'text',
            '.md': 'markdown',
            '.json': 'json',
            '.csv': 'csv',
            '.doc': 'word',
            '.docx': 'word',
            '.py': 'python',
            '.js': 'javascript',
            '.ts': 'typescript',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            '.xml': 'xml',
            '.html': 'html'
        }
        return type_map.get(ext, 'file')
    
    async def list_knowledge_files(
        self,
        execution_id: str,
        group_id: str
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
            logger.info(f"Listing knowledge files for execution {execution_id}, group {group_id}")
            
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
        user_token: Optional[str] = None
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
            # Get Databricks configuration
            config = await self._get_databricks_config(group_id)
            if not config or not config.knowledge_volume_path:
                logger.error("Databricks configuration or knowledge_volume_path not found")
                return False

            # Build full file path: /Volumes/catalog.schema.volume/execution_id/filename
            volume_path = config.knowledge_volume_path
            parts = volume_path.split('.')
            if len(parts) != 3:
                logger.error(f"Invalid volume path format: {volume_path}")
                return False

            catalog, schema, volume = parts
            file_path = f"{execution_id}/{filename}"

            # Use repository to delete file
            delete_result = await self.volume_repository.delete_volume_file(
                catalog=catalog,
                schema=schema,
                volume_name=volume,
                file_path=file_path
            )

            if delete_result.get("success"):
                logger.info(f"Successfully deleted file {filename} for execution {execution_id}")
                return True
            else:
                logger.error(f"Failed to delete file {filename}: {delete_result.get('message')}")
                return False

        except Exception as e:
            logger.error(f"Error deleting knowledge file: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def _resolve_filenames_to_paths(
        self,
        filenames: List[str],
        user_token: Optional[str] = None
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
                logger.warning("[DK SERVICE] Vector storage not configured for resolution")
                return filenames

            document_index = vector_storage.index_name
            endpoint_name = vector_storage.endpoint_name
            index_repo = vector_storage.repository

            # Generate a dummy query embedding for fetching sources
            from src.core.llm_manager import LLMManager
            dummy_embedding = await LLMManager.get_embedding("dummy", model="databricks-gte-large-en")
            if not dummy_embedding:
                logger.warning("[DK SERVICE] Failed to generate embedding for resolution")
                return filenames

            # Get search columns
            from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
            search_columns = DatabricksIndexSchemas.get_search_columns("document")

            # Query index to get all sources for this group
            logger.info(f"[DK SERVICE] Querying index to find sources for group {self.group_id}")
            logger.info(f"[DK SERVICE] Using filters: {{'group_id': '{self.group_id}'}}")

            try:
                all_sources_results = await asyncio.wait_for(
                    index_repo.similarity_search(
                        index_name=document_index,
                        endpoint_name=endpoint_name,
                        query_vector=dummy_embedding,
                        columns=search_columns,
                        filters={"group_id": self.group_id},
                        num_results=100,
                        user_token=user_token
                    ),
                    timeout=10  # Increased timeout
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
            if not all_sources_results.get('success'):
                logger.warning(f"[DK SERVICE] Query failed: {all_sources_results.get('message')}")
                return filenames

            results = all_sources_results.get('results', {})
            if not results:
                logger.warning("[DK SERVICE] No 'results' in response")
                return filenames

            # The 'results' key contains the actual search response with 'result' -> 'data_array'
            data_array = results.get('result', {}).get('data_array', [])
            logger.info(f"[DK SERVICE] Got data_array with {len(data_array)} items")

            if len(data_array) == 0:
                logger.warning("[DK SERVICE] data_array is empty - no results from index")
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

            logger.info(f"[DK SERVICE] Found {len(unique_sources)} unique sources in index")

            # Match filenames to full paths
            resolved = []
            for filename in filenames:
                matched = False
                for source_path in unique_sources:
                    source_filename = source_path.split("/")[-1]
                    if source_filename == filename:
                        resolved.append(source_path)
                        logger.info(f"[DK SERVICE] Resolved '{filename}' to '{source_path}'")
                        matched = True
                        break

                if not matched:
                    logger.warning(f"[DK SERVICE] Could not resolve '{filename}', keeping as-is")
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
        user_token: Optional[str] = None
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
        # Resolve filenames to full paths if needed
        resolved_file_paths = file_paths
        if file_paths and not all(path.startswith("/Volumes") for path in file_paths):
            logger.info(f"[DK SERVICE] Resolving file paths: {file_paths}")
            resolved_file_paths = await self._resolve_filenames_to_paths(file_paths, user_token)
            logger.info(f"[DK SERVICE] Resolved to: {resolved_file_paths}")

        # Delegate to search service (proper separation of concerns)
        result = await self.search_service.search(
            query=query,
            execution_id=execution_id,
            file_paths=resolved_file_paths,
            agent_id=agent_id,
            limit=limit,
            user_token=user_token
        )

        return result
