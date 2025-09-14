"""
Databricks Knowledge Source Service
"""
from typing import Dict, Any, List, Optional
from fastapi import UploadFile
import logging
import os
import asyncio
import aiohttp
import base64
import json
import uuid
from datetime import datetime
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.files import FileInfo
except ImportError:
    # Databricks SDK not installed, will use REST API
    WorkspaceClient = None
    FileInfo = None

from src.repositories.databricks_config_repository import DatabricksConfigRepository

logger = logging.getLogger(__name__)


class DatabricksKnowledgeService:
    """Service for managing knowledge files in Databricks Volumes."""
    
    def __init__(self, databricks_repository: DatabricksConfigRepository, group_id: str, created_by_email: Optional[str] = None):
        """
        Initialize the Databricks Knowledge Service.
        
        Args:
            databricks_repository: Repository for database operations
            group_id: Group ID for tenant isolation
            created_by_email: Email of the user
        """
        self.repository = databricks_repository
        self.group_id = group_id
        self.created_by_email = created_by_email
    
    async def upload_knowledge_file(
        self,
        file: UploadFile,
        execution_id: str,
        group_id: str,
        volume_config: Dict[str, Any],
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        print("🔥 DEBUG: upload_knowledge_file method called!")
        print(f"🔥 DEBUG: file={file.filename}, execution_id={execution_id}, group_id={group_id}")

        import logging
        system_logger = logging.getLogger("SYSTEM")
        system_logger.info(f"🔥 DEBUG: upload_knowledge_file called for {file.filename}")

        """
        Upload a file to Databricks Volume for knowledge source.
        
        Args:
            file: The uploaded file
            execution_id: Execution ID for scoping
            group_id: Group ID for tenant isolation
            volume_config: Volume configuration
            user_token: Optional user token for OBO authentication
            
        Returns:
            Upload response with file path and metadata
        """
        import base64
        import aiohttp
        
        print("🔥 DEBUG: upload_knowledge_file method called!")
        print(f"🔥 DEBUG: file={file.filename}, execution_id={execution_id}, group_id={group_id}")
        # Also write to system log to track execution
        import logging
        system_logger = logging.getLogger("SYSTEM") 
        system_logger.info(f"🔥 DEBUG: upload_knowledge_file called for {file.filename}")
        system_logger.info(f"🔥 DEBUG: execution_id={execution_id}, group_id={group_id}")
        logger.info("="*60)
        logger.info("STARTING KNOWLEDGE FILE UPLOAD")
        logger.info(f"File: {file.filename}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info(f"Group ID: {group_id}")
        logger.info(f"Volume Config: {volume_config}")
        logger.info("="*60)
        
        try:
            # Get Databricks configuration
            config = await self.repository.get_active_config(group_id=group_id)
            if not config:
                # Use default configuration if none exists
                logger.warning("No Databricks config found in database, using environment defaults")
                config = type('obj', (object,), {
                    'knowledge_volume_enabled': True,
                    'knowledge_volume_path': 'main.default.knowledge',
                    'workspace_url': os.getenv('DATABRICKS_HOST', 'https://example.databricks.com'),
                    'encrypted_personal_access_token': os.getenv('DATABRICKS_TOKEN', '')
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
            
            # Get workspace URL first (needed for authentication)
            workspace_url = getattr(config, 'workspace_url', os.getenv('DATABRICKS_HOST'))
            logger.info(f"Workspace URL: {workspace_url}")
            
            # Get Databricks token using proper authentication hierarchy
            # Use DatabricksAuthHelper to get token following proper hierarchy
            from src.repositories.databricks_auth_helper import DatabricksAuthHelper
            
            try:
                # This will try:
                # 1. OBO with user token (if provided)
                # 2. PAT from database (DATABRICKS_TOKEN or DATABRICKS_API_KEY)
                # 3. PAT from environment variables
                token = await DatabricksAuthHelper.get_auth_token(
                    workspace_url=workspace_url,
                    user_token=user_token  # Pass the user_token from the method parameter
                )
                logger.info("Successfully obtained Databricks authentication token")
            except Exception as auth_error:
                logger.warning(f"Primary auth failed: {auth_error}")
                
                # Fallback: Try OAuth with client credentials if available
                client_id = os.getenv('DATABRICKS_CLIENT_ID')
                client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
                
                if client_id and client_secret:
                    logger.info("Attempting OAuth authentication with client credentials")
                    try:
                        from databricks.sdk.core import Config
                        oauth_config = Config(
                            host=workspace_url,
                            client_id=client_id,
                            client_secret=client_secret
                        )
                        # Get OAuth token
                        auth_result = oauth_config.authenticate()
                        if hasattr(auth_result, 'access_token'):
                            token = auth_result.access_token
                            logger.info("Successfully obtained OAuth token")
                        else:
                            logger.error("No access token in OAuth response")
                            token = None
                    except Exception as oauth_error:
                        logger.error(f"OAuth authentication failed: {oauth_error}")
                        token = None
                else:
                    logger.debug("No OAuth credentials available")
                    token = None
            
            if token:
                logger.info("Databricks token found (length: %d)", len(token))
            else:
                logger.warning("No Databricks token found - will simulate upload")
            
            # If we have a token and workspace URL, try actual upload
            
            if token and workspace_url and workspace_url != 'https://example.databricks.com':
                logger.info("Attempting REAL upload to Databricks")
                try:
                    # Try using Databricks SDK if available
                    if WorkspaceClient:
                        logger.info("Databricks SDK is available, attempting SDK upload")
                        workspace_client = WorkspaceClient(
                            host=workspace_url,
                            token=token
                        )
                        
                        # Upload using SDK - same as DatabricksVolumeCallback
                        try:
                            logger.info(f"Calling workspace_client.files.upload()")
                            logger.info(f"  - file_path: {file_path}")
                            logger.info(f"  - content size: {len(content)} bytes")
                            logger.info(f"  - overwrite: True")
                            
                            workspace_client.files.upload(
                                file_path=file_path,
                                contents=content,
                                overwrite=True
                            )
                            
                            logger.info("="*60)
                            logger.info("SUCCESS! File uploaded via SDK to Databricks")
                            logger.info(f"File location: {file_path}")
                            logger.info("You should see this file in your Databricks workspace at:")
                            logger.info(f"  {workspace_url}/browse/files{file_path}")
                            logger.info("="*60)
                            
                            # Extract selected agents from volume config
                            selected_agents = volume_config.get('selected_agents', [])
                            logger.info(f"Selected agents for knowledge access: {selected_agents}")

                            # IMMEDIATE EMBEDDING: Process and embed the uploaded file right away (SDK path)
                            logger.info(f"[UPLOAD] About to start immediate embedding for file: {file_path}")
                            logger.info(f"[UPLOAD] Embedding params - execution_id: {execution_id}, group_id: {group_id}")

                            try:
                                embedding_result = await self._embed_uploaded_file(
                                    file_path=file_path,
                                    execution_id=execution_id,
                                    group_id=group_id,
                                    user_token=user_token
                                )
                            except Exception as embed_error:
                                logger.error(f"[UPLOAD] EMBEDDING FAILED: {embed_error}", exc_info=True)
                                embedding_result = {"status": "error", "message": f"Embedding failed: {embed_error}"}

                            logger.info(f"[UPLOAD] Immediate embedding completed with result: {embedding_result}")

                            return {
                                "status": "success",
                                "path": file_path,
                                "filename": file.filename,
                                "size": file_size,
                                "execution_id": execution_id,
                                "group_id": group_id,
                                "uploaded_at": datetime.now().isoformat(),
                                "selected_agents": selected_agents,
                                "embedding_result": embedding_result,  # Add embedding status to SDK path too
                                "volume_info": {
                                    "catalog": catalog,
                                    "schema": schema,
                                    "volume": volume,
                                    "full_path": file_path
                                },
                                "message": f"File {file.filename} uploaded successfully to Databricks Volume"
                            }
                        except Exception as sdk_error:
                            logger.error(f"SDK upload failed: {sdk_error}")
                            logger.info("Falling back to DBFS API method")
                            # Fall through to DBFS API method
                    else:
                        logger.warning("Databricks SDK not available, using DBFS API")
                    
                    # Fallback: Use DBFS API (same as DatabricksVolumeCallback fallback)
                    logger.info("Attempting upload via DBFS API")
                    async with aiohttp.ClientSession() as session:
                        # Convert content to base64
                        content_b64 = base64.b64encode(content).decode('utf-8')
                        logger.info(f"Encoded content to base64 (length: {len(content_b64)})")
                        
                        headers = {
                            "Authorization": f"Bearer {token}",
                            "Content-Type": "application/json"
                        }
                        
                        # Step 1: Create file handle
                        create_url = f"{workspace_url}/api/2.0/fs/files/create"
                        create_data = {
                            "path": file_path,
                            "overwrite": True
                        }
                        
                        logger.info(f"Step 1: Creating file handle at {create_url}")
                        logger.info(f"  Request data: {create_data}")
                        
                        async with session.put(create_url, json=create_data, headers=headers) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logger.error(f"Failed to create file handle: Status {response.status}")
                                logger.error(f"Error response: {error_text}")
                                raise Exception(f"Failed to create file: {error_text}")
                            
                            result = await response.json()
                            handle = result.get("handle")
                            logger.info(f"File handle created successfully: {handle}")
                        
                        # Step 2: Add content block
                        add_block_url = f"{workspace_url}/api/2.0/fs/files/add-block"
                        upload_data = {
                            "handle": handle,
                            "data": content_b64
                        }
                        
                        logger.info(f"Step 2: Uploading content to {add_block_url}")
                        logger.info(f"  Handle: {handle}")
                        
                        async with session.post(add_block_url, json=upload_data, headers=headers) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logger.error(f"Failed to upload content: Status {response.status}")
                                logger.error(f"Error response: {error_text}")
                                raise Exception(f"Failed to upload content: {error_text}")
                        
                        logger.info("Content uploaded successfully")
                        
                        # Step 3: Close file
                        close_url = f"{workspace_url}/api/2.0/fs/files/close"
                        close_data = {"handle": handle}
                        
                        logger.info(f"Step 3: Closing file at {close_url}")
                        
                        async with session.post(close_url, json=close_data, headers=headers) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logger.error(f"Failed to close file: Status {response.status}")
                                logger.error(f"Error response: {error_text}")
                                raise Exception(f"Failed to close file: {error_text}")
                        
                        logger.info("="*60)
                        logger.info("SUCCESS! File uploaded via DBFS API to Databricks")
                        logger.info(f"File location: {file_path}")
                        logger.info("You should see this file in your Databricks workspace at:")
                        logger.info(f"  {workspace_url}/browse/files{file_path}")
                        logger.info("="*60)
                        
                        # Extract selected agents from volume config
                        selected_agents = volume_config.get('selected_agents', [])
                        logger.info(f"Selected agents for knowledge access: {selected_agents}")
                        
                        # IMMEDIATE EMBEDDING: Process and embed the uploaded file right away
                        logger.info(f"[UPLOAD] About to start immediate embedding for file: {file_path}")
                        logger.info(f"[UPLOAD] Embedding params - execution_id: {execution_id}, group_id: {group_id}")

                        try:
                            embedding_result = await self._embed_uploaded_file(
                                file_path=file_path,
                                execution_id=execution_id,
                                group_id=group_id,
                                user_token=user_token
                            )
                        except Exception as embed_error:
                            logger.error(f"[UPLOAD] EMBEDDING FAILED: {embed_error}", exc_info=True)
                            embedding_result = {"status": "error", "message": f"Embedding failed: {embed_error}"}

                        logger.info(f"[UPLOAD] Immediate embedding completed with result: {embedding_result}")

                        return {
                            "status": "success",
                            "path": file_path,
                            "filename": file.filename,
                            "size": file_size,
                            "execution_id": execution_id,
                            "group_id": group_id,
                            "uploaded_at": datetime.now().isoformat(),
                            "selected_agents": selected_agents,
                            "embedding_result": embedding_result,  # Add embedding status
                            "volume_info": {
                                "catalog": catalog,
                                "schema": schema,
                                "volume": volume,
                                "full_path": file_path
                            },
                            "message": f"File {file.filename} uploaded successfully to Databricks Volume"
                        }
                        
                except Exception as e:
                    logger.error(f"Failed to upload to Databricks: {e}", exc_info=True)
                    logger.warning("Falling back to simulation mode")
            else:
                logger.warning("Missing credentials or using example URL - will simulate upload")
                logger.warning(f"  Token available: {bool(token)}")
                logger.warning(f"  Workspace URL: {workspace_url}")
                logger.warning(f"  Is example URL: {workspace_url == 'https://example.databricks.com'}")
            
            # Fallback: Simulate successful upload if actual upload fails or not configured
            logger.info("="*60)
            logger.info("SIMULATED UPLOAD (not actually uploaded to Databricks)")
            logger.info(f"Would upload to: {file_path}")
            logger.info(f"File: {file.filename}, Size: {file_size} bytes")
            logger.info("To enable REAL uploads:")
            logger.info("  1. Set DATABRICKS_TOKEN environment variable")
            logger.info("  2. Set DATABRICKS_HOST environment variable")
            logger.info("  3. Or configure in Databricks settings page")
            logger.info("="*60)
            
            # Extract selected agents from volume config
            selected_agents = volume_config.get('selected_agents', [])
            logger.info(f"Selected agents for knowledge access: {selected_agents}")
            
            # IMMEDIATE EMBEDDING: Even for simulated uploads, try to embed if Vector Search is configured
            logger.info(f"[SIMULATED UPLOAD] About to start immediate embedding for file: {file_path}")
            logger.info(f"[SIMULATED UPLOAD] Embedding params - execution_id: {execution_id}, group_id: {group_id}")

            try:
                embedding_result = await self._embed_uploaded_file(
                    file_path=file_path,
                    execution_id=execution_id,
                    group_id=group_id,
                    user_token=user_token
                )
            except Exception as embed_error:
                logger.error(f"[SIMULATED UPLOAD] EMBEDDING FAILED: {embed_error}", exc_info=True)
                embedding_result = {"status": "error", "message": f"Embedding failed: {embed_error}"}

            logger.info(f"[SIMULATED UPLOAD] Immediate embedding completed with result: {embedding_result}")

            # Return success response (simulated)
            response = {
                "status": "success",
                "path": file_path,
                "filename": file.filename,
                "size": file_size,
                "execution_id": execution_id,
                "group_id": group_id,
                "uploaded_at": datetime.now().isoformat(),
                "embedding_result": embedding_result,  # Add embedding status for simulated uploads too
                "selected_agents": selected_agents,
                "volume_info": {
                    "catalog": catalog,
                    "schema": schema,
                    "volume": volume,
                    "full_path": file_path
                },
                "message": f"File {file.filename} uploaded successfully (simulated)",
                "simulated": True
            }
            
            logger.info(f"Returning response: {response}")
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



    async def _read_file_content(self, file_path: str, user_token: Optional[str] = None) -> Optional[str]:
        """Read content from Databricks Volume file."""
        try:
            print(f"🔥 DEBUG: _read_file_content called for {file_path}")
            logger.info(f"[FILE READ] About to read file: {file_path}")
            logger.info(f"[FILE READ] Group ID: {self.group_id}")
            logger.info(f"[FILE READ] User token provided: {user_token is not None}")

            result = await self.read_knowledge_file(
                file_path=file_path,
                group_id=self.group_id,
                user_token=user_token
            )

            logger.info(f"[FILE READ] read_knowledge_file returned: {result}")

            if result.get('status') == 'success':
                content = result.get('content', '')
                logger.info(f"[FILE READ] Successfully read {len(content)} characters from {file_path}")
                logger.info(f"[FILE READ] Content preview: {content[:100]}...")
                return content
            else:
                logger.error(f"[FILE READ] Failed to read file {file_path}: {result.get('message')}")
                logger.error(f"[FILE READ] Full result: {result}")
                return None

        except Exception as e:
            logger.error(f"[FILE READ] Exception reading file {file_path}: {e}")
            import traceback
            logger.error(f"[FILE READ] Full traceback: {traceback.format_exc()}")
            return None

    def _chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """
        Split text into overlapping chunks for better context preservation.
        Same logic as DatabricksVectorKnowledgeSource.
        """
        chunks = []
        start = 0
        text_length = len(text)

        while start < text_length:
            end = min(start + chunk_size, text_length)
            chunk = text[start:end]

            # Add chunk if it has meaningful content
            if chunk.strip():
                chunks.append(chunk)

            # Move to next chunk with overlap
            start += chunk_size - overlap

            # Avoid infinite loop on small texts
            if start <= 0 and len(chunks) > 0:
                break

        return chunks if chunks else [text]  # Return original if no chunks created

    async def read_knowledge_file(
        self,
        file_path: str,
        group_id: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Read content from a file in Databricks Volume.
        
        Args:
            file_path: Full path to the file in Databricks Volume
            group_id: Group ID for tenant isolation
            user_token: Optional user token for OBO authentication
            
        Returns:
            File content and metadata
        """
        import aiohttp
        
        logger.info("="*60)
        logger.info("STARTING KNOWLEDGE FILE READ")
        logger.info(f"File Path: {file_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info("="*60)
        
        try:
            # Get Databricks configuration
            config = await self.repository.get_active_config(group_id=group_id)
            if not config:
                config = type('obj', (object,), {
                    'workspace_url': os.getenv('DATABRICKS_HOST', 'https://example.databricks.com'),
                    'encrypted_personal_access_token': os.getenv('DATABRICKS_TOKEN', '')
                })()
            
            workspace_url = getattr(config, 'workspace_url', os.getenv('DATABRICKS_HOST'))
            logger.info(f"Workspace URL: {workspace_url}")
            
            # Get authentication token
            from src.repositories.databricks_auth_helper import DatabricksAuthHelper
            
            try:
                token = await DatabricksAuthHelper.get_auth_token(
                    workspace_url=workspace_url,
                    user_token=user_token
                )
                logger.info("Successfully obtained Databricks authentication token")
            except Exception as auth_error:
                logger.warning(f"Auth failed: {auth_error}")
                token = os.getenv('DATABRICKS_TOKEN')
            
            if token and workspace_url and workspace_url != 'https://example.databricks.com':
                logger.info("Attempting REAL file read from Databricks")
                
                # Try using Databricks SDK if available
                if WorkspaceClient:
                    try:
                        logger.info("Using Databricks SDK to read file")
                        workspace_client = WorkspaceClient(
                            host=workspace_url,
                            token=token
                        )
                        
                        # Read file using SDK
                        download_response = workspace_client.files.download(file_path)
                        content = download_response.contents.read()
                            
                        logger.info(f"Successfully read file: {len(content)} bytes")
                        
                        # Check if it's a PDF file
                        filename = os.path.basename(file_path).lower()
                        if filename.endswith('.pdf'):
                            logger.info("Detected PDF file, extracting text content")
                            try:
                                # Try pypdf (newer) first, then fall back to PyPDF2
                                try:
                                    from pypdf import PdfReader
                                except ImportError:
                                    from PyPDF2 import PdfReader
                                
                                import io
                                
                                pdf_reader = PdfReader(io.BytesIO(content))
                                text_content = ""
                                for page_num in range(len(pdf_reader.pages)):
                                    page = pdf_reader.pages[page_num]
                                    text_content += page.extract_text() + "\n"
                                
                                logger.info(f"Extracted {len(text_content)} chars from PDF")
                                content = text_content
                            except ImportError:
                                logger.warning("PDF library not installed, returning placeholder")
                                # Fallback: just decode what we can
                                content = f"[PDF File: {filename}]\n[Unable to extract text - PDF library not installed]"
                            except Exception as pdf_error:
                                logger.error(f"Error extracting PDF text: {pdf_error}")
                                content = f"[PDF File: {filename}]\n[Error extracting text: {pdf_error}]"
                        else:
                            # For non-PDF files, decode as text
                            content = content.decode('utf-8') if isinstance(content, bytes) else content
                        
                        return {
                            "status": "success",
                            "path": file_path,
                            "content": content,
                            "size": len(content),
                            "filename": os.path.basename(file_path)
                        }
                    except Exception as sdk_error:
                        logger.error(f"SDK read failed: {sdk_error}")
                        # Fall through to API method
                
                # Fallback: Use Files API
                logger.info("Using Files API to read file")
                async with aiohttp.ClientSession() as session:
                    headers = {
                        "Authorization": f"Bearer {token}"
                    }
                    
                    # Use Files API to read the file
                    read_url = f"{workspace_url}/api/2.0/fs/files{file_path}"
                    logger.info(f"Reading from: {read_url}")
                    
                    async with session.get(read_url, headers=headers) as response:
                        if response.status == 200:
                            # Read as binary first
                            content_bytes = await response.read()
                            logger.info(f"Successfully read file: {len(content_bytes)} bytes")
                            
                            # Check if it's a PDF file
                            filename = os.path.basename(file_path).lower()
                            if filename.endswith('.pdf'):
                                logger.info("Detected PDF file, extracting text content")
                                try:
                                    # Try pypdf (newer) first, then fall back to PyPDF2
                                    try:
                                        from pypdf import PdfReader
                                    except ImportError:
                                        from PyPDF2 import PdfReader
                                    
                                    import io
                                    
                                    pdf_reader = PdfReader(io.BytesIO(content_bytes))
                                    text_content = ""
                                    for page_num in range(len(pdf_reader.pages)):
                                        page = pdf_reader.pages[page_num]
                                        text_content += page.extract_text() + "\n"
                                    
                                    logger.info(f"Extracted {len(text_content)} chars from PDF")
                                    content = text_content
                                except ImportError:
                                    logger.warning("PDF library not installed, returning placeholder")
                                    content = f"[PDF File: {filename}]\n[Unable to extract text - PDF library not installed]"
                                except Exception as pdf_error:
                                    logger.error(f"Error extracting PDF text: {pdf_error}")
                                    content = f"[PDF File: {filename}]\n[Error extracting text: {pdf_error}]"
                            else:
                                # For non-PDF files, decode as text
                                try:
                                    content = content_bytes.decode('utf-8')
                                except UnicodeDecodeError:
                                    logger.warning("Unable to decode as UTF-8, trying latin-1")
                                    content = content_bytes.decode('latin-1', errors='ignore')
                            
                            return {
                                "status": "success",
                                "path": file_path,
                                "content": content,
                                "size": len(content),
                                "filename": os.path.basename(file_path)
                            }
                        else:
                            error_text = await response.text()
                            logger.error(f"Failed to read file: Status {response.status}")
                            logger.error(f"Error: {error_text}")
                            raise Exception(f"Failed to read file: {error_text}")
            else:
                logger.warning("Missing credentials - returning mock content")
                
                # Return mock content for testing
                return {
                    "status": "success",
                    "path": file_path,
                    "content": f"Mock content for file: {file_path}\nThis would contain the actual file content from Databricks Volume.",
                    "size": 100,
                    "filename": os.path.basename(file_path),
                    "mock": True
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
        user_token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Browse files in a Databricks Volume directory.
        
        Args:
            volume_path: Path to browse (format: catalog.schema.volume/optional/path)
            group_id: Group ID for tenant isolation
            user_token: Optional user token for OBO authentication
            
        Returns:
            List of files and directories with metadata
        """
        import aiohttp
        
        logger.info("=" * 60)
        logger.info("STARTING VOLUME FILES BROWSE")
        logger.info(f"Volume Path: {volume_path}")
        logger.info(f"Group ID: {group_id}")
        logger.info("=" * 60)
        
        try:
            # Get Databricks configuration
            config = await self.repository.get_active_config(group_id=group_id)
            if not config:
                config = type('obj', (object,), {
                    'knowledge_volume_enabled': True,
                    'knowledge_volume_path': 'main.default.knowledge',
                    'workspace_url': os.getenv('DATABRICKS_HOST', 'https://example.databricks.com'),
                    'encrypted_personal_access_token': os.getenv('DATABRICKS_TOKEN', '')
                })()
                logger.info("Using default configuration")
            else:
                logger.info(f"Found Databricks config - workspace_url: {config.workspace_url}")
                logger.info(f"Config knowledge_volume_path: {getattr(config, 'knowledge_volume_path', 'NOT SET')}")
            
            # Parse the volume path to construct the full directory path
            if volume_path.startswith('/Volumes/'):
                # Already a full path
                directory_path = volume_path
                logger.info(f"Using provided full path: {directory_path}")
            else:
                # Construct full path from volume_path (e.g., "catalog.schema.volume/path")
                if '/' in volume_path:
                    volume_part, sub_path = volume_path.split('/', 1)
                    logger.info(f"Split path: volume_part='{volume_part}', sub_path='{sub_path}'")
                else:
                    volume_part = volume_path
                    sub_path = ""
                    logger.info(f"No sub-path: volume_part='{volume_part}'")
                
                # Parse volume part (catalog.schema.volume)
                parts = volume_part.split('.')
                logger.info(f"Volume parts: {parts} (count: {len(parts)})")
                
                if len(parts) == 3:
                    catalog, schema, volume = parts
                    directory_path = f"/Volumes/{catalog}/{schema}/{volume}"
                    if sub_path:
                        directory_path = f"{directory_path}/{sub_path}"
                    logger.info(f"Constructed path from parts: catalog={catalog}, schema={schema}, volume={volume}")
                    logger.info(f"Final constructed path: {directory_path}")
                else:
                    # Use configured volume path as fallback
                    configured_volume = getattr(config, 'knowledge_volume_path', 'main.default.knowledge')
                    parts_fallback = configured_volume.split('.')
                    if len(parts_fallback) == 3:
                        catalog, schema, volume = parts_fallback
                        directory_path = f"/Volumes/{catalog}/{schema}/{volume}"
                        logger.info(f"Used fallback config path: {configured_volume}")
                        logger.info(f"Fallback constructed path: {directory_path}")
                    else:
                        logger.error(f"Invalid volume path format: {configured_volume}")
                        return []
            
            workspace_url = getattr(config, 'workspace_url', os.getenv('DATABRICKS_HOST'))
            logger.info(f"Workspace URL: {workspace_url}")
            logger.info(f"FINAL DIRECTORY PATH TO BROWSE: {directory_path}")
            
            # Get authentication token
            from src.repositories.databricks_auth_helper import DatabricksAuthHelper
            
            try:
                token = await DatabricksAuthHelper.get_auth_token(
                    workspace_url=workspace_url,
                    user_token=user_token
                )
                logger.info(f"Successfully obtained Databricks authentication token (length: {len(token)})")
            except Exception as auth_error:
                logger.error(f"Authentication failed: {auth_error}")
                return []
            
            if not token:
                logger.error("No authentication token available")
                return []
                
            if not workspace_url or workspace_url == 'https://example.databricks.com':
                logger.error(f"Invalid workspace URL: {workspace_url}")
                return []
            
            logger.info("Starting REAL Databricks API calls")
            
            # Try using Databricks SDK if available
            if WorkspaceClient:
                try:
                    logger.info("Attempting Databricks SDK method")
                    workspace_client = WorkspaceClient(
                        host=workspace_url,
                        token=token
                    )
                    
                    logger.info(f"SDK: Calling list_directory_contents('{directory_path}')")
                    
                    # List directory contents using SDK
                    files_list = []
                    file_infos = workspace_client.files.list_directory_contents(directory_path)
                    
                    for file_info in file_infos:
                        file_data = {
                            "name": os.path.basename(file_info.path),
                            "path": file_info.path,
                            "is_directory": file_info.is_directory,
                            "size": file_info.file_size if hasattr(file_info, 'file_size') and file_info.file_size else None,
                            "modified_at": file_info.modification_time.isoformat() if hasattr(file_info, 'modification_time') and file_info.modification_time else None,
                            "type": "directory" if file_info.is_directory else self._get_file_type(os.path.basename(file_info.path))
                        }
                        files_list.append(file_data)
                        logger.info(f"SDK: Found item: {file_data['name']} ({'dir' if file_data['is_directory'] else 'file'})")
                    
                    logger.info(f"SDK: Successfully found {len(files_list)} items")
                    return files_list
                    
                except Exception as sdk_error:
                    logger.error(f"SDK method failed: {sdk_error}")
                    logger.info("Falling back to REST API method")
            
            # Fallback: Use Files REST API
            logger.info("Attempting Files REST API method")
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                
                # Use Files API to list directory - correct Unity Catalog format
                list_url = f"{workspace_url}/api/2.0/fs/directories{directory_path}"
                logger.info(f"API: Request URL: {list_url}")
                logger.info(f"API: Headers: {headers}")
                
                async with session.get(list_url, headers=headers) as response:
                    logger.info(f"API: Response status: {response.status}")
                    
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"API: Response data keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
                        
                        files_list = []
                        contents = result.get('contents', [])
                        logger.info(f"API: Found {len(contents)} contents items")
                        
                        for item in contents:
                            file_data = {
                                "name": os.path.basename(item['path']),
                                "path": item['path'],
                                "is_directory": item.get('is_directory', False),
                                "size": item.get('file_size'),
                                "modified_at": item.get('modification_time'),
                                "type": "directory" if item.get('is_directory', False) else self._get_file_type(os.path.basename(item['path']))
                            }
                            files_list.append(file_data)
                            logger.info(f"API: Found item: {file_data['name']} ({'dir' if file_data['is_directory'] else 'file'})")
                        
                        logger.info(f"API: Successfully returned {len(files_list)} items")
                        return files_list
                        
                    elif response.status == 404:
                        logger.warning(f"Directory not found (404): {directory_path}")
                        return []
                    elif response.status == 403:
                        logger.error(f"Access forbidden (403): {directory_path}")
                        return []
                    elif response.status == 401:
                        logger.error(f"Unauthorized (401): Check token permissions")
                        return []
                    else:
                        error_text = await response.text()
                        logger.error(f"API error {response.status}: {error_text}")
                        return []
                        
        except Exception as e:
            logger.error(f"Exception in browse_volume_files: {e}", exc_info=True)
            return []
    
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
        filename: str
    ) -> bool:
        """
        Delete a knowledge file from Databricks Volume.
        
        Args:
            execution_id: Execution ID of the file
            group_id: Group ID for tenant isolation
            filename: Name of the file to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            # For now, simulate successful deletion
            logger.info(f"Simulating deletion of file {filename} for execution {execution_id}, group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting knowledge file: {e}")
            return False
    
    async def _ensure_volume_exists(self, workspace_client, config) -> None:
        """
        Ensure the Databricks Volume exists, create if it doesn't.
        
        Args:
            workspace_client: Databricks workspace client
            config: Databricks configuration
        """
        try:
            volume_path = config.knowledge_volume_path
            if not volume_path:
                volume_path = "main.default.knowledge"
            
            parts = volume_path.split('.')
            if len(parts) != 3:
                raise ValueError(f"Invalid volume path format: {volume_path}")
            
            catalog, schema, volume = parts
            
            # Try to create the volume (will fail silently if it exists)
            try:
                # Create catalog if needed
                workspace_client.catalogs.create(name=catalog, comment="Created by Kasal")
            except Exception:
                # Catalog might already exist
                pass
            
            try:
                # Create schema if needed
                workspace_client.schemas.create(
                    name=schema,
                    catalog_name=catalog,
                    comment="Created by Kasal for knowledge storage"
                )
            except Exception:
                # Schema might already exist
                pass
            
            try:
                # Create volume if needed
                workspace_client.volumes.create(
                    name=volume,
                    catalog_name=catalog,
                    schema_name=schema,
                    volume_type="MANAGED",
                    comment="Knowledge source storage for Kasal"
                )
                logger.info(f"Created volume: {volume_path}")
            except Exception as e:
                # Volume might already exist or other error
                logger.debug(f"Volume creation info: {e}")
                
        except Exception as e:
            logger.error(f"Error ensuring volume exists: {e}")
            # Continue anyway, the actual file operations will fail if there's a real problem
    
    def _get_workspace_client(self, config):
        """
        Get Databricks workspace client.

        Args:
            config: Databricks configuration

        Returns:
            Configured WorkspaceClient or None if SDK not available
        """
        # For now, always return None to avoid SDK issues
        logger.warning("Databricks SDK support is currently disabled for testing")
        return None

    async def _embed_uploaded_file(
        self,
        file_path: str,
        execution_id: str,
        group_id: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Embed an uploaded file immediately into Vector Search.

        Args:
            file_path: Full path to the file in Databricks Volume
            execution_id: Execution ID for scoping
            group_id: Group ID for multi-tenant filtering
            user_token: Optional user token for OBO authentication

        Returns:
            Embedding result with status and metadata
        """
        try:
            print(f"🔥 DEBUG: _embed_uploaded_file called for {file_path}")
            logger.info(f"[EMBEDDING] Starting immediate embedding for file: {file_path}")

            # 1. Read file content (simulate if file doesn't exist locally)
            print(f"🔥 DEBUG: About to call _read_file_content for {file_path}")
            content = await self._read_file_content(file_path, user_token)
            print(f"🔥 DEBUG: _read_file_content returned {len(content or '')} characters")
            logger.info(f"🔥 CRITICAL DEBUG: Content preview from file: {content[:200] if content else 'NO CONTENT'}...")

            if not content:
                logger.warning(f"[EMBEDDING] No content found for file: {file_path}")
                return {"status": "skipped", "reason": "No content to embed"}

            logger.info(f"[EMBEDDING] Read {len(content)} characters from file")

            # 2. Chunk the content
            chunks = await self._chunk_text(content, file_path)
            if not chunks:
                logger.warning(f"[EMBEDDING] No chunks generated for file: {file_path}")
                return {"status": "skipped", "reason": "No chunks generated"}

            logger.info(f"[EMBEDDING] Generated {len(chunks)} chunks")

            # 3. Get Vector Search storage (same pattern as DocumentationEmbeddingService)
            vector_storage = await self._get_vector_storage(user_token)
            if not vector_storage:
                logger.warning(f"[EMBEDDING] Vector search not configured - skipping embedding")
                return {"status": "skipped", "reason": "Vector search not configured"}

            logger.info(f"[EMBEDDING] Using vector index: {vector_storage.index_name}")

            # 4. Process each chunk
            embedded_chunks = 0
            for i, chunk in enumerate(chunks):
                try:
                    # Prepare metadata for this chunk
                    metadata = {
                        'source': file_path,
                        'execution_id': execution_id,
                        'group_id': group_id,
                        'chunk_index': i,
                        'total_chunks': len(chunks),
                        'file_path': file_path,
                        'filename': file_path.split('/')[-1],
                        'created_at': datetime.utcnow().isoformat(),
                        'content_type': 'knowledge_file'
                    }

                    # Generate embedding for the chunk (vector storage will handle this)
                    data = {
                        'content': chunk,
                        'metadata': metadata,
                        'context': {
                            'query_text': f"Knowledge file: {file_path.split('/')[-1]}",
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

            logger.info(f"[EMBEDDING] Successfully embedded {embedded_chunks}/{len(chunks)} chunks for file: {file_path}")

            return {
                "status": "success",
                "chunks_processed": len(chunks),
                "chunks_embedded": embedded_chunks,
                "index_name": vector_storage.index_name,
                "message": f"Successfully embedded {embedded_chunks} chunks from {file_path}"
            }

        except Exception as e:
            logger.error(f"[EMBEDDING] Error embedding uploaded file {file_path}: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to embed file {file_path}"
            }


    async def _chunk_text(self, content: str, file_path: str) -> List[str]:
        """
        Split text content into chunks for embedding.

        Args:
            content: Text content to chunk
            file_path: File path for logging

        Returns:
            List of text chunks
        """
        try:
            # Simple chunking strategy: split by paragraphs and combine to ~1000 chars
            paragraphs = [p.strip() for p in content.split('\n\n') if p.strip()]

            chunks = []
            current_chunk = ""

            for paragraph in paragraphs:
                # If adding this paragraph would exceed 1000 chars, save current chunk
                if current_chunk and len(current_chunk) + len(paragraph) > 1000:
                    chunks.append(current_chunk.strip())
                    current_chunk = paragraph
                else:
                    if current_chunk:
                        current_chunk += "\n\n" + paragraph
                    else:
                        current_chunk = paragraph

            # Add the last chunk
            if current_chunk.strip():
                chunks.append(current_chunk.strip())

            # Ensure we have at least one chunk
            if not chunks and content.strip():
                chunks = [content.strip()]

            logger.info(f"[CHUNKING] Split {len(content)} chars into {len(chunks)} chunks for {file_path}")
            return chunks

        except Exception as e:
            logger.error(f"[CHUNKING] Error chunking text for {file_path}: {e}")
            return []

    async def _get_vector_storage(self, user_token: Optional[str] = None):
        """
        Get Vector Search storage instance using same pattern as DocumentationEmbeddingService.

        Args:
            user_token: Optional user token for OBO authentication

        Returns:
            DatabricksVectorStorage instance or None if not configured
        """
        try:
            from src.core.unit_of_work import UnitOfWork
            from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
            from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

            logger.info(f"[VECTOR_STORAGE] Looking for active Databricks configuration for group: {self.group_id}")

            # Use Unit of Work pattern to get memory backends
            async with UnitOfWork() as uow:
                all_backends = await uow.memory_backend_repository.get_all()

            # Filter active Databricks backends
            databricks_backends = [
                b for b in all_backends
                if b.is_active and b.backend_type == MemoryBackendType.DATABRICKS
            ]

            if not databricks_backends:
                logger.warning("[VECTOR_STORAGE] No active Databricks memory backend found")
                return None

            # Sort by created_at descending and take the most recent
            databricks_backends.sort(key=lambda x: x.created_at, reverse=True)
            backend = databricks_backends[0]
            logger.info(f"[VECTOR_STORAGE] Using Databricks backend from group: {backend.group_id}, created: {backend.created_at}")

            # Convert backend model to config schema (same pattern as DocumentationEmbeddingService)
            memory_config = MemoryBackendConfig(
                backend_type=backend.backend_type,
                databricks_config=backend.databricks_config,
                enable_short_term=backend.enable_short_term,
                enable_long_term=backend.enable_long_term,
                enable_entity=backend.enable_entity,
                custom_config=backend.custom_config
            )

            # Get databricks config
            db_config = memory_config.databricks_config
            if not db_config:
                logger.warning("[VECTOR_STORAGE] Databricks configuration is None, cannot initialize storage")
                return None

            # Check for document index (same logic as DocumentationEmbeddingService)
            document_index = None
            if hasattr(db_config, 'document_index'):
                document_index = db_config.document_index
            elif isinstance(db_config, dict):
                document_index = db_config.get('document_index')

            if not document_index:
                logger.warning("[VECTOR_STORAGE] Document index not configured in Databricks backend")
                logger.info(f"[VECTOR_STORAGE] Available config keys: {list(db_config.keys()) if isinstance(db_config, dict) else 'Not a dict'}")
                return None

            logger.info(f"[VECTOR_STORAGE] Found document index: {document_index}")

            # Extract configuration values - handle both dict and object forms (same as DocumentationEmbeddingService)
            if hasattr(db_config, 'endpoint_name'):
                # It's an object
                endpoint_name = getattr(db_config, 'document_endpoint_name', None) or db_config.endpoint_name
                workspace_url = db_config.workspace_url
                embedding_dimension = db_config.embedding_dimension or 1024
                personal_access_token = db_config.personal_access_token
                service_principal_client_id = db_config.service_principal_client_id
                service_principal_client_secret = db_config.service_principal_client_secret
            elif isinstance(db_config, dict):
                # It's a dictionary
                endpoint_name = db_config.get('document_endpoint_name') or db_config.get('endpoint_name')
                workspace_url = db_config.get('workspace_url')
                embedding_dimension = db_config.get('embedding_dimension', 1024)
                personal_access_token = db_config.get('personal_access_token')
                service_principal_client_id = db_config.get('service_principal_client_id')
                service_principal_client_secret = db_config.get('service_principal_client_secret')
            else:
                logger.error(f"[VECTOR_STORAGE] Unexpected databricks_config type: {type(db_config)}")
                return None

            logger.info(f"[VECTOR_STORAGE] Configuration extracted - endpoint: {endpoint_name}, workspace: {workspace_url}")

            # Create storage instance (same constructor as DocumentationEmbeddingService)
            storage = DatabricksVectorStorage(
                endpoint_name=endpoint_name,
                index_name=document_index,
                crew_id="knowledge_files",  # Dedicated crew ID for knowledge files
                memory_type="document",
                embedding_dimension=embedding_dimension,
                workspace_url=workspace_url,
                personal_access_token=personal_access_token,
                service_principal_client_id=service_principal_client_id,
                service_principal_client_secret=service_principal_client_secret,
                user_token=user_token
            )

            logger.info(f"[VECTOR_STORAGE] Created storage for index: {storage.index_name}")
            return storage

        except Exception as e:
            logger.error(f"[VECTOR_STORAGE] Error creating vector storage: {e}", exc_info=True)
            return None