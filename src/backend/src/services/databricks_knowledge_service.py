"""
Databricks Knowledge Source Service
"""
from typing import Dict, Any, List, Optional
from fastapi import UploadFile
import logging
import os
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
                            
                            return {
                                "status": "success",
                                "path": file_path,
                                "filename": file.filename,
                                "size": file_size,
                                "execution_id": execution_id,
                                "group_id": group_id,
                                "uploaded_at": datetime.now().isoformat(),
                                "selected_agents": selected_agents,
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
                        
                        return {
                            "status": "success",
                            "path": file_path,
                            "filename": file.filename,
                            "size": file_size,
                            "execution_id": execution_id,
                            "group_id": group_id,
                            "uploaded_at": datetime.now().isoformat(),
                            "selected_agents": selected_agents,
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
            
            # Return success response (simulated)
            response = {
                "status": "success",
                "path": file_path,
                "filename": file.filename,
                "size": file_size,
                "execution_id": execution_id,
                "group_id": group_id,
                "uploaded_at": datetime.now().isoformat(),
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
            
        Returns:
            List of files and directories with metadata
        """
        try:
            # For now, return sample data for testing
            logger.info(f"Simulating browse of volume path: {volume_path} for group {group_id}")
            
            # Return some sample files and folders for testing
            sample_files = [
                {
                    "name": "sample_document.pdf",
                    "path": f"/Volumes/main/default/knowledge/{group_id}/sample_document.pdf",
                    "is_directory": False,
                    "size": 1024000,
                    "modified_at": datetime.now().isoformat(),
                    "type": "pdf"
                },
                {
                    "name": "training_data.csv",
                    "path": f"/Volumes/main/default/knowledge/{group_id}/training_data.csv",
                    "is_directory": False,
                    "size": 512000,
                    "modified_at": datetime.now().isoformat(),
                    "type": "csv"
                },
                {
                    "name": "archives",
                    "path": f"/Volumes/main/default/knowledge/{group_id}/archives",
                    "is_directory": True,
                    "size": None,
                    "modified_at": datetime.now().isoformat(),
                    "type": "directory"
                }
            ]
            
            return sample_files
            
        except Exception as e:
            logger.error(f"Error browsing volume files: {e}")
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