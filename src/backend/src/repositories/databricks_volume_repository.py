"""
Repository for interacting with Databricks Unity Catalog Volumes using WorkspaceClient.
"""
import os
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor

from src.core.logger import LoggerManager
from src.utils.databricks_auth import get_workspace_client, get_databricks_auth_headers

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = LoggerManager.get_instance().system


class DatabricksVolumeRepository:
    """Repository for Databricks Unity Catalog Volume operations using WorkspaceClient."""

    def __init__(self, user_token: Optional[str] = None):
        """Initialize the repository with Databricks authentication.

        Args:
            user_token: Optional user access token for OBO authentication
        """
        self._workspace_client: Optional["WorkspaceClient"] = None
        self._user_token = user_token
        self._executor = ThreadPoolExecutor(max_workers=1)

        # Log what authentication method will be used
        if user_token:
            logger.info(f"DatabricksVolumeRepository: Initialized with user token (length: {len(user_token)})")
        else:
            logger.warning("DatabricksVolumeRepository: No user token provided, will use fallback authentication")

    async def _ensure_client(self) -> bool:
        """Ensure we have a WorkspaceClient instance using centralized authentication."""
        if self._workspace_client:
            return True

        try:
            logger.info(f"DatabricksVolumeRepository._ensure_client: Creating WorkspaceClient with user_token={bool(self._user_token)}")

            # Use centralized authentication from databricks_auth module
            self._workspace_client = await get_workspace_client(self._user_token)

            if self._workspace_client:
                logger.info("Successfully created WorkspaceClient using centralized authentication")
                return True
            else:
                logger.error("Failed to create WorkspaceClient using centralized authentication")
                return False

        except Exception as e:
            logger.error(f"Error ensuring WorkspaceClient: {e}")
            return False
    
    async def create_volume_if_not_exists(
        self,
        catalog: str,
        schema: str,
        volume_name: str
    ) -> Dict[str, Any]:
        """
        Create a Unity Catalog volume if it doesn't exist.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            
        Returns:
            Creation result
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Check if volume exists and create if not using SDK
            def _create_volume():
                try:
                    # Check if volume exists
                    full_name = f"{catalog}.{schema}.{volume_name}"
                    try:
                        volume = self._workspace_client.volumes.read(full_name)
                        if volume:
                            logger.info(f"Volume {full_name} already exists")
                            return {
                                "success": True,
                                "exists": True,
                                "message": "Volume already exists"
                            }
                    except Exception as e:
                        # Volume doesn't exist, continue to create it
                        logger.debug(f"Volume check returned: {e}")
                    
                    # Create the volume
                    from databricks.sdk.service.catalog import VolumeType
                    
                    logger.info(f"Creating volume {full_name}")
                    volume = self._workspace_client.volumes.create(
                        catalog_name=catalog,
                        schema_name=schema,
                        name=volume_name,
                        volume_type=VolumeType.MANAGED,
                        comment="Created by Kasal for database backups"
                    )
                    
                    logger.info(f"Successfully created volume {full_name}")
                    return {
                        "success": True,
                        "created": True,
                        "message": f"Volume {full_name} created successfully"
                    }
                    
                except Exception as e:
                    error_msg = str(e)
                    # Check if error is because catalog or schema doesn't exist
                    if "does not exist" in error_msg.lower():
                        if f"catalog '{catalog}'" in error_msg.lower() or f"catalog `{catalog}`" in error_msg.lower():
                            return {
                                "success": False,
                                "error": f"Catalog '{catalog}' does not exist. Please create it first in Databricks."
                            }
                        elif f"schema '{schema}'" in error_msg.lower() or f"schema `{catalog}`.`{schema}`" in error_msg.lower():
                            return {
                                "success": False,
                                "error": f"Schema '{catalog}.{schema}' does not exist. Please create it first in Databricks."
                            }
                    
                    logger.error(f"Failed to create volume: {error_msg}")
                    return {
                        "success": False,
                        "error": f"Failed to create volume: {error_msg}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _create_volume)
            
        except Exception as e:
            logger.error(f"Error creating volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def upload_file_to_volume(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        file_name: str,
        file_content: bytes
    ) -> Dict[str, Any]:
        """
        Upload a file to a Unity Catalog volume using WorkspaceClient.
        Creates the volume if it doesn't exist.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to upload
            file_content: Content of the file as bytes
            
        Returns:
            Upload result
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Ensure volume exists
            volume_result = await self.create_volume_if_not_exists(catalog, schema, volume_name)
            if not volume_result["success"]:
                return volume_result
            
            # Construct the volume path
            volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
            
            logger.info(f"Uploading file {file_name}: size={len(file_content)} bytes")
            
            def _upload_file():
                try:
                    # Use the Files API through the SDK
                    self._workspace_client.files.upload(
                        file_path=volume_path,
                        contents=file_content,
                        overwrite=True
                    )
                    
                    logger.info(f"Successfully uploaded file to {volume_path}")
                    return {
                        "success": True,
                        "path": volume_path,
                        "size": len(file_content)
                    }
                    
                except Exception as e:
                    logger.error(f"Failed to upload file: {e}")
                    return {
                        "success": False,
                        "error": f"Upload failed: {str(e)}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _upload_file)
            
        except Exception as e:
            logger.error(f"Error uploading file to volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def download_file_from_volume(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        file_name: str
    ) -> Dict[str, Any]:
        """
        Download a file from a Unity Catalog volume using WorkspaceClient.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to download
            
        Returns:
            Download result with file content
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Construct the volume path
            volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
            
            def _download_file():
                try:
                    # Use the Files API through the SDK. SDK may return a DownloadResponse with .contents,
                    # a streaming response, raw bytes, or a file-like object. Normalize to bytes.
                    resp = self._workspace_client.files.download(volume_path)

                    def _to_bytes(obj):
                        if obj is None:
                            return None
                        if isinstance(obj, (bytes, bytearray)):
                            return bytes(obj)
                        # Common DownloadResponse path
                        if hasattr(obj, "contents"):
                            return _to_bytes(getattr(obj, "contents"))
                        # File-like
                        if hasattr(obj, "read"):
                            try:
                                return _to_bytes(obj.read())
                            except Exception:
                                pass
                        # Streaming API
                        if hasattr(obj, "iter_bytes"):
                            try:
                                return b"".join(obj.iter_bytes())
                            except Exception:
                                pass
                        # Generic iterable of chunks
                        try:
                            it = iter(obj)
                            chunks = []
                            for chunk in it:
                                if isinstance(chunk, (bytes, bytearray)):
                                    chunks.append(bytes(chunk))
                                elif hasattr(chunk, "read"):
                                    b = _to_bytes(chunk)
                                    if b:
                                        chunks.append(b)
                            if chunks:
                                return b"".join(chunks)
                        except TypeError:
                            pass
                        # Fallback attribute
                        possible = getattr(obj, "data", None)
                        if isinstance(possible, (bytes, bytearray)):
                            return bytes(possible)
                        return None

                    content = _to_bytes(resp)
                    if content is None:
                        raise RuntimeError(f"Unexpected download response type; no bytes available (type={type(resp).__name__})")

                    size = len(content)
                    logger.info(f"Successfully downloaded file from {volume_path}, size: {size} bytes")
                    return {
                        "success": True,
                        "path": volume_path,
                        "content": content,
                        "size": size
                    }

                except Exception as e:
                    error_msg = str(e)
                    if "not found" in error_msg.lower() or "404" in error_msg:
                        return {
                            "success": False,
                            "error": f"File not found: {volume_path}"
                        }

                    logger.error(f"Failed to download file: {e}")
                    return {
                        "success": False,
                        "error": f"Download failed: {error_msg}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _download_file)
            
        except Exception as e:
            logger.error(f"Error downloading file from volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def list_volume_contents(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        path: str = ""
    ) -> Dict[str, Any]:
        """
        List contents of a Unity Catalog volume directory.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            path: Optional path within the volume
            
        Returns:
            List of files and directories
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Construct the volume path
            base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
            full_path = f"{base_path}/{path}" if path else base_path
            
            def _list_files():
                try:
                    # Use the Files API through the SDK
                    file_list = list(self._workspace_client.files.list_directory_contents(full_path))
                    
                    files = []
                    for item in file_list:
                        file_info = {
                            "path": item.path,
                            "name": item.name if hasattr(item, 'name') else item.path.split('/')[-1],
                            "is_directory": item.is_directory if hasattr(item, 'is_directory') else False,
                            "file_size": item.file_size if hasattr(item, 'file_size') else None,
                            "modification_time": item.modification_time if hasattr(item, 'modification_time') else None
                        }
                        files.append(file_info)
                    
                    logger.info(f"Successfully listed {len(files)} items in {full_path}")
                    return {
                        "success": True,
                        "path": full_path,
                        "files": files
                    }
                    
                except Exception as e:
                    error_msg = str(e)
                    if "not found" in error_msg.lower() or "404" in error_msg:
                        return {
                            "success": False,
                            "error": f"Volume or path not found: {full_path}"
                        }
                    
                    logger.error(f"Failed to list volume contents: {e}")
                    return {
                        "success": False,
                        "error": f"List failed: {error_msg}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _list_files)
            
        except Exception as e:
            logger.error(f"Error listing volume contents: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def create_volume_directory(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        directory_path: str = ""
    ) -> Dict[str, Any]:
        """
        Create a directory in a Unity Catalog volume.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            directory_path: Path for the new directory
            
        Returns:
            Creation result
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Construct the volume path
            base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
            full_path = f"{base_path}/{directory_path}" if directory_path else base_path
            
            def _create_directory():
                try:
                    # Use the Files API through the SDK to create directory
                    self._workspace_client.files.create_directory(full_path)
                    
                    logger.info(f"Successfully created directory: {full_path}")
                    return {
                        "success": True,
                        "path": full_path
                    }
                    
                except Exception as e:
                    error_msg = str(e)
                    if "already exists" in error_msg.lower():
                        logger.info(f"Directory already exists: {full_path}")
                        return {
                            "success": True,
                            "path": full_path,
                            "exists": True
                        }
                    
                    logger.error(f"Failed to create directory: {e}")
                    return {
                        "success": False,
                        "error": f"Directory creation failed: {error_msg}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _create_directory)
            
        except Exception as e:
            logger.error(f"Error creating volume directory: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def delete_volume_file(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        file_name: str
    ) -> Dict[str, Any]:
        """
        Delete a file from a Unity Catalog volume.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to delete
            
        Returns:
            Deletion result
        """
        try:
            if not await self._ensure_client():
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }
            
            # Construct the volume path
            volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
            
            def _delete_file():
                try:
                    # Use the Files API through the SDK
                    self._workspace_client.files.delete(volume_path)
                    
                    logger.info(f"Successfully deleted file: {volume_path}")
                    return {
                        "success": True,
                        "path": volume_path
                    }
                    
                except Exception as e:
                    error_msg = str(e)
                    if "not found" in error_msg.lower() or "404" in error_msg:
                        return {
                            "success": False,
                            "error": f"File not found: {volume_path}"
                        }
                    
                    logger.error(f"Failed to delete file: {e}")
                    return {
                        "success": False,
                        "error": f"Deletion failed: {error_msg}"
                    }
            
            # Run synchronously in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _delete_file)
            
        except Exception as e:
            logger.error(f"Error deleting file from volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_databricks_url(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        file_name: Optional[str] = None
    ) -> str:
        """
        Generate a Databricks workspace URL for viewing a volume or file.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Optional file name

        Returns:
            Databricks workspace URL
        """
        # Import here to avoid circular dependency
        from src.utils.databricks_auth import _databricks_auth

        # Get workspace URL from centralized auth
        workspace_url = await _databricks_auth.get_workspace_url()
        if not workspace_url:
            workspace_url = os.environ.get("DATABRICKS_HOST", "https://your-workspace.databricks.com").rstrip('/')

        base_url = workspace_url.rstrip('/')

        if file_name:
            return f"{base_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}/{file_name}"
        else:
            return f"{base_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}"