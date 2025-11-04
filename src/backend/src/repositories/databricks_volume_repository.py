"""
Repository for interacting with Databricks Unity Catalog Volumes using WorkspaceClient.
"""
import os
import io
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor

from src.core.logger import LoggerManager
from src.utils.databricks_auth import (
    get_workspace_client,
    get_workspace_client_with_fallback,
    is_scope_error,
    get_databricks_auth_headers
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = LoggerManager.get_instance().system


class DatabricksVolumeRepository:
    """Repository for Databricks Unity Catalog Volume operations using WorkspaceClient."""

    def __init__(self, user_token: Optional[str] = None, group_id: Optional[str] = None):
        """Initialize the repository with Databricks authentication.

        Args:
            user_token: Optional user access token for OBO authentication
            group_id: Optional group ID for PAT token lookup (multi-tenant isolation)
        """
        self._workspace_client: Optional["WorkspaceClient"] = None
        self._user_token = user_token
        self._group_id = group_id
        self._executor = ThreadPoolExecutor(max_workers=1)

        # Log what authentication method will be used
        if user_token:
            logger.info(f"DatabricksVolumeRepository: Initialized with user token (length: {len(user_token)})")
        else:
            logger.warning("DatabricksVolumeRepository: No user token provided, will use fallback authentication")

        if group_id:
            logger.info(f"DatabricksVolumeRepository: Group ID set to: {group_id}")

    async def _get_client_with_group_context(
        self,
        user_token: Optional[str] = None,
        operation_name: str = "operation"
    ) -> tuple[Optional["WorkspaceClient"], Optional[str]]:
        """
        Get workspace client with group context set for PAT lookup.

        Args:
            user_token: Optional user token for OBO
            operation_name: Operation name for logging

        Returns:
            Tuple of (client, retry_token)
        """
        # If we have a group_id, temporarily set UserContext for PAT lookup
        if self._group_id:
            from src.utils.user_context import UserContext, GroupContext

            # Create minimal GroupContext for PAT lookup
            group_context = GroupContext(
                group_ids=[self._group_id],
                group_email=None,
                access_token=user_token
            )

            # Set context before auth
            UserContext.set_group_context(group_context)
            logger.debug(f"[{operation_name}] Set UserContext with group_id={self._group_id}")

        # Now call the standard auth function
        return await get_workspace_client_with_fallback(
            user_token=user_token,
            operation_name=operation_name
        )

    async def _ensure_client(self) -> bool:
        """Ensure we have a WorkspaceClient instance using centralized authentication."""
        if self._workspace_client:
            return True

        try:
            logger.info(f"DatabricksVolumeRepository._ensure_client: Creating WorkspaceClient with user_token={bool(self._user_token)}")

            # Use centralized authentication from databricks_auth module
            # Unified priority: OBO (user_token) → PAT → SPN
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
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name

        Returns:
            Creation result
        """
        # Helper to perform the actual volume creation
        async def _try_create_with_client(client, retry_token):
            def _create_volume():
                try:
                    full_name = f"{catalog}.{schema}.{volume_name}"

                    # Check if volume exists
                    try:
                        volume = client.volumes.read(full_name)
                        if volume:
                            logger.info(f"[VOLUME] Volume {full_name} already exists")
                            return {
                                "success": True,
                                "exists": True,
                                "message": "Volume already exists"
                            }
                    except Exception as e:
                        logger.debug(f"[VOLUME] Volume check returned: {e}")

                    # Create the volume
                    from databricks.sdk.service.catalog import VolumeType
                    logger.info(f"[VOLUME] Creating volume {full_name}")

                    try:
                        volume = client.volumes.create(
                            catalog_name=catalog,
                            schema_name=schema,
                            name=volume_name,
                            volume_type=VolumeType.MANAGED,
                            comment="Created by Kasal for database backups"
                        )
                        logger.info(f"[VOLUME] Successfully created volume {full_name}")
                    except Exception as create_error:
                        logger.error(f"[VOLUME] Failed to create volume: {str(create_error)}")
                        raise

                    return {
                        "success": True,
                        "created": True,
                        "message": f"Volume {full_name} created successfully"
                    }

                except Exception as e:
                    error_msg = str(e)

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

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

                    logger.error(f"[VOLUME] Failed to create volume: {error_msg}")
                    return {
                        "success": False,
                        "error": f"Failed to create volume: {error_msg}"
                    }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _create_volume)

        try:
            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=self._user_token,
                operation_name="volume_create"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_create_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[VOLUME] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[VOLUME] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_create_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_create_with_client(client_pat, None)

            return result

        except Exception as e:
            logger.error(f"[VOLUME] Error creating volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _upload_via_rest_api(self, file_path: str, file_content: bytes) -> Dict[str, Any]:
        """
        Upload file using Databricks Files REST API directly.
        This is a fallback for when SDK upload fails due to network zone errors in Databricks Apps.

        Uses the simple PUT method to upload directly to the file path.

        Args:
            file_path: Full volume path (e.g., /Volumes/catalog/schema/volume/file.db)
            file_content: File content as bytes

        Returns:
            Upload result dictionary
        """
        import aiohttp

        try:
            # Get authentication context
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context(user_token=None)  # Force PAT/SPN

            if not auth:
                return {
                    "success": False,
                    "error": "Failed to get authentication for REST API upload"
                }

            workspace_url = auth.workspace_url
            token = auth.token

            logger.info(f"[REST API] Uploading file via Databricks Files API: {file_path}")
            logger.info(f"[REST API] File size: {len(file_content)} bytes, Auth method: {auth.auth_method}")

            # For Unity Catalog volumes, use simple PUT to /api/2.0/fs/files{path}?overwrite=true
            # The path should start with /Volumes/
            # See: https://docs.databricks.com/aws/en/volumes/volume-files
            upload_url = f"{workspace_url}/api/2.0/fs/files{file_path}?overwrite=true"

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream"
            }

            logger.info(f"[REST API] PUT {upload_url}")

            async with aiohttp.ClientSession() as session:
                async with session.put(upload_url, data=file_content, headers=headers) as response:
                    logger.info(f"[REST API] Response status: {response.status}")
                    logger.info(f"[REST API] Response headers: {dict(response.headers)}")

                    # 200 OK, 201 Created, 204 No Content are all success statuses
                    if response.status not in [200, 201, 204]:
                        error_text = await response.text()
                        logger.error(f"[REST API] Upload failed with status {response.status}")
                        logger.error(f"[REST API] Error response: {error_text}")
                        logger.error(f"[REST API] Request URL: {upload_url}")
                        logger.error(f"[REST API] Request headers: {headers}")
                        return {
                            "success": False,
                            "error": f"REST API upload failed (status {response.status}): {error_text if error_text else 'No error message returned'}"
                        }

                    # Status 204 means success but no content in response
                    if response.status == 204:
                        logger.info(f"[REST API] ✓ File uploaded successfully to {file_path} (204 No Content)")
                    else:
                        response_text = await response.text()
                        logger.info(f"[REST API] ✓ File uploaded successfully to {file_path}")
                        logger.info(f"[REST API] Response: {response_text}")

                    return {
                        "success": True,
                        "path": file_path,
                        "size": len(file_content)
                    }

        except Exception as e:
            logger.error(f"[REST API] Upload exception: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"REST API upload exception: {str(e)}"
            }

    async def _download_via_rest_api(self, file_path: str) -> Dict[str, Any]:
        """
        Download file using Databricks Files REST API directly.
        This is a fallback for when SDK download fails due to network zone errors in Databricks Apps.

        Uses the simple GET method to download directly from the file path.

        Args:
            file_path: Full volume path (e.g., /Volumes/catalog/schema/volume/file.db)

        Returns:
            Download result dictionary with file content
        """
        import aiohttp

        try:
            # Get authentication context
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context(user_token=None)  # Force PAT/SPN

            if not auth:
                return {
                    "success": False,
                    "error": "Failed to get authentication for REST API download"
                }

            workspace_url = auth.workspace_url
            token = auth.token

            logger.info(f"[REST API] Downloading file via Databricks Files API: {file_path}")
            logger.info(f"[REST API] Auth method: {auth.auth_method}")

            # For Unity Catalog volumes, use simple GET from /api/2.0/fs/files{path}
            download_url = f"{workspace_url}/api/2.0/fs/files{file_path}"

            headers = {
                "Authorization": f"Bearer {token}"
            }

            logger.info(f"[REST API] GET {download_url}")

            async with aiohttp.ClientSession() as session:
                async with session.get(download_url, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"[REST API] Download failed with status {response.status}: {error_text}")

                        if response.status == 404:
                            return {
                                "success": False,
                                "error": f"File not found: {file_path}"
                            }

                        return {
                            "success": False,
                            "error": f"REST API download failed (status {response.status}): {error_text if error_text else 'No error message returned'}"
                        }

                    # Read the file content as bytes
                    content = await response.read()
                    logger.info(f"[REST API] ✓ File downloaded successfully from {file_path}")
                    logger.info(f"[REST API] Downloaded {len(content)} bytes")

                    return {
                        "success": True,
                        "path": file_path,
                        "content": content,
                        "size": len(content)
                    }

        except Exception as e:
            logger.error(f"[REST API] Download exception: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"REST API download exception: {str(e)}"
            }

    async def upload_file_to_volume(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        file_name: str,
        file_content: bytes,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upload a file to a Unity Catalog volume using WorkspaceClient.
        Creates the volume if it doesn't exist.
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to upload
            file_content: Content of the file as bytes
            user_token: Optional user token for OBO authentication (overrides instance token)

        Returns:
            Upload result
        """
        # Helper to perform the actual file upload
        async def _try_upload_with_client(client, retry_token, use_rest_api=False):
            # Construct the volume path
            volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"

            # If use_rest_api flag is set, use REST API instead of SDK
            if use_rest_api:
                return await self._upload_via_rest_api(volume_path, file_content)

            def _upload_file():
                try:
                    logger.info(f"Uploading file {file_name}: size={len(file_content)} bytes")

                    # Use the Files API through the SDK
                    # Wrap bytes in BytesIO to create a file-like object
                    content_stream = io.BytesIO(file_content) if isinstance(file_content, bytes) else file_content
                    client.files.upload(
                        file_path=volume_path,
                        content=content_stream,
                        overwrite=True
                    )

                    logger.info(f"Successfully uploaded file to {volume_path}")
                    return {
                        "success": True,
                        "path": volume_path,
                        "size": len(file_content)
                    }

                except Exception as e:
                    error_msg = str(e)

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

                    # Check if this is a network zone error in Databricks Apps
                    if "network zone" in error_msg.lower() and retry_token is None:
                        # We're using PAT in Databricks Apps - signal to use REST API
                        return {"_use_rest_api": True, "error": error_msg}

                    # Check if this is a SDK API version mismatch (databricks-sdk 0.71.0+)
                    if "unexpected keyword argument 'content'" in error_msg.lower():
                        logger.warning(f"SDK API mismatch detected - falling back to REST API")
                        return {"_use_rest_api": True, "error": error_msg}

                    logger.error(f"Failed to upload file: {e}")
                    return {
                        "success": False,
                        "error": f"Upload failed: {error_msg}"
                    }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _upload_file)

        try:
            # Use provided user_token if available, otherwise fall back to instance token
            effective_token = user_token if user_token is not None else self._user_token

            # Try to ensure volume exists first, but continue even if this fails
            # (volume might already exist but we lack permission to check/create)
            volume_result = await self.create_volume_if_not_exists(catalog, schema, volume_name)
            if not volume_result["success"]:
                error_msg = volume_result.get("error", "")
                # If it's a permission/scope error, log warning and continue (volume might exist)
                if "scope" in error_msg.lower() or "permission" in error_msg.lower() or "forbidden" in error_msg.lower():
                    logger.warning(f"[UPLOAD] Cannot verify volume exists due to permissions, attempting upload anyway: {error_msg}")
                else:
                    # Other errors (catalog/schema doesn't exist, etc.) should fail
                    return volume_result

            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=effective_token,
                operation_name="volume_upload"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_upload_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[UPLOAD] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[UPLOAD] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_upload_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_upload_with_client(client_pat, None)

            # Check if we got a network zone error and should use REST API
            if result.get("_use_rest_api"):
                logger.warning(f"[UPLOAD] Network zone error with SDK: {result.get('error')}")
                logger.info("[UPLOAD] Falling back to REST API for Databricks Apps compatibility")

                # Try upload using REST API instead
                result = await _try_upload_with_client(None, None, use_rest_api=True)

            return result

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
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to download

        Returns:
            Download result with file content
        """
        # Helper to perform the actual file download
        async def _try_download_with_client(client, retry_token):
            def _download_file():
                try:
                    # Construct the volume path
                    volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"

                    # Use the Files API through the SDK. SDK may return a DownloadResponse with .contents,
                    # a streaming response, raw bytes, or a file-like object. Normalize to bytes.
                    resp = client.files.download(volume_path)

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

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

                    # Check if this is a network zone error in Databricks Apps
                    if "network zone" in error_msg.lower() and retry_token is None:
                        # We're using PAT in Databricks Apps - signal to use REST API
                        return {"_use_rest_api": True, "error": error_msg}

                    if "not found" in error_msg.lower() or "404" in error_msg:
                        return {
                            "success": False,
                            "error": f"File not found: /Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
                        }

                    logger.error(f"Failed to download file: {e}")
                    return {
                        "success": False,
                        "error": f"Download failed: {error_msg}"
                    }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _download_file)

        try:
            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=self._user_token,
                operation_name="volume_download"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_download_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[DOWNLOAD] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[DOWNLOAD] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_download_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_download_with_client(client_pat, None)

            # Check if we got a network zone error and should use REST API
            if result.get("_use_rest_api"):
                logger.warning(f"[DOWNLOAD] Network zone error with SDK: {result.get('error')}")
                logger.info("[DOWNLOAD] Falling back to REST API for Databricks Apps compatibility")

                # Try download using REST API instead
                volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
                result = await self._download_via_rest_api(volume_path)

            return result

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
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            path: Optional path within the volume

        Returns:
            List of files and directories
        """
        # Helper to perform the actual list operation
        async def _try_list_with_client(client, retry_token):
            def _list_files():
                try:
                    # Construct the volume path
                    base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
                    full_path = f"{base_path}/{path}" if path else base_path

                    # Use the Files API through the SDK
                    file_list = list(client.files.list_directory_contents(full_path))

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

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

                    if "not found" in error_msg.lower() or "404" in error_msg:
                        base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
                        full_path = f"{base_path}/{path}" if path else base_path
                        return {
                            "success": False,
                            "error": f"Volume or path not found: {full_path}"
                        }

                    logger.error(f"Failed to list volume contents: {e}")
                    return {
                        "success": False,
                        "error": f"List failed: {error_msg}"
                    }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _list_files)

        try:
            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=self._user_token,
                operation_name="volume_list"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_list_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[LIST] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[LIST] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_list_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_list_with_client(client_pat, None)

            return result

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
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            directory_path: Path for the new directory

        Returns:
            Creation result
        """
        # Helper to perform the actual directory creation
        async def _try_create_with_client(client, retry_token):
            def _create_directory():
                try:
                    # Construct the volume path
                    base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
                    full_path = f"{base_path}/{directory_path}" if directory_path else base_path

                    # Use the Files API through the SDK to create directory
                    client.files.create_directory(full_path)

                    logger.info(f"Successfully created directory: {full_path}")
                    return {
                        "success": True,
                        "path": full_path
                    }

                except Exception as e:
                    error_msg = str(e)

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

                    if "already exists" in error_msg.lower():
                        base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
                        full_path = f"{base_path}/{directory_path}" if directory_path else base_path
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

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _create_directory)

        try:
            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=self._user_token,
                operation_name="volume_mkdir"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_create_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[MKDIR] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[MKDIR] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_mkdir_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_create_with_client(client_pat, None)

            return result

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
        Automatically falls back to PAT if OBO token lacks required scopes.

        Args:
            catalog: Unity Catalog name
            schema: Schema name
            volume_name: Volume name
            file_name: Name of the file to delete

        Returns:
            Deletion result
        """
        # Helper to perform the actual file deletion
        async def _try_delete_with_client(client, retry_token):
            def _delete_file():
                try:
                    # Construct the volume path
                    volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"

                    # Use the Files API through the SDK
                    client.files.delete(volume_path)

                    logger.info(f"Successfully deleted file: {volume_path}")
                    return {
                        "success": True,
                        "path": volume_path
                    }

                except Exception as e:
                    error_msg = str(e)

                    # Check if this is a scope error and we can retry
                    # Local check for "invalid scope" (Databricks Apps OBO token error)
                    is_invalid_scope = "invalid scope" in error_msg.lower()
                    if retry_token is not None and (is_scope_error(e) or is_invalid_scope):
                        # Return special marker to trigger retry
                        return {"_scope_error": True, "error": error_msg}

                    if "not found" in error_msg.lower() or "404" in error_msg:
                        volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
                        return {
                            "success": False,
                            "error": f"File not found: {volume_path}"
                        }

                    logger.error(f"Failed to delete file: {e}")
                    return {
                        "success": False,
                        "error": f"Deletion failed: {error_msg}"
                    }

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, _delete_file)

        try:
            # First attempt: Try with OBO if available
            client, retry_token = await self._get_client_with_group_context(
                user_token=self._user_token,
                operation_name="volume_delete"
            )

            if not client:
                return {
                    "success": False,
                    "error": "Failed to create Databricks client"
                }

            result = await _try_delete_with_client(client, retry_token)

            # Check if we got a scope error and should retry with PAT
            if result.get("_scope_error") and retry_token is not None:
                logger.warning(f"[DELETE] OBO token lacks required scopes: {result.get('error')}")
                logger.info("[DELETE] Retrying with PAT/SPN authentication (skipping OBO)")

                # Retry with PAT by passing user_token=None
                client_pat, _ = await self._get_client_with_group_context(
                    user_token=None,
                    operation_name="volume_delete_retry"
                )

                if not client_pat:
                    return {
                        "success": False,
                        "error": "Failed to create Databricks client with PAT fallback"
                    }

                # Retry with PAT (no retry_token this time)
                result = await _try_delete_with_client(client_pat, None)

            return result

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
            # Use unified auth instead of environment variables
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context()
            workspace_url = auth.workspace_url if auth else "https://your-workspace.databricks.com"
            workspace_url = workspace_url.rstrip('/')

        base_url = workspace_url.rstrip('/')

        if file_name:
            return f"{base_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}/{file_name}"
        else:
            return f"{base_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}"