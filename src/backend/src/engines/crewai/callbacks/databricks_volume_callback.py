"""
Databricks Volume callback for storing task outputs in Databricks Volumes.
"""
import os
import json
import logging
import asyncio
import io
from typing import Any, Optional, Dict
from datetime import datetime
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files

from src.engines.crewai.callbacks.base import CrewAICallback
from src.utils.databricks_auth import get_workspace_client

logger = logging.getLogger(__name__)


class DatabricksVolumeCallback(CrewAICallback):
    """Stores task outputs in Databricks Volumes."""
    
    def __init__(
        self,
        volume_path: str,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        create_date_dirs: bool = True,
        file_format: str = "json",
        max_file_size_mb: float = 100.0,
        task_key: Optional[str] = None,
        execution_name: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize the Databricks Volume callback.
        
        Args:
            volume_path: Path to the Databricks volume 
                        (e.g., 'catalog.schema.volume' or '/Volumes/catalog/schema/volume')
            workspace_url: Databricks workspace URL
            token: Databricks access token
            create_date_dirs: Whether to create date-based subdirectories
            file_format: Output file format (json, txt, csv)
            max_file_size_mb: Maximum file size in MB
            task_key: Optional task identifier
            execution_name: Optional execution/run name for organizing outputs
            **kwargs: Additional arguments for the base class
        """
        super().__init__(task_key=task_key, **kwargs)
        
        # Convert catalog.schema.volume format to /Volumes/catalog/schema/volume
        if not volume_path.startswith("/Volumes/"):
            # Assume it's in catalog.schema.volume format
            parts = volume_path.split(".")
            if len(parts) == 3:
                volume_path = f"/Volumes/{parts[0]}/{parts[1]}/{parts[2]}"
            else:
                # If it doesn't match the expected format, prepend /Volumes/
                volume_path = f"/Volumes/{volume_path.replace('.', '/')}"
        
        self.volume_path = volume_path
        self.workspace_url = workspace_url
        self.token = token
        self._auth_initialized = False
        self.create_date_dirs = create_date_dirs
        self.file_format = file_format
        self.max_file_size_mb = max_file_size_mb
        self.execution_name = execution_name

        # Initialize Databricks client
        self._client = None

    async def _ensure_auth(self) -> None:
        """Ensure authentication context is initialized."""
        if self._auth_initialized:
            return

        if not self.workspace_url or not self.token:
            try:
                from src.utils.databricks_auth import get_auth_context
                auth = await get_auth_context()
                if auth:
                    self.workspace_url = self.workspace_url or auth.workspace_url
                    self.token = self.token or auth.token
                    logger.debug(f"Using unified {auth.auth_method} authentication for Volume callback")
            except Exception as e:
                logger.warning(f"Failed to get unified auth for Volume callback: {e}")

        self._auth_initialized = True

    async def _ensure_client(self) -> WorkspaceClient:
        """Lazy initialization of Databricks client using centralized auth."""
        if self._client is None:
            await self._ensure_auth()

            # Get workspace client from centralized auth middleware
            # This supports OBO, PAT, and Service Principal OAuth
            self._client = await get_workspace_client(user_token=self.token)

            if not self._client:
                raise ValueError(
                    "Failed to get Databricks workspace client. "
                    "Ensure authentication is properly configured via databricks_auth middleware."
                )
        return self._client
    
    async def execute(self, output: Any) -> Dict[str, Any]:
        """
        Execute the callback to store output in Databricks Volume.
        
        Args:
            output: The task output to store
            
        Returns:
            Dictionary containing the file path and metadata
        """
        try:
            # Generate file path
            file_path = self._generate_file_path()
            
            # Convert output to appropriate format
            content = self._format_output(output)
            
            # Check file size
            size_mb = len(content.encode('utf-8')) / (1024 * 1024)
            if size_mb > self.max_file_size_mb:
                raise ValueError(
                    f"Output size ({size_mb:.2f}MB) exceeds maximum allowed size "
                    f"({self.max_file_size_mb}MB)"
                )
            
            # Upload to Databricks Volume
            full_path = await self._upload_to_volume(file_path, content)
            
            # Prepare metadata
            metadata = {
                "volume_path": full_path,
                "file_size_mb": size_mb,
                "task_key": self.task_key,
                "timestamp": datetime.now().isoformat(),
                "format": self.file_format
            }
            
            logger.info(f"Successfully uploaded output to Databricks Volume: {full_path}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to upload to Databricks Volume: {str(e)}")
            raise
    
    def _generate_file_path(self) -> str:
        """Generate the file path within the volume."""
        current_date = datetime.now()
        
        # Build path components
        path_components = []
        
        # Add execution name as parent folder if provided
        if self.execution_name:
            # Clean the execution name to be filesystem-safe
            safe_execution_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' 
                                         for c in self.execution_name)
            safe_execution_name = safe_execution_name.replace(' ', '_')
            path_components.append(safe_execution_name)
        
        if self.create_date_dirs:
            path_components.extend([
                str(current_date.year),
                f"{current_date.month:02d}",
                f"{current_date.day:02d}"
            ])
        
        # Generate filename
        timestamp = current_date.strftime("%Y%m%d_%H%M%S")
        task_identifier = self.task_key or "output"
        filename = f"{task_identifier}_{timestamp}.{self.file_format}"
        path_components.append(filename)
        
        return "/".join(path_components)
    
    def _format_output(self, output: Any) -> str:
        """
        Format the output based on the specified format.
        
        Args:
            output: The output to format
            
        Returns:
            Formatted string content
        """
        if self.file_format == "json":
            if hasattr(output, 'raw'):
                # Handle CrewAI output objects
                content = {
                    "raw": output.raw,
                    "json_dict": output.json_dict if hasattr(output, 'json_dict') else None,
                    "pydantic": output.pydantic.dict() if hasattr(output, 'pydantic') and output.pydantic else None,
                    "metadata": {
                        "task_key": self.task_key,
                        "timestamp": datetime.now().isoformat()
                    }
                }
            elif isinstance(output, dict):
                content = output
            else:
                content = {
                    "output": str(output),
                    "metadata": {
                        "task_key": self.task_key,
                        "timestamp": datetime.now().isoformat()
                    }
                }
            return json.dumps(content, indent=2, default=str)
            
        elif self.file_format == "csv":
            # Handle CSV format (simplified for now)
            if isinstance(output, (list, tuple)):
                import csv
                import io
                
                output_buffer = io.StringIO()
                writer = csv.writer(output_buffer)
                for row in output:
                    if isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    else:
                        writer.writerow([row])
                return output_buffer.getvalue()
            else:
                return str(output)
                
        else:  # Default to text format
            if hasattr(output, 'raw'):
                return output.raw
            return str(output)
    
    async def _upload_to_volume(self, file_path: str, content: str) -> str:
        """
        Upload content to Databricks Volume.

        Args:
            file_path: Relative path within the volume
            content: Content to upload

        Returns:
            Full path to the uploaded file
        """
        # Ensure volume path starts with /Volumes
        if not self.volume_path.startswith("/Volumes"):
            raise ValueError("Volume path must start with /Volumes")

        # Parse volume path to extract catalog, schema, and volume name
        # Expected format: /Volumes/catalog/schema/volume
        path_parts = self.volume_path.strip("/").split("/")
        if len(path_parts) < 4 or path_parts[0] != "Volumes":
            raise ValueError(f"Invalid volume path format: {self.volume_path}. Expected: /Volumes/catalog/schema/volume")

        catalog = path_parts[1]
        schema = path_parts[2]
        volume_name = path_parts[3]

        # Ensure the volume exists using the repository
        from src.repositories.databricks_volume_repository import DatabricksVolumeRepository

        volume_repo = DatabricksVolumeRepository(user_token=self.token)
        create_result = await volume_repo.create_volume_if_not_exists(
            catalog=catalog,
            schema=schema,
            volume_name=volume_name
        )

        if not create_result.get("success"):
            error_msg = create_result.get("error", "Unknown error creating volume")
            logger.error(f"Failed to create volume: {error_msg}")
            raise ValueError(f"Failed to ensure volume exists: {error_msg}")

        if create_result.get("created"):
            logger.info(f"Created new volume: {catalog}.{schema}.{volume_name}")
        elif create_result.get("exists"):
            logger.debug(f"Volume already exists: {catalog}.{schema}.{volume_name}")

        # Construct full path
        full_path = f"{self.volume_path.rstrip('/')}/{file_path}"

        # Create parent directories if needed
        parent_dir_path = "/".join(file_path.split("/")[:-1])
        if parent_dir_path:
            dir_result = await volume_repo.create_volume_directory(
                catalog=catalog,
                schema=schema,
                volume_name=volume_name,
                directory_path=parent_dir_path
            )
            if not dir_result.get("success"):
                logger.warning(f"Failed to create directory {parent_dir_path}: {dir_result.get('error')}")

        # Get the workspace client
        client = await self._ensure_client()

        # Convert content string to BinaryIO (file-like object)
        # The SDK expects a file-like object, not raw bytes
        content_bytes = content.encode('utf-8')
        binary_content = io.BytesIO(content_bytes)

        # Upload the file using Databricks SDK
        # File path must be in format: /Volumes/catalog/schema/volume/path
        client.files.upload(
            file_path=full_path,
            content=binary_content,
            overwrite=True
        )

        logger.info(f"File uploaded successfully to {full_path} ({len(content_bytes)} bytes)")

        return full_path