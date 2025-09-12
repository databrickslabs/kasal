"""
Databricks Volume callback for storing task outputs in Databricks Volumes.
"""
import os
import json
import logging
from typing import Any, Optional, Dict
from datetime import datetime
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files

from src.engines.crewai.callbacks.base import CrewAICallback

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
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        self.create_date_dirs = create_date_dirs
        self.file_format = file_format
        self.max_file_size_mb = max_file_size_mb
        self.execution_name = execution_name
        
        # Initialize Databricks client
        self._client = None
        
    @property
    def client(self) -> WorkspaceClient:
        """Lazy initialization of Databricks client."""
        if self._client is None:
            if not self.workspace_url or not self.token:
                raise ValueError(
                    "Databricks workspace URL and token are required. "
                    "Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables "
                    "or pass them to the constructor."
                )
            
            self._client = WorkspaceClient(
                host=self.workspace_url,
                token=self.token
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
            full_path = self._upload_to_volume(file_path, content)
            
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
    
    def _upload_to_volume(self, file_path: str, content: str) -> str:
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
        
        # Construct full path
        full_path = f"{self.volume_path.rstrip('/')}/{file_path}"
        
        # Create parent directories if needed
        parent_path = "/".join(full_path.split("/")[:-1])
        
        try:
            # Upload the file using Databricks SDK
            self.client.files.upload(
                file_path=full_path,
                contents=content.encode('utf-8'),
                overwrite=True
            )
            
            logger.info(f"File uploaded successfully to {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to upload file to {full_path}: {str(e)}")
            # Try alternative approach using DBFS API
            self._upload_via_dbfs_api(full_path, content)
        
        return full_path
    
    def _upload_via_dbfs_api(self, full_path: str, content: str) -> None:
        """
        Alternative upload method using DBFS API directly.
        
        Args:
            full_path: Full path to the file
            content: Content to upload
        """
        import base64
        import requests
        
        # Convert content to base64
        content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        
        # Prepare API request
        url = f"{self.workspace_url}/api/2.0/fs/files"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Create file
        create_data = {
            "path": full_path,
            "overwrite": True
        }
        
        response = requests.put(
            f"{url}/create",
            headers=headers,
            json=create_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create file: {response.text}")
        
        handle = response.json().get("handle")
        
        # Upload content
        upload_data = {
            "handle": handle,
            "data": content_b64
        }
        
        response = requests.post(
            f"{url}/add-block",
            headers=headers,
            json=upload_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to upload content: {response.text}")
        
        # Close the file
        close_data = {"handle": handle}
        response = requests.post(
            f"{url}/close",
            headers=headers,
            json=close_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to close file: {response.text}")
        
        logger.info(f"File uploaded via DBFS API to {full_path}")