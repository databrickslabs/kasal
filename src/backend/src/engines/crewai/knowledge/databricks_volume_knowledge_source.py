"""
Databricks Volume Knowledge Source for CrewAI.

This module provides a custom knowledge source that integrates with Databricks volumes,
allowing files to be uploaded, stored, and used as knowledge sources for AI agents.
"""

import os
import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from crewai.knowledge.source.base_knowledge_source import BaseKnowledgeSource
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import DownloadResponse

logger = logging.getLogger(__name__)


class DatabricksVolumeKnowledgeSource(BaseKnowledgeSource):
    """
    Knowledge source that fetches and processes files from Databricks volumes.
    
    This source supports multiple file formats (PDF, TXT, JSON, CSV) and handles
    file retrieval from Databricks volumes with proper authentication and path management.
    """
    
    volume_path: str = Field(description="Databricks volume path (catalog.schema.volume)")
    execution_id: str = Field(description="Execution ID for scoping files")
    group_id: str = Field(description="Group ID for tenant isolation")
    file_paths: List[str] = Field(default_factory=list, description="List of file paths in the volume")
    workspace_url: Optional[str] = Field(default=None, description="Databricks workspace URL")
    token: Optional[str] = Field(default=None, description="Databricks access token")
    file_format: str = Field(default="auto", description="File format: auto, pdf, txt, json, csv")
    chunk_size: int = Field(default=1000, description="Size of text chunks")
    chunk_overlap: int = Field(default=200, description="Overlap between chunks")
    
    def __init__(self, **data):
        """Initialize the Databricks volume knowledge source."""
        super().__init__(**data)
        self._client = None
        self._initialize_paths()
    
    def _initialize_paths(self):
        """Initialize volume paths with execution and group scope."""
        # Create execution-specific path
        base_path = f"/Volumes/{self.volume_path.replace('.', '/')}"
        self.execution_path = f"{base_path}/knowledge/{self.group_id}/{self.execution_id}"
        logger.info(f"Initialized Databricks volume path: {self.execution_path}")
    
    @property
    def client(self) -> WorkspaceClient:
        """Get or create Databricks workspace client."""
        if self._client is None:
            workspace_url = self.workspace_url or os.environ.get("DATABRICKS_HOST")
            token = self.token or os.environ.get("DATABRICKS_TOKEN")
            
            if not workspace_url or not token:
                raise ValueError(
                    "Databricks credentials not configured. "
                    "Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables."
                )
            
            self._client = WorkspaceClient(
                host=workspace_url.rstrip('/'),
                token=token
            )
        return self._client
    
    def load_content(self) -> Dict[Any, str]:
        """
        Load and format content from Databricks volume files.
        
        Returns:
            Dictionary mapping file paths to their content
        """
        content_map = {}
        
        try:
            # List files if not specified
            if not self.file_paths:
                self.file_paths = self._list_volume_files()
            
            # Download and process each file
            for file_path in self.file_paths:
                try:
                    # Check if file_path is already a full path
                    if file_path.startswith('/Volumes/'):
                        full_path = file_path
                    else:
                        full_path = f"{self.execution_path}/{file_path}"
                    
                    logger.info(f"Loading file from Databricks: {full_path}")
                    
                    # Download file content
                    response = self.client.files.download(full_path)
                    content = self._process_file_content(response, file_path)
                    
                    if content:
                        content_map[full_path] = content
                        logger.info(f"Successfully loaded file: {file_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to load file {file_path}: {str(e)}")
                    continue
            
            if not content_map:
                raise ValueError("No files could be loaded from Databricks volume")
            
            return content_map
            
        except Exception as e:
            logger.error(f"Failed to load content from Databricks: {str(e)}")
            raise ValueError(f"Failed to fetch files from Databricks: {str(e)}")
    
    def _list_volume_files(self) -> List[str]:
        """
        List all files in the execution-specific volume path.
        
        Returns:
            List of file paths relative to the execution path
        """
        try:
            files = []
            file_list = self.client.files.list_directory_contents(self.execution_path)
            
            for file_info in file_list:
                if not file_info.is_directory:
                    # Get relative path from execution path
                    relative_path = file_info.path.replace(f"{self.execution_path}/", "")
                    files.append(relative_path)
            
            logger.info(f"Found {len(files)} files in volume: {files}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files in volume: {str(e)}")
            return []
    
    def _process_file_content(self, response: DownloadResponse, file_path: str) -> str:
        """
        Process downloaded file content based on file format.
        
        Args:
            response: Download response from Databricks
            file_path: Path to the file for format detection
            
        Returns:
            Processed text content
        """
        try:
            # Read content from response
            content_bytes = response.contents.read()
            
            # Detect format if auto
            if self.file_format == "auto":
                file_format = self._detect_format(file_path)
            else:
                file_format = self.file_format
            
            # Process based on format
            if file_format == "json":
                data = json.loads(content_bytes.decode('utf-8'))
                return self._format_json_content(data)
            
            elif file_format == "csv":
                import csv
                import io
                text = content_bytes.decode('utf-8')
                reader = csv.DictReader(io.StringIO(text))
                return self._format_csv_content(list(reader))
            
            elif file_format == "pdf":
                # For PDF, we'll need to use a PDF processing library
                # For now, we'll treat it as binary and skip
                logger.warning(f"PDF processing not yet implemented for {file_path}")
                return ""
            
            else:  # txt or unknown
                return content_bytes.decode('utf-8', errors='ignore')
                
        except Exception as e:
            logger.error(f"Failed to process file content: {str(e)}")
            return ""
    
    def _detect_format(self, file_path: str) -> str:
        """Detect file format from extension."""
        ext = Path(file_path).suffix.lower()
        format_map = {
            '.json': 'json',
            '.csv': 'csv',
            '.pdf': 'pdf',
            '.txt': 'txt',
            '.md': 'txt',
            '.log': 'txt'
        }
        return format_map.get(ext, 'txt')
    
    def _format_json_content(self, data: Any) -> str:
        """Format JSON data into readable text."""
        if isinstance(data, dict):
            lines = []
            for key, value in data.items():
                lines.append(f"{key}: {value}")
            return "\n".join(lines)
        elif isinstance(data, list):
            return "\n".join([str(item) for item in data])
        else:
            return str(data)
    
    def _format_csv_content(self, rows: List[Dict]) -> str:
        """Format CSV data into readable text."""
        if not rows:
            return ""
        
        lines = []
        for i, row in enumerate(rows, 1):
            lines.append(f"Row {i}:")
            for key, value in row.items():
                lines.append(f"  {key}: {value}")
        return "\n".join(lines)
    
    def validate_content(self, content: Any) -> str:
        """
        Validate and format content for knowledge storage.
        
        Args:
            content: Content to validate
            
        Returns:
            Validated and formatted content string
        """
        if isinstance(content, str):
            return content
        elif isinstance(content, (dict, list)):
            return json.dumps(content, indent=2)
        else:
            return str(content)
    
    def add(self) -> None:
        """Process and store the files as knowledge chunks."""
        try:
            content = self.load_content()
            
            for file_path, text in content.items():
                # Validate content
                validated_text = self.validate_content(text)
                
                # Chunk the text
                chunks = self._chunk_text(validated_text)
                
                # Add metadata to chunks
                for chunk in chunks:
                    chunk_with_metadata = f"Source: {file_path}\n{chunk}"
                    self.chunks.append(chunk_with_metadata)
            
            # Save documents to knowledge storage
            self._save_documents()
            logger.info(f"Successfully added {len(self.chunks)} chunks to knowledge base")
            
        except Exception as e:
            logger.error(f"Failed to add knowledge source: {str(e)}")
            raise
    
    async def upload_file(self, file_path: str, content: bytes) -> str:
        """
        Upload a file to the Databricks volume.
        
        Args:
            file_path: Relative path for the file in the volume
            content: File content as bytes
            
        Returns:
            Full path of the uploaded file
        """
        try:
            full_path = f"{self.execution_path}/{file_path}"
            
            # Ensure directory exists
            dir_path = os.path.dirname(full_path)
            try:
                self.client.files.create_directory(dir_path)
            except:
                pass  # Directory might already exist
            
            # Upload file
            self.client.files.upload(full_path, content)
            
            # Add to file paths if not already there
            if file_path not in self.file_paths:
                self.file_paths.append(file_path)
            
            logger.info(f"Successfully uploaded file to Databricks: {full_path}")
            return full_path
            
        except Exception as e:
            logger.error(f"Failed to upload file to Databricks: {str(e)}")
            raise