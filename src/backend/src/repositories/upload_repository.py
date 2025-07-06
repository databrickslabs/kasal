import os
import shutil
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from fastapi import UploadFile

logger = logging.getLogger(__name__)

class UploadRepository:
    """
    Repository for file upload operations.
    Handles file system operations for uploaded files.
    """
    
    def __init__(self, upload_dir: Path):
        """
        Initialize the repository with upload directory.
        
        Args:
            upload_dir: Directory path where files will be stored
        """
        self.upload_dir = upload_dir
        self._ensure_directory_exists()
    
    def _ensure_directory_exists(self) -> None:
        """Ensure that the upload directory exists"""
        self.upload_dir.mkdir(parents=True, exist_ok=True)
    
    async def save_file(self, file: UploadFile) -> Dict[str, Any]:
        """
        Save an uploaded file to the filesystem.
        
        Args:
            file: FastAPI UploadFile object
            
        Returns:
            Dictionary with file metadata
            
        Raises:
            Exception: If file saving fails
        """
        try:
            self._ensure_directory_exists()
            
            # Create file path
            file_path = self.upload_dir / file.filename
            
            # Save the file
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            # Return the file metadata
            return {
                "filename": file.filename,
                "path": str(file.filename),
                "full_path": str(file_path),
                "file_size_bytes": os.path.getsize(file_path),
                "is_uploaded": True
            }
        except Exception as e:
            logger.error(f"Error saving file {file.filename}: {str(e)}")
            raise
    
    async def save_multiple_files(self, files: List[UploadFile]) -> List[Dict[str, Any]]:
        """
        Save multiple uploaded files to the filesystem.
        
        Args:
            files: List of FastAPI UploadFile objects
            
        Returns:
            List of dictionaries with file metadata
            
        Raises:
            Exception: If file saving fails
        """
        results = []
        try:
            self._ensure_directory_exists()
            
            for file in files:
                # Save each file and collect metadata
                file_info = await self.save_file(file)
                results.append(file_info)
                
            return results
        except Exception as e:
            logger.error(f"Error saving multiple files: {str(e)}")
            raise
    
    async def check_file_exists(self, filename: str) -> Dict[str, Any]:
        """
        Check if a file exists and get its metadata.
        
        Args:
            filename: Name of the file to check
            
        Returns:
            Dictionary with file metadata and existence status
        """
        try:
            file_path = self.upload_dir / filename
            
            if file_path.exists():
                # Return file metadata
                return {
                    "filename": filename,
                    "path": str(filename),
                    "full_path": str(file_path),
                    "file_size_bytes": os.path.getsize(file_path),
                    "is_uploaded": True,
                    "exists": True
                }
            else:
                return {
                    "filename": filename,
                    "exists": False,
                    "is_uploaded": False
                }
        except Exception as e:
            logger.error(f"Error checking file {filename}: {str(e)}")
            raise
    
    async def list_files(self) -> List[Dict[str, Any]]:
        """
        List all files in the upload directory.
        
        Returns:
            List of dictionaries with file metadata
            
        Raises:
            Exception: If listing files fails
        """
        try:
            self._ensure_directory_exists()
            
            files = []
            # List all files in the directory
            for file_path in self.upload_dir.iterdir():
                if file_path.is_file():
                    files.append({
                        "filename": file_path.name,
                        "path": str(file_path.name),
                        "full_path": str(file_path),
                        "file_size_bytes": os.path.getsize(file_path),
                        "is_uploaded": True
                    })
                    
            return files
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            raise 