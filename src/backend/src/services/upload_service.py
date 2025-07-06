import os
import logging
from typing import List, Dict, Any
from pathlib import Path
from fastapi import UploadFile, HTTPException, status

from src.repositories.upload_repository import UploadRepository
from src.schemas.upload import FileResponse, MultiFileResponse, FileCheckResponse, FileCheckNotFoundResponse, FileListResponse

logger = logging.getLogger(__name__)

class UploadService:
    """
    Service for file upload business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """
    
    def __init__(self, uploads_dir: Path = None):
        """
        Initialize service with uploads directory.
        
        Args:
            uploads_dir: Directory path where files will be stored, 
                         defaults to environment variable or 'uploads/knowledge'
        """
        if uploads_dir is None:
            uploads_dir = Path(os.environ.get('KNOWLEDGE_DIR', 'uploads/knowledge'))
            
        self.uploads_dir = uploads_dir
        self.repository = UploadRepository(uploads_dir)
    
    async def upload_file(self, file: UploadFile) -> FileResponse:
        """
        Upload a single file.
        
        Args:
            file: FastAPI UploadFile object
            
        Returns:
            FileResponse with file metadata
            
        Raises:
            HTTPException: If file upload fails
        """
        try:
            file_info = await self.repository.save_file(file)
            return FileResponse(**file_info, success=True)
        except Exception as e:
            logger.error(f"Failed to upload file {file.filename}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload file: {str(e)}"
            )
    
    async def upload_multiple_files(self, files: List[UploadFile]) -> MultiFileResponse:
        """
        Upload multiple files.
        
        Args:
            files: List of FastAPI UploadFile objects
            
        Returns:
            MultiFileResponse with file metadata list
            
        Raises:
            HTTPException: If file upload fails
        """
        try:
            file_infos = await self.repository.save_multiple_files(files)
            return MultiFileResponse(files=file_infos, success=True)
        except Exception as e:
            logger.error(f"Failed to upload multiple files: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload files: {str(e)}"
            )
    
    async def check_file(self, filename: str) -> FileCheckResponse | FileCheckNotFoundResponse:
        """
        Check if a file exists and get its metadata.
        
        Args:
            filename: Name of the file to check
            
        Returns:
            FileCheckResponse with file metadata if file exists,
            FileCheckNotFoundResponse if file does not exist
            
        Raises:
            HTTPException: If file check fails
        """
        try:
            file_info = await self.repository.check_file_exists(filename)
            
            if file_info["exists"]:
                return FileCheckResponse(**file_info)
            else:
                return FileCheckNotFoundResponse(**file_info)
        except Exception as e:
            logger.error(f"Failed to check file {filename}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to check file: {str(e)}"
            )
    
    async def list_files(self) -> FileListResponse:
        """
        List all files in the upload directory.
        
        Returns:
            FileListResponse with file metadata list
            
        Raises:
            HTTPException: If file listing fails
        """
        try:
            files = await self.repository.list_files()
            return FileListResponse(files=files, success=True)
        except Exception as e:
            logger.error(f"Failed to list files: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list files: {str(e)}"
            ) 