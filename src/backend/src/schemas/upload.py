from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class FileInfo(BaseModel):
    """Schema for file information"""
    filename: str = Field(..., description="Name of the file")
    path: str = Field(..., description="Relative path of the file")
    full_path: str = Field(..., description="Full path of the file")
    file_size_bytes: int = Field(..., description="Size of the file in bytes")
    is_uploaded: bool = Field(..., description="Whether the file has been uploaded")


class FileResponse(BaseModel):
    """Schema for file upload response"""
    filename: str = Field(..., description="Name of the file")
    path: str = Field(..., description="Path of the file in storage")
    size: int = Field(..., description="Size of the file in bytes")
    content_type: str = Field(..., description="MIME type of the file")
    upload_timestamp: str = Field(..., description="Timestamp of upload")
    execution_id: Optional[str] = Field(None, description="Execution ID if applicable")
    group_id: Optional[str] = Field(None, description="Group ID if applicable")
    success: bool = Field(default=True, description="Whether the operation was successful")


class FileCheckResponse(FileInfo):
    """Schema for file check response"""
    exists: bool = Field(..., description="Whether the file exists")


class FileCheckNotFoundResponse(BaseModel):
    """Schema for file check response when file not found"""
    filename: str = Field(..., description="Name of the file")
    exists: bool = Field(False, description="Whether the file exists")
    is_uploaded: bool = Field(False, description="Whether the file has been uploaded")


class MultiFileResponse(BaseModel):
    """Schema for multiple files upload response"""
    files: List[FileResponse] = Field(..., description="List of uploaded files")
    failed_files: Optional[List[Dict[str, Any]]] = Field(None, description="List of failed uploads")
    success: bool = Field(default=True, description="Whether the operation was successful")


class FileListResponse(BaseModel):
    """Schema for file list response"""
    files: List[Any] = Field(..., description="List of files")
    count: Optional[int] = Field(None, description="Total count of files")
    success: bool = Field(default=True, description="Whether the operation was successful") 