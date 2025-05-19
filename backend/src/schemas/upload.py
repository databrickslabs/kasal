from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class FileInfo(BaseModel):
    """Schema for file information"""
    filename: str = Field(..., description="Name of the file")
    path: str = Field(..., description="Relative path of the file")
    full_path: str = Field(..., description="Full path of the file")
    file_size_bytes: int = Field(..., description="Size of the file in bytes")
    is_uploaded: bool = Field(..., description="Whether the file has been uploaded")


class FileResponse(FileInfo):
    """Schema for file upload response"""
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
    files: List[FileInfo] = Field(..., description="List of uploaded files")
    success: bool = Field(default=True, description="Whether the operation was successful")


class FileListResponse(BaseModel):
    """Schema for file list response"""
    files: List[FileInfo] = Field(..., description="List of files")
    success: bool = Field(default=True, description="Whether the operation was successful") 