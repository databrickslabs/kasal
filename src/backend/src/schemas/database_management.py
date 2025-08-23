"""
Pydantic schemas for database management operations.
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class ExportRequest(BaseModel):
    """Request model for database export."""
    catalog: str = Field(default="users", description="Databricks catalog name")
    schema_name: str = Field(default="default", description="Databricks schema name", alias="schema")
    volume_name: str = Field(default="kasal_backups", description="Volume name for backups")
    export_format: str = Field(default="sql", description="Export format: 'sql' (SQL dump) or 'sqlite' (SQLite DB)")
    
    @field_validator('catalog', 'schema_name', 'volume_name')
    @classmethod
    def validate_names(cls, v: str) -> str:
        """Validate catalog, schema, and volume names."""
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid name: contains illegal characters")
        return v.strip()
    
    @field_validator('export_format')
    @classmethod
    def validate_format(cls, v: str) -> str:
        """Validate export format."""
        valid_formats = ['sql', 'sqlite']
        if v.lower() not in valid_formats:
            raise ValueError(f"Invalid export format. Must be one of: {', '.join(valid_formats)}")
        return v.lower()


class ImportRequest(BaseModel):
    """Request model for database import."""
    catalog: str = Field(..., description="Databricks catalog name")
    schema_name: str = Field(..., description="Databricks schema name", alias="schema")
    volume_name: str = Field(..., description="Volume name containing backups")
    backup_filename: str = Field(..., description="Name of the backup file to import")
    
    @field_validator('catalog', 'schema_name', 'volume_name')
    @classmethod
    def validate_names(cls, v: str) -> str:
        """Validate catalog, schema, and volume names."""
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid name: contains illegal characters")
        return v.strip()
    
    @field_validator('backup_filename')
    @classmethod
    def validate_filename(cls, v: str) -> str:
        """Validate backup filename."""
        if not v or not v.strip():
            raise ValueError("Filename cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid filename: contains illegal characters")
        # Validate file extension
        if not (v.endswith('.db') or v.endswith('.json') or v.endswith('.sql')):
            raise ValueError("Invalid backup file extension. Must be .db, .json, or .sql")
        return v.strip()


class ListBackupsRequest(BaseModel):
    """Request model for listing backups."""
    catalog: str = Field(default="users", description="Databricks catalog name")
    schema_name: str = Field(default="default", description="Databricks schema name", alias="schema")
    volume_name: str = Field(default="kasal_backups", description="Volume name containing backups")
    
    @field_validator('catalog', 'schema_name', 'volume_name')
    @classmethod
    def validate_names(cls, v: str) -> str:
        """Validate catalog, schema, and volume names."""
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid name: contains illegal characters")
        return v.strip()


class BackupInfo(BaseModel):
    """Model for backup file information."""
    filename: str
    size_mb: float
    created_at: str
    databricks_url: Optional[str] = None  # Optional, only used in list endpoint
    backup_type: str = Field(default="unknown", description="Type of backup (sqlite, postgres_json, postgres_sql)")


class ExportResponse(BaseModel):
    """Response model for database export."""
    success: bool
    backup_path: Optional[str] = None
    backup_filename: Optional[str] = None
    volume_path: Optional[str] = None
    volume_browse_url: Optional[str] = None  # URL to browse the entire volume
    export_files: Optional[List[BackupInfo]] = None  # List of all backup files in the volume
    size_mb: Optional[float] = None
    original_size_mb: Optional[float] = None
    timestamp: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    volume: Optional[str] = None
    database_type: Optional[str] = None
    error: Optional[str] = None


class ImportResponse(BaseModel):
    """Response model for database import."""
    success: bool
    imported_from: Optional[str] = None
    backup_filename: Optional[str] = None
    volume_path: Optional[str] = None
    size_mb: Optional[float] = None
    timestamp: Optional[str] = None
    database_type: Optional[str] = None
    restored_tables: Optional[List[str]] = None
    error: Optional[str] = None


class ListBackupsResponse(BaseModel):
    """Response model for listing backups."""
    success: bool
    backups: Optional[List[BackupInfo]] = None
    volume_path: Optional[str] = None
    total_backups: Optional[int] = None
    error: Optional[str] = None


class MemoryBackendInfo(BaseModel):
    """Model for memory backend information."""
    id: str
    name: str
    backend_type: str
    is_default: bool
    created_at: str
    group_id: Optional[str] = None


class DatabaseInfoResponse(BaseModel):
    """Response model for database information."""
    success: bool
    database_type: Optional[str] = None
    database_path: Optional[str] = None
    size_mb: Optional[float] = None
    created_at: Optional[str] = None
    modified_at: Optional[str] = None
    tables: Optional[Dict[str, int]] = None
    total_tables: Optional[int] = None
    memory_backends: Optional[List[MemoryBackendInfo]] = None
    error: Optional[str] = None


class DeleteBackupRequest(BaseModel):
    """Request model for deleting a backup."""
    catalog: str = Field(..., description="Databricks catalog name")
    schema_name: str = Field(..., description="Databricks schema name", alias="schema")
    volume_name: str = Field(..., description="Volume name containing backups")
    backup_filename: str = Field(..., description="Name of the backup file to delete")
    
    @field_validator('catalog', 'schema_name', 'volume_name')
    @classmethod
    def validate_names(cls, v: str) -> str:
        """Validate catalog, schema, and volume names."""
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid name: contains illegal characters")
        return v.strip()
    
    @field_validator('backup_filename')
    @classmethod
    def validate_filename(cls, v: str) -> str:
        """Validate backup filename."""
        if not v or not v.strip():
            raise ValueError("Filename cannot be empty")
        # Validate no path traversal attempts
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid filename: contains illegal characters")
        return v.strip()


class DeleteBackupResponse(BaseModel):
    """Response model for deleting a backup."""
    success: bool
    message: Optional[str] = None
    error: Optional[str] = None