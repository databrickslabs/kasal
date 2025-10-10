"""
Comprehensive unit tests for Database Management schemas.

Tests all Pydantic models for validation, serialization, and edge cases.
"""
import pytest
from typing import Dict, Any, List, Optional
from pydantic import ValidationError

from src.schemas.database_management import (
    ExportRequest,
    ImportRequest,
    ListBackupsRequest,
    BackupInfo,
    ExportResponse,
    ImportResponse,
    ListBackupsResponse,
    MemoryBackendInfo,
    DatabaseInfoResponse,
    DeleteBackupRequest,
    DeleteBackupResponse
)


class TestExportRequest:
    """Test ExportRequest schema."""

    def test_export_request_defaults(self):
        """Test ExportRequest with default values."""
        request = ExportRequest()

        assert request.catalog == "users"
        assert request.schema_name == "default"
        assert request.volume_name == "kasal_backups"
        assert request.export_format == "sqlite"

    def test_export_request_validate_names_empty_catalog(self):
        """Test ExportRequest validation with empty catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="")
        assert "Name cannot be empty" in str(exc_info.value)

    def test_export_request_validate_names_whitespace_catalog(self):
        """Test ExportRequest validation with whitespace-only catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="   ")
        assert "Name cannot be empty" in str(exc_info.value)

    def test_export_request_validate_names_path_traversal_catalog(self):
        """Test ExportRequest validation with path traversal in catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="../malicious")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_export_request_validate_names_slash_in_catalog(self):
        """Test ExportRequest validation with slash in catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="test/catalog")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_export_request_validate_names_backslash_in_catalog(self):
        """Test ExportRequest validation with backslash in catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="test\\catalog")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_export_request_validate_names_empty_schema(self):
        """Test ExportRequest validation with empty schema."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(schema="")
        assert "Name cannot be empty" in str(exc_info.value)

    def test_export_request_validate_names_empty_volume(self):
        """Test ExportRequest validation with empty volume."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(volume_name="")
        assert "Name cannot be empty" in str(exc_info.value)



    def test_export_request_custom_values(self):
        """Test ExportRequest with custom values."""
        request = ExportRequest(
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            export_format="sql"
        )

        assert request.catalog == "test_catalog"
        assert request.schema_name == "test_schema"
        assert request.volume_name == "test_volume"
        assert request.export_format == "sql"

    def test_export_request_schema_alias(self):
        """Test ExportRequest with schema alias."""
        request = ExportRequest(schema="test_schema")
        
        assert request.schema_name == "test_schema"

    def test_export_request_invalid_catalog_empty(self):
        """Test ExportRequest validation error for empty catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="")
        
        assert "Name cannot be empty" in str(exc_info.value)

    def test_export_request_invalid_catalog_path_traversal(self):
        """Test ExportRequest validation error for path traversal in catalog."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(catalog="../test")
        
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_export_request_invalid_export_format(self):
        """Test ExportRequest validation error for invalid export format."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(export_format="invalid")
        
        assert "Invalid export format" in str(exc_info.value)

    def test_export_request_export_format_case_insensitive(self):
        """Test ExportRequest export format is case insensitive."""
        request = ExportRequest(export_format="SQL")
        
        assert request.export_format == "sql"


class TestImportRequest:
    """Test ImportRequest schema."""

    def test_import_request_valid(self):
        """Test ImportRequest with valid data."""
        request = ImportRequest(
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        assert request.catalog == "test_catalog"
        assert request.schema_name == "test_schema"
        assert request.volume_name == "test_volume"
        assert request.backup_filename == "backup.db"

    def test_import_request_schema_alias(self):
        """Test ImportRequest with schema alias."""
        request = ImportRequest(
            catalog="test",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.json"
        )
        
        assert request.schema_name == "test_schema"

    def test_import_request_missing_required_fields(self):
        """Test ImportRequest validation error when required fields missing."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest()
        
        error_str = str(exc_info.value)
        assert "catalog" in error_str
        assert "schema" in error_str
        assert "volume_name" in error_str
        assert "backup_filename" in error_str

    def test_import_request_invalid_filename_extension(self):
        """Test ImportRequest validation error for invalid filename extension."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="backup.txt"
            )
        
        assert "Invalid backup file extension" in str(exc_info.value)

    def test_import_request_validate_filename_empty(self):
        """Test ImportRequest validation with empty backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename=""
            )
        assert "Filename cannot be empty" in str(exc_info.value)

    def test_import_request_validate_filename_whitespace(self):
        """Test ImportRequest validation with whitespace-only backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="   "
            )
        assert "Filename cannot be empty" in str(exc_info.value)

    def test_import_request_validate_filename_path_traversal(self):
        """Test ImportRequest validation with path traversal in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="../malicious.db"
            )
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)

    def test_import_request_validate_filename_slash(self):
        """Test ImportRequest validation with slash in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="path/file.db"
            )
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)

    def test_import_request_validate_filename_backslash(self):
        """Test ImportRequest validation with backslash in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="path\\file.db"
            )
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)

    def test_import_request_valid_filename_extensions(self):
        """Test ImportRequest with valid filename extensions."""
        valid_extensions = [".db", ".json", ".sql"]
        
        for ext in valid_extensions:
            request = ImportRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename=f"backup{ext}"
            )
            assert request.backup_filename == f"backup{ext}"


class TestListBackupsRequest:
    """Test ListBackupsRequest schema."""

    def test_list_backups_request_defaults(self):
        """Test ListBackupsRequest with default values."""
        request = ListBackupsRequest()
        
        assert request.catalog == "users"
        assert request.schema_name == "default"
        assert request.volume_name == "kasal_backups"

    def test_list_backups_request_custom_values(self):
        """Test ListBackupsRequest with custom values."""
        request = ListBackupsRequest(
            catalog="custom_catalog",
            schema="custom_schema",
            volume_name="custom_volume"
        )

        assert request.catalog == "custom_catalog"
        assert request.schema_name == "custom_schema"
        assert request.volume_name == "custom_volume"

    def test_list_backups_request_invalid_names(self):
        """Test ListBackupsRequest validation error for invalid names."""
        with pytest.raises(ValidationError) as exc_info:
            ListBackupsRequest(catalog="test/path")
        
        assert "Invalid name: contains illegal characters" in str(exc_info.value)


class TestBackupInfo:
    """Test BackupInfo schema."""

    def test_backup_info_minimal(self):
        """Test BackupInfo with minimal data."""
        backup = BackupInfo(
            filename="backup.db",
            size_mb=10.5,
            created_at="2023-01-01T00:00:00Z"
        )
        
        assert backup.filename == "backup.db"
        assert backup.size_mb == 10.5
        assert backup.created_at == "2023-01-01T00:00:00Z"
        assert backup.databricks_url is None
        assert backup.backup_type == "unknown"

    def test_backup_info_full(self):
        """Test BackupInfo with all fields."""
        backup = BackupInfo(
            filename="backup.db",
            size_mb=10.5,
            created_at="2023-01-01T00:00:00Z",
            databricks_url="https://databricks.com/volume/path",
            backup_type="sqlite"
        )
        
        assert backup.filename == "backup.db"
        assert backup.size_mb == 10.5
        assert backup.created_at == "2023-01-01T00:00:00Z"
        assert backup.databricks_url == "https://databricks.com/volume/path"
        assert backup.backup_type == "sqlite"


class TestExportResponse:
    """Test ExportResponse schema."""

    def test_export_response_minimal(self):
        """Test ExportResponse with minimal data."""
        response = ExportResponse(success=True)
        
        assert response.success is True
        assert response.backup_path is None
        assert response.error is None

    def test_export_response_success_full(self):
        """Test ExportResponse with success data."""
        response = ExportResponse(
            success=True,
            backup_path="/volume/path/backup.db",
            backup_filename="backup.db",
            size_mb=15.2,
            timestamp="2023-01-01T00:00:00Z",
            catalog="test_catalog",
            schema="test_schema"
        )
        
        assert response.success is True
        assert response.backup_path == "/volume/path/backup.db"
        assert response.backup_filename == "backup.db"
        assert response.size_mb == 15.2
        assert response.timestamp == "2023-01-01T00:00:00Z"
        assert response.catalog == "test_catalog"
        assert response.schema_name == "test_schema"

    def test_export_response_error(self):
        """Test ExportResponse with error."""
        response = ExportResponse(success=False, error="Export failed")
        
        assert response.success is False
        assert response.error == "Export failed"


class TestImportResponse:
    """Test ImportResponse schema."""

    def test_import_response_minimal(self):
        """Test ImportResponse with minimal data."""
        response = ImportResponse(success=True)
        
        assert response.success is True
        assert response.imported_from is None
        assert response.error is None

    def test_import_response_success_full(self):
        """Test ImportResponse with success data."""
        response = ImportResponse(
            success=True,
            imported_from="/volume/path/backup.db",
            backup_filename="backup.db",
            size_mb=20.5,
            timestamp="2023-01-01T00:00:00Z",
            database_type="sqlite",
            restored_tables=["table1", "table2"]
        )
        
        assert response.success is True
        assert response.imported_from == "/volume/path/backup.db"
        assert response.backup_filename == "backup.db"
        assert response.size_mb == 20.5
        assert response.timestamp == "2023-01-01T00:00:00Z"
        assert response.database_type == "sqlite"
        assert response.restored_tables == ["table1", "table2"]

    def test_import_response_error(self):
        """Test ImportResponse with error."""
        response = ImportResponse(success=False, error="Import failed")
        
        assert response.success is False
        assert response.error == "Import failed"


class TestListBackupsResponse:
    """Test ListBackupsResponse schema."""

    def test_list_backups_response_minimal(self):
        """Test ListBackupsResponse with minimal data."""
        response = ListBackupsResponse(success=True)
        
        assert response.success is True
        assert response.backups is None
        assert response.error is None

    def test_list_backups_response_with_backups(self):
        """Test ListBackupsResponse with backup list."""
        backups = [
            BackupInfo(filename="backup1.db", size_mb=10.0, created_at="2023-01-01T00:00:00Z"),
            BackupInfo(filename="backup2.db", size_mb=15.0, created_at="2023-01-02T00:00:00Z")
        ]
        response = ListBackupsResponse(
            success=True,
            backups=backups,
            total_backups=2,
            volume_path="/volume/path"
        )
        
        assert response.success is True
        assert len(response.backups) == 2
        assert response.total_backups == 2
        assert response.volume_path == "/volume/path"


class TestMemoryBackendInfo:
    """Test MemoryBackendInfo schema."""

    def test_memory_backend_info_minimal(self):
        """Test MemoryBackendInfo with minimal data."""
        backend = MemoryBackendInfo(
            id="backend-1",
            name="Test Backend",
            backend_type="chroma",
            is_default=True,
            created_at="2023-01-01T00:00:00Z"
        )
        
        assert backend.id == "backend-1"
        assert backend.name == "Test Backend"
        assert backend.backend_type == "chroma"
        assert backend.is_default is True
        assert backend.created_at == "2023-01-01T00:00:00Z"
        assert backend.group_id is None

    def test_memory_backend_info_with_group(self):
        """Test MemoryBackendInfo with group_id."""
        backend = MemoryBackendInfo(
            id="backend-1",
            name="Test Backend",
            backend_type="databricks",
            is_default=False,
            created_at="2023-01-01T00:00:00Z",
            group_id="group-123"
        )
        
        assert backend.group_id == "group-123"


class TestDeleteBackupRequest:
    """Test DeleteBackupRequest schema."""

    def test_delete_backup_request_valid(self):
        """Test DeleteBackupRequest with valid data."""
        request = DeleteBackupRequest(
            catalog="test_catalog",
            schema="test_schema",
            volume_name="test_volume",
            backup_filename="backup.db"
        )

        assert request.catalog == "test_catalog"
        assert request.schema_name == "test_schema"
        assert request.volume_name == "test_volume"
        assert request.backup_filename == "backup.db"

    def test_delete_backup_request_invalid_filename(self):
        """Test DeleteBackupRequest validation error for invalid filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(
                catalog="test",
                schema="test",
                volume_name="test",
                backup_filename="../backup.db"
            )
        
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)


class TestDeleteBackupResponse:
    """Test DeleteBackupResponse schema."""

    def test_delete_backup_response_success(self):
        """Test DeleteBackupResponse with success."""
        response = DeleteBackupResponse(
            success=True,
            message="Backup deleted successfully"
        )
        
        assert response.success is True
        assert response.message == "Backup deleted successfully"
        assert response.error is None

    def test_delete_backup_response_error(self):
        """Test DeleteBackupResponse with error."""
        response = DeleteBackupResponse(
            success=False,
            error="Failed to delete backup"
        )
        
        assert response.success is False
        assert response.error == "Failed to delete backup"

    def test_delete_backup_request_validate_names_empty_catalog(self):
        """Test DeleteBackupRequest validation with empty catalog."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(catalog="", backup_filename="test.db")
        assert "Name cannot be empty" in str(exc_info.value)

    def test_delete_backup_request_validate_names_whitespace_catalog(self):
        """Test DeleteBackupRequest validation with whitespace-only catalog."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(catalog="   ", backup_filename="test.db")
        assert "Name cannot be empty" in str(exc_info.value)

    def test_delete_backup_request_validate_names_path_traversal_catalog(self):
        """Test DeleteBackupRequest validation with path traversal in catalog."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(catalog="../malicious", backup_filename="test.db")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_delete_backup_request_validate_names_slash_in_schema(self):
        """Test DeleteBackupRequest validation with slash in schema."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(schema="test/schema", backup_filename="test.db")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_delete_backup_request_validate_names_backslash_in_volume(self):
        """Test DeleteBackupRequest validation with backslash in volume."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(volume_name="test\\volume", backup_filename="test.db")
        assert "Invalid name: contains illegal characters" in str(exc_info.value)

    def test_delete_backup_request_validate_filename_empty(self):
        """Test DeleteBackupRequest validation with empty backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(backup_filename="")
        assert "Filename cannot be empty" in str(exc_info.value)

    def test_delete_backup_request_validate_filename_whitespace(self):
        """Test DeleteBackupRequest validation with whitespace-only backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(backup_filename="   ")
        assert "Filename cannot be empty" in str(exc_info.value)

    def test_delete_backup_request_validate_filename_path_traversal(self):
        """Test DeleteBackupRequest validation with path traversal in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(backup_filename="../malicious.db")
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)

    def test_delete_backup_request_validate_filename_slash(self):
        """Test DeleteBackupRequest validation with slash in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(backup_filename="path/file.db")
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)

    def test_delete_backup_request_validate_filename_backslash(self):
        """Test DeleteBackupRequest validation with backslash in backup_filename."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteBackupRequest(backup_filename="path\\file.db")
        assert "Invalid filename: contains illegal characters" in str(exc_info.value)
