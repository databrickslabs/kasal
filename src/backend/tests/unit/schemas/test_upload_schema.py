"""
Unit tests for upload schemas.

Tests the functionality of Pydantic schemas for file upload operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError
from typing import List

from src.schemas.upload import (
    FileInfo, FileResponse, FileCheckResponse, FileCheckNotFoundResponse,
    MultiFileResponse, FileListResponse
)


class TestFileInfo:
    """Test cases for FileInfo schema."""
    
    def test_valid_file_info(self):
        """Test FileInfo with all required fields."""
        file_data = {
            "filename": "test_document.pdf",
            "path": "uploads/documents/test_document.pdf",
            "full_path": "/var/app/uploads/documents/test_document.pdf",
            "file_size_bytes": 1024576,
            "is_uploaded": True
        }
        file_info = FileInfo(**file_data)
        assert file_info.filename == "test_document.pdf"
        assert file_info.path == "uploads/documents/test_document.pdf"
        assert file_info.full_path == "/var/app/uploads/documents/test_document.pdf"
        assert file_info.file_size_bytes == 1024576
        assert file_info.is_uploaded is True
    
    def test_file_info_missing_required_fields(self):
        """Test FileInfo validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            FileInfo(filename="test.txt")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "path" in missing_fields
        assert "full_path" in missing_fields
        assert "file_size_bytes" in missing_fields
        assert "is_uploaded" in missing_fields
    
    def test_file_info_zero_file_size(self):
        """Test FileInfo with zero file size."""
        file_data = {
            "filename": "empty.txt",
            "path": "uploads/empty.txt",
            "full_path": "/var/app/uploads/empty.txt",
            "file_size_bytes": 0,
            "is_uploaded": True
        }
        file_info = FileInfo(**file_data)
        assert file_info.file_size_bytes == 0
    
    def test_file_info_large_file_size(self):
        """Test FileInfo with large file size."""
        file_data = {
            "filename": "large_file.zip",
            "path": "uploads/large_file.zip",
            "full_path": "/var/app/uploads/large_file.zip",
            "file_size_bytes": 5368709120,  # 5GB
            "is_uploaded": False
        }
        file_info = FileInfo(**file_data)
        assert file_info.file_size_bytes == 5368709120
        assert file_info.is_uploaded is False
    
    def test_file_info_various_file_types(self):
        """Test FileInfo with various file types and paths."""
        file_scenarios = [
            {
                "filename": "image.jpg",
                "path": "uploads/images/image.jpg",
                "full_path": "/var/app/uploads/images/image.jpg",
                "file_size_bytes": 2048000,
                "is_uploaded": True
            },
            {
                "filename": "data.csv",
                "path": "uploads/data/data.csv",
                "full_path": "/var/app/uploads/data/data.csv",
                "file_size_bytes": 512000,
                "is_uploaded": True
            },
            {
                "filename": "archive.tar.gz",
                "path": "uploads/archives/archive.tar.gz",
                "full_path": "/var/app/uploads/archives/archive.tar.gz",
                "file_size_bytes": 10485760,
                "is_uploaded": False
            },
            {
                "filename": "presentation.pptx",
                "path": "uploads/presentations/presentation.pptx",
                "full_path": "/var/app/uploads/presentations/presentation.pptx",
                "file_size_bytes": 15728640,
                "is_uploaded": True
            }
        ]
        
        for scenario in file_scenarios:
            file_info = FileInfo(**scenario)
            assert file_info.filename == scenario["filename"]
            assert file_info.path == scenario["path"]
            assert file_info.full_path == scenario["full_path"]
    
    def test_file_info_boolean_conversion(self):
        """Test FileInfo boolean field conversion."""
        file_data = {
            "filename": "bool_test.txt",
            "path": "uploads/bool_test.txt",
            "full_path": "/var/app/uploads/bool_test.txt",
            "file_size_bytes": 100,
            "is_uploaded": "true"
        }
        file_info = FileInfo(**file_data)
        assert file_info.is_uploaded is True
        
        file_data["is_uploaded"] = 0
        file_info = FileInfo(**file_data)
        assert file_info.is_uploaded is False
        
        file_data["is_uploaded"] = 1
        file_info = FileInfo(**file_data)
        assert file_info.is_uploaded is True


class TestFileResponse:
    """Test cases for FileResponse schema."""
    
    def test_valid_file_response(self):
        """Test FileResponse with all fields."""
        response_data = {
            "filename": "response_test.pdf",
            "path": "uploads/response_test.pdf",
            "full_path": "/var/app/uploads/response_test.pdf",
            "file_size_bytes": 2048000,
            "is_uploaded": True,
            "success": True
        }
        response = FileResponse(**response_data)
        assert response.filename == "response_test.pdf"
        assert response.path == "uploads/response_test.pdf"
        assert response.full_path == "/var/app/uploads/response_test.pdf"
        assert response.file_size_bytes == 2048000
        assert response.is_uploaded is True
        assert response.success is True
    
    def test_file_response_inheritance(self):
        """Test that FileResponse inherits from FileInfo."""
        response_data = {
            "filename": "inherit_test.txt",
            "path": "uploads/inherit_test.txt",
            "full_path": "/var/app/uploads/inherit_test.txt",
            "file_size_bytes": 512,
            "is_uploaded": True
        }
        response = FileResponse(**response_data)
        
        # Should have all FileInfo attributes
        assert hasattr(response, 'filename')
        assert hasattr(response, 'path')
        assert hasattr(response, 'full_path')
        assert hasattr(response, 'file_size_bytes')
        assert hasattr(response, 'is_uploaded')
        
        # Should have FileResponse-specific attributes
        assert hasattr(response, 'success')
        
        # Should behave like base class with default success
        assert response.filename == "inherit_test.txt"
        assert response.success is True  # Default value
    
    def test_file_response_failure_scenario(self):
        """Test FileResponse for failed upload scenario."""
        response_data = {
            "filename": "failed_upload.txt",
            "path": "uploads/failed_upload.txt",
            "full_path": "/var/app/uploads/failed_upload.txt",
            "file_size_bytes": 1024,
            "is_uploaded": False,
            "success": False
        }
        response = FileResponse(**response_data)
        assert response.is_uploaded is False
        assert response.success is False


class TestFileCheckResponse:
    """Test cases for FileCheckResponse schema."""
    
    def test_valid_file_check_response_exists(self):
        """Test FileCheckResponse when file exists."""
        check_data = {
            "filename": "existing_file.txt",
            "path": "uploads/existing_file.txt",
            "full_path": "/var/app/uploads/existing_file.txt",
            "file_size_bytes": 2048,
            "is_uploaded": True,
            "exists": True
        }
        check_response = FileCheckResponse(**check_data)
        assert check_response.filename == "existing_file.txt"
        assert check_response.exists is True
        assert check_response.is_uploaded is True
    
    def test_file_check_response_inheritance(self):
        """Test that FileCheckResponse inherits from FileInfo."""
        check_data = {
            "filename": "check_inherit.txt",
            "path": "uploads/check_inherit.txt",
            "full_path": "/var/app/uploads/check_inherit.txt",
            "file_size_bytes": 1024,
            "is_uploaded": False,
            "exists": False
        }
        check_response = FileCheckResponse(**check_data)
        
        # Should have all FileInfo attributes
        assert hasattr(check_response, 'filename')
        assert hasattr(check_response, 'path')
        assert hasattr(check_response, 'full_path')
        assert hasattr(check_response, 'file_size_bytes')
        assert hasattr(check_response, 'is_uploaded')
        
        # Should have FileCheckResponse-specific attributes
        assert hasattr(check_response, 'exists')
        
        assert check_response.exists is False
        assert check_response.is_uploaded is False
    
    def test_file_check_response_missing_exists(self):
        """Test FileCheckResponse validation with missing exists field."""
        with pytest.raises(ValidationError) as exc_info:
            FileCheckResponse(
                filename="test.txt",
                path="uploads/test.txt",
                full_path="/var/app/uploads/test.txt",
                file_size_bytes=100,
                is_uploaded=True
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "exists" in missing_fields


class TestFileCheckNotFoundResponse:
    """Test cases for FileCheckNotFoundResponse schema."""
    
    def test_valid_file_check_not_found_response(self):
        """Test FileCheckNotFoundResponse with required fields."""
        not_found_data = {
            "filename": "missing_file.txt"
        }
        not_found_response = FileCheckNotFoundResponse(**not_found_data)
        assert not_found_response.filename == "missing_file.txt"
        assert not_found_response.exists is False  # Default value
        assert not_found_response.is_uploaded is False  # Default value
    
    def test_file_check_not_found_response_explicit_values(self):
        """Test FileCheckNotFoundResponse with explicit field values."""
        not_found_data = {
            "filename": "explicit_missing.txt",
            "exists": False,
            "is_uploaded": False
        }
        not_found_response = FileCheckNotFoundResponse(**not_found_data)
        assert not_found_response.filename == "explicit_missing.txt"
        assert not_found_response.exists is False
        assert not_found_response.is_uploaded is False
    
    def test_file_check_not_found_response_missing_filename(self):
        """Test FileCheckNotFoundResponse validation with missing filename."""
        with pytest.raises(ValidationError) as exc_info:
            FileCheckNotFoundResponse()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "filename" in missing_fields
    
    def test_file_check_not_found_response_overridden_defaults(self):
        """Test FileCheckNotFoundResponse when trying to override defaults."""
        # Even if we try to set exists=True, it should work (no constraint preventing it)
        not_found_data = {
            "filename": "contradictory.txt",
            "exists": True,
            "is_uploaded": True
        }
        not_found_response = FileCheckNotFoundResponse(**not_found_data)
        assert not_found_response.exists is True
        assert not_found_response.is_uploaded is True


class TestMultiFileResponse:
    """Test cases for MultiFileResponse schema."""
    
    def test_valid_multi_file_response(self):
        """Test MultiFileResponse with multiple files."""
        files = [
            FileInfo(
                filename="file1.txt",
                path="uploads/file1.txt",
                full_path="/var/app/uploads/file1.txt",
                file_size_bytes=1024,
                is_uploaded=True
            ),
            FileInfo(
                filename="file2.pdf",
                path="uploads/file2.pdf",
                full_path="/var/app/uploads/file2.pdf",
                file_size_bytes=2048,
                is_uploaded=True
            )
        ]
        
        multi_response_data = {
            "files": files,
            "success": True
        }
        multi_response = MultiFileResponse(**multi_response_data)
        assert len(multi_response.files) == 2
        assert multi_response.success is True
        assert multi_response.files[0].filename == "file1.txt"
        assert multi_response.files[1].filename == "file2.pdf"
    
    def test_multi_file_response_empty_files(self):
        """Test MultiFileResponse with empty file list."""
        multi_response_data = {
            "files": [],
            "success": True
        }
        multi_response = MultiFileResponse(**multi_response_data)
        assert len(multi_response.files) == 0
        assert multi_response.success is True
    
    def test_multi_file_response_default_success(self):
        """Test MultiFileResponse with default success value."""
        files = [
            FileInfo(
                filename="default_success.txt",
                path="uploads/default_success.txt",
                full_path="/var/app/uploads/default_success.txt",
                file_size_bytes=512,
                is_uploaded=True
            )
        ]
        
        multi_response_data = {"files": files}
        multi_response = MultiFileResponse(**multi_response_data)
        assert len(multi_response.files) == 1
        assert multi_response.success is True  # Default value
    
    def test_multi_file_response_failure_scenario(self):
        """Test MultiFileResponse for failed multi-upload scenario."""
        files = [
            FileInfo(
                filename="partial1.txt",
                path="uploads/partial1.txt",
                full_path="/var/app/uploads/partial1.txt",
                file_size_bytes=1024,
                is_uploaded=True
            ),
            FileInfo(
                filename="partial2.txt",
                path="uploads/partial2.txt",
                full_path="/var/app/uploads/partial2.txt",
                file_size_bytes=2048,
                is_uploaded=False
            )
        ]
        
        multi_response_data = {
            "files": files,
            "success": False
        }
        multi_response = MultiFileResponse(**multi_response_data)
        assert len(multi_response.files) == 2
        assert multi_response.success is False
        assert multi_response.files[0].is_uploaded is True
        assert multi_response.files[1].is_uploaded is False
    
    def test_multi_file_response_missing_files(self):
        """Test MultiFileResponse validation with missing files field."""
        with pytest.raises(ValidationError) as exc_info:
            MultiFileResponse()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "files" in missing_fields
    
    def test_multi_file_response_with_file_dicts(self):
        """Test MultiFileResponse creation with file dicts."""
        multi_response_data = {
            "files": [
                {
                    "filename": "dict_file.txt",
                    "path": "uploads/dict_file.txt",
                    "full_path": "/var/app/uploads/dict_file.txt",
                    "file_size_bytes": 256,
                    "is_uploaded": True
                }
            ],
            "success": True
        }
        multi_response = MultiFileResponse(**multi_response_data)
        assert len(multi_response.files) == 1
        assert isinstance(multi_response.files[0], FileInfo)
        assert multi_response.files[0].filename == "dict_file.txt"


class TestFileListResponse:
    """Test cases for FileListResponse schema."""
    
    def test_valid_file_list_response(self):
        """Test FileListResponse with file list."""
        files = [
            FileInfo(
                filename="list_file1.txt",
                path="uploads/list_file1.txt",
                full_path="/var/app/uploads/list_file1.txt",
                file_size_bytes=1024,
                is_uploaded=True
            ),
            FileInfo(
                filename="list_file2.jpg",
                path="uploads/images/list_file2.jpg",
                full_path="/var/app/uploads/images/list_file2.jpg",
                file_size_bytes=512000,
                is_uploaded=True
            )
        ]
        
        list_response_data = {
            "files": files,
            "success": True
        }
        list_response = FileListResponse(**list_response_data)
        assert len(list_response.files) == 2
        assert list_response.success is True
        assert list_response.files[0].filename == "list_file1.txt"
        assert list_response.files[1].filename == "list_file2.jpg"
    
    def test_file_list_response_empty(self):
        """Test FileListResponse with empty file list."""
        list_response_data = {
            "files": [],
            "success": True
        }
        list_response = FileListResponse(**list_response_data)
        assert len(list_response.files) == 0
        assert list_response.success is True
    
    def test_file_list_response_default_success(self):
        """Test FileListResponse with default success value."""
        files = [
            FileInfo(
                filename="default_list.txt",
                path="uploads/default_list.txt",
                full_path="/var/app/uploads/default_list.txt",
                file_size_bytes=128,
                is_uploaded=False
            )
        ]
        
        list_response_data = {"files": files}
        list_response = FileListResponse(**list_response_data)
        assert len(list_response.files) == 1
        assert list_response.success is True  # Default value
    
    def test_file_list_response_mixed_upload_states(self):
        """Test FileListResponse with mixed upload states."""
        files = [
            FileInfo(
                filename="uploaded.txt",
                path="uploads/uploaded.txt",
                full_path="/var/app/uploads/uploaded.txt",
                file_size_bytes=1024,
                is_uploaded=True
            ),
            FileInfo(
                filename="pending.txt",
                path="uploads/pending.txt",
                full_path="/var/app/uploads/pending.txt",
                file_size_bytes=2048,
                is_uploaded=False
            ),
            FileInfo(
                filename="completed.pdf",
                path="uploads/completed.pdf",
                full_path="/var/app/uploads/completed.pdf",
                file_size_bytes=4096,
                is_uploaded=True
            )
        ]
        
        list_response_data = {
            "files": files,
            "success": True
        }
        list_response = FileListResponse(**list_response_data)
        
        uploaded_files = [f for f in list_response.files if f.is_uploaded]
        pending_files = [f for f in list_response.files if not f.is_uploaded]
        
        assert len(uploaded_files) == 2
        assert len(pending_files) == 1
        assert uploaded_files[0].filename == "uploaded.txt"
        assert pending_files[0].filename == "pending.txt"


class TestSchemaIntegration:
    """Integration tests for upload schema interactions."""
    
    def test_file_upload_workflow(self):
        """Test complete file upload workflow."""
        # Initial file info
        file_info = FileInfo(
            filename="workflow_test.docx",
            path="uploads/documents/workflow_test.docx",
            full_path="/var/app/uploads/documents/workflow_test.docx",
            file_size_bytes=1048576,
            is_uploaded=False
        )
        
        # Check file before upload (not found)
        check_not_found = FileCheckNotFoundResponse(
            filename="workflow_test.docx"
        )
        
        # File upload response (successful)
        upload_response = FileResponse(
            filename=file_info.filename,
            path=file_info.path,
            full_path=file_info.full_path,
            file_size_bytes=file_info.file_size_bytes,
            is_uploaded=True,  # Now uploaded
            success=True
        )
        
        # Check file after upload (exists)
        check_exists = FileCheckResponse(
            filename=upload_response.filename,
            path=upload_response.path,
            full_path=upload_response.full_path,
            file_size_bytes=upload_response.file_size_bytes,
            is_uploaded=upload_response.is_uploaded,
            exists=True
        )
        
        # Verify workflow
        assert file_info.is_uploaded is False
        assert check_not_found.exists is False
        assert upload_response.success is True
        assert upload_response.is_uploaded is True
        assert check_exists.exists is True
        assert check_exists.filename == file_info.filename
    
    def test_multi_file_upload_scenarios(self):
        """Test various multi-file upload scenarios."""
        # Successful multi-upload
        successful_files = [
            FileInfo(
                filename=f"success_{i}.txt",
                path=f"uploads/success_{i}.txt",
                full_path=f"/var/app/uploads/success_{i}.txt",
                file_size_bytes=1024 * i,
                is_uploaded=True
            )
            for i in range(1, 4)
        ]
        
        successful_response = MultiFileResponse(
            files=successful_files,
            success=True
        )
        assert len(successful_response.files) == 3
        assert all(f.is_uploaded for f in successful_response.files)
        assert successful_response.success is True
        
        # Partial failure scenario
        mixed_files = [
            FileInfo(
                filename="partial_success_1.txt",
                path="uploads/partial_success_1.txt",
                full_path="/var/app/uploads/partial_success_1.txt",
                file_size_bytes=1024,
                is_uploaded=True
            ),
            FileInfo(
                filename="partial_failure_2.txt",
                path="uploads/partial_failure_2.txt",
                full_path="/var/app/uploads/partial_failure_2.txt",
                file_size_bytes=2048,
                is_uploaded=False
            )
        ]
        
        partial_response = MultiFileResponse(
            files=mixed_files,
            success=False
        )
        assert len(partial_response.files) == 2
        assert partial_response.files[0].is_uploaded is True
        assert partial_response.files[1].is_uploaded is False
        assert partial_response.success is False
    
    def test_file_listing_scenarios(self):
        """Test different file listing scenarios."""
        # Empty directory
        empty_list = FileListResponse(files=[], success=True)
        assert len(empty_list.files) == 0
        
        # Directory with various file types
        diverse_files = [
            FileInfo(
                filename="document.pdf",
                path="uploads/documents/document.pdf",
                full_path="/var/app/uploads/documents/document.pdf",
                file_size_bytes=2097152,
                is_uploaded=True
            ),
            FileInfo(
                filename="image.png",
                path="uploads/images/image.png",
                full_path="/var/app/uploads/images/image.png",
                file_size_bytes=524288,
                is_uploaded=True
            ),
            FileInfo(
                filename="data.csv",
                path="uploads/data/data.csv",
                full_path="/var/app/uploads/data/data.csv",
                file_size_bytes=1048576,
                is_uploaded=False
            )
        ]
        
        diverse_list = FileListResponse(files=diverse_files, success=True)
        assert len(diverse_list.files) == 3
        
        # Group by upload status
        uploaded = [f for f in diverse_list.files if f.is_uploaded]
        pending = [f for f in diverse_list.files if not f.is_uploaded]
        
        assert len(uploaded) == 2
        assert len(pending) == 1
        assert uploaded[0].filename in ["document.pdf", "image.png"]
        assert pending[0].filename == "data.csv"
        
        # Calculate total size
        total_size = sum(f.file_size_bytes for f in diverse_list.files)
        assert total_size == 2097152 + 524288 + 1048576  # Sum of all file sizes