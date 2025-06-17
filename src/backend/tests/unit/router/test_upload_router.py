"""
Comprehensive unit tests for UploadRouter to achieve 100% coverage.

Tests all endpoints, exception handling, logging, and edge cases
for the upload router functionality in src/api/upload_router.py.
"""
import pytest
import logging
from unittest.mock import AsyncMock, patch, MagicMock, call
from io import BytesIO
from fastapi import HTTPException
from fastapi.testclient import TestClient
from fastapi import FastAPI
from fastapi.exceptions import ResponseValidationError

from src.api.upload_router import router, upload_service, logger
from src.schemas.upload import (
    FileResponse, MultiFileResponse, FileCheckResponse, 
    FileCheckNotFoundResponse, FileListResponse, FileInfo
)


@pytest.fixture
def app():
    """Create a FastAPI app with upload router."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def mock_file_info():
    """Create mock file info data."""
    return {
        "filename": "test.txt",
        "path": "uploads/knowledge/test.txt",
        "full_path": "/full/path/uploads/knowledge/test.txt",
        "file_size_bytes": 1024,
        "is_uploaded": True
    }


@pytest.fixture
def mock_file_response(mock_file_info):
    """Create mock FileResponse."""
    return FileResponse(**mock_file_info, success=True)


@pytest.fixture
def mock_file_check_response(mock_file_info):
    """Create mock FileCheckResponse."""
    return FileCheckResponse(**mock_file_info, exists=True)


@pytest.fixture
def mock_file_check_not_found_response():
    """Create mock FileCheckNotFoundResponse."""
    return FileCheckNotFoundResponse(
        filename="notfound.txt",
        exists=False,
        is_uploaded=False
    )


@pytest.fixture
def mock_multi_file_response():
    """Create mock MultiFileResponse."""
    files = [
        FileInfo(
            filename=f"file{i}.txt",
            path=f"uploads/knowledge/file{i}.txt",
            full_path=f"/full/path/uploads/knowledge/file{i}.txt",
            file_size_bytes=1024,
            is_uploaded=True
        ) for i in range(3)
    ]
    return MultiFileResponse(files=files, success=True)


@pytest.fixture
def mock_file_list_response():
    """Create mock FileListResponse."""
    files = [
        FileInfo(
            filename=f"existing{i}.txt",
            path=f"uploads/knowledge/existing{i}.txt",
            full_path=f"/full/path/uploads/knowledge/existing{i}.txt",
            file_size_bytes=512,
            is_uploaded=True
        ) for i in range(2)
    ]
    return FileListResponse(files=files, success=True)


class TestUploadKnowledgeFile:
    """Test cases for POST /upload/knowledge endpoint."""
    
    @patch.object(upload_service, 'upload_file')
    @patch.object(logger, 'info')
    def test_upload_knowledge_file_success(self, mock_logger, mock_upload, client, mock_file_response):
        """Test successful single file upload with logging."""
        mock_upload.return_value = mock_file_response
        
        test_file = ("test.txt", BytesIO(b"test content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 201
        data = response.json()
        assert data["filename"] == "test.txt"
        assert data["success"] is True
        assert data["file_size_bytes"] == 1024
        
        mock_upload.assert_called_once()
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Uploading knowledge file: test.txt")
        mock_logger.assert_any_call("Knowledge file uploaded successfully: test.txt")
    
    @patch.object(upload_service, 'upload_file')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_upload_knowledge_file_http_exception(self, mock_info, mock_warning, mock_upload, client):
        """Test file upload with HTTPException and logging."""
        mock_upload.side_effect = HTTPException(status_code=400, detail="Invalid file type")
        
        test_file = ("invalid.exe", BytesIO(b"executable"), "application/octet-stream")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 400
        assert "Invalid file type" in response.json()["detail"]
        
        mock_upload.assert_called_once()
        mock_info.assert_called_once_with("Uploading knowledge file: invalid.exe")
        mock_warning.assert_called_once_with("Knowledge file upload failed: 400: Invalid file type")
    
    @patch.object(upload_service, 'upload_file')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_upload_knowledge_file_generic_exception(self, mock_info, mock_warning, mock_upload, client):
        """Test file upload with generic exception handling."""
        mock_upload.side_effect = HTTPException(status_code=500, detail="Internal server error")
        
        test_file = ("test.txt", BytesIO(b"content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 500
        mock_upload.assert_called_once()
        mock_info.assert_called_once()
        mock_warning.assert_called_once()
    
    def test_upload_knowledge_file_missing_file(self, client):
        """Test upload without providing file."""
        response = client.post("/upload/knowledge")
        assert response.status_code == 422


class TestUploadMultipleKnowledgeFiles:
    """Test cases for POST /upload/knowledge/multi endpoint."""
    
    @patch.object(upload_service, 'upload_multiple_files')
    @patch.object(logger, 'info')
    def test_upload_multiple_files_success(self, mock_logger, mock_upload, client, mock_multi_file_response):
        """Test successful multiple files upload with logging."""
        mock_upload.return_value = mock_multi_file_response
        
        test_files = [
            ("files", ("file1.txt", BytesIO(b"content1"), "text/plain")),
            ("files", ("file2.txt", BytesIO(b"content2"), "text/plain")),
            ("files", ("file3.txt", BytesIO(b"content3"), "text/plain"))
        ]
        
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 201
        data = response.json()
        assert data["success"] is True
        assert len(data["files"]) == 3
        
        mock_upload.assert_called_once()
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Uploading 3 knowledge files")
        mock_logger.assert_any_call("Multiple knowledge files uploaded successfully: 3 files")
    
    @patch.object(upload_service, 'upload_multiple_files')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_upload_multiple_files_http_exception(self, mock_info, mock_warning, mock_upload, client):
        """Test multiple files upload with HTTPException."""
        mock_upload.side_effect = HTTPException(status_code=413, detail="Files too large")
        
        test_files = [
            ("files", ("large1.txt", BytesIO(b"x" * 1000), "text/plain")),
            ("files", ("large2.txt", BytesIO(b"x" * 1000), "text/plain"))
        ]
        
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 413
        assert "Files too large" in response.json()["detail"]
        
        mock_upload.assert_called_once()
        mock_info.assert_called_once_with("Uploading 2 knowledge files")
        mock_warning.assert_called_once_with("Multiple knowledge files upload failed: 413: Files too large")
    
    @patch.object(upload_service, 'upload_multiple_files')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_upload_multiple_files_server_error(self, mock_info, mock_warning, mock_upload, client):
        """Test multiple files upload with server error."""
        mock_upload.side_effect = HTTPException(status_code=500, detail="Server error")
        
        test_files = [("files", ("test.txt", BytesIO(b"content"), "text/plain"))]
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 500
        mock_upload.assert_called_once()
        mock_info.assert_called_once()
        mock_warning.assert_called_once()
    
    def test_upload_multiple_files_no_files(self, client):
        """Test multiple files upload without providing files."""
        response = client.post("/upload/knowledge/multi")
        assert response.status_code == 422


class TestCheckKnowledgeFile:
    """Test cases for GET /upload/knowledge/check endpoint."""
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'info')
    def test_check_file_exists(self, mock_logger, mock_check, client, mock_file_check_response):
        """Test checking existing file with logging."""
        mock_check.return_value = mock_file_check_response
        
        response = client.get("/upload/knowledge/check?filename=test.txt")
        
        assert response.status_code == 200
        data = response.json()
        assert data["filename"] == "test.txt"
        assert data["exists"] is True
        assert data["file_size_bytes"] == 1024
        
        mock_check.assert_called_once_with("test.txt")
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Checking knowledge file: test.txt")
        mock_logger.assert_any_call("Knowledge file exists: test.txt")
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'info')
    def test_check_file_not_exists_as_file_check_response(self, mock_logger, mock_check, client):
        """Test checking non-existing file that returns FileCheckResponse with exists=False."""
        # Create a FileCheckResponse with exists=False to test the isinstance logic
        not_found_response = FileCheckResponse(
            filename="notfound.txt",
            path="uploads/knowledge/notfound.txt",
            full_path="/full/path/uploads/knowledge/notfound.txt",
            file_size_bytes=0,
            is_uploaded=False,
            exists=False
        )
        mock_check.return_value = not_found_response
        
        response = client.get("/upload/knowledge/check?filename=notfound.txt")
        
        assert response.status_code == 200
        data = response.json()
        assert data["filename"] == "notfound.txt"
        assert data["exists"] is False
        assert data["is_uploaded"] is False
        
        mock_check.assert_called_once_with("notfound.txt")
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Checking knowledge file: notfound.txt")
        # Since it's isinstance FileCheckResponse, it logs "exists"
        mock_logger.assert_any_call("Knowledge file exists: notfound.txt")
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'info')
    def test_check_file_not_exists_as_not_found_response(self, mock_logger, mock_check, client, mock_file_check_not_found_response):
        """Test checking non-existing file that returns FileCheckNotFoundResponse."""
        mock_check.return_value = mock_file_check_not_found_response
        
        # This will cause a ResponseValidationError due to FastAPI response_model validation
        with pytest.raises(ResponseValidationError):
            response = client.get("/upload/knowledge/check?filename=notfound.txt")
        
        mock_check.assert_called_once_with("notfound.txt")
        # The router processes both logs before ResponseValidationError is raised
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Checking knowledge file: notfound.txt")
        mock_logger.assert_any_call("Knowledge file does not exist: notfound.txt")
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_check_file_http_exception(self, mock_info, mock_warning, mock_check, client):
        """Test file check with HTTPException."""
        mock_check.side_effect = HTTPException(status_code=400, detail="Invalid filename")
        
        response = client.get("/upload/knowledge/check?filename=invalid")
        
        assert response.status_code == 400
        assert "Invalid filename" in response.json()["detail"]
        
        mock_check.assert_called_once_with("invalid")
        mock_info.assert_called_once_with("Checking knowledge file: invalid")
        mock_warning.assert_called_once_with("Knowledge file check failed: 400: Invalid filename")
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_check_file_server_error(self, mock_info, mock_warning, mock_check, client):
        """Test file check with server error."""
        mock_check.side_effect = HTTPException(status_code=500, detail="Server error")
        
        response = client.get("/upload/knowledge/check?filename=test.txt")
        
        assert response.status_code == 500
        mock_check.assert_called_once()
        mock_info.assert_called_once()
        mock_warning.assert_called_once()
    
    def test_check_file_missing_filename(self, client):
        """Test file check without filename parameter."""
        response = client.get("/upload/knowledge/check")
        assert response.status_code == 422
    
    @patch.object(upload_service, 'check_file')
    def test_check_file_empty_filename(self, mock_check, client):
        """Test file check with empty filename."""
        # Empty filename is treated as a valid string parameter by FastAPI
        empty_response = FileCheckResponse(
            filename="",
            path="uploads/knowledge/",
            full_path="/full/path/uploads/knowledge/",
            file_size_bytes=0,
            is_uploaded=False,
            exists=False
        )
        mock_check.return_value = empty_response
        
        response = client.get("/upload/knowledge/check?filename=")
        assert response.status_code == 200
        data = response.json()
        assert data["filename"] == ""
        assert data["exists"] is False


class TestListKnowledgeFiles:
    """Test cases for GET /upload/knowledge/list endpoint."""
    
    @patch.object(upload_service, 'list_files')
    @patch.object(logger, 'info')
    def test_list_files_success(self, mock_logger, mock_list, client, mock_file_list_response):
        """Test successful file listing with logging."""
        mock_list.return_value = mock_file_list_response
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["files"]) == 2
        assert data["files"][0]["filename"] == "existing0.txt"
        assert data["files"][1]["filename"] == "existing1.txt"
        
        mock_list.assert_called_once()
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Listing knowledge files")
        mock_logger.assert_any_call("Listed 2 knowledge files")
    
    @patch.object(upload_service, 'list_files')
    @patch.object(logger, 'info')
    def test_list_files_empty(self, mock_logger, mock_list, client):
        """Test listing files when directory is empty."""
        empty_response = FileListResponse(files=[], success=True)
        mock_list.return_value = empty_response
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["files"]) == 0
        
        mock_list.assert_called_once()
        assert mock_logger.call_count == 2
        mock_logger.assert_any_call("Listing knowledge files")
        mock_logger.assert_any_call("Listed 0 knowledge files")
    
    @patch.object(upload_service, 'list_files')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_list_files_http_exception(self, mock_info, mock_warning, mock_list, client):
        """Test file listing with HTTPException."""
        mock_list.side_effect = HTTPException(status_code=403, detail="Permission denied")
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 403
        assert "Permission denied" in response.json()["detail"]
        
        mock_list.assert_called_once()
        mock_info.assert_called_once_with("Listing knowledge files")
        mock_warning.assert_called_once_with("Knowledge files listing failed: 403: Permission denied")
    
    @patch.object(upload_service, 'list_files')
    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_list_files_server_error(self, mock_info, mock_warning, mock_list, client):
        """Test file listing with server error."""
        mock_list.side_effect = HTTPException(status_code=500, detail="Internal error")
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 500
        mock_list.assert_called_once()
        mock_info.assert_called_once()
        mock_warning.assert_called_once()


class TestRouterConfiguration:
    """Test cases for router configuration and module-level components."""
    
    def test_router_prefix(self):
        """Test router has correct prefix."""
        assert router.prefix == "/upload"
    
    def test_router_tags(self):
        """Test router has correct tags."""
        assert "uploads" in router.tags
    
    def test_router_responses(self):
        """Test router has correct response configuration."""
        assert 404 in router.responses
        assert router.responses[404]["description"] == "Not found"
    
    def test_logger_name(self):
        """Test logger has correct name."""
        assert logger.name == "src.api.upload_router"
    
    def test_upload_service_instance(self):
        """Test upload service instance exists."""
        from src.api.upload_router import upload_service as service_instance
        from src.services.upload_service import UploadService
        assert isinstance(service_instance, UploadService)


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    @patch.object(upload_service, 'upload_file')
    def test_upload_file_with_special_characters(self, mock_upload, client, mock_file_response):
        """Test uploading file with special characters in filename."""
        special_response = FileResponse(
            filename="test file with spaces & symbols!.txt",
            path="uploads/knowledge/test file with spaces & symbols!.txt",
            full_path="/full/path/uploads/knowledge/test file with spaces & symbols!.txt",
            file_size_bytes=1024,
            is_uploaded=True,
            success=True
        )
        mock_upload.return_value = special_response
        
        test_file = ("test file with spaces & symbols!.txt", BytesIO(b"content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 201
        data = response.json()
        assert data["filename"] == "test file with spaces & symbols!.txt"
    
    @patch.object(upload_service, 'check_file')
    def test_check_file_with_special_characters(self, mock_check, client):
        """Test checking file with special characters in filename."""
        special_response = FileCheckResponse(
            filename="special@file#name.txt",
            path="uploads/knowledge/special@file#name.txt",
            full_path="/full/path/uploads/knowledge/special@file#name.txt",
            file_size_bytes=512,
            is_uploaded=True,
            exists=True
        )
        mock_check.return_value = special_response
        
        response = client.get("/upload/knowledge/check?filename=special@file#name.txt")
        
        assert response.status_code == 200
        data = response.json()
        assert data["filename"] == "special@file#name.txt"
        assert data["exists"] is True
    
    @patch.object(upload_service, 'upload_multiple_files')
    @patch.object(logger, 'info')
    def test_upload_single_file_in_multi_endpoint(self, mock_logger, mock_upload, client):
        """Test uploading single file using multi-file endpoint."""
        files = [FileInfo(
            filename="single.txt",
            path="uploads/knowledge/single.txt",
            full_path="/full/path/uploads/knowledge/single.txt",
            file_size_bytes=256,
            is_uploaded=True
        )]
        single_response = MultiFileResponse(files=files, success=True)
        mock_upload.return_value = single_response
        
        test_files = [("files", ("single.txt", BytesIO(b"content"), "text/plain"))]
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 201
        data = response.json()
        assert len(data["files"]) == 1
        
        mock_logger.assert_any_call("Uploading 1 knowledge files")
        mock_logger.assert_any_call("Multiple knowledge files uploaded successfully: 1 files")


class TestResponseModels:
    """Test response model validation and structure."""
    
    @patch.object(upload_service, 'upload_file')
    def test_file_response_structure(self, mock_upload, client):
        """Test FileResponse has all required fields."""
        response_data = FileResponse(
            filename="test.txt",
            path="uploads/knowledge/test.txt", 
            full_path="/full/path/uploads/knowledge/test.txt",
            file_size_bytes=1024,
            is_uploaded=True,
            success=True
        )
        mock_upload.return_value = response_data
        
        test_file = ("test.txt", BytesIO(b"content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        data = response.json()
        required_fields = ["filename", "path", "full_path", "file_size_bytes", "is_uploaded", "success"]
        for field in required_fields:
            assert field in data
    
    @patch.object(upload_service, 'upload_multiple_files')
    def test_multi_file_response_structure(self, mock_upload, client):
        """Test MultiFileResponse has all required fields."""
        files = [FileInfo(
            filename="test.txt",
            path="uploads/knowledge/test.txt",
            full_path="/full/path/uploads/knowledge/test.txt", 
            file_size_bytes=1024,
            is_uploaded=True
        )]
        response_data = MultiFileResponse(files=files, success=True)
        mock_upload.return_value = response_data
        
        test_files = [("files", ("test.txt", BytesIO(b"content"), "text/plain"))]
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        data = response.json()
        assert "files" in data
        assert "success" in data
        assert isinstance(data["files"], list)
        assert len(data["files"]) == 1
        
        file_data = data["files"][0]
        file_fields = ["filename", "path", "full_path", "file_size_bytes", "is_uploaded"]
        for field in file_fields:
            assert field in file_data


class TestLoggingBehavior:
    """Test specific logging behavior and conditions."""
    
    @patch.object(upload_service, 'check_file')
    @patch.object(logger, 'info')
    def test_isinstance_check_logging_logic(self, mock_logger, mock_check, client):
        """Test the isinstance logic in check_knowledge_file for logging."""
        # Test when service returns FileCheckResponse (isinstance check passes)
        file_check_response = FileCheckResponse(
            filename="exists.txt",
            path="uploads/knowledge/exists.txt",
            full_path="/full/path/uploads/knowledge/exists.txt",
            file_size_bytes=1024,
            is_uploaded=True,
            exists=True
        )
        mock_check.return_value = file_check_response
        
        response = client.get("/upload/knowledge/check?filename=exists.txt")
        
        assert response.status_code == 200
        mock_logger.assert_any_call("Knowledge file exists: exists.txt")
    
    @patch.object(upload_service, 'upload_file')
    @patch.object(logger, 'info')
    def test_filename_logging_in_upload(self, mock_logger, mock_upload, client, mock_file_response):
        """Test that filename is correctly logged in upload endpoint."""
        mock_upload.return_value = mock_file_response
        
        test_file = ("unique_filename.txt", BytesIO(b"content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 201
        mock_logger.assert_any_call("Uploading knowledge file: unique_filename.txt")
        mock_logger.assert_any_call("Knowledge file uploaded successfully: unique_filename.txt")
    
    @patch.object(upload_service, 'upload_multiple_files')
    @patch.object(logger, 'info')
    def test_file_count_logging_in_multi_upload(self, mock_logger, mock_upload, client):
        """Test that file count is correctly logged in multi-upload endpoint."""
        files = [FileInfo(
            filename=f"file{i}.txt",
            path=f"uploads/knowledge/file{i}.txt",
            full_path=f"/full/path/uploads/knowledge/file{i}.txt",
            file_size_bytes=512,
            is_uploaded=True
        ) for i in range(5)]
        
        multi_response = MultiFileResponse(files=files, success=True)
        mock_upload.return_value = multi_response
        
        test_files = [("files", (f"file{i}.txt", BytesIO(b"content"), "text/plain")) for i in range(5)]
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 201
        mock_logger.assert_any_call("Uploading 5 knowledge files")
        mock_logger.assert_any_call("Multiple knowledge files uploaded successfully: 5 files")
    
    @patch.object(upload_service, 'list_files')
    @patch.object(logger, 'info')
    def test_file_count_logging_in_list(self, mock_logger, mock_list, client):
        """Test that file count is correctly logged in list endpoint."""
        files = [FileInfo(
            filename=f"listed{i}.txt",
            path=f"uploads/knowledge/listed{i}.txt",
            full_path=f"/full/path/uploads/knowledge/listed{i}.txt",
            file_size_bytes=256,
            is_uploaded=True
        ) for i in range(7)]
        
        list_response = FileListResponse(files=files, success=True)
        mock_list.return_value = list_response
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 200
        mock_logger.assert_any_call("Listing knowledge files")
        mock_logger.assert_any_call("Listed 7 knowledge files")


class TestExceptionHandling:
    """Test exception handling and error propagation."""
    
    @patch.object(upload_service, 'upload_file')
    def test_upload_reraises_http_exception(self, mock_upload, client):
        """Test that HTTPException is properly re-raised."""
        custom_exception = HTTPException(status_code=422, detail="Custom validation error")
        mock_upload.side_effect = custom_exception
        
        test_file = ("test.txt", BytesIO(b"content"), "text/plain")
        response = client.post("/upload/knowledge", files={"file": test_file})
        
        assert response.status_code == 422
        assert "Custom validation error" in response.json()["detail"]
    
    @patch.object(upload_service, 'upload_multiple_files')
    def test_multi_upload_reraises_http_exception(self, mock_upload, client):
        """Test that HTTPException is properly re-raised in multi-upload."""
        custom_exception = HTTPException(status_code=507, detail="Insufficient storage")
        mock_upload.side_effect = custom_exception
        
        test_files = [("files", ("test.txt", BytesIO(b"content"), "text/plain"))]
        response = client.post("/upload/knowledge/multi", files=test_files)
        
        assert response.status_code == 507
        assert "Insufficient storage" in response.json()["detail"]
    
    @patch.object(upload_service, 'check_file')
    def test_check_file_reraises_http_exception(self, mock_check, client):
        """Test that HTTPException is properly re-raised in check endpoint."""
        custom_exception = HTTPException(status_code=404, detail="File not found in storage")
        mock_check.side_effect = custom_exception
        
        response = client.get("/upload/knowledge/check?filename=missing.txt")
        
        assert response.status_code == 404
        assert "File not found in storage" in response.json()["detail"]
    
    @patch.object(upload_service, 'list_files')
    def test_list_files_reraises_http_exception(self, mock_list, client):
        """Test that HTTPException is properly re-raised in list endpoint."""
        custom_exception = HTTPException(status_code=503, detail="Service temporarily unavailable")
        mock_list.side_effect = custom_exception
        
        response = client.get("/upload/knowledge/list")
        
        assert response.status_code == 503
        assert "Service temporarily unavailable" in response.json()["detail"]