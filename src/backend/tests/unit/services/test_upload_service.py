"""
Unit tests for UploadService.

Tests the functionality of file upload operations including
single file upload, multiple file upload, file checking, and file listing.
"""
import pytest
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import UploadFile, HTTPException, status

from src.services.upload_service import UploadService
from src.schemas.upload import FileResponse, MultiFileResponse, FileCheckResponse, FileCheckNotFoundResponse, FileListResponse


class MockUploadFile:
    """Mock FastAPI UploadFile for testing."""
    
    def __init__(self, filename="test.txt", content_type="text/plain", size=100):
        self.filename = filename
        self.content_type = content_type
        self.size = size
        
    async def read(self):
        return b"test content"


@pytest.fixture
def mock_upload_repository():
    """Create a mock UploadRepository."""
    return AsyncMock()


@pytest.fixture
def upload_service_with_mock_repo(mock_upload_repository):
    """Create an UploadService with mocked repository."""
    with patch('src.services.upload_service.UploadRepository') as mock_repo_class:
        mock_repo_class.return_value = mock_upload_repository
        service = UploadService(uploads_dir=Path("/test/uploads"))
        return service


@pytest.fixture
def mock_upload_file():
    """Create a mock upload file."""
    return MockUploadFile()


@pytest.fixture
def mock_multiple_upload_files():
    """Create multiple mock upload files."""
    return [
        MockUploadFile("file1.txt", "text/plain", 100),
        MockUploadFile("file2.pdf", "application/pdf", 200),
        MockUploadFile("file3.jpg", "image/jpeg", 300)
    ]


class TestUploadService:
    """Test cases for UploadService."""
    
    def test_upload_service_initialization_default_dir(self):
        """Test UploadService initialization with default directory."""
        with patch.dict(os.environ, {'KNOWLEDGE_DIR': '/env/knowledge'}, clear=True), \
             patch('src.services.upload_service.UploadRepository') as mock_repo_class:
            mock_repo_class.return_value = MagicMock()
            service = UploadService()
            
            assert service.uploads_dir == Path('/env/knowledge')
            assert service.repository is not None
    
    def test_upload_service_initialization_no_env_var(self):
        """Test UploadService initialization without environment variable."""
        with patch.dict(os.environ, {}, clear=True), \
             patch('src.services.upload_service.UploadRepository') as mock_repo_class:
            mock_repo_class.return_value = MagicMock()
            service = UploadService()
            
            assert service.uploads_dir == Path('uploads/knowledge')
            assert service.repository is not None
    
    def test_upload_service_initialization_custom_dir(self):
        """Test UploadService initialization with custom directory."""
        custom_dir = Path("/custom/uploads")
        with patch('src.services.upload_service.UploadRepository') as mock_repo_class:
            mock_repo_class.return_value = MagicMock()
            service = UploadService(uploads_dir=custom_dir)
            
            assert service.uploads_dir == custom_dir
            assert service.repository is not None
    
    @pytest.mark.asyncio
    async def test_upload_file_success(self, upload_service_with_mock_repo, mock_upload_repository, mock_upload_file):
        """Test successful file upload."""
        mock_file_info = {
            "filename": "test.txt",
            "path": "uploads/test.txt",
            "full_path": "/uploads/test.txt",
            "file_size_bytes": 100,
            "is_uploaded": True
        }
        mock_upload_repository.save_file.return_value = mock_file_info
        
        result = await upload_service_with_mock_repo.upload_file(mock_upload_file)
        
        assert isinstance(result, FileResponse)
        assert result.success is True
        assert result.filename == "test.txt"
        assert result.file_size_bytes == 100
        assert result.is_uploaded is True
        assert result.path == "uploads/test.txt"
        mock_upload_repository.save_file.assert_called_once_with(mock_upload_file)
    
    @pytest.mark.asyncio
    async def test_upload_file_repository_exception(self, upload_service_with_mock_repo, mock_upload_repository, mock_upload_file):
        """Test file upload with repository exception."""
        mock_upload_repository.save_file.side_effect = Exception("Repository error")
        
        with pytest.raises(HTTPException) as exc_info:
            await upload_service_with_mock_repo.upload_file(mock_upload_file)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to upload file" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_upload_multiple_files_success(self, upload_service_with_mock_repo, mock_upload_repository, mock_multiple_upload_files):
        """Test successful multiple file upload."""
        mock_file_infos = [
            {"filename": "file1.txt", "path": "uploads/file1.txt", "full_path": "/uploads/file1.txt", "file_size_bytes": 100, "is_uploaded": True},
            {"filename": "file2.pdf", "path": "uploads/file2.pdf", "full_path": "/uploads/file2.pdf", "file_size_bytes": 200, "is_uploaded": True},
            {"filename": "file3.jpg", "path": "uploads/file3.jpg", "full_path": "/uploads/file3.jpg", "file_size_bytes": 300, "is_uploaded": True}
        ]
        mock_upload_repository.save_multiple_files.return_value = mock_file_infos
        
        result = await upload_service_with_mock_repo.upload_multiple_files(mock_multiple_upload_files)
        
        assert isinstance(result, MultiFileResponse)
        assert result.success is True
        assert len(result.files) == 3
        assert result.files[0].filename == "file1.txt"
        assert result.files[1].filename == "file2.pdf"
        assert result.files[2].filename == "file3.jpg"
        mock_upload_repository.save_multiple_files.assert_called_once_with(mock_multiple_upload_files)
    
    @pytest.mark.asyncio
    async def test_upload_multiple_files_repository_exception(self, upload_service_with_mock_repo, mock_upload_repository, mock_multiple_upload_files):
        """Test multiple file upload with repository exception."""
        mock_upload_repository.save_multiple_files.side_effect = Exception("Repository error")
        
        with pytest.raises(HTTPException) as exc_info:
            await upload_service_with_mock_repo.upload_multiple_files(mock_multiple_upload_files)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to upload files" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_upload_multiple_files_empty_list(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test multiple file upload with empty list."""
        mock_upload_repository.save_multiple_files.return_value = []
        
        result = await upload_service_with_mock_repo.upload_multiple_files([])
        
        assert isinstance(result, MultiFileResponse)
        assert result.success is True
        assert len(result.files) == 0
        mock_upload_repository.save_multiple_files.assert_called_once_with([])
    
    @pytest.mark.asyncio
    async def test_check_file_exists(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test checking file that exists."""
        mock_file_info = {
            "exists": True,
            "filename": "existing.txt",
            "path": "uploads/existing.txt",
            "full_path": "/uploads/existing.txt",
            "file_size_bytes": 150,
            "is_uploaded": True
        }
        mock_upload_repository.check_file_exists.return_value = mock_file_info
        
        result = await upload_service_with_mock_repo.check_file("existing.txt")
        
        assert isinstance(result, FileCheckResponse)
        assert result.exists is True
        assert result.filename == "existing.txt"
        assert result.file_size_bytes == 150
        assert result.is_uploaded is True
        mock_upload_repository.check_file_exists.assert_called_once_with("existing.txt")
    
    @pytest.mark.asyncio
    async def test_check_file_not_exists(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test checking file that does not exist."""
        mock_file_info = {
            "exists": False,
            "filename": "nonexistent.txt",
            "is_uploaded": False
        }
        mock_upload_repository.check_file_exists.return_value = mock_file_info
        
        result = await upload_service_with_mock_repo.check_file("nonexistent.txt")
        
        assert isinstance(result, FileCheckNotFoundResponse)
        assert result.exists is False
        assert result.filename == "nonexistent.txt"
        assert result.is_uploaded is False
        mock_upload_repository.check_file_exists.assert_called_once_with("nonexistent.txt")
    
    @pytest.mark.asyncio
    async def test_check_file_repository_exception(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test file check with repository exception."""
        mock_upload_repository.check_file_exists.side_effect = Exception("Repository error")
        
        with pytest.raises(HTTPException) as exc_info:
            await upload_service_with_mock_repo.check_file("test.txt")
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to check file" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_list_files_success(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test successful file listing."""
        mock_files = [
            {"filename": "file1.txt", "path": "uploads/file1.txt", "full_path": "/uploads/file1.txt", "file_size_bytes": 100, "is_uploaded": True},
            {"filename": "file2.pdf", "path": "uploads/file2.pdf", "full_path": "/uploads/file2.pdf", "file_size_bytes": 200, "is_uploaded": True},
            {"filename": "file3.jpg", "path": "uploads/file3.jpg", "full_path": "/uploads/file3.jpg", "file_size_bytes": 300, "is_uploaded": True}
        ]
        mock_upload_repository.list_files.return_value = mock_files
        
        result = await upload_service_with_mock_repo.list_files()
        
        assert isinstance(result, FileListResponse)
        assert result.success is True
        assert len(result.files) == 3
        assert result.files[0].filename == "file1.txt"
        assert result.files[1].filename == "file2.pdf"
        assert result.files[2].filename == "file3.jpg"
        mock_upload_repository.list_files.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_files_empty_directory(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test listing files in empty directory."""
        mock_upload_repository.list_files.return_value = []
        
        result = await upload_service_with_mock_repo.list_files()
        
        assert isinstance(result, FileListResponse)
        assert result.success is True
        assert len(result.files) == 0
        mock_upload_repository.list_files.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_files_repository_exception(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test file listing with repository exception."""
        mock_upload_repository.list_files.side_effect = Exception("Repository error")
        
        with pytest.raises(HTTPException) as exc_info:
            await upload_service_with_mock_repo.list_files()
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to list files" in str(exc_info.value.detail)
    
    @patch('src.services.upload_service.UploadRepository')
    def test_repository_initialization(self, mock_repository_class):
        """Test that repository is initialized correctly."""
        mock_repository_instance = MagicMock()
        mock_repository_class.return_value = mock_repository_instance
        
        uploads_dir = Path("/test/dir")
        service = UploadService(uploads_dir=uploads_dir)
        
        mock_repository_class.assert_called_once_with(uploads_dir)
        assert service.repository == mock_repository_instance
    
    @patch('src.services.upload_service.logger')
    @pytest.mark.asyncio
    async def test_upload_file_logging(self, mock_logger, upload_service_with_mock_repo, mock_upload_repository, mock_upload_file):
        """Test that errors are properly logged."""
        mock_upload_repository.save_file.side_effect = Exception("Test error")
        
        with pytest.raises(HTTPException):
            await upload_service_with_mock_repo.upload_file(mock_upload_file)
        
        mock_logger.error.assert_called_once()
        log_call = mock_logger.error.call_args[0][0]
        assert "Failed to upload file test.txt" in log_call
        assert "Test error" in log_call
    
    @patch('src.services.upload_service.logger')
    @pytest.mark.asyncio
    async def test_upload_multiple_files_logging(self, mock_logger, upload_service_with_mock_repo, mock_upload_repository, mock_multiple_upload_files):
        """Test that errors are properly logged for multiple file upload."""
        mock_upload_repository.save_multiple_files.side_effect = Exception("Test error")
        
        with pytest.raises(HTTPException):
            await upload_service_with_mock_repo.upload_multiple_files(mock_multiple_upload_files)
        
        mock_logger.error.assert_called_once()
        log_call = mock_logger.error.call_args[0][0]
        assert "Failed to upload multiple files" in log_call
        assert "Test error" in log_call
    
    @patch('src.services.upload_service.logger')
    @pytest.mark.asyncio
    async def test_check_file_logging(self, mock_logger, upload_service_with_mock_repo, mock_upload_repository):
        """Test that errors are properly logged for file check."""
        mock_upload_repository.check_file_exists.side_effect = Exception("Test error")
        
        with pytest.raises(HTTPException):
            await upload_service_with_mock_repo.check_file("test.txt")
        
        mock_logger.error.assert_called_once()
        log_call = mock_logger.error.call_args[0][0]
        assert "Failed to check file test.txt" in log_call
        assert "Test error" in log_call
    
    @patch('src.services.upload_service.logger')
    @pytest.mark.asyncio
    async def test_list_files_logging(self, mock_logger, upload_service_with_mock_repo, mock_upload_repository):
        """Test that errors are properly logged for file listing."""
        mock_upload_repository.list_files.side_effect = Exception("Test error")
        
        with pytest.raises(HTTPException):
            await upload_service_with_mock_repo.list_files()
        
        mock_logger.error.assert_called_once()
        log_call = mock_logger.error.call_args[0][0]
        assert "Failed to list files" in log_call
        assert "Test error" in log_call
    
    def test_service_attributes(self, upload_service_with_mock_repo):
        """Test that service has correct attributes."""
        assert hasattr(upload_service_with_mock_repo, 'uploads_dir')
        assert hasattr(upload_service_with_mock_repo, 'repository')
        assert upload_service_with_mock_repo.uploads_dir == Path("/test/uploads")
    
    @pytest.mark.asyncio
    async def test_upload_file_with_none_filename(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test upload file with None filename."""
        mock_file = MockUploadFile(filename=None)
        mock_upload_repository.save_file.side_effect = Exception("Invalid filename")
        
        with pytest.raises(HTTPException) as exc_info:
            await upload_service_with_mock_repo.upload_file(mock_file)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to upload file" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_check_file_with_special_characters(self, upload_service_with_mock_repo, mock_upload_repository):
        """Test file check with special characters in filename."""
        special_filename = "file with spaces & symbols!.txt"
        mock_file_info = {
            "exists": True,
            "filename": special_filename,
            "path": f"uploads/{special_filename}",
            "full_path": f"/uploads/{special_filename}",
            "file_size_bytes": 100,
            "is_uploaded": True
        }
        mock_upload_repository.check_file_exists.return_value = mock_file_info
        
        result = await upload_service_with_mock_repo.check_file(special_filename)
        
        assert isinstance(result, FileCheckResponse)
        assert result.filename == special_filename
        mock_upload_repository.check_file_exists.assert_called_once_with(special_filename)