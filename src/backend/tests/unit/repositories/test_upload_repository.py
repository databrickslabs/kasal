"""
Unit tests for UploadRepository.

Tests the functionality of upload repository including
file saving, file listing, file existence checking, and error handling.
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from pathlib import Path
from typing import List, Dict, Any
from io import BytesIO

from fastapi import UploadFile

from src.repositories.upload_repository import UploadRepository


# Mock UploadFile class
class MockUploadFile:
    def __init__(self, filename: str, content: bytes = b"test content", content_type: str = "text/plain"):
        self.filename = filename
        self.content_type = content_type
        self.file = BytesIO(content)
        self.size = len(content)


@pytest.fixture
def temp_upload_dir():
    """Create a temporary directory for testing uploads."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def upload_repository(temp_upload_dir):
    """Create an upload repository with temporary directory."""
    return UploadRepository(upload_dir=temp_upload_dir)


@pytest.fixture
def sample_upload_file():
    """Create a sample upload file for testing."""
    return MockUploadFile(filename="test_file.txt", content=b"Hello, World!")


@pytest.fixture
def sample_upload_files():
    """Create multiple sample upload files for testing."""
    return [
        MockUploadFile(filename="file1.txt", content=b"Content 1"),
        MockUploadFile(filename="file2.txt", content=b"Content 2"),
        MockUploadFile(filename="file3.txt", content=b"Content 3")
    ]


class TestUploadRepositoryInit:
    """Test cases for UploadRepository initialization."""
    
    def test_init_success(self, temp_upload_dir):
        """Test successful initialization."""
        repository = UploadRepository(upload_dir=temp_upload_dir)
        
        assert repository.upload_dir == temp_upload_dir
        assert temp_upload_dir.exists()
    
    def test_init_creates_directory(self):
        """Test initialization creates directory if it doesn't exist."""
        non_existent_dir = Path("/tmp/test_upload_dir_12345")
        
        try:
            repository = UploadRepository(upload_dir=non_existent_dir)
            
            assert repository.upload_dir == non_existent_dir
            assert non_existent_dir.exists()
        finally:
            # Cleanup
            if non_existent_dir.exists():
                shutil.rmtree(non_existent_dir, ignore_errors=True)
    
    def test_init_with_nested_directory(self):
        """Test initialization with nested directory structure."""
        nested_dir = Path("/tmp/test_upload/nested/deep/dir")
        
        try:
            repository = UploadRepository(upload_dir=nested_dir)
            
            assert repository.upload_dir == nested_dir
            assert nested_dir.exists()
            assert nested_dir.parent.exists()
        finally:
            # Cleanup
            if nested_dir.exists():
                shutil.rmtree(nested_dir.parents[2], ignore_errors=True)


class TestUploadRepositoryEnsureDirectoryExists:
    """Test cases for _ensure_directory_exists method."""
    
    def test_ensure_directory_exists_new_dir(self, temp_upload_dir):
        """Test ensuring directory exists creates new directory."""
        # Remove the directory to test creation
        shutil.rmtree(temp_upload_dir, ignore_errors=True)
        
        repository = UploadRepository(upload_dir=temp_upload_dir)
        repository._ensure_directory_exists()
        
        assert temp_upload_dir.exists()
    
    def test_ensure_directory_exists_existing_dir(self, upload_repository, temp_upload_dir):
        """Test ensuring directory exists with existing directory."""
        # Directory should already exist from fixture
        assert temp_upload_dir.exists()
        
        # Should not raise any errors
        upload_repository._ensure_directory_exists()
        
        assert temp_upload_dir.exists()


class TestUploadRepositorySaveFile:
    """Test cases for save_file method."""
    
    @pytest.mark.asyncio
    async def test_save_file_success(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test successful file saving."""
        result = await upload_repository.save_file(sample_upload_file)
        
        # Check returned metadata
        assert result["filename"] == "test_file.txt"
        assert result["path"] == "test_file.txt"
        assert result["is_uploaded"] is True
        assert "full_path" in result
        assert "file_size_bytes" in result
        
        # Check file was actually saved
        saved_file_path = temp_upload_dir / "test_file.txt"
        assert saved_file_path.exists()
        
        # Check file content
        with open(saved_file_path, "rb") as f:
            content = f.read()
            assert content == b"Hello, World!"
    
    @pytest.mark.asyncio
    async def test_save_file_with_special_characters(self, upload_repository, temp_upload_dir):
        """Test saving file with special characters in filename."""
        special_file = MockUploadFile(filename="special-file_123.txt", content=b"Special content")
        
        result = await upload_repository.save_file(special_file)
        
        assert result["filename"] == "special-file_123.txt"
        assert result["is_uploaded"] is True
        
        saved_file_path = temp_upload_dir / "special-file_123.txt"
        assert saved_file_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_file_binary_content(self, upload_repository, temp_upload_dir):
        """Test saving file with binary content."""
        binary_content = bytes(range(256))  # Binary data
        binary_file = MockUploadFile(filename="binary_file.bin", content=binary_content)
        
        result = await upload_repository.save_file(binary_file)
        
        assert result["filename"] == "binary_file.bin"
        assert result["file_size_bytes"] == 256
        
        saved_file_path = temp_upload_dir / "binary_file.bin"
        assert saved_file_path.exists()
        
        with open(saved_file_path, "rb") as f:
            saved_content = f.read()
            assert saved_content == binary_content
    
    @pytest.mark.asyncio
    async def test_save_file_empty_file(self, upload_repository, temp_upload_dir):
        """Test saving empty file."""
        empty_file = MockUploadFile(filename="empty.txt", content=b"")
        
        result = await upload_repository.save_file(empty_file)
        
        assert result["filename"] == "empty.txt"
        assert result["file_size_bytes"] == 0
        assert result["is_uploaded"] is True
        
        saved_file_path = temp_upload_dir / "empty.txt"
        assert saved_file_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_file_overwrite_existing(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test saving file overwrites existing file."""
        # Save file first time
        await upload_repository.save_file(sample_upload_file)
        
        # Create new file with same name but different content
        new_file = MockUploadFile(filename="test_file.txt", content=b"New content")
        result = await upload_repository.save_file(new_file)
        
        assert result["filename"] == "test_file.txt"
        assert result["is_uploaded"] is True
        
        # Check file was overwritten
        saved_file_path = temp_upload_dir / "test_file.txt"
        with open(saved_file_path, "rb") as f:
            content = f.read()
            assert content == b"New content"
    
    @pytest.mark.asyncio
    async def test_save_file_directory_creation_error(self, temp_upload_dir):
        """Test save file when directory creation fails."""
        # Create repository with existing directory first
        upload_repository = UploadRepository(upload_dir=temp_upload_dir)
        sample_file = MockUploadFile(filename="test.txt")
        
        # Patch the _ensure_directory_exists method to simulate mkdir failure
        with patch.object(upload_repository, '_ensure_directory_exists', side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError, match="Permission denied"):
                await upload_repository.save_file(sample_file)
    
    @pytest.mark.asyncio
    async def test_save_file_write_error(self, upload_repository, sample_upload_file):
        """Test save file when file writing fails."""
        with patch("builtins.open", side_effect=IOError("Disk full")):
            with pytest.raises(IOError, match="Disk full"):
                await upload_repository.save_file(sample_upload_file)
    
    @pytest.mark.asyncio
    async def test_save_file_copyfileobj_error(self, upload_repository, sample_upload_file):
        """Test save file when copyfileobj fails."""
        with patch("shutil.copyfileobj", side_effect=OSError("Copy failed")):
            with pytest.raises(OSError, match="Copy failed"):
                await upload_repository.save_file(sample_upload_file)


class TestUploadRepositorySaveMultipleFiles:
    """Test cases for save_multiple_files method."""
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_success(self, upload_repository, temp_upload_dir, sample_upload_files):
        """Test successful saving of multiple files."""
        results = await upload_repository.save_multiple_files(sample_upload_files)
        
        assert len(results) == 3
        
        for i, result in enumerate(results):
            expected_filename = f"file{i+1}.txt"
            assert result["filename"] == expected_filename
            assert result["is_uploaded"] is True
            
            # Check file was saved
            saved_file_path = temp_upload_dir / expected_filename
            assert saved_file_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_empty_list(self, upload_repository):
        """Test saving empty list of files."""
        results = await upload_repository.save_multiple_files([])
        
        assert results == []
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_single_file(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test saving list with single file."""
        results = await upload_repository.save_multiple_files([sample_upload_file])
        
        assert len(results) == 1
        assert results[0]["filename"] == "test_file.txt"
        
        saved_file_path = temp_upload_dir / "test_file.txt"
        assert saved_file_path.exists()
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_partial_failure(self, upload_repository, sample_upload_files):
        """Test saving multiple files when one fails."""
        # Mock save_file to fail on second file
        original_save_file = upload_repository.save_file
        call_count = 0
        
        async def mock_save_file(file):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise IOError("Save failed for file 2")
            return await original_save_file(file)
        
        with patch.object(upload_repository, 'save_file', side_effect=mock_save_file):
            with pytest.raises(IOError, match="Save failed for file 2"):
                await upload_repository.save_multiple_files(sample_upload_files)
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_mixed_content_types(self, upload_repository, temp_upload_dir):
        """Test saving files with different content types."""
        mixed_files = [
            MockUploadFile(filename="text.txt", content=b"Text content", content_type="text/plain"),
            MockUploadFile(filename="image.jpg", content=b"\xff\xd8\xff\xe0", content_type="image/jpeg"),
            MockUploadFile(filename="data.json", content=b'{"key": "value"}', content_type="application/json")
        ]
        
        results = await upload_repository.save_multiple_files(mixed_files)
        
        assert len(results) == 3
        for result in results:
            assert result["is_uploaded"] is True
            file_path = temp_upload_dir / result["filename"]
            assert file_path.exists()


class TestUploadRepositoryCheckFileExists:
    """Test cases for check_file_exists method."""
    
    @pytest.mark.asyncio
    async def test_check_file_exists_true(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test checking file that exists."""
        # First save the file
        await upload_repository.save_file(sample_upload_file)
        
        result = await upload_repository.check_file_exists("test_file.txt")
        
        assert result["filename"] == "test_file.txt"
        assert result["exists"] is True
        assert result["is_uploaded"] is True
        assert "file_size_bytes" in result
        assert "full_path" in result
    
    @pytest.mark.asyncio
    async def test_check_file_exists_false(self, upload_repository):
        """Test checking file that doesn't exist."""
        result = await upload_repository.check_file_exists("nonexistent.txt")
        
        assert result["filename"] == "nonexistent.txt"
        assert result["exists"] is False
        assert result["is_uploaded"] is False
        assert "file_size_bytes" not in result
        assert "full_path" not in result
    
    @pytest.mark.asyncio
    async def test_check_file_exists_special_filename(self, upload_repository, temp_upload_dir):
        """Test checking file with special characters in filename."""
        special_file = MockUploadFile(filename="special-file_123.txt")
        await upload_repository.save_file(special_file)
        
        result = await upload_repository.check_file_exists("special-file_123.txt")
        
        assert result["filename"] == "special-file_123.txt"
        assert result["exists"] is True
    
    @pytest.mark.asyncio
    async def test_check_file_exists_empty_filename(self, upload_repository, temp_upload_dir):
        """Test checking file with empty filename."""
        # When empty string is used as filename, it resolves to the directory itself
        # The directory exists but is not a file, so the method should still return exists=True
        # because the path exists, even though it's a directory
        
        result = await upload_repository.check_file_exists("")
        
        assert result["filename"] == ""
        # Empty filename resolves to the directory path, which exists
        assert result["exists"] is True
        assert result["is_uploaded"] is True
    
    @pytest.mark.asyncio
    async def test_check_file_exists_os_error(self, upload_repository):
        """Test check file exists when os.path.getsize fails."""
        # Create a file first
        sample_file = MockUploadFile(filename="error_file.txt")
        await upload_repository.save_file(sample_file)
        
        with patch("os.path.getsize", side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                await upload_repository.check_file_exists("error_file.txt")


class TestUploadRepositoryListFiles:
    """Test cases for list_files method."""
    
    @pytest.mark.asyncio
    async def test_list_files_empty_directory(self, upload_repository):
        """Test listing files in empty directory."""
        results = await upload_repository.list_files()
        
        assert results == []
    
    @pytest.mark.asyncio
    async def test_list_files_single_file(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test listing directory with single file."""
        await upload_repository.save_file(sample_upload_file)
        
        results = await upload_repository.list_files()
        
        assert len(results) == 1
        assert results[0]["filename"] == "test_file.txt"
        assert results[0]["is_uploaded"] is True
        assert "file_size_bytes" in results[0]
        assert "full_path" in results[0]
    
    @pytest.mark.asyncio
    async def test_list_files_multiple_files(self, upload_repository, temp_upload_dir, sample_upload_files):
        """Test listing directory with multiple files."""
        await upload_repository.save_multiple_files(sample_upload_files)
        
        results = await upload_repository.list_files()
        
        assert len(results) == 3
        filenames = [result["filename"] for result in results]
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames
        assert "file3.txt" in filenames
        
        for result in results:
            assert result["is_uploaded"] is True
            assert "file_size_bytes" in result
    
    @pytest.mark.asyncio
    async def test_list_files_ignores_subdirectories(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test listing files ignores subdirectories."""
        # Create a file
        await upload_repository.save_file(sample_upload_file)
        
        # Create a subdirectory
        sub_dir = temp_upload_dir / "subdir"
        sub_dir.mkdir()
        
        results = await upload_repository.list_files()
        
        # Should only return the file, not the subdirectory
        assert len(results) == 1
        assert results[0]["filename"] == "test_file.txt"
    
    @pytest.mark.asyncio
    async def test_list_files_different_file_sizes(self, upload_repository, temp_upload_dir):
        """Test listing files with different sizes."""
        files = [
            MockUploadFile(filename="small.txt", content=b"small"),
            MockUploadFile(filename="medium.txt", content=b"medium content here"),
            MockUploadFile(filename="large.txt", content=b"large content with much more text and data")
        ]
        
        await upload_repository.save_multiple_files(files)
        
        results = await upload_repository.list_files()
        
        assert len(results) == 3
        
        # Find each file and check sizes
        size_map = {result["filename"]: result["file_size_bytes"] for result in results}
        assert size_map["small.txt"] == 5
        assert size_map["medium.txt"] == 19  # "medium content here" is 19 bytes, not 20
        assert size_map["large.txt"] == 42  # "large content with much more text and data" is 42 bytes
    
    @pytest.mark.asyncio
    async def test_list_files_os_error(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test list files when os.path.getsize fails."""
        await upload_repository.save_file(sample_upload_file)
        
        with patch("os.path.getsize", side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                await upload_repository.list_files()
    
    @pytest.mark.asyncio
    async def test_list_files_iterdir_error(self, upload_repository):
        """Test list files when directory iteration fails."""
        with patch.object(Path, 'iterdir', side_effect=PermissionError("Access denied")):
            with pytest.raises(PermissionError, match="Access denied"):
                await upload_repository.list_files()


class TestUploadRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_save_then_check_then_list_workflow(self, upload_repository, temp_upload_dir, sample_upload_file):
        """Test complete workflow: save, check, list."""
        # Save file
        save_result = await upload_repository.save_file(sample_upload_file)
        assert save_result["is_uploaded"] is True
        
        # Check file exists
        check_result = await upload_repository.check_file_exists("test_file.txt")
        assert check_result["exists"] is True
        assert check_result["filename"] == save_result["filename"]
        
        # List files
        list_result = await upload_repository.list_files()
        assert len(list_result) == 1
        assert list_result[0]["filename"] == "test_file.txt"
    
    @pytest.mark.asyncio
    async def test_save_multiple_then_list_workflow(self, upload_repository, temp_upload_dir, sample_upload_files):
        """Test saving multiple files then listing them."""
        # Save multiple files
        save_results = await upload_repository.save_multiple_files(sample_upload_files)
        assert len(save_results) == 3
        
        # List all files
        list_results = await upload_repository.list_files()
        assert len(list_results) == 3
        
        # Check each saved file appears in list
        saved_filenames = {result["filename"] for result in save_results}
        listed_filenames = {result["filename"] for result in list_results}
        assert saved_filenames == listed_filenames
    
    @pytest.mark.asyncio
    async def test_save_overwrite_check_workflow(self, upload_repository, temp_upload_dir):
        """Test saving file, overwriting it, then checking."""
        # Save original file
        original_file = MockUploadFile(filename="overwrite.txt", content=b"Original content")
        original_result = await upload_repository.save_file(original_file)
        original_size = original_result["file_size_bytes"]
        
        # Overwrite with different content
        new_file = MockUploadFile(filename="overwrite.txt", content=b"New content that is much longer")
        new_result = await upload_repository.save_file(new_file)
        new_size = new_result["file_size_bytes"]
        
        # Check file
        check_result = await upload_repository.check_file_exists("overwrite.txt")
        
        assert check_result["exists"] is True
        assert check_result["file_size_bytes"] == new_size
        assert check_result["file_size_bytes"] != original_size


class TestUploadRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_save_file_logging_on_error(self, upload_repository, sample_upload_file):
        """Test that errors are properly logged."""
        with patch("builtins.open", side_effect=IOError("Test error")):
            with patch("src.repositories.upload_repository.logger") as mock_logger:
                with pytest.raises(IOError, match="Test error"):
                    await upload_repository.save_file(sample_upload_file)
                
                mock_logger.error.assert_called_once()
                assert "Error saving file test_file.txt" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_save_multiple_files_logging_on_error(self, upload_repository, sample_upload_files):
        """Test that errors in save_multiple_files are properly logged."""
        with patch.object(upload_repository, 'save_file', side_effect=IOError("Test error")):
            with patch("src.repositories.upload_repository.logger") as mock_logger:
                with pytest.raises(IOError, match="Test error"):
                    await upload_repository.save_multiple_files(sample_upload_files)
                
                mock_logger.error.assert_called_once()
                assert "Error saving multiple files" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_check_file_exists_logging_on_error(self, upload_repository):
        """Test that errors in check_file_exists are properly logged."""
        with patch.object(Path, 'exists', side_effect=OSError("Test error")):
            with patch("src.repositories.upload_repository.logger") as mock_logger:
                with pytest.raises(OSError, match="Test error"):
                    await upload_repository.check_file_exists("test.txt")
                
                mock_logger.error.assert_called_once()
                assert "Error checking file test.txt" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_list_files_logging_on_error(self, upload_repository):
        """Test that errors in list_files are properly logged."""
        with patch.object(Path, 'iterdir', side_effect=OSError("Test error")):
            with patch("src.repositories.upload_repository.logger") as mock_logger:
                with pytest.raises(OSError, match="Test error"):
                    await upload_repository.list_files()
                
                mock_logger.error.assert_called_once()
                assert "Error listing files" in mock_logger.error.call_args[0][0]


class TestUploadRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_save_file_very_long_filename(self, upload_repository, temp_upload_dir):
        """Test saving file with very long filename."""
        long_filename = "a" * 200 + ".txt"
        long_file = MockUploadFile(filename=long_filename, content=b"Long filename content")
        
        try:
            result = await upload_repository.save_file(long_file)
            assert result["filename"] == long_filename
            assert result["is_uploaded"] is True
        except OSError:
            # Some filesystems have filename length limits, which is acceptable
            pytest.skip("Filesystem doesn't support long filenames")
    
    @pytest.mark.asyncio
    async def test_save_file_unicode_filename(self, upload_repository, temp_upload_dir):
        """Test saving file with unicode characters in filename."""
        unicode_filename = "测试文件_émoji🎉.txt"
        unicode_file = MockUploadFile(filename=unicode_filename, content=b"Unicode content")
        
        try:
            result = await upload_repository.save_file(unicode_file)
            assert result["filename"] == unicode_filename
            assert result["is_uploaded"] is True
            
            # Check file actually exists
            saved_file_path = temp_upload_dir / unicode_filename
            assert saved_file_path.exists()
        except (UnicodeEncodeError, OSError):
            # Some filesystems don't support unicode filenames
            pytest.skip("Filesystem doesn't support unicode filenames")
    
    @pytest.mark.asyncio
    async def test_save_file_large_content(self, upload_repository, temp_upload_dir):
        """Test saving file with large content."""
        # Create 1MB of content
        large_content = b"x" * (1024 * 1024)
        large_file = MockUploadFile(filename="large_file.txt", content=large_content)
        
        result = await upload_repository.save_file(large_file)
        
        assert result["filename"] == "large_file.txt"
        assert result["file_size_bytes"] == 1024 * 1024
        assert result["is_uploaded"] is True
        
        # Verify content was saved correctly
        saved_file_path = temp_upload_dir / "large_file.txt"
        assert saved_file_path.exists()
        assert os.path.getsize(saved_file_path) == 1024 * 1024
    
    @pytest.mark.asyncio
    async def test_check_file_exists_path_traversal_attempt(self, upload_repository):
        """Test check file exists with path traversal attempt."""
        # This should be handled safely by Path operations
        result = await upload_repository.check_file_exists("../../../etc/passwd")
        
        # Should check within upload directory only
        assert result["exists"] is False
    
    @pytest.mark.asyncio
    async def test_list_files_with_hidden_files(self, upload_repository, temp_upload_dir):
        """Test listing files includes hidden files (starting with dot)."""
        # Create regular and hidden files
        regular_file = MockUploadFile(filename="regular.txt", content=b"Regular")
        hidden_file = MockUploadFile(filename=".hidden.txt", content=b"Hidden")
        
        await upload_repository.save_file(regular_file)
        await upload_repository.save_file(hidden_file)
        
        results = await upload_repository.list_files()
        
        assert len(results) == 2
        filenames = [result["filename"] for result in results]
        assert "regular.txt" in filenames
        assert ".hidden.txt" in filenames
    
    @pytest.mark.asyncio
    async def test_save_file_zero_bytes_read(self, upload_repository):
        """Test saving file when file.file.read() returns zero bytes."""
        mock_file = MagicMock()
        mock_file.filename = "zero_bytes.txt"
        mock_file.file = BytesIO(b"")  # Empty content
        
        result = await upload_repository.save_file(mock_file)
        
        assert result["filename"] == "zero_bytes.txt"
        assert result["file_size_bytes"] == 0
        assert result["is_uploaded"] is True