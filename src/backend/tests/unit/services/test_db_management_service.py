"""
Unit tests for DBManagementService.

Tests the functionality of database file management operations including
database export, import, status checking, backup handling, and error cases.
"""
import pytest
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from fastapi import UploadFile, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from src.services.db_management_service import DBManagementService


class MockUploadFile:
    """Mock FastAPI UploadFile for testing."""
    
    def __init__(self, content=b"test database content"):
        self.file = MagicMock()
        self.file.read.return_value = content
        self.filename = "test.db"


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_file.write(b"test database content")
        temp_path = temp_file.name
    
    yield temp_path
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def temp_dir():
    """Create a temporary directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def db_management_service(temp_db_path):
    """Create a DBManagementService with temporary database."""
    service = DBManagementService(db_path=temp_db_path)
    yield service
    # Cleanup backup directory
    if service.backup_dir.exists():
        shutil.rmtree(service.backup_dir, ignore_errors=True)


@pytest.fixture
def mock_background_tasks():
    """Create a mock BackgroundTasks."""
    return MagicMock(spec=BackgroundTasks)


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return MagicMock(spec=Session)


class TestDBManagementService:
    """Test cases for DBManagementService."""
    
    def test_db_management_service_initialization(self, temp_db_path):
        """Test DBManagementService initialization."""
        service = DBManagementService(db_path=temp_db_path)
        
        assert service.db_path == Path(temp_db_path)
        assert service.backup_dir == Path("./tmp")
        assert service.backup_dir.exists()
    
    def test_initialization_creates_backup_directory(self, temp_dir):
        """Test that initialization creates backup directory."""
        nonexistent_db = os.path.join(temp_dir, "nonexistent.db")
        
        with patch.object(Path, 'mkdir') as mock_mkdir:
            service = DBManagementService(db_path=nonexistent_db)
            mock_mkdir.assert_called_once_with(exist_ok=True)
    
    @pytest.mark.asyncio
    async def test_export_database_success(self, db_management_service, mock_background_tasks):
        """Test successful database export."""
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy, \
             patch('src.services.db_management_service.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            result = await db_management_service.export_database(mock_background_tasks)
            
            assert "path" in result
            assert "filename" in result
            assert result["filename"] == "crewai_backup_20230615_143000.db"
            assert "tmp/crewai_backup_20230615_143000.db" in result["path"]
            
            # Verify copy was called
            mock_copy.assert_called_once()
            
            # Verify background task was added
            mock_background_tasks.add_task.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_export_database_file_not_found(self, temp_dir, mock_background_tasks):
        """Test database export when database file doesn't exist."""
        nonexistent_db = os.path.join(temp_dir, "nonexistent.db")
        service = DBManagementService(db_path=nonexistent_db)
        
        with pytest.raises(HTTPException) as exc_info:
            await service.export_database(mock_background_tasks)
        
        assert exc_info.value.status_code == 404
        assert "Database file not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_export_database_copy_error(self, db_management_service, mock_background_tasks):
        """Test database export when copy operation fails."""
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy:
            mock_copy.side_effect = Exception("Copy failed")
            
            with pytest.raises(HTTPException) as exc_info:
                await db_management_service.export_database(mock_background_tasks)
            
            assert exc_info.value.status_code == 500
            assert "Failed to export database" in str(exc_info.value.detail)
    
    def test_cleanup_backup_function(self, db_management_service, mock_background_tasks):
        """Test the cleanup function created during export."""
        with patch('src.services.db_management_service.shutil.copy2'), \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('src.services.db_management_service.os.unlink') as mock_unlink:
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            # Run export to get the cleanup function
            import asyncio
            result = asyncio.run(db_management_service.export_database(mock_background_tasks))
            
            # Get the cleanup function that was added as a background task
            cleanup_func = mock_background_tasks.add_task.call_args[0][0]
            
            # Test successful cleanup
            with patch.object(Path, 'exists', return_value=True):
                cleanup_func()
                mock_unlink.assert_called_once()
    
    def test_cleanup_backup_function_error_handling(self, db_management_service, mock_background_tasks):
        """Test cleanup function handles errors gracefully."""
        with patch('src.services.db_management_service.shutil.copy2'), \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('src.services.db_management_service.os.unlink') as mock_unlink, \
             patch('src.services.db_management_service.logger') as mock_logger:
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            mock_unlink.side_effect = Exception("Delete failed")
            
            # Run export to get the cleanup function
            import asyncio
            result = asyncio.run(db_management_service.export_database(mock_background_tasks))
            
            # Get and run the cleanup function
            cleanup_func = mock_background_tasks.add_task.call_args[0][0]
            
            with patch.object(Path, 'exists', return_value=True):
                cleanup_func()  # Should not raise exception
                mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_import_database_success(self, db_management_service, mock_db_session):
        """Test successful database import."""
        mock_file = MockUploadFile()
        
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy, \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('builtins.open', mock_open()) as mock_file_open, \
             patch('src.services.db_management_service.shutil.copyfileobj') as mock_copyobj, \
             patch('src.services.db_management_service.os.unlink') as mock_unlink, \
             patch.object(Path, 'exists', return_value=True):
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            result = await db_management_service.import_database(mock_file, mock_db_session)
            
            assert result["message"] == "Database imported successfully"
            
            # Verify session was closed
            mock_db_session.close.assert_called_once()
            
            # Verify backup was created
            assert mock_copy.call_count >= 2  # At least backup + replace operations
            
            # Verify file operations
            mock_file_open.assert_called_once()
            mock_copyobj.assert_called_once()
            mock_unlink.assert_called_once()  # Cleanup temp file
    
    @pytest.mark.asyncio
    async def test_import_database_no_existing_db(self, temp_dir, mock_db_session):
        """Test database import when no existing database exists."""
        nonexistent_db = os.path.join(temp_dir, "nonexistent.db")
        service = DBManagementService(db_path=nonexistent_db)
        mock_file = MockUploadFile()
        
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy, \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('builtins.open', mock_open()), \
             patch('src.services.db_management_service.shutil.copyfileobj'), \
             patch('src.services.db_management_service.os.unlink'), \
             patch.object(Path, 'exists', side_effect=lambda: service.db_path.name not in str(service.db_path)):
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            result = await service.import_database(mock_file, mock_db_session)
            
            assert result["message"] == "Database imported successfully"
            mock_db_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_import_database_with_restore_on_failure(self, db_management_service, mock_db_session):
        """Test database import with restore when import fails."""
        mock_file = MockUploadFile()
        
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy, \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('builtins.open', mock_open()), \
             patch('src.services.db_management_service.shutil.copyfileobj') as mock_copyobj, \
             patch.object(Path, 'exists', return_value=True):
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            # Make the database replacement fail
            mock_copy.side_effect = [None, Exception("Replace failed"), None]  # backup, fail, restore
            
            with pytest.raises(HTTPException) as exc_info:
                await db_management_service.import_database(mock_file, mock_db_session)
            
            assert exc_info.value.status_code == 500
            assert "Failed to import database" in str(exc_info.value.detail)
            
            # Verify restore was attempted (3 copy calls: backup, fail, restore)
            assert mock_copy.call_count == 3
    
    @pytest.mark.asyncio
    async def test_import_database_general_error(self, db_management_service, mock_db_session):
        """Test database import with general error."""
        mock_file = MockUploadFile()
        
        with patch.object(mock_db_session, 'close', side_effect=Exception("Session close failed")):
            with pytest.raises(HTTPException) as exc_info:
                await db_management_service.import_database(mock_file, mock_db_session)
            
            assert exc_info.value.status_code == 500
            assert "Failed to import database" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_database_status_exists(self, db_management_service):
        """Test getting database status when database exists."""
        with patch.object(Path, 'exists', return_value=True), \
             patch.object(Path, 'stat') as mock_stat:
            
            # Mock file stats
            mock_stat_result = MagicMock()
            mock_stat_result.st_size = 1048576  # 1 MB
            mock_stat_result.st_mtime = 1686831000  # Mock timestamp
            mock_stat.return_value = mock_stat_result
            
            result = await db_management_service.get_database_status()
            
            assert result["exists"] is True
            assert result["size"] == 1048576
            assert result["size_human"] == "1.00 MB"
            assert result["path"] == str(db_management_service.db_path)
            assert "last_modified" in result
    
    @pytest.mark.asyncio
    async def test_get_database_status_not_exists(self, temp_dir):
        """Test getting database status when database doesn't exist."""
        nonexistent_db = os.path.join(temp_dir, "nonexistent.db")
        service = DBManagementService(db_path=nonexistent_db)
        
        result = await service.get_database_status()
        
        assert result["exists"] is False
        assert result["size"] == 0
        assert result["path"] == str(service.db_path)
        assert result["last_modified"] is None
    
    @pytest.mark.asyncio
    async def test_get_database_status_error(self, db_management_service):
        """Test getting database status with error."""
        with patch.object(Path, 'exists', side_effect=Exception("Stat failed")):
            with pytest.raises(HTTPException) as exc_info:
                await db_management_service.get_database_status()
            
            assert exc_info.value.status_code == 500
            assert "Failed to get database status" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    @patch('src.services.db_management_service.logger')
    async def test_logging_in_export_error(self, mock_logger, db_management_service, mock_background_tasks):
        """Test that errors are properly logged during export."""
        with patch('src.services.db_management_service.shutil.copy2', side_effect=Exception("Copy error")):
            with pytest.raises(HTTPException):
                await db_management_service.export_database(mock_background_tasks)
            
            mock_logger.error.assert_called_once()
            assert "Database export error" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    @patch('src.services.db_management_service.logger')
    async def test_logging_in_import_error(self, mock_logger, db_management_service, mock_db_session):
        """Test that errors are properly logged during import."""
        mock_file = MockUploadFile()
        
        with patch.object(mock_db_session, 'close', side_effect=Exception("Session error")):
            with pytest.raises(HTTPException):
                await db_management_service.import_database(mock_file, mock_db_session)
            
            mock_logger.error.assert_called_once()
            assert "Database import error" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    @patch('src.services.db_management_service.logger')
    async def test_logging_in_status_error(self, mock_logger, db_management_service):
        """Test that errors are properly logged during status check."""
        with patch.object(Path, 'exists', side_effect=Exception("Status error")):
            with pytest.raises(HTTPException):
                await db_management_service.get_database_status()
            
            mock_logger.error.assert_called_once()
            assert "Database status error" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    @patch('src.services.db_management_service.logger')
    async def test_successful_backup_creation_logging(self, mock_logger, db_management_service, mock_db_session):
        """Test that successful backup creation is logged."""
        mock_file = MockUploadFile()
        
        with patch('src.services.db_management_service.shutil.copy2'), \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('builtins.open', mock_open()), \
             patch('src.services.db_management_service.shutil.copyfileobj'), \
             patch('src.services.db_management_service.os.unlink'), \
             patch.object(Path, 'exists', return_value=True):
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            await db_management_service.import_database(mock_file, mock_db_session)
            
            # Should log successful backup creation
            mock_logger.info.assert_called_once()
            assert "Created backup of original database" in mock_logger.info.call_args[0][0]
    
    def test_service_attributes(self, db_management_service):
        """Test that service has correct attributes."""
        assert hasattr(db_management_service, 'db_path')
        assert hasattr(db_management_service, 'backup_dir')
        assert isinstance(db_management_service.db_path, Path)
        assert isinstance(db_management_service.backup_dir, Path)
    
    @pytest.mark.asyncio
    async def test_export_backup_filename_format(self, db_management_service, mock_background_tasks):
        """Test that export creates backup with correct filename format."""
        with patch('src.services.db_management_service.shutil.copy2'), \
             patch('src.services.db_management_service.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            result = await db_management_service.export_database(mock_background_tasks)
            
            expected_filename = "crewai_backup_20230615_143000.db"
            assert result["filename"] == expected_filename
            assert expected_filename in result["path"]
    
    @pytest.mark.asyncio
    async def test_import_backup_filename_format(self, db_management_service, mock_db_session):
        """Test that import creates backup with correct filename format."""
        mock_file = MockUploadFile()
        
        with patch('src.services.db_management_service.shutil.copy2') as mock_copy, \
             patch('src.services.db_management_service.datetime') as mock_datetime, \
             patch('builtins.open', mock_open()), \
             patch('src.services.db_management_service.shutil.copyfileobj'), \
             patch('src.services.db_management_service.os.unlink'), \
             patch.object(Path, 'exists', return_value=True):
            
            mock_datetime.now.return_value.strftime.return_value = "20230615_143000"
            
            await db_management_service.import_database(mock_file, mock_db_session)
            
            # Check that backup was created with correct naming pattern
            backup_calls = [call for call in mock_copy.call_args_list if 'crewai_original_20230615_143000.db' in str(call)]
            assert len(backup_calls) > 0
    
    @pytest.mark.asyncio
    async def test_database_status_size_calculation(self, db_management_service):
        """Test database status size calculation in MB."""
        with patch.object(Path, 'exists', return_value=True), \
             patch.object(Path, 'stat') as mock_stat:
            
            # Test different file sizes
            test_cases = [
                (1024*1024, "1.00 MB"),      # 1 MB
                (2.5*1024*1024, "2.50 MB"),  # 2.5 MB
                (512*1024, "0.50 MB"),       # 0.5 MB
            ]
            
            for size_bytes, expected_mb in test_cases:
                mock_stat_result = MagicMock()
                mock_stat_result.st_size = size_bytes
                mock_stat_result.st_mtime = 1686831000
                mock_stat.return_value = mock_stat_result
                
                result = await db_management_service.get_database_status()
                
                assert result["size"] == size_bytes
                assert result["size_human"] == expected_mb