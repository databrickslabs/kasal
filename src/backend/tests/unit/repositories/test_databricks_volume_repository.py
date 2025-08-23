"""
Unit tests for Databricks Volume Repository.

Tests the repository for interacting with Databricks Unity Catalog Volumes using WorkspaceClient.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock, call
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor

from src.repositories.databricks_volume_repository import DatabricksVolumeRepository


class TestDatabricksVolumeRepository:
    """Test suite for DatabricksVolumeRepository."""
    
    def test_initialization(self):
        """Test repository initialization."""
        repo = DatabricksVolumeRepository(user_token="test-token")
        
        assert repo._workspace_client is None
        assert repo._workspace_url is None
        assert repo._user_token == "test-token"
        assert isinstance(repo._executor, ThreadPoolExecutor)
    
    def test_initialization_without_token(self):
        """Test repository initialization without user token."""
        with patch('src.repositories.databricks_volume_repository.logger') as mock_logger:
            repo = DatabricksVolumeRepository()
            
            assert repo._workspace_client is None
            assert repo._workspace_url is None
            assert repo._user_token is None
            mock_logger.warning.assert_called_with(
                "DatabricksVolumeRepository: No user token provided, will use fallback authentication"
            )
    
    @pytest.mark.asyncio
    @patch('src.repositories.databricks_volume_repository.os.environ.get')
    async def test_ensure_client_already_exists(self, mock_env_get):
        """Test _ensure_client when client already exists."""
        repo = DatabricksVolumeRepository()
        repo._workspace_client = Mock()
        
        result = await repo._ensure_client()
        
        assert result is True
        mock_env_get.assert_not_called()
    
    @pytest.mark.asyncio
    @patch.dict(os.environ, {"DATABRICKS_HOST": "workspace.databricks.com"})
    async def test_ensure_client_success_with_env(self):
        """Test successful client creation with workspace URL from environment."""
        repo = DatabricksVolumeRepository(user_token="test-token")
        
        with patch.object(repo, '_create_workspace_client', return_value=Mock()) as mock_create:
            result = await repo._ensure_client()
            
            assert result is True
            assert repo._workspace_url == "https://workspace.databricks.com"
            mock_create.assert_called_once()
    
    @pytest.mark.asyncio
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com/"})
    async def test_ensure_client_strips_trailing_slash(self):
        """Test that trailing slash is stripped from workspace URL."""
        repo = DatabricksVolumeRepository(user_token="test-token")
        
        with patch.object(repo, '_create_workspace_client', return_value=Mock()):
            result = await repo._ensure_client()
            
            assert result is True
            assert repo._workspace_url == "https://workspace.databricks.com"
    
    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_ensure_client_gets_url_from_config(self):
        """Test getting workspace URL from database config."""
        repo = DatabricksVolumeRepository(user_token="test-token")
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
                # Setup mock UOW and service
                mock_uow = AsyncMock()
                mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                mock_uow.__aexit__ = AsyncMock(return_value=None)
                mock_uow_class.return_value = mock_uow
                
                mock_service = AsyncMock()
                mock_config = Mock()
                mock_config.workspace_url = "config.databricks.com"
                mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
                mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
                
                with patch.object(repo, '_create_workspace_client', return_value=Mock()):
                    result = await repo._ensure_client()
                    
                    assert result is True
                    assert repo._workspace_url == "https://config.databricks.com"
    
    @pytest.mark.asyncio
    async def test_ensure_client_failure(self):
        """Test client creation failure."""
        repo = DatabricksVolumeRepository(user_token="test-token")
        
        with patch.object(repo, '_create_workspace_client', return_value=None):
            with patch.dict(os.environ, {"DATABRICKS_HOST": "workspace.databricks.com"}):
                result = await repo._ensure_client()
                
                assert result is False
                assert repo._workspace_client is None
    
    @pytest.mark.asyncio
    async def test_create_workspace_client_obo_primary(self):
        """Test WorkspaceClient creation with OBO authentication as primary."""
        repo = DatabricksVolumeRepository(user_token="test-obo-token")
        repo._workspace_url = "https://workspace.databricks.com"
        
        with patch('databricks.sdk.WorkspaceClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            result = await repo._create_workspace_client()
            
            assert result == mock_client
            mock_client_class.assert_called_once_with(
                host="https://workspace.databricks.com",
                token="test-obo-token",
                auth_type="pat"
            )
    
    @pytest.mark.asyncio
    async def test_create_workspace_client_obo_failure_fallback_to_db(self):
        """Test fallback to database PAT when OBO fails."""
        repo = DatabricksVolumeRepository(user_token="bad-token")
        repo._workspace_url = "https://workspace.databricks.com"
        
        with patch('databricks.sdk.WorkspaceClient') as mock_client_class:
            # First call (OBO) fails, second call (PAT from DB) succeeds
            mock_client = Mock()
            mock_client_class.side_effect = [Exception("OBO failed"), mock_client]
            
            with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
                with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
                    with patch('src.utils.encryption_utils.EncryptionUtils') as mock_encryption:
                        # Setup mock UOW and service
                        mock_uow = AsyncMock()
                        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                        mock_uow.__aexit__ = AsyncMock(return_value=None)
                        mock_uow_class.return_value = mock_uow
                        
                        # Setup API key service
                        mock_api_service = AsyncMock()
                        mock_api_key = Mock()
                        mock_api_key.encrypted_value = "encrypted-pat"
                        mock_api_service.find_by_name = AsyncMock(return_value=mock_api_key)
                        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
                        
                        # Setup encryption
                        mock_encryption.decrypt_value.return_value = "decrypted-pat-token"
                        
                        result = await repo._create_workspace_client()
                        
                        assert result == mock_client
                        assert mock_client_class.call_count == 2
                        # Check second call uses PAT from database
                        mock_client_class.assert_called_with(
                            host="https://workspace.databricks.com",
                            token="decrypted-pat-token",
                            auth_type="pat"
                        )
    
    @pytest.mark.asyncio
    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "env-pat-token"})
    async def test_create_workspace_client_fallback_to_env(self):
        """Test fallback to environment PAT when OBO and DB fail."""
        repo = DatabricksVolumeRepository(user_token="bad-token")
        repo._workspace_url = "https://workspace.databricks.com"
        
        with patch('databricks.sdk.WorkspaceClient') as mock_client_class:
            # First call (OBO) fails, second call (DB) fails, third call (env) succeeds
            mock_client = Mock()
            mock_client_class.side_effect = [
                Exception("OBO failed"),
                Exception("DB PAT failed"),
                mock_client
            ]
            
            with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
                with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
                    # Setup mock UOW and service to return no API key
                    mock_uow = AsyncMock()
                    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                    mock_uow.__aexit__ = AsyncMock(return_value=None)
                    mock_uow_class.return_value = mock_uow
                    
                    mock_api_service = AsyncMock()
                    mock_api_service.find_by_name = AsyncMock(return_value=None)
                    mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
                    
                    result = await repo._create_workspace_client()
                    
                    assert result == mock_client
                    assert mock_client_class.call_count == 3
                    # Check third call uses default SDK authentication (no token/auth_type)
                    mock_client_class.assert_called_with(
                        host="https://workspace.databricks.com"
                    )
    
    @pytest.mark.asyncio
    async def test_create_workspace_client_default_fallback(self):
        """Test fallback to default SDK authentication."""
        repo = DatabricksVolumeRepository()  # No user token
        repo._workspace_url = "https://workspace.databricks.com"
        
        with patch('databricks.sdk.WorkspaceClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            with patch.dict(os.environ, {}, clear=True):
                with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
                    with patch('src.core.unit_of_work.UnitOfWork') as mock_uow_class:
                        # Setup mock UOW and service to return no API key
                        mock_uow = AsyncMock()
                        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
                        mock_uow.__aexit__ = AsyncMock(return_value=None)
                        mock_uow_class.return_value = mock_uow
                        
                        mock_api_service = AsyncMock()
                        mock_api_service.find_by_name = AsyncMock(return_value=None)
                        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
                        
                        result = await repo._create_workspace_client()
                        
                        assert result == mock_client
                        mock_client_class.assert_called_once_with(
                            host="https://workspace.databricks.com"
                        )
    
    @pytest.mark.asyncio
    async def test_create_workspace_client_import_error(self):
        """Test handling of missing databricks-sdk package."""
        repo = DatabricksVolumeRepository()
        
        with patch('src.repositories.databricks_volume_repository.logger') as mock_logger:
            with patch.dict('sys.modules', {'databricks.sdk': None}):
                result = await repo._create_workspace_client()
                
                assert result is None
                mock_logger.error.assert_called_with(
                    "databricks-sdk package not found. Please install it with: pip install databricks-sdk"
                )
    
    @pytest.mark.asyncio
    async def test_create_volume_if_not_exists_already_exists(self):
        """Test creating volume when it already exists."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_volume = Mock()
        mock_client.volumes.read.return_value = mock_volume
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.create_volume_if_not_exists("catalog", "schema", "volume")
        
        assert result["success"] is True
        assert result["exists"] is True
        assert result["message"] == "Volume already exists"
        mock_client.volumes.read.assert_called_once_with("catalog.schema.volume")
    
    @pytest.mark.asyncio
    async def test_create_volume_if_not_exists_creates_new(self):
        """Test creating a new volume."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.volumes.read.side_effect = Exception("Volume not found")
        mock_volume = Mock()
        mock_client.volumes.create.return_value = mock_volume
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            with patch('databricks.sdk.service.catalog.VolumeType') as mock_volume_type:
                mock_volume_type.MANAGED = "MANAGED"
                
                result = await repo.create_volume_if_not_exists("catalog", "schema", "volume")
        
        assert result["success"] is True
        assert result["created"] is True
        assert "created successfully" in result["message"]
        
        mock_client.volumes.create.assert_called_once_with(
            catalog_name="catalog",
            schema_name="schema",
            name="volume",
            volume_type="MANAGED",
            comment="Created by Kasal for database backups"
        )
    
    @pytest.mark.asyncio
    async def test_create_volume_catalog_not_exists(self):
        """Test error when catalog doesn't exist."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.volumes.read.side_effect = Exception("Volume not found")
        mock_client.volumes.create.side_effect = Exception("Catalog 'catalog' does not exist")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.create_volume_if_not_exists("catalog", "schema", "volume")
        
        assert result["success"] is False
        assert "Catalog 'catalog' does not exist" in result["error"]
    
    @pytest.mark.asyncio
    async def test_create_volume_client_failure(self):
        """Test volume creation with client creation failure."""
        repo = DatabricksVolumeRepository()
        
        with patch.object(repo, '_ensure_client', return_value=False):
            result = await repo.create_volume_if_not_exists("catalog", "schema", "volume")
        
        assert result["success"] is False
        assert "Failed to create Databricks client" in result["error"]
    
    @pytest.mark.asyncio
    async def test_upload_file_to_volume_success(self):
        """Test successful file upload to volume."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.upload.return_value = None
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            with patch.object(repo, 'create_volume_if_not_exists', return_value={"success": True}):
                result = await repo.upload_file_to_volume(
                    "catalog", "schema", "volume", "file.txt", b"file content"
                )
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume/file.txt"
        assert result["size"] == len(b"file content")
        
        mock_client.files.upload.assert_called_once_with(
            file_path="/Volumes/catalog/schema/volume/file.txt",
            contents=b"file content",
            overwrite=True
        )
    
    @pytest.mark.asyncio
    async def test_upload_file_volume_creation_fails(self):
        """Test file upload when volume creation fails."""
        repo = DatabricksVolumeRepository()
        
        with patch.object(repo, '_ensure_client', return_value=True):
            with patch.object(repo, 'create_volume_if_not_exists', return_value={"success": False, "error": "Volume error"}):
                result = await repo.upload_file_to_volume(
                    "catalog", "schema", "volume", "file.txt", b"content"
                )
        
        assert result["success"] is False
        assert result["error"] == "Volume error"
    
    @pytest.mark.asyncio
    async def test_upload_file_upload_failure(self):
        """Test file upload failure."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.upload.side_effect = Exception("Upload failed")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            with patch.object(repo, 'create_volume_if_not_exists', return_value={"success": True}):
                result = await repo.upload_file_to_volume(
                    "catalog", "schema", "volume", "file.txt", b"content"
                )
        
        assert result["success"] is False
        assert "Upload failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_download_file_from_volume_success(self):
        """Test successful file download from volume."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client with context manager
        mock_file = Mock()
        mock_file.read.return_value = b"file content"
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        
        mock_client = Mock()
        mock_client.files.download.return_value = mock_file
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.download_file_from_volume(
                "catalog", "schema", "volume", "file.txt"
            )
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume/file.txt"
        assert result["content"] == b"file content"
        
        mock_client.files.download.assert_called_once_with(
            "/Volumes/catalog/schema/volume/file.txt"
        )
    
    @pytest.mark.asyncio
    async def test_download_file_not_found(self):
        """Test download when file doesn't exist."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.download.side_effect = Exception("File not found")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.download_file_from_volume(
                "catalog", "schema", "volume", "nonexistent.txt"
            )
        
        assert result["success"] is False
        assert "File not found" in result["error"]
    
    @pytest.mark.asyncio
    async def test_download_file_client_failure(self):
        """Test file download with client creation failure."""
        repo = DatabricksVolumeRepository()
        
        with patch.object(repo, '_ensure_client', return_value=False):
            result = await repo.download_file_from_volume(
                "catalog", "schema", "volume", "file.txt"
            )
        
        assert result["success"] is False
        assert "Failed to create Databricks client" in result["error"]
    
    @pytest.mark.asyncio
    async def test_list_volume_contents_success(self):
        """Test successful listing of volume contents."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_file1 = Mock()
        mock_file1.path = "/Volumes/catalog/schema/volume/file1.txt"
        mock_file1.name = "file1.txt"
        mock_file1.is_directory = False
        mock_file1.file_size = 100
        mock_file1.modification_time = 1234567890
        
        mock_file2 = Mock()
        mock_file2.path = "/Volumes/catalog/schema/volume/dir1"
        mock_file2.name = "dir1"
        mock_file2.is_directory = True
        mock_file2.file_size = None
        mock_file2.modification_time = 1234567891
        
        mock_client = Mock()
        mock_client.files.list_directory_contents.return_value = [mock_file1, mock_file2]
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.list_volume_contents("catalog", "schema", "volume")
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume"
        assert len(result["files"]) == 2
        assert result["files"][0]["name"] == "file1.txt"
        assert result["files"][0]["is_directory"] is False
        assert result["files"][1]["name"] == "dir1"
        assert result["files"][1]["is_directory"] is True
    
    @pytest.mark.asyncio
    async def test_list_volume_contents_not_found(self):
        """Test listing when volume doesn't exist."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.list_directory_contents.side_effect = Exception("Volume not found")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.list_volume_contents("catalog", "schema", "nonexistent")
        
        assert result["success"] is False
        assert "not found" in result["error"].lower()
    
    @pytest.mark.asyncio
    async def test_create_volume_directory_success(self):
        """Test successful directory creation."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.create_directory.return_value = None
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.create_volume_directory(
                "catalog", "schema", "volume", "new_dir"
            )
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume/new_dir"
        
        mock_client.files.create_directory.assert_called_once_with(
            "/Volumes/catalog/schema/volume/new_dir"
        )
    
    @pytest.mark.asyncio
    async def test_create_volume_directory_already_exists(self):
        """Test directory creation when it already exists."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.create_directory.side_effect = Exception("Directory already exists")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.create_volume_directory(
                "catalog", "schema", "volume", "existing_dir"
            )
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume/existing_dir"
        assert result.get("exists") is True
    
    @pytest.mark.asyncio
    async def test_delete_volume_file_success(self):
        """Test successful file deletion."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.delete.return_value = None
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.delete_volume_file(
                "catalog", "schema", "volume", "file.txt"
            )
        
        assert result["success"] is True
        assert result["path"] == "/Volumes/catalog/schema/volume/file.txt"
        
        mock_client.files.delete.assert_called_once_with(
            "/Volumes/catalog/schema/volume/file.txt"
        )
    
    @pytest.mark.asyncio
    async def test_delete_volume_file_not_found(self):
        """Test deletion when file doesn't exist."""
        repo = DatabricksVolumeRepository()
        
        # Setup mock client
        mock_client = Mock()
        mock_client.files.delete.side_effect = Exception("File not found")
        repo._workspace_client = mock_client
        
        with patch.object(repo, '_ensure_client', return_value=True):
            result = await repo.delete_volume_file(
                "catalog", "schema", "volume", "nonexistent.txt"
            )
        
        assert result["success"] is False
        assert "File not found" in result["error"]
    
    def test_get_databricks_url_with_workspace_url(self):
        """Test URL generation with workspace URL set."""
        repo = DatabricksVolumeRepository()
        repo._workspace_url = "https://workspace.databricks.com"
        
        # Test volume URL
        volume_url = repo.get_databricks_url("catalog", "schema", "volume")
        assert volume_url == "https://workspace.databricks.com/explore/data/volumes/catalog/schema/volume"
        
        # Test file URL
        file_url = repo.get_databricks_url("catalog", "schema", "volume", "file.txt")
        assert file_url == "https://workspace.databricks.com/explore/data/volumes/catalog/schema/volume/file.txt"
    
    @patch.dict(os.environ, {"DATABRICKS_HOST": "env.databricks.com"})
    def test_get_databricks_url_with_env(self):
        """Test URL generation with environment variable."""
        repo = DatabricksVolumeRepository()
        
        volume_url = repo.get_databricks_url("catalog", "schema", "volume")
        assert volume_url == "env.databricks.com/explore/data/volumes/catalog/schema/volume"
    
    def test_get_databricks_url_default(self):
        """Test URL generation with default."""
        repo = DatabricksVolumeRepository()
        
        with patch.dict(os.environ, {}, clear=True):
            volume_url = repo.get_databricks_url("catalog", "schema", "volume")
            assert volume_url == "https://your-workspace.databricks.com/explore/data/volumes/catalog/schema/volume"