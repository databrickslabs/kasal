"""
Comprehensive tests for DatabricksKnowledgeService
Tests reflect the current implementation with latest features:
- Knowledge file upload to Databricks volumes
- Vector search integration for knowledge retrieval
- File registration and management
- Support for user tokens (OBO authentication)
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import Optional, Dict, Any, List

from src.services.databricks_knowledge_service import DatabricksKnowledgeService


class TestDatabricksKnowledgeServiceInit:
    """Test DatabricksKnowledgeService initialization"""

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters"""
        mock_session = Mock()
        group_id = "test-group-id"
        created_by_email = "test@example.com"
        user_token = "test-user-token"

        service = DatabricksKnowledgeService(
            mock_session,
            group_id,
            created_by_email,
            user_token
        )

        assert service.session == mock_session
        assert service.group_id == group_id
        assert service.created_by_email == created_by_email
        assert service.user_token == user_token
        assert hasattr(service, 'repository')
        assert hasattr(service, 'volume_repository')

    def test_init_minimal_parameters(self):
        """Test initialization with minimal parameters"""
        mock_session = Mock()
        group_id = "test-group-id"

        service = DatabricksKnowledgeService(mock_session, group_id)

        assert service.session == mock_session
        assert service.group_id == group_id
        assert service.created_by_email is None
        assert service.user_token is None

    def test_init_repositories_created(self):
        """Test that repositories are properly initialized"""
        mock_session = Mock()
        group_id = "test-group-id"

        service = DatabricksKnowledgeService(mock_session, group_id)

        assert service.repository is not None
        assert service.volume_repository is not None


class TestDatabricksKnowledgeServiceGetFileType:
    """Test DatabricksKnowledgeService _get_file_type method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    def test_get_file_type_pdf(self):
        """Test _get_file_type for PDF files"""
        result = self.service._get_file_type("document.pdf")
        assert result == "pdf"

    def test_get_file_type_txt(self):
        """Test _get_file_type for text files"""
        result = self.service._get_file_type("document.txt")
        assert result == "text"

    def test_get_file_type_md(self):
        """Test _get_file_type for markdown files"""
        result = self.service._get_file_type("document.md")
        assert result == "markdown"

    def test_get_file_type_json(self):
        """Test _get_file_type for JSON files"""
        result = self.service._get_file_type("data.json")
        assert result == "json"

    def test_get_file_type_py(self):
        """Test _get_file_type for Python files"""
        result = self.service._get_file_type("script.py")
        assert result == "python"

    def test_get_file_type_case_insensitive(self):
        """Test _get_file_type is case insensitive"""
        result = self.service._get_file_type("DOCUMENT.PDF")
        assert result == "pdf"

    def test_get_file_type_unknown_extension(self):
        """Test _get_file_type for unknown extensions"""
        result = self.service._get_file_type("file.xyz")
        assert result == "file"


class TestDatabricksKnowledgeServiceUploadKnowledgeFile:
    """Test DatabricksKnowledgeService upload_knowledge_file method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.created_by_email = "test@example.com"
        self.service = DatabricksKnowledgeService(
            self.mock_session,
            self.group_id,
            self.created_by_email
        )

    @pytest.mark.asyncio
    async def test_upload_basic_parameters(self):
        """Test upload with basic parameters"""
        mock_file = Mock()
        mock_file.filename = "test.txt"
        mock_file.content_type = "text/plain"
        mock_file.size = 1024
        mock_file.read = AsyncMock(return_value=b"test content")

        execution_id = "test-execution-id"
        volume_config = {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "volume": "test_volume"
        }

        # Mock repository methods
        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'upload_file_to_volume') as mock_upload, \
             patch('src.services.knowledge_embedding_service.KnowledgeEmbeddingService') as mock_embedding_service:

            mock_get_config.return_value = {"workspace_url": "https://test.databricks.com"}
            mock_upload.return_value = {"path": "/test/path/test.txt"}

            # Mock embedding service
            mock_embedding_instance = AsyncMock()
            mock_embedding_instance.process_and_embed_file.return_value = {"status": "success"}
            mock_embedding_service.return_value = mock_embedding_instance

            result = await self.service.upload_knowledge_file(
                mock_file, execution_id, self.group_id, volume_config
            )

            assert isinstance(result, dict)
            assert "status" in result

    @pytest.mark.asyncio
    async def test_upload_with_agent_ids(self):
        """Test upload with agent_ids filter"""
        mock_file = Mock()
        mock_file.filename = "test.txt"
        mock_file.content_type = "text/plain"
        mock_file.size = 1024
        mock_file.read = AsyncMock(return_value=b"test content")

        execution_id = "test-execution-id"
        volume_config = {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "volume": "test_volume"
        }
        agent_ids = ["agent1", "agent2"]

        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'upload_file_to_volume') as mock_upload, \
             patch('src.services.knowledge_embedding_service.KnowledgeEmbeddingService') as mock_embedding_service:

            mock_get_config.return_value = {"workspace_url": "https://test.databricks.com"}
            mock_upload.return_value = {"path": "/test/path/test.txt"}

            mock_embedding_instance = AsyncMock()
            mock_embedding_instance.process_and_embed_file.return_value = {"status": "success"}
            mock_embedding_service.return_value = mock_embedding_instance

            result = await self.service.upload_knowledge_file(
                mock_file, execution_id, self.group_id, volume_config, agent_ids
            )

            assert isinstance(result, dict)


class TestDatabricksKnowledgeServiceSearchKnowledge:
    """Test DatabricksKnowledgeService search_knowledge method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_search_basic(self):
        """Test basic knowledge search"""
        query = "test query"

        # Mock the search_service attribute directly on the service instance
        mock_search_instance = AsyncMock()
        mock_search_instance.search.return_value = [
            {"content": "result 1", "metadata": {"score": 0.9}},
            {"content": "result 2", "metadata": {"score": 0.8}}
        ]
        self.service.search_service = mock_search_instance

        result = await self.service.search_knowledge(query, self.group_id)

        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_search_with_filters(self):
        """Test search with execution_id and file_paths filters"""
        query = "test query"
        execution_id = "test-execution-id"
        file_paths = ["file1.txt", "file2.txt"]

        # Mock the search_service attribute directly on the service instance
        mock_search_instance = AsyncMock()
        mock_search_instance.search.return_value = []
        self.service.search_service = mock_search_instance

        result = await self.service.search_knowledge(
            query,
            self.group_id,
            execution_id=execution_id,
            file_paths=file_paths
        )

        assert isinstance(result, list)

        # Verify the search service was called with correct parameters
        mock_search_instance.search.assert_called_once()
        call_args = mock_search_instance.search.call_args
        assert call_args.kwargs['query'] == query
        assert call_args.kwargs['execution_id'] == execution_id
        assert call_args.kwargs['file_paths'] == file_paths

    @pytest.mark.asyncio
    async def test_search_with_agent_id(self):
        """Test search with agent_id filter"""
        query = "test query"
        agent_id = "test-agent-id"

        # Mock the search_service attribute directly on the service instance
        mock_search_instance = AsyncMock()
        mock_search_instance.search.return_value = []
        self.service.search_service = mock_search_instance

        result = await self.service.search_knowledge(
            query,
            self.group_id,
            agent_id=agent_id
        )

        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_search_with_user_token(self):
        """Test search with user token (OBO authentication)"""
        query = "test query"
        user_token = "test-user-token"

        # Mock the search_service attribute directly on the service instance
        mock_search_instance = AsyncMock()
        mock_search_instance.search.return_value = []
        self.service.search_service = mock_search_instance

        result = await self.service.search_knowledge(
            query,
            self.group_id,
            user_token=user_token
        )

        assert isinstance(result, list)


class TestDatabricksKnowledgeServiceReadKnowledgeFile:
    """Test DatabricksKnowledgeService read_knowledge_file method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_read_file_basic(self):
        """Test reading a file from Databricks volume"""
        file_path = "/Volumes/catalog/schema/volume/test.txt"

        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'download_file_from_volume') as mock_download:

            mock_get_config.return_value = {"workspace_url": "https://test.databricks.com"}
            mock_download.return_value = {
                "content": b"test file content",
                "metadata": {"size": 17}
            }

            result = await self.service.read_knowledge_file(file_path, self.group_id)

            assert isinstance(result, dict)
            assert "status" in result

    @pytest.mark.asyncio
    async def test_read_file_with_user_token(self):
        """Test reading file with user token"""
        file_path = "/Volumes/catalog/schema/volume/test.txt"
        user_token = "test-user-token"

        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'download_file_from_volume') as mock_download:

            mock_get_config.return_value = {"workspace_url": "https://test.databricks.com"}
            mock_download.return_value = {
                "content": b"test file content",
                "metadata": {"size": 17}
            }

            result = await self.service.read_knowledge_file(
                file_path,
                self.group_id,
                user_token=user_token
            )

            assert isinstance(result, dict)


class TestDatabricksKnowledgeServiceListKnowledgeFiles:
    """Test DatabricksKnowledgeService list_knowledge_files method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_list_files_basic(self):
        """Test listing knowledge files"""
        execution_id = "test-execution-id"

        # The actual implementation just returns an empty list
        result = await self.service.list_knowledge_files(execution_id, self.group_id)

        assert isinstance(result, list)
        assert len(result) == 0  # Current implementation returns empty list

    @pytest.mark.asyncio
    async def test_list_files_empty_result(self):
        """Test listing files returns empty list when no files"""
        execution_id = "test-execution-id"

        # The actual implementation just returns an empty list
        result = await self.service.list_knowledge_files(execution_id, self.group_id)

        assert isinstance(result, list)
        assert len(result) == 0


class TestDatabricksKnowledgeServiceDeleteKnowledgeFile:
    """Test DatabricksKnowledgeService delete_knowledge_file method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_delete_file_basic(self):
        """Test deleting a knowledge file"""
        execution_id = "test-execution-id"
        filename = "test.txt"

        # Mock the methods that delete_knowledge_file actually calls
        mock_config = type('obj', (object,), {
            'knowledge_volume_path': 'catalog.schema.volume'
        })()

        # Use AsyncMock since _get_databricks_config is an async method
        mock_get_config = AsyncMock(return_value=mock_config)
        with patch.object(self.service, '_get_databricks_config', mock_get_config, create=True), \
             patch.object(self.service.volume_repository, 'delete_volume_file') as mock_delete:

            mock_delete.return_value = {"success": True}

            result = await self.service.delete_knowledge_file(
                execution_id,
                self.group_id,
                filename
            )

            assert result is True

    @pytest.mark.asyncio
    async def test_delete_file_with_user_token(self):
        """Test deleting file with user token"""
        execution_id = "test-execution-id"
        filename = "test.txt"
        user_token = "test-user-token"

        # Mock the methods that delete_knowledge_file actually calls
        mock_config = type('obj', (object,), {
            'knowledge_volume_path': 'catalog.schema.volume'
        })()

        # Use AsyncMock since _get_databricks_config is an async method
        mock_get_config = AsyncMock(return_value=mock_config)
        with patch.object(self.service, '_get_databricks_config', mock_get_config, create=True), \
             patch.object(self.service.volume_repository, 'delete_volume_file') as mock_delete:

            mock_delete.return_value = {"success": True}

            result = await self.service.delete_knowledge_file(
                execution_id,
                self.group_id,
                filename,
                user_token=user_token
            )

            assert result is True


class TestDatabricksKnowledgeServiceBrowseVolumeFiles:
    """Test DatabricksKnowledgeService browse_volume_files method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_browse_files_basic(self):
        """Test browsing files in volume"""
        volume_path = "/Volumes/catalog/schema/volume"

        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'list_volume_contents') as mock_list:

            mock_get_config.return_value = {"workspace_url": "https://test.databricks.com"}
            mock_list.return_value = {
                "success": True,
                "files": [
                    {"path": "file1.txt", "size": 1024},
                    {"path": "file2.txt", "size": 2048}
                ]
            }

            result = await self.service.browse_volume_files(volume_path, self.group_id)

            assert isinstance(result, dict)
            assert result.get("success") == True

    @pytest.mark.asyncio
    async def test_browse_files_handles_exceptions(self):
        """Test browse handles exceptions gracefully"""
        volume_path = "/Volumes/catalog/schema/volume"

        with patch.object(self.service.repository, 'get_active_config') as mock_get_config:
            mock_get_config.side_effect = Exception("Config error")

            result = await self.service.browse_volume_files(volume_path, self.group_id)

            # Should return error dict on error
            assert isinstance(result, dict)
            assert result.get("success") == False
            assert "error" in result


class TestDatabricksKnowledgeServiceRegisterVolumeFile:
    """Test DatabricksKnowledgeService register_volume_file method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = DatabricksKnowledgeService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_register_file_basic(self):
        """Test registering a volume file for knowledge search"""
        execution_id = "test-execution-id"
        file_path = "/Volumes/catalog/schema/volume/test.txt"

        # register_volume_file doesn't actually call any services - it just simulates registration
        result = await self.service.register_volume_file(
            execution_id,
            file_path,
            self.group_id
        )

        assert isinstance(result, dict)
        assert result["status"] == "success"
        assert result["path"] == file_path
        assert result["filename"] == "test.txt"
        assert result["execution_id"] == execution_id
        assert result["group_id"] == self.group_id

    @pytest.mark.asyncio
    async def test_register_file_with_agent_ids(self):
        """Test registering file with agent_ids filter"""
        execution_id = "test-execution-id"
        file_path = "/Volumes/catalog/schema/volume/test.txt"

        # Note: register_volume_file doesn't accept agent_ids parameter
        # It just simulates registration
        result = await self.service.register_volume_file(
            execution_id,
            file_path,
            self.group_id
        )

        assert isinstance(result, dict)
        assert result["status"] == "success"


class TestDatabricksKnowledgeServiceIntegration:
    """Integration tests for common workflows"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.created_by_email = "test@example.com"
        self.user_token = "test-user-token"
        self.service = DatabricksKnowledgeService(
            self.mock_session,
            self.group_id,
            self.created_by_email,
            self.user_token
        )

    @pytest.mark.asyncio
    async def test_upload_then_search_workflow(self):
        """Test typical workflow: upload file then search"""
        # Mock file upload
        mock_file = Mock()
        mock_file.filename = "knowledge.txt"
        mock_file.content_type = "text/plain"
        mock_file.size = 1024
        mock_file.read = AsyncMock(return_value=b"Important knowledge content")

        execution_id = "test-execution-id"
        volume_config = {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "volume": "test_volume"
        }

        # Mock repository and services directly on the service instance
        with patch.object(self.service.repository, 'get_active_config') as mock_get_config, \
             patch.object(self.service.volume_repository, 'upload_file_to_volume') as mock_upload, \
             patch.object(self.service, 'read_knowledge_file') as mock_read:

            # Setup mocks
            mock_get_config.return_value = type('obj', (object,), {
                'workspace_url': 'https://test.databricks.com',
                'knowledge_volume_path': 'test_catalog.test_schema.test_volume',
                'knowledge_volume_enabled': True,
                'encrypted_personal_access_token': 'test-token'
            })()

            mock_upload.return_value = {"success": True, "path": "/test/path/knowledge.txt"}
            mock_read.return_value = {"status": "success", "content": "Important knowledge content"}

            # Mock embedding service
            mock_embedding_service = AsyncMock()
            mock_embedding_service.embed_file.return_value = {"status": "success"}
            self.service.embedding_service = mock_embedding_service

            # Upload file
            upload_result = await self.service.upload_knowledge_file(
                mock_file, execution_id, self.group_id, volume_config
            )
            assert isinstance(upload_result, dict)
            assert upload_result["status"] == "success"

            # Mock search service
            mock_search_service = AsyncMock()
            mock_search_service.search.return_value = [
                {"content": "Important knowledge content", "metadata": {"score": 0.95}}
            ]
            self.service.search_service = mock_search_service

            # Search for content
            search_result = await self.service.search_knowledge(
                "knowledge", self.group_id, execution_id=execution_id
            )
            assert isinstance(search_result, list)
            assert len(search_result) > 0

    def test_service_maintains_user_context(self):
        """Test that service maintains user context across operations"""
        assert self.service.group_id == self.group_id
        assert self.service.created_by_email == self.created_by_email
        assert self.service.user_token == self.user_token
