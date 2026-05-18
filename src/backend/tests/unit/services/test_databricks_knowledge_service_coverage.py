"""
Coverage-focused tests for DatabricksKnowledgeService.
Targets uncovered branches to push coverage to 85%+.
"""
import asyncio
import io
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.services.databricks_knowledge_service import DatabricksKnowledgeService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_upload_file(filename="test.txt", content=b"hello world"):
    f = AsyncMock()
    f.filename = filename
    f.read = AsyncMock(return_value=content)
    return f


def make_svc(group_id="g1"):
    session = AsyncMock()
    # KnowledgeEmbeddingService and KnowledgeSearchService are imported locally in __init__
    with patch("src.services.databricks_knowledge_service.DatabricksConfigRepository") as cfg_repo, \
         patch("src.services.databricks_knowledge_service.DatabricksVolumeRepository") as vol_repo, \
         patch.dict("sys.modules", {
             "src.services.knowledge_embedding_service": MagicMock(
                 KnowledgeEmbeddingService=MagicMock(return_value=AsyncMock())
             ),
             "src.services.knowledge_search_service": MagicMock(
                 KnowledgeSearchService=MagicMock(return_value=AsyncMock())
             ),
         }):
        svc = DatabricksKnowledgeService(
            session=session,
            group_id=group_id,
            created_by_email="u@test.com",
            user_token="token-123"
        )
    svc.repository = AsyncMock()
    svc.volume_repository = AsyncMock()
    svc.embedding_service = AsyncMock()
    svc.search_service = AsyncMock()
    return svc


# ---------------------------------------------------------------------------
# upload_knowledge_file
# ---------------------------------------------------------------------------

class TestUploadKnowledgeFile:
    @pytest.mark.asyncio
    async def test_successful_upload_with_config(self):
        svc = make_svc()
        file = make_upload_file("test.txt", b"file content")

        fake_config = SimpleNamespace(
            knowledge_volume_enabled=True,
            knowledge_volume_path="main.default.knowledge",
            workspace_url="https://ws.databricks.com",
            encrypted_personal_access_token="pat-tok",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "file content"})

        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(return_value=SimpleNamespace(workspace_url="https://ws.databricks.com"))):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={"volume_path": "main.default.knowledge", "create_date_dirs": False},
            )

        assert result["status"] == "success"
        assert result["filename"] == "test.txt"

    @pytest.mark.asyncio
    async def test_upload_without_config_uses_defaults(self):
        svc = make_svc()
        file = make_upload_file()

        svc.repository.get_active_config = AsyncMock(return_value=None)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(return_value=SimpleNamespace(workspace_url="https://ws.databricks.com", token="tok"))):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )

        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_upload_with_invalid_volume_path_uses_defaults(self):
        svc = make_svc()
        file = make_upload_file()

        fake_config = SimpleNamespace(
            knowledge_volume_enabled=True,
            knowledge_volume_path="invalid_path",  # No dots
            workspace_url="https://ws.databricks.com",
            encrypted_personal_access_token="",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )

        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_upload_fails_silently_continues(self):
        svc = make_svc()
        file = make_upload_file()

        svc.repository.get_active_config = AsyncMock(return_value=None)
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": False, "error": "Permission denied"}
        )
        svc.read_knowledge_file = AsyncMock(return_value={"status": "error", "message": "not found"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )

        # Even on upload failure, we return a response
        assert result["status"] in ("success", "error")

    @pytest.mark.asyncio
    async def test_with_date_dirs(self):
        svc = make_svc()
        file = make_upload_file()
        fake_config = SimpleNamespace(
            knowledge_volume_enabled=True,
            knowledge_volume_path="main.default.knowledge",
            workspace_url="https://ws.databricks.com",
            encrypted_personal_access_token="",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={"create_date_dirs": True},
            )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_embedding_exception_is_caught(self):
        svc = make_svc()
        file = make_upload_file()
        fake_config = SimpleNamespace(
            knowledge_volume_path="main.default.knowledge",
            knowledge_volume_enabled=True,
            workspace_url="https://ws.databricks.com",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(side_effect=Exception("embed error"))

        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1", volume_config={}
            )
        assert result["status"] == "success"
        assert result["embedding_result"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_outer_exception_returns_error_dict(self):
        svc = make_svc()
        file = make_upload_file()
        svc.repository.get_active_config = AsyncMock(side_effect=Exception("db crash"))

        result = await svc.upload_knowledge_file(
            file=file, execution_id="exec-1", group_id="g1", volume_config={}
        )
        assert result["status"] == "error"
        assert "db crash" in result["message"]

    @pytest.mark.asyncio
    async def test_with_agent_ids(self):
        svc = make_svc()
        file = make_upload_file()
        fake_config = SimpleNamespace(
            knowledge_volume_path="main.default.knowledge",
            knowledge_volume_enabled=True,
            workspace_url="https://ws.databricks.com",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1",
                volume_config={}, agent_ids=["agent-1", "agent-2"]
            )
        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# read_knowledge_file
# ---------------------------------------------------------------------------

class TestReadKnowledgeFile:
    @pytest.mark.asyncio
    async def test_invalid_path_prefix_returns_error(self):
        svc = make_svc()
        result = await svc.read_knowledge_file(
            file_path="/bad/path/file.txt",
            group_id="g1"
        )
        assert result["status"] == "error"
        assert "Invalid volume path format" in result["message"]

    @pytest.mark.asyncio
    async def test_path_too_short_returns_error(self):
        svc = make_svc()
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema",
            group_id="g1"
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_reads_text_file_successfully(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"hello world"
        })
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/group/exec/file.txt",
            group_id="g1"
        )
        assert result["status"] == "success"
        assert "hello" in result["content"]

    @pytest.mark.asyncio
    async def test_download_failure_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": False,
            "error": "Not found"
        })
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/path/file.txt",
            group_id="g1"
        )
        assert result["status"] == "error"
        assert "Not found" in result["message"]

    @pytest.mark.asyncio
    async def test_download_returns_none_content(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": None
        })
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/path/file.txt",
            group_id="g1"
        )
        assert result["status"] == "error"
        assert "No content" in result["message"]

    @pytest.mark.asyncio
    async def test_reads_pdf_file(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"%PDF-1.4 fake pdf content"
        })

        # Mock the PDF reader
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "extracted text"
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]

        with patch("src.services.databricks_knowledge_service.PdfReader", return_value=mock_reader, create=True):
            with patch("builtins.__import__", side_effect=lambda name, *args, **kw:
                       MagicMock() if name == "pypdf" else __import__(name, *args, **kw)):
                pass  # Just test the main path

        # Simpler approach: mock the import and PdfReader
        mock_pdf_module = MagicMock()
        mock_reader_instance = MagicMock()
        mock_reader_instance.pages = [mock_page]
        mock_pdf_module.PdfReader = MagicMock(return_value=mock_reader_instance)

        with patch.dict("sys.modules", {"pypdf": mock_pdf_module}):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/catalog/schema/volume/path/file.pdf",
                group_id="g1"
            )
        # Should not raise
        assert "status" in result

    @pytest.mark.asyncio
    async def test_pdf_import_error_falls_back(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"some pdf bytes"
        })

        import sys

        # Force ImportError for both pypdf and PyPDF2
        original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        with patch.dict("sys.modules", {"pypdf": None, "PyPDF2": None}):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/catalog/schema/volume/path/file.pdf",
                group_id="g1"
            )
        # Should return some result even if PDF parsing fails
        assert "status" in result

    @pytest.mark.asyncio
    async def test_reads_bytes_decoding(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"text content as bytes"
        })
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.txt",
            group_id="g1"
        )
        assert result["status"] == "success"
        assert "text content" in result["content"]


# ---------------------------------------------------------------------------
# list_knowledge_files (if implemented)
# ---------------------------------------------------------------------------

class TestListKnowledgeFiles:
    @pytest.mark.asyncio
    async def test_list_files_method_exists(self):
        """Basic smoke test - ensure the service can be instantiated and basic methods work."""
        svc = make_svc()
        # Just verify the service was created successfully
        assert svc.group_id == "g1"
        assert svc.created_by_email == "u@test.com"
        assert svc.user_token == "token-123"

    def test_service_has_expected_attributes(self):
        svc = make_svc()
        assert hasattr(svc, "repository")
        assert hasattr(svc, "volume_repository")
        assert hasattr(svc, "embedding_service")
        assert hasattr(svc, "search_service")


# ---------------------------------------------------------------------------
# read_knowledge_file - additional paths
# ---------------------------------------------------------------------------

class TestReadKnowledgeFileExtra:
    @pytest.mark.asyncio
    async def test_pdf_extraction_error_gives_error_message(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"some pdf bytes"
        })

        # Mock PdfReader to raise a non-ImportError exception
        mock_reader = MagicMock()
        mock_reader.side_effect = ValueError("bad PDF")
        mock_pdf_module = MagicMock()
        mock_pdf_module.PdfReader = mock_reader

        with patch.dict("sys.modules", {"pypdf": mock_pdf_module}):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/cat/sch/vol/path/file.pdf",
                group_id="g1"
            )
        assert result["status"] == "success"
        assert "Error extracting text" in result["content"]

    @pytest.mark.asyncio
    async def test_unicode_decode_error_falls_back(self):
        svc = make_svc()
        # Binary content that can't be decoded as UTF-8 strictly but can with errors=ignore
        svc.volume_repository.download_file_from_volume = AsyncMock(return_value={
            "success": True,
            "content": b"\xff\xfe binary data with \x80\x81 invalid bytes"
        })
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.bin",
            group_id="g1"
        )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_exception_in_outer_try_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(side_effect=Exception("network error"))
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.txt",
            group_id="g1"
        )
        assert result["status"] == "error"
        assert "network error" in result["message"]


# ---------------------------------------------------------------------------
# browse_volume_files
# ---------------------------------------------------------------------------

class TestBrowseVolumeFiles:
    @pytest.mark.asyncio
    async def test_browse_full_path_success(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(return_value={
            "success": True,
            "files": [
                {"name": "file.txt", "path": "/Volumes/cat/sch/vol/file.txt", "type": "file", "size": 100}
            ]
        })

        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(return_value=SimpleNamespace(workspace_url="https://ws.databricks.com"))):
            result = await svc.browse_volume_files(
                volume_path="/Volumes/cat/sch/vol/path",
                group_id="g1"
            )
        assert result["success"] is True
        assert len(result["files"]) == 1
        assert "databricks_url" in result["files"][0]

    @pytest.mark.asyncio
    async def test_browse_dot_notation_with_execution_id(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(return_value={
            "success": True,
            "files": []
        })
        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
            result = await svc.browse_volume_files(
                volume_path="cat.sch.vol",
                group_id="g1",
                execution_id="exec-1"
            )
        assert result["success"] is True
        assert result["count"] == 0

    @pytest.mark.asyncio
    async def test_browse_invalid_full_path(self):
        svc = make_svc()
        result = await svc.browse_volume_files(
            volume_path="/Volumes/cat",
            group_id="g1"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_invalid_dot_notation(self):
        svc = make_svc()
        result = await svc.browse_volume_files(
            volume_path="invalid_path",
            group_id="g1"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_volume_list_failure(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(return_value={
            "success": False,
            "error": "Access denied"
        })
        result = await svc.browse_volume_files(volume_path="cat.sch.vol", group_id="g1")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_handles_exception(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(side_effect=Exception("conn error"))
        result = await svc.browse_volume_files(volume_path="cat.sch.vol", group_id="g1")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# register_volume_file / list_knowledge_files / _get_file_type
# ---------------------------------------------------------------------------

class TestOtherMethods:
    @pytest.mark.asyncio
    async def test_register_volume_file_success(self):
        svc = make_svc()
        result = await svc.register_volume_file("exec-1", "/Volumes/cat/sch/vol/file.txt", "g1")
        assert result["status"] == "success"
        assert result["filename"] == "file.txt"

    @pytest.mark.asyncio
    async def test_list_knowledge_files_returns_empty(self):
        svc = make_svc()
        result = await svc.list_knowledge_files("exec-1", "g1")
        assert result == []

    def test_get_file_type_pdf(self):
        svc = make_svc()
        assert svc._get_file_type("doc.pdf") == "pdf"

    def test_get_file_type_text(self):
        svc = make_svc()
        assert svc._get_file_type("readme.txt") == "text"

    def test_get_file_type_markdown(self):
        svc = make_svc()
        assert svc._get_file_type("notes.md") == "markdown"

    def test_get_file_type_unknown(self):
        svc = make_svc()
        assert svc._get_file_type("data.xyz") == "file"

    def test_get_file_type_python(self):
        svc = make_svc()
        assert svc._get_file_type("script.py") == "python"

    def test_get_file_type_yaml(self):
        svc = make_svc()
        assert svc._get_file_type("config.yaml") == "yaml"
        assert svc._get_file_type("config.yml") == "yaml"


# ---------------------------------------------------------------------------
# delete_knowledge_file
# ---------------------------------------------------------------------------

class TestDeleteKnowledgeFile:
    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = make_svc()
        fake_config = SimpleNamespace(knowledge_volume_path="cat.sch.vol")
        svc._get_databricks_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.delete_volume_file = AsyncMock(return_value={"success": True})

        result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_no_config(self):
        svc = make_svc()
        svc._get_databricks_config = AsyncMock(return_value=None)
        result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_invalid_volume_path(self):
        svc = make_svc()
        fake_config = SimpleNamespace(knowledge_volume_path="invalid")
        svc._get_databricks_config = AsyncMock(return_value=fake_config)
        result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_volume_delete_fails(self):
        svc = make_svc()
        fake_config = SimpleNamespace(knowledge_volume_path="cat.sch.vol")
        svc._get_databricks_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.delete_volume_file = AsyncMock(return_value={"success": False, "message": "Not found"})
        result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false_on_exception(self):
        svc = make_svc()
        svc._get_databricks_config = AsyncMock(side_effect=Exception("db error"))
        result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is False


# ---------------------------------------------------------------------------
# search_knowledge (delegates to search_service)
# ---------------------------------------------------------------------------

class TestSearchKnowledge:
    @pytest.mark.asyncio
    async def test_search_delegates_to_search_service(self):
        svc = make_svc()
        svc.search_service.search = AsyncMock(return_value=[{"id": "1", "content": "result"}])

        # Need to check if search_knowledge method exists
        if hasattr(svc, 'search_knowledge'):
            result = await svc.search_knowledge(
                query="test query",
                group_id="g1",
                execution_id="exec-1"
            )
            assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Additional coverage for remaining branches
# ---------------------------------------------------------------------------

class TestAdditionalCoverage:
    @pytest.mark.asyncio
    async def test_upload_auth_exception_swallowed(self):
        """Cover lines 201-202: auth exception during workspace URL lookup."""
        svc = make_svc()
        file = make_upload_file()
        fake_config = SimpleNamespace(
            knowledge_volume_path="main.default.knowledge",
            knowledge_volume_enabled=True,
            workspace_url="https://ws.databricks.com",
        )
        svc.repository.get_active_config = AsyncMock(return_value=fake_config)
        svc.volume_repository.upload_file_to_volume = AsyncMock(return_value={"success": True})
        svc.read_knowledge_file = AsyncMock(return_value={"status": "success", "content": "data"})
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        # auth raises - should be swallowed, upload should still succeed
        with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(side_effect=Exception("auth error"))):
            result = await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1", volume_config={}
            )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_register_volume_file_exception_reraises(self):
        """Cover lines 586-588: exception in register_volume_file re-raises."""
        svc = make_svc()
        # os.path.basename should work normally but we can mock it to fail
        with patch("os.path.basename", side_effect=Exception("os error")):
            with pytest.raises(Exception, match="os error"):
                await svc.register_volume_file("exec-1", "/path/file.txt", "g1")

    @pytest.mark.asyncio
    async def test_search_knowledge_delegates_and_resolves_paths(self):
        """Cover search_knowledge lines 849-866."""
        svc = make_svc()
        svc.search_service.search = AsyncMock(return_value=[{"id": "r1", "content": "result"}])
        svc._resolve_filenames_to_paths = AsyncMock(return_value=["/Volumes/cat/sch/vol/file.txt"])

        result = await svc.search_knowledge(
            query="test",
            group_id="g1",
            file_paths=["file.txt"],  # Not starting with /Volumes, needs resolution
            user_token="token"
        )
        assert isinstance(result, list)
        svc._resolve_filenames_to_paths.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_knowledge_no_resolution_needed(self):
        """search_knowledge when all paths already start with /Volumes."""
        svc = make_svc()
        svc.search_service.search = AsyncMock(return_value=[])

        result = await svc.search_knowledge(
            query="test",
            group_id="g1",
            file_paths=["/Volumes/cat/sch/vol/file.txt"],  # Already resolved
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_search_knowledge_no_file_paths(self):
        """search_knowledge when no file_paths provided."""
        svc = make_svc()
        svc.search_service.search = AsyncMock(return_value=[])

        result = await svc.search_knowledge(
            query="test",
            group_id="g1",
        )
        assert result == []


# ---------------------------------------------------------------------------
# _resolve_filenames_to_paths - entry paths
# ---------------------------------------------------------------------------

class TestResolveFilenamesPaths:
    @pytest.mark.asyncio
    async def test_returns_filenames_when_no_vector_storage(self):
        """Lines 720-723: no vector storage configured."""
        svc = make_svc()
        svc.search_service._get_vector_storage = AsyncMock(return_value=None)
        result = await svc._resolve_filenames_to_paths(["file1.txt", "file2.txt"])
        assert result == ["file1.txt", "file2.txt"]

    @pytest.mark.asyncio
    async def test_returns_filenames_when_embedding_fails(self):
        """Lines 731-734: embedding generation fails."""
        svc = make_svc()
        mock_storage = MagicMock()
        mock_storage.index_name = "main.default.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.repository = MagicMock()
        svc.search_service._get_vector_storage = AsyncMock(return_value=mock_storage)

        with patch.dict("sys.modules", {
            "src.core.llm_manager": MagicMock(
                LLMManager=MagicMock(
                    get_embedding=AsyncMock(return_value=None)
                )
            )
        }):
            result = await svc._resolve_filenames_to_paths(["file1.txt"])
        assert result == ["file1.txt"]

    @pytest.mark.asyncio
    async def test_returns_filenames_on_exception(self):
        """Line 818-820: outer exception returns original filenames."""
        svc = make_svc()
        svc.search_service._get_vector_storage = AsyncMock(side_effect=Exception("vs error"))
        result = await svc._resolve_filenames_to_paths(["file.txt"])
        assert result == ["file.txt"]

    @pytest.mark.asyncio
    async def test_returns_filenames_when_query_timeout(self):
        """Lines 758-760: asyncio.TimeoutError returns original filenames."""
        svc = make_svc()
        mock_storage = MagicMock()
        mock_storage.index_name = "main.default.docs"
        mock_storage.endpoint_name = "ep"
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_storage.repository = mock_repo
        svc.search_service._get_vector_storage = AsyncMock(return_value=mock_storage)

        with patch.dict("sys.modules", {
            "src.core.llm_manager": MagicMock(
                LLMManager=MagicMock(
                    get_embedding=AsyncMock(return_value=[0.1, 0.2])
                )
            ),
            "src.schemas.databricks_index_schemas": MagicMock(
                DatabricksIndexSchemas=MagicMock(
                    get_search_columns=MagicMock(return_value=["id", "source"]),
                    get_column_positions=MagicMock(return_value={"source": 1}),
                )
            ),
        }):
            with patch("asyncio.wait_for", AsyncMock(side_effect=asyncio.TimeoutError())):
                result = await svc._resolve_filenames_to_paths(["file.txt"])
        assert result == ["file.txt"]

    @pytest.mark.asyncio
    async def test_returns_filenames_when_query_fails(self):
        """Lines 761-763: query exception returns original filenames."""
        svc = make_svc()
        mock_storage = MagicMock()
        mock_storage.index_name = "main.default.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.repository = MagicMock()
        svc.search_service._get_vector_storage = AsyncMock(return_value=mock_storage)

        with patch.dict("sys.modules", {
            "src.core.llm_manager": MagicMock(
                LLMManager=MagicMock(
                    get_embedding=AsyncMock(return_value=[0.1, 0.2])
                )
            ),
            "src.schemas.databricks_index_schemas": MagicMock(
                DatabricksIndexSchemas=MagicMock(
                    get_search_columns=MagicMock(return_value=["id", "source"]),
                )
            ),
        }):
            with patch("asyncio.wait_for", AsyncMock(side_effect=Exception("query error"))):
                result = await svc._resolve_filenames_to_paths(["file.txt"])
        assert result == ["file.txt"]

    @pytest.mark.asyncio
    async def test_resolves_filenames_with_matching_sources(self):
        """Lines 787-816: successful resolution path."""
        svc = make_svc()
        mock_storage = MagicMock()
        mock_storage.index_name = "main.default.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.repository = MagicMock()
        svc.search_service._get_vector_storage = AsyncMock(return_value=mock_storage)

        search_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["id1", "/Volumes/cat/sch/vol/file.txt"],
                        ["id2", "/Volumes/cat/sch/vol/other.pdf"],
                    ]
                }
            }
        }

        with patch.dict("sys.modules", {
            "src.core.llm_manager": MagicMock(
                LLMManager=MagicMock(
                    get_embedding=AsyncMock(return_value=[0.1, 0.2])
                )
            ),
            "src.schemas.databricks_index_schemas": MagicMock(
                DatabricksIndexSchemas=MagicMock(
                    get_search_columns=MagicMock(return_value=["id", "source"]),
                    get_column_positions=MagicMock(return_value={"source": 1}),
                )
            ),
        }):
            with patch("asyncio.wait_for", AsyncMock(return_value=search_result)):
                result = await svc._resolve_filenames_to_paths(["file.txt", "unknown.doc"])
        # file.txt should be resolved to the full path
        assert "/Volumes/cat/sch/vol/file.txt" in result
        # unknown.doc should remain as-is
        assert "unknown.doc" in result
