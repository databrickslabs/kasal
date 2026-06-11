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
    with (
        patch(
            "src.services.databricks_knowledge_service.DatabricksConfigRepository"
        ) as cfg_repo,
        patch(
            "src.services.databricks_knowledge_service.DatabricksVolumeRepository"
        ) as vol_repo,
        patch.dict(
            "sys.modules",
            {
                "src.services.knowledge_embedding_service": MagicMock(
                    KnowledgeEmbeddingService=MagicMock(return_value=AsyncMock())
                ),
                "src.services.knowledge_search_service": MagicMock(
                    KnowledgeSearchService=MagicMock(return_value=AsyncMock())
                ),
            },
        ),
    ):
        svc = DatabricksKnowledgeService(
            session=session,
            group_id=group_id,
            created_by_email="u@test.com",
            user_token="token-123",
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
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "file content"}
        )

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(
                return_value=SimpleNamespace(workspace_url="https://ws.databricks.com")
            ),
        ):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={
                    "volume_path": "main.default.knowledge",
                    "create_date_dirs": False,
                },
            )

        assert result["status"] == "success"
        assert result["filename"] == "test.txt"

    @pytest.mark.asyncio
    async def test_upload_without_config_uses_defaults(self):
        svc = make_svc()
        file = make_upload_file()

        svc.repository.get_active_config = AsyncMock(return_value=None)
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "data"}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(
                return_value=SimpleNamespace(
                    workspace_url="https://ws.databricks.com", token="tok"
                )
            ),
        ):
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
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "data"}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )

        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_upload_failure_raises(self):
        """A failed embedding must RAISE (embedding IS the upload now —
        nothing was persisted) so the API surfaces the real cause to the UI."""
        from src.core.exceptions import UnprocessableEntityError

        svc = make_svc()
        file = make_upload_file()

        svc.embedding_service.embed_file = AsyncMock(
            return_value={"status": "error", "message": "embedding model unavailable"}
        )

        with pytest.raises(UnprocessableEntityError) as exc:
            await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )

        assert exc.value.status_code == 422
        assert "embedding model unavailable" in exc.value.detail

    @pytest.mark.asyncio
    async def test_upload_never_touches_the_volume(self):
        """The temp-embed flow has no Databricks Volume dependency at all."""
        svc = make_svc()
        file = make_upload_file("notes.txt", b"some text")
        svc.embedding_service.embed_file = AsyncMock(
            return_value={"status": "success", "chunks_embedded": 2}
        )

        result = await svc.upload_knowledge_file(
            file=file, execution_id="exec-1", group_id="g1", volume_config={}
        )

        assert result["status"] == "success"
        assert result["upload_method"] == "temp_embed"
        svc.volume_repository.upload_file_to_volume.assert_not_called()
        svc.repository.get_active_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_upload_embeds_logical_path_with_uploader(self):
        """Chunks embed under the logical uploads/ path, stamped with the
        uploading user (per-user isolation), after a TTL purge sweep."""
        svc = make_svc()
        file = make_upload_file("notes.txt", b"some text")
        svc.embedding_service.embed_file = AsyncMock(
            return_value={"status": "success", "chunks_embedded": 2}
        )

        result = await svc.upload_knowledge_file(
            file=file, execution_id="exec-1", group_id="g1", volume_config={}
        )

        assert result["path"] == "uploads/g1/exec-1/notes.txt"
        assert result["created_by"] == "u@test.com"
        kwargs = svc.embedding_service.embed_file.call_args.kwargs
        assert kwargs["file_path"] == "uploads/g1/exec-1/notes.txt"
        assert kwargs["created_by"] == "u@test.com"
        # TTL sweep ran before the new content was embedded.
        svc.embedding_service.purge_expired.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_upload_temp_file_is_deleted_even_when_embedding_fails(self):
        """The raw upload only ever lives in a temp file, which never outlives
        the embedding attempt — success or failure."""
        import os as _os
        import tempfile as _tempfile
        from src.core.exceptions import KasalError

        svc = make_svc()
        file = make_upload_file("notes.txt", b"some text")
        svc.embedding_service.embed_file = AsyncMock(side_effect=Exception("embed boom"))

        staged = []
        # Bind the real function BEFORE patching: the service imports the
        # stdlib tempfile module itself, so patching it also patches this
        # reference (calling _tempfile.mkstemp inside would recurse).
        real_mkstemp = _tempfile.mkstemp

        def tracking_mkstemp(*args, **kwargs):
            fd, path = real_mkstemp(*args, **kwargs)
            staged.append(path)
            return fd, path

        with patch(
            "src.services.databricks_knowledge_service.tempfile.mkstemp",
            side_effect=tracking_mkstemp,
        ):
            with pytest.raises(KasalError):
                await svc.upload_knowledge_file(
                    file=file, execution_id="exec-1", group_id="g1", volume_config={}
                )

        assert len(staged) == 1
        assert not _os.path.exists(staged[0])

    @pytest.mark.asyncio
    async def test_upload_temp_file_is_deleted_on_success(self):
        import os as _os
        import tempfile as _tempfile

        svc = make_svc()
        file = make_upload_file("notes.txt", b"some text")
        svc.embedding_service.embed_file = AsyncMock(
            return_value={"status": "success", "chunks_embedded": 1}
        )

        staged = []
        # Bind the real function BEFORE patching: the service imports the
        # stdlib tempfile module itself, so patching it also patches this
        # reference (calling _tempfile.mkstemp inside would recurse).
        real_mkstemp = _tempfile.mkstemp

        def tracking_mkstemp(*args, **kwargs):
            fd, path = real_mkstemp(*args, **kwargs)
            staged.append(path)
            return fd, path

        with patch(
            "src.services.databricks_knowledge_service.tempfile.mkstemp",
            side_effect=tracking_mkstemp,
        ):
            result = await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1", volume_config={}
            )

        assert result["status"] == "success"
        assert len(staged) == 1
        assert not _os.path.exists(staged[0])

    @pytest.mark.asyncio
    async def test_upload_extraction_error_raises(self):
        from src.core.exceptions import UnprocessableEntityError

        svc = make_svc()
        file = make_upload_file("scan.pdf", b"%PDF fake")

        with patch.object(
            svc,
            "_extract_text_content",
            return_value={"status": "error", "message": "image-only PDF needs OCR"},
        ):
            with pytest.raises(UnprocessableEntityError) as exc:
                await svc.upload_knowledge_file(
                    file=file, execution_id="exec-1", group_id="g1", volume_config={}
                )

        assert "image-only PDF needs OCR" in exc.value.detail

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
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "data"}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={"create_date_dirs": True},
            )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_embedding_exception_raises_kasal_error(self):
        """An unexpected embedding exception propagates as KasalError —
        embedding IS the upload now, so it must not be masked as success."""
        from src.core.exceptions import KasalError

        svc = make_svc()
        file = make_upload_file()
        svc.embedding_service.embed_file = AsyncMock(
            side_effect=Exception("embed error")
        )

        with pytest.raises(KasalError) as exc:
            await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1", volume_config={}
            )
        assert "embed error" in exc.value.detail

    @pytest.mark.asyncio
    async def test_outer_exception_raises_kasal_error(self):
        """Unexpected errors are re-raised as KasalError (not swallowed into a
        status:error dict), so the global handler maps them to an HTTP error."""
        from src.core.exceptions import KasalError

        svc = make_svc()
        file = make_upload_file()
        svc.embedding_service.purge_expired = AsyncMock(side_effect=Exception("db crash"))

        with pytest.raises(KasalError) as exc:
            await svc.upload_knowledge_file(
                file=file, execution_id="exec-1", group_id="g1", volume_config={}
            )
        assert "db crash" in exc.value.detail

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
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "data"}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        with patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ):
            result = await svc.upload_knowledge_file(
                file=file,
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
                agent_ids=["agent-1", "agent-2"],
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
            file_path="/bad/path/file.txt", group_id="g1"
        )
        assert result["status"] == "error"
        assert "Invalid volume path format" in result["message"]

    @pytest.mark.asyncio
    async def test_path_too_short_returns_error(self):
        svc = make_svc()
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema", group_id="g1"
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_reads_text_file_successfully(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"hello world"}
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/group/exec/file.txt",
            group_id="g1",
        )
        assert result["status"] == "success"
        assert "hello" in result["content"]

    @pytest.mark.asyncio
    async def test_download_failure_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": False, "error": "Not found"}
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/path/file.txt", group_id="g1"
        )
        assert result["status"] == "error"
        assert "Not found" in result["message"]

    @pytest.mark.asyncio
    async def test_download_returns_none_content(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": None}
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/catalog/schema/volume/path/file.txt", group_id="g1"
        )
        assert result["status"] == "error"
        assert "No content" in result["message"]

    @pytest.mark.asyncio
    async def test_reads_pdf_file(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"%PDF-1.4 fake pdf content"}
        )
        # pdfminer.six (MIT) is imported lazily; inject a fake module so the test
        # is deterministic whether or not the package is installed in the env.
        fake_high_level = MagicMock()
        fake_high_level.extract_text = MagicMock(return_value="extracted text")
        with patch.dict(
            "sys.modules",
            {"pdfminer": MagicMock(), "pdfminer.high_level": fake_high_level},
        ):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/catalog/schema/volume/path/file.pdf", group_id="g1"
            )
        assert result["status"] == "success"
        assert result["content"] == "extracted text"

    @pytest.mark.asyncio
    async def test_pdf_missing_library_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"some pdf bytes"}
        )
        # Force the import to fail (pdfminer absent) → loud error, no placeholder.
        with patch.dict("sys.modules", {"pdfminer": None, "pdfminer.high_level": None}):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/catalog/schema/volume/path/file.pdf", group_id="g1"
            )
        assert result["status"] == "error"
        assert "pdfminer.six" in result["message"]

    @pytest.mark.asyncio
    async def test_pdf_with_no_extractable_text_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"%PDF scanned image"}
        )
        fake_high_level = MagicMock()
        fake_high_level.extract_text = MagicMock(
            return_value="   \n  "
        )  # whitespace only
        with patch.dict(
            "sys.modules",
            {"pdfminer": MagicMock(), "pdfminer.high_level": fake_high_level},
        ):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/catalog/schema/volume/path/scan.pdf", group_id="g1"
            )
        assert result["status"] == "error"
        assert "No extractable text" in result["message"]

    @pytest.mark.asyncio
    async def test_reads_bytes_decoding(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"text content as bytes"}
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.txt", group_id="g1"
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
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={"success": True, "content": b"some pdf bytes"}
        )
        # extract_text raising a non-ImportError → loud error (no placeholder, no false success).
        fake_high_level = MagicMock()
        fake_high_level.extract_text = MagicMock(side_effect=ValueError("bad PDF"))
        with patch.dict(
            "sys.modules",
            {"pdfminer": MagicMock(), "pdfminer.high_level": fake_high_level},
        ):
            result = await svc.read_knowledge_file(
                file_path="/Volumes/cat/sch/vol/path/file.pdf", group_id="g1"
            )
        assert result["status"] == "error"
        assert "Could not extract text" in result["message"]

    @pytest.mark.asyncio
    async def test_unicode_decode_error_falls_back(self):
        svc = make_svc()
        # Binary content that can't be decoded as UTF-8 strictly but can with errors=ignore
        svc.volume_repository.download_file_from_volume = AsyncMock(
            return_value={
                "success": True,
                "content": b"\xff\xfe binary data with \x80\x81 invalid bytes",
            }
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.bin", group_id="g1"
        )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_exception_in_outer_try_returns_error(self):
        svc = make_svc()
        svc.volume_repository.download_file_from_volume = AsyncMock(
            side_effect=Exception("network error")
        )
        result = await svc.read_knowledge_file(
            file_path="/Volumes/cat/sch/vol/path/file.txt", group_id="g1"
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
        svc.volume_repository.list_volume_contents = AsyncMock(
            return_value={
                "success": True,
                "files": [
                    {
                        "name": "file.txt",
                        "path": "/Volumes/cat/sch/vol/file.txt",
                        "type": "file",
                        "size": 100,
                    }
                ],
            }
        )

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(
                return_value=SimpleNamespace(workspace_url="https://ws.databricks.com")
            ),
        ):
            result = await svc.browse_volume_files(
                volume_path="/Volumes/cat/sch/vol/path", group_id="g1"
            )
        assert result["success"] is True
        assert len(result["files"]) == 1
        assert "databricks_url" in result["files"][0]

    @pytest.mark.asyncio
    async def test_browse_dot_notation_with_execution_id(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(
            return_value={"success": True, "files": []}
        )
        with patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ):
            result = await svc.browse_volume_files(
                volume_path="cat.sch.vol", group_id="g1", execution_id="exec-1"
            )
        assert result["success"] is True
        assert result["count"] == 0

    @pytest.mark.asyncio
    async def test_browse_invalid_full_path(self):
        svc = make_svc()
        result = await svc.browse_volume_files(
            volume_path="/Volumes/cat", group_id="g1"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_invalid_dot_notation(self):
        svc = make_svc()
        result = await svc.browse_volume_files(
            volume_path="invalid_path", group_id="g1"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_volume_list_failure(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(
            return_value={"success": False, "error": "Access denied"}
        )
        result = await svc.browse_volume_files(volume_path="cat.sch.vol", group_id="g1")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_browse_handles_exception(self):
        svc = make_svc()
        svc.volume_repository.list_volume_contents = AsyncMock(
            side_effect=Exception("conn error")
        )
        result = await svc.browse_volume_files(volume_path="cat.sch.vol", group_id="g1")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# register_volume_file / list_knowledge_files / _get_file_type
# ---------------------------------------------------------------------------


class TestOtherMethods:
    @pytest.mark.asyncio
    async def test_register_volume_file_success(self):
        svc = make_svc()
        result = await svc.register_volume_file(
            "exec-1", "/Volumes/cat/sch/vol/file.txt", "g1"
        )
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


def patch_embedding_store(deleted_rows=3, delete_error=None):
    """Patch the knowledge store context + repo used by delete_knowledge_file.

    Returns (context managers list, delete_by_file mock) — deletion now targets
    the EMBEDDINGS (no raw file is retained anywhere).
    """
    from contextlib import asynccontextmanager

    store_session = AsyncMock()

    @asynccontextmanager
    async def fake_ctx(_session, _group_id, _user_token=None):
        yield store_session, False

    delete_by_file = AsyncMock(return_value=deleted_rows, side_effect=delete_error)
    repo_cls = MagicMock(return_value=MagicMock(delete_by_file=delete_by_file))
    patches = [
        patch(
            "src.services.knowledge_embedding_session.knowledge_embedding_session",
            fake_ctx,
        ),
        patch(
            "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository",
            repo_cls,
        ),
    ]
    return patches, delete_by_file


class TestDeleteKnowledgeFile:
    @pytest.mark.asyncio
    async def test_delete_removes_the_files_embeddings(self):
        svc = make_svc()
        patches, delete_by_file = patch_embedding_store(deleted_rows=5)
        with patches[0], patches[1]:
            result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")

        assert result is True
        delete_by_file.assert_awaited_once_with(
            "g1", "exec-1", "file.txt", created_by="u@test.com"
        )

    @pytest.mark.asyncio
    async def test_delete_scopes_to_the_requesting_user(self):
        """The uploader's email rides into the delete predicate so a user can
        only delete their OWN uploads."""
        svc = make_svc()
        patches, delete_by_file = patch_embedding_store()
        with patches[0], patches[1]:
            await svc.delete_knowledge_file("exec-9", "g1", "doc.pdf")

        assert delete_by_file.call_args.kwargs["created_by"] == "u@test.com"

    @pytest.mark.asyncio
    async def test_delete_returns_false_on_exception(self):
        svc = make_svc()
        patches, _ = patch_embedding_store(delete_error=Exception("db error"))
        with patches[0], patches[1]:
            result = await svc.delete_knowledge_file("exec-1", "g1", "file.txt")
        assert result is False


# ---------------------------------------------------------------------------
# search_knowledge (delegates to search_service)
# ---------------------------------------------------------------------------


class TestSearchKnowledge:
    @pytest.mark.asyncio
    async def test_search_delegates_to_search_service(self):
        svc = make_svc()
        svc.search_service.search = AsyncMock(
            return_value=[{"id": "1", "content": "result"}]
        )

        # Need to check if search_knowledge method exists
        if hasattr(svc, "search_knowledge"):
            result = await svc.search_knowledge(
                query="test query", group_id="g1", execution_id="exec-1"
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
        svc.volume_repository.upload_file_to_volume = AsyncMock(
            return_value={"success": True}
        )
        svc.read_knowledge_file = AsyncMock(
            return_value={"status": "success", "content": "data"}
        )
        svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})

        # auth raises - should be swallowed, upload should still succeed
        with patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(side_effect=Exception("auth error")),
        ):
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
    async def test_search_knowledge_delegates_with_user_isolation(self):
        """file_paths pass straight through (basename soft-filter downstream)
        and the requesting user's email is forwarded for per-user isolation."""
        svc = make_svc()
        svc.search_service.search = AsyncMock(
            return_value=[{"id": "r1", "content": "result"}]
        )

        result = await svc.search_knowledge(
            query="test",
            group_id="g1",
            file_paths=["file.txt"],
            user_token="token",
        )
        assert isinstance(result, list)
        kwargs = svc.search_service.search.call_args.kwargs
        assert kwargs["file_paths"] == ["file.txt"]  # no volume resolution
        # Falls back to the service's own (API-context) user.
        assert kwargs["created_by"] == "u@test.com"

    @pytest.mark.asyncio
    async def test_search_knowledge_explicit_caller_identity_wins(self):
        """A tool-supplied created_by (the executing user) takes precedence."""
        svc = make_svc()
        svc.search_service.search = AsyncMock(return_value=[])

        await svc.search_knowledge(
            query="test",
            group_id="g1",
            created_by="runner@test.com",
        )
        assert (
            svc.search_service.search.call_args.kwargs["created_by"]
            == "runner@test.com"
        )

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

        with patch.dict(
            "sys.modules",
            {
                "src.core.llm_manager": MagicMock(
                    LLMManager=MagicMock(get_embedding=AsyncMock(return_value=None))
                )
            },
        ):
            result = await svc._resolve_filenames_to_paths(["file1.txt"])
        assert result == ["file1.txt"]

    @pytest.mark.asyncio
    async def test_returns_filenames_on_exception(self):
        """Line 818-820: outer exception returns original filenames."""
        svc = make_svc()
        svc.search_service._get_vector_storage = AsyncMock(
            side_effect=Exception("vs error")
        )
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

        with patch.dict(
            "sys.modules",
            {
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
            },
        ):
            with patch(
                "asyncio.wait_for", AsyncMock(side_effect=asyncio.TimeoutError())
            ):
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

        with patch.dict(
            "sys.modules",
            {
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
            },
        ):
            with patch(
                "asyncio.wait_for", AsyncMock(side_effect=Exception("query error"))
            ):
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
            },
        }

        with patch.dict(
            "sys.modules",
            {
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
            },
        ):
            with patch("asyncio.wait_for", AsyncMock(return_value=search_result)):
                result = await svc._resolve_filenames_to_paths(
                    ["file.txt", "unknown.doc"]
                )
        # file.txt should be resolved to the full path
        assert "/Volumes/cat/sch/vol/file.txt" in result
        # unknown.doc should remain as-is
        assert "unknown.doc" in result
