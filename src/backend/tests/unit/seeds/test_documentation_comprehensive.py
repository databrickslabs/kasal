import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock, mock_open
from typing import List, Dict, Any
import json
import httpx
from pathlib import Path
from datetime import datetime
from src.schemas.memory_backend import MemoryBackendType

from src.seeds.documentation import (
    extract_content, load_best_practices, create_best_practices_content,
    mock_create_embedding, fetch_url, DOCS_URLS, BEST_PRACTICES_PATH, EMBEDDING_MODEL
)


# ---- Helpers ----

def _mock_session_cm(mock_session):
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=mock_session)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _make_backend(is_databricks=True, doc_index="idx", workspace_url="https://example.com",
                  endpoint_name="ep", created_at=None, group_id="g1", config_as_dict=True):
    b = MagicMock()
    b.is_active = True
    b.backend_type = MemoryBackendType.DATABRICKS if is_databricks else MemoryBackendType.DEFAULT
    b.group_id = group_id
    b.created_at = created_at or datetime(2025, 1, 1)
    if config_as_dict:
        b.databricks_config = {
            "document_index": doc_index,
            "workspace_url": workspace_url,
            "document_endpoint_name": endpoint_name,
            "endpoint_name": endpoint_name,
        }
    else:
        cfg = MagicMock(spec=[])
        cfg.document_index = doc_index
        cfg.workspace_url = workspace_url
        cfg.document_endpoint_name = endpoint_name
        cfg.endpoint_name = endpoint_name
        b.databricks_config = cfg
    return b


def _patch_session(mock_session):
    return patch('src.seeds.documentation.async_session_factory', return_value=_mock_session_cm(mock_session))


# ---- Constants ----

class TestDocumentationConstants:
    def test_docs_urls_defined(self):
        assert isinstance(DOCS_URLS, list) and len(DOCS_URLS) > 0
        for url in DOCS_URLS:
            assert "crewai.com" in url

    def test_best_practices_path_defined(self):
        assert isinstance(BEST_PRACTICES_PATH, Path)
        assert str(BEST_PRACTICES_PATH).endswith("tool_best_practices.json")

    def test_embedding_model_defined(self):
        assert isinstance(EMBEDDING_MODEL, str) and "databricks" in EMBEDDING_MODEL.lower()


# ---- extract_content ----

class TestExtractContent:
    def test_basic_html(self):
        result = extract_content("<html><body><h1>Title</h1><p>Text</p></body></html>")
        assert "Title" in result and "Text" in result

    def test_with_main_tag(self):
        result = extract_content("<html><body><nav>Nav</nav><main><p>Main</p></main></body></html>")
        assert "Main" in result

    def test_empty_html(self):
        assert extract_content("<html><body></body></html>").strip() == ""

    def test_exception_returns_empty(self):
        """Lines 75-77."""
        with patch('src.seeds.documentation.BeautifulSoup', side_effect=Exception("parse error")):
            assert extract_content("<html>x</html>") == ""


# ---- mock_create_embedding ----

class TestMockCreateEmbedding:
    @pytest.mark.asyncio
    async def test_basic(self):
        result = await mock_create_embedding("test")
        assert len(result) == 1024 and all(isinstance(x, float) for x in result)

    @pytest.mark.asyncio
    async def test_deterministic(self):
        assert await mock_create_embedding("x") == await mock_create_embedding("x")

    @pytest.mark.asyncio
    async def test_different(self):
        assert await mock_create_embedding("a") != await mock_create_embedding("b")


# ---- load_best_practices ----

class TestLoadBestPractices:
    def test_file_exists(self):
        data = {"tools": {}}
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mp:
            mp.exists.return_value = True
            with patch('builtins.open', mock_open(read_data=json.dumps(data))):
                assert load_best_practices() == data

    def test_file_not_exists(self):
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mp:
            mp.exists.return_value = False
            assert load_best_practices() == {}

    def test_json_error(self):
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mp:
            mp.exists.return_value = True
            with patch('builtins.open', mock_open(read_data="bad")):
                assert load_best_practices() == {}

    def test_io_error(self):
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mp:
            mp.exists.return_value = True
            with patch('builtins.open', side_effect=IOError("err")):
                assert load_best_practices() == {}


# ---- create_best_practices_content ----

class TestCreateBestPracticesContent:
    def test_empty(self):
        assert create_best_practices_content({}) == []

    def test_no_tool_key(self):
        assert create_best_practices_content({"other": 1}) == []

    def test_basic(self):
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "template": "tpl", "example": "ex"}]}}}}}
        r = create_best_practices_content(bp)
        assert len(r) > 0 and "T" in str(r)

    def test_guidelines(self):
        """Lines 153-156."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "guidelines": ["g1", "g2"]}]}}}}}
        content = "\n".join(d["content"] for d in create_best_practices_content(bp))
        assert "g1" in content and "g2" in content

    def test_description_field(self):
        """Line 160."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "description": "desc1"}]}}}}}
        content = "\n".join(d["content"] for d in create_best_practices_content(bp))
        assert "desc1" in content

    def test_implementation_field(self):
        """Line 163."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "implementation": "impl1"}]}}}}}
        content = "\n".join(d["content"] for d in create_best_practices_content(bp))
        assert "impl1" in content

    def test_benefits_field(self):
        """Lines 166-169."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "benefits": ["b1"]}]}}}}}
        content = "\n".join(d["content"] for d in create_best_practices_content(bp))
        assert "b1" in content

    def test_use_cases_field(self):
        """Lines 172-175."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{"pattern": "P", "use_cases": ["uc1"]}]}}}}}
        content = "\n".join(d["content"] for d in create_best_practices_content(bp))
        assert "uc1" in content

    def test_scenario_error_handling(self):
        """Lines 188-202."""
        bp = {"tool_best_practices": {"t": {"name": "T", "description": "d",
              "categories": {"c": {"title": "C", "practices": [{
                  "pattern": "P", "scenario": "Net fail",
                  "task_description_addon": "retry", "expected_output_addon": "graceful"
              }]}}}}}
        r = create_best_practices_content(bp)
        assert any("Net fail" in d.get("content", "") for d in r)

    def test_integration_guidelines(self):
        """Lines 206-224. Need non-empty tool_best_practices to pass line 123 check."""
        bp = {
            "tool_best_practices": {
                "t": {"name": "T", "description": "d",
                      "categories": {"c": {"title": "C", "practices": [{"pattern": "P"}]}}}
            },
            "integration_guidelines": {
                "title": "IG",
                "principles": ["p1"],
                "checklist": ["c1"]
            }
        }
        r = create_best_practices_content(bp)
        ig_docs = [d for d in r if d["source"] == "integration_guidelines"]
        assert len(ig_docs) == 1
        assert "p1" in ig_docs[0]["content"] and "c1" in ig_docs[0]["content"]


# ---- create_best_practices_chunks ----

class TestCreateBestPracticesChunks:
    @pytest.mark.asyncio
    async def test_creates_chunks(self):
        """Lines 231-259."""
        from src.seeds.documentation import create_best_practices_chunks
        docs = [{"source": "s", "title": "T", "content": "word " * 200}]
        splitter = MagicMock()
        splitter.split_text.return_value = ["c1", "c2"]
        with patch('langchain.text_splitter.RecursiveCharacterTextSplitter', return_value=splitter):
            result = await create_best_practices_chunks(docs)
        assert len(result) == 2
        assert result[0]["doc_type"] == "best_practices"
        assert result[1]["chunk_index"] == 1

    @pytest.mark.asyncio
    async def test_skips_empty(self):
        from src.seeds.documentation import create_best_practices_chunks
        docs = [{"source": "s", "title": "T", "content": ""}]
        splitter = MagicMock()
        with patch('langchain.text_splitter.RecursiveCharacterTextSplitter', return_value=splitter):
            assert await create_best_practices_chunks(docs) == []


# ---- create_documentation_chunks ----

class TestCreateDocumentationChunks:
    @pytest.mark.asyncio
    async def test_success(self):
        """Lines 264-302."""
        from src.seeds.documentation import create_documentation_chunks
        splitter = MagicMock()
        splitter.split_text.return_value = ["a", "b"]
        with patch('src.seeds.documentation.fetch_url', new_callable=AsyncMock, return_value="<html>x</html>"), \
             patch('src.seeds.documentation.extract_content', return_value="content"), \
             patch('langchain.text_splitter.RecursiveCharacterTextSplitter', return_value=splitter):
            r = await create_documentation_chunks("https://example.com/concepts/tasks")
        assert len(r) == 2
        assert r[0]["source"] == "https://example.com/concepts/tasks"
        assert "Tasks" in r[0]["title"]

    @pytest.mark.asyncio
    async def test_no_html(self):
        from src.seeds.documentation import create_documentation_chunks
        with patch('src.seeds.documentation.fetch_url', new_callable=AsyncMock, return_value=""):
            assert await create_documentation_chunks("https://example.com/x") == []

    @pytest.mark.asyncio
    async def test_no_extracted_content(self):
        from src.seeds.documentation import create_documentation_chunks
        with patch('src.seeds.documentation.fetch_url', new_callable=AsyncMock, return_value="<html>x</html>"), \
             patch('src.seeds.documentation.extract_content', return_value=""):
            assert await create_documentation_chunks("https://example.com/x") == []


# ---- check_existing_documentation ----

class TestCheckExistingDocumentation:
    """Lines 311-470."""

    @pytest.mark.asyncio
    async def test_databricks_docs_above_threshold(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "READY", "ready": True, "doc_count": 150}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, count = await check_existing_documentation()
        assert exists is True and count == 150

    @pytest.mark.asyncio
    async def test_databricks_docs_below_threshold(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "READY", "ready": True, "doc_count": 50}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, count = await check_existing_documentation()
        assert exists is False and count == 0

    @pytest.mark.asyncio
    async def test_databricks_no_workspace_url(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        b = _make_backend(workspace_url=None)
        b.databricks_config["workspace_url"] = None
        mem_svc.get_all.return_value = [b]

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_databricks_no_document_index(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        b = _make_backend(doc_index=None)
        b.databricks_config["document_index"] = None
        mem_svc.get_all.return_value = [b]

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_state_not_found(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "NOT_FOUND", "ready": False}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_state_provisioning(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "PROVISIONING", "ready": False}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_not_ready_generic_state(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "UPDATING", "ready": False}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_info_returns_failure(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": False, "message": "err"}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_info_returns_none(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = None

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_info_exception_does_not_exist(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.side_effect = Exception("does not exist")

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_info_exception_not_ready(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.side_effect = Exception("not ready")

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_index_info_exception_generic(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.side_effect = Exception("timeout")

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_doc_count_exception(self):
        """Lines 437-440."""
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]

        class FailingDict(dict):
            _fail = False
            def get(self, key, default=None):
                if key in ('doc_count', 'indexed_row_count', 'row_count'):
                    if self._fail:
                        raise RuntimeError("count err")
                    self._fail = True
                return super().get(key, default)

        info = FailingDict(success=True, state="READY", ready=True)
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = info

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, count = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_databricks_import_error(self):
        """Lines 442-445."""
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', side_effect=Exception("import err")):
            exists, _ = await check_existing_documentation()
        assert exists is False

    @pytest.mark.asyncio
    async def test_no_databricks_no_embeddings(self):
        """Lines 450-466."""
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc.get_documentation_embeddings.return_value = []

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc):
            exists, count = await check_existing_documentation()
        assert exists is False and count == 0

    @pytest.mark.asyncio
    async def test_no_databricks_embeddings_exist(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc.get_documentation_embeddings.return_value = [MagicMock()]

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc):
            exists, count = await check_existing_documentation()
        assert exists is True and count == 1

    @pytest.mark.asyncio
    async def test_outer_exception_reraises(self):
        """Lines 468-470."""
        from src.seeds.documentation import check_existing_documentation
        with patch('src.seeds.documentation.async_session_factory', side_effect=RuntimeError("db err")):
            with pytest.raises(RuntimeError, match="db err"):
                await check_existing_documentation()

    @pytest.mark.asyncio
    async def test_config_as_object(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        b = _make_backend(config_as_dict=False)
        mem_svc.get_all.return_value = [b]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "READY", "ready": True, "doc_count": 200}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, count = await check_existing_documentation()
        assert exists is True

    @pytest.mark.asyncio
    async def test_no_endpoint_name_defaults_empty(self):
        from src.seeds.documentation import check_existing_documentation
        session = AsyncMock()
        mem_svc = AsyncMock()
        doc_svc = AsyncMock()
        b = _make_backend()
        b.databricks_config = {"document_index": "idx", "workspace_url": "https://example.com",
                               "document_endpoint_name": None, "endpoint_name": None}
        mem_svc.get_all.return_value = [b]
        idx_svc = AsyncMock()
        idx_svc.get_index_info.return_value = {"success": True, "state": "READY", "ready": True, "doc_count": 200}

        with _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc):
            exists, _ = await check_existing_documentation()
        assert exists is True


# ---- seed_documentation_embeddings ----

class TestSeedDocumentationEmbeddings:
    """Lines 478-743."""

    @pytest.mark.asyncio
    async def test_local_dev_mock_embeddings(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}
        bp_chunk = {"source": "bp", "title": "BP", "content": "c", "chunk_index": 0, "total_chunks": 1, "doc_type": "best_practices"}

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={"tool_best_practices": {"t": {"name": "T", "description": "d", "categories": {"c": {"title": "C", "practices": [{"pattern": "P"}]}}}}}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]), \
             patch('src.seeds.documentation.create_best_practices_content', return_value=[{"source": "s", "title": "T", "content": "c"}]), \
             patch('src.seeds.documentation.create_best_practices_chunks', new_callable=AsyncMock, return_value=[bp_chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.1] * 1024):
            llm.get_embedding = AsyncMock(side_effect=Exception("no embed"))
            await seed_documentation_embeddings()
        assert doc_svc.create_documentation_embedding.call_count > 0

    @pytest.mark.asyncio
    async def test_not_local_no_databricks_skips(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc):
            await seed_documentation_embeddings()
        doc_svc.create_documentation_embedding.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_exception_not_local_skips(self):
        from src.seeds.documentation import seed_documentation_embeddings
        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             patch('src.seeds.documentation.async_session_factory', side_effect=Exception("err")):
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_backend_exception_local_proceeds(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        doc_svc = AsyncMock()
        call_count = 0
        def factory():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("err")
            return _mock_session_cm(session)

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             patch('src.seeds.documentation.async_session_factory', side_effect=factory), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_databricks_real_embeddings_index_ready(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        doc_svc = AsyncMock()
        idx_svc = AsyncMock()
        idx_svc.wait_for_index_ready.return_value = {"ready": True, "attempts": 1, "elapsed_time": 1.0}
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]):
            llm.get_embedding = AsyncMock(return_value=[0.1] * 1024)
            await seed_documentation_embeddings()
        assert doc_svc.create_documentation_embedding.call_count > 0

    @pytest.mark.asyncio
    async def test_databricks_index_not_ready_exits(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        doc_svc = AsyncMock()
        idx_svc = AsyncMock()
        idx_svc.wait_for_index_ready.return_value = {"ready": False, "message": "timeout", "attempts": 5, "elapsed_time": 120.0}

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}):
            llm.get_embedding = AsyncMock(return_value=[0.1] * 1024)
            await seed_documentation_embeddings()
        doc_svc.create_documentation_embedding.assert_not_called()

    @pytest.mark.asyncio
    async def test_databricks_index_validation_error(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        doc_svc = AsyncMock()
        idx_svc = AsyncMock()
        idx_svc.wait_for_index_ready.side_effect = Exception("validation err")
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.1] * 1024):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_databricks_missing_config(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        b = _make_backend()
        b.databricks_config = {"document_index": None, "workspace_url": None, "endpoint_name": None}
        mem_svc.get_all.return_value = [b]
        doc_svc = AsyncMock()

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_chunk_processing_error(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        doc_svc.create_documentation_embedding.side_effect = Exception("save err")
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.1] * 1024):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_url_processing_error(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, side_effect=Exception("url err")):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_real_embedding_fallback(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}
        count = 0
        async def embed_se(*a, **kw):
            nonlocal count
            count += 1
            if count == 1:
                return [0.1] * 1024
            raise Exception("fail")

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.2] * 1024):
            llm.get_embedding = AsyncMock(side_effect=embed_se)
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_best_practices_error(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={"tool_best_practices": {}}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]), \
             patch('src.seeds.documentation.create_best_practices_content', side_effect=Exception("bp err")):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_best_practice_chunk_error(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        doc_svc.create_documentation_embedding.side_effect = Exception("bp chunk err")
        bp_chunk = {"source": "bp", "title": "BP", "content": "c", "chunk_index": 0, "total_chunks": 1, "doc_type": "best_practices"}

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={"tool_best_practices": {"t": {"name": "T", "description": "d", "categories": {"c": {"title": "C", "practices": [{"pattern": "P"}]}}}}}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]), \
             patch('src.seeds.documentation.create_best_practices_content', return_value=[{"source": "s", "title": "T", "content": "c"}]), \
             patch('src.seeds.documentation.create_best_practices_chunks', new_callable=AsyncMock, return_value=[bp_chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.1] * 1024):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_best_practice_real_embedding(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        bp_chunk = {"source": "bp", "title": "BP", "content": "c", "chunk_index": 0, "total_chunks": 1, "doc_type": "best_practices"}

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={"tool_best_practices": {"t": {"name": "T", "description": "d", "categories": {"c": {"title": "C", "practices": [{"pattern": "P"}]}}}}}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]), \
             patch('src.seeds.documentation.create_best_practices_content', return_value=[{"source": "s", "title": "T", "content": "c"}]), \
             patch('src.seeds.documentation.create_best_practices_chunks', new_callable=AsyncMock, return_value=[bp_chunk]):
            llm.get_embedding = AsyncMock(return_value=[0.1] * 1024)
            await seed_documentation_embeddings()
        assert doc_svc.create_documentation_embedding.call_count == 1

    @pytest.mark.asyncio
    async def test_best_practice_real_embedding_fallback(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = []
        doc_svc = AsyncMock()
        bp_chunk = {"source": "bp", "title": "BP", "content": "c", "chunk_index": 0, "total_chunks": 1, "doc_type": "best_practices"}
        count = 0
        async def embed_se(*a, **kw):
            nonlocal count
            count += 1
            if count == 1:
                return [0.1] * 1024
            raise Exception("fail on bp")

        with patch.dict('os.environ', {"DATABASE_TYPE": "sqlite"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={"tool_best_practices": {"t": {"name": "T", "description": "d", "categories": {"c": {"title": "C", "practices": [{"pattern": "P"}]}}}}}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]), \
             patch('src.seeds.documentation.create_best_practices_content', return_value=[{"source": "s", "title": "T", "content": "c"}]), \
             patch('src.seeds.documentation.create_best_practices_chunks', new_callable=AsyncMock, return_value=[bp_chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.2] * 1024):
            llm.get_embedding = AsyncMock(side_effect=embed_se)
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_databricks_config_as_object(self):
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        b = _make_backend(config_as_dict=False)
        mem_svc.get_all.return_value = [b]
        doc_svc = AsyncMock()
        idx_svc = AsyncMock()
        idx_svc.wait_for_index_ready.return_value = {"ready": True, "attempts": 1, "elapsed_time": 1.0}

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[]):
            llm.get_embedding = AsyncMock(return_value=[0.1] * 1024)
            await seed_documentation_embeddings()

    @pytest.mark.asyncio
    async def test_databricks_with_chunks_success_log(self):
        """Lines 741-743."""
        from src.seeds.documentation import seed_documentation_embeddings
        session = AsyncMock()
        mem_svc = AsyncMock()
        mem_svc.get_all.return_value = [_make_backend()]
        doc_svc = AsyncMock()
        idx_svc = AsyncMock()
        idx_svc.wait_for_index_ready.return_value = {"ready": True, "attempts": 1, "elapsed_time": 1.0}
        chunk = {"source": "u", "title": "T", "content": "c", "chunk_index": 0, "total_chunks": 1}

        with patch.dict('os.environ', {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote"}, clear=False), \
             _patch_session(session), \
             patch('src.services.memory_backend_service.MemoryBackendService', return_value=mem_svc), \
             patch('src.seeds.documentation.DocumentationEmbeddingService', return_value=doc_svc), \
             patch('src.services.databricks_index_service.DatabricksIndexService', return_value=idx_svc), \
             patch('src.seeds.documentation.LLMManager') as llm, \
             patch('src.seeds.documentation.load_best_practices', return_value={}), \
             patch('src.seeds.documentation.create_documentation_chunks', new_callable=AsyncMock, return_value=[chunk]), \
             patch('src.seeds.documentation.mock_create_embedding', new_callable=AsyncMock, return_value=[0.1] * 1024):
            llm.get_embedding = AsyncMock(side_effect=Exception("no"))
            await seed_documentation_embeddings()


# ---- seed_async ----

class TestSeedAsync:
    """Lines 747-756."""

    @pytest.mark.asyncio
    async def test_seed_returns_true_docs_above_100(self):
        from src.seeds.documentation import seed_async
        with patch('src.seeds.documentation.seed', new_callable=AsyncMock, return_value=True), \
             patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(True, 200)):
            assert await seed_async() == ("skipped", 200)

    @pytest.mark.asyncio
    async def test_seed_returns_true_docs_below_100(self):
        from src.seeds.documentation import seed_async
        with patch('src.seeds.documentation.seed', new_callable=AsyncMock, return_value=True), \
             patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(True, 50)):
            assert await seed_async() == ("success", 50)

    @pytest.mark.asyncio
    async def test_seed_returns_true_no_docs(self):
        from src.seeds.documentation import seed_async
        with patch('src.seeds.documentation.seed', new_callable=AsyncMock, return_value=True), \
             patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(False, 0)):
            assert await seed_async() == ("error", 0)

    @pytest.mark.asyncio
    async def test_seed_returns_false(self):
        from src.seeds.documentation import seed_async
        with patch('src.seeds.documentation.seed', new_callable=AsyncMock, return_value=False):
            assert await seed_async() == ("error", 0)


# ---- seed_sync ----

class TestSeedSync:
    """Lines 760-770."""

    def test_existing_loop(self):
        from src.seeds.documentation import seed_sync
        mock_loop = MagicMock()
        mock_loop.run_until_complete.return_value = ("success", 10)
        with patch('asyncio.get_event_loop', return_value=mock_loop):
            assert seed_sync() == ("success", 10)

    def test_no_loop(self):
        from src.seeds.documentation import seed_sync
        mock_loop = MagicMock()
        mock_loop.run_until_complete.return_value = ("error", 0)
        with patch('asyncio.get_event_loop', side_effect=RuntimeError("no loop")), \
             patch('asyncio.new_event_loop', return_value=mock_loop), \
             patch('asyncio.set_event_loop'):
            assert seed_sync() == ("error", 0)


# ---- seed ----

class TestSeed:
    """Lines 774-804."""

    @pytest.mark.asyncio
    async def test_already_exists(self):
        from src.seeds.documentation import seed
        with patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(True, 100)):
            assert await seed() is True

    @pytest.mark.asyncio
    async def test_check_fails_then_seeds(self):
        from src.seeds.documentation import seed
        with patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, side_effect=Exception("err")), \
             patch('src.seeds.documentation.seed_documentation_embeddings', new_callable=AsyncMock):
            assert await seed() is True

    @pytest.mark.asyncio
    async def test_needs_seeding_success(self):
        from src.seeds.documentation import seed
        with patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(False, 0)), \
             patch('src.seeds.documentation.seed_documentation_embeddings', new_callable=AsyncMock):
            assert await seed() is True

    @pytest.mark.asyncio
    async def test_seeding_fails(self):
        from src.seeds.documentation import seed
        with patch('src.seeds.documentation.check_existing_documentation', new_callable=AsyncMock, return_value=(False, 0)), \
             patch('src.seeds.documentation.seed_documentation_embeddings', new_callable=AsyncMock, side_effect=Exception("err")):
            assert await seed() is False


# ---- __main__ block ----

class TestMainBlock:
    """Lines 808-809."""

    def test_main_block_runs(self):
        """Execute the actual source file's __main__ block via runpy."""
        import runpy
        with patch('src.seeds.documentation.seed', new_callable=AsyncMock, return_value=True) as mock_seed, \
             patch('asyncio.run') as mock_run:
            runpy.run_module('src.seeds.documentation', run_name='__main__', alter_sys=False)
            mock_run.assert_called_once()


# ---- fetch_url ----

class TestFetchUrl:
    @pytest.mark.asyncio
    async def test_success(self):
        client = AsyncMock()
        resp = MagicMock()
        resp.text = "<html>ok</html>"
        resp.raise_for_status = MagicMock()
        client.get.return_value = resp
        with patch('src.seeds.documentation.httpx.AsyncClient') as MC:
            MC.return_value.__aenter__ = AsyncMock(return_value=client)
            MC.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await fetch_url("https://example.com") == "<html>ok</html>"

    @pytest.mark.asyncio
    async def test_http_error(self):
        client = AsyncMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError(
            "err", request=httpx.Request("GET", "https://example.com"), response=httpx.Response(404)))
        client.get.return_value = resp
        with patch('src.seeds.documentation.httpx.AsyncClient') as MC:
            MC.return_value.__aenter__ = AsyncMock(return_value=client)
            MC.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await fetch_url("https://example.com") == ""

    @pytest.mark.asyncio
    async def test_connect_error(self):
        client = AsyncMock()
        client.get.side_effect = httpx.ConnectError("fail")
        with patch('src.seeds.documentation.httpx.AsyncClient') as MC:
            MC.return_value.__aenter__ = AsyncMock(return_value=client)
            MC.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await fetch_url("https://example.com") == ""
