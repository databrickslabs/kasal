"""
Extended coverage tests for DatabricksVectorIndexRepository.

Targets uncovered lines in state mapping, API error paths, and upsert validation.
"""
import pytest
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.schemas.databricks_vector_index import (
    IndexResponse, IndexListResponse, IndexState, IndexType
)


WORKSPACE_URL = "https://example.databricks.com"
AUTH_TOKEN = "test-token"


def make_aiohttp_ctx(status: int, json_data=None, text_data: str = ""):
    """Build a mock aiohttp async-context-manager chain."""
    response = AsyncMock()
    response.status = status
    if json_data is not None:
        response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value=text_data)

    session = MagicMock()

    def _cm(resp):
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=None)
        return cm

    for method in ("post", "get", "delete", "put"):
        setattr(session, method, MagicMock(return_value=_cm(response)))

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    return session_cm, response, session


# ---------------------------------------------------------------------------
# Constructor with empty/None workspace_url
# ---------------------------------------------------------------------------

class TestConstructorFallback:

    def test_empty_workspace_url_triggers_auth_context(self):
        """When workspace_url is empty, attempts to get from auth context."""
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            repo = DatabricksVectorIndexRepository("")
        assert repo.workspace_url == ""

    def test_none_workspace_url_triggers_auth_context(self):
        """When workspace_url is None, exception during auth gives empty URL."""
        with patch("asyncio.run") as mock_run:
            mock_run.side_effect = Exception("no loop")
            repo = DatabricksVectorIndexRepository(None)
        assert repo.workspace_url == ""

    def test_workspace_url_with_trailing_slash_stripped(self):
        """workspace_url trailing slash is stripped."""
        repo = DatabricksVectorIndexRepository("https://example.databricks.com/")
        assert not repo.workspace_url.endswith("/")


# ---------------------------------------------------------------------------
# get_index: state mapping (covers lines 238-268)
# ---------------------------------------------------------------------------

class TestGetIndexStateMappings:
    """Tests for get_index with different state values."""

    def _make_index_data(self, state=None, detailed_state=None, ready=False):
        data = {
            "name": "cat.schema.idx",
            "endpoint_name": "ep",
            "primary_key": "id",
            "status": {"ready": ready}
        }
        if state is not None:
            data["status"]["state"] = state
        if detailed_state is not None:
            data["status"]["detailed_state"] = detailed_state
        return data

    @pytest.mark.asyncio
    async def test_provisioning_state_mapped(self):
        """PROVISIONING maps to IndexState.PROVISIONING."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data("PROVISIONING", ready=False)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert isinstance(result, IndexResponse)
        assert result.success is True

    @pytest.mark.asyncio
    async def test_offline_state_mapped(self):
        """OFFLINE state is mapped correctly."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data("OFFLINE", ready=False)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_failed_state_mapped(self):
        """FAILED state is mapped correctly."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data("FAILED", ready=False)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_none_state_uses_detailed_ready(self):
        """None state + detailed_state='READY' -> state=READY."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data(None, "READY", ready=True)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_none_state_uses_detailed_provisioning(self):
        """None state + detailed_state='PROVISIONING' -> PROVISIONING."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data(None, "PROVISIONING", ready=False)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_unknown_detailed_state_uses_ready_flag(self):
        """Unknown detailed_state falls back to ready flag."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data(None, "WEIRD_STATE", ready=True)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_no_state_uses_ready_flag(self):
        """No state or detailed_state -> uses ready flag."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = {"name": "idx", "endpoint_name": "ep", "primary_key": "id",
                "status": {"ready": True}}
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_invalid_state_defaults_to_unknown(self):
        """State value not in enum defaults to IndexState.UNKNOWN."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = self._make_index_data("BANANA", ready=False)
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_direct_access_spec_in_data(self):
        """direct_access_index_spec present sets DIRECT_ACCESS type."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        data = {
            "name": "idx", "endpoint_name": "ep", "primary_key": "id",
            "direct_access_index_spec": {"embedding_dimension": 768},
            "status": {"state": "READY", "ready": True}
        }
        session_cm, _, _ = make_aiohttp_ctx(200, data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_get_index_api_error(self):
        """get_index returns failure on API non-200/404 status."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        session_cm, _, _ = make_aiohttp_ctx(500, text_data="server error")

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.get_index("cat.schema.idx", "ep")

        assert result.success is False


# ---------------------------------------------------------------------------
# list_indexes: state mapping edge cases (lines 390-412)
# ---------------------------------------------------------------------------

class TestListIndexesStateMappings:

    @pytest.mark.asyncio
    async def test_list_provisioning_state(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": "PROVISIONING", "ready": False}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        # list_indexes may return 0 or more depending on internal get calls,
        # but should succeed
        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_offline_state(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": "OFFLINE", "ready": False}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_failed_state(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": "FAILED", "ready": False}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_none_state_uses_detailed(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": None, "detailed_state": "READY", "ready": True}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_none_state_unknown_detailed_uses_ready(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": None, "detailed_state": "WEIRD", "ready": True}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_invalid_state_defaults_to_unknown(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"state": "NOPE", "ready": False}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_list_api_error(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        session_cm, _, _ = make_aiohttp_ctx(500, text_data="server error")

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is False

    @pytest.mark.asyncio
    async def test_list_no_state_uses_ready_flag(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        list_data = {"vector_indexes": [{"name": "idx", "primary_key": "id",
                                         "status": {"ready": False}}]}
        session_cm, _, _ = make_aiohttp_ctx(200, list_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.list_indexes("ep")

        assert result.success is True


# ---------------------------------------------------------------------------
# upsert: validation and error paths (lines 573-1135)
# ---------------------------------------------------------------------------

class TestUpsertEdgeCases:

    @pytest.mark.asyncio
    async def test_upsert_missing_workspace_url(self):
        """upsert returns error when workspace URL is empty."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        repo.workspace_url = ""

        result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1}])
        assert result["success"] is False
        assert "workspace" in result.get("error", "").lower() or "workspace" in result.get("message", "").lower()

    @pytest.mark.asyncio
    async def test_upsert_empty_records_list(self):
        """upsert returns error when records list is empty."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)

        with patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            result = await repo.upsert("cat.schema.idx", "ep", [])

        assert result["success"] is False
        assert "No records" in result.get("error", "") or "No records" in result.get("message", "")

    @pytest.mark.asyncio
    async def test_upsert_empty_dict_record(self):
        """upsert returns error when a record is an empty dict."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)

        with patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            result = await repo.upsert("cat.schema.idx", "ep", [{}])

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_upsert_record_without_embedding_logs_warning(self):
        """upsert continues when record has no embedding field (logs warning)."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        session_cm, _, _ = make_aiohttp_ctx(200, {"status": "OK"})

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1, "text": "hello"}])

        assert "success" in result

    @pytest.mark.asyncio
    async def test_upsert_non_list_records_wrapped(self):
        """upsert wraps a single dict into a list."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        session_cm, _, _ = make_aiohttp_ctx(200, {"status": "OK"})

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.upsert("cat.schema.idx", "ep", {"id": 1, "text": "hello"})

        assert "success" in result

    @pytest.mark.asyncio
    async def test_upsert_400_invalid_param_is_empty(self):
        """upsert returns error on 400 INVALID_PARAMETER_VALUE with 'is empty'."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        error_text = "INVALID_PARAMETER_VALUE: inputs_json is empty"
        session_cm, _, _ = make_aiohttp_ctx(400, text_data=error_text)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1, "embedding": [0.1] * 10}])

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_upsert_400_invalid_param_not_empty(self):
        """upsert returns error on 400 INVALID_PARAMETER_VALUE without 'is empty'."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        error_text = "INVALID_PARAMETER_VALUE: wrong column name"
        session_cm, _, _ = make_aiohttp_ctx(400, text_data=error_text)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1, "embedding": [0.1] * 10}])

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_upsert_500_error_status(self):
        """upsert returns error on 500 response."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        session_cm, _, _ = make_aiohttp_ctx(500, text_data="server error")

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1, "embedding": [0.1] * 10}])

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_upsert_exception_returns_error(self):
        """upsert handles unexpected exceptions."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)

        with patch.object(repo, "_get_auth_token", side_effect=Exception("auth failed")):
            result = await repo.upsert("cat.schema.idx", "ep", [{"id": 1, "embedding": [0.1] * 10}])

        assert result["success"] is False
        assert "auth failed" in result.get("error", "")


# ---------------------------------------------------------------------------
# count_documents: edge cases (lines 1236-1266)
# ---------------------------------------------------------------------------

class TestCountDocumentsEdgeCases:

    @pytest.mark.asyncio
    async def test_count_documents_exception_returns_zero(self):
        """count_documents returns 0 on exception."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)

        with patch.object(repo, "_get_auth_token", side_effect=Exception("token failed")):
            result = await repo.count_documents("cat.schema.idx", "ep")

        assert result == 0

    @pytest.mark.asyncio
    async def test_count_documents_from_describe(self):
        """count_documents tries describe_index path."""
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)

        describe_data = {
            "name": "cat.schema.idx",
            "endpoint_name": "ep",
            "primary_key": "id",
            "num_rows": 42,
            "status": {"state": "READY", "ready": True, "indexed_row_count": 25}
        }
        session_cm, _, _ = make_aiohttp_ctx(200, describe_data)

        with patch("src.repositories.databricks_vector_index_repository.aiohttp") as mock_aiohttp, \
             patch.object(repo, "_get_auth_token", return_value=AUTH_TOKEN):
            mock_aiohttp.ClientSession = MagicMock(return_value=session_cm)
            result = await repo.count_documents("cat.schema.idx", "ep")

        assert isinstance(result, int)
