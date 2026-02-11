"""
Unit tests for DatabricksVectorSearchSetupService.

Tests one-click Databricks Vector Search setup including endpoint creation,
index creation with retry logic, configuration saving, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from types import SimpleNamespace
from datetime import datetime

from src.services.databricks_vectorsearch_setup_service import DatabricksVectorSearchSetupService
from src.schemas.databricks_vector_endpoint import (
    EndpointCreate,
    EndpointResponse,
    EndpointInfo,
    EndpointType,
    EndpointState,
)
from src.schemas.databricks_vector_index import IndexCreate, IndexResponse, IndexInfo, IndexState
from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    MemoryBackendType,
    MemoryBackendCreate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def service():
    """Create a DatabricksVectorSearchSetupService with no session."""
    return DatabricksVectorSearchSetupService(session=None)


@pytest.fixture
def service_with_session():
    """Create a DatabricksVectorSearchSetupService with a mock session."""
    mock_session = AsyncMock()
    return DatabricksVectorSearchSetupService(session=mock_session)


@pytest.fixture
def mock_endpoint_repo():
    """Create a mock DatabricksVectorEndpointRepository."""
    repo = AsyncMock()
    return repo


@pytest.fixture
def mock_index_repo():
    """Create a mock DatabricksVectorIndexRepository."""
    repo = AsyncMock()
    return repo


def _endpoint_response(success=True, name="ep", message="ok", error=None):
    """Helper to build an EndpointResponse."""
    return EndpointResponse(
        success=success,
        endpoint=EndpointInfo(
            name=name,
            endpoint_type=EndpointType.STANDARD,
            state=EndpointState.PROVISIONING,
            ready=False,
        )
        if success
        else None,
        message=message,
        error=error,
    )


def _index_response(success=True, name="idx", message="ok"):
    """Helper to build an IndexResponse."""
    return IndexResponse(
        success=success,
        index=IndexInfo(
            name=name,
            endpoint_name="ep",
            state=IndexState.READY if success else IndexState.FAILED,
            ready=success,
        )
        if success
        else None,
        message=message,
    )


# ---------------------------------------------------------------------------
# Tests: __init__
# ---------------------------------------------------------------------------

class TestInit:
    """Test service initialisation."""

    def test_init_no_session(self):
        svc = DatabricksVectorSearchSetupService()
        assert svc.session is None

    def test_init_with_session(self):
        session = MagicMock()
        svc = DatabricksVectorSearchSetupService(session=session)
        assert svc.session is session


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- happy path
# ---------------------------------------------------------------------------

class TestOneClickSetupHappyPath:
    """Tests for a successful end-to-end one-click setup without group_id."""

    @pytest.mark.asyncio
    async def test_full_success_no_group_id(self, service):
        """All endpoints and indexes created successfully, no config saved."""
        ep_resp = _endpoint_response(success=True, name="ep", message="Endpoint ep created successfully")
        idx_resp = _index_response(success=True, name="ml.agents.idx", message="Index created successfully")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                catalog="ml",
                schema="agents",
                embedding_dimension=1024,
                user_token="tok",
                group_id=None,
            )

        assert result["success"] is True
        assert "endpoints" in result
        assert "indexes" in result
        assert result["config"] is not None
        # Without group_id, config is not saved
        assert result.get("info") is not None
        assert "backend_id" not in result

    @pytest.mark.asyncio
    async def test_endpoint_names_contain_unique_suffix(self, service):
        """Verify endpoint names include timestamps and random suffix."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        # The memory endpoint name starts with "kasal_memory_"
        memory_ep_name = result["endpoints"]["memory"]["name"]
        assert memory_ep_name.startswith("kasal_memory_")

        # The document endpoint name starts with "kasal_docs_"
        doc_ep_name = result["endpoints"]["document"]["name"]
        assert doc_ep_name.startswith("kasal_docs_")


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- endpoint failures
# ---------------------------------------------------------------------------

class TestEndpointCreationFailures:
    """Tests for endpoint creation failures."""

    @pytest.mark.asyncio
    async def test_memory_endpoint_failure_raises_and_returns_error(self, service):
        """When memory endpoint fails, setup returns error."""
        ep_resp = _endpoint_response(
            success=False, message="Failed to create memory endpoint: auth error", error="auth error"
        )

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep
            MockIdxRepo.return_value = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_doc_endpoint_failure_continues(self, service):
        """When doc endpoint fails, setup continues with memory endpoint only."""
        memory_ep_resp = _endpoint_response(success=True, message="ok")
        doc_ep_resp = _endpoint_response(success=False, message="Failed", error="quota exceeded")
        idx_resp = _index_response(success=True, message="ok")

        call_count = 0

        async def side_effect_create_ep(request, token=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return memory_ep_resp
            return doc_ep_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.side_effect = side_effect_create_ep
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # Document endpoint records error
        assert "error" in result["endpoints"]["document"]
        # Only 3 indexes (no document index when doc endpoint fails)
        assert "short_term" in result["indexes"]
        assert "long_term" in result["indexes"]
        assert "entity" in result["indexes"]
        assert "document" not in result["indexes"]

    @pytest.mark.asyncio
    async def test_endpoint_already_exists(self, service):
        """When endpoint already exists, status is marked as already_exists."""
        ep_resp = _endpoint_response(success=True, message="Endpoint already exists")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        assert result["endpoints"]["memory"]["status"] == "already_exists"


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- index creation with retry
# ---------------------------------------------------------------------------

class TestIndexCreationRetry:
    """Tests for index creation retry logic inside one_click_databricks_setup."""

    @pytest.mark.asyncio
    async def test_index_already_exists_exception(self, service):
        """When create_index raises 'already exists' exception, index is recorded as already_exists."""
        ep_resp = _endpoint_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = Exception("Index already exists in catalog")
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        assert result["indexes"]["short_term"]["status"] == "already_exists"

    @pytest.mark.asyncio
    async def test_index_table_does_not_exist_retries(self, service):
        """When index creation fails with 'table does not exist', it retries."""
        ep_resp = _endpoint_response(success=True, message="ok")

        call_counts = {}

        async def create_index_side_effect(request, token=None):
            name = request.name
            call_counts[name] = call_counts.get(name, 0) + 1
            if call_counts[name] == 1:
                # First call: table does not exist error
                return IndexResponse(
                    success=False,
                    message="Table does not exist for index " + name,
                )
            # Subsequent calls succeed
            return _index_response(success=True, name=name, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = create_index_side_effect
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # Each index should have succeeded (after retry)
        for idx_type in ["short_term", "long_term", "entity", "document"]:
            assert result["indexes"][idx_type]["status"] == "created"

    @pytest.mark.asyncio
    async def test_index_does_not_exist_exception_retries(self, service):
        """When create_index raises an exception containing 'does not exist', it retries."""
        ep_resp = _endpoint_response(success=True, message="ok")

        attempt_tracker = {"count": 0}

        async def create_index_side_effect(request, token=None):
            attempt_tracker["count"] += 1
            if attempt_tracker["count"] <= 4:
                # First few calls: raise "does not exist"
                raise Exception("Catalog resource does not exist")
            # After enough retries, succeed
            return _index_response(success=True, name=request.name, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = create_index_side_effect
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        # The first index will exhaust retries; second index will succeed, etc.
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_index_exhausts_retries_via_response(self, service):
        """When all retry attempts fail via response (table does not exist), error is recorded.

        The retry logic: on the last attempt (attempt == max_retries - 1), the code
        raises Exception("Failed to create index: ...") since 'attempt < max_retries - 1'
        is False. That exception is caught by the outer except and since it contains
        "does not exist" but attempt is at max, it falls to the else branch recording
        the error string directly.
        """
        ep_resp = _endpoint_response(success=True, message="ok")

        # Always fail with "table does not exist"
        table_err_resp = IndexResponse(
            success=False,
            message="Table does not exist for this endpoint",
        )

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = table_err_resp
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # The raised exception "Failed to create index: ..." is caught and stored
        assert "error" in result["indexes"]["short_term"]
        assert "Failed to create index" in result["indexes"]["short_term"]["error"]

    @pytest.mark.asyncio
    async def test_index_exhausts_retries_via_exception(self, service):
        """When all retries fail via exception with 'does not exist', the raw error is recorded.

        On attempts 0 and 1, the exception triggers a retry (continue). On attempt 2
        (the last), attempt < max_retries - 1 is False, so the else branch returns
        the raw exception message as the error.
        """
        ep_resp = _endpoint_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = Exception("Resource does not exist yet")
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # On the final attempt, the exception is caught and recorded directly
        assert "error" in result["indexes"]["short_term"]
        assert "does not exist" in result["indexes"]["short_term"]["error"]

    @pytest.mark.asyncio
    async def test_index_generic_failure_returns_error(self, service):
        """When create_index raises a non-retryable exception, index records error."""
        ep_resp = _endpoint_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = Exception("Unexpected network error")
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        assert "error" in result["indexes"]["short_term"]
        assert "Unexpected network error" in result["indexes"]["short_term"]["error"]

    @pytest.mark.asyncio
    async def test_index_creation_failure_response_not_retryable(self, service):
        """When create_index returns failure with non-retryable message, it raises and records error."""
        ep_resp = _endpoint_response(success=True, message="ok")

        non_retryable_resp = IndexResponse(
            success=False,
            message="Permission denied for catalog ml",
        )

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = non_retryable_resp
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        # The exception from the non-retryable path is caught and stored as error
        assert result["success"] is True
        assert "error" in result["indexes"]["short_term"]

    @pytest.mark.asyncio
    async def test_unknown_index_type_returns_error(self, service):
        """When DatabricksIndexSchemas.get_schema returns empty dict for unknown type."""
        ep_resp = _endpoint_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksIndexSchemas"
            ) as MockSchemas,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            MockIdxRepo.return_value = mock_idx

            # Return empty schema (falsy) for all types
            MockSchemas.get_schema.return_value = {}

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # All indexes should have error about unknown index type
        for idx_type in ["short_term", "long_term", "entity", "document"]:
            assert "error" in result["indexes"][idx_type]
            assert "Unknown index type" in result["indexes"][idx_type]["error"]

    @pytest.mark.asyncio
    async def test_retry_calls_asyncio_sleep_with_increasing_wait(self, service):
        """When retries happen, asyncio.sleep is called with increasing wait times."""
        ep_resp = _endpoint_response(success=True, message="ok")

        # Fail twice then succeed -- only for the first index to keep it simple
        per_index_counts = {}

        async def side_effect(request, token=None):
            name = request.name
            per_index_counts[name] = per_index_counts.get(name, 0) + 1
            if per_index_counts[name] <= 2:
                return IndexResponse(
                    success=False,
                    message="Table does not exist for this index",
                )
            return _index_response(success=True, name=name, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch("src.services.databricks_vectorsearch_setup_service.asyncio") as mock_asyncio,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = side_effect
            MockIdxRepo.return_value = mock_idx

            mock_asyncio.sleep = AsyncMock()

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is True
        # asyncio.sleep should have been called with increasing wait times (10, 20)
        sleep_calls = [c[0][0] for c in mock_asyncio.sleep.call_args_list]
        assert 10 in sleep_calls
        assert 20 in sleep_calls


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- config / model_dump
# ---------------------------------------------------------------------------

class TestConfigGeneration:
    """Tests that the config dict is properly generated."""

    @pytest.mark.asyncio
    async def test_config_contains_expected_fields(self, service):
        """Verify the config dict in the result has workspace_url, catalog, schema, etc."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                catalog="test_cat",
                schema="test_sch",
                embedding_dimension=512,
            )

        config = result["config"]
        assert config is not None
        assert config["workspace_url"] == "https://example.com"
        assert config["embedding_dimension"] == 512
        assert result["catalog"] == "test_cat"
        assert result["schema"] == "test_sch"

    @pytest.mark.asyncio
    async def test_config_document_endpoint_none_when_failed(self, service):
        """When doc endpoint fails, document_endpoint_name is None in config."""
        memory_ep_resp = _endpoint_response(success=True, message="ok")
        doc_ep_resp = _endpoint_response(success=False, message="Failed", error="err")
        idx_resp = _index_response(success=True, message="ok")

        call_count = 0

        async def side_effect_create_ep(request, token=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return memory_ep_resp
            return doc_ep_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.side_effect = side_effect_create_ep
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        config = result["config"]
        assert config["document_endpoint_name"] is None


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- saving configuration with group_id
# ---------------------------------------------------------------------------

class TestConfigSaving:
    """Tests for saving configuration when group_id and session are provided."""

    @pytest.mark.asyncio
    async def test_config_saved_when_group_id_and_session(self):
        """When group_id + session are provided, config is saved via MemoryBackendBaseService."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        mock_saved_backend = MagicMock()
        mock_saved_backend.id = "backend-123"

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService"
            ) as MockBaseService,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            mock_base_svc_inst = AsyncMock()
            mock_base_svc_inst.create_memory_backend.return_value = mock_saved_backend
            MockBaseService.return_value = mock_base_svc_inst

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo_inst = AsyncMock()
                mock_mb_repo_inst.get_by_group_id.return_value = []
                MockMBRepo.return_value = mock_mb_repo_inst

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="group-abc",
                )

        assert result["success"] is True
        assert result["backend_id"] == "backend-123"
        assert result["message"] == "Setup completed and configuration saved"

    @pytest.mark.asyncio
    async def test_existing_disabled_configs_deleted_before_save(self):
        """When existing configs are all DEFAULT (disabled), they are deleted before saving new one."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        mock_saved_backend = MagicMock()
        mock_saved_backend.id = "new-backend"

        disabled_config_1 = MagicMock()
        disabled_config_1.backend_type = MemoryBackendType.DEFAULT
        disabled_config_1.id = "old-1"

        disabled_config_2 = MagicMock()
        disabled_config_2.backend_type = MemoryBackendType.DEFAULT
        disabled_config_2.id = "old-2"

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService"
            ) as MockBaseService,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            mock_base_svc_inst = AsyncMock()
            mock_base_svc_inst.create_memory_backend.return_value = mock_saved_backend
            MockBaseService.return_value = mock_base_svc_inst

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo_inst = AsyncMock()
                mock_mb_repo_inst.get_by_group_id.return_value = [disabled_config_1, disabled_config_2]
                MockMBRepo.return_value = mock_mb_repo_inst

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-1",
                )

        assert result["success"] is True
        assert mock_mb_repo_inst.delete.call_count == 2
        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_existing_active_configs_not_deleted(self):
        """When existing configs have active (non-DEFAULT) ones, they are NOT deleted."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        mock_saved_backend = MagicMock()
        mock_saved_backend.id = "new-backend"

        active_config = MagicMock()
        active_config.backend_type = MemoryBackendType.DATABRICKS
        active_config.id = "active-1"

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService"
            ) as MockBaseService,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            mock_base_svc_inst = AsyncMock()
            mock_base_svc_inst.create_memory_backend.return_value = mock_saved_backend
            MockBaseService.return_value = mock_base_svc_inst

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo_inst = AsyncMock()
                mock_mb_repo_inst.get_by_group_id.return_value = [active_config]
                MockMBRepo.return_value = mock_mb_repo_inst

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-2",
                )

        assert result["success"] is True
        mock_mb_repo_inst.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_save_error_foreign_key_constraint(self):
        """When save fails with foreign key constraint, warning and info are set."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo_inst = AsyncMock()
                mock_mb_repo_inst.get_by_group_id.side_effect = Exception(
                    "FOREIGN KEY constraint failed for group"
                )
                MockMBRepo.return_value = mock_mb_repo_inst

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-fk",
                )

        assert result["success"] is True
        assert "warning" in result
        assert "ensure you are logged in" in result["warning"]
        assert "info" in result

    @pytest.mark.asyncio
    async def test_save_error_generic(self):
        """When save fails with a generic error, warning includes the error message."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo_inst = AsyncMock()
                mock_mb_repo_inst.get_by_group_id.side_effect = Exception("DB connection lost")
                MockMBRepo.return_value = mock_mb_repo_inst

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-err",
                )

        assert result["success"] is True
        assert "warning" in result
        assert "DB connection lost" in result["warning"]

    @pytest.mark.asyncio
    async def test_no_save_when_no_group_id(self, service):
        """When group_id is None, info message is set and no save happens."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                group_id=None,
            )

        assert result["info"] is not None
        assert "log in" in result["info"]

    @pytest.mark.asyncio
    async def test_no_save_when_no_session(self):
        """When session is None (even with group_id), info message is set."""
        svc = DatabricksVectorSearchSetupService(session=None)
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await svc.one_click_databricks_setup(
                workspace_url="https://example.com",
                group_id="grp-no-sess",
            )

        assert result["info"] is not None


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- top-level exception handling
# ---------------------------------------------------------------------------

class TestTopLevelExceptionHandling:
    """Tests for the outermost try/except in one_click_databricks_setup."""

    @pytest.mark.asyncio
    async def test_top_level_exception_returns_failure(self, service):
        """When an unexpected exception occurs at the top level, result is failure."""
        with patch(
            "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
        ) as MockEpRepo:
            MockEpRepo.side_effect = RuntimeError("Cannot initialise repo")

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert result["success"] is False
        assert "Setup failed" in result["message"]
        assert "Cannot initialise repo" in result["error"]


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- user_token propagation
# ---------------------------------------------------------------------------

class TestUserTokenPropagation:
    """Tests that user_token is forwarded to repositories."""

    @pytest.mark.asyncio
    async def test_user_token_forwarded_to_endpoint_repo(self, service):
        """user_token is passed to endpoint_repo.create_endpoint."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                user_token="my-secret-token",
            )

        for c in mock_ep.create_endpoint.call_args_list:
            assert c[0][1] == "my-secret-token" or c.kwargs.get("user_token") == "my-secret-token" or c[0][-1] == "my-secret-token"

    @pytest.mark.asyncio
    async def test_user_token_forwarded_to_index_repo(self, service):
        """user_token is passed to index_repo.create_index."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                user_token="idx-token",
            )

        for c in mock_idx.create_index.call_args_list:
            assert c[0][1] == "idx-token"


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- default parameters
# ---------------------------------------------------------------------------

class TestDefaultParameters:
    """Tests for default parameter values."""

    @pytest.mark.asyncio
    async def test_defaults_catalog_schema_dimension(self, service):
        """Default catalog='ml', schema='agents', embedding_dimension=1024."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        config = result["config"]
        assert result["catalog"] == "ml"
        assert result["schema"] == "agents"
        assert config["embedding_dimension"] == 1024


# ---------------------------------------------------------------------------
# Tests: one_click_databricks_setup -- index names contain catalog.schema
# ---------------------------------------------------------------------------

class TestIndexNaming:
    """Tests that index names use catalog.schema prefix."""

    @pytest.mark.asyncio
    async def test_index_names_use_catalog_schema(self, service):
        """All index names must be formatted as catalog.schema.table_name."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        captured_requests = []

        async def capture_create_index(request, token=None):
            captured_requests.append(request)
            return idx_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = capture_create_index
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                catalog="mycat",
                schema="mysch",
            )

        for req in captured_requests:
            assert req.name.startswith("mycat.mysch."), f"Index name {req.name} does not start with mycat.mysch."


# ---------------------------------------------------------------------------
# Tests: document index conditional creation
# ---------------------------------------------------------------------------

class TestDocumentIndexConditional:
    """Test that document index is only created when document endpoint succeeds."""

    @pytest.mark.asyncio
    async def test_document_index_created_when_doc_endpoint_succeeds(self, service):
        """When both endpoints succeed, all 4 indexes are created."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert "short_term" in result["indexes"]
        assert "long_term" in result["indexes"]
        assert "entity" in result["indexes"]
        assert "document" in result["indexes"]
        assert mock_idx.create_index.call_count == 4


# ---------------------------------------------------------------------------
# Tests: endpoint type
# ---------------------------------------------------------------------------

class TestEndpointType:
    """Tests that endpoints are created with correct type."""

    @pytest.mark.asyncio
    async def test_endpoints_created_as_standard(self, service):
        """Both endpoints should be created with STANDARD type."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        captured_ep_requests = []

        async def capture_create_ep(request, token=None):
            captured_ep_requests.append(request)
            return ep_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.side_effect = capture_create_ep
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert len(captured_ep_requests) == 2
        for req in captured_ep_requests:
            assert req.endpoint_type == "STANDARD" or req.endpoint_type == EndpointType.STANDARD


# ---------------------------------------------------------------------------
# Tests: workspace_url propagation
# ---------------------------------------------------------------------------

class TestWorkspaceUrlPropagation:
    """Tests that workspace_url is passed correctly to repository constructors."""

    @pytest.mark.asyncio
    async def test_workspace_url_passed_to_repos(self, service):
        """workspace_url is forwarded to both repository constructors."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://my-workspace.databricks.com",
            )

        MockEpRepo.assert_called_once_with("https://my-workspace.databricks.com")
        MockIdxRepo.assert_called_once_with("https://my-workspace.databricks.com")


# ---------------------------------------------------------------------------
# Tests: embedding_dimension propagation
# ---------------------------------------------------------------------------

class TestEmbeddingDimensionPropagation:
    """Tests that embedding_dimension flows into IndexCreate requests."""

    @pytest.mark.asyncio
    async def test_custom_embedding_dimension(self, service):
        """embedding_dimension=768 should be reflected in IndexCreate."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        captured = []

        async def capture(request, token=None):
            captured.append(request)
            return idx_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = capture
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                embedding_dimension=768,
            )

        for req in captured:
            assert req.embedding_dimension == 768


# ---------------------------------------------------------------------------
# Tests: mixed existing configs
# ---------------------------------------------------------------------------

class TestMixedExistingConfigs:
    """Tests for groups with a mix of active and disabled existing configs."""

    @pytest.mark.asyncio
    async def test_mixed_configs_active_not_deleted(self):
        """When one active and one disabled config exist, neither is deleted."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        mock_saved = MagicMock()
        mock_saved.id = "new-id"

        active_cfg = MagicMock()
        active_cfg.backend_type = MemoryBackendType.DATABRICKS

        disabled_cfg = MagicMock()
        disabled_cfg.backend_type = MemoryBackendType.DEFAULT

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService"
            ) as MockBaseService,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            mock_base_svc = AsyncMock()
            mock_base_svc.create_memory_backend.return_value = mock_saved
            MockBaseService.return_value = mock_base_svc

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo = AsyncMock()
                mock_mb_repo.get_by_group_id.return_value = [active_cfg, disabled_cfg]
                MockMBRepo.return_value = mock_mb_repo

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-mixed",
                )

        mock_mb_repo.delete.assert_not_called()
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Tests: no existing configs
# ---------------------------------------------------------------------------

class TestNoExistingConfigs:
    """Tests for groups with no existing configs."""

    @pytest.mark.asyncio
    async def test_no_existing_configs_proceeds(self):
        """When no existing configs, save proceeds without deletion."""
        mock_session = AsyncMock()
        svc = DatabricksVectorSearchSetupService(session=mock_session)

        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        mock_saved = MagicMock()
        mock_saved.id = "brand-new"

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService"
            ) as MockBaseService,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            mock_base_svc = AsyncMock()
            mock_base_svc.create_memory_backend.return_value = mock_saved
            MockBaseService.return_value = mock_base_svc

            with patch(
                "src.repositories.memory_backend_repository.MemoryBackendRepository"
            ) as MockMBRepo:
                mock_mb_repo = AsyncMock()
                mock_mb_repo.get_by_group_id.return_value = []
                MockMBRepo.return_value = mock_mb_repo

                result = await svc.one_click_databricks_setup(
                    workspace_url="https://example.com",
                    group_id="grp-empty",
                )

        mock_mb_repo.delete.assert_not_called()
        assert result["backend_id"] == "brand-new"


# ---------------------------------------------------------------------------
# Tests: user_token logging paths
# ---------------------------------------------------------------------------

class TestUserTokenLogging:
    """Tests for user_token logging branches."""

    @pytest.mark.asyncio
    async def test_user_token_none_does_not_log_length(self, service):
        """When user_token is None, no length logging occurs (no crash)."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                user_token=None,
            )

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_user_token_present_logs_length(self, service):
        """When user_token is provided, the service runs without issue."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
                user_token="a-long-token-value-here",
            )

        assert result["success"] is True


# ---------------------------------------------------------------------------
# Tests: schema integration
# ---------------------------------------------------------------------------

class TestSchemaIntegration:
    """Tests that DatabricksIndexSchemas.get_schema is called correctly."""

    @pytest.mark.asyncio
    async def test_get_schema_called_for_each_index_type(self, service):
        """get_schema is invoked for short_term, long_term, entity, and document."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksIndexSchemas"
            ) as MockSchemas,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            MockSchemas.get_schema.return_value = {"id": "string", "embedding": "array<float>"}

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        called_types = [c[0][0] for c in MockSchemas.get_schema.call_args_list]
        assert "short_term" in called_types
        assert "long_term" in called_types
        assert "entity" in called_types
        assert "document" in called_types


# ---------------------------------------------------------------------------
# Tests: document_index in config
# ---------------------------------------------------------------------------

class TestDocumentIndexInConfig:
    """Tests for document_index presence in the resulting config."""

    @pytest.mark.asyncio
    async def test_document_index_in_config_when_created(self, service):
        """document_index is populated in config when document index is created."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        config = result["config"]
        assert "document_index" in config

    @pytest.mark.asyncio
    async def test_document_index_none_when_not_created(self, service):
        """document_index is None in config when document endpoint fails."""
        memory_ep_resp = _endpoint_response(success=True, message="ok")
        doc_ep_resp = _endpoint_response(success=False, message="Fail", error="err")
        idx_resp = _index_response(success=True, message="ok")

        call_count = 0

        async def side_effect(request, token=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return memory_ep_resp
            return doc_ep_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.side_effect = side_effect
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        config = result["config"]
        assert config["document_index"] is None


# ---------------------------------------------------------------------------
# Tests: IndexCreate request fields
# ---------------------------------------------------------------------------

class TestIndexCreateRequestFields:
    """Tests that IndexCreate requests have correct primary_key and embedding_vector_column."""

    @pytest.mark.asyncio
    async def test_index_create_has_correct_primary_key_and_embedding_col(self, service):
        """Each IndexCreate should have primary_key='id' and embedding_vector_column='embedding'."""
        ep_resp = _endpoint_response(success=True, message="ok")
        idx_resp = _index_response(success=True, message="ok")

        captured = []

        async def capture(request, token=None):
            captured.append(request)
            return idx_resp

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.return_value = ep_resp
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.side_effect = capture
            MockIdxRepo.return_value = mock_idx

            await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        assert len(captured) == 4
        for req in captured:
            assert req.primary_key == "id"
            assert req.embedding_vector_column == "embedding"


# ---------------------------------------------------------------------------
# Tests: doc endpoint error format
# ---------------------------------------------------------------------------

class TestDocEndpointErrorFormat:
    """Tests the error recording format when document endpoint fails."""

    @pytest.mark.asyncio
    async def test_doc_endpoint_error_or_message_recorded(self, service):
        """When doc endpoint fails with error=None, message is used as fallback."""
        memory_ep_resp = _endpoint_response(success=True, message="ok")
        doc_ep_resp = EndpointResponse(
            success=False,
            endpoint=None,
            message="Quota exceeded",
            error=None,
        )

        call_count = 0

        async def side_effect(request, token=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return memory_ep_resp
            return doc_ep_resp

        idx_resp = _index_response(success=True, message="ok")

        with (
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository"
            ) as MockEpRepo,
            patch(
                "src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository"
            ) as MockIdxRepo,
        ):
            mock_ep = AsyncMock()
            mock_ep.create_endpoint.side_effect = side_effect
            MockEpRepo.return_value = mock_ep

            mock_idx = AsyncMock()
            mock_idx.create_index.return_value = idx_resp
            MockIdxRepo.return_value = mock_idx

            result = await service.one_click_databricks_setup(
                workspace_url="https://example.com",
            )

        doc_ep = result["endpoints"]["document"]
        assert "error" in doc_ep
        # When error is None, it falls back to message
        assert doc_ep["error"] == "Quota exceeded"
