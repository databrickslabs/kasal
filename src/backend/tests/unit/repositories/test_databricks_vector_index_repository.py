"""
Unit tests for DatabricksVectorIndexRepository.

Tests the REST API implementation for Vector Search operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import List, Dict, Any
import json
import aiohttp

from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.schemas.databricks_vector_index import (
    IndexCreate,
    IndexInfo,
    IndexResponse,
    IndexListResponse,
    IndexState,
    IndexType
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WORKSPACE_URL = "https://example.databricks.com"
AUTH_TOKEN = "test-auth-token"


def make_aiohttp_ctx(status: int, json_data: dict | None = None, text_data: str = ""):
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

    for method in ("post", "get", "delete"):
        setattr(session, method, MagicMock(return_value=_cm(response)))

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    return session_cm, response, session


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_auth_token():
    return AUTH_TOKEN


@pytest.fixture
def repository():
    return DatabricksVectorIndexRepository(WORKSPACE_URL)


@pytest.fixture
def sample_index_create():
    return IndexCreate(
        name="catalog.schema.test_index",
        endpoint_name="test_endpoint",
        primary_key="id",
        embedding_dimension=768,
        embedding_vector_column="embedding",
        schema={"type": "object", "properties": {}},
    )


@pytest.fixture
def sample_index_info():
    return IndexInfo(
        name="catalog.schema.test_index",
        endpoint_name="test_endpoint",
        index_type=IndexType.DIRECT_ACCESS,
        state=IndexState.READY,
        ready=True,
        row_count=100,
        indexed_row_count=100,
        embedding_dimension=768,
        primary_key="id",
    )


# ---------------------------------------------------------------------------
# Existing tests preserved
# ---------------------------------------------------------------------------

class TestDatabricksVectorIndexRepository:
    """Test suite for DatabricksVectorIndexRepository."""

    @pytest.mark.asyncio
    async def test_similarity_search_success(self, repository, mock_auth_token):
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        columns = ["id", "content", "metadata"]
        num_results = 10

        expected_results = {
            "result": {
                "data_array": [
                    ["doc1", "Test content 1", {"key": "value1"}],
                    ["doc2", "Test content 2", {"key": "value2"}]
                ],
                "row_count": 2
            }
        }

        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token

            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value=expected_results)

                mock_session = MagicMock()
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)

                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_scs.return_value = mock_session_cm

                result = await repository.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns,
                    num_results=num_results
                )

                assert result["success"] is True
                assert result["message"] == "Search completed successfully"
                assert result["results"] == expected_results
                mock_get_auth.assert_called_once_with(None)
                expected_url = f"https://example.databricks.com/api/2.0/vector-search/indexes/catalog.schema.test_index/query"
                actual_url = mock_session.post.call_args[0][0]
                assert actual_url == expected_url

    @pytest.mark.asyncio
    async def test_similarity_search_with_filters(self, repository, mock_auth_token):
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        columns = ["id", "content"]
        num_results = 5
        filters = {"metadata.category": "test"}

        expected_results = {
            "result": {
                "data_array": [["doc1", "Filtered content"]],
                "row_count": 1
            }
        }

        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token

            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value=expected_results)

                mock_session = MagicMock()
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)

                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_scs.return_value = mock_session_cm

                result = await repository.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns,
                    num_results=num_results,
                    filters=filters
                )

                assert result["success"] is True
                call_kwargs = mock_session.post.call_args[1]
                assert "filters_json" in call_kwargs["json"]
                assert json.loads(call_kwargs["json"]["filters_json"]) == filters

    @pytest.mark.asyncio
    async def test_similarity_search_failure(self, repository, mock_auth_token):
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token

            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_response = AsyncMock()
                mock_response.status = 500
                mock_response.text = AsyncMock(return_value="Internal Server Error")

                mock_session = MagicMock()
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)

                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_scs.return_value = mock_session_cm

                result = await repository.similarity_search(
                    index_name="catalog.schema.test_index",
                    endpoint_name="test_endpoint",
                    query_vector=[0.1, 0.2, 0.3, 0.4, 0.5],
                    columns=["id", "content"]
                )

                assert result["success"] is False
                assert "error" in result
                assert "Failed to perform search" in result["message"]
                assert result["results"] is None

    @pytest.mark.asyncio
    async def test_describe_index_success(self, repository, mock_auth_token):
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"

        mock_index_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name=index_name,
                endpoint_name=endpoint_name,
                index_type=IndexType.DIRECT_ACCESS,
                state=IndexState.READY,
                ready=True,
                row_count=1000,
                indexed_row_count=1000,
                embedding_dimension=768,
                primary_key="id"
            ),
            message="Index retrieved successfully"
        )

        with patch.object(repository, 'get_index', new_callable=AsyncMock) as mock_get_index:
            mock_get_index.return_value = mock_index_response

            result = await repository.describe_index(
                index_name=index_name,
                endpoint_name=endpoint_name
            )

            assert result["success"] is True
            assert result["message"] == "Index description retrieved successfully"
            assert result["description"]["name"] == index_name
            assert result["description"]["num_rows"] == 1000
            assert result["description"]["status"]["ready"] is True

    @pytest.mark.asyncio
    async def test_upsert_success(self, repository, mock_auth_token):
        records = [
            {"id": "doc1", "content": "Test content 1", "embedding": [0.1, 0.2, 0.3]},
            {"id": "doc2", "content": "Test content 2", "embedding": [0.4, 0.5, 0.6]}
        ]

        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token

            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.text = AsyncMock(return_value="Success")

                mock_session = MagicMock()
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)

                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_scs.return_value = mock_session_cm

                result = await repository.upsert(
                    index_name="catalog.schema.test_index",
                    endpoint_name="test_endpoint",
                    records=records
                )

                assert result["success"] is True
                assert result["upserted_count"] == 2
                assert "Successfully upserted" in result["message"]

                call_kwargs = mock_session.post.call_args[1]
                parsed_inputs = json.loads(call_kwargs["json"]["inputs_json"])
                assert parsed_inputs == records

    @pytest.mark.asyncio
    async def test_delete_records_success(self, repository, mock_auth_token):
        primary_keys = ["doc1", "doc2", "doc3"]

        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token

            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_response = AsyncMock()
                mock_response.status = 204

                mock_session = MagicMock()
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)

                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_scs.return_value = mock_session_cm

                result = await repository.delete_records(
                    index_name="catalog.schema.test_index",
                    endpoint_name="test_endpoint",
                    primary_keys=primary_keys
                )

                assert result["success"] is True
                assert result["deleted_count"] == 3
                assert "Successfully deleted" in result["message"]

    @pytest.mark.asyncio
    async def test_count_documents_without_filters(self, repository, mock_auth_token):
        describe_response = {
            "success": True,
            "description": {
                "status": {
                    "indexed_row_count": 5000
                }
            }
        }

        with patch.object(repository, 'describe_index', new_callable=AsyncMock) as mock_describe:
            mock_describe.return_value = describe_response
            count = await repository.count_documents(
                index_name="catalog.schema.test_index",
                endpoint_name="test_endpoint"
            )
            assert count == 5000
            mock_describe.assert_called_once_with("catalog.schema.test_index", "test_endpoint", None)

    @pytest.mark.asyncio
    async def test_count_documents_with_filters(self, repository, mock_auth_token):
        filters = {"category": "test"}
        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [["id1"], ["id2"], ["id3"]]
                }
            }
        }

        with patch.object(repository, 'similarity_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = search_response
            count = await repository.count_documents(
                index_name="catalog.schema.test_index",
                endpoint_name="test_endpoint",
                filters=filters
            )
            assert count == 3
            call_args = mock_search.call_args
            assert call_args[1]["filters"] == filters

    @pytest.mark.asyncio
    async def test_count_documents_uses_1024_dim_dummy_vector(self, repository, mock_auth_token):
        filters = {"agent_id": "test-agent"}
        search_response = {
            "success": True,
            "results": {
                "result": {"data_array": [["id1"]]}
            }
        }

        with patch.object(repository, 'similarity_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = search_response
            await repository.count_documents(
                index_name="catalog.schema.test_index",
                endpoint_name="test_endpoint",
                filters=filters
            )
            call_kwargs = mock_search.call_args[1]
            query_vector = call_kwargs["query_vector"]
            assert len(query_vector) == 1024
            assert all(v == 0.0 for v in query_vector)

    @pytest.mark.asyncio
    async def test_count_documents_fallback_when_describe_fails(self, repository, mock_auth_token):
        describe_response = {"success": False, "description": None}
        search_response = {
            "success": True,
            "results": {"result": {"data_array": [["id1"], ["id2"]]}}
        }

        with patch.object(repository, 'describe_index', new_callable=AsyncMock) as mock_describe:
            mock_describe.return_value = describe_response
            with patch.object(repository, 'similarity_search', new_callable=AsyncMock) as mock_search:
                mock_search.return_value = search_response
                count = await repository.count_documents(
                    index_name="catalog.schema.test_index",
                    endpoint_name="test_endpoint"
                )
                assert count == 2
                call_kwargs = mock_search.call_args[1]
                assert len(call_kwargs["query_vector"]) == 1024


# ---------------------------------------------------------------------------
# Utility method tests (already had some, keeping and extending)
# ---------------------------------------------------------------------------

class TestDatabricksVectorIndexRepositoryUtilityMethods:
    def test_repository_initialization(self):
        repository = DatabricksVectorIndexRepository(WORKSPACE_URL)
        assert repository.workspace_url == WORKSPACE_URL
        assert hasattr(repository, '_get_auth_token')
        assert callable(repository._get_auth_token)

    def test_repository_has_expected_methods(self):
        repository = DatabricksVectorIndexRepository(WORKSPACE_URL)
        expected_methods = [
            '_get_auth_token', 'create_index', 'get_index', 'list_indexes',
            'delete_index', 'empty_index', 'similarity_search', 'describe_index',
            'upsert', 'delete_records', 'count_documents'
        ]
        for method_name in expected_methods:
            assert hasattr(repository, method_name)
            assert callable(getattr(repository, method_name))

    def test_repository_workspace_url_property(self):
        repository = DatabricksVectorIndexRepository(WORKSPACE_URL)
        assert repository.workspace_url == WORKSPACE_URL

    def test_repository_with_different_workspace_urls(self):
        urls = [
            "https://example.databricks.com",
            "https://test.cloud.databricks.com",
            "https://workspace.databricks.com"
        ]
        for url in urls:
            repository = DatabricksVectorIndexRepository(url)
            assert repository.workspace_url == url

    def test_repository_strips_trailing_slash(self):
        repository = DatabricksVectorIndexRepository("https://example.databricks.com/")
        assert not repository.workspace_url.endswith("/")


# ---------------------------------------------------------------------------
# _get_auth_token
# ---------------------------------------------------------------------------

class TestGetAuthToken:
    @pytest.mark.asyncio
    async def test_returns_token(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        auth = MagicMock()
        auth.token = AUTH_TOKEN
        with patch("src.repositories.databricks_vector_index_repository.get_auth_context",
                   new_callable=AsyncMock, return_value=auth):
            token = await repo._get_auth_token("user-tok")
        assert token == AUTH_TOKEN

    @pytest.mark.asyncio
    async def test_raises_when_no_auth(self):
        repo = DatabricksVectorIndexRepository(WORKSPACE_URL)
        with patch("src.repositories.databricks_vector_index_repository.get_auth_context",
                   new_callable=AsyncMock, return_value=None):
            with pytest.raises(Exception, match="Failed to get authentication context"):
                await repo._get_auth_token()


# ---------------------------------------------------------------------------
# create_index
# ---------------------------------------------------------------------------

class TestCreateIndex:
    @pytest.mark.asyncio
    async def test_create_index_success(self, repository, sample_index_create, sample_index_info):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN), \
             patch.object(repository, "get_index", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = IndexResponse(
                success=True, index=sample_index_info, message="ok"
            )
            session_cm, _, _ = make_aiohttp_ctx(201, text_data="created")
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.create_index(sample_index_create)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_create_index_failure(self, repository, sample_index_create):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(400, text_data="Bad request")
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.create_index(sample_index_create)

        assert result.success is False

    @pytest.mark.asyncio
    async def test_create_index_exception(self, repository, sample_index_create):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("auth failed")):
            result = await repository.create_index(sample_index_create)
        assert result.success is False
        assert "auth failed" in result.error


# ---------------------------------------------------------------------------
# get_index
# ---------------------------------------------------------------------------

class TestGetIndex:
    @pytest.mark.asyncio
    async def test_get_index_online_state_maps_to_ready(self, repository):
        data = {
            "name": "catalog.schema.idx",
            "status": {"state": "ONLINE", "ready": True, "indexed_row_count": 100},
        }
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(200, json_data=data)
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.get_index("catalog.schema.idx", "ep")

        assert result.success is True
        assert result.index.state == "READY"

    @pytest.mark.asyncio
    async def test_get_index_not_found(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(404)
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.get_index("missing.idx", "ep")

        assert result.success is False
        assert result.index.state == "NOT_FOUND"

    @pytest.mark.asyncio
    async def test_get_index_exception(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("timeout")):
            result = await repository.get_index("catalog.schema.idx", "ep")
        assert result.success is False
        assert "timeout" in result.error


# ---------------------------------------------------------------------------
# list_indexes
# ---------------------------------------------------------------------------

class TestListIndexes:
    @pytest.mark.asyncio
    async def test_list_indexes_success(self, repository):
        data = {
            "indexes": [
                {"name": "catalog.schema.idx1", "status": {"state": "ONLINE", "ready": True}},
                {"name": "catalog.schema.idx2", "status": {"state": "PROVISIONING", "ready": False}},
            ]
        }
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(200, json_data=data)
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.list_indexes("my_endpoint")

        assert result.success is True
        assert len(result.indexes) == 2

    @pytest.mark.asyncio
    async def test_list_indexes_empty(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(200, json_data={"indexes": []})
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.list_indexes("ep")

        assert result.success is True
        assert result.indexes == []

    @pytest.mark.asyncio
    async def test_list_indexes_api_error(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(500, text_data="Server error")
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.list_indexes("ep")

        assert result.success is False
        assert result.indexes == []

    @pytest.mark.asyncio
    async def test_list_indexes_exception(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("crash")):
            result = await repository.list_indexes("ep")
        assert result.success is False


# ---------------------------------------------------------------------------
# delete_index
# ---------------------------------------------------------------------------

class TestDeleteIndex:
    @pytest.mark.asyncio
    async def test_delete_index_success(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(200)
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.delete_index("catalog.schema.idx", "ep")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_index_not_found(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(404)
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.delete_index("missing.idx", "ep")

        assert result.success is False

    @pytest.mark.asyncio
    async def test_delete_index_exception(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("delete failed")):
            result = await repository.delete_index("idx", "ep")
        assert result.success is False
        assert "delete failed" in result.error


# ---------------------------------------------------------------------------
# upsert - error paths
# ---------------------------------------------------------------------------

class TestUpsert:
    @pytest.mark.asyncio
    async def test_upsert_api_error(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(400, text_data="Bad request")
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.upsert(
                    index_name="catalog.schema.idx",
                    endpoint_name="ep",
                    records=[{"id": "1", "embedding": [0.1]}]
                )

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_upsert_exception_returns_error(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("upsert crash")):
            result = await repository.upsert(
                index_name="idx", endpoint_name="ep", records=[{"id": "1"}]
            )
        assert result["success"] is False
        assert "upsert crash" in result["error"]


# ---------------------------------------------------------------------------
# delete_records - error paths
# ---------------------------------------------------------------------------

class TestDeleteRecords:
    @pytest.mark.asyncio
    async def test_delete_records_api_error(self, repository):
        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN):
            session_cm, _, _ = make_aiohttp_ctx(500, text_data="Error")
            with patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                       return_value=session_cm):
                result = await repository.delete_records(
                    index_name="idx", endpoint_name="ep", primary_keys=["k1"]
                )

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_delete_records_exception(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("del crash")):
            result = await repository.delete_records(
                index_name="idx", endpoint_name="ep", primary_keys=["k1"]
            )
        assert result["success"] is False


# ---------------------------------------------------------------------------
# describe_index - failure path
# ---------------------------------------------------------------------------

class TestDescribeIndex:
    @pytest.mark.asyncio
    async def test_describe_index_when_get_index_fails(self, repository):
        with patch.object(repository, "get_index", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = IndexResponse(
                success=False, error="not found", message="Index not found"
            )
            result = await repository.describe_index("missing.idx", "ep")

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_describe_index_exception(self, repository):
        with patch.object(repository, "get_index", side_effect=Exception("crash")):
            result = await repository.describe_index("idx", "ep")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# similarity_search - exception path
# ---------------------------------------------------------------------------

class TestSimilaritySearchException:
    @pytest.mark.asyncio
    async def test_exception_returns_error_dict(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("search crash")):
            result = await repository.similarity_search(
                index_name="idx", endpoint_name="ep",
                query_vector=[0.1] * 5, columns=["id"]
            )
        assert result["success"] is False
        assert "search crash" in result["error"]


# ---------------------------------------------------------------------------
# similarity_search - debug path (empty results with filters triggers second req)
# ---------------------------------------------------------------------------

class TestSimilaritySearchDebugPath:
    @pytest.mark.asyncio
    async def test_empty_results_with_filters_triggers_debug_search(self, repository, monkeypatch):
        """With KASAL_VS_DEBUG opted in, 0 filtered results trigger the
        unfiltered diagnostic re-query (default-off since PERF-029)."""
        monkeypatch.setenv("KASAL_VS_DEBUG", "1")
        # First call: main search - 0 results
        main_results = {"result": {"data_array": [], "row_count": 0}}
        # Second call: debug search - some results
        debug_results = {"result": {"data_array": [["id1", "crew1"]], "row_count": 1}}

        responses = [
            (200, main_results),
            (200, debug_results),
        ]
        call_index = [0]

        async def fake_response_json():
            idx = call_index[0]
            _, data = responses[idx]
            call_index[0] += 1
            return data

        # Build mock session that returns different responses on successive calls
        first_response = AsyncMock()
        first_response.status = 200
        first_response.json = AsyncMock(return_value=main_results)

        second_response = AsyncMock()
        second_response.status = 200
        second_response.json = AsyncMock(return_value=debug_results)

        call_count = [0]
        def make_post_cm(resp):
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        session = MagicMock()
        responses_queue = [make_post_cm(first_response), make_post_cm(second_response)]

        def fake_post(*args, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return responses_queue[idx]

        session.post = MagicMock(side_effect=fake_post)

        session_cm = MagicMock()
        session_cm.__aenter__ = AsyncMock(return_value=session)
        session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN), \
             patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                   return_value=session_cm):
            result = await repository.similarity_search(
                index_name="catalog.schema.idx",
                endpoint_name="ep",
                query_vector=[0.1] * 5,
                columns=["id"],
                filters={"crew_id": "crew-x"}
            )

        assert result["success"] is True
        # Both calls were made - main search and debug search
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_debug_search_with_failed_second_request(self, repository):
        """When debug search fails (non-200), still returns success from main search."""
        main_results = {"result": {"data_array": [], "row_count": 0}}

        first_response = AsyncMock()
        first_response.status = 200
        first_response.json = AsyncMock(return_value=main_results)

        second_response = AsyncMock()
        second_response.status = 500
        second_response.text = AsyncMock(return_value="Internal error")

        call_count = [0]

        def make_cm(resp):
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        session = MagicMock()
        queue = [make_cm(first_response), make_cm(second_response)]

        def fake_post(*a, **k):
            idx = call_count[0]
            call_count[0] += 1
            return queue[idx]

        session.post = MagicMock(side_effect=fake_post)
        session_cm = MagicMock()
        session_cm.__aenter__ = AsyncMock(return_value=session)
        session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN), \
             patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                   return_value=session_cm):
            result = await repository.similarity_search(
                index_name="catalog.schema.idx",
                endpoint_name="ep",
                query_vector=[0.1] * 5,
                columns=["id"],
                filters={"crew_id": "crew-x"}
            )

        assert result["success"] is True


# ---------------------------------------------------------------------------
# empty_index
# ---------------------------------------------------------------------------

class TestEmptyIndex:
    @pytest.mark.asyncio
    async def test_empty_index_not_found_creates_new(self, repository):
        """When index not found, empty_index creates a new one."""
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

        # First call: GET index - 404 (not found)
        get_response = AsyncMock()
        get_response.status = 404
        get_response.text = AsyncMock(return_value="not found")

        # Second call: POST create index - 201
        create_response = AsyncMock()
        create_response.status = 201
        create_response.text = AsyncMock(return_value="created")

        # Third call: GET created index info (called by describe)
        info_response = AsyncMock()
        info_response.status = 200
        info_response.json = AsyncMock(return_value={
            "name": "catalog.schema.short_term",
            "status": {"state": "ONLINE", "ready": True, "indexed_row_count": 0}
        })

        call_count = [0]

        def make_cm(resp):
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        session = MagicMock()
        get_queue = [make_cm(get_response), make_cm(info_response)]
        get_call_count = [0]

        def fake_get(*a, **k):
            idx = get_call_count[0]
            get_call_count[0] += 1
            if idx < len(get_queue):
                return get_queue[idx]
            return make_cm(info_response)

        session.get = MagicMock(side_effect=fake_get)
        session.post = MagicMock(return_value=make_cm(create_response))

        session_cm = MagicMock()
        session_cm.__aenter__ = AsyncMock(return_value=session)
        session_cm.__aexit__ = AsyncMock(return_value=None)

        # Mock DatabricksIndexSchemas
        mock_schema = {"columns": [{"name": "id", "type": "string"}], "primary_key": "id"}

        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN), \
             patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                   return_value=session_cm), \
             patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas.get_schema",
                   return_value=mock_schema):
            result = await repository.empty_index(
                index_name="catalog.schema.short_term",
                endpoint_name="my_endpoint",
                embedding_dimension=1024,
            )

        assert "success" in result

    @pytest.mark.asyncio
    async def test_empty_index_exception_returns_error(self, repository):
        with patch.object(repository, "_get_auth_token", side_effect=Exception("auth crash")):
            result = await repository.empty_index(
                index_name="catalog.schema.idx",
                endpoint_name="ep",
                embedding_dimension=768,
            )
        assert result["success"] is False
        assert "auth crash" in result["error"]

    @pytest.mark.asyncio
    async def test_empty_index_with_existing_records(self, repository):
        """When index exists with records, deletes them all."""
        # GET index - 200
        get_response = AsyncMock()
        get_response.status = 200
        get_response.json = AsyncMock(return_value={
            "name": "catalog.schema.idx",
            "index_type": "DIRECT_ACCESS",
            "status": {"state": "ONLINE", "ready": True, "indexed_row_count": 3},
            "primary_key": "id",
            "direct_access_index_spec": {
                "embedding_vector_columns": [{"name": "embedding", "embedding_dimension": 1024}],
                "schema_json": '{"columns": [{"name": "id", "type": "string"}]}'
            }
        })

        # POST scan/query - 200 with some records
        scan_response = AsyncMock()
        scan_response.status = 200
        scan_response.json = AsyncMock(return_value={
            "result": {"data_array": [["id1"], ["id2"], ["id3"]], "row_count": 3}
        })

        # POST delete - 204
        delete_response = AsyncMock()
        delete_response.status = 204

        def make_cm(resp):
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        session = MagicMock()
        session.get = MagicMock(return_value=make_cm(get_response))
        session.post = MagicMock(side_effect=[make_cm(scan_response), make_cm(delete_response)])

        session_cm = MagicMock()
        session_cm.__aenter__ = AsyncMock(return_value=session)
        session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch.object(repository, "_get_auth_token", new_callable=AsyncMock, return_value=AUTH_TOKEN), \
             patch("src.repositories.databricks_vector_index_repository.shared_client_session",
                   return_value=session_cm):
            result = await repository.empty_index(
                index_name="catalog.schema.idx",
                endpoint_name="ep",
                embedding_dimension=1024,
            )

        assert "success" in result


class TestDebugRequeryGate:
    """Regression (PERF-029): the unfiltered diagnostic re-query must be
    OPT-IN. It used to fire on EVERY filtered search that returned zero rows
    (the hottest cold-start path), doubling vector-search round trips and
    reading other tenants' crew_ids into the logs."""

    def _empty_result_session(self):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": {"data_array": []}})

        mock_session = MagicMock()
        mock_post_cm = MagicMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post = MagicMock(return_value=mock_post_cm)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        return mock_session, mock_session_cm

    async def _search_with_empty_filtered_result(self, repository, mock_auth_token, monkeypatch, debug_env):
        if debug_env is None:
            monkeypatch.delenv("KASAL_VS_DEBUG", raising=False)
        else:
            monkeypatch.setenv("KASAL_VS_DEBUG", debug_env)

        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            with patch('src.repositories.databricks_vector_index_repository.shared_client_session') as mock_scs:
                mock_session, mock_session_cm = self._empty_result_session()
                mock_scs.return_value = mock_session_cm

                await repository.similarity_search(
                    index_name="catalog.schema.test_index",
                    endpoint_name="test_endpoint",
                    query_vector=[0.1, 0.2],
                    columns=["id", "content"],
                    num_results=5,
                    filters={"crew_id": "crew-1", "group_id": "g-1"},
                )
                return mock_session.post.call_count

    @pytest.mark.asyncio
    async def test_no_debug_requery_by_default(self, repository, mock_auth_token, monkeypatch):
        post_calls = await self._search_with_empty_filtered_result(
            repository, mock_auth_token, monkeypatch, debug_env=None
        )
        assert post_calls == 1  # the real query only — no unfiltered re-query

    @pytest.mark.asyncio
    async def test_debug_requery_fires_when_opted_in(self, repository, mock_auth_token, monkeypatch):
        post_calls = await self._search_with_empty_filtered_result(
            repository, mock_auth_token, monkeypatch, debug_env="1"
        )
        assert post_calls == 2  # real query + diagnostic re-query
