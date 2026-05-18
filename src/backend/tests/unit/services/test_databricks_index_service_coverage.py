"""
Coverage-focused tests for DatabricksIndexService.
Targets uncovered branches to push coverage to 85%+.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.services.databricks_index_service import DatabricksIndexService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(**kw):
    defaults = dict(
        workspace_url="https://ws.databricks.com",
        endpoint_name="vs-endpoint",
        document_endpoint_name=None,
        embedding_dimension=1024,
        short_term_index=None,
        long_term_index=None,
        entity_index=None,
        document_index=None,
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


def make_svc(workspace_url=None):
    with patch("src.services.databricks_index_service.DatabricksVectorIndexRepository"), \
         patch("src.services.databricks_index_service.DatabricksVectorEndpointRepository"):
        svc = DatabricksIndexService(workspace_url=workspace_url)
    svc._index_repo = None
    svc._endpoint_repo = None
    return svc


# ---------------------------------------------------------------------------
# _get_index_repository
# ---------------------------------------------------------------------------

class TestGetIndexRepository:
    def test_creates_new_repo(self):
        svc = make_svc()
        with patch("src.services.databricks_index_service.DatabricksVectorIndexRepository") as cls:
            cls.return_value = MagicMock(workspace_url="https://ws.databricks.com", group_id=None)
            repo = svc._get_index_repository("https://ws.databricks.com")
        assert repo is not None

    def test_reuses_existing_repo_same_url(self):
        svc = make_svc()
        mock_repo = MagicMock(workspace_url="https://ws.databricks.com", group_id=None)
        svc._index_repo = mock_repo
        repo = svc._get_index_repository("https://ws.databricks.com")
        assert repo is mock_repo

    def test_creates_new_repo_for_different_url(self):
        svc = make_svc()
        mock_repo = MagicMock(workspace_url="https://old.databricks.com", group_id=None)
        svc._index_repo = mock_repo
        with patch("src.services.databricks_index_service.DatabricksVectorIndexRepository") as cls:
            new_repo = MagicMock(workspace_url="https://new.databricks.com", group_id=None)
            cls.return_value = new_repo
            repo = svc._get_index_repository("https://new.databricks.com")
        assert repo is new_repo


# ---------------------------------------------------------------------------
# _get_endpoint_repository
# ---------------------------------------------------------------------------

class TestGetEndpointRepository:
    def test_creates_new_endpoint_repo(self):
        svc = make_svc()
        with patch("src.services.databricks_index_service.DatabricksVectorEndpointRepository") as cls:
            cls.return_value = MagicMock(workspace_url="https://ws.databricks.com")
            repo = svc._get_endpoint_repository("https://ws.databricks.com")
        assert repo is not None

    def test_reuses_existing_endpoint_repo(self):
        svc = make_svc()
        mock_repo = MagicMock(workspace_url="https://ws.databricks.com")
        svc._endpoint_repo = mock_repo
        repo = svc._get_endpoint_repository("https://ws.databricks.com")
        assert repo is mock_repo


# ---------------------------------------------------------------------------
# create_databricks_index
# ---------------------------------------------------------------------------

class TestCreateDatabricksIndex:
    @pytest.mark.asyncio
    async def test_create_short_term_success(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=True,
            message="Created",
            error=None,
        ))
        svc._index_repo = mock_repo
        mock_repo.workspace_url = "https://ws.databricks.com"
        mock_repo.group_id = None

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config,
                    index_type="short_term",
                    catalog="main",
                    schema="default",
                    table_name="st_memory",
                )
        assert result["success"] is True
        assert config.short_term_index == "main.default.st_memory"

    @pytest.mark.asyncio
    async def test_create_long_term_success(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=True, message="Created", error=None
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="long_term", catalog="main", schema="default", table_name="lt"
                )
        assert result["success"] is True
        assert config.long_term_index == "main.default.lt"

    @pytest.mark.asyncio
    async def test_create_entity_index_success(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=True, message="Created", error=None
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="entity", catalog="main", schema="default", table_name="entity"
                )
        assert result["success"] is True
        assert config.entity_index == "main.default.entity"

    @pytest.mark.asyncio
    async def test_create_document_index_uses_document_endpoint(self):
        svc = make_svc()
        config = make_config(document_endpoint_name="doc-endpoint")
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=True, message="Created", error=None
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="document", catalog="main", schema="default", table_name="doc"
                )
        assert result["success"] is True
        assert config.document_index == "main.default.doc"

    @pytest.mark.asyncio
    async def test_create_fails_already_exists(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=False, message="Index already exists", error="Index already exists"
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="short_term", catalog="main", schema="default", table_name="st"
                )
        assert result["success"] is False
        assert "already exists" in result["message"]

    @pytest.mark.asyncio
    async def test_create_fails_other_error(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(return_value=SimpleNamespace(
            success=False, message="Some error", error="some error"
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="short_term", catalog="main", schema="default", table_name="st"
                )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_create_unknown_index_type_raises(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = None  # Unknown type
                result = await svc.create_databricks_index(
                    config=config, index_type="unknown", catalog="main", schema="default", table_name="x"
                )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_create_handles_exception(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.create_index = AsyncMock(side_effect=Exception("connection error"))
        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_schema.return_value = {"fields": []}
                result = await svc.create_databricks_index(
                    config=config, index_type="short_term", catalog="main", schema="default", table_name="x"
                )
        assert result["success"] is False
        assert "connection error" in result["message"]


# ---------------------------------------------------------------------------
# get_databricks_indexes
# ---------------------------------------------------------------------------

class TestGetDatabricksIndexes:
    @pytest.mark.asyncio
    async def test_lists_indexes_successfully(self):
        svc = make_svc()
        config = make_config()
        mock_index = SimpleNamespace(
            name="main.default.st",
            state="READY",
            embedding_dimension=1024,
            primary_key="id",
            row_count=100,
        )
        mock_repo = AsyncMock()
        mock_repo.list_indexes = AsyncMock(return_value=SimpleNamespace(
            success=True, message="OK", indexes=[mock_index]
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.get_databricks_indexes(config)
        assert result["success"] is True
        assert len(result["indexes"]) == 1
        assert result["indexes"][0]["name"] == "main.default.st"

    @pytest.mark.asyncio
    async def test_returns_failure_on_list_error(self):
        svc = make_svc()
        config = make_config()
        mock_repo = AsyncMock()
        mock_repo.list_indexes = AsyncMock(return_value=SimpleNamespace(
            success=False, message="List failed", indexes=[]
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.get_databricks_indexes(config)
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        config = make_config()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("conn error")):
            result = await svc.get_databricks_indexes(config)
        assert result["success"] is False


# ---------------------------------------------------------------------------
# delete_databricks_index
# ---------------------------------------------------------------------------

class TestDeleteDatabricksIndex:
    @pytest.mark.asyncio
    async def test_deletes_successfully(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.delete_index = AsyncMock(return_value=SimpleNamespace(success=True, message="Deleted"))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.delete_databricks_index("https://ws.databricks.com", "main.default.st", "endpoint")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("fail")):
            result = await svc.delete_databricks_index("https://ws.databricks.com", "idx", "ep")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# delete_databricks_endpoint
# ---------------------------------------------------------------------------

class TestDeleteDatabricksEndpoint:
    @pytest.mark.asyncio
    async def test_deletes_endpoint_successfully(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.delete_endpoint = AsyncMock(return_value=SimpleNamespace(success=True, message="Deleted"))

        with patch.object(svc, "_get_endpoint_repository", return_value=mock_repo):
            result = await svc.delete_databricks_endpoint("https://ws.databricks.com", "my-endpoint")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_endpoint_repository", side_effect=Exception("fail")):
            result = await svc.delete_databricks_endpoint("https://ws.databricks.com", "ep")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# get_index_info
# ---------------------------------------------------------------------------

class TestGetIndexInfo:
    @pytest.mark.asyncio
    async def test_returns_index_info_on_success(self):
        svc = make_svc()
        mock_index = SimpleNamespace(
            row_count=50,
            indexed_row_count=50,
            index_type="DIRECT_ACCESS",
            embedding_dimension=768,
            primary_key="id",
            state="READY",
            ready=True,
        )
        mock_repo = AsyncMock()
        mock_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=True, index=mock_index, message="OK"
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.IndexType") as it:
                it.DIRECT_ACCESS = "DIRECT_ACCESS"
                result = await svc.get_index_info("https://ws.databricks.com", "main.default.st", "endpoint")
        assert result["success"] is True
        assert result["doc_count"] == 50

    @pytest.mark.asyncio
    async def test_returns_failure_when_not_found(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=False, index=None, message="Not found"
        ))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.get_index_info("https://ws.databricks.com", "x", "ep")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("error")):
            result = await svc.get_index_info("https://ws.databricks.com", "x", "ep")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# empty_index
# ---------------------------------------------------------------------------

class TestEmptyIndex:
    @pytest.mark.asyncio
    async def test_empties_index_successfully(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.empty_index = AsyncMock(return_value={"success": True, "message": "Emptied"})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.empty_index("https://ws.databricks.com", "main.default.st", "endpoint", "short_term", 1024)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_logs_failure(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.empty_index = AsyncMock(return_value={"success": False, "message": "Empty failed"})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            result = await svc.empty_index("https://ws.databricks.com", "main.default.st", "ep", "short_term", 1024)
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("fail")):
            result = await svc.empty_index("https://ws.databricks.com", "idx", "ep", "short_term", 1024)
        assert result["success"] is False


# ---------------------------------------------------------------------------
# get_index_documents
# ---------------------------------------------------------------------------

class TestGetIndexDocuments:
    @pytest.mark.asyncio
    async def test_returns_documents_for_short_term(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        data_array = [
            ["id1", "content text", "query", "sess1", "1", "2024-01-01", "crew1", "agent1", "{}"]
        ]
        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": data_array
                }
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = ["id", "content", "query_text", "session_id",
                                                           "interaction_sequence", "timestamp",
                                                           "crew_id", "agent_id", "metadata"]
                schemas.get_column_positions.return_value = {
                    "id": 0, "content": 1, "query_text": 2, "session_id": 3,
                    "interaction_sequence": 4, "timestamp": 5, "crew_id": 6,
                    "agent_id": 7, "metadata": 8
                }
                result = await svc.get_index_documents("https://ws.databricks.com", "endpoint", "main.default.st",
                                                        index_type="short_term")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_returns_documents_for_long_term(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        search_response = {
            "success": True,
            "results": {"data_array": [{"id": "1", "content": "c"}]}
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = ["id", "content"]
                schemas.get_column_positions.return_value = {"id": 0, "content": 1}
                result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.long_term_x",
                                                        index_type="long_term")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_infers_memory_type_from_index_name(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={"success": True, "results": {}})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = []
                schemas.get_column_positions.return_value = {}
                result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.entity_store")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_returns_failure_when_search_fails(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={
            "success": False, "message": "Search failed"
        })

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = []
                result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.x")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_handles_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("error")):
            result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.x")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_results_as_list(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={
            "success": True,
            "results": [{"id": "r1", "content": "c"}]
        })

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = ["id", "content"]
                schemas.get_column_positions.return_value = {"id": 0, "content": 1}
                result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.x",
                                                        index_type="document")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_results_data_key(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={
            "success": True,
            "results": {"data": [{"id": "d1", "content": "c"}]}
        })

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = ["id", "content"]
                schemas.get_column_positions.return_value = {"id": 0, "content": 1}
                result = await svc.get_index_documents("https://ws.databricks.com", "ep", "main.default.x",
                                                        index_type="entity")
        assert result["success"] is True


# ---------------------------------------------------------------------------
# wait_for_index_ready (basic paths)
# ---------------------------------------------------------------------------

class TestWaitForIndexReady:
    @pytest.mark.asyncio
    async def test_returns_success_when_ready(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_index = SimpleNamespace(state="READY", ready=True)
        mock_repo.get_index = AsyncMock(return_value=SimpleNamespace(success=True, index=mock_index, message="OK"))
        mock_repo.describe_index = AsyncMock(return_value={"success": False})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("asyncio.sleep", new=AsyncMock()):
                result = await svc.wait_for_index_ready("https://ws.databricks.com", "main.default.x", "ep",
                                                         max_wait_seconds=10, check_interval_seconds=1)
        assert result["success"] is True
        assert result["ready"] is True

    @pytest.mark.asyncio
    async def test_times_out_when_not_ready(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_index = SimpleNamespace(state="PROVISIONING", ready=False)
        mock_repo.get_index = AsyncMock(return_value=SimpleNamespace(success=True, index=mock_index, message="OK"))
        mock_repo.describe_index = AsyncMock(return_value={"success": False})

        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1

        async def fake_get_event_loop_time():
            return call_count * 100  # Always exceed max_wait

        import asyncio

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("asyncio.sleep", new=fake_sleep):
                with patch("asyncio.get_event_loop") as gel:
                    mock_loop = MagicMock()
                    # First call returns 0, subsequent calls return large numbers
                    mock_loop.time.side_effect = [0, 400, 400]
                    gel.return_value = mock_loop
                    result = await svc.wait_for_index_ready(
                        "https://ws.databricks.com", "main.default.x", "ep",
                        max_wait_seconds=300, check_interval_seconds=10
                    )
        # Should have timed out
        assert result["ready"] is False

    @pytest.mark.asyncio
    async def test_handles_exception_in_check(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.get_index = AsyncMock(side_effect=Exception("check failed"))

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("asyncio.get_event_loop") as gel:
                mock_loop = MagicMock()
                mock_loop.time.side_effect = [0, 400]
                gel.return_value = mock_loop
                with patch("asyncio.sleep", new=AsyncMock()):
                    result = await svc.wait_for_index_ready(
                        "https://ws.databricks.com", "main.default.x", "ep",
                        max_wait_seconds=5, check_interval_seconds=1
                    )
        assert result["ready"] is False

    @pytest.mark.asyncio
    async def test_handles_outer_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("outer error")):
            result = await svc.wait_for_index_ready("https://ws.databricks.com", "x", "ep")
        assert result["success"] is False
        assert result["ready"] is False

# ---------------------------------------------------------------------------
# query_entity_data
# ---------------------------------------------------------------------------

class TestQueryEntityData:
    @pytest.mark.asyncio
    async def test_query_entity_data_success(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        # Return entity data with relationships
        data_array = [
            ["entity_1", "person", "Alice", "Alice is a researcher", "[]", "{}", 0.9, "crew1", "agent1", "2024-01-01", "0.9", "context", "entity_name:Alice"],
        ]
        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": data_array,
                    "column_names": []
                }
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_type", "entity_name", "description",
                                                   "relationships", "attributes", "confidence_score",
                                                   "crew_id", "agent_id", "timestamp", "source_context",
                                                   "relationship_data", "metadata"]
                result = await svc.query_entity_data(
                    workspace_url="https://ws.databricks.com",
                    endpoint_name="endpoint",
                    index_name="main.default.entity",
                )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_query_entity_data_with_relationships_string(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        import json
        data_array = [
            ["e1", "person", "Bob", "Bob is a scientist",
             json.dumps(["Alice", {"target": "Carol", "type": "knows", "strength": 0.8}]),
             "{}", 0.9, "crew1", "agent1", "2024-01-01", "", "", ""],
        ]
        search_response = {
            "success": True,
            "results": {
                "result": {"data_array": data_array}
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_type", "entity_name", "description",
                                                   "relationships", "attributes", "confidence_score",
                                                   "crew_id", "agent_id", "timestamp", "source_context",
                                                   "relationship_data", "metadata"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_query_entity_data_search_fails(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={"success": False, "message": "fail", "error": "err"})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_name"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_query_entity_data_exception_not_found(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("index not found")):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_query_entity_data_exception_other(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("other error")):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_query_entity_with_list_results(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        # List of lists format (row format)
        search_response = {
            "success": True,
            "results": [["e1", "person", "Alice", "Researcher", "[]", "{}", "0.9", "c1", "a1", "2024", "ctx", "{}", "meta"]]
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_type", "entity_name", "description",
                                                   "relationships", "attributes", "confidence_score",
                                                   "crew_id", "agent_id", "timestamp", "source_context",
                                                   "relationship_data", "metadata"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_query_entity_with_data_array_format(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        search_response = {
            "success": True,
            "results": {
                "data_array": [["e1", "Alice"]],
                "column_names": ["id", "entity_name"]
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_name"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_query_entity_manifest_format(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        search_response = {
            "success": True,
            "results": {
                "manifest": {"columns": [{"name": "id"}, {"name": "entity_name"}]},
                "result": {"data_array": [["e1", "Alice"]]}
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = ["id", "entity_name"]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")
        assert result["success"] is True


# ---------------------------------------------------------------------------
# search_vectors and _process_search_results
# ---------------------------------------------------------------------------

class TestSearchVectors:
    @pytest.mark.asyncio
    async def test_search_vectors_success(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        import json
        data_array = [
            ["id1", "content text", "query", "sess1", "1", "2024-01-01", "crew1", "agent1", json.dumps({"k": "v"}), 0.9],
        ]
        mock_repo.similarity_search = AsyncMock(return_value={
            "success": True,
            "results": {
                "result": {"data_array": data_array}
            }
        })

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = ["id", "content", "query_text", "session_id",
                                                            "interaction_sequence", "timestamp", "crew_id",
                                                            "agent_id", "metadata"]
                result = await svc.search_vectors(
                    workspace_url="https://ws.databricks.com",
                    index_name="main.default.st",
                    endpoint_name="endpoint",
                    query_embedding=[0.1, 0.2, 0.3],
                    memory_type="short_term",
                )
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_search_vectors_empty_results(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={"success": True, "results": {}})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = []
                result = await svc.search_vectors("https://ws.databricks.com", "main.default.st", "ep", [0.1], "short_term")
        assert result == []

    @pytest.mark.asyncio
    async def test_search_vectors_failure(self):
        svc = make_svc()
        mock_repo = AsyncMock()
        mock_repo.similarity_search = AsyncMock(return_value={"success": False, "message": "fail"})

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
                schemas.get_search_columns.return_value = []
                result = await svc.search_vectors("https://ws.databricks.com", "main.default.st", "ep", [0.1], "short_term")
        assert result == []

    @pytest.mark.asyncio
    async def test_search_vectors_exception(self):
        svc = make_svc()
        with patch.object(svc, "_get_index_repository", side_effect=Exception("conn error")):
            result = await svc.search_vectors("https://ws.databricks.com", "x", "ep", [0.1], "short_term")
        assert result == []


class TestProcessSearchResults:
    def test_process_short_term_results(self):
        svc = make_svc()
        import json
        raw = {
            "result": {
                "data_array": [
                    ["id1", "content text", "query", "sess1", "1", "2024-01-01", "crew1", "agent1",
                     json.dumps({"k": "v"}), 0.95]
                ]
            }
        }
        with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
            schemas.get_search_columns.return_value = ["id", "content", "query_text", "session_id",
                                                        "interaction_sequence", "timestamp", "crew_id",
                                                        "agent_id", "metadata"]
            results = svc._process_search_results(raw, "short_term")
        assert len(results) == 1
        assert results[0]["id"] == "id1"
        assert results[0]["score"] == 0.95
        assert isinstance(results[0]["metadata"], dict)

    def test_process_results_empty_row(self):
        svc = make_svc()
        raw = {"result": {"data_array": [[], ["id2"]]}}
        with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
            schemas.get_search_columns.return_value = ["id"]
            results = svc._process_search_results(raw, "long_term")
        # Empty row should be skipped, non-empty processed
        assert len(results) == 1

    def test_process_results_no_result_key(self):
        svc = make_svc()
        raw = {}
        with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
            schemas.get_search_columns.return_value = ["id"]
            results = svc._process_search_results(raw, "entity")
        assert results == []

    def test_process_results_exception(self):
        svc = make_svc()
        # Passing non-dict to cause exception
        with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
            schemas.get_search_columns.side_effect = Exception("schema error")
            results = svc._process_search_results({"result": {"data_array": [["id1"]]}}, "short_term")
        assert results == []

    def test_process_results_no_score(self):
        svc = make_svc()
        raw = {"result": {"data_array": [["id1", "content"]]}}
        with patch("src.schemas.databricks_index_schemas.DatabricksIndexSchemas") as schemas:
            schemas.get_search_columns.return_value = ["id", "content", "extra_col"]
            results = svc._process_search_results(raw, "document")
        assert len(results) == 1
        assert results[0]["score"] == 0.0


# ---------------------------------------------------------------------------
# query_entity_data - additional entity resolution paths
# ---------------------------------------------------------------------------

class TestEntityRelationshipResolution:
    @pytest.mark.asyncio
    async def test_relationship_targets_create_new_nodes(self):
        """Test that targets not in entity_map get new nodes created."""
        svc = make_svc()
        mock_repo = AsyncMock()

        import json
        data_array = [
            # Entity with a relationship target that doesn't exist
            ["e1", "person", "Alice", "Researcher",
             json.dumps([{"target": "new_team_researchers", "type": "works_with", "strength": 0.9,
                          "description": "Works with"}]),
             "{}", "0.9", "crew1", "agent1", "2024-01-01", "ctx", "{}", "meta"],
        ]
        search_response = {
            "success": True,
            "results": {
                "result": {"data_array": data_array}
            }
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = [
                    "id", "entity_type", "entity_name", "description",
                    "relationships", "attributes", "confidence_score",
                    "crew_id", "agent_id", "timestamp", "source_context",
                    "relationship_data", "metadata"
                ]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")

        assert result["success"] is True
        # The new target should create an additional entity node
        entity_names = [e["name"] for e in result["entities"]]
        assert "Alice" in entity_names

    @pytest.mark.asyncio
    async def test_entity_with_null_name_uses_description(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        data_array = [
            # Null entity_name should fall back to description
            ["e1", "person", None, "Alice is a researcher", "[]", "{}", "0.9", "crew1", "agent1", "2024-01-01", "", "", ""],
        ]
        search_response = {
            "success": True,
            "results": {"result": {"data_array": data_array}}
        }
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = [
                    "id", "entity_type", "entity_name", "description",
                    "relationships", "attributes", "confidence_score",
                    "crew_id", "agent_id", "timestamp", "source_context",
                    "relationship_data", "metadata"
                ]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_relationship_data_extended(self):
        svc = make_svc()
        mock_repo = AsyncMock()

        import json
        # relationship_data as list
        data_array = [
            ["e1", "org", "TechCorp", "A tech company", "[]",
             json.dumps({"key": "val"}), "0.9", "crew1", "agent1", "2024-01-01",
             "ctx", json.dumps([{"target": "Engineers", "type": "employs"}]), "meta"],
        ]
        search_response = {"success": True, "results": {"result": {"data_array": data_array}}}
        mock_repo.similarity_search = AsyncMock(return_value=search_response)

        with patch.object(svc, "_get_index_repository", return_value=mock_repo):
            with patch("src.services.databricks_index_service.DatabricksIndexSchemas") as schemas:
                schemas.ENTITY_SEARCH_COLUMNS = [
                    "id", "entity_type", "entity_name", "description",
                    "relationships", "attributes", "confidence_score",
                    "crew_id", "agent_id", "timestamp", "source_context",
                    "relationship_data", "metadata"
                ]
                result = await svc.query_entity_data("https://ws.databricks.com", "ep", "main.default.entity")

        assert result["success"] is True
