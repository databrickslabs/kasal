"""Contract tests for DatabricksStorageBackend._similarity_query.

Pins the repository response shape: DatabricksVectorIndexRepository methods
return ``{"success": bool, "results": <raw API json>, "message": str}`` with
rows at ``results["result"]["data_array"]``. A previous bug read
``response["result"]`` (one level too shallow), so every vector search
silently returned zero records while still paying the full embed + auth +
search round trip.
"""

import pytest
from unittest.mock import AsyncMock, patch

from src.engines.crewai.memory.databricks_storage_backend import (
    DatabricksStorageBackend,
    _SCHEMA_COLUMNS,
)
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas


def _make_backend():
    with patch(
        "src.engines.crewai.memory.databricks_storage_backend.DatabricksVectorIndexRepository"
    ):
        backend = DatabricksStorageBackend(
            index_name="cat.schema.idx",
            endpoint_name="ep",
            workspace_url="https://example.com",
            crew_id="crew-1",
            group_id="group-1",
        )
    backend._repo = AsyncMock()
    return backend


def _row(content="remembered fact", score=0.87):
    positions = DatabricksIndexSchemas.get_column_positions("unified")
    row: list = [None] * len(_SCHEMA_COLUMNS)
    row[positions["id"]] = "rec-1"
    row[positions["content"]] = content
    row[positions["scope"]] = "/crew"
    row[positions["categories"]] = "[]"
    row[positions["importance"]] = 0.5
    row[positions["metadata"]] = "{}"
    row[positions["crew_id"]] = "crew-1"
    row[positions["group_id"]] = "group-1"
    row.append(score)  # similarity score is appended after requested columns
    return row


@pytest.mark.asyncio
async def test_similarity_query_parses_repository_response_shape():
    """Rows at results['result']['data_array'] must be returned, with scores."""
    backend = _make_backend()
    backend._repo.similarity_search.return_value = {
        "success": True,
        "results": {"result": {"data_array": [_row()]}},
        "message": "Search completed successfully",
    }

    out = await backend._similarity_query(query_vector=[0.0], limit=5)

    assert len(out) == 1
    record, score = out[0]
    assert record.id == "rec-1"
    assert record.content == "remembered fact"
    assert score == pytest.approx(0.87)


@pytest.mark.asyncio
async def test_similarity_query_legacy_shallow_shape_returns_nothing():
    """The pre-fix shallow shape must not be silently accepted as rows."""
    backend = _make_backend()
    backend._repo.similarity_search.return_value = {
        "result": {"data_array": [_row()]},
    }

    out = await backend._similarity_query(query_vector=[0.0], limit=5)

    assert out == []  # no 'success' key -> treated as failure, not silence


@pytest.mark.asyncio
async def test_similarity_query_failure_response_returns_empty():
    backend = _make_backend()
    backend._repo.similarity_search.return_value = {
        "success": False,
        "error": "Query failed (403)",
        "message": "Search failed",
    }

    out = await backend._similarity_query(query_vector=[0.0], limit=5)

    assert out == []


@pytest.mark.asyncio
async def test_similarity_query_none_response_returns_empty():
    backend = _make_backend()
    backend._repo.similarity_search.return_value = None

    out = await backend._similarity_query(query_vector=[0.0], limit=5)

    assert out == []


class TestBridgeLoop:
    """PERF-012/013: the sync->async bridge must reuse ONE long-lived loop —
    a fresh loop per call defeated engine/session/auth caching entirely."""

    def test_run_sync_uses_same_loop_across_calls(self):
        import asyncio

        backend = _make_backend()

        async def whoami():
            return id(asyncio.get_running_loop())

        loop_a = backend._run_sync(whoami())
        loop_b = backend._run_sync(whoami())
        assert loop_a == loop_b

    def test_lakebase_backend_shares_the_bridge_loop(self):
        import asyncio
        from src.engines.crewai.memory.lakebase_storage_backend import LakebaseStorageBackend

        backend = _make_backend()

        async def whoami():
            return id(asyncio.get_running_loop())

        databricks_loop = backend._run_sync(whoami())
        lakebase_loop = LakebaseStorageBackend._run_sync(
            object.__new__(LakebaseStorageBackend), whoami()
        )
        assert databricks_loop == lakebase_loop

    def test_run_sync_passthrough_for_non_coroutine(self):
        backend = _make_backend()
        assert backend._run_sync(42) == 42

    def test_run_sync_propagates_exceptions(self):
        backend = _make_backend()

        async def boom():
            raise ValueError("inner failure")

        with pytest.raises(ValueError, match="inner failure"):
            backend._run_sync(boom())

    def test_bridge_loop_recreated_if_closed(self):
        from src.engines.crewai.memory import databricks_storage_backend as m

        loop1 = m._get_bridge_loop()
        loop1.call_soon_threadsafe(loop1.stop)
        import time
        for _ in range(50):
            if not loop1.is_running():
                break
            time.sleep(0.05)
        loop1.close()
        loop2 = m._get_bridge_loop()
        assert loop2 is not loop1
        assert not loop2.is_closed()
