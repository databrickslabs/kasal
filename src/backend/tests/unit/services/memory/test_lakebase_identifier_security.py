"""Caller strings never reach Lakebase SQL text unvalidated.

Found during the Apps security scan triage (2026-10-28):

* ``/lakebase/initialize-tables`` takes ``memory_table`` from a raw request body
  (the schema's validator never runs on that path) and the service interpolated
  it into CREATE TABLE / CREATE INDEX DDL. ``get_table_stats`` had the same gap.
* ``metadata_filter`` keys were interpolated into ``metadata->>'{key}'``.
"""

import asyncio
from unittest.mock import patch

import pytest

from src.services.memory.config.lakebase_service import LakebaseMemoryService
from tests.unit.services.memory.test_lakebase_kinds_sql import _backend, _capture

HOSTILE = "crew_memory (id int); DROP TABLE users; --"


class TestMemoryTableNameIsValidatedAtTheSink:
    @pytest.mark.parametrize("method", ["initialize_tables", "get_table_stats"])
    def test_a_hostile_name_is_refused_before_any_session(self, method):
        service = LakebaseMemoryService()
        with (
            patch(
                "src.services.memory.config.lakebase_service.get_lakebase_session"
            ) as session,
            pytest.raises(ValueError, match="Invalid memory table name"),
        ):
            asyncio.run(getattr(service, method)(memory_table=HOSTILE))
        session.assert_not_called()


class TestMetadataFilterKeysAreBound:
    @pytest.mark.asyncio
    async def test_search_binds_the_key(self):
        key = "scope' OR '1'='1"
        sql, params = await _capture(
            lambda s: _backend().asearch(
                query_embedding=[0.1, 0.2, 0.3, 0.4], metadata_filter={key: "v"}
            )
        )
        assert "metadata ->> :mk_0 = :mf_0" in sql
        assert key not in sql
        assert params["mk_0"] == key and params["mf_0"] == "v"

    @pytest.mark.asyncio
    async def test_delete_binds_the_key(self):
        key = "x'; DELETE FROM crew_memory; --"
        sql, params = await _capture(
            lambda s: _backend().adelete(metadata_filter={key: "v"})
        )
        assert key not in sql
        assert params["mk_0"] == key
