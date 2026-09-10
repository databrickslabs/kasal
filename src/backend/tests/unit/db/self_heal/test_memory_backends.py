"""Existing deployments must gain memory tuning before ORM reads start."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal.columns import _ensure_memory_backend_columns
from src.db.self_heal.runner import run_schema_self_heal
from src.models.memory_backend import MemoryBackend


@pytest.mark.asyncio
async def test_startup_repairs_legacy_memory_and_preserves_settings():
    engine = create_async_engine("sqlite+aiosqlite://")
    table = MemoryBackend.__table__
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE memory_backends ("
                "id VARCHAR PRIMARY KEY, group_id VARCHAR(100) NOT NULL, "
                "name VARCHAR(255) NOT NULL, description VARCHAR(1000), "
                "backend_type VARCHAR NOT NULL, databricks_config JSON, "
                "lakebase_config JSON, custom_config JSON, is_active BOOLEAN, "
                "is_default BOOLEAN, created_at DATETIME, updated_at DATETIME, "
                "enable_short_term BOOLEAN, enable_long_term BOOLEAN, "
                "enable_entity BOOLEAN, enable_relationship_retrieval BOOLEAN)"
            )
            await conn.exec_driver_sql(
                "INSERT INTO memory_backends "
                "(id, group_id, name, backend_type, lakebase_config, "
                "is_active, enable_short_term) "
                "VALUES ('memory-1', 'team-1', 'Existing memory', 'LAKEBASE', "
                '\'{"schema": "existing_memory"}\', 1, 1)'
            )
            with pytest.raises(OperationalError, match="cognitive_config"):
                await conn.execute(select(table))

            # Exercise the actual startup pass: a helper alone is insufficient
            # if it is never registered in the deployment path.
            await run_schema_self_heal(conn)
            row = (await conn.execute(select(table))).mappings().one()
            assert row["cognitive_config"] is None
            assert row["lakebase_config"] == {"schema": "existing_memory"}
            assert row["name"] == "Existing memory"
            assert row["group_id"] == "team-1"
            assert row["is_active"] is True

            tuning = {"semantic_weight": 0.6, "recall_max_depth": 4}
            await conn.execute(
                update(table)
                .where(table.c.id == "memory-1")
                .values(cognitive_config=tuning)
            )
            await run_schema_self_heal(conn)
            assert (await conn.execute(select(table))).mappings().one()[
                "cognitive_config"
            ] == tuning
            assert (
                await conn.exec_driver_sql(
                    "SELECT enable_short_term FROM memory_backends"
                )
            ).scalar_one() == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [False, True])
async def test_postgres_adds_native_json_only_when_missing(present):
    conn = MagicMock()
    conn.engine.dialect.name = "postgresql"
    catalogue = MagicMock()
    catalogue.fetchall.return_value = [("id",)] + (
        [("cognitive_config",)] if present else []
    )
    conn.exec_driver_sql = AsyncMock(return_value=catalogue)

    await _ensure_memory_backend_columns(conn)

    statements = [call.args[0] for call in conn.exec_driver_sql.await_args_list]
    assert "information_schema.columns" in statements[0]
    assert "memory_backends" in statements[0]
    assert statements[1:] == (
        []
        if present
        else [
            "ALTER TABLE memory_backends ADD COLUMN IF NOT EXISTS cognitive_config JSON"
        ]
    )
