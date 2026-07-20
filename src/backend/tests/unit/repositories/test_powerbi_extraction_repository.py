"""
Unit tests for PowerBIExtractionRepository and the config-gen persistence hook.

Uses a real in-memory SQLite engine so the JSON columns, indexes and query
helpers are exercised end-to-end (the artifacts are the point of the feature —
mocking the session would test nothing meaningful).
"""
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.models.powerbi_extraction import PowerBIExtraction
from src.repositories.powerbi_extraction_repository import PowerBIExtractionRepository
from src.schemas.powerbi_extraction import PowerBIExtractionCreate


@pytest_asyncio.fixture
async def repo():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(PowerBIExtraction.__table__.create)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        yield PowerBIExtractionRepository(session)
    await engine.dispose()


def _payload(**over):
    base = dict(
        execution_id="job-1",
        workspace_id="ws-1",
        dataset_id="ds-1",
        report_id="rep-1",
        relationships=[{
            "from_table": "FactSales", "from_column": "DateKey",
            "from_cardinality": "Many", "to_table": "DimDate",
            "to_column": "DateKey", "to_cardinality": "One", "is_active": True,
        }],
        measures=[{"measure_name": "Total", "expression": "SUM(F[x])", "table_name": "FactSales"}],
        admin_tables={"FactSales": {"columns": [], "mquery_expression": "let ...", "measures": []}},
        report_definition={"visuals": []},
        proposed_config={"catalog": "main"},
        warnings=[],
        relationships_count=1,
        measures_count=1,
        measures_with_dax_count=1,
        admin_tables_count=1,
        summary="1 relationships, 1 measures (1 with DAX), 1 tables",
        group_id="g1",
        created_by_email="user@example.com",
    )
    base.update(over)
    return PowerBIExtractionCreate(**base).model_dump()


@pytest.mark.asyncio
async def test_create_persists_all_artifacts(repo):
    rec = await repo.create(_payload())
    await repo.session.commit()
    assert rec.id is not None
    # Raw artifacts survive the JSON round-trip.
    assert rec.relationships[0]["from_table"] == "FactSales"
    assert rec.measures[0]["expression"] == "SUM(F[x])"
    assert rec.admin_tables["FactSales"]["mquery_expression"] == "let ..."
    # Promoted counts are queryable columns.
    assert rec.relationships_count == 1
    assert rec.measures_with_dax_count == 1


@pytest.mark.asyncio
async def test_find_by_execution_id(repo):
    await repo.create(_payload())
    await repo.session.commit()
    rows = await repo.find_by_execution_id("job-1")
    assert len(rows) == 1
    assert rows[0].dataset_id == "ds-1"


@pytest.mark.asyncio
async def test_find_by_dataset_group_scoped(repo):
    await repo.create(_payload(group_id="g1"))
    await repo.create(_payload(group_id="g2"))
    await repo.session.commit()
    g1 = await repo.find_by_dataset("ds-1", group_id="g1")
    assert len(g1) == 1 and g1[0].group_id == "g1"
    both = await repo.find_by_dataset("ds-1")  # no group filter
    assert len(both) == 2


@pytest.mark.asyncio
async def test_get_latest_for_dataset(repo):
    await repo.create(_payload(summary="older"))
    await repo.create(_payload(summary="newer"))
    await repo.session.commit()
    latest = await repo.get_latest_for_dataset("ds-1")
    assert latest is not None
    # Newest-first ordering — the second insert is the latest.
    rows = await repo.find_by_dataset("ds-1")
    assert rows[0].id >= rows[-1].id


@pytest.mark.asyncio
async def test_find_by_group_pagination(repo):
    for _ in range(3):
        await repo.create(_payload())
    await repo.session.commit()
    page = await repo.find_by_group("g1", limit=2, offset=0)
    assert len(page) == 2
    page2 = await repo.find_by_group("g1", limit=2, offset=2)
    assert len(page2) == 1


class TestToolPersistenceHook:
    """The tool's _save_powerbi_extraction must be fail-open (never raise)."""

    @pytest.mark.asyncio
    async def test_save_is_fail_open_on_repo_error(self, monkeypatch):
        from src.engines.crewai.tools.custom.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        # The tool is a pydantic BaseTool; instantiate normally. The method reads
        # trace_context via getattr(..., None), so we don't need to set it.
        tool = PipelineConfigGeneratorTool()

        # Force the persistence path to blow up; the method must swallow it.
        import src.engines.crewai.tools.tool_session_provider as tsp

        class _Boom:
            async def __aenter__(self): raise RuntimeError("db down")
            async def __aexit__(self, *a): return False

        monkeypatch.setattr(
            tsp.ToolSessionProvider, "powerbi_extraction_repo",
            staticmethod(lambda: _Boom()),
        )

        # Should complete without raising.
        await tool._save_powerbi_extraction(
            relationships=[], measures=[], admin_tables={}, report_def=None,
            config={}, warnings=[], workspace_id="ws", dataset_id="ds", report_id=None,
        )
