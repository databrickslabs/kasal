"""The in-memory run registry never shows one teamspace another's runs.

``ExecutionService.executions`` is process-wide: it holds every teamspace's
in-flight runs. ``GET /executions`` merged it into the list unfiltered, so a
``bi-specialist`` list also showed ``user_dev_localhost``'s RUNNING entries. The
registry is now scoped by the same rule as the DB rows, and an entry whose row
exists (off-page, or terminal while the entry went stale) is left to the DB.
"""

from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.execution_history import ExecutionHistory
from src.services.execution.listing import memory_rows
from src.services.execution.service import ExecutionService

A, B = "bi-specialist", "user_dev_localhost"


def _entry(execution_id, group_id, **extra):
    return {
        "execution_id": execution_id,
        "status": "RUNNING",
        "created_at": datetime(2026, 9, 27, 10, 0, 0),
        "run_name": execution_id,
        "group_id": group_id,
        "group_email": f"{group_id}@example.com",
        **extra,
    }


@pytest.fixture
def registry():
    saved = dict(ExecutionService.executions)
    ExecutionService.executions.clear()
    ExecutionService.executions.update(
        {
            "a-live": _entry("a-live", A),
            "b-live": _entry("b-live", B),
            "no-group": _entry("no-group", None),
        }
    )
    yield ExecutionService.executions
    ExecutionService.executions.clear()
    ExecutionService.executions.update(saved)


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync: ExecutionHistory.__table__.create(sync, checkfirst=True)
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _listed(session, group_ids, **kwargs):
    rows = await ExecutionService(session=session).list_executions(
        group_ids=group_ids, **kwargs
    )
    return [row["execution_id"] for row in rows]


class TestListIsolation:
    @pytest.mark.asyncio
    async def test_a_group_sees_only_its_own_in_memory_runs(self, registry, session):
        assert await _listed(session, [A]) == ["a-live"]
        assert await _listed(session, [B]) == ["b-live"]

    @pytest.mark.asyncio
    async def test_no_group_lists_nothing(self, registry, session):
        # Fail closed, like the DB filter: the router passes [] when no
        # workspace is selected.
        assert await _listed(session, []) == []
        assert await _listed(session, None) == []

    @pytest.mark.asyncio
    async def test_a_stale_entry_whose_row_is_terminal_is_not_appended(
        self, registry, session
    ):
        # The run finished (row COMPLETED) but its RUNNING entry lingered. It
        # must not come back as a second, RUNNING copy of the run.
        registry["a-done"] = _entry("a-done", A)
        session.add(
            ExecutionHistory(
                job_id="a-done",
                status="COMPLETED",
                group_id=A,
                group_email=f"{A}@example.com",
                created_at=datetime(2026, 9, 27, 9, 0, 0),
            )
        )
        await session.commit()

        listed = await ExecutionService(session=session).list_executions(group_ids=[A])
        by_id = {row["execution_id"]: row["status"] for row in listed}
        assert by_id == {"a-done": "COMPLETED", "a-live": "RUNNING"}

    @pytest.mark.asyncio
    async def test_an_entry_whose_row_is_off_page_is_not_appended(
        self, registry, session
    ):
        # Two rows for A, page size 1: the other row is off-page, not missing.
        registry.clear()
        for job_id, hour in (("old", 8), ("new", 9)):
            registry[job_id] = _entry(job_id, A)
            session.add(
                ExecutionHistory(
                    job_id=job_id,
                    status="RUNNING",
                    group_id=A,
                    created_at=datetime(2026, 9, 27, hour, 0, 0),
                )
            )
        await session.commit()

        assert await _listed(session, [A], limit=1) == ["new"]


class TestMemoryRows:
    def test_user_email_narrows_like_the_db_filter(self, registry):
        rows = memory_rows(registry, [], [A, B], f"{B}@example.com", False)
        assert [r["execution_id"] for r in rows] == ["b-live"]

    def test_task_handle_and_payload_are_not_listed(self, registry):
        registry["a-live"].update(task=object(), result={"x": 1}, inputs={"y": 2})
        (row,) = memory_rows(registry, [], [A], None, False)
        assert not {"task", "result", "inputs"} & row.keys()
        (row,) = memory_rows(registry, [], [A], None, True)
        assert "task" not in row and row["result"] == {"x": 1}


class TestStatusFallbackIsolation:
    @pytest.mark.asyncio
    async def test_another_groups_in_memory_run_is_not_found(self, registry, session):
        service = ExecutionService(session=session)
        assert await service.get_execution_status("b-live", group_ids=[A]) is None
        mine = await service.get_execution_status("a-live", group_ids=[A])
        assert mine["status"] == "RUNNING"
