"""MLflowRepository — the settings live in their OWN table now.

They used to be columns on ``databricksconfig``, which the previous version of
this file mocked (``repo.dbx_repo`` / ``repo._base_repo``). That coupling was not
merely untidy: ``is_enabled`` read the flag off the Databricks row and returned
False whenever no such row existed, so a workspace with no Databricks
configuration could never turn MLflow on — by construction, not by choice.

These run against a REAL in-memory SQLite session rather than mocked
collaborators. The old suite asserted which methods were called on a mock, which
is precisely the shape of test that keeps passing while the storage underneath is
wrong; what matters is what a later read sees.
"""

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models.mlflow_config import MLflowConfig
from src.repositories.mlflow_repository import MLflowRepository


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MLflowConfig.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def repo(session):
    return MLflowRepository(session)


async def _row_count(session) -> int:
    result = await session.execute(select(func.count()).select_from(MLflowConfig))
    return result.scalar()


class TestDefaults:
    """A workspace that has never touched MLflow."""

    @pytest.mark.asyncio
    async def test_disabled_by_default(self, repo):
        assert await repo.is_enabled(group_id="g1") is False

    @pytest.mark.asyncio
    async def test_evaluation_disabled_by_default(self, repo):
        assert await repo.is_evaluation_enabled(group_id="g1") is False

    @pytest.mark.asyncio
    async def test_reading_does_not_create_a_row(self, repo, session):
        """Otherwise every status poll seeds a row for a feature nobody enabled."""
        await repo.is_enabled(group_id="g1")
        await repo.is_evaluation_enabled(group_id="g1")
        assert await _row_count(session) == 0


class TestEnableWithoutDatabricks:
    """The bug this move exists to fix."""

    @pytest.mark.asyncio
    async def test_can_be_enabled_with_no_databricks_config_anywhere(self, repo):
        # There is no databricksconfig table in this database at all.
        assert await repo.set_enabled(True, group_id="g1") is True
        assert await repo.is_enabled(group_id="g1") is True


class TestToggles:
    @pytest.mark.asyncio
    async def test_enable_then_disable(self, repo):
        await repo.set_enabled(True, group_id="g1")
        await repo.set_enabled(False, group_id="g1")
        assert await repo.is_enabled(group_id="g1") is False

    @pytest.mark.asyncio
    async def test_evaluation_is_independent_of_tracing(self, repo):
        """A more expensive opt-in, hence its own flag rather than a mode."""
        await repo.set_enabled(True, group_id="g1")
        assert await repo.is_evaluation_enabled(group_id="g1") is False

        await repo.set_evaluation_enabled(True, group_id="g1")
        assert await repo.is_enabled(group_id="g1") is True
        assert await repo.is_evaluation_enabled(group_id="g1") is True

    @pytest.mark.asyncio
    async def test_repeated_writes_reuse_one_row(self, repo, session):
        for value in (True, False, True):
            await repo.set_enabled(value, group_id="g1")
        assert await _row_count(session) == 1


class TestGroupIsolation:
    @pytest.mark.asyncio
    async def test_groups_do_not_see_each_other(self, repo):
        await repo.set_enabled(True, group_id="g1")
        assert await repo.is_enabled(group_id="g2") is False

    @pytest.mark.asyncio
    async def test_each_group_gets_its_own_row(self, repo, session):
        await repo.set_enabled(True, group_id="g1")
        await repo.set_enabled(True, group_id="g2")
        assert await _row_count(session) == 2


class TestExperimentName:
    @pytest.mark.asyncio
    async def test_a_new_row_stores_no_name_so_the_backend_derives_one(self, repo):
        """No column default on purpose.

        A stored default is indistinguishable from a name the user typed, so it
        silently outranks the derived ``kasal-<teamspace>-traces`` — which is
        exactly what an earlier ``default="kasal-crew-execution-traces"`` did.
        NULL is the only value the resolver can safely override."""
        await repo.set_enabled(True, group_id="g1")
        assert await repo.get_experiment_name(group_id="g1") is None

    @pytest.mark.asyncio
    async def test_round_trips(self, repo):
        await repo.set_experiment_name("my-traces", group_id="g1")
        assert await repo.get_experiment_name(group_id="g1") == "my-traces"

    @pytest.mark.asyncio
    async def test_blank_is_stored_as_unset(self, repo):
        await repo.set_experiment_name("   ", group_id="g1")
        assert await repo.get_experiment_name(group_id="g1") is None


class TestJudgeModel:
    @pytest.mark.asyncio
    async def test_none_when_no_row(self, repo):
        assert await repo.get_evaluation_judge_model(group_id="g1") is None

    @pytest.mark.asyncio
    async def test_none_when_blank(self, repo, session):
        session.add(MLflowConfig(group_id="g1", evaluation_judge_model="   "))
        await session.commit()
        assert await repo.get_evaluation_judge_model(group_id="g1") is None

    @pytest.mark.asyncio
    async def test_returned_when_set(self, repo, session):
        session.add(
            MLflowConfig(group_id="g1", evaluation_judge_model="databricks:/judge")
        )
        await session.commit()
        assert (
            await repo.get_evaluation_judge_model(group_id="g1") == "databricks:/judge"
        )
