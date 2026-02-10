"""
Unit tests for CrewRepository.

Tests all methods of the crew repository including
CRUD operations, group isolation, flush behavior, and edge cases.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.crew_repository import CrewRepository
from src.models.crew import Crew


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Scalars:
    """Minimal mock for ``Result.scalars()``."""
    def __init__(self, items):
        self._items = items

    def first(self):
        return self._items[0] if self._items else None

    def all(self):
        return self._items


class _Result:
    """Minimal mock for ``session.execute()`` return value."""
    def __init__(self, items):
        self._scalars = _Scalars(items)

    def scalars(self):
        return self._scalars


def _make_crew(**overrides):
    defaults = dict(
        id=uuid4(),
        name="Test Crew",
        group_id="group-1",
    )
    defaults.update(overrides)
    obj = MagicMock(spec=Crew)
    for k, v in defaults.items():
        setattr(obj, k, v)
    return obj


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def session():
    s = AsyncMock(spec=AsyncSession)
    s.execute = AsyncMock()
    s.delete = AsyncMock()
    s.flush = AsyncMock()
    return s


@pytest.fixture
def repo(session):
    return CrewRepository(session=session)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_sets_model_and_session(self, session):
        repo = CrewRepository(session=session)
        assert repo.model is Crew
        assert repo.session is session


# ---------------------------------------------------------------------------
# find_by_name
# ---------------------------------------------------------------------------

class TestFindByName:
    @pytest.mark.asyncio
    async def test_returns_crew_when_found(self, repo, session):
        crew = _make_crew(name="alpha")
        session.execute.return_value = _Result([crew])

        result = await repo.find_by_name("alpha")

        assert result is crew
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.find_by_name("missing")

        assert result is None


# ---------------------------------------------------------------------------
# find_all
# ---------------------------------------------------------------------------

class TestFindAll:
    @pytest.mark.asyncio
    async def test_returns_all_crews(self, repo, session):
        crews = [_make_crew(), _make_crew()]
        session.execute.return_value = _Result(crews)

        result = await repo.find_all()

        assert result == crews
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_none(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.find_all()

        assert result == []


# ---------------------------------------------------------------------------
# find_by_group
# ---------------------------------------------------------------------------

class TestFindByGroup:
    @pytest.mark.asyncio
    async def test_returns_crews_for_groups(self, repo, session):
        crews = [_make_crew(group_id="g1"), _make_crew(group_id="g2")]
        session.execute.return_value = _Result(crews)

        result = await repo.find_by_group(["g1", "g2"])

        assert len(result) == 2
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_group_list(self, repo, session):
        result = await repo.find_by_group([])

        assert result == []
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_match(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.find_by_group(["unknown"])

        assert result == []


# ---------------------------------------------------------------------------
# get_by_group
# ---------------------------------------------------------------------------

class TestGetByGroup:
    @pytest.mark.asyncio
    async def test_returns_crew_when_found(self, repo, session):
        cid = uuid4()
        crew = _make_crew(id=cid, group_id="g1")
        session.execute.return_value = _Result([crew])

        result = await repo.get_by_group(cid, ["g1"])

        assert result is crew

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.get_by_group(uuid4(), ["g1"])

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_group_list(self, repo, session):
        result = await repo.get_by_group(uuid4(), [])

        assert result is None
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_supports_multiple_groups(self, repo, session):
        cid = uuid4()
        crew = _make_crew(id=cid, group_id="g2")
        session.execute.return_value = _Result([crew])

        result = await repo.get_by_group(cid, ["g1", "g2"])

        assert result is crew


# ---------------------------------------------------------------------------
# delete_by_group
# ---------------------------------------------------------------------------

class TestDeleteByGroup:
    @pytest.mark.asyncio
    async def test_deletes_and_flushes(self, repo, session):
        cid = uuid4()
        crew = _make_crew(id=cid, group_id="g1")
        session.execute.return_value = _Result([crew])

        result = await repo.delete_by_group(cid, ["g1"])

        assert result is True
        session.delete.assert_awaited_once_with(crew)
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.delete_by_group(uuid4(), ["g1"])

        assert result is False
        session.delete.assert_not_awaited()
        session.flush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_false_for_empty_group_list(self, repo, session):
        result = await repo.delete_by_group(uuid4(), [])

        assert result is False
        session.execute.assert_not_awaited()
        session.delete.assert_not_awaited()
        session.flush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_false_for_wrong_group(self, repo, session):
        session.execute.return_value = _Result([])

        result = await repo.delete_by_group(uuid4(), ["wrong"])

        assert result is False
        session.delete.assert_not_awaited()


# ---------------------------------------------------------------------------
# delete_all_by_group
# ---------------------------------------------------------------------------

class TestDeleteAllByGroup:
    @pytest.mark.asyncio
    async def test_executes_delete_and_flushes(self, repo, session):
        await repo.delete_all_by_group(["g1"])

        session.execute.assert_awaited_once()
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handles_multiple_groups(self, repo, session):
        await repo.delete_all_by_group(["g1", "g2", "g3"])

        session.execute.assert_awaited_once()
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_for_empty_group_list(self, repo, session):
        await repo.delete_all_by_group([])

        session.execute.assert_not_awaited()
        session.flush.assert_not_awaited()


# ---------------------------------------------------------------------------
# delete_all
# ---------------------------------------------------------------------------

class TestDeleteAll:
    @pytest.mark.asyncio
    async def test_executes_delete(self, repo, session):
        await repo.delete_all()

        session.execute.assert_awaited_once()
