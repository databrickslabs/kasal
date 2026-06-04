"""
Unit tests for PowerBISemanticModelCacheRepository.

Tests cache retrieval, creation, updating, and cleanup operations.
"""
import pytest
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.powerbi_semantic_model_cache_repository import (
    PowerBISemanticModelCacheRepository,
)
from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_cache(
    id=1,
    group_id="group1",
    dataset_id="ds-1",
    workspace_id="ws-1",
    report_id=None,
    cached_date=None,
    cache_data=None,
):
    obj = MagicMock(spec=PowerBISemanticModelCache)
    obj.id = id
    obj.group_id = group_id
    obj.dataset_id = dataset_id
    obj.workspace_id = workspace_id
    obj.report_id = report_id
    obj.cached_date = cached_date or date.today()
    obj.cache_data = cache_data or {"measures": [], "schema": {}}
    obj.is_valid_for_today.return_value = (obj.cached_date == date.today())
    return obj


def _scalar_result(item):
    mock_scalars = MagicMock()
    mock_scalars.first.return_value = item
    mock_scalars.all.return_value = [item] if item is not None else []
    mock_result = MagicMock()
    mock_result.scalars.return_value = mock_scalars
    return mock_result


def _scalar_list_result(items):
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = items
    mock_scalars.first.return_value = items[0] if items else None
    mock_result = MagicMock()
    mock_result.scalars.return_value = mock_scalars
    return mock_result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.delete = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def repo(mock_session):
    return PowerBISemanticModelCacheRepository(session=mock_session)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestRepositoryInit:
    def test_session_stored(self, mock_session):
        r = PowerBISemanticModelCacheRepository(session=mock_session)
        assert r.session is mock_session


# ---------------------------------------------------------------------------
# get_cache_for_today
# ---------------------------------------------------------------------------

class TestGetCacheForToday:
    @pytest.mark.asyncio
    async def test_returns_cache_when_found(self, repo, mock_session):
        cache = _make_cache()
        mock_session.execute.return_value = _scalar_result(cache)

        result = await repo.get_cache_for_today(
            group_id="group1", dataset_id="ds-1", workspace_id="ws-1"
        )

        assert result is cache
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_result(None)

        result = await repo.get_cache_for_today(
            group_id="group1", dataset_id="ds-1", workspace_id="ws-1"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_with_specific_report_id(self, repo, mock_session):
        cache = _make_cache(report_id="rpt-123")
        mock_session.execute.return_value = _scalar_result(cache)

        result = await repo.get_cache_for_today(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            report_id="rpt-123",
        )

        assert result is cache

    @pytest.mark.asyncio
    async def test_with_any_report_id_flag(self, repo, mock_session):
        # any_report_id=True uses OR clause excluding 'reduced'
        cache = _make_cache(report_id="rpt-456")
        mock_session.execute.return_value = _scalar_result(cache)

        result = await repo.get_cache_for_today(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            any_report_id=True,
        )

        assert result is cache

    @pytest.mark.asyncio
    async def test_without_report_id_uses_null_filter(self, repo, mock_session):
        cache = _make_cache(report_id=None)
        mock_session.execute.return_value = _scalar_result(cache)

        result = await repo.get_cache_for_today(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            report_id=None,
        )

        assert result is cache

    @pytest.mark.asyncio
    async def test_db_error_propagates(self, repo, mock_session):
        mock_session.execute.side_effect = RuntimeError("db down")

        with pytest.raises(RuntimeError, match="db down"):
            await repo.get_cache_for_today(
                group_id="group1", dataset_id="ds-1", workspace_id="ws-1"
            )


# ---------------------------------------------------------------------------
# create_cache
# ---------------------------------------------------------------------------

class TestCreateCache:
    @pytest.mark.asyncio
    async def test_create_adds_and_commits(self, repo, mock_session):
        metadata = {"measures": [{"name": "Total Sales"}], "schema": {}}

        result = await repo.create_cache(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            metadata=metadata,
        )

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_with_report_id(self, repo, mock_session):
        metadata = {"measures": []}

        await repo.create_cache(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            metadata=metadata,
            report_id="rpt-999",
        )

        call_arg = mock_session.add.call_args[0][0]
        assert call_arg.report_id == "rpt-999"

    @pytest.mark.asyncio
    async def test_create_sets_today_as_cached_date(self, repo, mock_session):
        metadata = {}

        await repo.create_cache(
            group_id="group1",
            dataset_id="ds-1",
            workspace_id="ws-1",
            metadata=metadata,
        )

        call_arg = mock_session.add.call_args[0][0]
        assert call_arg.cached_date == date.today()

    @pytest.mark.asyncio
    async def test_create_stores_correct_metadata(self, repo, mock_session):
        metadata = {"measures": ["m1"], "relationships": ["r1"]}

        await repo.create_cache(
            group_id="grp",
            dataset_id="d",
            workspace_id="w",
            metadata=metadata,
        )

        call_arg = mock_session.add.call_args[0][0]
        assert call_arg.cache_data == metadata


# ---------------------------------------------------------------------------
# update_cache
# ---------------------------------------------------------------------------

class TestUpdateCache:
    @pytest.mark.asyncio
    async def test_update_refreshes_and_commits(self, repo, mock_session):
        cache = _make_cache()
        new_meta = {"measures": [{"name": "New Measure"}]}

        result = await repo.update_cache(cache=cache, metadata=new_meta)

        assert cache.cache_data == new_meta
        assert cache.cached_date == date.today()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once_with(cache)
        assert result is cache

    @pytest.mark.asyncio
    async def test_update_overwrites_old_data(self, repo, mock_session):
        cache = _make_cache(cache_data={"measures": ["old"]})
        new_meta = {"measures": ["new"]}

        await repo.update_cache(cache=cache, metadata=new_meta)

        assert cache.cache_data == {"measures": ["new"]}


# ---------------------------------------------------------------------------
# delete_old_caches
# ---------------------------------------------------------------------------

class TestDeleteOldCaches:
    @pytest.mark.asyncio
    async def test_deletes_old_entries_and_returns_count(self, repo, mock_session):
        old_cache1 = _make_cache(id=1)
        old_cache2 = _make_cache(id=2)
        mock_session.execute.return_value = _scalar_list_result([old_cache1, old_cache2])

        count = await repo.delete_old_caches(days_to_keep=7)

        assert count == 2
        assert mock_session.delete.call_count == 2
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_old_caches_returns_zero(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_list_result([])

        count = await repo.delete_old_caches(days_to_keep=7)

        assert count == 0
        mock_session.delete.assert_not_called()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_days_to_keep(self, repo, mock_session):
        old_cache = _make_cache(id=99)
        mock_session.execute.return_value = _scalar_list_result([old_cache])

        count = await repo.delete_old_caches(days_to_keep=30)

        assert count == 1

    @pytest.mark.asyncio
    async def test_default_days_to_keep_is_seven(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_list_result([])

        # No exception means default param accepted
        count = await repo.delete_old_caches()

        assert count == 0
