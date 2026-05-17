"""
Unit tests for PowerBISemanticModelCacheService.

Tests cache retrieval, save (create/update), cleanup, and metadata
dictionary building logic.
"""
import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService
from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cache(valid_today=True, cache_data=None):
    obj = MagicMock(spec=PowerBISemanticModelCache)
    obj.cache_data = cache_data or {"measures": [], "schema": {}}
    obj.cached_date = date.today() if valid_today else date(2000, 1, 1)
    obj.is_valid_for_today.return_value = valid_today
    return obj


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def mock_repo():
    return AsyncMock()


@pytest.fixture
def service(mock_session, mock_repo):
    with patch(
        "src.services.powerbi_semantic_model_cache_service.PowerBISemanticModelCacheRepository",
        return_value=mock_repo,
    ):
        svc = PowerBISemanticModelCacheService(session=mock_session)
        svc.repository = mock_repo
        return svc


# ---------------------------------------------------------------------------
# get_cached_metadata
# ---------------------------------------------------------------------------

class TestGetCachedMetadata:
    @pytest.mark.asyncio
    async def test_returns_cache_data_when_valid(self, service, mock_repo):
        cache = _make_cache(valid_today=True, cache_data={"measures": ["m1"]})
        mock_repo.get_cache_for_today.return_value = cache

        result = await service.get_cached_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
        )

        assert result == {"measures": ["m1"]}

    @pytest.mark.asyncio
    async def test_returns_none_when_cache_missing(self, service, mock_repo):
        mock_repo.get_cache_for_today.return_value = None

        result = await service.get_cached_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_cache_expired(self, service, mock_repo):
        # is_valid_for_today returns False → stale cache
        cache = _make_cache(valid_today=False)
        mock_repo.get_cache_for_today.return_value = cache

        result = await service.get_cached_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_passes_report_id_to_repo(self, service, mock_repo):
        mock_repo.get_cache_for_today.return_value = None

        await service.get_cached_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            report_id="rpt-1",
        )

        mock_repo.get_cache_for_today.assert_called_once_with(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            report_id="rpt-1",
            any_report_id=False,
        )

    @pytest.mark.asyncio
    async def test_passes_any_report_id_flag(self, service, mock_repo):
        mock_repo.get_cache_for_today.return_value = None

        await service.get_cached_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            any_report_id=True,
        )

        call_kwargs = mock_repo.get_cache_for_today.call_args.kwargs
        assert call_kwargs["any_report_id"] is True


# ---------------------------------------------------------------------------
# save_metadata
# ---------------------------------------------------------------------------

class TestSaveMetadata:
    @pytest.mark.asyncio
    async def test_creates_new_when_no_existing_cache(self, service, mock_repo):
        mock_repo.get_cache_for_today.return_value = None
        new_cache = _make_cache()
        mock_repo.create_cache.return_value = new_cache

        metadata = {"measures": ["m1"], "schema": {}}
        result = await service.save_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            metadata=metadata,
        )

        mock_repo.create_cache.assert_called_once()
        mock_repo.update_cache.assert_not_called()
        assert result is new_cache

    @pytest.mark.asyncio
    async def test_updates_when_cache_already_exists(self, service, mock_repo):
        existing = _make_cache()
        mock_repo.get_cache_for_today.return_value = existing
        updated = _make_cache()
        mock_repo.update_cache.return_value = updated

        metadata = {"measures": ["new"]}
        result = await service.save_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            metadata=metadata,
        )

        mock_repo.update_cache.assert_called_once_with(existing, metadata)
        mock_repo.create_cache.assert_not_called()
        assert result is updated

    @pytest.mark.asyncio
    async def test_passes_report_id_to_create(self, service, mock_repo):
        mock_repo.get_cache_for_today.return_value = None
        mock_repo.create_cache.return_value = _make_cache()

        await service.save_metadata(
            group_id="grp",
            dataset_id="ds",
            workspace_id="ws",
            metadata={},
            report_id="rpt-5",
        )

        call_kwargs = mock_repo.create_cache.call_args.kwargs
        assert call_kwargs.get("report_id") == "rpt-5"


# ---------------------------------------------------------------------------
# cleanup_old_caches
# ---------------------------------------------------------------------------

class TestCleanupOldCaches:
    @pytest.mark.asyncio
    async def test_delegates_to_repository(self, service, mock_repo):
        mock_repo.delete_old_caches.return_value = 5

        result = await service.cleanup_old_caches(days_to_keep=7)

        assert result == 5
        mock_repo.delete_old_caches.assert_called_once_with(7)

    @pytest.mark.asyncio
    async def test_default_days_to_keep(self, service, mock_repo):
        mock_repo.delete_old_caches.return_value = 0

        await service.cleanup_old_caches()

        mock_repo.delete_old_caches.assert_called_once_with(7)

    @pytest.mark.asyncio
    async def test_returns_zero_when_nothing_deleted(self, service, mock_repo):
        mock_repo.delete_old_caches.return_value = 0

        result = await service.cleanup_old_caches()

        assert result == 0


# ---------------------------------------------------------------------------
# build_metadata_dict  (synchronous helper method)
# ---------------------------------------------------------------------------

class TestBuildMetadataDict:
    def test_basic_fields_included(self, service):
        result = service.build_metadata_dict(
            measures=["m1"],
            relationships=["r1"],
            schema={"tables": []},
            sample_data={"t": ["v"]},
        )

        assert result["measures"] == ["m1"]
        assert result["relationships"] == ["r1"]
        assert result["schema"] == {"tables": []}
        assert result["sample_data"] == {"t": ["v"]}

    def test_default_filters_included_when_provided(self, service):
        result = service.build_metadata_dict(
            measures=[],
            relationships=[],
            schema={},
            sample_data={},
            default_filters={"Region": "East"},
        )

        assert result["default_filters"] == {"Region": "East"}

    def test_default_filters_absent_when_not_provided(self, service):
        result = service.build_metadata_dict(
            measures=[],
            relationships=[],
            schema={},
            sample_data={},
        )

        assert "default_filters" not in result

    def test_slicers_included_when_provided(self, service):
        result = service.build_metadata_dict(
            measures=[],
            relationships=[],
            schema={},
            sample_data={},
            slicers=[{"name": "DateSlicer"}],
        )

        assert result["slicers"] == [{"name": "DateSlicer"}]

    def test_slicers_absent_when_none(self, service):
        result = service.build_metadata_dict(
            measures=[],
            relationships=[],
            schema={},
            sample_data={},
            slicers=None,
        )

        assert "slicers" not in result

    def test_all_optional_fields_included(self, service):
        result = service.build_metadata_dict(
            measures=["m"],
            relationships=["r"],
            schema={"tables": []},
            sample_data={"t": []},
            default_filters={"k": "v"},
            slicers=[{"s": 1}],
        )

        keys = set(result.keys())
        assert keys == {"measures", "relationships", "schema", "sample_data", "default_filters", "slicers"}

    def test_empty_collections_preserved(self, service):
        result = service.build_metadata_dict(
            measures=[],
            relationships=[],
            schema={},
            sample_data={},
        )

        assert result["measures"] == []
        assert result["relationships"] == []
