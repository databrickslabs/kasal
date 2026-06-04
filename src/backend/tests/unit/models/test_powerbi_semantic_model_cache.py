"""
Unit tests for PowerBISemanticModelCache SQLAlchemy model.

Tests:
- Model instantiation with required and optional fields
- Column definitions, types, and constraints
- is_valid_for_today() returns True for today and False for past/future dates
- __tablename__, unique constraints, and indexes are correctly defined
"""
import pytest
from datetime import date, datetime, timezone, timedelta

from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cache(**overrides) -> PowerBISemanticModelCache:
    defaults = dict(
        group_id="grp-001",
        dataset_id="ds-abc",
        workspace_id="ws-xyz",
        cached_date=date.today(),
        cache_data={
            "measures": [{"name": "Revenue", "expr": "SUM(Sales[Revenue])"}],
            "relationships": [],
            "schema": {"tables": [], "columns": []},
            "sample_data": {},
            "default_filters": {},
        },
    )
    defaults.update(overrides)
    return PowerBISemanticModelCache(**defaults)


# ---------------------------------------------------------------------------
# Tests: instantiation
# ---------------------------------------------------------------------------

class TestPowerBISemanticModelCacheInstantiation:

    def test_basic_instantiation(self):
        """Model can be created with required fields."""
        cache = _make_cache()
        assert cache.group_id == "grp-001"
        assert cache.dataset_id == "ds-abc"
        assert cache.workspace_id == "ws-xyz"

    def test_report_id_defaults_to_none(self):
        """report_id is optional and defaults to None."""
        cache = _make_cache()
        assert cache.report_id is None

    def test_report_id_can_be_set(self):
        """report_id can be supplied explicitly."""
        cache = _make_cache(report_id="rpt-999")
        assert cache.report_id == "rpt-999"

    def test_cached_date_stored_correctly(self):
        """cached_date stores the provided date object."""
        today = date.today()
        cache = _make_cache(cached_date=today)
        assert cache.cached_date == today

    def test_cache_data_stored_correctly(self):
        """cache_data stores the provided JSON-compatible dict."""
        data = {"measures": [{"name": "Profit"}], "relationships": []}
        cache = _make_cache(cache_data=data)
        assert cache.cache_data["measures"][0]["name"] == "Profit"

    def test_empty_cache_data(self):
        """cache_data can be an empty dict."""
        cache = _make_cache(cache_data={})
        assert cache.cache_data == {}

    def test_timestamps_can_be_set(self):
        """created_at and updated_at can be set explicitly."""
        now = datetime.now(timezone.utc)
        cache = _make_cache(created_at=now, updated_at=now)
        assert cache.created_at == now
        assert cache.updated_at == now

    def test_id_not_set_before_db_flush(self):
        """id is None before the object is persisted."""
        cache = _make_cache()
        assert cache.id is None

    def test_multiple_independent_instances(self):
        """Creating two instances does not share state."""
        c1 = _make_cache(dataset_id="ds-1")
        c2 = _make_cache(dataset_id="ds-2")
        assert c1.dataset_id != c2.dataset_id


# ---------------------------------------------------------------------------
# Tests: is_valid_for_today()
# ---------------------------------------------------------------------------

class TestIsValidForToday:

    def test_today_is_valid(self):
        """Cache dated today is valid."""
        cache = _make_cache(cached_date=date.today())
        assert cache.is_valid_for_today() is True

    def test_yesterday_is_not_valid(self):
        """Cache dated yesterday is no longer valid."""
        yesterday = date.today() - timedelta(days=1)
        cache = _make_cache(cached_date=yesterday)
        assert cache.is_valid_for_today() is False

    def test_two_days_ago_is_not_valid(self):
        """Cache dated two days ago is not valid."""
        two_days_ago = date.today() - timedelta(days=2)
        cache = _make_cache(cached_date=two_days_ago)
        assert cache.is_valid_for_today() is False

    def test_tomorrow_is_not_valid(self):
        """Cache dated tomorrow is not valid for today."""
        tomorrow = date.today() + timedelta(days=1)
        cache = _make_cache(cached_date=tomorrow)
        assert cache.is_valid_for_today() is False

    def test_one_week_ago_is_not_valid(self):
        """Cache dated one week ago is not valid."""
        one_week_ago = date.today() - timedelta(days=7)
        cache = _make_cache(cached_date=one_week_ago)
        assert cache.is_valid_for_today() is False

    def test_returns_bool(self):
        """is_valid_for_today() always returns a bool."""
        cache = _make_cache(cached_date=date.today())
        result = cache.is_valid_for_today()
        assert isinstance(result, bool)

    def test_consistent_results_called_twice(self):
        """Calling is_valid_for_today() twice gives the same result."""
        cache = _make_cache(cached_date=date.today())
        assert cache.is_valid_for_today() == cache.is_valid_for_today()


# ---------------------------------------------------------------------------
# Tests: table name
# ---------------------------------------------------------------------------

class TestTableName:

    def test_tablename(self):
        """__tablename__ is correctly set."""
        assert PowerBISemanticModelCache.__tablename__ == "powerbi_semantic_model_cache"


# ---------------------------------------------------------------------------
# Tests: column definitions
# ---------------------------------------------------------------------------

class TestColumnDefinitions:

    def test_primary_key_column(self):
        """id is the integer primary key."""
        id_col = PowerBISemanticModelCache.__table__.columns["id"]
        assert id_col.primary_key is True
        assert "INTEGER" in str(id_col.type).upper()

    def test_group_id_column(self):
        """group_id is a non-nullable indexed string."""
        col = PowerBISemanticModelCache.__table__.columns["group_id"]
        assert col.nullable is False
        assert col.index is True

    def test_dataset_id_column(self):
        """dataset_id is a non-nullable string column."""
        col = PowerBISemanticModelCache.__table__.columns["dataset_id"]
        assert col.nullable is False

    def test_workspace_id_column(self):
        """workspace_id is a non-nullable string column."""
        col = PowerBISemanticModelCache.__table__.columns["workspace_id"]
        assert col.nullable is False

    def test_report_id_column_is_nullable(self):
        """report_id is nullable (optional report scoping)."""
        col = PowerBISemanticModelCache.__table__.columns["report_id"]
        assert col.nullable is True

    def test_cached_date_column(self):
        """cached_date is a non-nullable Date column."""
        col = PowerBISemanticModelCache.__table__.columns["cached_date"]
        assert col.nullable is False
        assert "DATE" in str(col.type).upper()

    def test_cache_data_column(self):
        """cache_data is a non-nullable JSON column."""
        col = PowerBISemanticModelCache.__table__.columns["cache_data"]
        assert col.nullable is False
        assert "JSON" in str(col.type).upper()

    def test_created_at_column(self):
        """created_at is a DateTime column with a default."""
        col = PowerBISemanticModelCache.__table__.columns["created_at"]
        assert col.default is not None
        assert "DATETIME" in str(col.type).upper()

    def test_updated_at_column_has_onupdate(self):
        """updated_at has both a default and an onupdate hook."""
        col = PowerBISemanticModelCache.__table__.columns["updated_at"]
        assert col.default is not None
        assert col.onupdate is not None

    def test_all_expected_columns_present(self):
        """All expected columns exist in the table definition."""
        expected = {
            "id", "group_id", "dataset_id", "workspace_id",
            "report_id", "cached_date", "cache_data",
            "created_at", "updated_at",
        }
        actual = set(PowerBISemanticModelCache.__table__.columns.keys())
        assert expected.issubset(actual)


# ---------------------------------------------------------------------------
# Tests: unique constraints and indexes
# ---------------------------------------------------------------------------

class TestConstraintsAndIndexes:

    def _constraint_names(self):
        return {c.name for c in PowerBISemanticModelCache.__table__.constraints}

    def _index_names(self):
        return {i.name for i in PowerBISemanticModelCache.__table__.indexes}

    def test_daily_unique_constraint_exists(self):
        """The unique constraint 'uq_semantic_model_cache_daily' is defined."""
        assert "uq_semantic_model_cache_daily" in self._constraint_names()

    def test_unique_constraint_covers_correct_columns(self):
        """The daily unique constraint covers group_id, dataset_id, cached_date, report_id."""
        uc = next(
            c for c in PowerBISemanticModelCache.__table__.constraints
            if getattr(c, "name", None) == "uq_semantic_model_cache_daily"
        )
        col_names = {col.name for col in uc.columns}
        assert {"group_id", "dataset_id", "cached_date", "report_id"}.issubset(col_names)

    def test_group_dataset_index_exists(self):
        """The composite index 'idx_semantic_cache_group_dataset' is defined."""
        assert "idx_semantic_cache_group_dataset" in self._index_names()

    def test_date_index_exists(self):
        """The index 'idx_semantic_cache_date' is defined."""
        assert "idx_semantic_cache_date" in self._index_names()
