"""
Unit tests for Conversion Repositories.

Tests the functionality of ConversionHistoryRepository, ConversionJobRepository,
and SavedConverterConfigurationRepository including CRUD operations and custom queries.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.conversion_repository import (
    ConversionHistoryRepository,
    ConversionJobRepository,
    SavedConverterConfigurationRepository,
)
from src.models.conversion import (
    ConversionHistory,
    ConversionJob,
    SavedConverterConfiguration,
)


# Mock Models
class MockConversionHistory:
    def __init__(self, id=1, execution_id="exec-123", source_format="powerbi",
                 target_format="dax", status="success", measure_count=5,
                 execution_time_ms=1500, group_id="group-1", created_by_email="user@example.com"):
        self.id = id
        self.execution_id = execution_id
        self.source_format = source_format
        self.target_format = target_format
        self.status = status
        self.measure_count = measure_count
        self.execution_time_ms = execution_time_ms
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.input_data = {"measures": []}
        self.output_data = {"dax": "MEASURE Sales = SUM(Sales[Amount])"}


class MockConversionJob:
    def __init__(self, id="job-123", source_format="powerbi", target_format="dax",
                 status="pending", progress=0.0, group_id="group-1"):
        self.id = id
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {"option1": "value1"}
        self.status = status
        self.progress = progress
        self.group_id = group_id
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.started_at = None
        self.completed_at = None


class MockSavedConfiguration:
    def __init__(self, id=1, name="My Config", source_format="powerbi",
                 target_format="dax", is_public=False, is_template=False,
                 use_count=0, group_id="group-1", created_by_email="user@example.com"):
        self.id = id
        self.name = name
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {"option1": "value1"}
        self.is_public = is_public
        self.is_template = is_template
        self.use_count = use_count
        self.last_used_at = None
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results

    def first(self):
        return self.results[0] if self.results else None

    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)

    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    return AsyncMock(spec=AsyncSession)


# ===== ConversionHistoryRepository Tests =====

@pytest.fixture
def history_repository(mock_async_session):
    """Create a ConversionHistoryRepository with mock session."""
    return ConversionHistoryRepository(session=mock_async_session)


@pytest.fixture
def sample_history_entries():
    """Create sample history entries for testing."""
    return [
        MockConversionHistory(id=1, status="success", source_format="powerbi", target_format="dax"),
        MockConversionHistory(id=2, status="failed", source_format="yaml", target_format="sql"),
        MockConversionHistory(id=3, status="success", source_format="powerbi", target_format="uc_metrics"),
    ]


class TestConversionHistoryRepository:
    """Test cases for ConversionHistoryRepository."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ConversionHistoryRepository(session=mock_async_session)

        assert repository.session == mock_async_session
        assert repository.model == ConversionHistory

    @pytest.mark.asyncio
    async def test_find_by_execution_id_success(self, history_repository, mock_async_session):
        """Test successful find by execution ID."""
        history_entry = MockConversionHistory(execution_id="exec-123")
        mock_result = MockResult([history_entry])
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_by_execution_id("exec-123")

        assert len(result) == 1
        assert result[0] == history_entry
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_execution_id_not_found(self, history_repository, mock_async_session):
        """Test find by execution ID when not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_by_execution_id("nonexistent")

        assert result == []
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_formats_success(self, history_repository, mock_async_session, sample_history_entries):
        """Test successful find by formats."""
        matching_entries = [e for e in sample_history_entries if e.source_format == "powerbi" and e.target_format == "dax"]
        mock_result = MockResult(matching_entries)
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_by_formats("powerbi", "dax", group_id="group-1", limit=10)

        assert len(result) == 1
        assert result[0].source_format == "powerbi"
        assert result[0].target_format == "dax"
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_successful(self, history_repository, mock_async_session, sample_history_entries):
        """Test find successful conversions."""
        successful_entries = [e for e in sample_history_entries if e.status == "success"]
        mock_result = MockResult(successful_entries)
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_successful(group_id="group-1", limit=10)

        assert len(result) == 2
        assert all(e.status == "success" for e in result)
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_failed(self, history_repository, mock_async_session, sample_history_entries):
        """Test find failed conversions."""
        failed_entries = [e for e in sample_history_entries if e.status == "failed"]
        mock_result = MockResult(failed_entries)
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_failed(group_id="group-1", limit=10)

        assert len(result) == 1
        assert result[0].status == "failed"
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_group(self, history_repository, mock_async_session, sample_history_entries):
        """Test find by group ID."""
        mock_result = MockResult(sample_history_entries)
        mock_async_session.execute.return_value = mock_result

        result = await history_repository.find_by_group(group_id="group-1", limit=10, offset=0)

        assert len(result) == 3
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_statistics(self, history_repository, mock_async_session):
        """Test get statistics."""
        # Create mock rows for popular conversions
        mock_row1 = MagicMock()
        mock_row1.source_format = "powerbi"
        mock_row1.target_format = "dax"
        mock_row1.count = 50

        mock_row2 = MagicMock()
        mock_row2.source_format = "yaml"
        mock_row2.target_format = "sql"
        mock_row2.count = 30

        # Mock count queries
        mock_total_result = MagicMock()
        mock_total_result.scalar.return_value = 100

        mock_success_result = MagicMock()
        mock_success_result.scalar.return_value = 85

        mock_failed_result = MagicMock()
        mock_failed_result.scalar.return_value = 15

        mock_avg_time_result = MagicMock()
        mock_avg_time_result.scalar.return_value = 1500.0

        mock_popular_result = MockResult([mock_row1, mock_row2])

        # Set up session.execute to return different results based on call order
        mock_async_session.execute.side_effect = [
            mock_total_result,
            mock_success_result,
            mock_failed_result,
            mock_avg_time_result,
            mock_popular_result
        ]

        result = await history_repository.get_statistics(group_id="group-1", days=30)

        assert result["total_conversions"] == 100
        assert result["successful"] == 85
        assert result["failed"] == 15
        assert result["success_rate"] == 85.0
        assert result["average_execution_time_ms"] == 1500.0
        assert len(result["popular_conversions"]) == 2
        assert result["period_days"] == 30


# ===== ConversionJobRepository Tests =====

@pytest.fixture
def job_repository(mock_async_session):
    """Create a ConversionJobRepository with mock session."""
    return ConversionJobRepository(session=mock_async_session)


@pytest.fixture
def sample_jobs():
    """Create sample jobs for testing."""
    return [
        MockConversionJob(id="job-1", status="pending"),
        MockConversionJob(id="job-2", status="running", progress=0.5),
        MockConversionJob(id="job-3", status="completed", progress=1.0),
    ]


class TestConversionJobRepository:
    """Test cases for ConversionJobRepository."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ConversionJobRepository(session=mock_async_session)

        assert repository.session == mock_async_session
        assert repository.model == ConversionJob

    @pytest.mark.asyncio
    async def test_find_by_status_success(self, job_repository, mock_async_session, sample_jobs):
        """Test successful find by status."""
        pending_jobs = [j for j in sample_jobs if j.status == "pending"]
        mock_result = MockResult(pending_jobs)
        mock_async_session.execute.return_value = mock_result

        result = await job_repository.find_by_status(status="pending", group_id="group-1", limit=10)

        assert len(result) == 1
        assert result[0].status == "pending"
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_active_jobs(self, job_repository, mock_async_session, sample_jobs):
        """Test find active jobs (pending or running)."""
        active_jobs = [j for j in sample_jobs if j.status in ["pending", "running"]]
        mock_result = MockResult(active_jobs)
        mock_async_session.execute.return_value = mock_result

        result = await job_repository.find_active_jobs(group_id="group-1")

        assert len(result) == 2
        assert all(j.status in ["pending", "running"] for j in result)
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_status_success(self, job_repository, mock_async_session):
        """Test successful status update."""
        job = MockConversionJob(id="job-123", status="pending")
        updated_job = MockConversionJob(id="job-123", status="running", progress=0.3)

        # Mock the update result with rowcount
        mock_update_result = MagicMock()
        mock_update_result.rowcount = 1

        mock_result_get = MockResult([job])
        mock_result_updated = MockResult([updated_job])

        mock_async_session.execute.side_effect = [
            mock_result_get,      # First call to get the job
            mock_update_result,   # Update query execution with rowcount
            mock_result_updated   # Get updated job
        ]
        mock_async_session.flush = AsyncMock()

        result = await job_repository.update_status("job-123", status="running", progress=0.3)

        assert result is not None
        assert result.id == "job-123"

    @pytest.mark.asyncio
    async def test_update_status_not_found(self, job_repository, mock_async_session):
        """Test status update when job not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await job_repository.update_status("nonexistent", status="running")

        assert result is None

    @pytest.mark.asyncio
    async def test_cancel_job_success(self, job_repository, mock_async_session):
        """Test successful job cancellation."""
        job = MockConversionJob(id="job-123", status="pending")
        cancelled_job = MockConversionJob(id="job-123", status="cancelled")

        # Mock the update result with rowcount
        mock_update_result = MagicMock()
        mock_update_result.rowcount = 1

        mock_result_get = MockResult([job])
        mock_result_updated = MockResult([cancelled_job])

        mock_async_session.execute.side_effect = [
            mock_result_get,      # Get job
            mock_update_result,   # Update query with rowcount
            mock_result_updated   # Get updated job
        ]
        mock_async_session.flush = AsyncMock()

        result = await job_repository.cancel_job("job-123")

        assert result is not None
        assert result.id == "job-123"

    @pytest.mark.asyncio
    async def test_cancel_job_not_cancellable(self, job_repository, mock_async_session):
        """Test cancellation of completed job fails."""
        completed_job = MockConversionJob(id="job-123", status="completed")

        # Mock the update result with rowcount = 0 (no rows updated)
        mock_update_result = MagicMock()
        mock_update_result.rowcount = 0

        mock_result_get = MockResult([completed_job])

        mock_async_session.execute.side_effect = [
            mock_result_get,      # Get job
            mock_update_result    # Update query returns 0 rows
        ]

        result = await job_repository.cancel_job("job-123")

        assert result is None


# ===== SavedConverterConfigurationRepository Tests =====

@pytest.fixture
def config_repository(mock_async_session):
    """Create a SavedConverterConfigurationRepository with mock session."""
    return SavedConverterConfigurationRepository(session=mock_async_session)


@pytest.fixture
def sample_configurations():
    """Create sample configurations for testing."""
    return [
        MockSavedConfiguration(id=1, name="PowerBI to DAX", is_public=True, use_count=10),
        MockSavedConfiguration(id=2, name="YAML to SQL", is_public=False, use_count=5),
        MockSavedConfiguration(id=3, name="Template Config", is_public=True, use_count=20),
    ]


class TestSavedConverterConfigurationRepository:
    """Test cases for SavedConverterConfigurationRepository."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = SavedConverterConfigurationRepository(session=mock_async_session)

        assert repository.session == mock_async_session
        assert repository.model == SavedConverterConfiguration

    @pytest.mark.asyncio
    async def test_find_by_user_success(self, config_repository, mock_async_session, sample_configurations):
        """Test successful find by user."""
        user_configs = [sample_configurations[1]]  # Second config belongs to user
        mock_result = MockResult(user_configs)
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.find_by_user(
            created_by_email="user@example.com",
            group_id="group-1"
        )

        assert len(result) == 1
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_public_success(self, config_repository, mock_async_session, sample_configurations):
        """Test successful find public configurations."""
        public_configs = [c for c in sample_configurations if c.is_public]
        mock_result = MockResult(public_configs)
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.find_public(group_id="group-1")

        assert len(result) == 2
        assert all(c.is_public for c in result)
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_templates_success(self, config_repository, mock_async_session):
        """Test successful find templates."""
        template_config = MockSavedConfiguration(id=1, name="Template", is_template=True)
        mock_result = MockResult([template_config])
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.find_templates()

        assert len(result) == 1
        assert result[0].is_template
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_formats_success(self, config_repository, mock_async_session):
        """Test successful find by formats."""
        config = MockSavedConfiguration(source_format="powerbi", target_format="dax")
        mock_result = MockResult([config])
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.find_by_formats(
            source_format="powerbi",
            target_format="dax",
            group_id="group-1",
            user_email="user@example.com"
        )

        assert len(result) == 1
        assert result[0].source_format == "powerbi"
        assert result[0].target_format == "dax"
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_by_name_success(self, config_repository, mock_async_session):
        """Test successful search by name."""
        config = MockSavedConfiguration(name="PowerBI Config")
        mock_result = MockResult([config])
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.search_by_name(
            search_term="PowerBI",
            group_id="group-1",
            user_email="user@example.com"
        )

        assert len(result) == 1
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_increment_use_count_success(self, config_repository, mock_async_session):
        """Test successful use count increment."""
        config = MockSavedConfiguration(id=1, use_count=5)
        updated_config = MockSavedConfiguration(id=1, use_count=6)
        updated_config.last_used_at = datetime.utcnow()

        # Mock the update result with rowcount
        mock_update_result = MagicMock()
        mock_update_result.rowcount = 1

        mock_result_get = MockResult([config])
        mock_result_updated = MockResult([updated_config])

        mock_async_session.execute.side_effect = [
            mock_result_get,      # Get config
            mock_update_result,   # Update query with rowcount
            mock_result_updated   # Get updated config
        ]
        mock_async_session.flush = AsyncMock()

        result = await config_repository.increment_use_count(1)

        assert result is not None
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_increment_use_count_not_found(self, config_repository, mock_async_session):
        """Test use count increment when config not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await config_repository.increment_use_count(999)

        assert result is None
