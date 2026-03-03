"""
Unit tests for ConverterService.

Tests the business logic for converter operations including
history tracking, job management, and saved configurations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from fastapi import HTTPException

from src.services.converter_service import ConverterService
from src.schemas.conversion import (
    ConversionHistoryCreate,
    ConversionHistoryUpdate,
    ConversionHistoryFilter,
    ConversionJobCreate,
    ConversionJobUpdate,
    ConversionJobStatusUpdate,
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    SavedConfigurationFilter,
)
from src.utils.user_context import GroupContext


# Mock models for testing
class MockConversionHistory:
    def __init__(self, id=1, source_format="powerbi", target_format="dax",
                 status="success", group_id="group-1", created_by_email="user@example.com"):
        self.id = id
        self.source_format = source_format
        self.target_format = target_format
        self.status = status
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.execution_id = None
        self.measure_count = 5
        self.execution_time_ms = 1500


class MockConversionJob:
    def __init__(self, id="job-123", status="pending", source_format="powerbi",
                 target_format="dax", group_id="group-1"):
        self.id = id
        self.status = status
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {"option1": "value1"}
        self.group_id = group_id
        self.progress = 0.0
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class MockSavedConfiguration:
    def __init__(self, id=1, name="Config", source_format="powerbi",
                 target_format="dax", created_by_email="user@example.com",
                 is_public=False, is_template=False, use_count=0):
        self.id = id
        self.name = name
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {"option1": "value1"}
        self.description = None
        self.created_by_email = created_by_email
        self.is_public = is_public
        self.is_template = is_template
        self.tags = None
        self.use_count = use_count
        self.last_used_at = None
        self.group_id = "group-1"
        self.extra_metadata = None
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return AsyncMock()


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock(spec=GroupContext)
    context.primary_group_id = "group-1"
    context.user_email = "user@example.com"
    return context


@pytest.fixture
def converter_service(mock_session, mock_group_context):
    """Create a ConverterService with mocked dependencies."""
    service = ConverterService(mock_session, group_context=mock_group_context)

    # Mock repositories
    service.history_repo = AsyncMock()
    service.job_repo = AsyncMock()
    service.config_repo = AsyncMock()

    return service


# ===== ConversionHistory Service Tests =====

class TestConverterServiceHistory:
    """Test cases for conversion history operations."""

    @pytest.mark.asyncio
    async def test_create_history_success(self, converter_service):
        """Test successful history creation."""
        history_data = ConversionHistoryCreate(
            source_format="powerbi",
            target_format="dax",
            status="success"
        )

        mock_history = MockConversionHistory()
        converter_service.history_repo.create.return_value = mock_history

        result = await converter_service.create_history(history_data)

        assert result.id == 1
        assert result.source_format == "powerbi"
        converter_service.history_repo.create.assert_called_once()

        # Verify group context was added
        call_args = converter_service.history_repo.create.call_args[0][0]
        assert call_args["group_id"] == "group-1"
        assert call_args["created_by_email"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_create_history_without_group_context(self, mock_session):
        """Test history creation without group context."""
        service = ConverterService(mock_session, group_context=None)
        service.history_repo = AsyncMock()

        history_data = ConversionHistoryCreate(
            source_format="powerbi",
            target_format="dax"
        )

        mock_history = MockConversionHistory()
        service.history_repo.create.return_value = mock_history

        result = await service.create_history(history_data)

        # Should work without group context
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_get_history_success(self, converter_service):
        """Test successful history retrieval."""
        mock_history = MockConversionHistory(id=123)
        converter_service.history_repo.get.return_value = mock_history

        result = await converter_service.get_history(123)

        assert result.id == 123
        converter_service.history_repo.get.assert_called_once_with(123)

    @pytest.mark.asyncio
    async def test_get_history_not_found(self, converter_service):
        """Test history retrieval when not found."""
        converter_service.history_repo.get.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.get_history(999)

        assert exc_info.value.status_code == 404
        assert "not found" in str(exc_info.value.detail).lower()

    @pytest.mark.asyncio
    async def test_update_history_success(self, converter_service):
        """Test successful history update."""
        existing_history = MockConversionHistory(id=123)
        updated_history = MockConversionHistory(id=123, status="failed")

        converter_service.history_repo.get.return_value = existing_history
        converter_service.history_repo.update.return_value = updated_history

        update_data = ConversionHistoryUpdate(status="failed")
        result = await converter_service.update_history(123, update_data)

        assert result.id == 123
        assert result.status == "failed"

    @pytest.mark.asyncio
    async def test_update_history_not_found(self, converter_service):
        """Test history update when not found."""
        converter_service.history_repo.get.return_value = None

        update_data = ConversionHistoryUpdate(status="failed")

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.update_history(999, update_data)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_list_history_with_execution_id(self, converter_service):
        """Test list history filtered by execution ID."""
        mock_histories = [MockConversionHistory(id=1), MockConversionHistory(id=2)]
        converter_service.history_repo.find_by_execution_id.return_value = mock_histories

        filter_params = ConversionHistoryFilter(execution_id="exec-123")
        result = await converter_service.list_history(filter_params)

        assert result.count == 2
        assert len(result.history) == 2
        converter_service.history_repo.find_by_execution_id.assert_called_once_with("exec-123")

    @pytest.mark.asyncio
    async def test_list_history_by_formats(self, converter_service):
        """Test list history filtered by formats."""
        mock_histories = [MockConversionHistory()]
        converter_service.history_repo.find_by_formats.return_value = mock_histories

        filter_params = ConversionHistoryFilter(
            source_format="powerbi",
            target_format="dax",
            limit=10
        )
        result = await converter_service.list_history(filter_params)

        assert result.count == 1
        converter_service.history_repo.find_by_formats.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_history_successful(self, converter_service):
        """Test list successful conversions."""
        mock_histories = [MockConversionHistory(status="success")]
        converter_service.history_repo.find_successful.return_value = mock_histories

        filter_params = ConversionHistoryFilter(status="success")
        result = await converter_service.list_history(filter_params)

        assert result.count == 1
        converter_service.history_repo.find_successful.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_statistics(self, converter_service):
        """Test get conversion statistics."""
        mock_stats = {
            "total_conversions": 100,
            "successful": 85,
            "failed": 15,
            "success_rate": 85.0,
            "average_execution_time_ms": 1500.0,
            "popular_conversions": [
                {"source_format": "powerbi", "target_format": "dax", "count": 50}
            ],
            "period_days": 30
        }
        converter_service.history_repo.get_statistics.return_value = mock_stats

        result = await converter_service.get_statistics(days=30)

        assert result.total_conversions == 100
        assert result.success_rate == 85.0
        assert len(result.popular_conversions) == 1


# ===== ConversionJob Service Tests =====

class TestConverterServiceJobs:
    """Test cases for conversion job operations."""

    @pytest.mark.asyncio
    async def test_create_job_success(self, converter_service):
        """Test successful job creation."""
        job_data = ConversionJobCreate(
            source_format="powerbi",
            target_format="dax",
            configuration={"option1": "value1"}
        )

        mock_job = MockConversionJob()
        converter_service.job_repo.create.return_value = mock_job

        result = await converter_service.create_job(job_data)

        assert result.status == "pending"
        assert result.source_format == "powerbi"
        converter_service.job_repo.create.assert_called_once()

        # Verify job ID is UUID and group context was added
        call_args = converter_service.job_repo.create.call_args[0][0]
        assert "id" in call_args
        assert call_args["group_id"] == "group-1"
        assert call_args["created_by_email"] == "user@example.com"
        assert call_args["status"] == "pending"

    @pytest.mark.asyncio
    async def test_get_job_success(self, converter_service):
        """Test successful job retrieval."""
        mock_job = MockConversionJob(id="job-123")
        converter_service.job_repo.get.return_value = mock_job

        result = await converter_service.get_job("job-123")

        assert result.id == "job-123"
        converter_service.job_repo.get.assert_called_once_with("job-123")

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, converter_service):
        """Test job retrieval when not found."""
        converter_service.job_repo.get.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.get_job("nonexistent")

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_update_job_success(self, converter_service):
        """Test successful job update."""
        existing_job = MockConversionJob(id="job-123")
        updated_job = MockConversionJob(id="job-123", status="running")

        converter_service.job_repo.get.return_value = existing_job
        converter_service.job_repo.update.return_value = updated_job

        update_data = ConversionJobUpdate(status="running")
        result = await converter_service.update_job("job-123", update_data)

        assert result.status == "running"

    @pytest.mark.asyncio
    async def test_update_job_status_success(self, converter_service):
        """Test successful job status update."""
        updated_job = MockConversionJob(id="job-123", status="running")
        converter_service.job_repo.update_status.return_value = updated_job

        status_update = ConversionJobStatusUpdate(
            status="running",
            progress=0.5
        )
        result = await converter_service.update_job_status("job-123", status_update)

        assert result.status == "running"
        converter_service.job_repo.update_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_job_status_not_found(self, converter_service):
        """Test job status update when not found."""
        converter_service.job_repo.update_status.return_value = None

        status_update = ConversionJobStatusUpdate(status="running")

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.update_job_status("nonexistent", status_update)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_list_jobs_with_status_filter(self, converter_service):
        """Test list jobs with status filter."""
        mock_jobs = [MockConversionJob(status="running")]
        converter_service.job_repo.find_by_status.return_value = mock_jobs

        result = await converter_service.list_jobs(status="running", limit=10)

        assert result.count == 1
        converter_service.job_repo.find_by_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_jobs_active_by_default(self, converter_service):
        """Test list jobs returns active jobs by default."""
        mock_jobs = [MockConversionJob(), MockConversionJob()]
        converter_service.job_repo.find_active_jobs.return_value = mock_jobs

        result = await converter_service.list_jobs(limit=10)

        assert result.count == 2
        converter_service.job_repo.find_active_jobs.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_job_success(self, converter_service):
        """Test successful job cancellation."""
        cancelled_job = MockConversionJob(id="job-123", status="cancelled")
        converter_service.job_repo.cancel_job.return_value = cancelled_job

        result = await converter_service.cancel_job("job-123")

        assert result.status == "cancelled"
        converter_service.job_repo.cancel_job.assert_called_once_with("job-123")

    @pytest.mark.asyncio
    async def test_cancel_job_not_found(self, converter_service):
        """Test job cancellation when not found."""
        converter_service.job_repo.cancel_job.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.cancel_job("nonexistent")

        assert exc_info.value.status_code == 400


# ===== SavedConfiguration Service Tests =====

class TestConverterServiceConfigurations:
    """Test cases for saved configuration operations."""

    @pytest.mark.asyncio
    async def test_create_saved_config_success(self, converter_service):
        """Test successful configuration creation."""
        config_data = SavedConfigurationCreate(
            name="My Config",
            source_format="powerbi",
            target_format="dax",
            configuration={"option1": "value1"}
        )

        mock_config = MockSavedConfiguration()
        converter_service.config_repo.create.return_value = mock_config

        result = await converter_service.create_saved_config(config_data)

        assert result.name == "Config"
        converter_service.config_repo.create.assert_called_once()

        # Verify group context was added
        call_args = converter_service.config_repo.create.call_args[0][0]
        assert call_args["group_id"] == "group-1"
        assert call_args["created_by_email"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_create_saved_config_without_auth(self, mock_session):
        """Test configuration creation without authentication."""
        service = ConverterService(mock_session, group_context=None)

        config_data = SavedConfigurationCreate(
            name="Config",
            source_format="powerbi",
            target_format="dax",
            configuration={}
        )

        with pytest.raises(HTTPException) as exc_info:
            await service.create_saved_config(config_data)

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_get_saved_config_success(self, converter_service):
        """Test successful configuration retrieval."""
        mock_config = MockSavedConfiguration(id=123)
        converter_service.config_repo.get.return_value = mock_config

        result = await converter_service.get_saved_config(123)

        assert result.id == 123

    @pytest.mark.asyncio
    async def test_get_saved_config_not_found(self, converter_service):
        """Test configuration retrieval when not found."""
        converter_service.config_repo.get.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.get_saved_config(999)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_update_saved_config_success(self, converter_service):
        """Test successful configuration update."""
        existing_config = MockSavedConfiguration(id=123, created_by_email="user@example.com")
        updated_config = MockSavedConfiguration(id=123, name="Updated Config")

        converter_service.config_repo.get.return_value = existing_config
        converter_service.config_repo.update.return_value = updated_config

        update_data = SavedConfigurationUpdate(name="Updated Config")
        result = await converter_service.update_saved_config(123, update_data)

        assert result.name == "Updated Config"

    @pytest.mark.asyncio
    async def test_update_saved_config_not_authorized(self, converter_service):
        """Test configuration update by non-owner."""
        existing_config = MockSavedConfiguration(
            id=123,
            created_by_email="other@example.com"
        )
        converter_service.config_repo.get.return_value = existing_config

        update_data = SavedConfigurationUpdate(name="Updated")

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.update_saved_config(123, update_data)

        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_saved_config_success(self, converter_service):
        """Test successful configuration deletion."""
        existing_config = MockSavedConfiguration(id=123, created_by_email="user@example.com")
        converter_service.config_repo.get.return_value = existing_config
        converter_service.config_repo.delete.return_value = True

        result = await converter_service.delete_saved_config(123)

        assert "deleted successfully" in result["message"]

    @pytest.mark.asyncio
    async def test_delete_saved_config_not_authorized(self, converter_service):
        """Test configuration deletion by non-owner."""
        existing_config = MockSavedConfiguration(
            id=123,
            created_by_email="other@example.com"
        )
        converter_service.config_repo.get.return_value = existing_config

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.delete_saved_config(123)

        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_saved_configs_templates(self, converter_service):
        """Test list template configurations."""
        mock_configs = [MockSavedConfiguration(is_template=True)]
        converter_service.config_repo.find_templates.return_value = mock_configs

        filter_params = SavedConfigurationFilter(is_template=True)
        result = await converter_service.list_saved_configs(filter_params)

        assert result.count == 1
        converter_service.config_repo.find_templates.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_saved_configs_public(self, converter_service):
        """Test list public configurations."""
        mock_configs = [MockSavedConfiguration(is_public=True)]
        converter_service.config_repo.find_public.return_value = mock_configs

        filter_params = SavedConfigurationFilter(is_public=True)
        result = await converter_service.list_saved_configs(filter_params)

        assert result.count == 1
        converter_service.config_repo.find_public.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_saved_configs_by_formats(self, converter_service):
        """Test list configurations by formats."""
        mock_configs = [MockSavedConfiguration()]
        converter_service.config_repo.find_by_formats.return_value = mock_configs

        filter_params = SavedConfigurationFilter(
            source_format="powerbi",
            target_format="dax"
        )
        result = await converter_service.list_saved_configs(filter_params)

        assert result.count == 1
        converter_service.config_repo.find_by_formats.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_saved_configs_search(self, converter_service):
        """Test search configurations by name."""
        mock_configs = [MockSavedConfiguration(name="PowerBI Config")]
        converter_service.config_repo.search_by_name.return_value = mock_configs

        filter_params = SavedConfigurationFilter(search="PowerBI")
        result = await converter_service.list_saved_configs(filter_params)

        assert result.count == 1
        converter_service.config_repo.search_by_name.assert_called_once()

    @pytest.mark.asyncio
    async def test_use_saved_config_success(self, converter_service):
        """Test marking configuration as used."""
        updated_config = MockSavedConfiguration(id=123, use_count=6)
        converter_service.config_repo.increment_use_count.return_value = updated_config

        result = await converter_service.use_saved_config(123)

        assert result.use_count == 6
        converter_service.config_repo.increment_use_count.assert_called_once_with(123)

    @pytest.mark.asyncio
    async def test_use_saved_config_not_found(self, converter_service):
        """Test marking non-existent configuration as used."""
        converter_service.config_repo.increment_use_count.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await converter_service.use_saved_config(999)

        assert exc_info.value.status_code == 404
