"""
Unit tests for billing repositories.

Tests the functionality of BillingRepository, BillingPeriodRepository, and BillingAlertRepository
including usage tracking, cost analysis, period management, and alert handling.

Note: The current billing repository implementation has async methods but uses synchronous sessions.
These tests adapt to the current implementation while testing all functionality.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import and_, func, desc, asc

from src.repositories.billing_repository import BillingRepository, BillingPeriodRepository, BillingAlertRepository
from src.models.billing import LLMUsageBilling, BillingPeriod, BillingAlert


# Mock billing models
class MockLLMUsageBilling:
    def __init__(self, id=1, execution_id="exec-123", group_id="group-1", user_email="user@test.com",
                 model_name="gpt-4", model_provider="openai", cost_usd=0.50, total_tokens=1000,
                 prompt_tokens=800, completion_tokens=200, usage_date=None, **kwargs):
        self.id = id
        self.execution_id = execution_id
        self.group_id = group_id
        self.user_email = user_email
        self.model_name = model_name
        self.model_provider = model_provider
        self.cost_usd = cost_usd
        self.total_tokens = total_tokens
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.usage_date = usage_date or datetime.utcnow()
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockBillingPeriod:
    def __init__(self, id=1, period_start=None, period_end=None, period_type="monthly",
                 group_id="group-1", status="active", **kwargs):
        self.id = id
        self.period_start = period_start or datetime.utcnow().replace(day=1)
        self.period_end = period_end or datetime.utcnow()
        self.period_type = period_type
        self.group_id = group_id
        self.status = status
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockBillingAlert:
    def __init__(self, id=1, group_id="group-1", alert_type="cost", threshold=100.0,
                 current_value=50.0, is_active="true", last_triggered=None, **kwargs):
        self.id = id
        self.group_id = group_id
        self.alert_type = alert_type
        self.threshold = threshold
        self.current_value = current_value
        self.is_active = is_active
        self.last_triggered = last_triggered
        self.updated_at = datetime.utcnow()
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock query result for aggregation queries
class MockQueryResult:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    from unittest.mock import AsyncMock
    session = MagicMock()
    session.query.return_value = session
    session.filter.return_value = session
    session.group_by.return_value = session
    session.order_by.return_value = session
    session.limit.return_value = session
    session.all.return_value = []
    session.first.return_value = None
    session.scalar.return_value = None
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.delete = MagicMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    session.execute = AsyncMock()
    
    # Create a proper subquery mock
    subquery_mock = MagicMock()
    subquery_mock.c.total_cost = MagicMock()
    subquery_mock.c.total_cost.__str__ = lambda x: "total_cost"
    subquery_mock.c.execution_id = MagicMock() 
    subquery_mock.c.execution_name = MagicMock()
    subquery_mock.c.execution_type = MagicMock()
    subquery_mock.c.total_tokens = MagicMock()
    subquery_mock.c.latest_usage = MagicMock()
    session.subquery.return_value = subquery_mock
    
    return session


@pytest.fixture
def billing_repository(mock_session):
    """Create a billing repository with mock session."""
    return BillingRepository(session=mock_session)


@pytest.fixture
def billing_period_repository(mock_session):
    """Create a billing period repository with mock session."""
    return BillingPeriodRepository(session=mock_session)


@pytest.fixture
def billing_alert_repository(mock_session):
    """Create a billing alert repository with mock session."""
    return BillingAlertRepository(session=mock_session)


@pytest.fixture
def sample_usage_records():
    """Create sample LLM usage records for testing."""
    return [
        MockLLMUsageBilling(id=1, execution_id="exec-1", cost_usd=1.50, total_tokens=2000),
        MockLLMUsageBilling(id=2, execution_id="exec-1", cost_usd=0.75, total_tokens=1000),
        MockLLMUsageBilling(id=3, execution_id="exec-2", cost_usd=2.25, total_tokens=3000)
    ]


@pytest.fixture
def sample_billing_periods():
    """Create sample billing periods for testing."""
    return [
        MockBillingPeriod(id=1, status="active", period_type="monthly"),
        MockBillingPeriod(id=2, status="closed", period_type="monthly")
    ]


@pytest.fixture
def sample_billing_alerts():
    """Create sample billing alerts for testing."""
    return [
        MockBillingAlert(id=1, alert_type="cost", threshold=100.0, is_active="true"),
        MockBillingAlert(id=2, alert_type="tokens", threshold=50000, is_active="false")
    ]


class TestBillingRepositoryInit:
    """Test cases for BillingRepository initialization."""
    
    def test_init_success(self, mock_session):
        """Test successful initialization."""
        repository = BillingRepository(session=mock_session)
        
        # Repository should properly initialize with model and session in correct order
        assert repository.session == mock_session
        assert repository.model == LLMUsageBilling


class TestBillingRepositoryInheritedMethods:
    """Test cases for inherited BaseRepository methods."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, billing_repository, mock_session):
        """Test successful get by ID."""
        mock_record = MockLLMUsageBilling(id=1)
        
        # Mock the async execute and result chain
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = mock_record
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await billing_repository.get(1)
        
        assert result == mock_record
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, billing_repository, mock_session):
        """Test get by ID when record not found."""
        # Mock the async execute and result chain
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await billing_repository.get(999)
        
        assert result is None
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_exception(self, billing_repository, mock_session):
        """Test get with database exception."""
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await billing_repository.get(1)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_success(self, billing_repository, mock_session):
        """Test successful list with pagination."""
        mock_records = [MockLLMUsageBilling(id=1), MockLLMUsageBilling(id=2)]
        
        # Mock the async execute and result chain
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_records
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await billing_repository.list(skip=0, limit=10)
        
        assert result == mock_records
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_exception(self, billing_repository, mock_session):
        """Test list with database exception."""
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await billing_repository.list()
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_success(self, billing_repository, mock_session):
        """Test successful create."""
        data = {"execution_id": "exec-123", "cost_usd": 1.50}
        
        # Mock the model constructor directly on the repository
        mock_record = MockLLMUsageBilling(**data)
        mock_model_class = MagicMock(return_value=mock_record)
        mock_model_class.__name__ = "LLMUsageBilling"
        
        with patch.object(billing_repository, 'model', mock_model_class):
            
            result = await billing_repository.create(data)
            
            assert result == mock_record
            mock_model_class.assert_called_once_with(**data)
            mock_session.add.assert_called_once_with(mock_record)
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once_with(mock_record)
    
    @pytest.mark.asyncio
    async def test_create_exception(self, billing_repository, mock_session):
        """Test create with database exception."""
        data = {"execution_id": "exec-123"}
        mock_session.flush.side_effect = Exception("Database error")
        
        mock_record = MockLLMUsageBilling(**data)
        mock_model_class = MagicMock(return_value=mock_record)
        mock_model_class.__name__ = "LLMUsageBilling"
        
        with patch.object(billing_repository, 'model', mock_model_class):
            with pytest.raises(Exception, match="Database error"):
                await billing_repository.create(data)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_success(self, billing_repository, mock_session):
        """Test successful add."""
        mock_record = MockLLMUsageBilling(id=1)
        
        result = await billing_repository.add(mock_record)
        
        assert result == mock_record
        mock_session.add.assert_called_once_with(mock_record)
        mock_session.flush.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once_with(mock_record)
    
    @pytest.mark.asyncio
    async def test_add_exception(self, billing_repository, mock_session):
        """Test add with database exception."""
        mock_record = MockLLMUsageBilling(id=1)
        mock_session.flush.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await billing_repository.add(mock_record)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_success(self, billing_repository, mock_session):
        """Test successful update."""
        data = {"cost_usd": 2.50}
        mock_record = MockLLMUsageBilling(id=1, cost_usd=1.50)
        updated_record = MockLLMUsageBilling(id=1, cost_usd=2.50)
        
        # Mock get method calls
        call_count = 0
        async def get_side_effect(record_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_record
            return updated_record
        
        with patch.object(billing_repository, 'get', side_effect=get_side_effect):
            result = await billing_repository.update(1, data)
            
            assert result == updated_record
            mock_session.execute.assert_called_once()
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, billing_repository, mock_session):
        """Test update when record not found."""
        data = {"cost_usd": 2.50}
        
        with patch.object(billing_repository, 'get', return_value=None):
            result = await billing_repository.update(999, data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_exception(self, billing_repository, mock_session):
        """Test update with database exception."""
        data = {"cost_usd": 2.50}
        mock_record = MockLLMUsageBilling(id=1)
        
        with patch.object(billing_repository, 'get', return_value=mock_record):
            mock_session.execute.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await billing_repository.update(1, data)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_success(self, billing_repository, mock_session):
        """Test successful delete."""
        mock_record = MockLLMUsageBilling(id=1)
        
        with patch.object(billing_repository, 'get', return_value=mock_record):
            result = await billing_repository.delete(1)
            
            assert result is True
            mock_session.delete.assert_called_once_with(mock_record)
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, billing_repository, mock_session):
        """Test delete when record not found."""
        with patch.object(billing_repository, 'get', return_value=None):
            result = await billing_repository.delete(999)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_exception(self, billing_repository, mock_session):
        """Test delete with database exception."""
        mock_record = MockLLMUsageBilling(id=1)
        
        with patch.object(billing_repository, 'get', return_value=mock_record):
            mock_session.flush.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await billing_repository.delete(1)
        
        mock_session.rollback.assert_called_once()


class TestBillingRepositoryCreateUsageRecord:
    """Test cases for create_usage_record method."""
    
    @pytest.mark.asyncio
    async def test_create_usage_record_success(self, billing_repository, mock_session):
        """Test successful usage record creation."""
        usage_data = {
            "execution_id": "exec-123",
            "cost_usd": 1.50,
            "total_tokens": 2000,
            "model_name": "gpt-4"
        }
        
        with patch('src.repositories.billing_repository.LLMUsageBilling') as mock_billing_class:
            created_record = MockLLMUsageBilling(**usage_data)
            mock_billing_class.return_value = created_record
            
            result = await billing_repository.create_usage_record(usage_data)
            
            assert result == created_record
            mock_billing_class.assert_called_once_with(**usage_data)
            mock_session.add.assert_called_once_with(created_record)
            mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_usage_record_complex_data(self, billing_repository, mock_session):
        """Test creating usage record with complex data."""
        complex_usage_data = {
            "execution_id": "exec-complex",
            "group_id": "group-enterprise",
            "user_email": "enterprise.user@company.com",
            "model_name": "gpt-4-turbo",
            "model_provider": "openai",
            "cost_usd": 5.75,
            "total_tokens": 8000,
            "prompt_tokens": 6000,
            "completion_tokens": 2000,
            "execution_name": "Complex Analysis Task",
            "execution_type": "analysis"
        }
        
        with patch('src.repositories.billing_repository.LLMUsageBilling') as mock_billing_class:
            created_record = MockLLMUsageBilling(**complex_usage_data)
            mock_billing_class.return_value = created_record
            
            result = await billing_repository.create_usage_record(complex_usage_data)
            
            assert result == created_record
            mock_billing_class.assert_called_once_with(**complex_usage_data)
    
    @pytest.mark.asyncio
    async def test_create_usage_record_session_error(self, billing_repository, mock_session):
        """Test create usage record with session error."""
        usage_data = {"execution_id": "exec-error"}
        
        with patch('src.repositories.billing_repository.LLMUsageBilling') as mock_billing_class:
            mock_billing_class.return_value = MockLLMUsageBilling()
            mock_session.flush.side_effect = Exception("Flush failed")
            
            with pytest.raises(Exception, match="Flush failed"):
                await billing_repository.create_usage_record(usage_data)


class TestBillingRepositoryGetUsageByExecution:
    """Test cases for get_usage_by_execution method."""
    
    @pytest.mark.asyncio
    async def test_get_usage_by_execution_success(self, billing_repository, mock_session, sample_usage_records):
        """Test successful retrieval of usage by execution."""
        execution_records = [record for record in sample_usage_records if record.execution_id == "exec-1"]
        mock_session.all.return_value = execution_records
        
        result = await billing_repository.get_usage_by_execution("exec-1")
        
        assert result == execution_records
        mock_session.query.assert_called_once_with(LLMUsageBilling)
        mock_session.filter.assert_called_once()
        mock_session.all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_usage_by_execution_with_group_id(self, billing_repository, mock_session, sample_usage_records):
        """Test retrieval of usage by execution with group filtering."""
        filtered_records = [r for r in sample_usage_records if r.execution_id == "exec-1" and r.group_id == "group-1"]
        mock_session.all.return_value = filtered_records
        
        result = await billing_repository.get_usage_by_execution("exec-1", group_id="group-1")
        
        assert result == filtered_records
        # Should call filter twice - once for execution_id, once for group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_usage_by_execution_not_found(self, billing_repository, mock_session):
        """Test retrieval when execution has no usage records."""
        mock_session.all.return_value = []
        
        result = await billing_repository.get_usage_by_execution("nonexistent")
        
        assert result == []
        mock_session.query.assert_called_once_with(LLMUsageBilling)
    
    @pytest.mark.asyncio
    async def test_get_usage_by_execution_database_error(self, billing_repository, mock_session):
        """Test get usage by execution with database error."""
        mock_session.all.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await billing_repository.get_usage_by_execution("exec-1")


class TestBillingRepositoryGetUsageByDateRange:
    """Test cases for get_usage_by_date_range method."""
    
    @pytest.mark.asyncio
    async def test_get_usage_by_date_range_success(self, billing_repository, mock_session, sample_usage_records):
        """Test successful retrieval of usage by date range."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = sample_usage_records
        
        result = await billing_repository.get_usage_by_date_range(start_date, end_date)
        
        assert result == sample_usage_records
        mock_session.query.assert_called_once_with(LLMUsageBilling)
        mock_session.filter.assert_called_once()
        mock_session.order_by.assert_called_once()
        mock_session.all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_usage_by_date_range_with_filters(self, billing_repository, mock_session, sample_usage_records):
        """Test retrieval with group and user filters."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        filtered_records = [r for r in sample_usage_records if r.group_id == "group-1"]
        mock_session.all.return_value = filtered_records
        
        result = await billing_repository.get_usage_by_date_range(
            start_date, end_date, group_id="group-1", user_email="user@test.com"
        )
        
        assert result == filtered_records
        # Should call filter 3 times: date range + group_id + user_email
        assert mock_session.filter.call_count == 3
    
    @pytest.mark.asyncio
    async def test_get_usage_by_date_range_empty_result(self, billing_repository, mock_session):
        """Test date range query with empty result."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)
        mock_session.all.return_value = []
        
        result = await billing_repository.get_usage_by_date_range(start_date, end_date)
        
        assert result == []


class TestBillingRepositoryGetCostSummaryByPeriod:
    """Test cases for get_cost_summary_by_period method."""
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_daily(self, billing_repository, mock_session):
        """Test cost summary grouped by day."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                period=datetime(2023, 12, 1),
                total_cost=10.50,
                total_tokens=15000,
                total_prompt_tokens=12000,
                total_completion_tokens=3000,
                total_requests=5
            ),
            MockQueryResult(
                period=datetime(2023, 12, 2),
                total_cost=8.25,
                total_tokens=12000,
                total_prompt_tokens=9000,
                total_completion_tokens=3000,
                total_requests=3
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_summary_by_period(
            start_date, end_date, group_by="day"
        )
        
        assert len(result) == 2
        assert result[0]["period"] == datetime(2023, 12, 1)
        assert result[0]["total_cost"] == 10.50
        assert result[0]["total_tokens"] == 15000
        assert result[0]["total_requests"] == 5
        
        mock_session.query.assert_called_once()
        mock_session.filter.assert_called_once()
        mock_session.group_by.assert_called_once()
        mock_session.order_by.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_weekly(self, billing_repository, mock_session):
        """Test cost summary grouped by week."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                period=datetime(2023, 11, 27),  # Start of week
                total_cost=25.75,
                total_tokens=35000,
                total_prompt_tokens=28000,
                total_completion_tokens=7000,
                total_requests=12
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_summary_by_period(
            start_date, end_date, group_by="week"
        )
        
        assert len(result) == 1
        assert result[0]["total_cost"] == 25.75
        assert result[0]["total_tokens"] == 35000
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_monthly(self, billing_repository, mock_session):
        """Test cost summary grouped by month."""
        start_date = datetime(2023, 10, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                period=datetime(2023, 10, 1),
                total_cost=150.25,
                total_tokens=200000,
                total_prompt_tokens=160000,
                total_completion_tokens=40000,
                total_requests=75
            ),
            MockQueryResult(
                period=datetime(2023, 11, 1),
                total_cost=175.50,
                total_tokens=250000,
                total_prompt_tokens=200000,
                total_completion_tokens=50000,
                total_requests=85
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_summary_by_period(
            start_date, end_date, group_by="month"
        )
        
        assert len(result) == 2
        assert result[0]["total_cost"] == 150.25
        assert result[1]["total_cost"] == 175.50
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_with_group_id(self, billing_repository, mock_session):
        """Test cost summary with group filtering."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = []
        
        result = await billing_repository.get_cost_summary_by_period(
            start_date, end_date, group_id="group-1"
        )
        
        assert result == []
        # Should call filter twice: date range + group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_invalid_group_by(self, billing_repository, mock_session):
        """Test cost summary with invalid group_by defaults to day."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = []
        
        result = await billing_repository.get_cost_summary_by_period(
            start_date, end_date, group_by="invalid"
        )
        
        # Should default to day grouping and return empty result
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_none_values(self, billing_repository, mock_session):
        """Test cost summary handling None values."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                period=datetime(2023, 12, 1),
                total_cost=None,
                total_tokens=None,
                total_prompt_tokens=None,
                total_completion_tokens=None,
                total_requests=None
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_summary_by_period(start_date, end_date)
        
        assert len(result) == 1
        assert result[0]["total_cost"] == 0.0
        assert result[0]["total_tokens"] == 0
        assert result[0]["total_requests"] == 0


class TestBillingRepositoryGetCostByModel:
    """Test cases for get_cost_by_model method."""
    
    @pytest.mark.asyncio
    async def test_get_cost_by_model_success(self, billing_repository, mock_session):
        """Test successful cost breakdown by model."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                model_name="gpt-4",
                model_provider="openai",
                total_cost=25.50,
                total_tokens=35000,
                total_requests=15
            ),
            MockQueryResult(
                model_name="claude-3",
                model_provider="anthropic",
                total_cost=18.75,
                total_tokens=28000,
                total_requests=12
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_by_model(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["model_name"] == "gpt-4"
        assert result[0]["model_provider"] == "openai"
        assert result[0]["total_cost"] == 25.50
        assert result[0]["total_tokens"] == 35000
        assert result[0]["total_requests"] == 15
        
        mock_session.group_by.assert_called_once()
        mock_session.order_by.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cost_by_model_with_group_id(self, billing_repository, mock_session):
        """Test cost breakdown by model with group filtering."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = []
        
        result = await billing_repository.get_cost_by_model(
            start_date, end_date, group_id="group-1"
        )
        
        assert result == []
        # Should call filter twice: date range + group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_cost_by_model_none_values(self, billing_repository, mock_session):
        """Test cost by model handling None values."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                model_name="gpt-4",
                model_provider="openai",
                total_cost=None,
                total_tokens=None,
                total_requests=None
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_by_model(start_date, end_date)
        
        assert len(result) == 1
        assert result[0]["total_cost"] == 0.0
        assert result[0]["total_tokens"] == 0
        assert result[0]["total_requests"] == 0


class TestBillingRepositoryGetCostByUser:
    """Test cases for get_cost_by_user method."""
    
    @pytest.mark.asyncio
    async def test_get_cost_by_user_success(self, billing_repository, mock_session):
        """Test successful cost breakdown by user."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        
        mock_results = [
            MockQueryResult(
                user_email="user1@test.com",
                total_cost=45.25,
                total_tokens=60000,
                total_requests=25
            ),
            MockQueryResult(
                user_email="user2@test.com",
                total_cost=32.50,
                total_tokens=45000,
                total_requests=18
            )
        ]
        mock_session.all.return_value = mock_results
        
        result = await billing_repository.get_cost_by_user(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["user_email"] == "user1@test.com"
        assert result[0]["total_cost"] == 45.25
        assert result[0]["total_tokens"] == 60000
        assert result[0]["total_requests"] == 25
        
        mock_session.group_by.assert_called_once()
        mock_session.order_by.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cost_by_user_with_group_id(self, billing_repository, mock_session):
        """Test cost breakdown by user with group filtering."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = []
        
        result = await billing_repository.get_cost_by_user(
            start_date, end_date, group_id="group-1"
        )
        
        assert result == []
        # Should call filter twice: date range + group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_cost_by_user_filters_null_emails(self, billing_repository, mock_session):
        """Test that cost by user filters out null email addresses."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.return_value = []
        
        await billing_repository.get_cost_by_user(start_date, end_date)
        
        # Should call filter once for the complex date + non-null email filter
        mock_session.filter.assert_called_once()


class TestBillingRepositoryGetMonthlyCostForGroup:
    """Test cases for get_monthly_cost_for_group method."""
    
    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group_success(self, billing_repository, mock_session):
        """Test successful monthly cost retrieval for group."""
        mock_session.scalar.return_value = 150.75
        
        result = await billing_repository.get_monthly_cost_for_group("group-1", 2023, 12)
        
        assert result == 150.75
        mock_session.query.assert_called_once()
        mock_session.filter.assert_called_once()
        mock_session.scalar.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group_december(self, billing_repository, mock_session):
        """Test monthly cost calculation for December (year boundary)."""
        mock_session.scalar.return_value = 200.00
        
        result = await billing_repository.get_monthly_cost_for_group("group-1", 2023, 12)
        
        assert result == 200.00
        # The method should handle December -> January year transition correctly
    
    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group_none_result(self, billing_repository, mock_session):
        """Test monthly cost when no records found."""
        mock_session.scalar.return_value = None
        
        result = await billing_repository.get_monthly_cost_for_group("group-nonexistent", 2023, 12)
        
        assert result == 0.0
    
    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group_february(self, billing_repository, mock_session):
        """Test monthly cost calculation for February."""
        mock_session.scalar.return_value = 85.50
        
        result = await billing_repository.get_monthly_cost_for_group("group-1", 2023, 2)
        
        assert result == 85.50


class TestBillingRepositoryGetRecentExpensiveExecutions:
    """Test cases for get_recent_expensive_executions method."""
    
    @pytest.mark.asyncio
    async def test_get_recent_expensive_executions_success(self, billing_repository, mock_session):
        """Test successful retrieval of expensive executions."""
        mock_results = [
            MockQueryResult(
                execution_id="exec-expensive-1",
                execution_name="Large Analysis",
                execution_type="analysis",
                total_cost=25.50,
                total_tokens=35000,
                latest_usage=datetime.utcnow()
            ),
            MockQueryResult(
                execution_id="exec-expensive-2",
                execution_name="Complex Report",
                execution_type="report",
                total_cost=18.75,
                total_tokens=28000,
                latest_usage=datetime.utcnow() - timedelta(days=1)
            )
        ]
        
        # Create a proper mock that returns results when queried
        subquery_query_mock = MagicMock()
        subquery_query_mock.order_by.return_value = subquery_query_mock
        subquery_query_mock.limit.return_value = subquery_query_mock
        subquery_query_mock.all.return_value = mock_results
        
        # Mock the session.query(subquery) call
        def query_side_effect(*args, **kwargs):
            # Check if first argument is a subquery (has 'c' attribute) or model
            if args and hasattr(args[0], 'c'):  # This is a subquery
                return subquery_query_mock
            else:
                return mock_session  # Return normal mock for model queries
        
        mock_session.query.side_effect = query_side_effect
        
        # Mock the desc function
        with patch('src.repositories.billing_repository.desc', return_value=MagicMock()):
            result = await billing_repository.get_recent_expensive_executions(limit=10)
        
        assert len(result) == 2
        assert result[0]["execution_id"] == "exec-expensive-1"
        assert result[0]["execution_name"] == "Large Analysis"
        assert result[0]["total_cost"] == 25.50
        assert result[0]["total_tokens"] == 35000
        
        mock_session.group_by.assert_called_once()
        mock_session.subquery.assert_called_once()
        subquery_query_mock.order_by.assert_called_once()
        subquery_query_mock.limit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_recent_expensive_executions_with_group_id(self, billing_repository, mock_session):
        """Test expensive executions with group filtering."""
        # Create a proper mock that returns empty results when queried
        subquery_query_mock = MagicMock()
        subquery_query_mock.order_by.return_value = subquery_query_mock
        subquery_query_mock.limit.return_value = subquery_query_mock
        subquery_query_mock.all.return_value = []
        
        # Mock the session.query(subquery) call
        def query_side_effect(*args, **kwargs):
            # Check if first argument is a subquery (has 'c' attribute) or model
            if args and hasattr(args[0], 'c'):  # This is a subquery
                return subquery_query_mock
            else:
                return mock_session  # Return normal mock for model queries
        
        mock_session.query.side_effect = query_side_effect
        
        # Mock the desc function
        with patch('src.repositories.billing_repository.desc', return_value=MagicMock()):
            result = await billing_repository.get_recent_expensive_executions(
                limit=5, group_id="group-1", days=30
            )
        
        assert result == []
        # Should call filter twice: date filter + group_id filter
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_recent_expensive_executions_custom_parameters(self, billing_repository, mock_session):
        """Test expensive executions with custom limit and days."""
        # Create a proper mock that returns empty results when queried
        subquery_query_mock = MagicMock()
        subquery_query_mock.order_by.return_value = subquery_query_mock
        subquery_query_mock.limit.return_value = subquery_query_mock
        subquery_query_mock.all.return_value = []
        
        # Mock the session.query(subquery) call
        def query_side_effect(*args, **kwargs):
            # Check if first argument is a subquery (has 'c' attribute) or model
            if args and hasattr(args[0], 'c'):  # This is a subquery
                return subquery_query_mock
            else:
                return mock_session  # Return normal mock for model queries
        
        mock_session.query.side_effect = query_side_effect
        
        # Mock the desc function
        with patch('src.repositories.billing_repository.desc', return_value=MagicMock()):
            result = await billing_repository.get_recent_expensive_executions(
                limit=5, days=14
            )
        
        assert result == []
        subquery_query_mock.limit.assert_called_with(5)
    
    @pytest.mark.asyncio
    async def test_get_recent_expensive_executions_none_values(self, billing_repository, mock_session):
        """Test expensive executions handling None values."""
        mock_results = [
            MockQueryResult(
                execution_id="exec-1",
                execution_name="Test Execution",
                execution_type="test",
                total_cost=None,
                total_tokens=None,
                latest_usage=datetime.utcnow()
            )
        ]
        
        # Create a proper mock that returns results when queried
        subquery_query_mock = MagicMock()
        subquery_query_mock.order_by.return_value = subquery_query_mock
        subquery_query_mock.limit.return_value = subquery_query_mock
        subquery_query_mock.all.return_value = mock_results
        
        # Mock the session.query(subquery) call
        def query_side_effect(*args, **kwargs):
            # Check if first argument is a subquery (has 'c' attribute) or model
            if args and hasattr(args[0], 'c'):  # This is a subquery
                return subquery_query_mock
            else:
                return mock_session  # Return normal mock for model queries
        
        mock_session.query.side_effect = query_side_effect
        
        # Mock the desc function
        with patch('src.repositories.billing_repository.desc', return_value=MagicMock()):
            result = await billing_repository.get_recent_expensive_executions()
        
        assert len(result) == 1
        assert result[0]["total_cost"] == 0.0
        assert result[0]["total_tokens"] == 0


class TestBillingPeriodRepositoryInit:
    """Test cases for BillingPeriodRepository initialization."""
    
    def test_init_success(self, mock_session):
        """Test successful initialization."""
        repository = BillingPeriodRepository(session=mock_session)
        assert repository.session == mock_session


class TestBillingPeriodRepositoryGetCurrentPeriod:
    """Test cases for get_current_period method."""
    
    @pytest.mark.asyncio
    async def test_get_current_period_success(self, billing_period_repository, mock_session, sample_billing_periods):
        """Test successful retrieval of current period."""
        active_period = sample_billing_periods[0]  # status="active"
        mock_session.first.return_value = active_period
        
        result = await billing_period_repository.get_current_period()
        
        assert result == active_period
        mock_session.query.assert_called_once_with(BillingPeriod)
        mock_session.filter.assert_called_once()
        mock_session.first.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_current_period_with_group_id(self, billing_period_repository, mock_session):
        """Test current period retrieval with group filtering."""
        mock_session.first.return_value = None
        
        result = await billing_period_repository.get_current_period(group_id="group-1")
        
        assert result is None
        # Should call filter twice: status + group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_current_period_none_found(self, billing_period_repository, mock_session):
        """Test current period when no active period exists."""
        mock_session.first.return_value = None
        
        result = await billing_period_repository.get_current_period()
        
        assert result is None


class TestBillingPeriodRepositoryCreateMonthlyPeriod:
    """Test cases for create_monthly_period method."""
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_success(self, billing_period_repository, mock_session):
        """Test successful monthly period creation."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            created_period = MockBillingPeriod(
                period_start=datetime(2023, 12, 1),
                period_end=datetime(2023, 12, 31, 23, 59, 59),
                period_type="monthly"
            )
            mock_period_class.return_value = created_period
            
            result = await billing_period_repository.create_monthly_period(2023, 12)
            
            assert result == created_period
            mock_session.add.assert_called_once_with(created_period)
            mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_december(self, billing_period_repository, mock_session):
        """Test monthly period creation for December (year boundary)."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            created_period = MockBillingPeriod()
            mock_period_class.return_value = created_period
            
            result = await billing_period_repository.create_monthly_period(2023, 12)
            
            # Verify the period was created with correct year transition
            assert result == created_period
            mock_period_class.assert_called_once()
            
            # Check the call arguments for correct date handling
            call_args = mock_period_class.call_args[1]
            assert call_args["period_start"] == datetime(2023, 12, 1)
            # December should end at 2024-01-01 minus 1 second
            expected_end = datetime(2024, 1, 1) - timedelta(seconds=1)
            assert call_args["period_end"] == expected_end
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_with_group_id(self, billing_period_repository, mock_session):
        """Test monthly period creation with group ID."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            created_period = MockBillingPeriod(group_id="group-enterprise")
            mock_period_class.return_value = created_period
            
            result = await billing_period_repository.create_monthly_period(
                2023, 11, group_id="group-enterprise"
            )
            
            assert result == created_period
            call_args = mock_period_class.call_args[1]
            assert call_args["group_id"] == "group-enterprise"
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_february(self, billing_period_repository, mock_session):
        """Test monthly period creation for February."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            created_period = MockBillingPeriod()
            mock_period_class.return_value = created_period
            
            result = await billing_period_repository.create_monthly_period(2023, 2)
            
            call_args = mock_period_class.call_args[1]
            assert call_args["period_start"] == datetime(2023, 2, 1)
            # February should end at March 1st minus 1 second
            expected_end = datetime(2023, 3, 1) - timedelta(seconds=1)
            assert call_args["period_end"] == expected_end


class TestBillingAlertRepositoryInit:
    """Test cases for BillingAlertRepository initialization."""
    
    def test_init_success(self, mock_session):
        """Test successful initialization."""
        repository = BillingAlertRepository(session=mock_session)
        assert repository.session == mock_session


class TestBillingAlertRepositoryGetActiveAlerts:
    """Test cases for get_active_alerts method."""
    
    @pytest.mark.asyncio
    async def test_get_active_alerts_success(self, billing_alert_repository, mock_session, sample_billing_alerts):
        """Test successful retrieval of active alerts."""
        active_alerts = [alert for alert in sample_billing_alerts if alert.is_active == "true"]
        mock_session.all.return_value = active_alerts
        
        result = await billing_alert_repository.get_active_alerts()
        
        assert result == active_alerts
        mock_session.query.assert_called_once_with(BillingAlert)
        mock_session.filter.assert_called_once()
        mock_session.all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_alerts_with_group_id(self, billing_alert_repository, mock_session):
        """Test active alerts retrieval with group filtering."""
        mock_session.all.return_value = []
        
        result = await billing_alert_repository.get_active_alerts(group_id="group-1")
        
        assert result == []
        # Should call filter twice: is_active + group_id
        assert mock_session.filter.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_active_alerts_none_found(self, billing_alert_repository, mock_session):
        """Test active alerts when no active alerts exist."""
        mock_session.all.return_value = []
        
        result = await billing_alert_repository.get_active_alerts()
        
        assert result == []


class TestBillingAlertRepositoryUpdateAlertCurrentValue:
    """Test cases for update_alert_current_value method."""
    
    @pytest.mark.asyncio
    async def test_update_alert_current_value_success(self, billing_alert_repository, mock_session, sample_billing_alerts):
        """Test successful update of alert current value."""
        alert = sample_billing_alerts[0]
        
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, return_value=alert):
            await billing_alert_repository.update_alert_current_value("alert-1", 75.5)
            
            assert alert.current_value == 75.5
            assert alert.updated_at is not None
    
    @pytest.mark.asyncio
    async def test_update_alert_current_value_alert_not_found(self, billing_alert_repository, mock_session):
        """Test update current value when alert not found."""
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, return_value=None):
            # Should not raise error, just do nothing
            await billing_alert_repository.update_alert_current_value("nonexistent", 100.0)
    
    @pytest.mark.asyncio
    async def test_update_alert_current_value_get_by_id_error(self, billing_alert_repository, mock_session):
        """Test update current value when get_by_id fails."""
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, side_effect=Exception("Get failed")):
            with pytest.raises(Exception, match="Get failed"):
                await billing_alert_repository.update_alert_current_value("alert-1", 50.0)


class TestBillingAlertRepositoryTriggerAlert:
    """Test cases for trigger_alert method."""
    
    @pytest.mark.asyncio
    async def test_trigger_alert_success(self, billing_alert_repository, mock_session, sample_billing_alerts):
        """Test successful alert triggering."""
        alert = sample_billing_alerts[0]
        original_triggered = alert.last_triggered
        
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, return_value=alert):
            await billing_alert_repository.trigger_alert("alert-1")
            
            assert alert.last_triggered != original_triggered
            assert alert.updated_at is not None
    
    @pytest.mark.asyncio
    async def test_trigger_alert_not_found(self, billing_alert_repository, mock_session):
        """Test trigger alert when alert not found."""
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, return_value=None):
            # Should not raise error, just do nothing
            await billing_alert_repository.trigger_alert("nonexistent")
    
    @pytest.mark.asyncio
    async def test_trigger_alert_get_by_id_error(self, billing_alert_repository, mock_session):
        """Test trigger alert when get_by_id fails."""
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, side_effect=Exception("Get failed")):
            with pytest.raises(Exception, match="Get failed"):
                await billing_alert_repository.trigger_alert("alert-1")


class TestBillingRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_cost_summary_database_error(self, billing_repository, mock_session):
        """Test cost summary with database error."""
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 31)
        mock_session.all.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await billing_repository.get_cost_summary_by_period(start_date, end_date)
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_flush_error(self, billing_period_repository, mock_session):
        """Test create monthly period with flush error."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            mock_period_class.return_value = MockBillingPeriod()
            mock_session.flush.side_effect = Exception("Flush failed")
            
            with pytest.raises(Exception, match="Flush failed"):
                await billing_period_repository.create_monthly_period(2023, 12)
    
    @pytest.mark.asyncio
    async def test_get_active_alerts_query_error(self, billing_alert_repository, mock_session):
        """Test get active alerts with query error."""
        mock_session.all.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            await billing_alert_repository.get_active_alerts()


class TestBillingRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_get_usage_by_execution_empty_execution_id(self, billing_repository, mock_session):
        """Test get usage with empty execution ID."""
        mock_session.all.return_value = []
        
        result = await billing_repository.get_usage_by_execution("")
        
        assert result == []
        mock_session.query.assert_called_once_with(LLMUsageBilling)
    
    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group_invalid_month(self, billing_repository, mock_session):
        """Test monthly cost with invalid month raises ValueError."""
        # Month 13 should raise ValueError
        with pytest.raises(ValueError, match="month must be in 1..12"):
            await billing_repository.get_monthly_cost_for_group("group-1", 2023, 13)
    
    @pytest.mark.asyncio
    async def test_create_monthly_period_month_1(self, billing_period_repository, mock_session):
        """Test create monthly period for January."""
        with patch('src.repositories.billing_repository.BillingPeriod') as mock_period_class:
            created_period = MockBillingPeriod()
            mock_period_class.return_value = created_period
            
            result = await billing_period_repository.create_monthly_period(2023, 1)
            
            call_args = mock_period_class.call_args[1]
            assert call_args["period_start"] == datetime(2023, 1, 1)
            expected_end = datetime(2023, 2, 1) - timedelta(seconds=1)
            assert call_args["period_end"] == expected_end
    
    @pytest.mark.asyncio
    async def test_update_alert_current_value_zero_value(self, billing_alert_repository, mock_session, sample_billing_alerts):
        """Test update alert current value with zero."""
        alert = sample_billing_alerts[0]
        
        with patch.object(billing_alert_repository, 'get', new_callable=AsyncMock, return_value=alert):
            await billing_alert_repository.update_alert_current_value("alert-1", 0.0)
            
            assert alert.current_value == 0.0
    
    @pytest.mark.asyncio
    async def test_get_recent_expensive_executions_zero_limit(self, billing_repository, mock_session):
        """Test get recent expensive executions with zero limit."""
        # Create a proper mock that returns empty results when queried
        subquery_query_mock = MagicMock()
        subquery_query_mock.order_by.return_value = subquery_query_mock
        subquery_query_mock.limit.return_value = subquery_query_mock
        subquery_query_mock.all.return_value = []
        
        # Mock the session.query(subquery) call
        def query_side_effect(*args, **kwargs):
            # Check if first argument is a subquery (has 'c' attribute) or model
            if args and hasattr(args[0], 'c'):  # This is a subquery
                return subquery_query_mock
            else:
                return mock_session  # Return normal mock for model queries
        
        mock_session.query.side_effect = query_side_effect
        
        # Mock the desc function
        with patch('src.repositories.billing_repository.desc', return_value=MagicMock()):
            result = await billing_repository.get_recent_expensive_executions(limit=0)
        
        assert result == []
        subquery_query_mock.limit.assert_called_with(0)