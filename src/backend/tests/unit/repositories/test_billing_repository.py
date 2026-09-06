"""Unit tests for BillingRepository, BillingPeriodRepository, BillingAlertRepository."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models.billing import LLMUsageBilling
from src.repositories.billing_repository import BillingRepository


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    # Sync query interface used by billing repo
    session.query = MagicMock()
    return session


class TestBillingRepository:

    @pytest.fixture
    def repo(self, mock_session):
        return BillingRepository(mock_session)

    @pytest.mark.asyncio
    async def test_create_usage_record(self, repo, mock_session):
        usage_data = {
            "execution_id": "exec-1",
            "model_name": "gpt-4",
            "cost_usd": 0.05,
            "total_tokens": 100,
        }

        result = await repo.create_usage_record(usage_data)

        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_usage_by_execution(self, repo, mock_session):
        records = [MagicMock(spec=LLMUsageBilling)]
        mock_session.query.return_value.filter.return_value.all.return_value = records

        result = await repo.get_usage_by_execution("exec-1")

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_usage_by_execution_with_group(self, repo, mock_session):
        records = [MagicMock(spec=LLMUsageBilling)]
        mock_session.query.return_value.filter.return_value.filter.return_value.all.return_value = (
            records
        )

        result = await repo.get_usage_by_execution("exec-1", group_id="g-1")

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_usage_by_date_range(self, repo, mock_session):
        records = [MagicMock(spec=LLMUsageBilling)]
        chain = mock_session.query.return_value.filter.return_value
        chain.order_by.return_value.all.return_value = records

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_usage_by_date_range(start, end)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_usage_by_date_range_with_filters(self, repo, mock_session):
        records = []
        chain = mock_session.query.return_value.filter.return_value
        chain.filter.return_value.filter.return_value.order_by.return_value.all.return_value = (
            records
        )

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_usage_by_date_range(
            start, end, group_id="g-1", user_email="a@b.com"
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_get_monthly_cost_for_group(self, repo, mock_session):
        mock_session.query.return_value.filter.return_value.scalar.return_value = 42.5

        result = await repo.get_monthly_cost_for_group("g-1", 2024, 6)

        assert result == 42.5

    @pytest.mark.asyncio
    async def test_get_monthly_cost_returns_zero_when_none(self, repo, mock_session):
        mock_session.query.return_value.filter.return_value.scalar.return_value = None

        result = await repo.get_monthly_cost_for_group("g-1", 2024, 12)

        assert result == 0.0


# ============================================================================
# get_cost_summary_by_period / get_cost_by_model / get_cost_by_user
# ============================================================================


def _make_repo():
    session = MagicMock()
    repo = BillingRepository(session)
    return repo, session


def _make_query_chain(results=None):
    """Create a chained mock query."""
    chain = MagicMock()
    chain.filter.return_value = chain
    chain.group_by.return_value = chain
    chain.order_by.return_value = chain
    chain.all.return_value = results or []
    chain.label.return_value = chain
    return chain


def _make_result_row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


class TestBillingRepositoryCostReporting:

    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_day(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_summary_by_period(start, end, group_by="day")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_week(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 3, 31)
        result = await repo.get_cost_summary_by_period(start, end, group_by="week")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_month(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        result = await repo.get_cost_summary_by_period(start, end, group_by="month")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_cost_summary_by_period_unknown(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_summary_by_period(start, end, group_by="unknown")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_cost_summary_with_group_id(self):
        repo, session = _make_repo()
        row = _make_result_row(
            period=datetime(2024, 1, 1),
            total_cost=10.5,
            total_tokens=1000,
            total_prompt_tokens=500,
            total_completion_tokens=500,
            total_requests=5,
        )
        query_chain = _make_query_chain(results=[row])
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_summary_by_period(
            start, end, group_id="g1", group_by="day"
        )
        assert len(result) == 1
        assert result[0]["total_cost"] == 10.5

    @pytest.mark.asyncio
    async def test_get_cost_by_model_no_group(self):
        repo, session = _make_repo()
        row = _make_result_row(
            model_name="gpt-4",
            model_provider="openai",
            total_cost=25.0,
            total_tokens=2000,
            total_requests=10,
        )
        query_chain = _make_query_chain(results=[row])
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_by_model(start, end)
        assert len(result) == 1
        assert result[0]["model_name"] == "gpt-4"

    @pytest.mark.asyncio
    async def test_get_cost_by_model_with_group(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_by_model(start, end, group_id="g1")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_cost_by_user_no_group(self):
        repo, session = _make_repo()
        row = _make_result_row(
            user_email="user@example.com",
            total_cost=15.0,
            total_tokens=1500,
            total_requests=8,
        )
        query_chain = _make_query_chain(results=[row])
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_by_user(start, end)
        assert len(result) == 1
        assert result[0]["user_email"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_get_cost_by_user_with_group(self):
        repo, session = _make_repo()
        query_chain = _make_query_chain()
        session.query.return_value = query_chain

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_cost_by_user(start, end, group_id="g1")
        assert result == []
