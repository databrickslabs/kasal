"""Unit tests for BillingRepository, BillingPeriodRepository, BillingAlertRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta

from src.repositories.billing_repository import (
    BillingRepository,
    BillingPeriodRepository,
    BillingAlertRepository,
)
from src.models.billing import LLMUsageBilling, BillingPeriod, BillingAlert


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
        mock_session.query.return_value.filter.return_value.filter.return_value.all.return_value = records

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
        chain.filter.return_value.filter.return_value.order_by.return_value.all.return_value = records

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        result = await repo.get_usage_by_date_range(start, end, group_id="g-1", user_email="a@b.com")

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


class TestBillingPeriodRepository:

    @pytest.fixture
    def repo(self, mock_session):
        return BillingPeriodRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_current_period(self, repo, mock_session):
        period = MagicMock(spec=BillingPeriod, status="active")
        mock_session.query.return_value.filter.return_value.first.return_value = period

        result = await repo.get_current_period()

        assert result == period

    @pytest.mark.asyncio
    async def test_get_current_period_with_group(self, repo, mock_session):
        period = MagicMock(spec=BillingPeriod)
        mock_session.query.return_value.filter.return_value.filter.return_value.first.return_value = period

        result = await repo.get_current_period(group_id="g-1")

        assert result == period

    @pytest.mark.asyncio
    async def test_get_current_period_none(self, repo, mock_session):
        mock_session.query.return_value.filter.return_value.first.return_value = None

        result = await repo.get_current_period()

        assert result is None

    @pytest.mark.asyncio
    async def test_create_monthly_period(self, repo, mock_session):
        result = await repo.create_monthly_period(2024, 6)

        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_monthly_period_december(self, repo, mock_session):
        result = await repo.create_monthly_period(2024, 12)

        mock_session.add.assert_called_once()


class TestBillingAlertRepository:

    @pytest.fixture
    def repo(self, mock_session):
        return BillingAlertRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_active_alerts(self, repo, mock_session):
        alerts = [MagicMock(spec=BillingAlert)]
        mock_session.query.return_value.filter.return_value.all.return_value = alerts

        result = await repo.get_active_alerts()

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_active_alerts_with_group(self, repo, mock_session):
        alerts = []
        mock_session.query.return_value.filter.return_value.filter.return_value.all.return_value = alerts

        result = await repo.get_active_alerts(group_id="g-1")

        assert result == []

    @pytest.mark.asyncio
    async def test_update_alert_current_value(self, repo, mock_session):
        alert = MagicMock(spec=BillingAlert, current_value=0)
        # Mock the base get() method
        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = alert
        mock_session.execute.return_value = get_result

        await repo.update_alert_current_value("alert-1", 75.5)

        assert alert.current_value == 75.5

    @pytest.mark.asyncio
    async def test_trigger_alert(self, repo, mock_session):
        alert = MagicMock(spec=BillingAlert, last_triggered=None)
        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = alert
        mock_session.execute.return_value = get_result

        await repo.trigger_alert("alert-1")

        assert alert.last_triggered is not None
