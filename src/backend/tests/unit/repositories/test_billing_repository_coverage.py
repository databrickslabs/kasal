"""
Coverage tests for repositories/billing_repository.py
Covers: get_cost_summary_by_period (67-95), get_cost_by_model (114-144),
get_cost_by_user (153-173), and other missing branches
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.repositories.billing_repository import BillingRepository


def make_repo():
    session = MagicMock()
    repo = BillingRepository(session)
    return repo, session


def make_query_chain(results=None):
    """Create a chained mock query."""
    chain = MagicMock()
    chain.filter.return_value = chain
    chain.group_by.return_value = chain
    chain.order_by.return_value = chain
    chain.all.return_value = results or []
    chain.label.return_value = chain
    return chain


def make_result_row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---- get_cost_summary_by_period ----

@pytest.mark.asyncio
async def test_get_cost_summary_by_period_day():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_summary_by_period(start, end, group_by="day")
    assert result == []


@pytest.mark.asyncio
async def test_get_cost_summary_by_period_week():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 3, 31)
    result = await repo.get_cost_summary_by_period(start, end, group_by="week")
    assert result == []


@pytest.mark.asyncio
async def test_get_cost_summary_by_period_month():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)
    result = await repo.get_cost_summary_by_period(start, end, group_by="month")
    assert result == []


@pytest.mark.asyncio
async def test_get_cost_summary_by_period_unknown():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_summary_by_period(start, end, group_by="unknown")
    assert result == []


@pytest.mark.asyncio
async def test_get_cost_summary_with_group_id():
    repo, session = make_repo()
    row = make_result_row(
        period=datetime(2024, 1, 1),
        total_cost=10.5,
        total_tokens=1000,
        total_prompt_tokens=500,
        total_completion_tokens=500,
        total_requests=5
    )
    query_chain = make_query_chain(results=[row])
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_summary_by_period(start, end, group_id="g1", group_by="day")
    assert len(result) == 1
    assert result[0]["total_cost"] == 10.5


# ---- get_cost_by_model ----

@pytest.mark.asyncio
async def test_get_cost_by_model_no_group():
    repo, session = make_repo()
    row = make_result_row(
        model_name="gpt-4",
        model_provider="openai",
        total_cost=25.0,
        total_tokens=2000,
        total_requests=10
    )
    query_chain = make_query_chain(results=[row])
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_by_model(start, end)
    assert len(result) == 1
    assert result[0]["model_name"] == "gpt-4"


@pytest.mark.asyncio
async def test_get_cost_by_model_with_group():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_by_model(start, end, group_id="g1")
    assert result == []


# ---- get_cost_by_user ----

@pytest.mark.asyncio
async def test_get_cost_by_user_no_group():
    repo, session = make_repo()
    row = make_result_row(
        user_email="user@example.com",
        total_cost=15.0,
        total_tokens=1500,
        total_requests=8
    )
    query_chain = make_query_chain(results=[row])
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_by_user(start, end)
    assert len(result) == 1
    assert result[0]["user_email"] == "user@example.com"


@pytest.mark.asyncio
async def test_get_cost_by_user_with_group():
    repo, session = make_repo()
    query_chain = make_query_chain()
    session.query.return_value = query_chain

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)
    result = await repo.get_cost_by_user(start, end, group_id="g1")
    assert result == []
