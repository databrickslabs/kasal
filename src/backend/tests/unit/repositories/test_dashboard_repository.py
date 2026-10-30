"""DashboardRepository builds its Lakeview URL from the resolved workspace host.

``_get_base_url`` was dropped by the auth-chain refactor while ``get_dashboard``
and ``list_dashboards`` still called it, so every Lakeview call raised
AttributeError before a request was made.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.dashboard_repository import DashboardRepository


@pytest.fixture
def repo() -> DashboardRepository:
    r = DashboardRepository(user_token="tok")
    r._resolve_host = AsyncMock(return_value="example.com")  # type: ignore[method-assign]
    r._get_headers = AsyncMock(return_value={"Authorization": "Bearer tok"})  # type: ignore[method-assign]
    return r


def _response(status: int, payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.asyncio
async def test_base_url_adds_scheme_and_lakeview_prefix(
    repo: DashboardRepository,
) -> None:
    assert await repo._get_base_url() == "https://example.com/api/2.0/lakeview"


@pytest.mark.asyncio
async def test_base_url_raises_without_host() -> None:
    r = DashboardRepository()
    r._resolve_host = AsyncMock(return_value=None)  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="No Databricks workspace URL"):
        await r._get_base_url()


@pytest.mark.asyncio
async def test_get_dashboard_requests_lakeview_url(repo: DashboardRepository) -> None:
    repo._client = MagicMock()
    repo._client.get = AsyncMock(return_value=_response(200, {"dashboard_id": "d1"}))

    assert await repo.get_dashboard("d1") == {"dashboard_id": "d1"}
    url = repo._client.get.call_args.args[0]
    assert url == "https://example.com/api/2.0/lakeview/dashboards/d1"


@pytest.mark.asyncio
async def test_list_dashboards_returns_page(repo: DashboardRepository) -> None:
    repo._client = MagicMock()
    repo._client.get = AsyncMock(
        return_value=_response(200, {"dashboards": [{"dashboard_id": "d1"}]})
    )

    assert await repo.list_dashboards() == [{"dashboard_id": "d1"}]
