"""Tests for database_management_router list_lakebase_instances pagination and enable endpoint."""
import sys
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

sys.path.insert(0, "/Users/nehme.tohme/workspace/kasal/src/backend")


@pytest.fixture
def mock_lakebase_service():
    service = AsyncMock()
    return service


@pytest.fixture
def client(mock_lakebase_service):
    """Create a test client with mocked dependencies."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from src.api.database_management_router import router, get_lakebase_service
    from src.core.exceptions import KasalError

    app = FastAPI()

    # Register the exception handler like the real app does
    @app.exception_handler(KasalError)
    async def kasal_error_handler(request, exc):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    app.include_router(router)

    # Override the dependency
    app.dependency_overrides[get_lakebase_service] = lambda: mock_lakebase_service

    return TestClient(app), mock_lakebase_service


# ---- list_lakebase_instances ----

def test_list_instances_default_params(client):
    """GET /lakebase/instances passes default search=None, page=1, page_size=30."""
    test_client, mock_svc = client
    mock_svc.list_instances = AsyncMock(return_value={
        "items": [], "total": 0, "page": 1, "page_size": 30, "total_pages": 0, "has_more": False
    })

    resp = test_client.get("/database-management/lakebase/instances")
    assert resp.status_code == 200
    mock_svc.list_instances.assert_called_once_with(search=None, page=1, page_size=30)


def test_list_instances_with_search(client):
    """GET /lakebase/instances?search=foo passes search param."""
    test_client, mock_svc = client
    mock_svc.list_instances = AsyncMock(return_value={
        "items": [{"name": "foo-inst"}], "total": 1, "page": 1, "page_size": 30, "total_pages": 1, "has_more": False
    })

    resp = test_client.get("/database-management/lakebase/instances?search=foo")
    assert resp.status_code == 200
    mock_svc.list_instances.assert_called_once_with(search="foo", page=1, page_size=30)
    assert resp.json()["items"][0]["name"] == "foo-inst"


def test_list_instances_with_pagination(client):
    """GET /lakebase/instances?page=2&page_size=10 passes pagination params."""
    test_client, mock_svc = client
    mock_svc.list_instances = AsyncMock(return_value={
        "items": [{"name": "inst-10"}], "total": 15, "page": 2, "page_size": 10, "total_pages": 2, "has_more": False
    })

    resp = test_client.get("/database-management/lakebase/instances?page=2&page_size=10")
    assert resp.status_code == 200
    mock_svc.list_instances.assert_called_once_with(search=None, page=2, page_size=10)
    data = resp.json()
    assert data["total"] == 15
    assert data["page"] == 2


# ---- enable endpoint auto-resolves endpoint ----

def test_enable_with_explicit_endpoint(client):
    """POST /lakebase/enable with endpoint skips instance lookup."""
    test_client, mock_svc = client
    mock_svc.enable_lakebase = AsyncMock(return_value={"success": True})

    resp = test_client.post(
        "/database-management/lakebase/enable",
        json={"instance_name": "my-inst", "endpoint": "my-dns.example.com"}
    )
    assert resp.status_code == 200
    mock_svc.get_instance.assert_not_called()
    mock_svc.enable_lakebase.assert_called_once_with("my-inst", "my-dns.example.com")


def test_enable_auto_resolves_endpoint(client):
    """POST /lakebase/enable without endpoint calls get_instance to resolve."""
    test_client, mock_svc = client
    mock_svc.get_instance = AsyncMock(return_value={"read_write_dns": "resolved-dns.example.com"})
    mock_svc.enable_lakebase = AsyncMock(return_value={"success": True})

    resp = test_client.post(
        "/database-management/lakebase/enable",
        json={"instance_name": "auto-inst"}
    )
    assert resp.status_code == 200
    mock_svc.get_instance.assert_called_once_with("auto-inst")
    mock_svc.enable_lakebase.assert_called_once_with("auto-inst", "resolved-dns.example.com")


def test_enable_no_instance_name_returns_400(client):
    """POST /lakebase/enable without instance_name returns 400."""
    test_client, mock_svc = client

    resp = test_client.post(
        "/database-management/lakebase/enable",
        json={}
    )
    assert resp.status_code == 400


def test_enable_unresolvable_endpoint_returns_400(client):
    """POST /lakebase/enable when instance has no DNS returns 400."""
    test_client, mock_svc = client
    mock_svc.get_instance = AsyncMock(return_value={"read_write_dns": None})

    resp = test_client.post(
        "/database-management/lakebase/enable",
        json={"instance_name": "no-dns-inst"}
    )
    assert resp.status_code == 400
