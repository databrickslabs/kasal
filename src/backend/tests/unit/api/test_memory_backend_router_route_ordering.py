"""
Unit tests for memory backend router route ordering.

Verifies that GET /configs/default is matched before GET /configs/{backend_id}
so that "default" is not captured as a backend_id path parameter.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.memory_backend_router import router, get_memory_backend_service
from src.core.dependencies import get_group_context
from src.db.database_router import get_smart_db_session
from src.schemas.memory_backend import MemoryBackendType
from src.utils.user_context import GroupContext
from tests.unit.router.conftest import register_exception_handlers


def _group_context():
    return GroupContext(
        group_ids=["g1"],
        group_email="u@example.com",
        email_domain="example.com",
        user_role="admin",
    )


def _backend_obj(**overrides):
    """Build a mock memory backend object that passes model_validate."""
    defaults = {
        "id": "uuid-123",
        "group_id": "g1",
        "name": "Test Backend",
        "description": None,
        "backend_type": MemoryBackendType.DATABRICKS,
        "databricks_config": None,
        "lakebase_config": None,
        "enable_short_term": True,
        "enable_long_term": False,
        "enable_entity": False,
        "enable_relationship_retrieval": False,
        "custom_config": None,
        "is_active": True,
        "is_default": True,
        "created_at": "2025-01-01T00:00:00",
        "updated_at": "2025-01-01T00:00:00",
    }
    defaults.update(overrides)
    obj = MagicMock()
    for k, v in defaults.items():
        setattr(obj, k, v)
    # model_validate needs dict-like access; MagicMock with attrs works via
    # Pydantic's from_attributes since MemoryBackendResponse uses model_validate.
    return obj


@pytest.fixture
def mock_service():
    svc = AsyncMock()
    svc.get_default_memory_backend = AsyncMock(return_value=None)
    svc.get_memory_backend = AsyncMock(return_value=None)
    return svc


@pytest.fixture
def client(mock_service):
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    async def override_group_context():
        return _group_context()

    async def override_session():
        return AsyncMock()

    def override_service(session=None):
        return mock_service

    app.dependency_overrides[get_group_context] = override_group_context
    app.dependency_overrides[get_smart_db_session] = override_session
    app.dependency_overrides[get_memory_backend_service] = override_service

    yield TestClient(app, raise_server_exceptions=False)

    app.dependency_overrides.clear()


class TestConfigsDefaultRouteOrdering:
    """Verify /configs/default is not swallowed by /configs/{backend_id}."""

    def test_configs_default_returns_200_with_backend(self, client, mock_service):
        """GET /configs/default returns 200 with config when one exists."""
        backend = _backend_obj()
        mock_service.get_default_memory_backend.return_value = backend

        resp = client.get("/memory-backend/configs/default")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "uuid-123"
        assert body["is_default"] is True
        mock_service.get_default_memory_backend.assert_awaited_once_with("g1")
        # Must NOT have called get_memory_backend (the {backend_id} handler)
        mock_service.get_memory_backend.assert_not_awaited()

    def test_configs_default_returns_200_null_when_none(self, client, mock_service):
        """GET /configs/default returns 200 with null body when none exists."""
        mock_service.get_default_memory_backend.return_value = None

        resp = client.get("/memory-backend/configs/default")

        assert resp.status_code == 200
        assert resp.json() is None
        mock_service.get_memory_backend.assert_not_awaited()

    def test_configs_by_id_still_works(self, client, mock_service):
        """GET /configs/{backend_id} still resolves for actual UUIDs."""
        backend = _backend_obj(id="real-uuid-456", is_default=False)
        mock_service.get_memory_backend.return_value = backend

        resp = client.get("/memory-backend/configs/real-uuid-456")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "real-uuid-456"
        mock_service.get_memory_backend.assert_awaited_once_with("g1", "real-uuid-456")

    def test_configs_by_id_not_found(self, client, mock_service):
        """GET /configs/{backend_id} returns 404 when config not found."""
        mock_service.get_memory_backend.return_value = None

        resp = client.get("/memory-backend/configs/nonexistent-id")

        assert resp.status_code == 404
