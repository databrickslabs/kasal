"""
Unit tests for crew generation API router.

Tests the /crew/create-crew POST endpoint with mocked
CrewGenerationService and dependency overrides.
"""
import pytest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.crew_generation_router import router
from src.core.dependencies import get_group_context
from src.db.database_router import get_smart_db_session
from src.utils.user_context import GroupContext
from tests.unit.router.conftest import register_exception_handlers


def _group_context():
    return GroupContext(
        group_ids=["g1"],
        group_email="u@example.com",
        email_domain="example.com",
        user_role="admin",
    )


@pytest.fixture
def mock_crew_service():
    """Create a mock CrewGenerationService instance."""
    svc = AsyncMock()
    svc.create_crew_complete = AsyncMock()
    return svc


@pytest.fixture
def client(mock_crew_service):
    """Create a TestClient with dependency overrides and service patch."""
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    async def override_group_context():
        return _group_context()

    async def override_session():
        return AsyncMock()

    app.dependency_overrides[get_group_context] = override_group_context
    app.dependency_overrides[get_smart_db_session] = override_session

    with patch(
        "src.api.crew_generation_router.CrewGenerationService",
        return_value=mock_crew_service,
    ):
        yield TestClient(app)


class TestCreateCrew:
    """Tests for POST /crew/create-crew."""

    def test_success_returns_agents_and_tasks(self, client, mock_crew_service):
        """Successful crew creation returns 200 with agents and tasks lists."""
        mock_crew_service.create_crew_complete.return_value = {
            "agents": [
                {
                    "id": "a1",
                    "name": "Researcher",
                    "role": "Research Assistant",
                    "goal": "Find info",
                    "backstory": "Expert researcher",
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Research Topic",
                    "description": "Research thoroughly",
                    "assigned_agent": "a1",
                }
            ],
        }

        response = client.post(
            "/crew/create-crew",
            json={
                "prompt": "Create a research crew",
                "model": "test-model",
                "tools": ["web_search"],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "agents" in data
        assert "tasks" in data
        assert len(data["agents"]) == 1
        assert len(data["tasks"]) == 1
        assert data["agents"][0]["name"] == "Researcher"

    def test_minimal_request_with_defaults(self, client, mock_crew_service):
        """Request with only required 'prompt' uses schema defaults."""
        mock_crew_service.create_crew_complete.return_value = {
            "agents": [],
            "tasks": [],
        }

        response = client.post(
            "/crew/create-crew",
            json={"prompt": "Simple crew"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["agents"] == []
        assert data["tasks"] == []

        # Verify the request object passed to service
        call_args = mock_crew_service.create_crew_complete.call_args
        request_arg = call_args[0][0]
        assert request_arg.prompt == "Simple crew"
        assert request_arg.model is None
        assert request_arg.tools == []

    def test_missing_prompt_returns_422(self, client, mock_crew_service):
        """Missing required 'prompt' field returns 422."""
        response = client.post(
            "/crew/create-crew",
            json={"model": "some-model"},
        )

        assert response.status_code == 422

    def test_empty_body_returns_422(self, client, mock_crew_service):
        """Empty request body returns 422."""
        response = client.post("/crew/create-crew", json={})

        assert response.status_code == 422

    def test_service_value_error_returns_400(self, client, mock_crew_service):
        """ValueError raised by service is mapped to 400."""
        mock_crew_service.create_crew_complete.side_effect = ValueError(
            "Invalid crew configuration"
        )

        response = client.post(
            "/crew/create-crew",
            json={"prompt": "bad crew"},
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid crew configuration"

    def test_service_unhandled_error_returns_500(self, client, mock_crew_service):
        """Unhandled exception from service is caught as 500."""
        mock_crew_service.create_crew_complete.side_effect = RuntimeError("boom")

        response = client.post(
            "/crew/create-crew",
            json={"prompt": "crew"},
        )

        assert response.status_code == 500

    def test_group_context_passed_to_service(self, client, mock_crew_service):
        """GroupContext is forwarded as the second argument to create_crew_complete."""
        mock_crew_service.create_crew_complete.return_value = {
            "agents": [],
            "tasks": [],
        }

        client.post(
            "/crew/create-crew",
            json={"prompt": "test crew"},
        )

        call_args = mock_crew_service.create_crew_complete.call_args
        gc = call_args[0][1]
        assert gc.group_ids == ["g1"]
        assert gc.group_email == "u@example.com"

    def test_multiple_agents_and_tasks(self, client, mock_crew_service):
        """Response correctly serializes multiple agents and tasks."""
        mock_crew_service.create_crew_complete.return_value = {
            "agents": [
                {"id": "a1", "name": "Agent1"},
                {"id": "a2", "name": "Agent2"},
                {"id": "a3", "name": "Agent3"},
            ],
            "tasks": [
                {"id": "t1", "name": "Task1"},
                {"id": "t2", "name": "Task2"},
            ],
        }

        response = client.post(
            "/crew/create-crew",
            json={"prompt": "Multi-agent crew"},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["agents"]) == 3
        assert len(data["tasks"]) == 2
