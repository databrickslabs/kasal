"""
Unit tests for task generation API router.

Tests the /task-generation/generate-task POST endpoint with mocked
TaskGenerationService and dependency overrides.
"""
import json
import pytest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.task_generation_router import router
from src.core.dependencies import get_group_context
from src.core.exceptions import KasalError
from src.db.database_router import get_smart_db_session
from src.schemas.task_generation import (
    AdvancedConfig,
    TaskGenerationRequest,
    TaskGenerationResponse,
)
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
def mock_task_service():
    """Create a mock TaskGenerationService instance."""
    svc = AsyncMock()
    svc.generate_task = AsyncMock()
    return svc


@pytest.fixture
def client(mock_task_service):
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
        "src.api.task_generation_router.TaskGenerationService",
        return_value=mock_task_service,
    ):
        yield TestClient(app)


class TestGenerateTask:
    """Tests for POST /task-generation/generate-task."""

    def test_success_returns_task_response(self, client, mock_task_service):
        """Successful generation returns 200 with a TaskGenerationResponse."""
        expected = TaskGenerationResponse(
            name="Data Analysis Task",
            description="Analyze customer data from the database",
            expected_output="Summary report of findings",
            tools=[],
            advanced_config=AdvancedConfig(),
        )
        mock_task_service.generate_task.return_value = expected

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "Create a data analysis task"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Data Analysis Task"
        assert data["description"] == "Analyze customer data from the database"
        assert data["expected_output"] == "Summary report of findings"
        assert "advanced_config" in data

    def test_request_with_agent_context(self, client, mock_task_service):
        """Request can include an optional agent context."""
        expected = TaskGenerationResponse(
            name="Coding Task",
            description="Write Python code",
            expected_output="Working code",
            tools=[{"name": "code_editor"}],
            advanced_config=AdvancedConfig(),
        )
        mock_task_service.generate_task.return_value = expected

        response = client.post(
            "/task-generation/generate-task",
            json={
                "text": "Write a task for a developer agent",
                "model": "test-model",
                "agent": {
                    "name": "Developer",
                    "role": "Software Engineer",
                    "goal": "Write quality code",
                    "backstory": "Experienced Python developer",
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Coding Task"
        assert len(data["tools"]) == 1

    def test_request_with_markdown_flag(self, client, mock_task_service):
        """Request can include the markdown flag."""
        expected = TaskGenerationResponse(
            name="Report Task",
            description="Generate report",
            expected_output="Markdown report",
            tools=[],
            advanced_config=AdvancedConfig(markdown=True),
        )
        mock_task_service.generate_task.return_value = expected

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "Create a markdown report task", "markdown": True},
        )

        assert response.status_code == 200

    def test_missing_text_returns_422(self, client, mock_task_service):
        """Missing required 'text' field returns 422."""
        response = client.post(
            "/task-generation/generate-task",
            json={"model": "some-model"},
        )

        assert response.status_code == 422

    def test_empty_body_returns_422(self, client, mock_task_service):
        """Empty request body returns 422."""
        response = client.post("/task-generation/generate-task", json={})

        assert response.status_code == 422

    def test_json_decode_error_returns_kasal_error(self, client, mock_task_service):
        """json.JSONDecodeError is caught and raised as KasalError (500)."""
        mock_task_service.generate_task.side_effect = json.JSONDecodeError(
            "Expecting value", "doc", 0
        )

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "generate a task"},
        )

        # JSONDecodeError is a subclass of ValueError, so it will be caught
        # by the ValueError handler (400) from conftest since it propagates
        # through the except json.JSONDecodeError block which re-raises KasalError.
        # KasalError has status_code=500 by default.
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to parse AI response as JSON"

    def test_service_value_error_returns_400(self, client, mock_task_service):
        """ValueError from service (not JSONDecodeError) returns 400."""
        mock_task_service.generate_task.side_effect = ValueError("Invalid input")

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "bad request"},
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid input"

    def test_service_unhandled_error_returns_500(self, client, mock_task_service):
        """Unhandled exception from service is caught as 500."""
        mock_task_service.generate_task.side_effect = RuntimeError("internal failure")

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "generate"},
        )

        assert response.status_code == 500

    def test_group_context_forwarded_to_service(self, client, mock_task_service):
        """GroupContext is forwarded to the service generate_task call."""
        expected = TaskGenerationResponse(
            name="T",
            description="D",
            expected_output="E",
            tools=[],
            advanced_config=AdvancedConfig(),
        )
        mock_task_service.generate_task.return_value = expected

        client.post(
            "/task-generation/generate-task",
            json={"text": "test"},
        )

        call_args = mock_task_service.generate_task.call_args
        gc = call_args[0][1]
        assert gc.group_ids == ["g1"]

    def test_advanced_config_defaults_in_response(self, client, mock_task_service):
        """Response includes advanced_config with proper defaults."""
        expected = TaskGenerationResponse(
            name="Task",
            description="Desc",
            expected_output="Output",
            tools=[],
            advanced_config=AdvancedConfig(),
        )
        mock_task_service.generate_task.return_value = expected

        response = client.post(
            "/task-generation/generate-task",
            json={"text": "test task"},
        )

        assert response.status_code == 200
        adv = response.json()["advanced_config"]
        assert adv["async_execution"] is False
        assert adv["retry_on_fail"] is True
        assert adv["max_retries"] == 3
        assert adv["cache_response"] is True
