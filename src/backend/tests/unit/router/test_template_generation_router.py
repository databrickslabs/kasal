"""
Unit tests for template generation API router.

Tests the /template-generation/generate-templates POST endpoint with
mocked TemplateGenerationService and dependency overrides.
"""
import json
import pytest
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.template_generation_router import (
    router,
    get_template_generation_service,
)
from src.core.dependencies import get_group_context
from src.db.database_router import get_smart_db_session
from src.schemas.template_generation import (
    TemplateGenerationRequest,
    TemplateGenerationResponse,
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
def mock_template_service():
    """Create a mock TemplateGenerationService instance."""
    svc = AsyncMock()
    svc.generate_templates = AsyncMock()
    return svc


@pytest.fixture
def client(mock_template_service):
    """Create a TestClient with dependency overrides."""
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    async def override_group_context():
        return _group_context()

    async def override_session():
        return AsyncMock()

    async def override_template_service():
        return mock_template_service

    app.dependency_overrides[get_group_context] = override_group_context
    app.dependency_overrides[get_smart_db_session] = override_session
    app.dependency_overrides[get_template_generation_service] = override_template_service

    return TestClient(app)


class TestGenerateTemplates:
    """Tests for POST /template-generation/generate-templates."""

    def test_success_returns_templates(self, client, mock_template_service):
        """Successful generation returns 200 with three template strings."""
        expected = TemplateGenerationResponse(
            system_template="You are a helpful assistant.",
            prompt_template="Help the user with: {query}",
            response_template="Here is my response: {response}",
        )
        mock_template_service.generate_templates.return_value = expected

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Customer Service Agent",
                "goal": "Help customers with inquiries",
                "backstory": "Experienced service representative",
                "model": "test-model",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["system_template"] == "You are a helpful assistant."
        assert data["prompt_template"] == "Help the user with: {query}"
        assert data["response_template"] == "Here is my response: {response}"

    def test_default_model_applied(self, client, mock_template_service):
        """When model is omitted, the schema default is used."""
        expected = TemplateGenerationResponse(
            system_template="sys",
            prompt_template="prompt",
            response_template="resp",
        )
        mock_template_service.generate_templates.return_value = expected

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Test Role",
                "goal": "Test Goal",
                "backstory": "Test Backstory",
            },
        )

        assert response.status_code == 200

        # Verify the request passed to service has the default model
        call_args = mock_template_service.generate_templates.call_args[0][0]
        assert call_args.model == "databricks-llama-4-maverick"

    def test_custom_model_passed_through(self, client, mock_template_service):
        """Custom model value is forwarded to the service."""
        expected = TemplateGenerationResponse(
            system_template="s",
            prompt_template="p",
            response_template="r",
        )
        mock_template_service.generate_templates.return_value = expected

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Role",
                "goal": "Goal",
                "backstory": "Backstory",
                "model": "custom-model",
            },
        )

        assert response.status_code == 200
        call_args = mock_template_service.generate_templates.call_args[0][0]
        assert call_args.model == "custom-model"

    def test_missing_role_returns_422(self, client, mock_template_service):
        """Missing required 'role' field returns 422."""
        response = client.post(
            "/template-generation/generate-templates",
            json={"goal": "Goal", "backstory": "Backstory"},
        )

        assert response.status_code == 422

    def test_missing_goal_returns_422(self, client, mock_template_service):
        """Missing required 'goal' field returns 422."""
        response = client.post(
            "/template-generation/generate-templates",
            json={"role": "Role", "backstory": "Backstory"},
        )

        assert response.status_code == 422

    def test_missing_backstory_returns_422(self, client, mock_template_service):
        """Missing required 'backstory' field returns 422."""
        response = client.post(
            "/template-generation/generate-templates",
            json={"role": "Role", "goal": "Goal"},
        )

        assert response.status_code == 422

    def test_empty_body_returns_422(self, client, mock_template_service):
        """Empty request body returns 422."""
        response = client.post("/template-generation/generate-templates", json={})

        assert response.status_code == 422

    def test_json_decode_error_returns_500(self, client, mock_template_service):
        """json.JSONDecodeError raised by service is caught as KasalError (500)."""
        mock_template_service.generate_templates.side_effect = json.JSONDecodeError(
            "Expecting value", "doc", 0
        )

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Role",
                "goal": "Goal",
                "backstory": "Backstory",
            },
        )

        # The router catches json.JSONDecodeError specifically and raises KasalError
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to parse AI response as JSON"

    def test_service_value_error_returns_400(self, client, mock_template_service):
        """ValueError raised by service is mapped to 400."""
        mock_template_service.generate_templates.side_effect = ValueError(
            "Invalid request data"
        )

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Role",
                "goal": "Goal",
                "backstory": "Backstory",
            },
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request data"

    def test_service_unhandled_error_returns_500(self, client, mock_template_service):
        """Unhandled exception from service is caught as 500."""
        mock_template_service.generate_templates.side_effect = RuntimeError("boom")

        response = client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Role",
                "goal": "Goal",
                "backstory": "Backstory",
            },
        )

        assert response.status_code == 500

    def test_service_called_once(self, client, mock_template_service):
        """Service generate_templates is called exactly once per request."""
        expected = TemplateGenerationResponse(
            system_template="s",
            prompt_template="p",
            response_template="r",
        )
        mock_template_service.generate_templates.return_value = expected

        client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Role",
                "goal": "Goal",
                "backstory": "Backstory",
            },
        )

        mock_template_service.generate_templates.assert_called_once()

    def test_request_fields_forwarded_correctly(self, client, mock_template_service):
        """All request fields are correctly forwarded to the service."""
        expected = TemplateGenerationResponse(
            system_template="s",
            prompt_template="p",
            response_template="r",
        )
        mock_template_service.generate_templates.return_value = expected

        client.post(
            "/template-generation/generate-templates",
            json={
                "role": "Senior Engineer",
                "goal": "Build reliable systems",
                "backstory": "20 years of experience",
                "model": "special-model",
            },
        )

        req = mock_template_service.generate_templates.call_args[0][0]
        assert req.role == "Senior Engineer"
        assert req.goal == "Build reliable systems"
        assert req.backstory == "20 years of experience"
        assert req.model == "special-model"
