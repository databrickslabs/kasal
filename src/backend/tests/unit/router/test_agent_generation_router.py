"""
Unit tests for agent generation API router.

Tests the /agent-generation/generate POST endpoint with mocked
AgentGenerationService and dependency overrides.
"""
import pytest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.agent_generation_router import router, AgentPrompt
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
def mock_agent_generation_service():
    """Create a mock AgentGenerationService instance."""
    svc = AsyncMock()
    svc.generate_agent = AsyncMock()
    return svc


@pytest.fixture
def client(mock_agent_generation_service):
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
        "src.api.agent_generation_router.AgentGenerationService",
        return_value=mock_agent_generation_service,
    ):
        yield TestClient(app)


class TestGenerateAgent:
    """Tests for POST /agent-generation/generate."""

    def test_success_returns_agent_config(self, client, mock_agent_generation_service):
        """Successful generation returns 200 with agent configuration dict."""
        expected = {
            "name": "Research Agent",
            "role": "Research Assistant",
            "goal": "Find relevant information",
            "backstory": "Specialized in research",
            "tools": ["web_search"],
            "advanced_config": {"llm": "test-model", "max_iter": 25},
        }
        mock_agent_generation_service.generate_agent.return_value = expected

        response = client.post(
            "/agent-generation/generate",
            json={
                "prompt": "Create a research agent",
                "model": "test-model",
                "tools": ["web_search"],
            },
        )

        assert response.status_code == 200
        assert response.json() == expected
        mock_agent_generation_service.generate_agent.assert_called_once()
        call_kwargs = mock_agent_generation_service.generate_agent.call_args.kwargs
        assert call_kwargs["prompt_text"] == "Create a research agent"
        assert call_kwargs["model"] == "test-model"
        assert call_kwargs["tools"] == ["web_search"]

    def test_default_model_and_empty_tools(self, client, mock_agent_generation_service):
        """When model and tools are omitted, defaults are applied."""
        mock_agent_generation_service.generate_agent.return_value = {"name": "Agent"}

        response = client.post(
            "/agent-generation/generate",
            json={"prompt": "Simple agent"},
        )

        assert response.status_code == 200
        call_kwargs = mock_agent_generation_service.generate_agent.call_args.kwargs
        assert call_kwargs["model"] == "databricks-llama-4-maverick"
        assert call_kwargs["tools"] == []

    def test_missing_prompt_returns_422(self, client, mock_agent_generation_service):
        """Missing required 'prompt' field returns 422 validation error."""
        response = client.post(
            "/agent-generation/generate",
            json={"model": "test-model"},
        )

        assert response.status_code == 422

    def test_empty_body_returns_422(self, client, mock_agent_generation_service):
        """Empty request body returns 422 validation error."""
        response = client.post("/agent-generation/generate", json={})

        assert response.status_code == 422

    def test_service_value_error_returns_400(self, client, mock_agent_generation_service):
        """ValueError raised by service is mapped to 400."""
        mock_agent_generation_service.generate_agent.side_effect = ValueError(
            "Invalid prompt"
        )

        response = client.post(
            "/agent-generation/generate",
            json={"prompt": "bad prompt"},
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid prompt"

    def test_service_unhandled_error_returns_500(
        self, client, mock_agent_generation_service
    ):
        """Unhandled exception from service is caught as 500."""
        mock_agent_generation_service.generate_agent.side_effect = RuntimeError(
            "boom"
        )

        response = client.post(
            "/agent-generation/generate",
            json={"prompt": "generate"},
        )

        assert response.status_code == 500

    def test_group_context_passed_to_service(
        self, client, mock_agent_generation_service
    ):
        """GroupContext is forwarded to the service call."""
        mock_agent_generation_service.generate_agent.return_value = {}

        client.post(
            "/agent-generation/generate",
            json={"prompt": "test"},
        )

        call_kwargs = mock_agent_generation_service.generate_agent.call_args.kwargs
        gc = call_kwargs["group_context"]
        assert gc.group_ids == ["g1"]
        assert gc.group_email == "u@example.com"


class TestAgentPromptSchema:
    """Tests for the AgentPrompt request model."""

    def test_defaults(self):
        prompt = AgentPrompt(prompt="hello")
        assert prompt.model == "databricks-llama-4-maverick"
        assert prompt.tools == []

    def test_custom_values(self):
        prompt = AgentPrompt(prompt="x", model="custom", tools=["t1", "t2"])
        assert prompt.model == "custom"
        assert prompt.tools == ["t1", "t2"]
