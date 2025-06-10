"""
Unit tests for agent generation API router.

Tests the functionality of the agent generation API endpoints.
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.agent_generation_router import router, AgentPrompt
from src.services.agent_generation_service import AgentGenerationService


@pytest.fixture
def app():
    """Create a FastAPI app for testing."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client for the app."""
    return TestClient(app)


@pytest.fixture
def mock_agent_generation_service():
    """Create a mock agent generation service."""
    service_mock = MagicMock(spec=AgentGenerationService)
    # Configure the async methods
    service_mock.generate_agent = AsyncMock()
    return service_mock


@pytest.mark.asyncio
async def test_generate_agent_success(client, mock_agent_generation_service):
    """Test successful agent generation."""
    # Configure the mock service to return a valid agent config
    mock_agent_config = {
        "name": "Research Agent",
        "role": "Research Assistant",
        "goal": "Find relevant information from the web",
        "backstory": "I am Kasal specialized in research",
        "tools": ["web_search", "document_analyzer"],
        "advanced_config": {
            "llm": "gpt-4o-mini",
            "max_iter": 25
        }
    }
    mock_agent_generation_service.generate_agent.return_value = mock_agent_config
    
    # Configure the create mock to return our service mock
    with patch.object(
        AgentGenerationService, 
        "create", 
        return_value=mock_agent_generation_service
    ):
        # Make the request
        response = client.post(
            "/agent-generation/generate",
            json={
                "prompt": "Create a research agent that can search the web",
                "model": "gpt-4o-mini",
                "tools": ["web_search", "document_analyzer"]
            }
        )
        
        # Assert response
        assert response.status_code == 200
        
        # Validate response content
        result = response.json()
        assert result == mock_agent_config
        
        # Verify service method was called with correct parameters
        mock_agent_generation_service.generate_agent.assert_called_once_with(
            prompt_text="Create a research agent that can search the web",
            model="gpt-4o-mini",
            tools=["web_search", "document_analyzer"]
        )


@pytest.mark.asyncio
async def test_generate_agent_with_default_model(client, mock_agent_generation_service):
    """Test agent generation with default model parameter."""
    mock_agent_config = {
        "name": "Simple Agent",
        "role": "Assistant",
        "goal": "Help the user",
        "backstory": "I'm a helpful assistant",
        "tools": [],
        "advanced_config": {
            "llm": "gpt-4o-mini"
        }
    }
    mock_agent_generation_service.generate_agent.return_value = mock_agent_config
    
    with patch.object(
        AgentGenerationService, 
        "create", 
        return_value=mock_agent_generation_service
    ):
        # Make a request with only prompt, use default model
        response = client.post(
            "/agent-generation/generate",
            json={
                "prompt": "Create a simple agent"
            }
        )
        
        # Assert response
        assert response.status_code == 200
        
        # Validate service call - should use default model
        mock_agent_generation_service.generate_agent.assert_called_once_with(
            prompt_text="Create a simple agent",
            model="gpt-4o-mini",
            tools=[]
        )


@pytest.mark.asyncio
async def test_generate_agent_validation_error(client, mock_agent_generation_service):
    """Test agent generation with validation error."""
    # Configure mock to raise ValueError
    mock_agent_generation_service.generate_agent.side_effect = ValueError("Invalid prompt")
    
    with patch.object(
        AgentGenerationService, 
        "create", 
        return_value=mock_agent_generation_service
    ):
        # Make the request
        response = client.post(
            "/agent-generation/generate",
            json={
                "prompt": "Invalid prompt",
                "model": "gpt-4o-mini"
            }
        )
        
        # Assert response code and detail
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid prompt"


@pytest.mark.asyncio
async def test_generate_agent_general_error(client, mock_agent_generation_service):
    """Test agent generation with general error."""
    # Configure mock to raise general exception
    mock_agent_generation_service.generate_agent.side_effect = Exception("Service unavailable")
    
    with patch.object(
        AgentGenerationService, 
        "create", 
        return_value=mock_agent_generation_service
    ):
        # Make the request
        response = client.post(
            "/agent-generation/generate",
            json={
                "prompt": "Generate agent",
                "model": "gpt-4o-mini"
            }
        )
        
        # Assert response code and detail
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to generate agent configuration" 