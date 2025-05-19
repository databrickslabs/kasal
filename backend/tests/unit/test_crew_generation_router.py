"""
Unit tests for crew generation API router.

Tests the functionality of the crew generation API endpoints.
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.crew_generation_router import router
from src.schemas.crew import CrewGenerationRequest, CrewCreationResponse
from src.services.crew_generation_service import CrewGenerationService


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
def mock_crew_generation_service():
    """Create a mock crew generation service."""
    service_mock = MagicMock(spec=CrewGenerationService)
    # Configure the async methods
    service_mock.create_crew_complete = AsyncMock()
    return service_mock


@pytest.mark.asyncio
async def test_create_crew_success(client, mock_crew_generation_service):
    """Test successful crew creation."""
    # Configure the mock service to return a valid crew setup
    mock_crew_result = {
        "agents": [
            {
                "id": "abc123",
                "name": "Researcher",
                "role": "Research Assistant",
                "goal": "Find relevant information",
                "backstory": "I am a specialized research assistant",
                "tools": ["web_search", "document_analyzer"]
            },
            {
                "id": "def456",
                "name": "Writer",
                "role": "Content Creator",
                "goal": "Create compelling content",
                "backstory": "I create high-quality content from research",
                "tools": ["text_editor"]
            }
        ],
        "tasks": [
            {
                "id": "task123",
                "name": "Research Topic",
                "description": "Research the given topic thoroughly",
                "assigned_agent": "abc123"
            },
            {
                "id": "task456",
                "name": "Create Report",
                "description": "Create a detailed report based on research",
                "assigned_agent": "def456",
                "context": ["task123"]
            }
        ]
    }
    mock_crew_generation_service.create_crew_complete.return_value = mock_crew_result
    
    # Configure the create mock to return our service mock
    with patch.object(
        CrewGenerationService, 
        "create", 
        return_value=mock_crew_generation_service
    ):
        # Make the request
        response = client.post(
            "/crew/create-crew",
            json={
                "prompt": "Create a research and writing crew",
                "model": "gpt-4o-mini",
                "tools": ["web_search", "document_analyzer", "text_editor"]
            }
        )
        
        # Assert response
        assert response.status_code == 200
        
        # Validate response content
        result = response.json()
        assert "agents" in result
        assert "tasks" in result
        assert len(result["agents"]) == 2
        assert len(result["tasks"]) == 2
        
        # Verify service method was called with correct parameters
        mock_crew_generation_service.create_crew_complete.assert_called_once()
        call_args = mock_crew_generation_service.create_crew_complete.call_args[0][0]
        assert call_args.prompt == "Create a research and writing crew"
        assert call_args.model == "gpt-4o-mini"
        assert call_args.tools == ["web_search", "document_analyzer", "text_editor"]


@pytest.mark.asyncio
async def test_create_crew_with_minimal_params(client, mock_crew_generation_service):
    """Test crew creation with minimal parameters."""
    # Configure mock service response
    mock_crew_result = {
        "agents": [
            {
                "id": "abc123",
                "name": "General Assistant",
                "role": "Assistant",
                "goal": "Help with tasks",
                "backstory": "I am a general-purpose assistant",
                "tools": []
            }
        ],
        "tasks": [
            {
                "id": "task123",
                "name": "Basic Task",
                "description": "Complete a simple task",
                "assigned_agent": "abc123"
            }
        ]
    }
    mock_crew_generation_service.create_crew_complete.return_value = mock_crew_result
    
    # Configure the create mock to return our service mock
    with patch.object(
        CrewGenerationService, 
        "create", 
        return_value=mock_crew_generation_service
    ):
        # Make the request with only the required prompt
        response = client.post(
            "/crew/create-crew",
            json={
                "prompt": "Create a simple assistant crew"
            }
        )
        
        # Assert response
        assert response.status_code == 200
        
        # Validate service call - should use default values for optional params
        mock_crew_generation_service.create_crew_complete.assert_called_once()
        call_args = mock_crew_generation_service.create_crew_complete.call_args[0][0]
        assert call_args.prompt == "Create a simple assistant crew"
        assert call_args.model is None  # Default in schema
        assert call_args.tools == []  # Default in schema


@pytest.mark.asyncio
async def test_create_crew_validation_error(client, mock_crew_generation_service):
    """Test crew creation with validation error."""
    # Configure mock to raise ValueError
    mock_crew_generation_service.create_crew_complete.side_effect = ValueError("Invalid crew configuration")
    
    with patch.object(
        CrewGenerationService, 
        "create", 
        return_value=mock_crew_generation_service
    ):
        # Make the request
        response = client.post(
            "/crew/create-crew",
            json={
                "prompt": "Create an invalid crew"
            }
        )
        
        # Assert response code and detail
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid crew configuration"


@pytest.mark.asyncio
async def test_create_crew_general_error(client, mock_crew_generation_service):
    """Test crew creation with general error."""
    # Configure mock to raise general exception
    mock_crew_generation_service.create_crew_complete.side_effect = Exception("Service unavailable")
    
    with patch.object(
        CrewGenerationService, 
        "create", 
        return_value=mock_crew_generation_service
    ):
        # Make the request
        response = client.post(
            "/crew/create-crew",
            json={
                "prompt": "Create a crew"
            }
        )
        
        # Assert response code and detail
        assert response.status_code == 500
        assert "Error creating crew" in response.json()["detail"] 