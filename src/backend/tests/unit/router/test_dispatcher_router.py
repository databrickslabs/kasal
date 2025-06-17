"""
Unit tests for DispatcherRouter.

Tests the functionality of the dispatcher endpoint for routing
natural language requests to appropriate generation services.
"""
import pytest
from unittest.mock import AsyncMock, patch
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.schemas.dispatcher import DispatcherRequest
from src.utils.user_context import GroupContext


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )
    return context


@pytest.fixture
def app(mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.dispatcher_router import router
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override function
    async def override_get_group_context():
        return mock_group_context
    
    # Override dependencies
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app



@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def client(app):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    return TestClient(app)


@pytest.fixture
def sample_dispatcher_request():
    """Create a sample dispatcher request."""
    return DispatcherRequest(
        message="Create an agent that can analyze data",
        options={"model": "gpt-4"}
    )


class TestDispatchRequest:
    """Test cases for dispatch request endpoint."""
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_success(self, mock_create_service, client, mock_group_context, sample_dispatcher_request):
        """Test successful request dispatching."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock dispatch result
        dispatch_result = {
            "intent": "agent_generation",
            "confidence": 0.95,
            "generated_content": {
                "name": "Data Analyst Agent",
                "role": "data analyst",
                "goal": "analyze data effectively"
            }
        }
        mock_service.dispatch.return_value = dispatch_result
        
        response = client.post("/dispatcher/dispatch", json=sample_dispatcher_request.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "agent_generation"
        assert data["confidence"] == 0.95
        assert "generated_content" in data
        mock_service.dispatch.assert_called_once_with(sample_dispatcher_request, mock_group_context)
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_task_generation(self, mock_create_service, client, mock_group_context):
        """Test dispatching request for task generation."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock dispatch result for task generation
        dispatch_result = {
            "intent": "task_generation",
            "confidence": 0.88,
            "generated_content": {
                "name": "Data Analysis Task",
                "description": "Analyze the provided dataset",
                "expected_output": "Analysis report with insights"
            }
        }
        mock_service.dispatch.return_value = dispatch_result
        
        task_request = DispatcherRequest(
            message="Create a task to analyze customer data",
            options={}
        )
        
        response = client.post("/dispatcher/dispatch", json=task_request.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "task_generation"
        assert data["confidence"] == 0.88
        assert "generated_content" in data
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_crew_generation(self, mock_create_service, client, mock_group_context):
        """Test dispatching request for crew generation."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock dispatch result for crew generation
        dispatch_result = {
            "intent": "crew_generation",
            "confidence": 0.92,
            "generated_content": {
                "name": "Data Analysis Crew",
                "agents": [
                    {"name": "Data Collector", "role": "data collector"},
                    {"name": "Data Analyst", "role": "data analyst"}
                ],
                "tasks": [
                    {"name": "Collect Data", "description": "Gather required data"},
                    {"name": "Analyze Data", "description": "Perform analysis"}
                ]
            }
        }
        mock_service.dispatch.return_value = dispatch_result
        
        crew_request = DispatcherRequest(
            message="Create a crew to handle data analysis workflow",
            options={"planning": True}
        )
        
        response = client.post("/dispatcher/dispatch", json=crew_request.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "crew_generation"
        assert data["confidence"] == 0.92
        assert "generated_content" in data
        assert len(data["generated_content"]["agents"]) == 2
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_low_confidence(self, mock_create_service, client, mock_group_context):
        """Test dispatching request with low confidence."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock dispatch result with low confidence
        dispatch_result = {
            "intent": "unknown",
            "confidence": 0.3,
            "error": "Unable to determine intent with sufficient confidence",
            "suggestions": [
                "Try being more specific about what you want to create",
                "Mention if you want to create an agent, task, or crew"
            ]
        }
        mock_service.dispatch.return_value = dispatch_result
        
        ambiguous_request = DispatcherRequest(
            message="Help me with something",
            options={}
        )
        
        response = client.post("/dispatcher/dispatch", json=ambiguous_request.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "unknown"
        assert data["confidence"] == 0.3
        assert "error" in data
        assert "suggestions" in data
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_service_error(self, mock_create_service, client, mock_group_context, sample_dispatcher_request):
        """Test dispatching request with service error."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock service error
        mock_service.dispatch.side_effect = Exception("Service unavailable")
        
        response = client.post("/dispatcher/dispatch", json=sample_dispatcher_request.model_dump())
        
        assert response.status_code == 500
        assert "Error processing request" in response.json()["detail"]
        assert "Service unavailable" in response.json()["detail"]
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_dispatch_request_with_options(self, mock_create_service, client, mock_group_context):
        """Test dispatching request with various options."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock dispatch result
        dispatch_result = {
            "intent": "agent_generation",
            "confidence": 0.95,
            "generated_content": {
                "name": "Advanced Data Agent",
                "role": "senior data scientist"
            },
            "options_used": {
                "model": "gpt-4",
                "temperature": 0.7,
                "planning": True
            }
        }
        mock_service.dispatch.return_value = dispatch_result
        
        request_with_options = DispatcherRequest(
            message="Create an advanced data science agent",
            options={
                "model": "gpt-4", 
                "temperature": 0.7,
                "planning": True
            }
        )
        
        response = client.post("/dispatcher/dispatch", json=request_with_options.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "agent_generation"
        assert "options_used" in data
        assert data["options_used"]["model"] == "gpt-4"
    
    def test_dispatch_request_invalid_data(self, client):
        """Test dispatching request with invalid data."""
        # Test missing required field
        invalid_request = {}  # Missing required 'message' field
        
        response = client.post("/dispatcher/dispatch", json=invalid_request)
        
        assert response.status_code == 422  # Validation error


class TestDetectIntentOnly:
    """Test cases for detect intent only endpoint."""
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_success(self, mock_create_service, client):
        """Test successful intent detection without dispatch."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result
        intent_result = {
            "intent": "generate_agent",
            "confidence": 0.95,
            "extracted_info": {
                "agent_type": "data analyst",
                "capabilities": ["analyze data", "generate reports"]
            },
            "suggested_prompt": "Create a data analyst agent that can analyze data and generate reports"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Create an agent that can analyze data",
            "model": "databricks-llama-4-maverick"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "generate_agent"
        assert data["confidence"] == 0.95
        assert data["extracted_info"]["agent_type"] == "data analyst"
        assert data["suggested_prompt"] == "Create a data analyst agent that can analyze data and generate reports"
        mock_service._detect_intent.assert_called_once_with(
            "Create an agent that can analyze data", 
            "databricks-llama-4-maverick"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_without_model(self, mock_create_service, client):
        """Test intent detection without specifying model (uses default)."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result
        intent_result = {
            "intent": "generate_task",
            "confidence": 0.88,
            "extracted_info": {
                "task_type": "data analysis",
                "action": "analyze"
            },
            "suggested_prompt": "Create a task to analyze the provided data"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Analyze this data for trends"
            # No model specified - should use default
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "generate_task"
        assert data["confidence"] == 0.88
        assert data["extracted_info"]["task_type"] == "data analysis"
        # Should use default model when none specified
        mock_service._detect_intent.assert_called_once_with(
            "Analyze this data for trends", 
            "databricks-llama-4-maverick"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_crew_generation(self, mock_create_service, client):
        """Test intent detection for crew generation."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result for crew
        intent_result = {
            "intent": "generate_crew",
            "confidence": 0.92,
            "extracted_info": {
                "workflow_type": "data processing",
                "agents_needed": ["data collector", "data analyst", "report writer"]
            },
            "suggested_prompt": "Create a crew for data processing workflow with collector, analyst, and writer"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Create a team to collect, analyze and report on data",
            "model": "gpt-4"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "generate_crew"
        assert data["confidence"] == 0.92
        assert "workflow_type" in data["extracted_info"]
        assert len(data["extracted_info"]["agents_needed"]) == 3
        mock_service._detect_intent.assert_called_once_with(
            "Create a team to collect, analyze and report on data", 
            "gpt-4"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_configure_intent(self, mock_create_service, client):
        """Test intent detection for configuration requests."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result for configuration
        intent_result = {
            "intent": "configure_crew",
            "confidence": 0.85,
            "extracted_info": {
                "config_type": "llm_settings",
                "parameters": ["model", "max_rpm"]
            },
            "suggested_prompt": "Configure crew settings for LLM model and rate limits"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Setup the LLM model and configure max RPM for the crew",
            "model": "claude-3-sonnet"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "configure_crew"
        assert data["confidence"] == 0.85
        assert data["extracted_info"]["config_type"] == "llm_settings"
        mock_service._detect_intent.assert_called_once_with(
            "Setup the LLM model and configure max RPM for the crew", 
            "claude-3-sonnet"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_conversation_intent(self, mock_create_service, client):
        """Test intent detection for conversational requests."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result for conversation
        intent_result = {
            "intent": "conversation",
            "confidence": 0.78,
            "extracted_info": {
                "question_type": "general_inquiry",
                "topic": "system_capabilities"
            },
            "suggested_prompt": "Answer the user's question about system capabilities"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "What can this system do?"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "conversation"
        assert data["confidence"] == 0.78
        assert data["extracted_info"]["question_type"] == "general_inquiry"
        # Should use default model when none specified
        mock_service._detect_intent.assert_called_once_with(
            "What can this system do?", 
            "databricks-llama-4-maverick"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_unknown_intent(self, mock_create_service, client):
        """Test intent detection for unclear/unknown requests."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result for unknown intent
        intent_result = {
            "intent": "unknown",
            "confidence": 0.25,
            "extracted_info": {
                "ambiguity_reason": "insufficient_context",
                "keywords_found": []
            },
            "suggested_prompt": "Please provide more specific information about what you want to create"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Help me with something",
            "model": "custom-model"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "unknown"
        assert data["confidence"] == 0.25
        assert data["extracted_info"]["ambiguity_reason"] == "insufficient_context"
        mock_service._detect_intent.assert_called_once_with(
            "Help me with something", 
            "custom-model"
        )
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_service_error(self, mock_create_service, client):
        """Test intent detection with service error."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock service error
        mock_service._detect_intent.side_effect = Exception("Intent detection failed")
        
        request_data = {
            "message": "Create an agent for data analysis"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 500
        assert "Error in intent detection" in response.json()["detail"]
        assert "Intent detection failed" in response.json()["detail"]
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_service_creation_error(self, mock_create_service, client):
        """Test intent detection when service creation fails."""
        # Mock service creation error
        mock_create_service.side_effect = Exception("Service creation failed")
        
        request_data = {
            "message": "Create an agent for data analysis"
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 500
        assert "Error in intent detection" in response.json()["detail"]
        assert "Service creation failed" in response.json()["detail"]
    
    def test_detect_intent_only_invalid_data(self, client):
        """Test intent detection with invalid request data."""
        # Test missing required field
        invalid_request = {}  # Missing required 'message' field
        
        response = client.post("/dispatcher/detect-intent", json=invalid_request)
        
        assert response.status_code == 422  # Validation error
    
    def test_detect_intent_only_empty_message(self, client):
        """Test intent detection with empty message."""
        request_data = {
            "message": ""  # Empty message
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        # Should still pass validation but may have low confidence
        assert response.status_code in [200, 422]  # Depends on validation rules
    
    @patch('src.api.dispatcher_router.DispatcherService.create')
    def test_detect_intent_only_with_tools(self, mock_create_service, client):
        """Test intent detection with tools specified in request."""
        # Mock service instance
        mock_service = AsyncMock()
        mock_create_service.return_value = mock_service
        
        # Mock intent detection result
        intent_result = {
            "intent": "generate_agent",
            "confidence": 0.93,
            "extracted_info": {
                "agent_type": "sql analyst",
                "tools_needed": ["NL2SQLTool", "DatabaseTool"]
            },
            "suggested_prompt": "Create a SQL analyst agent with database tools"
        }
        mock_service._detect_intent.return_value = intent_result
        
        request_data = {
            "message": "Create an agent that can query databases",
            "model": "databricks-llama-4-maverick",
            "tools": ["NL2SQLTool", "DatabaseTool", "FileReadTool"]
        }
        
        response = client.post("/dispatcher/detect-intent", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "generate_agent"
        assert data["confidence"] == 0.93
        # Verify the service was called with correct parameters
        mock_service._detect_intent.assert_called_once_with(
            "Create an agent that can query databases", 
            "databricks-llama-4-maverick"
        )