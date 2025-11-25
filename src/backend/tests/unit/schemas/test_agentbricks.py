"""
Unit tests for AgentBricks schemas.
"""

import pytest
from src.schemas.agentbricks import (
    AgentBricksEndpointState,
    AgentBricksQueryStatus,
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksMessage,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksAuthConfig,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
)


class TestAgentBricksEndpointState:
    """Tests for AgentBricksEndpointState enum."""

    def test_endpoint_state_values(self):
        """Test that all expected state values exist."""
        assert AgentBricksEndpointState.NOT_UPDATING == "NOT_UPDATING"
        assert AgentBricksEndpointState.UPDATING == "UPDATING"
        assert AgentBricksEndpointState.UPDATE_FAILED == "UPDATE_FAILED"
        assert AgentBricksEndpointState.READY == "READY"

    def test_endpoint_state_is_string_enum(self):
        """Test that enum values are strings."""
        for state in AgentBricksEndpointState:
            assert isinstance(state.value, str)


class TestAgentBricksQueryStatus:
    """Tests for AgentBricksQueryStatus enum."""

    def test_query_status_values(self):
        """Test that all expected status values exist."""
        assert AgentBricksQueryStatus.PENDING == "PENDING"
        assert AgentBricksQueryStatus.RUNNING == "RUNNING"
        assert AgentBricksQueryStatus.SUCCESS == "SUCCESS"
        assert AgentBricksQueryStatus.FAILED == "FAILED"
        assert AgentBricksQueryStatus.COMPLETED == "COMPLETED"

    def test_query_status_is_string_enum(self):
        """Test that enum values are strings."""
        for status in AgentBricksQueryStatus:
            assert isinstance(status.value, str)


class TestAgentBricksEndpoint:
    """Tests for AgentBricksEndpoint schema."""

    def test_endpoint_required_fields(self):
        """Test that required fields must be provided."""
        endpoint = AgentBricksEndpoint(
            id="endpoint-123",
            name="test-endpoint"
        )
        assert endpoint.id == "endpoint-123"
        assert endpoint.name == "test-endpoint"

    def test_endpoint_optional_fields(self):
        """Test endpoint with optional fields."""
        endpoint = AgentBricksEndpoint(
            id="endpoint-123",
            name="test-endpoint",
            creator="user@example.com",
            creation_timestamp=1700000000000,
            last_updated_timestamp=1700000001000,
            state=AgentBricksEndpointState.READY,
            config={"key": "value"},
            tags=[{"key": "env", "value": "prod"}],
            task="llm/v1/chat",
            endpoint_type="external"
        )
        assert endpoint.creator == "user@example.com"
        assert endpoint.creation_timestamp == 1700000000000
        assert endpoint.state == "READY"  # use_enum_values=True
        assert endpoint.task == "llm/v1/chat"

    def test_endpoint_default_values(self):
        """Test that optional fields have proper defaults."""
        endpoint = AgentBricksEndpoint(
            id="endpoint-123",
            name="test-endpoint"
        )
        assert endpoint.creator is None
        assert endpoint.state is None
        assert endpoint.tags == []

    def test_endpoint_enum_serialization(self):
        """Test that enums are serialized as values."""
        endpoint = AgentBricksEndpoint(
            id="endpoint-123",
            name="test-endpoint",
            state=AgentBricksEndpointState.READY
        )
        data = endpoint.model_dump()
        assert data["state"] == "READY"


class TestAgentBricksEndpointsRequest:
    """Tests for AgentBricksEndpointsRequest schema."""

    def test_request_defaults(self):
        """Test default values for request."""
        request = AgentBricksEndpointsRequest()
        assert request.search_query is None
        assert request.endpoint_ids is None
        assert request.ready_only is True
        assert request.creator_filter is None

    def test_request_with_filters(self):
        """Test request with filter parameters."""
        request = AgentBricksEndpointsRequest(
            search_query="agent",
            endpoint_ids=["ep-1", "ep-2"],
            ready_only=False,
            creator_filter="user@example.com"
        )
        assert request.search_query == "agent"
        assert len(request.endpoint_ids) == 2
        assert request.ready_only is False


class TestAgentBricksEndpointsResponse:
    """Tests for AgentBricksEndpointsResponse schema."""

    def test_response_defaults(self):
        """Test default values for response."""
        response = AgentBricksEndpointsResponse()
        assert response.endpoints == []
        assert response.total_count == 0
        assert response.filtered is False

    def test_response_with_endpoints(self):
        """Test response with endpoint list."""
        endpoints = [
            AgentBricksEndpoint(id="ep-1", name="endpoint-1"),
            AgentBricksEndpoint(id="ep-2", name="endpoint-2")
        ]
        response = AgentBricksEndpointsResponse(
            endpoints=endpoints,
            total_count=2,
            filtered=True
        )
        assert len(response.endpoints) == 2
        assert response.total_count == 2
        assert response.filtered is True


class TestAgentBricksMessage:
    """Tests for AgentBricksMessage schema."""

    def test_message_required_fields(self):
        """Test that required fields must be provided."""
        message = AgentBricksMessage(
            role="user",
            content="Hello, agent!"
        )
        assert message.role == "user"
        assert message.content == "Hello, agent!"

    def test_message_assistant_role(self):
        """Test message with assistant role."""
        message = AgentBricksMessage(
            role="assistant",
            content="How can I help you?"
        )
        assert message.role == "assistant"


class TestAgentBricksQueryRequest:
    """Tests for AgentBricksQueryRequest schema."""

    def test_query_request_required_fields(self):
        """Test required fields for query request."""
        messages = [AgentBricksMessage(role="user", content="Hello")]
        request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=messages
        )
        assert request.endpoint_name == "test-endpoint"
        assert len(request.messages) == 1

    def test_query_request_optional_fields(self):
        """Test query request with optional fields."""
        messages = [AgentBricksMessage(role="user", content="Hello")]
        request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=messages,
            custom_inputs={"context": "test"},
            return_trace=True,
            stream=True
        )
        assert request.custom_inputs == {"context": "test"}
        assert request.return_trace is True
        assert request.stream is True

    def test_query_request_defaults(self):
        """Test default values for optional fields."""
        messages = [AgentBricksMessage(role="user", content="Hello")]
        request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=messages
        )
        assert request.custom_inputs is None
        assert request.return_trace is False
        assert request.stream is False


class TestAgentBricksQueryResponse:
    """Tests for AgentBricksQueryResponse schema."""

    def test_response_success(self):
        """Test successful query response."""
        response = AgentBricksQueryResponse(
            response="Here is your answer",
            status=AgentBricksQueryStatus.SUCCESS
        )
        assert response.response == "Here is your answer"
        assert response.status == "SUCCESS"  # use_enum_values=True
        assert response.error is None

    def test_response_failure(self):
        """Test failed query response."""
        response = AgentBricksQueryResponse(
            response="",
            status=AgentBricksQueryStatus.FAILED,
            error="Authentication failed"
        )
        assert response.status == "FAILED"
        assert response.error == "Authentication failed"

    def test_response_with_trace(self):
        """Test response with trace information."""
        response = AgentBricksQueryResponse(
            response="Answer",
            status=AgentBricksQueryStatus.SUCCESS,
            trace={"steps": ["step1", "step2"]},
            usage={"prompt_tokens": 10, "completion_tokens": 20}
        )
        assert response.trace is not None
        assert response.usage is not None


class TestAgentBricksAuthConfig:
    """Tests for AgentBricksAuthConfig schema."""

    def test_auth_config_defaults(self):
        """Test default values for auth config."""
        config = AgentBricksAuthConfig()
        assert config.use_obo is True
        assert config.user_token is None
        assert config.pat_token is None
        assert config.host is None

    def test_auth_config_with_values(self):
        """Test auth config with values."""
        config = AgentBricksAuthConfig(
            use_obo=True,
            user_token="user-token-123",
            pat_token="pat-token-456",
            host="https://workspace.databricks.com"
        )
        assert config.user_token == "user-token-123"
        assert config.host == "https://workspace.databricks.com"

    def test_auth_config_excludes_sensitive_fields(self):
        """Test that sensitive fields are excluded from serialization."""
        config = AgentBricksAuthConfig(
            use_obo=True,
            user_token="secret-token",
            pat_token="secret-pat"
        )
        data = config.model_dump()
        assert "user_token" not in data
        assert "pat_token" not in data


class TestAgentBricksExecutionRequest:
    """Tests for AgentBricksExecutionRequest schema."""

    def test_execution_request_required_fields(self):
        """Test required fields for execution request."""
        request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="What is the weather?"
        )
        assert request.endpoint_name == "test-endpoint"
        assert request.question == "What is the weather?"

    def test_execution_request_defaults(self):
        """Test default values for execution request."""
        request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="Hello"
        )
        assert request.custom_inputs is None
        assert request.return_trace is False
        assert request.timeout == 120

    def test_execution_request_with_options(self):
        """Test execution request with optional fields."""
        request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="Hello",
            custom_inputs={"key": "value"},
            return_trace=True,
            timeout=60
        )
        assert request.custom_inputs == {"key": "value"}
        assert request.return_trace is True
        assert request.timeout == 60


class TestAgentBricksExecutionResponse:
    """Tests for AgentBricksExecutionResponse schema."""

    def test_execution_response_success(self):
        """Test successful execution response."""
        response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="Here is your answer"
        )
        assert response.endpoint_name == "test-endpoint"
        assert response.status == "SUCCESS"
        assert response.result == "Here is your answer"
        assert response.error is None

    def test_execution_response_failure(self):
        """Test failed execution response."""
        response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.FAILED,
            error="Endpoint not found"
        )
        assert response.status == "FAILED"
        assert response.error == "Endpoint not found"
        assert response.result is None

    def test_execution_response_with_trace(self):
        """Test execution response with trace."""
        response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="Answer",
            trace={"execution_time": 1.5}
        )
        assert response.trace is not None
