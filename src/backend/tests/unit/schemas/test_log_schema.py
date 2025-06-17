"""
Unit tests for log schemas.

Tests the functionality of Pydantic schemas for LLM log operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.log import (
    LLMLogBase, LLMLogCreate, LLMLogResponse, LLMLogsQueryParams
)


class TestLLMLogBase:
    """Test cases for LLMLogBase schema."""
    
    def test_valid_llm_log_base_minimal(self):
        """Test LLMLogBase with minimal required fields."""
        log_data = {
            "endpoint": "/api/completions",
            "prompt": "What is the weather today?",
            "response": "I don't have access to real-time weather data.",
            "model": "gpt-4",
            "status": "success"
        }
        log = LLMLogBase(**log_data)
        assert log.endpoint == "/api/completions"
        assert log.prompt == "What is the weather today?"
        assert log.response == "I don't have access to real-time weather data."
        assert log.model == "gpt-4"
        assert log.status == "success"
        assert log.tokens_used is None
        assert log.duration_ms is None
        assert log.error_message is None
        assert log.extra_data is None
    
    def test_valid_llm_log_base_complete(self):
        """Test LLMLogBase with all fields."""
        log_data = {
            "endpoint": "/api/chat/completions",
            "prompt": "Analyze this data: {data}",
            "response": "The data shows a clear upward trend...",
            "model": "claude-3-opus-20240229",
            "status": "success",
            "tokens_used": 150,
            "duration_ms": 2500,
            "error_message": None,
            "extra_data": {
                "temperature": 0.7,
                "max_tokens": 500,
                "user_id": "user123"
            }
        }
        log = LLMLogBase(**log_data)
        assert log.endpoint == "/api/chat/completions"
        assert log.tokens_used == 150
        assert log.duration_ms == 2500
        assert log.extra_data["temperature"] == 0.7
        assert log.extra_data["user_id"] == "user123"
    
    def test_llm_log_base_missing_required_fields(self):
        """Test LLMLogBase validation with missing required fields."""
        required_fields = ["endpoint", "prompt", "response", "model", "status"]
        
        for missing_field in required_fields:
            log_data = {
                "endpoint": "/api/test",
                "prompt": "test prompt",
                "response": "test response",
                "model": "test-model",
                "status": "success"
            }
            del log_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                LLMLogBase(**log_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_llm_log_base_empty_strings(self):
        """Test LLMLogBase with empty string values."""
        log_data = {
            "endpoint": "",
            "prompt": "",
            "response": "",
            "model": "",
            "status": ""
        }
        log = LLMLogBase(**log_data)
        assert log.endpoint == ""
        assert log.prompt == ""
        assert log.response == ""
        assert log.model == ""
        assert log.status == ""
    
    def test_llm_log_base_long_content(self):
        """Test LLMLogBase with long content."""
        long_prompt = "A" * 10000
        long_response = "B" * 15000
        
        log_data = {
            "endpoint": "/api/long-content",
            "prompt": long_prompt,
            "response": long_response,
            "model": "large-model",
            "status": "success"
        }
        log = LLMLogBase(**log_data)
        assert len(log.prompt) == 10000
        assert len(log.response) == 15000
    
    def test_llm_log_base_various_statuses(self):
        """Test LLMLogBase with various status values."""
        statuses = ["success", "error", "timeout", "rate_limited", "invalid_request"]
        
        for status in statuses:
            log_data = {
                "endpoint": "/api/test",
                "prompt": "test",
                "response": "test",
                "model": "test-model",
                "status": status
            }
            log = LLMLogBase(**log_data)
            assert log.status == status
    
    def test_llm_log_base_error_scenarios(self):
        """Test LLMLogBase with error scenarios."""
        error_log = LLMLogBase(
            endpoint="/api/completions",
            prompt="Generate a summary",
            response="",
            model="gpt-4",
            status="error",
            tokens_used=0,
            duration_ms=500,
            error_message="Rate limit exceeded",
            extra_data={"error_code": 429, "retry_after": 60}
        )
        assert error_log.status == "error"
        assert error_log.error_message == "Rate limit exceeded"
        assert error_log.extra_data["error_code"] == 429
    
    def test_llm_log_base_complex_extra_data(self):
        """Test LLMLogBase with complex extra_data."""
        complex_extra_data = {
            "request_metadata": {
                "user_id": "user123",
                "session_id": "session456",
                "client_version": "1.2.3"
            },
            "model_parameters": {
                "temperature": 0.8,
                "top_p": 0.9,
                "frequency_penalty": 0.1,
                "presence_penalty": 0.1
            },
            "performance_metrics": {
                "queue_time_ms": 50,
                "processing_time_ms": 2000,
                "total_time_ms": 2050
            },
            "content_analysis": {
                "input_language": "en",
                "output_language": "en",
                "sentiment": "neutral",
                "topics": ["technology", "AI", "programming"]
            }
        }
        
        log_data = {
            "endpoint": "/api/analyze",
            "prompt": "Analyze this complex dataset",
            "response": "Analysis complete with insights",
            "model": "claude-3-sonnet",
            "status": "success",
            "extra_data": complex_extra_data
        }
        log = LLMLogBase(**log_data)
        assert log.extra_data["request_metadata"]["user_id"] == "user123"
        assert log.extra_data["model_parameters"]["temperature"] == 0.8
        assert log.extra_data["content_analysis"]["topics"] == ["technology", "AI", "programming"]


class TestLLMLogCreate:
    """Test cases for LLMLogCreate schema."""
    
    def test_llm_log_create_inheritance(self):
        """Test that LLMLogCreate inherits from LLMLogBase."""
        log_data = {
            "endpoint": "/api/create-test",
            "prompt": "Create a new log entry",
            "response": "Log entry created successfully",
            "model": "gpt-3.5-turbo",
            "status": "success",
            "tokens_used": 75,
            "duration_ms": 1200
        }
        log_create = LLMLogCreate(**log_data)
        
        # Should have all base class attributes
        assert hasattr(log_create, 'endpoint')
        assert hasattr(log_create, 'prompt')
        assert hasattr(log_create, 'response')
        assert hasattr(log_create, 'model')
        assert hasattr(log_create, 'status')
        assert hasattr(log_create, 'tokens_used')
        assert hasattr(log_create, 'duration_ms')
        assert hasattr(log_create, 'error_message')
        assert hasattr(log_create, 'extra_data')
        
        # Values should match
        assert log_create.endpoint == "/api/create-test"
        assert log_create.tokens_used == 75
        assert log_create.duration_ms == 1200
    
    def test_llm_log_create_same_validation(self):
        """Test that LLMLogCreate has same validation as base."""
        # Should fail with missing required fields
        with pytest.raises(ValidationError):
            LLMLogCreate(endpoint="/api/test")
        
        # Should succeed with all required fields
        log_create = LLMLogCreate(
            endpoint="/api/test",
            prompt="test",
            response="test",
            model="test-model",
            status="success"
        )
        assert log_create.status == "success"
    
    def test_llm_log_create_realistic_scenarios(self):
        """Test LLMLogCreate with realistic creation scenarios."""
        # Chat completion log
        chat_log = LLMLogCreate(
            endpoint="/api/chat/completions",
            prompt="User: How do I implement a binary search tree?\nAssistant:",
            response="A binary search tree (BST) is a data structure where each node has at most two children...",
            model="gpt-4",
            status="success",
            tokens_used=342,
            duration_ms=1850,
            extra_data={
                "conversation_id": "conv_123",
                "message_count": 3,
                "user_tier": "premium"
            }
        )
        assert chat_log.model == "gpt-4"
        assert chat_log.extra_data["conversation_id"] == "conv_123"
        
        # Code generation log
        code_log = LLMLogCreate(
            endpoint="/api/completions",
            prompt="def fibonacci(n):",
            response="def fibonacci(n):\n    if n <= 1:\n        return n\n    return fibonacci(n-1) + fibonacci(n-2)",
            model="codex",
            status="success",
            tokens_used=45,
            duration_ms=800,
            extra_data={
                "language": "python",
                "task_type": "code_completion"
            }
        )
        assert code_log.extra_data["language"] == "python"


class TestLLMLogResponse:
    """Test cases for LLMLogResponse schema."""
    
    def test_valid_llm_log_response(self):
        """Test LLMLogResponse with valid data."""
        now = datetime.now()
        response_data = {
            "endpoint": "/api/completions",
            "prompt": "What is machine learning?",
            "response": "Machine learning is a subset of artificial intelligence...",
            "model": "gpt-4",
            "status": "success",
            "tokens_used": 200,
            "duration_ms": 1500,
            "id": 12345,
            "created_at": now
        }
        log_response = LLMLogResponse(**response_data)
        
        # Should have all base class attributes
        assert log_response.endpoint == "/api/completions"
        assert log_response.prompt == "What is machine learning?"
        assert log_response.model == "gpt-4"
        assert log_response.status == "success"
        assert log_response.tokens_used == 200
        
        # Should have response-specific attributes
        assert log_response.id == 12345
        assert log_response.created_at == now
    
    def test_llm_log_response_missing_response_fields(self):
        """Test LLMLogResponse validation with missing response-specific fields."""
        base_data = {
            "endpoint": "/api/test",
            "prompt": "test",
            "response": "test",
            "model": "test-model",
            "status": "success"
        }
        
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            LLMLogResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            LLMLogResponse(**base_data, id=123)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "created_at" in missing_fields
    
    def test_llm_log_response_config(self):
        """Test LLMLogResponse model configuration."""
        assert hasattr(LLMLogResponse, 'model_config')
        assert LLMLogResponse.model_config.get('from_attributes') is True
    
    def test_llm_log_response_datetime_handling(self):
        """Test LLMLogResponse with various datetime formats."""
        response_data = {
            "endpoint": "/api/test",
            "prompt": "test",
            "response": "test",
            "model": "test-model",
            "status": "success",
            "id": 456,
            "created_at": "2023-01-01T12:00:00"
        }
        log_response = LLMLogResponse(**response_data)
        assert isinstance(log_response.created_at, datetime)
    
    def test_llm_log_response_realistic_examples(self):
        """Test LLMLogResponse with realistic examples."""
        # Successful completion
        success_response = LLMLogResponse(
            endpoint="/api/chat/completions",
            prompt="Explain quantum computing in simple terms",
            response="Quantum computing uses quantum mechanical phenomena like superposition and entanglement...",
            model="claude-3-opus-20240229",
            status="success",
            tokens_used=287,
            duration_ms=2200,
            id=789,
            created_at=datetime(2023, 6, 15, 14, 30, 0),
            extra_data={
                "temperature": 0.7,
                "max_tokens": 500,
                "finish_reason": "stop"
            }
        )
        assert success_response.id == 789
        assert success_response.extra_data["finish_reason"] == "stop"
        
        # Error response
        error_response = LLMLogResponse(
            endpoint="/api/completions",
            prompt="Generate inappropriate content",
            response="",
            model="gpt-4",
            status="error",
            tokens_used=0,
            duration_ms=100,
            error_message="Content policy violation",
            id=790,
            created_at=datetime(2023, 6, 15, 14, 31, 0),
            extra_data={
                "error_code": "content_filter",
                "moderation_score": 0.95
            }
        )
        assert error_response.status == "error"
        assert error_response.error_message == "Content policy violation"


class TestLLMLogsQueryParams:
    """Test cases for LLMLogsQueryParams schema."""
    
    def test_valid_llm_logs_query_params_defaults(self):
        """Test LLMLogsQueryParams with default values."""
        params = LLMLogsQueryParams()
        assert params.page == 0
        assert params.per_page == 10
        assert params.endpoint is None
    
    def test_valid_llm_logs_query_params_custom(self):
        """Test LLMLogsQueryParams with custom values."""
        params_data = {
            "page": 5,
            "per_page": 25,
            "endpoint": "/api/completions"
        }
        params = LLMLogsQueryParams(**params_data)
        assert params.page == 5
        assert params.per_page == 25
        assert params.endpoint == "/api/completions"
    
    def test_llm_logs_query_params_validation_page(self):
        """Test LLMLogsQueryParams page validation."""
        # Valid page numbers
        for page in [0, 1, 10, 100]:
            params = LLMLogsQueryParams(page=page)
            assert params.page == page
        
        # Invalid page numbers (negative)
        with pytest.raises(ValidationError) as exc_info:
            LLMLogsQueryParams(page=-1)
        errors = exc_info.value.errors()
        assert any("greater_than_equal" in str(error) for error in errors)
    
    def test_llm_logs_query_params_validation_per_page(self):
        """Test LLMLogsQueryParams per_page validation."""
        # Valid per_page values
        for per_page in [1, 10, 50, 100]:
            params = LLMLogsQueryParams(per_page=per_page)
            assert params.per_page == per_page
        
        # Invalid per_page (too small)
        with pytest.raises(ValidationError) as exc_info:
            LLMLogsQueryParams(per_page=0)
        errors = exc_info.value.errors()
        assert any("greater_than_equal" in str(error) for error in errors)
        
        # Invalid per_page (too large)
        with pytest.raises(ValidationError) as exc_info:
            LLMLogsQueryParams(per_page=101)
        errors = exc_info.value.errors()
        assert any("less_than_equal" in str(error) for error in errors)
    
    def test_llm_logs_query_params_endpoint_values(self):
        """Test LLMLogsQueryParams with various endpoint values."""
        endpoints = [
            None,
            "all",
            "/api/completions",
            "/api/chat/completions",
            "/api/embeddings",
            "",
            "custom-endpoint"
        ]
        
        for endpoint in endpoints:
            params = LLMLogsQueryParams(endpoint=endpoint)
            assert params.endpoint == endpoint
    
    def test_llm_logs_query_params_realistic_scenarios(self):
        """Test LLMLogsQueryParams with realistic query scenarios."""
        # First page, default size
        first_page = LLMLogsQueryParams(page=0, per_page=10)
        assert first_page.page == 0
        assert first_page.per_page == 10
        
        # Large page size for admin
        admin_query = LLMLogsQueryParams(page=0, per_page=100)
        assert admin_query.per_page == 100
        
        # Filtered by endpoint
        filtered_query = LLMLogsQueryParams(
            page=2,
            per_page=20,
            endpoint="/api/chat/completions"
        )
        assert filtered_query.page == 2
        assert filtered_query.endpoint == "/api/chat/completions"
        
        # All endpoints query
        all_endpoints = LLMLogsQueryParams(endpoint="all")
        assert all_endpoints.endpoint == "all"


class TestLogSchemaIntegration:
    """Integration tests for log schema interactions."""
    
    def test_log_creation_to_response_workflow(self):
        """Test complete log creation to response workflow."""
        # Create log
        log_create = LLMLogCreate(
            endpoint="/api/chat/completions",
            prompt="Write a Python function to calculate factorial",
            response="def factorial(n):\n    if n <= 1:\n        return 1\n    return n * factorial(n - 1)",
            model="gpt-4",
            status="success",
            tokens_used=89,
            duration_ms=1200,
            extra_data={"language": "python", "task": "code_generation"}
        )
        
        # Simulate database storage and retrieval
        now = datetime.now()
        log_response = LLMLogResponse(
            **log_create.model_dump(),
            id=12345,
            created_at=now
        )
        
        # Verify workflow
        assert log_response.endpoint == log_create.endpoint
        assert log_response.prompt == log_create.prompt
        assert log_response.response == log_create.response
        assert log_response.model == log_create.model
        assert log_response.status == log_create.status
        assert log_response.tokens_used == log_create.tokens_used
        assert log_response.duration_ms == log_create.duration_ms
        assert log_response.extra_data == log_create.extra_data
        assert log_response.id == 12345
        assert log_response.created_at == now
    
    def test_query_params_usage_scenarios(self):
        """Test query parameters in realistic usage scenarios."""
        # Pagination scenarios
        queries = [
            LLMLogsQueryParams(page=0, per_page=10),  # First page
            LLMLogsQueryParams(page=1, per_page=10),  # Second page
            LLMLogsQueryParams(page=5, per_page=20),  # Middle page, larger size
        ]
        
        for i, query in enumerate(queries):
            assert query.page == [0, 1, 5][i]
            assert query.per_page == [10, 10, 20][i]
        
        # Filtering scenarios
        filter_queries = [
            LLMLogsQueryParams(endpoint="/api/completions"),
            LLMLogsQueryParams(endpoint="/api/chat/completions"),
            LLMLogsQueryParams(endpoint="all"),
            LLMLogsQueryParams()  # No filter
        ]
        
        endpoints = ["/api/completions", "/api/chat/completions", "all", None]
        for query, expected_endpoint in zip(filter_queries, endpoints):
            assert query.endpoint == expected_endpoint
    
    def test_error_tracking_workflow(self):
        """Test error tracking workflow through log schemas."""
        # Error during API call
        error_log = LLMLogCreate(
            endpoint="/api/completions",
            prompt="Generate a very long response",
            response="",
            model="gpt-4",
            status="error",
            tokens_used=0,
            duration_ms=50,
            error_message="Request timeout",
            extra_data={
                "error_type": "timeout",
                "timeout_duration": 30000,
                "retry_count": 3
            }
        )
        
        # Convert to response
        error_response = LLMLogResponse(
            **error_log.model_dump(),
            id=99999,
            created_at=datetime.now()
        )
        
        # Verify error tracking
        assert error_response.status == "error"
        assert error_response.error_message == "Request timeout"
        assert error_response.tokens_used == 0
        assert error_response.extra_data["error_type"] == "timeout"
        assert error_response.extra_data["retry_count"] == 3
    
    def test_analytics_data_collection(self):
        """Test data collection for analytics through log schemas."""
        # Simulate multiple API calls for analytics
        api_calls = [
            {
                "endpoint": "/api/completions",
                "model": "gpt-4",
                "status": "success",
                "tokens": 150,
                "duration": 1200
            },
            {
                "endpoint": "/api/chat/completions",
                "model": "gpt-3.5-turbo",
                "status": "success",
                "tokens": 75,
                "duration": 800
            },
            {
                "endpoint": "/api/completions",
                "model": "gpt-4",
                "status": "error",
                "tokens": 0,
                "duration": 100
            }
        ]
        
        log_responses = []
        for i, call in enumerate(api_calls):
            log_response = LLMLogResponse(
                endpoint=call["endpoint"],
                prompt=f"Test prompt {i}",
                response=f"Test response {i}" if call["status"] == "success" else "",
                model=call["model"],
                status=call["status"],
                tokens_used=call["tokens"],
                duration_ms=call["duration"],
                error_message="API Error" if call["status"] == "error" else None,
                id=i + 1,
                created_at=datetime.now()
            )
            log_responses.append(log_response)
        
        # Analytics calculations
        total_calls = len(log_responses)
        successful_calls = len([log for log in log_responses if log.status == "success"])
        total_tokens = sum(log.tokens_used or 0 for log in log_responses)
        avg_duration = sum(log.duration_ms or 0 for log in log_responses) / total_calls
        
        # Verify analytics data
        assert total_calls == 3
        assert successful_calls == 2
        assert total_tokens == 225
        assert avg_duration == 700  # (1200 + 800 + 100) / 3
        
        # Verify model distribution
        gpt4_calls = len([log for log in log_responses if log.model == "gpt-4"])
        gpt35_calls = len([log for log in log_responses if log.model == "gpt-3.5-turbo"])
        
        assert gpt4_calls == 2
        assert gpt35_calls == 1
    
    def test_log_schema_field_descriptions(self):
        """Test that schema field descriptions are properly set."""
        # Test query params field descriptions
        params_schema = LLMLogsQueryParams.model_json_schema()
        
        assert "description" in params_schema["properties"]["page"]
        assert "Page number, starting from 0" in params_schema["properties"]["page"]["description"]
        
        assert "description" in params_schema["properties"]["per_page"]
        assert "Items per page, between 1 and 100" in params_schema["properties"]["per_page"]["description"]
        
        assert "description" in params_schema["properties"]["endpoint"]
        assert "Filter by endpoint" in params_schema["properties"]["endpoint"]["description"]