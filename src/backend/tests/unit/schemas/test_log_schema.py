"""
Unit tests for log schemas.

Tests the functionality of Pydantic schemas for LLM log operations
including validation, serialization, and field constraints.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.schemas.log import LLMLogBase, LLMLogResponse


class TestLLMLogBase:
    """Test cases for LLMLogBase schema."""

    def test_valid_llm_log_base_minimal(self):
        """Test LLMLogBase with minimal required fields."""
        log_data = {
            "endpoint": "/api/completions",
            "prompt": "What is the weather today?",
            "response": "I don't have access to real-time weather data.",
            "model": "gpt-4",
            "status": "success",
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
            "extra_data": {"temperature": 0.7, "max_tokens": 500, "user_id": "user123"},
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
                "status": "success",
            }
            del log_data[missing_field]

            with pytest.raises(ValidationError) as exc_info:
                LLMLogBase(**log_data)

            errors = exc_info.value.errors()
            missing_fields = [
                error["loc"][0] for error in errors if error["type"] == "missing"
            ]
            assert missing_field in missing_fields

    def test_llm_log_base_empty_strings(self):
        """Test LLMLogBase with empty string values."""
        log_data = {
            "endpoint": "",
            "prompt": "",
            "response": "",
            "model": "",
            "status": "",
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
            "status": "success",
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
                "status": status,
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
            extra_data={"error_code": 429, "retry_after": 60},
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
                "client_version": "1.2.3",
            },
            "model_parameters": {
                "temperature": 0.8,
                "top_p": 0.9,
                "frequency_penalty": 0.1,
                "presence_penalty": 0.1,
            },
            "performance_metrics": {
                "queue_time_ms": 50,
                "processing_time_ms": 2000,
                "total_time_ms": 2050,
            },
            "content_analysis": {
                "input_language": "en",
                "output_language": "en",
                "sentiment": "neutral",
                "topics": ["technology", "AI", "programming"],
            },
        }

        log_data = {
            "endpoint": "/api/analyze",
            "prompt": "Analyze this complex dataset",
            "response": "Analysis complete with insights",
            "model": "claude-3-sonnet",
            "status": "success",
            "extra_data": complex_extra_data,
        }
        log = LLMLogBase(**log_data)
        assert log.extra_data["request_metadata"]["user_id"] == "user123"
        assert log.extra_data["model_parameters"]["temperature"] == 0.8
        assert log.extra_data["content_analysis"]["topics"] == [
            "technology",
            "AI",
            "programming",
        ]


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
            "created_at": now,
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
            "status": "success",
        }

        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            LLMLogResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "id" in missing_fields

        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            LLMLogResponse(**base_data, id=123)
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "created_at" in missing_fields

    def test_llm_log_response_config(self):
        """Test LLMLogResponse model configuration."""
        assert hasattr(LLMLogResponse, "model_config")
        assert LLMLogResponse.model_config.get("from_attributes") is True

    def test_llm_log_response_datetime_handling(self):
        """Test LLMLogResponse with various datetime formats."""
        response_data = {
            "endpoint": "/api/test",
            "prompt": "test",
            "response": "test",
            "model": "test-model",
            "status": "success",
            "id": 456,
            "created_at": "2023-01-01T12:00:00",
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
            extra_data={"temperature": 0.7, "max_tokens": 500, "finish_reason": "stop"},
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
            extra_data={"error_code": "content_filter", "moderation_score": 0.95},
        )
        assert error_response.status == "error"
        assert error_response.error_message == "Content policy violation"


class TestLogSchemaIntegration:
    """Integration tests for log schema interactions."""

    def test_analytics_data_collection(self):
        """Test data collection for analytics through log schemas."""
        # Simulate multiple API calls for analytics
        api_calls = [
            {
                "endpoint": "/api/completions",
                "model": "gpt-4",
                "status": "success",
                "tokens": 150,
                "duration": 1200,
            },
            {
                "endpoint": "/api/chat/completions",
                "model": "gpt-3.5-turbo",
                "status": "success",
                "tokens": 75,
                "duration": 800,
            },
            {
                "endpoint": "/api/completions",
                "model": "gpt-4",
                "status": "error",
                "tokens": 0,
                "duration": 100,
            },
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
                created_at=datetime.now(),
            )
            log_responses.append(log_response)

        # Analytics calculations
        total_calls = len(log_responses)
        successful_calls = len(
            [log for log in log_responses if log.status == "success"]
        )
        total_tokens = sum(log.tokens_used or 0 for log in log_responses)
        avg_duration = sum(log.duration_ms or 0 for log in log_responses) / total_calls

        # Verify analytics data
        assert total_calls == 3
        assert successful_calls == 2
        assert total_tokens == 225
        assert avg_duration == 700  # (1200 + 800 + 100) / 3

        # Verify model distribution
        gpt4_calls = len([log for log in log_responses if log.model == "gpt-4"])
        gpt35_calls = len(
            [log for log in log_responses if log.model == "gpt-3.5-turbo"]
        )

        assert gpt4_calls == 2
        assert gpt35_calls == 1
