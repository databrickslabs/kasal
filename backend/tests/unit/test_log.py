"""
Unit tests for log models.

Tests the functionality of LLMLog model including
field validation, logging functionality, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from src.models.log import LLMLog


class TestLLMLog:
    """Test cases for LLMLog model."""
    
    def test_llm_log_creation(self):
        """Test basic LLMLog creation."""
        log = LLMLog(
            endpoint="generate-agent",
            prompt="Create an agent for data analysis",
            response="Agent created successfully",
            model="gpt-4",
            status="success"
        )
        
        assert log.endpoint == "generate-agent"
        assert log.prompt == "Create an agent for data analysis"
        assert log.response == "Agent created successfully"
        assert log.model == "gpt-4"
        assert log.status == "success"
    
    def test_llm_log_required_fields(self):
        """Test LLMLog with only required fields."""
        log = LLMLog(
            endpoint="generate-crew",
            prompt="Generate a crew",
            response="Crew generated",
            model="gpt-3.5-turbo",
            status="success"
        )
        
        assert log.endpoint == "generate-crew"
        assert log.prompt == "Generate a crew"
        assert log.response == "Crew generated"
        assert log.model == "gpt-3.5-turbo"
        assert log.status == "success"
        assert log.tokens_used is None
        assert log.duration_ms is None
        assert log.error_message is None
        assert log.extra_data is None
    
    def test_llm_log_with_optional_fields(self):
        """Test LLMLog with optional fields."""
        extra_data = {"temperature": 0.7, "max_tokens": 1000}
        
        log = LLMLog(
            endpoint="generate-task",
            prompt="Create a task for processing",
            response="Task created with specifications",
            model="gpt-4",
            status="success",
            tokens_used=250,
            duration_ms=1500,
            extra_data=extra_data
        )
        
        assert log.tokens_used == 250
        assert log.duration_ms == 1500
        assert log.extra_data == extra_data
        assert log.error_message is None
    
    def test_llm_log_error_case(self):
        """Test LLMLog for error scenarios."""
        log = LLMLog(
            endpoint="generate-agent",
            prompt="Invalid prompt with missing data",
            response="Error occurred during generation",
            model="gpt-4",
            status="error",
            error_message="Invalid input: missing required fields"
        )
        
        assert log.status == "error"
        assert log.error_message == "Invalid input: missing required fields"
    
    def test_llm_log_created_at_default(self):
        """Test that created_at is set by default."""
        with patch('src.models.log.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            log = LLMLog(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="gpt-4",
                status="success"
            )
            
            assert log.created_at == mock_now
    
    def test_llm_log_custom_created_at(self):
        """Test LLMLog with custom created_at."""
        custom_time = datetime(2023, 1, 1, 13, 0, 0)
        
        log = LLMLog(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="gpt-4",
            status="success",
            created_at=custom_time
        )
        
        assert log.created_at == custom_time
    
    def test_llm_log_group_fields(self):
        """Test LLMLog with group-related fields."""
        log = LLMLog(
            endpoint="generate-agent",
            prompt="Create agent for group",
            response="Agent created",
            model="gpt-4",
            status="success",
            group_id="group_123",
            group_email="user@group.com"
        )
        
        assert log.group_id == "group_123"
        assert log.group_email == "user@group.com"
        assert log.tenant_id is None  # Legacy field should be None
        assert log.tenant_email is None  # Legacy field should be None
    
    def test_llm_log_legacy_tenant_fields(self):
        """Test LLMLog with legacy tenant fields."""
        log = LLMLog(
            endpoint="generate-crew",
            prompt="Create crew for tenant",
            response="Crew created",
            model="gpt-4",
            status="success",
            tenant_id="tenant_456",
            tenant_email="user@tenant.com"
        )
        
        assert log.tenant_id == "tenant_456"
        assert log.tenant_email == "user@tenant.com"
        assert log.group_id is None  # New field should be None
        assert log.group_email is None  # New field should be None
    
    def test_llm_log_all_fields(self):
        """Test LLMLog with all fields populated."""
        extra_data = {
            "temperature": 0.8,
            "max_tokens": 2000,
            "top_p": 0.9,
            "frequency_penalty": 0.1
        }
        
        log = LLMLog(
            endpoint="generate-workflow",
            prompt="Create a complex workflow with multiple agents",
            response="Workflow created with 5 agents and 12 tasks",
            model="gpt-4-turbo",
            status="success",
            tokens_used=1850,
            duration_ms=3200,
            extra_data=extra_data,
            group_id="group_789",
            group_email="admin@group.com",
            tenant_id="tenant_legacy",  # Legacy compatibility
            tenant_email="admin@tenant.com"  # Legacy compatibility
        )
        
        assert log.endpoint == "generate-workflow"
        assert log.tokens_used == 1850
        assert log.duration_ms == 3200
        assert log.extra_data["temperature"] == 0.8
        assert log.group_id == "group_789"
        assert log.group_email == "admin@group.com"
        assert log.tenant_id == "tenant_legacy"
        assert log.tenant_email == "admin@tenant.com"


class TestLLMLogFieldTypes:
    """Test cases for LLMLog field types and constraints."""
    
    def test_llm_log_field_existence(self):
        """Test that all expected fields exist."""
        log = LLMLog(
            endpoint="test",
            prompt="test",
            response="test",
            model="gpt-4",
            status="success"
        )
        
        # Check field existence
        assert hasattr(log, 'id')
        assert hasattr(log, 'endpoint')
        assert hasattr(log, 'prompt')
        assert hasattr(log, 'response')
        assert hasattr(log, 'model')
        assert hasattr(log, 'tokens_used')
        assert hasattr(log, 'duration_ms')
        assert hasattr(log, 'status')
        assert hasattr(log, 'error_message')
        assert hasattr(log, 'created_at')
        assert hasattr(log, 'extra_data')
        assert hasattr(log, 'group_id')
        assert hasattr(log, 'tenant_id')
        assert hasattr(log, 'group_email')
        assert hasattr(log, 'tenant_email')
    
    def test_llm_log_string_fields(self):
        """Test string field types and values."""
        log = LLMLog(
            endpoint="generate-agent",
            prompt="Create an agent",
            response="Agent created successfully",
            model="gpt-4",
            status="success",
            error_message="No error",
            group_id="group_123",
            group_email="user@test.com"
        )
        
        assert isinstance(log.endpoint, str)
        assert isinstance(log.prompt, str)
        assert isinstance(log.response, str)
        assert isinstance(log.model, str)
        assert isinstance(log.status, str)
        assert isinstance(log.error_message, str)
        assert isinstance(log.group_id, str)
        assert isinstance(log.group_email, str)
    
    def test_llm_log_integer_fields(self):
        """Test integer field types and values."""
        log = LLMLog(
            endpoint="test",
            prompt="test",
            response="test",
            model="gpt-4",
            status="success",
            tokens_used=500,
            duration_ms=2000
        )
        
        assert isinstance(log.tokens_used, int)
        assert isinstance(log.duration_ms, int)
        assert log.tokens_used == 500
        assert log.duration_ms == 2000
    
    def test_llm_log_datetime_fields(self):
        """Test datetime field types."""
        log = LLMLog(
            endpoint="test",
            prompt="test",
            response="test",
            model="gpt-4",
            status="success"
        )
        
        assert isinstance(log.created_at, datetime)
    
    def test_llm_log_json_fields(self):
        """Test JSON field types."""
        extra_data = {"param1": "value1", "param2": 42}
        
        log = LLMLog(
            endpoint="test",
            prompt="test",
            response="test",
            model="gpt-4",
            status="success",
            extra_data=extra_data
        )
        
        assert isinstance(log.extra_data, dict)
        assert log.extra_data == extra_data
    
    def test_llm_log_nullable_fields(self):
        """Test nullable field behavior."""
        log = LLMLog(
            endpoint="test",
            prompt="test",
            response="test",
            model="gpt-4",
            status="success"
        )
        
        # These fields should be nullable
        assert log.tokens_used is None
        assert log.duration_ms is None
        assert log.error_message is None
        assert log.extra_data is None
        assert log.group_id is None
        assert log.tenant_id is None
        assert log.group_email is None
        assert log.tenant_email is None
    
    def test_llm_log_non_nullable_fields(self):
        """Test non-nullable field requirements."""
        log = LLMLog(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="success"
        )
        
        # These fields are non-nullable
        assert log.endpoint is not None
        assert log.prompt is not None
        assert log.response is not None
        assert log.model is not None
        assert log.status is not None


class TestLLMLogUsagePatterns:
    """Test cases for common LLMLog usage patterns."""
    
    def test_llm_log_successful_generation(self):
        """Test LLMLog for successful generation scenario."""
        log = LLMLog(
            endpoint="generate-agent",
            prompt="Create a data analysis agent with SQL capabilities",
            response='{"name": "DataAnalyst", "tools": ["sql_query", "data_visualization"], "role": "Analyze data and create reports"}',
            model="gpt-4",
            status="success",
            tokens_used=456,
            duration_ms=2340,
            extra_data={
                "temperature": 0.7,
                "max_tokens": 1000,
                "generation_type": "agent_creation"
            },
            group_id="analytics_team",
            group_email="analyst@company.com"
        )
        
        assert log.status == "success"
        assert '"name": "DataAnalyst"' in log.response
        assert log.tokens_used > 0
        assert log.duration_ms > 0
        assert log.error_message is None
        assert log.extra_data["generation_type"] == "agent_creation"
    
    def test_llm_log_failed_generation(self):
        """Test LLMLog for failed generation scenario."""
        log = LLMLog(
            endpoint="generate-crew",
            prompt="Create a crew with invalid configuration",
            response="Generation failed due to invalid input",
            model="gpt-4",
            status="error",
            tokens_used=45,
            duration_ms=850,
            error_message="Invalid configuration: missing required agent roles",
            extra_data={
                "error_type": "validation_error",
                "retry_count": 0
            },
            group_id="dev_team",
            group_email="dev@company.com"
        )
        
        assert log.status == "error"
        assert log.error_message is not None
        assert "validation_error" in log.error_message or log.extra_data["error_type"] == "validation_error"
        assert log.tokens_used < 100  # Failed early
        assert log.extra_data["retry_count"] == 0
    
    def test_llm_log_performance_tracking(self):
        """Test LLMLog for performance tracking."""
        # Fast generation
        fast_log = LLMLog(
            endpoint="generate-task",
            prompt="Simple task generation",
            response="Task created",
            model="gpt-3.5-turbo",
            status="success",
            tokens_used=120,
            duration_ms=800
        )
        
        # Slow generation
        slow_log = LLMLog(
            endpoint="generate-complex-workflow",
            prompt="Create a complex multi-agent workflow with 20 tasks",
            response="Complex workflow created with detailed specifications",
            model="gpt-4",
            status="success",
            tokens_used=2500,
            duration_ms=15000
        )
        
        # Verify performance differences
        assert fast_log.duration_ms < 1000
        assert fast_log.tokens_used < 200
        
        assert slow_log.duration_ms > 10000
        assert slow_log.tokens_used > 2000
        
        # Performance comparison
        assert slow_log.duration_ms > fast_log.duration_ms
        assert slow_log.tokens_used > fast_log.tokens_used
    
    def test_llm_log_model_comparison(self):
        """Test LLMLog for comparing different models."""
        models_tested = ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
        logs = []
        
        for i, model in enumerate(models_tested):
            log = LLMLog(
                endpoint="generate-agent",
                prompt="Create a customer service agent",
                response=f"Agent created using {model}",
                model=model,
                status="success",
                tokens_used=300 + (i * 50),  # Simulate different token usage
                duration_ms=1000 + (i * 500)  # Simulate different response times
            )
            logs.append(log)
        
        # Verify model diversity
        assert len(set(log.model for log in logs)) == 3
        
        # Verify performance patterns
        for i in range(len(logs)):
            assert logs[i].model == models_tested[i]
            assert logs[i].tokens_used == 300 + (i * 50)
    
    def test_llm_log_group_isolation(self):
        """Test LLMLog group isolation pattern."""
        # Team A logs
        team_a_logs = [
            LLMLog(
                endpoint="generate-agent",
                prompt="Create agent for team A",
                response="Team A agent created",
                model="gpt-4",
                status="success",
                group_id="team_a",
                group_email="a@company.com"
            ),
            LLMLog(
                endpoint="generate-task",
                prompt="Create task for team A",
                response="Team A task created",
                model="gpt-4",
                status="success",
                group_id="team_a",
                group_email="a@company.com"
            )
        ]
        
        # Team B logs
        team_b_logs = [
            LLMLog(
                endpoint="generate-crew",
                prompt="Create crew for team B",
                response="Team B crew created",
                model="gpt-3.5-turbo",
                status="success",
                group_id="team_b",
                group_email="b@company.com"
            )
        ]
        
        # Verify group isolation
        for log in team_a_logs:
            assert log.group_id == "team_a"
            assert log.group_email == "a@company.com"
        
        for log in team_b_logs:
            assert log.group_id == "team_b"
            assert log.group_email == "b@company.com"
    
    def test_llm_log_endpoint_categorization(self):
        """Test LLMLog endpoint categorization."""
        endpoints = [
            "generate-agent",
            "generate-crew", 
            "generate-task",
            "generate-workflow",
            "generate-template"
        ]
        
        logs = []
        for endpoint in endpoints:
            log = LLMLog(
                endpoint=endpoint,
                prompt=f"Test prompt for {endpoint}",
                response=f"Test response for {endpoint}",
                model="gpt-4",
                status="success"
            )
            logs.append(log)
        
        # Verify endpoint diversity
        endpoint_set = set(log.endpoint for log in logs)
        assert len(endpoint_set) == len(endpoints)
        
        # Verify endpoint categories
        generation_endpoints = [log.endpoint for log in logs if "generate" in log.endpoint]
        assert len(generation_endpoints) == len(endpoints)
    
    def test_llm_log_migration_compatibility(self):
        """Test LLMLog migration compatibility between tenant and group fields."""
        # Legacy tenant-based log
        tenant_log = LLMLog(
            endpoint="generate-agent",
            prompt="Legacy tenant prompt",
            response="Legacy response",
            model="gpt-4",
            status="success",
            tenant_id="tenant_123",
            tenant_email="user@tenant.com"
        )
        
        # New group-based log
        group_log = LLMLog(
            endpoint="generate-crew",
            prompt="New group prompt",
            response="New response",
            model="gpt-4",
            status="success",
            group_id="group_456",
            group_email="user@group.com"
        )
        
        # Verify both can coexist
        assert tenant_log.tenant_id == "tenant_123"
        assert tenant_log.group_id is None
        
        assert group_log.group_id == "group_456"
        assert group_log.tenant_id is None
    
    def test_llm_log_extra_data_patterns(self):
        """Test LLMLog extra_data usage patterns."""
        # Configuration tracking
        config_log = LLMLog(
            endpoint="generate-agent",
            prompt="Create agent with custom config",
            response="Agent created",
            model="gpt-4",
            status="success",
            extra_data={
                "temperature": 0.8,
                "max_tokens": 1500,
                "top_p": 0.9,
                "frequency_penalty": 0.2,
                "presence_penalty": 0.1
            }
        )
        
        # Request metadata tracking
        metadata_log = LLMLog(
            endpoint="generate-workflow",
            prompt="Create workflow",
            response="Workflow created",
            model="gpt-4",
            status="success",
            extra_data={
                "user_id": "user_123",
                "session_id": "session_456", 
                "request_source": "web_ui",
                "feature_flags": ["new_ui", "beta_features"],
                "api_version": "v2.1"
            }
        )
        
        # Error context tracking
        error_log = LLMLog(
            endpoint="generate-crew",
            prompt="Invalid crew prompt",
            response="Error response",
            model="gpt-4",
            status="error",
            error_message="Validation failed",
            extra_data={
                "error_code": "VALIDATION_001",
                "error_category": "input_validation",
                "retry_count": 2,
                "original_request_id": "req_789",
                "validation_errors": ["missing_role", "invalid_tool"]
            }
        )
        
        # Verify extra_data content
        assert config_log.extra_data["temperature"] == 0.8
        assert metadata_log.extra_data["api_version"] == "v2.1"
        assert error_log.extra_data["error_code"] == "VALIDATION_001"
        assert "missing_role" in error_log.extra_data["validation_errors"]