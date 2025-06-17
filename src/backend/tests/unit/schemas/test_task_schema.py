"""
Unit tests for task schemas.

Tests the functionality of Pydantic schemas for task operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import List, Dict, Any, Union

from src.schemas.task import (
    ConditionConfig, TaskConfig, TaskBase, TaskCreate, TaskUpdate,
    TaskInDBBase, Task, TaskResponse
)


class TestConditionConfig:
    """Test cases for ConditionConfig schema."""
    
    def test_valid_condition_config_minimal(self):
        """Test ConditionConfig with minimal required fields."""
        condition_data = {"type": "wait"}
        condition = ConditionConfig(**condition_data)
        assert condition.type == "wait"
        assert condition.parameters == {}
        assert condition.dependent_task is None
    
    def test_valid_condition_config_full(self):
        """Test ConditionConfig with all fields."""
        condition_data = {
            "type": "timeout",
            "parameters": {"duration": 300, "unit": "seconds"},
            "dependent_task": "task-123"
        }
        condition = ConditionConfig(**condition_data)
        assert condition.type == "timeout"
        assert condition.parameters == {"duration": 300, "unit": "seconds"}
        assert condition.dependent_task == "task-123"
    
    def test_condition_config_missing_type(self):
        """Test ConditionConfig validation with missing type."""
        with pytest.raises(ValidationError) as exc_info:
            ConditionConfig()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "type" in missing_fields
    
    def test_condition_config_various_types(self):
        """Test ConditionConfig with various condition types."""
        condition_types = [
            "wait", "timeout", "dependency", "manual", "schedule",
            "webhook", "api_call", "file_exists", "custom"
        ]
        
        for cond_type in condition_types:
            condition_data = {"type": cond_type}
            condition = ConditionConfig(**condition_data)
            assert condition.type == cond_type
    
    def test_condition_config_complex_parameters(self):
        """Test ConditionConfig with complex parameters."""
        complex_params = {
            "timeout": 300,
            "retries": 3,
            "nested": {"key": "value", "list": [1, 2, 3]},
            "boolean": True,
            "null_value": None
        }
        condition_data = {
            "type": "complex",
            "parameters": complex_params
        }
        condition = ConditionConfig(**condition_data)
        assert condition.parameters == complex_params


class TestTaskConfig:
    """Test cases for TaskConfig schema."""
    
    def test_valid_task_config_empty(self):
        """Test TaskConfig with all default values."""
        config = TaskConfig()
        assert config.cache_response is None
        assert config.cache_ttl is None
        assert config.retry_on_fail is None
        assert config.max_retries is None
        assert config.timeout is None
        assert config.priority is None
        assert config.error_handling is None
        assert config.output_file is None
        assert config.output_json is None
        assert config.output_pydantic is None
        assert config.callback is None
        assert config.human_input is None
        assert config.condition is None
        assert config.guardrail is None
        assert config.markdown is None
    
    def test_valid_task_config_full(self):
        """Test TaskConfig with all fields specified."""
        condition = ConditionConfig(type="timeout", parameters={"duration": 60})
        config_data = {
            "cache_response": True,
            "cache_ttl": 3600,
            "retry_on_fail": True,
            "max_retries": 5,
            "timeout": 1200,
            "priority": 1,
            "error_handling": "continue",
            "output_file": "output.txt",
            "output_json": "result.json",
            "output_pydantic": "TaskResult",
            "callback": "task_complete_callback",
            "human_input": True,
            "condition": condition,
            "guardrail": "safety_check",
            "markdown": True
        }
        config = TaskConfig(**config_data)
        assert config.cache_response is True
        assert config.cache_ttl == 3600
        assert config.retry_on_fail is True
        assert config.max_retries == 5
        assert config.timeout == 1200
        assert config.priority == 1
        assert config.error_handling == "continue"
        assert config.output_file == "output.txt"
        assert config.output_json == "result.json"
        assert config.output_pydantic == "TaskResult"
        assert config.callback == "task_complete_callback"
        assert config.human_input is True
        assert isinstance(config.condition, ConditionConfig)
        assert config.condition.type == "timeout"
        assert config.guardrail == "safety_check"
        assert config.markdown is True
    
    def test_task_config_partial_fields(self):
        """Test TaskConfig with partial fields."""
        config_data = {
            "cache_response": False,
            "max_retries": 3,
            "human_input": True
        }
        config = TaskConfig(**config_data)
        assert config.cache_response is False
        assert config.max_retries == 3
        assert config.human_input is True
        assert config.cache_ttl is None
        assert config.timeout is None
        assert config.priority is None
    
    def test_task_config_with_condition_dict(self):
        """Test TaskConfig with condition provided as dictionary."""
        config_data = {
            "condition": {
                "type": "dependency",
                "parameters": {"wait_for": "previous_task"},
                "dependent_task": "task-456"
            }
        }
        config = TaskConfig(**config_data)
        assert isinstance(config.condition, ConditionConfig)
        assert config.condition.type == "dependency"
        assert config.condition.parameters == {"wait_for": "previous_task"}
        assert config.condition.dependent_task == "task-456"


class TestTaskBase:
    """Test cases for TaskBase schema."""
    
    def test_valid_task_base_minimal(self):
        """Test TaskBase with minimal required fields."""
        task_data = {
            "name": "Test Task",
            "description": "A test task",
            "expected_output": "Test results"
        }
        task = TaskBase(**task_data)
        assert task.name == "Test Task"
        assert task.description == "A test task"
        assert task.expected_output == "Test results"
        assert task.agent_id is None
        assert task.tools == []
        assert task.async_execution is False
        assert task.context == []
        assert isinstance(task.config, TaskConfig)
        assert task.output_json is None
        assert task.output_pydantic is None
        assert task.output_file is None
        assert task.output is None
        assert task.markdown is False
        assert task.callback is None
        assert task.human_input is False
        assert task.converter_cls is None
        assert task.guardrail is None
    
    def test_valid_task_base_full(self):
        """Test TaskBase with all fields specified."""
        config = TaskConfig(priority=1, human_input=True)
        task_data = {
            "name": "Complex Task",
            "description": "A complex task with all options",
            "agent_id": "agent-123",
            "expected_output": "Comprehensive analysis report",
            "tools": ["pandas", "numpy", "matplotlib"],
            "async_execution": True,
            "context": ["task-001", "task-002"],
            "config": config,
            "output_json": "results.json",
            "output_pydantic": "AnalysisResult",
            "output_file": "report.pdf",
            "output": {"format": "json", "compression": "gzip"},
            "markdown": True,
            "callback": "analysis_complete",
            "human_input": True,
            "converter_cls": "AnalysisConverter",
            "guardrail": "data_privacy_check"
        }
        task = TaskBase(**task_data)
        assert task.name == "Complex Task"
        assert task.description == "A complex task with all options"
        assert task.agent_id == "agent-123"
        assert task.expected_output == "Comprehensive analysis report"
        assert task.tools == ["pandas", "numpy", "matplotlib"]
        assert task.async_execution is True
        assert task.context == ["task-001", "task-002"]
        assert isinstance(task.config, TaskConfig)
        assert task.config.priority == 1
        assert task.config.human_input is True
        assert task.output_json == "results.json"
        assert task.output_pydantic == "AnalysisResult"
        assert task.output_file == "report.pdf"
        assert task.output == {"format": "json", "compression": "gzip"}
        assert task.markdown is True
        assert task.callback == "analysis_complete"
        assert task.human_input is True
        assert task.converter_cls == "AnalysisConverter"
        assert task.guardrail == "data_privacy_check"
    
    def test_task_base_missing_required_fields(self):
        """Test TaskBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            TaskBase(name="Test Task")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "description" in missing_fields
        assert "expected_output" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            TaskBase(description="Test description")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
        assert "expected_output" in missing_fields
    
    def test_task_base_empty_strings(self):
        """Test TaskBase with empty strings for required fields."""
        task_data = {
            "name": "",
            "description": "",
            "expected_output": ""
        }
        task = TaskBase(**task_data)
        assert task.name == ""
        assert task.description == ""
        assert task.expected_output == ""
    
    def test_task_base_config_auto_creation(self):
        """Test that TaskBase automatically creates TaskConfig."""
        task_data = {
            "name": "Auto Config Task",
            "description": "Task with auto-created config",
            "expected_output": "Results"
        }
        task = TaskBase(**task_data)
        assert isinstance(task.config, TaskConfig)
        assert task.config.cache_response is None
        assert task.config.priority is None


class TestTaskCreate:
    """Test cases for TaskCreate schema."""
    
    def test_task_create_inheritance(self):
        """Test that TaskCreate inherits from TaskBase."""
        task_data = {
            "name": "Create Task",
            "description": "Task creation test",
            "expected_output": "Creation results"
        }
        task = TaskCreate(**task_data)
        
        # Should have all base class attributes
        assert hasattr(task, 'name')
        assert hasattr(task, 'description')
        assert hasattr(task, 'expected_output')
        assert hasattr(task, 'agent_id')
        assert hasattr(task, 'tools')
        assert hasattr(task, 'async_execution')
        assert hasattr(task, 'context')
        assert hasattr(task, 'config')
        
        # Should behave like base class
        assert task.name == "Create Task"
        assert task.description == "Task creation test"
        assert task.expected_output == "Creation results"
        assert task.agent_id is None
        assert task.tools == []
        assert isinstance(task.config, TaskConfig)
    
    def test_task_create_with_custom_config(self):
        """Test TaskCreate with custom configuration."""
        custom_config = TaskConfig(
            priority=2,
            timeout=300,
            human_input=True
        )
        task_data = {
            "name": "Custom Config Task",
            "description": "Task with custom configuration",
            "expected_output": "Custom results",
            "config": custom_config,
            "agent_id": "agent-456",
            "tools": ["custom_tool"],
            "async_execution": True
        }
        task = TaskCreate(**task_data)
        assert task.config.priority == 2
        assert task.config.timeout == 300
        assert task.config.human_input is True
        assert task.agent_id == "agent-456"
        assert task.tools == ["custom_tool"]
        assert task.async_execution is True


class TestTaskUpdate:
    """Test cases for TaskUpdate schema."""
    
    def test_task_update_all_optional(self):
        """Test that all TaskUpdate fields are optional."""
        update = TaskUpdate()
        assert update.name is None
        assert update.description is None
        assert update.agent_id is None
        assert update.expected_output is None
        assert update.tools is None
        assert update.async_execution is None
        assert update.context is None
        assert update.config is None
        assert update.output_json is None
        assert update.output_pydantic is None
        assert update.output_file is None
        assert update.output is None
        assert update.markdown is None
        assert update.callback is None
        assert update.human_input is None
        assert update.converter_cls is None
        assert update.guardrail is None
    
    def test_task_update_partial(self):
        """Test TaskUpdate with partial fields."""
        update_data = {
            "name": "Updated Task",
            "agent_id": "new-agent-789",
            "async_execution": True
        }
        update = TaskUpdate(**update_data)
        assert update.name == "Updated Task"
        assert update.agent_id == "new-agent-789"
        assert update.async_execution is True
        assert update.description is None
        assert update.expected_output is None
        assert update.tools is None
    
    def test_task_update_full(self):
        """Test TaskUpdate with all fields."""
        config = TaskConfig(max_retries=10, priority=3)
        update_data = {
            "name": "Fully Updated Task",
            "description": "Updated description",
            "agent_id": "updated-agent-999",
            "expected_output": "Updated expected output",
            "tools": ["updated_tool1", "updated_tool2"],
            "async_execution": False,
            "context": ["updated-context-1"],
            "config": config,
            "output_json": "updated.json",
            "output_pydantic": "UpdatedResult",
            "output_file": "updated.txt",
            "output": {"updated": True},
            "markdown": True,
            "callback": "updated_callback",
            "human_input": False,
            "converter_cls": "UpdatedConverter",
            "guardrail": "updated_guardrail"
        }
        update = TaskUpdate(**update_data)
        assert update.name == "Fully Updated Task"
        assert update.description == "Updated description"
        assert update.agent_id == "updated-agent-999"
        assert update.expected_output == "Updated expected output"
        assert update.tools == ["updated_tool1", "updated_tool2"]
        assert update.async_execution is False
        assert update.context == ["updated-context-1"]
        assert isinstance(update.config, TaskConfig)
        assert update.config.max_retries == 10
        assert update.config.priority == 3
        assert update.output_json == "updated.json"
        assert update.output_pydantic == "UpdatedResult"
        assert update.output_file == "updated.txt"
        assert update.output == {"updated": True}
        assert update.markdown is True
        assert update.callback == "updated_callback"
        assert update.human_input is False
        assert update.converter_cls == "UpdatedConverter"
        assert update.guardrail == "updated_guardrail"


class TestTaskInDBBase:
    """Test cases for TaskInDBBase schema."""
    
    def test_valid_task_in_db_base(self):
        """Test TaskInDBBase with all required fields."""
        now = datetime.now()
        task_data = {
            "id": "task-123",
            "name": "DB Task",
            "description": "Task in database",
            "expected_output": "DB results",
            "created_at": now,
            "updated_at": now
        }
        task = TaskInDBBase(**task_data)
        assert task.id == "task-123"
        assert task.name == "DB Task"
        assert task.description == "Task in database"
        assert task.expected_output == "DB results"
        assert task.created_at == now
        assert task.updated_at == now
        
        # Should inherit all base class defaults
        assert task.agent_id is None
        assert task.tools == []
        assert task.async_execution is False
        assert isinstance(task.config, TaskConfig)
    
    def test_task_in_db_base_config(self):
        """Test TaskInDBBase model configuration."""
        assert hasattr(TaskInDBBase, 'model_config')
        assert TaskInDBBase.model_config['from_attributes'] is True
    
    def test_task_in_db_base_missing_fields(self):
        """Test TaskInDBBase validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            TaskInDBBase(
                name="Test Task",
                description="Test description",
                expected_output="Test output"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields
    
    def test_task_in_db_base_datetime_conversion(self):
        """Test TaskInDBBase with datetime string conversion."""
        task_data = {
            "id": "task-456",
            "name": "DateTime Task",
            "description": "Task with datetime strings",
            "expected_output": "DateTime results",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        task = TaskInDBBase(**task_data)
        assert task.id == "task-456"
        assert isinstance(task.created_at, datetime)
        assert isinstance(task.updated_at, datetime)


class TestTask:
    """Test cases for Task schema."""
    
    def test_task_inheritance(self):
        """Test that Task inherits from TaskInDBBase."""
        now = datetime.now()
        task_data = {
            "id": "task-789",
            "name": "Full Task",
            "description": "Complete task object",
            "expected_output": "Full results",
            "created_at": now,
            "updated_at": now
        }
        task = Task(**task_data)
        
        # Should have all TaskInDBBase attributes
        assert hasattr(task, 'id')
        assert hasattr(task, 'created_at')
        assert hasattr(task, 'updated_at')
        
        # Should have all TaskBase attributes
        assert hasattr(task, 'name')
        assert hasattr(task, 'description')
        assert hasattr(task, 'expected_output')
        assert hasattr(task, 'agent_id')
        assert hasattr(task, 'tools')
        assert hasattr(task, 'config')
        
        # Verify values
        assert task.id == "task-789"
        assert task.name == "Full Task"
        assert task.description == "Complete task object"
        assert task.expected_output == "Full results"
        assert task.created_at == now
        assert task.updated_at == now


class TestTaskResponse:
    """Test cases for TaskResponse (backward compatibility alias)."""
    
    def test_task_response_alias(self):
        """Test that TaskResponse is an alias for Task."""
        now = datetime.now()
        task_data = {
            "id": "response-task-1",
            "name": "Response Task",
            "description": "Task response test",
            "expected_output": "Response results",
            "created_at": now,
            "updated_at": now
        }
        
        # Both should create the same type of object
        task = Task(**task_data)
        task_response = TaskResponse(**task_data)
        
        assert type(task) == type(task_response)
        assert task.id == task_response.id
        assert task.name == task_response.name
        assert task.description == task_response.description
        
        # Verify they are actually the same class
        assert TaskResponse is Task


class TestSchemaIntegration:
    """Integration tests for task schema interactions."""
    
    def test_task_lifecycle_workflow(self):
        """Test complete task lifecycle workflow."""
        # Create task
        create_data = {
            "name": "Lifecycle Task",
            "description": "Testing complete lifecycle",
            "expected_output": "Lifecycle results",
            "agent_id": "agent-lifecycle",
            "tools": ["tool1", "tool2"],
            "config": TaskConfig(priority=1, timeout=600)
        }
        create_schema = TaskCreate(**create_data)
        
        # Update task
        update_data = {
            "name": "Updated Lifecycle Task",
            "agent_id": "agent-updated",
            "async_execution": True,
            "config": TaskConfig(priority=2, max_retries=5)
        }
        update_schema = TaskUpdate(**update_data)
        
        # Database entity (simulating what would come from database)
        now = datetime.now()
        db_data = {
            "id": "lifecycle-task-1",
            "name": update_data["name"],  # Updated name
            "description": create_schema.description,  # Original description
            "expected_output": create_schema.expected_output,  # Original output
            "agent_id": update_data["agent_id"],  # Updated agent
            "tools": create_schema.tools,  # Original tools
            "async_execution": update_data["async_execution"],  # Updated execution
            "config": update_data["config"],  # Updated config
            "created_at": now,
            "updated_at": now
        }
        task_response = Task(**db_data)
        
        # Verify the complete workflow
        assert create_schema.name == "Lifecycle Task"
        assert create_schema.config.priority == 1
        assert update_schema.name == "Updated Lifecycle Task"
        assert update_schema.config.priority == 2
        assert task_response.id == "lifecycle-task-1"
        assert task_response.name == "Updated Lifecycle Task"  # From update
        assert task_response.description == "Testing complete lifecycle"  # From creation
        assert task_response.agent_id == "agent-updated"  # From update
        assert task_response.async_execution is True  # From update
        assert task_response.config.priority == 2  # From update
    
    def test_task_configuration_scenarios(self):
        """Test different task configuration scenarios."""
        # Simple task
        simple_task = TaskCreate(
            name="Simple Task",
            description="A simple task",
            expected_output="Simple results"
        )
        assert simple_task.async_execution is False
        assert simple_task.human_input is False
        assert isinstance(simple_task.config, TaskConfig)
        assert simple_task.config.priority is None
        
        # Complex task with conditions
        condition = ConditionConfig(
            type="dependency",
            parameters={"wait_for": ["task-1", "task-2"]},
            dependent_task="task-3"
        )
        complex_config = TaskConfig(
            priority=1,
            timeout=1800,
            retry_on_fail=True,
            max_retries=3,
            condition=condition,
            human_input=True
        )
        complex_task = TaskCreate(
            name="Complex Task",
            description="A complex task with conditions",
            expected_output="Complex analysis results",
            agent_id="specialist-agent",
            tools=["analysis_tool", "reporting_tool"],
            async_execution=True,
            context=["context-data-1", "context-data-2"],
            config=complex_config,
            human_input=True,
            guardrail="safety_check"
        )
        assert complex_task.config.priority == 1
        assert complex_task.config.condition.type == "dependency"
        assert complex_task.async_execution is True
        assert complex_task.human_input is True
        assert complex_task.guardrail == "safety_check"
        
        # Output-focused task
        output_task = TaskCreate(
            name="Output Task",
            description="Task with specific output requirements",
            expected_output="Formatted report",
            output_json="report.json",
            output_pydantic="ReportModel",
            output_file="report.pdf",
            output={"format": "pdf", "template": "standard"},
            markdown=True,
            callback="report_ready_callback"
        )
        assert output_task.output_json == "report.json"
        assert output_task.output_pydantic == "ReportModel"
        assert output_task.output_file == "report.pdf"
        assert output_task.output == {"format": "pdf", "template": "standard"}
        assert output_task.markdown is True
        assert output_task.callback == "report_ready_callback"