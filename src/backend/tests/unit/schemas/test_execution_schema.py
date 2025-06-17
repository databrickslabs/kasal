"""
Unit tests for execution schemas.

Tests the functionality of Pydantic schemas for execution operations
including validation, serialization, and field constraints.
"""
import pytest
import json
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.execution import (
    ExecutionNameGenerationRequest, ExecutionNameGenerationResponse,
    CrewConfig, ExecutionBase, ExecutionResponse, ExecutionCreateResponse,
    FlowConfig
)


class TestExecutionNameGenerationRequest:
    """Test cases for ExecutionNameGenerationRequest schema."""
    
    def test_valid_execution_name_generation_request_minimal(self):
        """Test ExecutionNameGenerationRequest with minimal required fields."""
        request_data = {
            "agents_yaml": {
                "agent1": {"role": "analyst", "goal": "analyze data"}
            },
            "tasks_yaml": {
                "task1": {"description": "process data", "agent": "agent1"}
            }
        }
        request = ExecutionNameGenerationRequest(**request_data)
        assert request.agents_yaml == {"agent1": {"role": "analyst", "goal": "analyze data"}}
        assert request.tasks_yaml == {"task1": {"description": "process data", "agent": "agent1"}}
        assert request.model is None
    
    def test_valid_execution_name_generation_request_full(self):
        """Test ExecutionNameGenerationRequest with all fields."""
        request_data = {
            "agents_yaml": {
                "data_analyst": {
                    "role": "Senior Data Analyst",
                    "goal": "Analyze and interpret complex datasets",
                    "backstory": "Expert in statistical analysis"
                },
                "report_writer": {
                    "role": "Report Writer", 
                    "goal": "Create comprehensive reports",
                    "backstory": "Skilled in technical writing"
                }
            },
            "tasks_yaml": {
                "analysis_task": {
                    "description": "Perform statistical analysis on the dataset",
                    "agent": "data_analyst",
                    "expected_output": "Statistical summary"
                },
                "report_task": {
                    "description": "Write a comprehensive report",
                    "agent": "report_writer",
                    "expected_output": "Final report"
                }
            },
            "model": "databricks-llama-4-maverick"
        }
        request = ExecutionNameGenerationRequest(**request_data)
        assert len(request.agents_yaml) == 2
        assert len(request.tasks_yaml) == 2
        assert request.model == "databricks-llama-4-maverick"
        assert "data_analyst" in request.agents_yaml
        assert "analysis_task" in request.tasks_yaml
    
    def test_execution_name_generation_request_missing_fields(self):
        """Test ExecutionNameGenerationRequest validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionNameGenerationRequest(agents_yaml={"agent1": {}})
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tasks_yaml" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ExecutionNameGenerationRequest(tasks_yaml={"task1": {}})
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "agents_yaml" in missing_fields
    
    def test_execution_name_generation_request_empty_configs(self):
        """Test ExecutionNameGenerationRequest with empty configurations."""
        request_data = {
            "agents_yaml": {},
            "tasks_yaml": {}
        }
        request = ExecutionNameGenerationRequest(**request_data)
        assert request.agents_yaml == {}
        assert request.tasks_yaml == {}
    
    def test_execution_name_generation_request_complex_configs(self):
        """Test ExecutionNameGenerationRequest with complex configurations."""
        request_data = {
            "agents_yaml": {
                "senior_researcher": {
                    "role": "Senior Research Analyst",
                    "goal": "Conduct thorough research on market trends",
                    "backstory": "10+ years of market research experience",
                    "tools": ["web_search", "data_analysis", "report_generator"],
                    "max_iter": 5,
                    "memory": True,
                    "verbose": True
                }
            },
            "tasks_yaml": {
                "market_research": {
                    "description": "Research current market trends in AI technology",
                    "agent": "senior_researcher",
                    "expected_output": "Comprehensive market analysis report",
                    "tools": ["web_search", "data_analysis"],
                    "async_execution": False,
                    "context": ["previous_research", "industry_reports"]
                }
            },
            "model": "claude-3-sonnet"
        }
        request = ExecutionNameGenerationRequest(**request_data)
        
        # Verify complex structure preservation
        assert request.agents_yaml["senior_researcher"]["tools"] == ["web_search", "data_analysis", "report_generator"]
        assert request.tasks_yaml["market_research"]["context"] == ["previous_research", "industry_reports"]
        assert request.model == "claude-3-sonnet"


class TestExecutionNameGenerationResponse:
    """Test cases for ExecutionNameGenerationResponse schema."""
    
    def test_valid_execution_name_generation_response(self):
        """Test ExecutionNameGenerationResponse with valid data."""
        response_data = {"name": "Data Analysis Pipeline Q4 2023"}
        response = ExecutionNameGenerationResponse(**response_data)
        assert response.name == "Data Analysis Pipeline Q4 2023"
    
    def test_execution_name_generation_response_missing_name(self):
        """Test ExecutionNameGenerationResponse validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionNameGenerationResponse()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_execution_name_generation_response_empty_name(self):
        """Test ExecutionNameGenerationResponse with empty name."""
        response_data = {"name": ""}
        response = ExecutionNameGenerationResponse(**response_data)
        assert response.name == ""
    
    def test_execution_name_generation_response_various_names(self):
        """Test ExecutionNameGenerationResponse with various name formats."""
        name_formats = [
            "Simple Name",
            "Complex_Name_With_Underscores",
            "Name with Numbers 123",
            "Name-with-dashes",
            "Very Long Name That Describes A Complex Multi-Step Data Processing Pipeline",
            "名前",  # Japanese
            "Nombre con acentos: áéíóú",  # Spanish with accents
            "Name with symbols: @#$%"
        ]
        
        for name in name_formats:
            response_data = {"name": name}
            response = ExecutionNameGenerationResponse(**response_data)
            assert response.name == name


class TestCrewConfig:
    """Test cases for CrewConfig schema."""
    
    def test_valid_crew_config_minimal(self):
        """Test CrewConfig with minimal required fields."""
        config_data = {
            "agents_yaml": {
                "agent1": {"role": "worker", "goal": "work"}
            },
            "tasks_yaml": {
                "task1": {"description": "do work", "agent": "agent1"}
            },
            "inputs": {"data": "test_data"}
        }
        config = CrewConfig(**config_data)
        assert config.agents_yaml == {"agent1": {"role": "worker", "goal": "work"}}
        assert config.tasks_yaml == {"task1": {"description": "do work", "agent": "agent1"}}
        assert config.inputs == {"data": "test_data"}
        assert config.planning is False  # Default
        assert config.reasoning is False  # Default
        assert config.execution_type == "crew"  # Default
        assert config.schema_detection_enabled is True  # Default
    
    def test_valid_crew_config_full(self):
        """Test CrewConfig with all fields."""
        config_data = {
            "agents_yaml": {
                "analyst": {"role": "Data Analyst", "goal": "Analyze data"}
            },
            "tasks_yaml": {
                "analysis": {"description": "Analyze dataset", "agent": "analyst"}
            },
            "inputs": {"dataset": "sales_data.csv", "period": "Q4"},
            "planning": True,
            "reasoning": True,
            "model": "databricks-llama-4-maverick",
            "llm_provider": "databricks",
            "execution_type": "crew",
            "schema_detection_enabled": False
        }
        config = CrewConfig(**config_data)
        assert config.planning is True
        assert config.reasoning is True
        assert config.model == "databricks-llama-4-maverick"
        assert config.llm_provider == "databricks"
        assert config.execution_type == "crew"
        assert config.schema_detection_enabled is False
    
    def test_crew_config_missing_required_fields(self):
        """Test CrewConfig validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewConfig(agents_yaml={}, tasks_yaml={})
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "inputs" in missing_fields
    
    def test_crew_config_tasks_property(self):
        """Test CrewConfig tasks property with dict values."""
        # Test with dict values (standard format)
        config_data = {
            "agents_yaml": {"agent1": {"role": "worker"}},
            "tasks_yaml": {
                "task1": {"description": "work", "agent": "agent1"}
            },
            "inputs": {}
        }
        config = CrewConfig(**config_data)
        tasks = config.tasks
        assert tasks == {"task1": {"description": "work", "agent": "agent1"}}
        
        # Test with nested dict structure
        config_data["tasks_yaml"] = {
            "task1": {"description": "work", "agent": "agent1", "tools": ["tool1"]}
        }
        config = CrewConfig(**config_data)
        tasks = config.tasks
        assert tasks == {"task1": {"description": "work", "agent": "agent1", "tools": ["tool1"]}}
    
    def test_crew_config_agents_property(self):
        """Test CrewConfig agents property with dict values."""
        # Test with dict values (standard format)
        config_data = {
            "agents_yaml": {"agent1": {"role": "worker", "goal": "work"}},
            "tasks_yaml": {"task1": {"description": "work"}},
            "inputs": {}
        }
        config = CrewConfig(**config_data)
        agents = config.agents
        assert agents == {"agent1": {"role": "worker", "goal": "work"}}
        
        # Test with complex agent structure
        config_data["agents_yaml"] = {
            "agent1": {"role": "worker", "goal": "work", "tools": ["tool1"], "memory": True}
        }
        config = CrewConfig(**config_data)
        agents = config.agents
        assert agents == {"agent1": {"role": "worker", "goal": "work", "tools": ["tool1"], "memory": True}}
    
    def test_crew_config_validation_errors(self):
        """Test CrewConfig validation errors for invalid input types."""
        # Test invalid agents_yaml type
        with pytest.raises(ValidationError) as exc_info:
            CrewConfig(
                agents_yaml="not a dict",
                tasks_yaml={"task1": {"description": "work"}},
                inputs={}
            )
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "dict_type" for error in errors)
        
        # Test invalid tasks_yaml type
        with pytest.raises(ValidationError) as exc_info:
            CrewConfig(
                agents_yaml={"agent1": {"role": "worker"}},
                tasks_yaml="not a dict",
                inputs={}
            )
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "dict_type" for error in errors)
        
        # Test invalid nested structure in agents_yaml
        with pytest.raises(ValidationError) as exc_info:
            CrewConfig(
                agents_yaml={"agent1": "should be dict"},
                tasks_yaml={"task1": {"description": "work"}},
                inputs={}
            )
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "dict_type" for error in errors)
    
    def test_crew_config_property_error_handling(self):
        """Test CrewConfig property error handling by bypassing validation."""
        # Create a valid config first
        config_data = {
            "agents_yaml": {"agent1": {"role": "worker"}},
            "tasks_yaml": {"task1": {"description": "work"}},
            "inputs": {}
        }
        config = CrewConfig(**config_data)
        
        # Test agents property with non-dict tasks_yaml by directly setting attribute
        config.tasks_yaml = "not a dict"
        with pytest.raises(ValueError, match="Tasks configuration must be a dictionary"):
            _ = config.tasks
        
        # Test agents property with non-dict agents_yaml by directly setting attribute  
        config.agents_yaml = "not a dict"
        with pytest.raises(ValueError, match="Agents configuration must be a dictionary"):
            _ = config.agents
        
        # Test JSON parsing error in tasks property
        config.tasks_yaml = {"task1": "invalid json {"}
        with pytest.raises(ValueError, match="Task configuration for task1 is not a valid JSON string"):
            _ = config.tasks
        
        # Test JSON parsing error in agents property
        config.agents_yaml = {"agent1": "invalid json {"}
        with pytest.raises(ValueError, match="Agent configuration for agent1 is not a valid JSON string"):
            _ = config.agents
        
        # Test valid JSON string parsing in tasks
        config.tasks_yaml = {"task1": '{"description": "work", "agent": "agent1"}'}
        tasks = config.tasks
        assert tasks == {"task1": {"description": "work", "agent": "agent1"}}
        
        # Test valid JSON string parsing in agents
        config.agents_yaml = {"agent1": '{"role": "worker", "goal": "work"}'}
        agents = config.agents
        assert agents == {"agent1": {"role": "worker", "goal": "work"}}
    
    def test_crew_config_llm_providers(self):
        """Test CrewConfig with various LLM providers."""
        providers = [
            "openai",
            "anthropic", 
            "databricks",
            "azure_openai",
            "google",
            "local"
        ]
        
        for provider in providers:
            config_data = {
                "agents_yaml": {"agent1": {"role": "worker"}},
                "tasks_yaml": {"task1": {"description": "work"}},
                "inputs": {},
                "llm_provider": provider
            }
            config = CrewConfig(**config_data)
            assert config.llm_provider == provider


class TestExecutionBase:
    """Test cases for ExecutionBase schema."""
    
    def test_valid_execution_base(self):
        """Test ExecutionBase with all required fields."""
        now = datetime.now()
        base_data = {
            "execution_id": "exec_12345",
            "status": "running",
            "created_at": now,
            "result": {"output": "test result"},
            "error": None,
            "run_name": "Test Execution"
        }
        execution = ExecutionBase(**base_data)
        assert execution.execution_id == "exec_12345"
        assert execution.status == "running"
        assert execution.created_at == now
        assert execution.result == {"output": "test result"}
        assert execution.error is None
        assert execution.run_name == "Test Execution"
    
    def test_execution_base_missing_required_fields(self):
        """Test ExecutionBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionBase(execution_id="test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "status" in missing_fields
        assert "created_at" in missing_fields
    
    def test_execution_base_optional_fields(self):
        """Test ExecutionBase with optional fields as None."""
        now = datetime.now()
        base_data = {
            "execution_id": "exec_minimal",
            "status": "pending",
            "created_at": now
        }
        execution = ExecutionBase(**base_data)
        assert execution.result is None
        assert execution.error is None
        assert execution.run_name is None
    
    def test_execution_base_various_statuses(self):
        """Test ExecutionBase with various status values."""
        statuses = ["pending", "running", "completed", "failed", "cancelled"]
        now = datetime.now()
        
        for status in statuses:
            base_data = {
                "execution_id": f"exec_{status}",
                "status": status,
                "created_at": now
            }
            execution = ExecutionBase(**base_data)
            assert execution.status == status


class TestExecutionResponse:
    """Test cases for ExecutionResponse schema."""
    
    def test_valid_execution_response_minimal(self):
        """Test ExecutionResponse with minimal required fields."""
        now = datetime.now()
        response_data = {
            "execution_id": "exec_response_123",
            "status": "completed",
            "created_at": now
        }
        response = ExecutionResponse(**response_data)
        assert response.execution_id == "exec_response_123"
        assert response.status == "completed"
        assert response.created_at == now
        # Check optional fields have default values
        assert response.id is None
        assert response.flow_id is None
        assert response.crew_id is None
    
    def test_valid_execution_response_full(self):
        """Test ExecutionResponse with all fields."""
        now = datetime.now()
        started = datetime(2023, 1, 1, 10, 0, 0)
        completed = datetime(2023, 1, 1, 10, 30, 0)
        
        response_data = {
            "execution_id": "exec_full_456",
            "status": "completed",
            "created_at": now,
            "result": {"final_output": "Analysis complete"},
            "error": None,
            "run_name": "Quarterly Analysis",
            "id": 789,
            "flow_id": 101,
            "crew_id": 202,
            "execution_key": "quarterly_2023_q4",
            "started_at": started,
            "completed_at": completed,
            "updated_at": now,
            "execution_inputs": {"dataset": "q4_sales.csv"},
            "execution_outputs": {"report": "q4_analysis.pdf"},
            "execution_config": {"model": "databricks-llama-4-maverick"},
            "group_email": "analyst@company.com"
        }
        response = ExecutionResponse(**response_data)
        
        assert response.execution_id == "exec_full_456"
        assert response.id == 789
        assert response.flow_id == 101
        assert response.crew_id == 202
        assert response.execution_key == "quarterly_2023_q4"
        assert response.started_at == started
        assert response.completed_at == completed
        assert response.execution_inputs == {"dataset": "q4_sales.csv"}
        assert response.execution_outputs == {"report": "q4_analysis.pdf"}
        assert response.group_email == "analyst@company.com"
    
    def test_execution_response_config(self):
        """Test ExecutionResponse model configuration."""
        # Verify model config is set correctly
        assert hasattr(ExecutionResponse, 'model_config')
        assert ExecutionResponse.model_config.get('from_attributes') is True
    
    def test_execution_response_datetime_conversion(self):
        """Test ExecutionResponse with datetime string conversion."""
        response_data = {
            "execution_id": "exec_datetime_test",
            "status": "running",
            "created_at": "2023-01-01T12:00:00",
            "started_at": "2023-01-01T12:05:00",
            "updated_at": "2023-01-01T12:10:00"
        }
        response = ExecutionResponse(**response_data)
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.started_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestExecutionCreateResponse:
    """Test cases for ExecutionCreateResponse schema."""
    
    def test_valid_execution_create_response(self):
        """Test ExecutionCreateResponse with valid data."""
        response_data = {
            "execution_id": "create_exec_123",
            "status": "pending",
            "run_name": "New Data Analysis"
        }
        response = ExecutionCreateResponse(**response_data)
        assert response.execution_id == "create_exec_123"
        assert response.status == "pending"
        assert response.run_name == "New Data Analysis"
    
    def test_execution_create_response_missing_required_fields(self):
        """Test ExecutionCreateResponse validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionCreateResponse(execution_id="test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "status" in missing_fields
    
    def test_execution_create_response_optional_run_name(self):
        """Test ExecutionCreateResponse with optional run_name."""
        response_data = {
            "execution_id": "create_exec_456",
            "status": "pending"
        }
        response = ExecutionCreateResponse(**response_data)
        assert response.run_name is None


class TestFlowConfig:
    """Test cases for FlowConfig schema."""
    
    def test_valid_flow_config_minimal(self):
        """Test FlowConfig with minimal required fields."""
        config_data = {"name": "Test Flow"}
        config = FlowConfig(**config_data)
        assert config.name == "Test Flow"
        assert config.listeners == []  # Default
        assert config.actions == []  # Default
        assert config.startingPoints == []  # Default
        assert config.execution_type == "flow"  # Default
        assert config.tools == []  # Default
        assert config.max_rpm == 10  # Default
        assert config.planning is False  # Default
        assert config.reasoning is False  # Default
    
    def test_valid_flow_config_full(self):
        """Test FlowConfig with all fields."""
        config_data = {
            "id": "flow_123",
            "name": "Complex Data Pipeline",
            "listeners": [
                {"event": "start", "action": "initialize"}
            ],
            "actions": [
                {"type": "data_load", "config": {"source": "database"}},
                {"type": "transform", "config": {"method": "normalize"}}
            ],
            "startingPoints": [
                {"node": "data_loader", "inputs": ["raw_data"]}
            ],
            "type": "sequential",
            "crewName": "Data Processing Crew",
            "crewRef": "crew_456",
            "model": "databricks-llama-4-maverick",
            "llm_provider": "databricks",
            "tools": [
                {"name": "sql_tool", "type": "database"},
                {"name": "pandas_tool", "type": "processing"}
            ],
            "max_rpm": 50,
            "output_dir": "/outputs/flow_123",
            "planning": True,
            "planning_llm": "gpt-4",
            "reasoning": True,
            "reasoning_llm": "claude-3-sonnet"
        }
        config = FlowConfig(**config_data)
        
        assert config.id == "flow_123"
        assert config.name == "Complex Data Pipeline"
        assert len(config.listeners) == 1
        assert len(config.actions) == 2
        assert len(config.startingPoints) == 1
        assert config.type == "sequential"
        assert config.crewName == "Data Processing Crew"
        assert config.crewRef == "crew_456"
        assert config.model == "databricks-llama-4-maverick"
        assert config.llm_provider == "databricks"
        assert len(config.tools) == 2
        assert config.max_rpm == 50
        assert config.output_dir == "/outputs/flow_123"
        assert config.planning is True
        assert config.planning_llm == "gpt-4"
        assert config.reasoning is True
        assert config.reasoning_llm == "claude-3-sonnet"
    
    def test_flow_config_missing_name(self):
        """Test FlowConfig validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_flow_config_normalize_method(self):
        """Test FlowConfig normalize method."""
        config_data = {
            "name": "Normalize Test Flow",
            "listeners": [{"event": "test"}],
            "actions": [{"type": "test_action"}],
            "model": "test_model"
        }
        config = FlowConfig(**config_data)
        normalized = config.normalize()
        
        # Check that normalize returns a dictionary
        assert isinstance(normalized, dict)
        
        # Check that all expected keys are present
        expected_keys = {
            "id", "name", "listeners", "actions", "startingPoints", "type",
            "crewName", "crewRef", "model", "llm_provider", "tools", "max_rpm",
            "output_dir", "planning", "planning_llm", "reasoning", "reasoning_llm"
        }
        assert set(normalized.keys()) == expected_keys
        
        # Check specific values
        assert normalized["name"] == "Normalize Test Flow"
        assert normalized["listeners"] == [{"event": "test"}]
        assert normalized["actions"] == [{"type": "test_action"}]
        assert normalized["model"] == "test_model"
        assert normalized["max_rpm"] == 10  # Default value
        
        # Check that ID is generated if not provided
        assert normalized["id"] is not None
        assert normalized["id"].startswith("flow-")
    
    def test_flow_config_normalize_with_id(self):
        """Test FlowConfig normalize method with provided ID."""
        config_data = {
            "id": "custom_flow_id",
            "name": "Custom ID Flow"
        }
        config = FlowConfig(**config_data)
        normalized = config.normalize()
        
        # Check that provided ID is preserved
        assert normalized["id"] == "custom_flow_id"
    
    def test_flow_config_execution_type_validation(self):
        """Test FlowConfig execution_type field."""
        config_data = {
            "name": "Test Flow",
            "execution_type": "custom_flow"
        }
        config = FlowConfig(**config_data)
        assert config.execution_type == "custom_flow"
        
        # Test default value
        config_data = {"name": "Default Flow"}
        config = FlowConfig(**config_data)
        assert config.execution_type == "flow"


class TestSchemaIntegration:
    """Integration tests for execution schema interactions."""
    
    def test_execution_workflow_crew(self):
        """Test complete execution workflow with crew configuration."""
        # Name generation request
        name_request = ExecutionNameGenerationRequest(
            agents_yaml={
                "analyst": {"role": "Data Analyst", "goal": "Analyze sales data"}
            },
            tasks_yaml={
                "analysis": {"description": "Analyze Q4 sales data", "agent": "analyst"}
            },
            model="databricks-llama-4-maverick"
        )
        
        # Name generation response
        name_response = ExecutionNameGenerationResponse(
            name="Q4 Sales Data Analysis Pipeline"
        )
        
        # Crew configuration
        crew_config = CrewConfig(
            agents_yaml=name_request.agents_yaml,
            tasks_yaml=name_request.tasks_yaml,
            inputs={"sales_data": "q4_sales.csv", "year": 2023},
            planning=True,
            model=name_request.model,
            llm_provider="databricks"
        )
        
        # Execution creation response
        create_response = ExecutionCreateResponse(
            execution_id="exec_q4_sales_001",
            status="pending",
            run_name=name_response.name
        )
        
        # Final execution response
        now = datetime.now()
        execution_response = ExecutionResponse(
            execution_id=create_response.execution_id,
            status="completed",
            created_at=now,
            result={"analysis_complete": True, "insights": ["insight1", "insight2"]},
            run_name=create_response.run_name,
            crew_id=123,
            execution_inputs=crew_config.inputs,
            execution_config={"model": crew_config.model, "planning": crew_config.planning},
            started_at=now,
            completed_at=now,
            group_email="analyst@company.com"
        )
        
        # Verify workflow
        assert name_request.model == crew_config.model
        assert name_response.name == create_response.run_name
        assert create_response.execution_id == execution_response.execution_id
        assert execution_response.crew_id == 123
        assert execution_response.execution_inputs == {"sales_data": "q4_sales.csv", "year": 2023}
        assert execution_response.result["analysis_complete"] is True
    
    def test_execution_workflow_flow(self):
        """Test complete execution workflow with flow configuration."""
        # Flow configuration
        flow_config = FlowConfig(
            name="Customer Analytics Flow",
            listeners=[
                {"event": "data_received", "trigger": "process_data"}
            ],
            actions=[
                {"type": "load_data", "source": "customer_db"},
                {"type": "analyze", "method": "clustering"},
                {"type": "report", "format": "dashboard"}
            ],
            startingPoints=[
                {"node": "data_loader"}
            ],
            model="claude-3-sonnet",
            llm_provider="anthropic",
            planning=True,
            reasoning=True,
            max_rpm=20
        )
        
        # Execution creation
        create_response = ExecutionCreateResponse(
            execution_id="exec_flow_customer_001",
            status="pending",
            run_name="Customer Analytics - Weekly Run"
        )
        
        # Execution response
        now = datetime.now()
        execution_response = ExecutionResponse(
            execution_id=create_response.execution_id,
            status="running",
            created_at=now,
            flow_id=456,
            execution_config=flow_config.normalize(),
            started_at=now,
            group_email="analytics@company.com"
        )
        
        # Verify flow workflow
        assert flow_config.execution_type == "flow"
        assert execution_response.flow_id == 456
        assert execution_response.execution_config["name"] == "Customer Analytics Flow"
        assert execution_response.execution_config["planning"] is True
        assert execution_response.execution_config["reasoning"] is True
        assert execution_response.execution_config["max_rpm"] == 20
    
    def test_execution_error_scenarios(self):
        """Test execution error scenarios."""
        now = datetime.now()
        
        # Failed execution
        failed_execution = ExecutionResponse(
            execution_id="exec_failed_001",
            status="failed",
            created_at=now,
            error="Model endpoint unavailable",
            started_at=now,
            completed_at=now,
            result=None
        )
        
        # Cancelled execution
        cancelled_execution = ExecutionResponse(
            execution_id="exec_cancelled_001", 
            status="cancelled",
            created_at=now,
            error="User cancelled execution",
            started_at=now,
            completed_at=now
        )
        
        # Verify error scenarios
        assert failed_execution.status == "failed"
        assert failed_execution.error == "Model endpoint unavailable"
        assert failed_execution.result is None
        
        assert cancelled_execution.status == "cancelled"
        assert cancelled_execution.error == "User cancelled execution"
    
    def test_execution_configuration_variations(self):
        """Test various execution configuration scenarios."""
        # Simple crew configuration
        simple_crew = CrewConfig(
            agents_yaml={"worker": {"role": "Worker"}},
            tasks_yaml={"work": {"description": "Do work"}},
            inputs={"data": "simple"}
        )
        
        # Advanced crew configuration
        advanced_crew = CrewConfig(
            agents_yaml={
                "senior_analyst": {
                    "role": "Senior Data Analyst",
                    "goal": "Provide deep insights",
                    "backstory": "Expert with 10+ years experience"
                },
                "junior_analyst": {
                    "role": "Junior Data Analyst", 
                    "goal": "Support analysis tasks",
                    "backstory": "Eager to learn and contribute"
                }
            },
            tasks_yaml={
                "data_collection": {
                    "description": "Collect and validate data",
                    "agent": "junior_analyst",
                    "expected_output": "Clean dataset"
                },
                "advanced_analysis": {
                    "description": "Perform statistical analysis",
                    "agent": "senior_analyst",
                    "expected_output": "Statistical insights",
                    "context": ["data_collection"]
                }
            },
            inputs={
                "raw_data": "sales_data.csv",
                "analysis_period": "2023-Q4",
                "confidence_level": 0.95
            },
            planning=True,
            reasoning=True,
            model="databricks-llama-4-maverick",
            llm_provider="databricks",
            schema_detection_enabled=True
        )
        
        # Simple flow configuration
        simple_flow = FlowConfig(name="Simple ETL")
        
        # Complex flow configuration
        complex_flow = FlowConfig(
            name="Advanced ML Pipeline",
            listeners=[
                {"event": "data_arrival", "action": "validate_schema"},
                {"event": "validation_complete", "action": "start_processing"}
            ],
            actions=[
                {"type": "extract", "config": {"sources": ["db1", "db2"]}},
                {"type": "transform", "config": {"operations": ["clean", "normalize"]}},
                {"type": "feature_engineering", "config": {"methods": ["pca", "scaling"]}},
                {"type": "model_training", "config": {"algorithm": "xgboost"}},
                {"type": "model_evaluation", "config": {"metrics": ["accuracy", "f1"]}},
                {"type": "deploy", "config": {"environment": "production"}}
            ],
            startingPoints=[
                {"node": "data_extractor", "schedule": "daily"}
            ],
            tools=[
                {"name": "pandas", "version": "2.0"},
                {"name": "scikit-learn", "version": "1.3"},
                {"name": "xgboost", "version": "1.7"}
            ],
            model="gpt-4",
            llm_provider="openai",
            planning=True,
            planning_llm="gpt-4",
            reasoning=True, 
            reasoning_llm="claude-3-sonnet",
            max_rpm=100,
            output_dir="/ml_pipeline/outputs"
        )
        
        # Verify configuration variations
        assert simple_crew.execution_type == "crew"
        assert len(simple_crew.agents) == 1
        
        assert advanced_crew.planning is True
        assert advanced_crew.reasoning is True
        assert len(advanced_crew.agents) == 2
        assert advanced_crew.inputs["confidence_level"] == 0.95
        
        assert simple_flow.execution_type == "flow"
        assert len(simple_flow.actions) == 0
        
        assert complex_flow.execution_type == "flow"
        assert len(complex_flow.actions) == 6
        assert len(complex_flow.tools) == 3
        assert complex_flow.max_rpm == 100
        assert complex_flow.planning_llm == "gpt-4"
        assert complex_flow.reasoning_llm == "claude-3-sonnet"