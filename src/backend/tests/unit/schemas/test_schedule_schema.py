"""
Unit tests for schedule schemas.

Tests the functionality of Pydantic schemas for schedule operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.schedule import (
    ScheduleBase, ScheduleCreate, ScheduleCreateFromExecution,
    ScheduleUpdate, ScheduleResponse, ScheduleListResponse,
    ToggleResponse, CrewConfig
)


class TestScheduleBase:
    """Test cases for ScheduleBase schema."""
    
    def test_valid_schedule_base_minimal(self):
        """Test ScheduleBase with minimal required fields."""
        data = {
            "name": "test-schedule",
            "cron_expression": "0 9 * * MON-FRI",
            "agents_yaml": {"agent1": {"role": "analyst"}},
            "tasks_yaml": {"task1": {"description": "Analysis task"}}
        }
        schedule = ScheduleBase(**data)
        assert schedule.name == "test-schedule"
        assert schedule.cron_expression == "0 9 * * MON-FRI"
        assert schedule.agents_yaml == {"agent1": {"role": "analyst"}}
        assert schedule.tasks_yaml == {"task1": {"description": "Analysis task"}}
        assert schedule.inputs == {}  # Default
        assert schedule.is_active is True  # Default
        assert schedule.planning is False  # Default
        assert schedule.model == "gpt-4o-mini"  # Default

    def test_valid_schedule_base_full(self):
        """Test ScheduleBase with all fields specified."""
        data = {
            "name": "full-schedule",
            "cron_expression": "0 */6 * * *",
            "agents_yaml": {
                "agent1": {"role": "researcher", "goal": "research topics"},
                "agent2": {"role": "writer", "goal": "write reports"}
            },
            "tasks_yaml": {
                "task1": {"description": "Research task", "agent": "agent1"},
                "task2": {"description": "Writing task", "agent": "agent2"}
            },
            "inputs": {"topic": "AI trends", "format": "report"},
            "is_active": False,
            "planning": True,
            "model": "gpt-4"
        }
        schedule = ScheduleBase(**data)
        assert schedule.name == "full-schedule"
        assert schedule.cron_expression == "0 */6 * * *"
        assert len(schedule.agents_yaml) == 2
        assert len(schedule.tasks_yaml) == 2
        assert schedule.inputs == {"topic": "AI trends", "format": "report"}
        assert schedule.is_active is False
        assert schedule.planning is True
        assert schedule.model == "gpt-4"

    def test_schedule_base_missing_required_fields(self):
        """Test ScheduleBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ScheduleBase(name="test-schedule")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "cron_expression" in missing_fields
        assert "agents_yaml" in missing_fields
        assert "tasks_yaml" in missing_fields

    def test_schedule_base_empty_yaml_fields(self):
        """Test ScheduleBase with empty YAML dictionaries."""
        data = {
            "name": "empty-schedule",
            "cron_expression": "0 12 * * *",
            "agents_yaml": {},
            "tasks_yaml": {}
        }
        schedule = ScheduleBase(**data)
        assert schedule.agents_yaml == {}
        assert schedule.tasks_yaml == {}

    def test_schedule_base_boolean_conversions(self):
        """Test ScheduleBase boolean field conversions."""
        data = {
            "name": "bool-schedule",
            "cron_expression": "0 8 * * *",
            "agents_yaml": {"agent": {"role": "test"}},
            "tasks_yaml": {"task": {"description": "test"}},
            "is_active": "false",
            "planning": 1
        }
        schedule = ScheduleBase(**data)
        assert schedule.is_active is False
        assert schedule.planning is True


class TestScheduleCreate:
    """Test cases for ScheduleCreate schema."""
    
    def test_schedule_create_inheritance(self):
        """Test that ScheduleCreate inherits from ScheduleBase."""
        data = {
            "name": "create-schedule",
            "cron_expression": "0 10 * * *",
            "agents_yaml": {"agent": {"role": "creator"}},
            "tasks_yaml": {"task": {"description": "creation task"}}
        }
        create_schedule = ScheduleCreate(**data)
        
        # Should have all base class attributes
        assert hasattr(create_schedule, 'name')
        assert hasattr(create_schedule, 'cron_expression')
        assert hasattr(create_schedule, 'agents_yaml')
        assert hasattr(create_schedule, 'tasks_yaml')
        assert hasattr(create_schedule, 'inputs')
        assert hasattr(create_schedule, 'is_active')
        assert hasattr(create_schedule, 'planning')
        assert hasattr(create_schedule, 'model')
        
        # Should behave like base class
        assert create_schedule.name == "create-schedule"
        assert create_schedule.cron_expression == "0 10 * * *"
        assert create_schedule.is_active is True  # Default

    def test_schedule_create_with_custom_values(self):
        """Test ScheduleCreate with custom values."""
        data = {
            "name": "custom-create-schedule",
            "cron_expression": "30 14 * * FRI",
            "agents_yaml": {"custom_agent": {"role": "custom"}},
            "tasks_yaml": {"custom_task": {"description": "custom task"}},
            "inputs": {"custom_input": "value"},
            "planning": True,
            "model": "claude-3"
        }
        create_schedule = ScheduleCreate(**data)
        assert create_schedule.name == "custom-create-schedule"
        assert create_schedule.inputs == {"custom_input": "value"}
        assert create_schedule.planning is True
        assert create_schedule.model == "claude-3"


class TestScheduleCreateFromExecution:
    """Test cases for ScheduleCreateFromExecution schema."""
    
    def test_valid_schedule_create_from_execution(self):
        """Test ScheduleCreateFromExecution with all fields."""
        data = {
            "name": "execution-schedule",
            "cron_expression": "0 16 * * *",
            "execution_id": 123,
            "is_active": False
        }
        schedule = ScheduleCreateFromExecution(**data)
        assert schedule.name == "execution-schedule"
        assert schedule.cron_expression == "0 16 * * *"
        assert schedule.execution_id == 123
        assert schedule.is_active is False

    def test_schedule_create_from_execution_defaults(self):
        """Test ScheduleCreateFromExecution with default values."""
        data = {
            "name": "default-execution-schedule",
            "cron_expression": "0 18 * * *",
            "execution_id": 456
        }
        schedule = ScheduleCreateFromExecution(**data)
        assert schedule.name == "default-execution-schedule"
        assert schedule.execution_id == 456
        assert schedule.is_active is True  # Default

    def test_schedule_create_from_execution_missing_fields(self):
        """Test ScheduleCreateFromExecution validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ScheduleCreateFromExecution(
                name="incomplete-schedule",
                cron_expression="0 20 * * *"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "execution_id" in missing_fields

    def test_execution_id_type_validation(self):
        """Test execution_id field type validation."""
        data = {
            "name": "type-test-schedule",
            "cron_expression": "0 22 * * *",
            "execution_id": "123"  # String that can be converted to int
        }
        schedule = ScheduleCreateFromExecution(**data)
        assert schedule.execution_id == 123
        assert isinstance(schedule.execution_id, int)


class TestScheduleUpdate:
    """Test cases for ScheduleUpdate schema."""
    
    def test_schedule_update_inheritance(self):
        """Test that ScheduleUpdate inherits from ScheduleBase."""
        data = {
            "name": "update-schedule",
            "cron_expression": "0 11 * * *",
            "agents_yaml": {"updated_agent": {"role": "updater"}},
            "tasks_yaml": {"updated_task": {"description": "update task"}}
        }
        update_schedule = ScheduleUpdate(**data)
        
        # Should have all base class attributes
        assert hasattr(update_schedule, 'name')
        assert hasattr(update_schedule, 'cron_expression')
        assert hasattr(update_schedule, 'agents_yaml')
        assert hasattr(update_schedule, 'tasks_yaml')
        assert hasattr(update_schedule, 'inputs')
        assert hasattr(update_schedule, 'is_active')
        assert hasattr(update_schedule, 'planning')
        assert hasattr(update_schedule, 'model')
        
        # Should behave like base class
        assert update_schedule.name == "update-schedule"
        assert update_schedule.cron_expression == "0 11 * * *"

    def test_schedule_update_partial_fields(self):
        """Test ScheduleUpdate with partial field updates."""
        data = {
            "name": "partial-update-schedule",
            "is_active": False,
            "model": "gpt-3.5-turbo"
        }
        # Note: ScheduleUpdate inherits from ScheduleBase, so required fields are still required
        # This test would need to include all required fields from ScheduleBase
        with pytest.raises(ValidationError):
            ScheduleUpdate(**data)


class TestScheduleResponse:
    """Test cases for ScheduleResponse schema."""
    
    def test_valid_schedule_response(self):
        """Test ScheduleResponse with all required fields."""
        now = datetime.now()
        next_run = datetime(2023, 12, 25, 9, 0, 0)
        
        data = {
            "id": 1,
            "name": "response-schedule",
            "cron_expression": "0 9 * * *",
            "agents_yaml": {"response_agent": {"role": "responder"}},
            "tasks_yaml": {"response_task": {"description": "response task"}},
            "last_run_at": now,
            "next_run_at": next_run,
            "created_at": now,
            "updated_at": now
        }
        response = ScheduleResponse(**data)
        assert response.id == 1
        assert response.name == "response-schedule"
        assert response.cron_expression == "0 9 * * *"
        assert response.last_run_at == now
        assert response.next_run_at == next_run
        assert response.created_at == now
        assert response.updated_at == now
        
        # Should inherit all base class defaults
        assert response.inputs == {}
        assert response.is_active is True
        assert response.planning is False
        assert response.model == "gpt-4o-mini"

    def test_schedule_response_config(self):
        """Test ScheduleResponse model config."""
        assert hasattr(ScheduleResponse, 'model_config')
        assert ScheduleResponse.model_config['from_attributes'] is True

    def test_schedule_response_missing_fields(self):
        """Test ScheduleResponse validation with missing fields."""
        now = datetime.now()
        with pytest.raises(ValidationError) as exc_info:
            ScheduleResponse(
                name="incomplete-schedule",
                cron_expression="0 9 * * *",
                agents_yaml={},
                tasks_yaml={},
                created_at=now,
                updated_at=now
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields

    def test_schedule_response_optional_timestamps(self):
        """Test ScheduleResponse with optional timestamp fields."""
        now = datetime.now()
        data = {
            "id": 2,
            "name": "optional-schedule",
            "cron_expression": "0 12 * * *",
            "agents_yaml": {"agent": {"role": "test"}},
            "tasks_yaml": {"task": {"description": "test"}},
            "created_at": now,
            "updated_at": now
        }
        response = ScheduleResponse(**data)
        assert response.id == 2
        assert response.last_run_at is None
        assert response.next_run_at is None

    def test_schedule_response_datetime_conversion(self):
        """Test ScheduleResponse with datetime string conversion."""
        data = {
            "id": 3,
            "name": "datetime-schedule",
            "cron_expression": "0 15 * * *",
            "agents_yaml": {"agent": {"role": "datetime"}},
            "tasks_yaml": {"task": {"description": "datetime test"}},
            "last_run_at": "2023-01-01T12:00:00",
            "next_run_at": "2023-01-02T12:00:00",
            "created_at": "2023-01-01T10:00:00",
            "updated_at": "2023-01-01T11:00:00"
        }
        response = ScheduleResponse(**data)
        assert response.id == 3
        assert isinstance(response.last_run_at, datetime)
        assert isinstance(response.next_run_at, datetime)
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestScheduleListResponse:
    """Test cases for ScheduleListResponse schema."""
    
    def test_valid_schedule_list_response(self):
        """Test ScheduleListResponse with all fields."""
        now = datetime.now()
        schedules = [
            ScheduleResponse(
                id=1,
                name="schedule-1",
                cron_expression="0 9 * * *",
                agents_yaml={"agent1": {"role": "agent1"}},
                tasks_yaml={"task1": {"description": "task1"}},
                created_at=now,
                updated_at=now
            ),
            ScheduleResponse(
                id=2,
                name="schedule-2",
                cron_expression="0 17 * * *",
                agents_yaml={"agent2": {"role": "agent2"}},
                tasks_yaml={"task2": {"description": "task2"}},
                created_at=now,
                updated_at=now
            )
        ]
        
        data = {
            "schedules": schedules,
            "count": 2
        }
        list_response = ScheduleListResponse(**data)
        
        assert len(list_response.schedules) == 2
        assert list_response.count == 2
        assert list_response.schedules[0].name == "schedule-1"
        assert list_response.schedules[1].name == "schedule-2"

    def test_empty_schedule_list_response(self):
        """Test ScheduleListResponse with empty schedule list."""
        data = {
            "schedules": [],
            "count": 0
        }
        list_response = ScheduleListResponse(**data)
        assert len(list_response.schedules) == 0
        assert list_response.count == 0

    def test_schedule_list_response_missing_fields(self):
        """Test ScheduleListResponse validation with missing fields."""
        now = datetime.now()
        schedules = [
            ScheduleResponse(
                id=1,
                name="test-schedule",
                cron_expression="0 9 * * *",
                agents_yaml={},
                tasks_yaml={},
                created_at=now,
                updated_at=now
            )
        ]
        
        with pytest.raises(ValidationError) as exc_info:
            ScheduleListResponse(schedules=schedules)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields


class TestToggleResponse:
    """Test cases for ToggleResponse schema."""
    
    def test_toggle_response_inheritance(self):
        """Test that ToggleResponse inherits from ScheduleResponse."""
        now = datetime.now()
        data = {
            "id": 5,
            "name": "toggle-schedule",
            "cron_expression": "0 13 * * *",
            "agents_yaml": {"toggle_agent": {"role": "toggler"}},
            "tasks_yaml": {"toggle_task": {"description": "toggle task"}},
            "is_active": False,
            "created_at": now,
            "updated_at": now
        }
        toggle_response = ToggleResponse(**data)
        
        # Should have all ScheduleResponse attributes
        assert hasattr(toggle_response, 'id')
        assert hasattr(toggle_response, 'name')
        assert hasattr(toggle_response, 'cron_expression')
        assert hasattr(toggle_response, 'agents_yaml')
        assert hasattr(toggle_response, 'tasks_yaml')
        assert hasattr(toggle_response, 'is_active')
        assert hasattr(toggle_response, 'created_at')
        assert hasattr(toggle_response, 'updated_at')
        
        # Should behave like ScheduleResponse
        assert toggle_response.id == 5
        assert toggle_response.name == "toggle-schedule"
        assert toggle_response.is_active is False


class TestCrewConfig:
    """Test cases for CrewConfig schema."""
    
    def test_valid_crew_config_minimal(self):
        """Test CrewConfig with minimal required fields."""
        data = {
            "agents_yaml": {"agent": {"role": "worker"}},
            "tasks_yaml": {"task": {"description": "work task"}}
        }
        config = CrewConfig(**data)
        assert config.agents_yaml == {"agent": {"role": "worker"}}
        assert config.tasks_yaml == {"task": {"description": "work task"}}
        assert config.inputs == {}  # Default
        assert config.planning is False  # Default
        assert config.model == "gpt-4o-mini"  # Default

    def test_valid_crew_config_full(self):
        """Test CrewConfig with all fields specified."""
        data = {
            "agents_yaml": {
                "analyst": {"role": "data_analyst", "goal": "analyze data"},
                "reporter": {"role": "report_writer", "goal": "write reports"}
            },
            "tasks_yaml": {
                "analysis": {"description": "analyze the data", "agent": "analyst"},
                "reporting": {"description": "write the report", "agent": "reporter"}
            },
            "inputs": {"dataset": "sales_data.csv", "period": "Q4"},
            "planning": True,
            "model": "gpt-4"
        }
        config = CrewConfig(**data)
        assert len(config.agents_yaml) == 2
        assert len(config.tasks_yaml) == 2
        assert config.inputs == {"dataset": "sales_data.csv", "period": "Q4"}
        assert config.planning is True
        assert config.model == "gpt-4"

    def test_crew_config_missing_required_fields(self):
        """Test CrewConfig validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewConfig(agents_yaml={"agent": {"role": "test"}})
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tasks_yaml" in missing_fields

    def test_crew_config_empty_yaml_fields(self):
        """Test CrewConfig with empty YAML dictionaries."""
        data = {
            "agents_yaml": {},
            "tasks_yaml": {}
        }
        config = CrewConfig(**data)
        assert config.agents_yaml == {}
        assert config.tasks_yaml == {}

    def test_crew_config_boolean_and_string_defaults(self):
        """Test CrewConfig default values for boolean and string fields."""
        data = {
            "agents_yaml": {"agent": {"role": "default_test"}},
            "tasks_yaml": {"task": {"description": "default test task"}}
        }
        config = CrewConfig(**data)
        assert config.planning is False
        assert config.model == "gpt-4o-mini"
        assert isinstance(config.inputs, dict)
        assert len(config.inputs) == 0


class TestSchemaIntegration:
    """Integration tests for schedule schema interactions."""
    
    def test_schedule_creation_workflow(self):
        """Test complete schedule creation workflow."""
        # Create schedule
        create_data = {
            "name": "workflow-schedule",
            "cron_expression": "0 8 * * MON-FRI",
            "agents_yaml": {
                "researcher": {"role": "research_agent", "goal": "research topics"},
                "writer": {"role": "content_writer", "goal": "write articles"}
            },
            "tasks_yaml": {
                "research": {"description": "research latest trends", "agent": "researcher"},
                "writing": {"description": "write article based on research", "agent": "writer"}
            },
            "inputs": {"topic": "AI trends", "length": "2000 words"},
            "planning": True,
            "model": "gpt-4"
        }
        create_schedule = ScheduleCreate(**create_data)
        
        # Update schedule
        update_data = {
            "name": "updated-workflow-schedule",
            "cron_expression": "0 9 * * MON-FRI",
            "agents_yaml": create_data["agents_yaml"],
            "tasks_yaml": create_data["tasks_yaml"],
            "is_active": False,
            "model": "claude-3"
        }
        update_schedule = ScheduleUpdate(**update_data)
        
        # Simulate database entity
        now = datetime.now()
        next_run = datetime(2023, 12, 25, 9, 0, 0)
        
        db_data = {
            "id": 1,
            "name": update_data["name"],
            "cron_expression": update_data["cron_expression"],
            "agents_yaml": create_schedule.agents_yaml,
            "tasks_yaml": create_schedule.tasks_yaml,
            "inputs": create_schedule.inputs,
            "is_active": update_data["is_active"],
            "planning": create_schedule.planning,
            "model": update_data["model"],
            "next_run_at": next_run,
            "created_at": now,
            "updated_at": now
        }
        schedule_response = ScheduleResponse(**db_data)
        
        # Toggle response
        toggle_response = ToggleResponse(**db_data)
        
        # Verify the complete workflow
        assert create_schedule.name == "workflow-schedule"
        assert create_schedule.planning is True
        assert update_schedule.name == "updated-workflow-schedule"
        assert update_schedule.is_active is False
        assert schedule_response.id == 1
        assert schedule_response.name == "updated-workflow-schedule"
        assert schedule_response.is_active is False
        assert schedule_response.model == "claude-3"
        assert schedule_response.next_run_at == next_run
        assert toggle_response.id == 1

    def test_schedule_from_execution_workflow(self):
        """Test schedule creation from execution workflow."""
        # Create from execution
        from_execution_data = {
            "name": "execution-based-schedule",
            "cron_expression": "0 14 * * *",
            "execution_id": 789,
            "is_active": True
        }
        from_execution_schedule = ScheduleCreateFromExecution(**from_execution_data)
        
        # Simulate conversion to full schedule response
        now = datetime.now()
        response_data = {
            "id": 2,
            "name": from_execution_schedule.name,
            "cron_expression": from_execution_schedule.cron_expression,
            "agents_yaml": {"execution_agent": {"role": "executor"}},  # Would come from execution
            "tasks_yaml": {"execution_task": {"description": "execute task"}},  # Would come from execution
            "is_active": from_execution_schedule.is_active,
            "created_at": now,
            "updated_at": now
        }
        schedule_response = ScheduleResponse(**response_data)
        
        # Verify workflow
        assert from_execution_schedule.execution_id == 789
        assert from_execution_schedule.is_active is True
        assert schedule_response.id == 2
        assert schedule_response.name == "execution-based-schedule"
        assert schedule_response.is_active is True

    def test_crew_config_integration(self):
        """Test CrewConfig integration with schedule schemas."""
        # Create crew config
        crew_config = CrewConfig(
            agents_yaml={
                "data_analyst": {
                    "role": "Senior Data Analyst",
                    "goal": "Analyze business data and provide insights",
                    "backstory": "Expert in data analysis with 10 years experience"
                },
                "report_writer": {
                    "role": "Business Report Writer", 
                    "goal": "Create comprehensive business reports",
                    "backstory": "Professional writer specializing in business communication"
                }
            },
            tasks_yaml={
                "data_analysis": {
                    "description": "Analyze the provided dataset for trends and insights",
                    "agent": "data_analyst",
                    "expected_output": "Data analysis report with key findings"
                },
                "report_creation": {
                    "description": "Create a business report based on the analysis",
                    "agent": "report_writer",
                    "expected_output": "Professional business report"
                }
            },
            inputs={"data_source": "quarterly_sales", "format": "executive_summary"},
            planning=True,
            model="gpt-4"
        )
        
        # Use crew config in schedule
        schedule_data = {
            "name": "business-analysis-schedule",
            "cron_expression": "0 6 1 * *",  # First day of every month at 6 AM
            "agents_yaml": crew_config.agents_yaml,
            "tasks_yaml": crew_config.tasks_yaml,
            "inputs": crew_config.inputs,
            "planning": crew_config.planning,
            "model": crew_config.model
        }
        schedule = ScheduleCreate(**schedule_data)
        
        # Verify integration
        assert len(schedule.agents_yaml) == 2
        assert len(schedule.tasks_yaml) == 2
        assert schedule.inputs["data_source"] == "quarterly_sales"
        assert schedule.planning is True
        assert schedule.model == "gpt-4"
        assert "data_analyst" in schedule.agents_yaml
        assert "report_writer" in schedule.agents_yaml
        assert schedule.tasks_yaml["data_analysis"]["agent"] == "data_analyst"
        assert schedule.tasks_yaml["report_creation"]["agent"] == "report_writer"

    def test_schedule_list_management(self):
        """Test schedule list management workflow."""
        now = datetime.now()
        
        # Create multiple schedules
        schedules = []
        for i in range(3):
            schedule_data = {
                "id": i + 1,
                "name": f"schedule-{i + 1}",
                "cron_expression": f"0 {8 + i} * * *",
                "agents_yaml": {f"agent{i + 1}": {"role": f"role{i + 1}"}},
                "tasks_yaml": {f"task{i + 1}": {"description": f"task {i + 1}"}},
                "is_active": i % 2 == 0,  # Alternate active/inactive
                "created_at": now,
                "updated_at": now
            }
            schedules.append(ScheduleResponse(**schedule_data))
        
        # Create list response
        list_response = ScheduleListResponse(
            schedules=schedules,
            count=len(schedules)
        )
        
        # Verify list management
        assert list_response.count == 3
        assert len(list_response.schedules) == 3
        assert list_response.schedules[0].name == "schedule-1"
        assert list_response.schedules[0].is_active is True
        assert list_response.schedules[1].is_active is False
        assert list_response.schedules[2].is_active is True
        
        # Test filtering active schedules
        active_schedules = [s for s in list_response.schedules if s.is_active]
        assert len(active_schedules) == 2
        assert active_schedules[0].name == "schedule-1"
        assert active_schedules[1].name == "schedule-3"