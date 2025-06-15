"""
Unit tests for schedule models.

Tests the functionality of Schedule model including
cron expressions, scheduling logic, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from src.models.schedule import Schedule


class TestSchedule:
    """Test cases for Schedule model."""
    
    def test_schedule_creation(self):
        """Test basic Schedule creation."""
        agents_config = [{"name": "agent1", "role": "analyst"}]
        tasks_config = [{"name": "task1", "description": "analyze data"}]
        
        schedule = Schedule(
            name="Daily Analysis",
            cron_expression="0 9 * * *",
            agents_yaml=agents_config,
            tasks_yaml=tasks_config
        )
        
        assert schedule.name == "Daily Analysis"
        assert schedule.cron_expression == "0 9 * * *"
        assert schedule.agents_yaml == agents_config
        assert schedule.tasks_yaml == tasks_config
    
    def test_schedule_required_fields(self):
        """Test Schedule with required fields only."""
        agents_config = [{"name": "agent1"}]
        tasks_config = [{"name": "task1"}]
        
        schedule = Schedule(
            name="Basic Schedule",
            cron_expression="0 0 * * *",
            agents_yaml=agents_config,
            tasks_yaml=tasks_config
        )
        
        assert schedule.name == "Basic Schedule"
        assert schedule.cron_expression == "0 0 * * *"
        assert schedule.agents_yaml == agents_config
        assert schedule.tasks_yaml == tasks_config
        
        # Check defaults
        assert schedule.inputs == {}
        assert schedule.is_active is True
        assert schedule.planning is False
        assert schedule.model == "gpt-4o-mini"
        assert schedule.last_run_at is None
        assert schedule.next_run_at is None
    
    def test_schedule_with_all_fields(self):
        """Test Schedule creation with all fields."""
        agents_config = [{"name": "agent1", "role": "analyst", "tools": ["sql", "python"]}]
        tasks_config = [{"name": "task1", "description": "comprehensive analysis"}]
        inputs_data = {"data_source": "database", "format": "json"}
        last_run = datetime(2023, 1, 1, 9, 0, 0)
        next_run = datetime(2023, 1, 2, 9, 0, 0)
        
        schedule = Schedule(
            name="Comprehensive Schedule",
            cron_expression="0 9 * * MON-FRI",
            agents_yaml=agents_config,
            tasks_yaml=tasks_config,
            inputs=inputs_data,
            is_active=True,
            planning=True,
            model="gpt-4",
            last_run_at=last_run,
            next_run_at=next_run
        )
        
        assert schedule.name == "Comprehensive Schedule"
        assert schedule.cron_expression == "0 9 * * MON-FRI"
        assert schedule.inputs == inputs_data
        assert schedule.is_active is True
        assert schedule.planning is True
        assert schedule.model == "gpt-4"
        assert schedule.last_run_at == last_run
        assert schedule.next_run_at == next_run
    
    def test_schedule_defaults(self):
        """Test Schedule default values."""
        schedule = Schedule(
            name="Default Schedule",
            cron_expression="0 12 * * *",
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.inputs == {}
        assert schedule.is_active is True
        assert schedule.planning is False
        assert schedule.model == "gpt-4o-mini"
        assert schedule.last_run_at is None
        assert schedule.next_run_at is None
    
    def test_schedule_timestamps(self):
        """Test Schedule timestamp fields."""
        with patch('src.models.schedule.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            schedule = Schedule(
                name="Timestamp Schedule",
                cron_expression="0 0 * * *",
                agents_yaml=[],
                tasks_yaml=[]
            )
            
            assert schedule.created_at == mock_now
            assert schedule.updated_at == mock_now
    
    def test_schedule_group_fields(self):
        """Test Schedule group-related fields."""
        schedule = Schedule(
            name="Group Schedule",
            cron_expression="0 6 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            group_id="group_123",
            created_by_email="user@group.com"
        )
        
        assert schedule.group_id == "group_123"
        assert schedule.created_by_email == "user@group.com"
    
    def test_schedule_group_fields(self):
        """Test Schedule group fields."""
        schedule = Schedule(
            name="Group Schedule",
            cron_expression="0 18 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            group_id="group_456"
        )
        
        assert schedule.group_id == "group_456"
    
    def test_schedule_inactive(self):
        """Test Schedule with is_active=False."""
        schedule = Schedule(
            name="Inactive Schedule",
            cron_expression="0 0 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            is_active=False
        )
        
        assert schedule.is_active is False
    
    def test_schedule_planning_enabled(self):
        """Test Schedule with planning enabled."""
        schedule = Schedule(
            name="Planning Schedule",
            cron_expression="0 8 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            planning=True,
            model="gpt-4-turbo"
        )
        
        assert schedule.planning is True
        assert schedule.model == "gpt-4-turbo"


class TestScheduleCronExpressions:
    """Test cases for Schedule cron expressions."""
    
    def test_schedule_daily_cron(self):
        """Test Schedule with daily cron expression."""
        schedule = Schedule(
            name="Daily Schedule",
            cron_expression="0 9 * * *",  # Every day at 9 AM
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.cron_expression == "0 9 * * *"
    
    def test_schedule_weekly_cron(self):
        """Test Schedule with weekly cron expression."""
        schedule = Schedule(
            name="Weekly Schedule",
            cron_expression="0 10 * * MON",  # Every Monday at 10 AM
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.cron_expression == "0 10 * * MON"
    
    def test_schedule_monthly_cron(self):
        """Test Schedule with monthly cron expression."""
        schedule = Schedule(
            name="Monthly Schedule",
            cron_expression="0 0 1 * *",  # First day of every month at midnight
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.cron_expression == "0 0 1 * *"
    
    def test_schedule_complex_cron(self):
        """Test Schedule with complex cron expression."""
        schedule = Schedule(
            name="Complex Schedule",
            cron_expression="30 14 * * MON,WED,FRI",  # 2:30 PM on Mon, Wed, Fri
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.cron_expression == "30 14 * * MON,WED,FRI"
    
    def test_schedule_hourly_cron(self):
        """Test Schedule with hourly cron expression."""
        schedule = Schedule(
            name="Hourly Schedule",
            cron_expression="0 * * * *",  # Every hour
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.cron_expression == "0 * * * *"


class TestScheduleAgentsAndTasks:
    """Test cases for Schedule agents and tasks configuration."""
    
    def test_schedule_simple_agents_config(self):
        """Test Schedule with simple agents configuration."""
        agents_config = [
            {"name": "data_analyst", "role": "Data Analyst"},
            {"name": "reporter", "role": "Report Generator"}
        ]
        
        schedule = Schedule(
            name="Simple Agents Schedule",
            cron_expression="0 9 * * *",
            agents_yaml=agents_config,
            tasks_yaml=[]
        )
        
        assert len(schedule.agents_yaml) == 2
        assert schedule.agents_yaml[0]["name"] == "data_analyst"
        assert schedule.agents_yaml[1]["role"] == "Report Generator"
    
    def test_schedule_complex_agents_config(self):
        """Test Schedule with complex agents configuration."""
        agents_config = [
            {
                "name": "senior_analyst",
                "role": "Senior Data Analyst",
                "goal": "Analyze complex datasets and identify trends",
                "backstory": "Expert with 10+ years in data analysis",
                "tools": ["sql_query", "python_analysis", "visualization"],
                "llm": "gpt-4",
                "max_iter": 5,
                "memory": True,
                "verbose": True
            }
        ]
        
        schedule = Schedule(
            name="Complex Agents Schedule",
            cron_expression="0 8 * * *",
            agents_yaml=agents_config,
            tasks_yaml=[]
        )
        
        agent = schedule.agents_yaml[0]
        assert agent["name"] == "senior_analyst"
        assert agent["goal"] == "Analyze complex datasets and identify trends"
        assert "sql_query" in agent["tools"]
        assert agent["max_iter"] == 5
        assert agent["memory"] is True
    
    def test_schedule_simple_tasks_config(self):
        """Test Schedule with simple tasks configuration."""
        tasks_config = [
            {"name": "extract_data", "description": "Extract data from source"},
            {"name": "analyze_data", "description": "Analyze extracted data"}
        ]
        
        schedule = Schedule(
            name="Simple Tasks Schedule",
            cron_expression="0 10 * * *",
            agents_yaml=[],
            tasks_yaml=tasks_config
        )
        
        assert len(schedule.tasks_yaml) == 2
        assert schedule.tasks_yaml[0]["name"] == "extract_data"
        assert schedule.tasks_yaml[1]["description"] == "Analyze extracted data"
    
    def test_schedule_complex_tasks_config(self):
        """Test Schedule with complex tasks configuration."""
        tasks_config = [
            {
                "name": "comprehensive_analysis",
                "description": "Perform comprehensive data analysis with reporting",
                "expected_output": "Detailed analysis report with visualizations",
                "agent": "senior_analyst",
                "tools": ["sql_query", "python_analysis"],
                "async_execution": False,
                "context": ["previous_reports", "data_dictionary"],
                "output_file": "analysis_report.pdf",
                "human_input": False
            }
        ]
        
        schedule = Schedule(
            name="Complex Tasks Schedule",
            cron_expression="0 7 * * *",
            agents_yaml=[],
            tasks_yaml=tasks_config
        )
        
        task = schedule.tasks_yaml[0]
        assert task["name"] == "comprehensive_analysis"
        assert task["expected_output"] == "Detailed analysis report with visualizations"
        assert task["agent"] == "senior_analyst"
        assert task["async_execution"] is False
        assert "previous_reports" in task["context"]
    
    def test_schedule_empty_agents_and_tasks(self):
        """Test Schedule with empty agents and tasks."""
        schedule = Schedule(
            name="Empty Schedule",
            cron_expression="0 0 * * *",
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        assert schedule.agents_yaml == []
        assert schedule.tasks_yaml == []
        assert len(schedule.agents_yaml) == 0
        assert len(schedule.tasks_yaml) == 0


class TestScheduleInputsAndConfiguration:
    """Test cases for Schedule inputs and configuration."""
    
    def test_schedule_simple_inputs(self):
        """Test Schedule with simple inputs."""
        inputs = {"source": "database", "format": "json"}
        
        schedule = Schedule(
            name="Simple Inputs Schedule",
            cron_expression="0 11 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            inputs=inputs
        )
        
        assert schedule.inputs == inputs
        assert schedule.inputs["source"] == "database"
        assert schedule.inputs["format"] == "json"
    
    def test_schedule_complex_inputs(self):
        """Test Schedule with complex inputs."""
        inputs = {
            "data_sources": [
                {"type": "database", "connection": "postgres://..."},
                {"type": "api", "endpoint": "https://api.example.com"}
            ],
            "parameters": {
                "date_range": "last_30_days",
                "filters": ["active_users", "premium_accounts"],
                "output_format": "detailed_report"
            },
            "notifications": {
                "email": ["admin@company.com"],
                "slack": "#data-team"
            }
        }
        
        schedule = Schedule(
            name="Complex Inputs Schedule",
            cron_expression="0 6 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            inputs=inputs
        )
        
        assert len(schedule.inputs["data_sources"]) == 2
        assert schedule.inputs["parameters"]["date_range"] == "last_30_days"
        assert "admin@company.com" in schedule.inputs["notifications"]["email"]
    
    def test_schedule_model_configurations(self):
        """Test Schedule with different model configurations."""
        models = ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo", "gpt-4o-mini"]
        
        for model in models:
            schedule = Schedule(
                name=f"Schedule with {model}",
                cron_expression="0 12 * * *",
                agents_yaml=[],
                tasks_yaml=[],
                model=model,
                planning=True
            )
            
            assert schedule.model == model
            assert schedule.planning is True
    
    def test_schedule_planning_configurations(self):
        """Test Schedule planning configurations."""
        # Planning disabled
        no_planning = Schedule(
            name="No Planning Schedule",
            cron_expression="0 13 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            planning=False
        )
        
        # Planning enabled
        with_planning = Schedule(
            name="With Planning Schedule",
            cron_expression="0 14 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            planning=True,
            model="gpt-4"
        )
        
        assert no_planning.planning is False
        assert with_planning.planning is True
        assert with_planning.model == "gpt-4"


class TestScheduleFieldTypes:
    """Test cases for Schedule field types and constraints."""
    
    def test_schedule_field_existence(self):
        """Test that all expected fields exist."""
        schedule = Schedule(
            name="Field Test Schedule",
            cron_expression="0 15 * * *",
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        # Check field existence
        assert hasattr(schedule, 'id')
        assert hasattr(schedule, 'name')
        assert hasattr(schedule, 'cron_expression')
        assert hasattr(schedule, 'agents_yaml')
        assert hasattr(schedule, 'tasks_yaml')
        assert hasattr(schedule, 'inputs')
        assert hasattr(schedule, 'is_active')
        assert hasattr(schedule, 'planning')
        assert hasattr(schedule, 'model')
        assert hasattr(schedule, 'last_run_at')
        assert hasattr(schedule, 'next_run_at')
        assert hasattr(schedule, 'created_at')
        assert hasattr(schedule, 'updated_at')
        assert hasattr(schedule, 'group_id')
        assert hasattr(schedule, 'created_by_email')
    
    def test_schedule_string_fields(self):
        """Test string field types."""
        schedule = Schedule(
            name="String Fields Schedule",
            cron_expression="0 16 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            model="gpt-4",
            group_id="group_123",
            created_by_email="user@test.com"
        )
        
        assert isinstance(schedule.name, str)
        assert isinstance(schedule.cron_expression, str)
        assert isinstance(schedule.model, str)
        assert isinstance(schedule.group_id, str)
        assert isinstance(schedule.created_by_email, str)
    
    def test_schedule_json_fields(self):
        """Test JSON field types."""
        agents = [{"name": "agent1"}]
        tasks = [{"name": "task1"}]
        inputs = {"key": "value"}
        
        schedule = Schedule(
            name="JSON Fields Schedule",
            cron_expression="0 17 * * *",
            agents_yaml=agents,
            tasks_yaml=tasks,
            inputs=inputs
        )
        
        assert isinstance(schedule.agents_yaml, list)
        assert isinstance(schedule.tasks_yaml, list)
        assert isinstance(schedule.inputs, dict)
    
    def test_schedule_boolean_fields(self):
        """Test boolean field types."""
        schedule = Schedule(
            name="Boolean Fields Schedule",
            cron_expression="0 18 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            is_active=True,
            planning=False
        )
        
        assert isinstance(schedule.is_active, bool)
        assert isinstance(schedule.planning, bool)
        assert schedule.is_active is True
        assert schedule.planning is False
    
    def test_schedule_datetime_fields(self):
        """Test datetime field types."""
        last_run = datetime(2023, 1, 1, 10, 0, 0)
        next_run = datetime(2023, 1, 1, 11, 0, 0)
        
        schedule = Schedule(
            name="DateTime Fields Schedule",
            cron_expression="0 19 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            last_run_at=last_run,
            next_run_at=next_run
        )
        
        assert isinstance(schedule.last_run_at, datetime)
        assert isinstance(schedule.next_run_at, datetime)
        assert isinstance(schedule.created_at, datetime)
        assert isinstance(schedule.updated_at, datetime)
    
    def test_schedule_nullable_fields(self):
        """Test nullable field behavior."""
        schedule = Schedule(
            name="Nullable Fields Schedule",
            cron_expression="0 20 * * *",
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        # These fields should be nullable
        assert schedule.last_run_at is None
        assert schedule.next_run_at is None
        assert schedule.group_id is None
        assert schedule.created_by_email is None


class TestScheduleIndexes:
    """Test cases for Schedule table indexes."""
    
    def test_schedule_table_args_defined(self):
        """Test that table args with indexes are defined."""
        assert hasattr(Schedule, '__table_args__')
        assert Schedule.__table_args__ is not None
    
    def test_schedule_indexes_exist(self):
        """Test that expected indexes are defined."""
        table_args = Schedule.__table_args__
        
        # Convert to list of index names for easier testing
        index_names = []
        for arg in table_args:
            if hasattr(arg, 'name'):
                index_names.append(arg.name)
        
        # Check that expected indexes exist
        expected_indexes = [
            'ix_schedule_group_id',
            'ix_schedule_created_by_email'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names


class TestScheduleUsagePatterns:
    """Test cases for common Schedule usage patterns."""
    
    def test_schedule_workflow_automation(self):
        """Test Schedule for workflow automation."""
        workflow_schedule = Schedule(
            name="Daily ETL Workflow",
            cron_expression="0 2 * * *",  # 2 AM daily
            agents_yaml=[
                {
                    "name": "etl_agent",
                    "role": "ETL Specialist",
                    "tools": ["sql_query", "data_transformation", "file_writer"]
                }
            ],
            tasks_yaml=[
                {
                    "name": "extract_data",
                    "description": "Extract data from source systems",
                    "agent": "etl_agent"
                },
                {
                    "name": "transform_data", 
                    "description": "Transform and clean data",
                    "agent": "etl_agent"
                },
                {
                    "name": "load_data",
                    "description": "Load data into warehouse",
                    "agent": "etl_agent"
                }
            ],
            inputs={"source_db": "production", "target_db": "warehouse"},
            is_active=True,
            planning=False
        )
        
        assert workflow_schedule.name == "Daily ETL Workflow"
        assert len(workflow_schedule.tasks_yaml) == 3
        assert workflow_schedule.inputs["source_db"] == "production"
        assert workflow_schedule.is_active is True
    
    def test_schedule_group_isolation(self):
        """Test Schedule group isolation pattern."""
        # Team A schedule
        team_a_schedule = Schedule(
            name="Team A Daily Reports",
            cron_expression="0 9 * * MON-FRI",
            agents_yaml=[{"name": "team_a_reporter"}],
            tasks_yaml=[{"name": "generate_team_a_report"}],
            group_id="team_a",
            created_by_email="lead@teama.com"
        )
        
        # Team B schedule
        team_b_schedule = Schedule(
            name="Team B Weekly Analysis",
            cron_expression="0 8 * * MON",
            agents_yaml=[{"name": "team_b_analyst"}],
            tasks_yaml=[{"name": "weekly_analysis"}],
            group_id="team_b",
            created_by_email="manager@teamb.com"
        )
        
        # Verify group isolation
        assert team_a_schedule.group_id == "team_a"
        assert team_a_schedule.created_by_email == "lead@teama.com"
        assert team_b_schedule.group_id == "team_b"
        assert team_b_schedule.created_by_email == "manager@teamb.com"
    
    def test_schedule_execution_tracking(self):
        """Test Schedule execution tracking."""
        last_execution = datetime(2023, 1, 1, 9, 0, 0)
        next_execution = datetime(2023, 1, 2, 9, 0, 0)
        
        schedule = Schedule(
            name="Tracked Schedule",
            cron_expression="0 9 * * *",
            agents_yaml=[{"name": "tracking_agent"}],
            tasks_yaml=[{"name": "tracked_task"}],
            last_run_at=last_execution,
            next_run_at=next_execution
        )
        
        assert schedule.last_run_at == last_execution
        assert schedule.next_run_at == next_execution
        
        # Simulate execution update
        new_last_run = datetime(2023, 1, 2, 9, 0, 0)
        new_next_run = datetime(2023, 1, 3, 9, 0, 0)
        
        schedule.last_run_at = new_last_run
        schedule.next_run_at = new_next_run
        
        assert schedule.last_run_at == new_last_run
        assert schedule.next_run_at == new_next_run
    
    def test_schedule_active_inactive_management(self):
        """Test Schedule active/inactive management."""
        # Active schedule
        active_schedule = Schedule(
            name="Active Schedule",
            cron_expression="0 10 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            is_active=True
        )
        
        # Inactive schedule
        inactive_schedule = Schedule(
            name="Inactive Schedule",
            cron_expression="0 11 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            is_active=False
        )
        
        assert active_schedule.is_active is True
        assert inactive_schedule.is_active is False
        
        # Toggle activation
        active_schedule.is_active = False
        inactive_schedule.is_active = True
        
        assert active_schedule.is_active is False
        assert inactive_schedule.is_active is True
    
    def test_schedule_migration_compatibility(self):
        """Test Schedule migration compatibility between tenant and group fields."""
        # Schedule without group assignment
        tenant_schedule = Schedule(
            name="Unassigned Schedule",
            cron_expression="0 12 * * *",
            agents_yaml=[],
            tasks_yaml=[]
        )
        
        # New group-based schedule
        group_schedule = Schedule(
            name="New Group Schedule",
            cron_expression="0 13 * * *",
            agents_yaml=[],
            tasks_yaml=[],
            group_id="group_456",
            created_by_email="user@group.com"
        )
        
        # Verify group-based scheduling
        assert tenant_schedule.group_id is None
        
        assert group_schedule.group_id == "group_456"
    
    def test_schedule_planning_workflow(self):
        """Test Schedule with planning workflow."""
        planning_schedule = Schedule(
            name="Planning Enabled Workflow",
            cron_expression="0 7 * * *",
            agents_yaml=[
                {
                    "name": "planner_agent",
                    "role": "Workflow Planner",
                    "tools": ["planning_tool", "analysis_tool"]
                }
            ],
            tasks_yaml=[
                {
                    "name": "plan_workflow",
                    "description": "Create execution plan for workflow"
                }
            ],
            planning=True,
            model="gpt-4-turbo",
            inputs={"planning_horizon": "1_week", "constraints": ["budget", "resources"]}
        )
        
        assert planning_schedule.planning is True
        assert planning_schedule.model == "gpt-4-turbo"
        assert planning_schedule.inputs["planning_horizon"] == "1_week"
        assert "budget" in planning_schedule.inputs["constraints"]