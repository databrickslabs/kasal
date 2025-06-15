"""
Unit tests for task models.

Tests the functionality of Task model including
initialization, configuration synchronization, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from uuid import uuid4

from src.models.task import Task, generate_uuid


class TestGenerateUuid:
    """Test cases for generate_uuid function."""
    
    def test_generate_uuid_returns_string(self):
        """Test that generate_uuid returns a string."""
        uuid_str = generate_uuid()
        assert isinstance(uuid_str, str)
    
    def test_generate_uuid_is_unique(self):
        """Test that generate_uuid returns unique values."""
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        assert uuid1 != uuid2
    
    def test_generate_uuid_is_valid_format(self):
        """Test that generate_uuid returns valid UUID format."""
        uuid_str = generate_uuid()
        # Should be able to create UUID from the string
        from uuid import UUID
        uuid_obj = UUID(uuid_str)
        assert str(uuid_obj) == uuid_str
    
    @patch('src.models.task.uuid4')
    def test_generate_uuid_calls_uuid4(self, mock_uuid4):
        """Test that generate_uuid calls uuid4."""
        mock_uuid = MagicMock()
        mock_uuid.__str__ = MagicMock(return_value="test-uuid")
        mock_uuid4.return_value = mock_uuid
        
        result = generate_uuid()
        
        mock_uuid4.assert_called_once()
        assert result == "test-uuid"


class TestTask:
    """Test cases for Task model."""
    
    def test_task_creation(self):
        """Test basic Task creation."""
        task = Task(
            name="Data Analysis Task",
            description="Analyze sales data and generate insights",
            expected_output="Comprehensive sales analysis report"
        )
        
        assert task.name == "Data Analysis Task"
        assert task.description == "Analyze sales data and generate insights"
        assert task.expected_output == "Comprehensive sales analysis report"
    
    def test_task_required_fields(self):
        """Test Task with required fields only."""
        task = Task(
            name="Simple Task",
            description="Simple task description",
            expected_output="Simple output"
        )
        
        assert task.name == "Simple Task"
        assert task.description == "Simple task description"
        assert task.expected_output == "Simple output"
        assert task.agent_id is None
        assert task.tools == []
        assert task.async_execution is False
        assert task.context == []
        assert task.config == {}
    
    def test_task_with_all_fields(self):
        """Test Task creation with all fields."""
        agent_id = "agent_123"
        tools = ["sql_query", "data_visualization"]
        context = ["previous_analysis", "data_dictionary"]
        config = {"timeout": 3600, "retry_count": 3}
        
        task = Task(
            name="Complete Task",
            description="Complete task with all fields",
            agent_id=agent_id,
            expected_output="Complete analysis with visualizations",
            tools=tools,
            async_execution=True,
            context=context,
            config=config,
            output_json="analysis_results.json",
            output_pydantic="AnalysisResult",
            output_file="report.pdf",
            markdown=True,
            callback="notify_completion",
            human_input=True,
            converter_cls="CustomConverter",
            guardrail="data_validation"
        )
        
        assert task.agent_id == agent_id
        assert task.tools == tools
        assert task.async_execution is True
        assert task.context == context
        assert task.config == config
        assert task.output_json == "analysis_results.json"
        assert task.output_pydantic == "AnalysisResult"
        assert task.output_file == "report.pdf"
        assert task.markdown is True
        assert task.callback == "notify_completion"
        assert task.human_input is True
        assert task.converter_cls == "CustomConverter"
        assert task.guardrail == "data_validation"
    
    def test_task_id_generation(self):
        """Test that Task id is automatically generated."""
        task = Task(
            name="ID Test Task",
            description="Test task for ID generation",
            expected_output="Test output"
        )
        
        assert task.id is not None
        assert isinstance(task.id, str)
    
    def test_task_custom_id(self):
        """Test Task with custom id."""
        custom_id = "custom_task_123"
        task = Task(
            id=custom_id,
            name="Custom ID Task",
            description="Task with custom ID",
            expected_output="Custom output"
        )
        
        assert task.id == custom_id
    
    def test_task_timestamps(self):
        """Test Task timestamp fields."""
        with patch('src.models.task.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            task = Task(
                name="Timestamp Task",
                description="Task for timestamp testing",
                expected_output="Timestamp output"
            )
            
            assert task.created_at == mock_now
            assert task.updated_at == mock_now
    
    def test_task_group_fields(self):
        """Test Task group-related fields."""
        task = Task(
            name="Group Task",
            description="Task for group testing",
            expected_output="Group output",
            group_id="group_123",
            created_by_email="user@group.com"
        )
        
        assert task.group_id == "group_123"
        assert task.created_by_email == "user@group.com"
    
    def test_task_legacy_tenant_fields(self):
        """Test Task legacy tenant fields - skip as tenant removed."""
        # Tenant concept has been removed from the codebase
        pass
    
    def test_task_tablename(self):
        """Test Task table name."""
        assert Task.__tablename__ == "tasks"


class TestTaskInitialization:
    """Test cases for Task __init__ method."""
    
    def test_task_init_tools_none_handling(self):
        """Test Task initialization when tools is None."""
        task = Task(
            name="Tools None Task",
            description="Task with tools=None",
            expected_output="Output",
            tools=None
        )
        
        assert task.tools == []
        assert isinstance(task.tools, list)
    
    def test_task_init_context_none_handling(self):
        """Test Task initialization when context is None."""
        task = Task(
            name="Context None Task",
            description="Task with context=None",
            expected_output="Output",
            context=None
        )
        
        assert task.context == []
        assert isinstance(task.context, list)
    
    def test_task_init_config_none_handling(self):
        """Test Task initialization when config is None."""
        task = Task(
            name="Config None Task",
            description="Task with config=None",
            expected_output="Output",
            config=None
        )
        
        assert task.config == {}
        assert isinstance(task.config, dict)
    
    def test_task_init_superclass_called(self):
        """Test that Task initialization calls superclass __init__."""
        task = Task(
            name="Superclass Task",
            description="Task for superclass testing",
            expected_output="Output"
        )
        
        # Should have SQLAlchemy instance attributes
        assert hasattr(task, '__class__')
        assert hasattr(task, '__tablename__')


class TestTaskConfigSynchronization:
    """Test cases for Task configuration synchronization."""
    
    def test_task_output_pydantic_config_to_field(self):
        """Test synchronization from config to output_pydantic field."""
        config = {"output_pydantic": "DataAnalysisResult"}
        
        task = Task(
            name="Pydantic Config Task",
            description="Task with pydantic in config",
            expected_output="Output",
            config=config
        )
        
        assert task.output_pydantic == "DataAnalysisResult"
        assert task.config["output_pydantic"] == "DataAnalysisResult"
    
    def test_task_output_pydantic_field_to_config(self):
        """Test synchronization from output_pydantic field to config."""
        task = Task(
            name="Pydantic Field Task",
            description="Task with pydantic field",
            expected_output="Output",
            output_pydantic="AnalysisModel"
        )
        
        assert task.output_pydantic == "AnalysisModel"
        assert task.config["output_pydantic"] == "AnalysisModel"
    
    def test_task_output_json_config_to_field(self):
        """Test synchronization from config to output_json field."""
        config = {"output_json": "results.json"}
        
        task = Task(
            name="JSON Config Task",
            description="Task with json in config",
            expected_output="Output",
            config=config
        )
        
        assert task.output_json == "results.json"
        assert task.config["output_json"] == "results.json"
    
    def test_task_output_json_field_to_config(self):
        """Test synchronization from output_json field to config."""
        task = Task(
            name="JSON Field Task",
            description="Task with json field",
            expected_output="Output",
            output_json="output.json"
        )
        
        assert task.output_json == "output.json"
        assert task.config["output_json"] == "output.json"
    
    def test_task_output_file_config_to_field(self):
        """Test synchronization from config to output_file field."""
        config = {"output_file": "report.pdf"}
        
        task = Task(
            name="File Config Task",
            description="Task with file in config",
            expected_output="Output",
            config=config
        )
        
        assert task.output_file == "report.pdf"
        assert task.config["output_file"] == "report.pdf"
    
    def test_task_output_file_field_to_config(self):
        """Test synchronization from output_file field to config."""
        task = Task(
            name="File Field Task",
            description="Task with file field",
            expected_output="Output",
            output_file="document.pdf"
        )
        
        assert task.output_file == "document.pdf"
        assert task.config["output_file"] == "document.pdf"
    
    def test_task_callback_config_to_field(self):
        """Test synchronization from config to callback field."""
        config = {"callback": "completion_handler"}
        
        task = Task(
            name="Callback Config Task",
            description="Task with callback in config",
            expected_output="Output",
            config=config
        )
        
        assert task.callback == "completion_handler"
        assert task.config["callback"] == "completion_handler"
    
    def test_task_callback_field_to_config(self):
        """Test synchronization from callback field to config."""
        task = Task(
            name="Callback Field Task",
            description="Task with callback field",
            expected_output="Output",
            callback="notification_handler"
        )
        
        assert task.callback == "notification_handler"
        assert task.config["callback"] == "notification_handler"
    
    def test_task_markdown_config_to_field(self):
        """Test synchronization from config to markdown field."""
        config = {"markdown": True}
        
        task = Task(
            name="Markdown Config Task",
            description="Task with markdown in config",
            expected_output="Output",
            config=config
        )
        
        assert task.markdown is True
        assert task.config["markdown"] is True
    
    def test_task_markdown_field_to_config(self):
        """Test synchronization from markdown field to config."""
        task = Task(
            name="Markdown Field Task",
            description="Task with markdown field",
            expected_output="Output",
            markdown=False
        )
        
        assert task.markdown is False
        assert task.config["markdown"] is False
    
    def test_task_condition_handling(self):
        """Test condition handling in configuration."""
        condition = {
            "type": "success",
            "parameters": {"threshold": 0.95},
            "dependent_task": "previous_task"
        }
        
        task = Task(
            name="Condition Task",
            description="Task with condition",
            expected_output="Output",
            condition=condition
        )
        
        assert "condition" in task.config
        assert task.config["condition"]["type"] == "success"
        assert task.config["condition"]["parameters"]["threshold"] == 0.95
        assert task.config["condition"]["dependent_task"] == "previous_task"
    
    def test_task_multiple_config_synchronization(self):
        """Test synchronization of multiple configuration fields."""
        config = {
            "output_json": "data.json",
            "output_pydantic": "DataModel",
            "callback": "handler",
            "markdown": True
        }
        
        task = Task(
            name="Multi Config Task",
            description="Task with multiple config fields",
            expected_output="Output",
            config=config,
            output_file="report.pdf"  # Additional field
        )
        
        # All config values should be synchronized to fields
        assert task.output_json == "data.json"
        assert task.output_pydantic == "DataModel"
        assert task.callback == "handler"
        assert task.markdown is True
        
        # Additional field should be added to config
        assert task.config["output_file"] == "report.pdf"
        
        # All values should be in config
        assert task.config["output_json"] == "data.json"
        assert task.config["output_pydantic"] == "DataModel"
        assert task.config["callback"] == "handler"
        assert task.config["markdown"] is True


class TestTaskFieldTypes:
    """Test cases for Task field types and constraints."""
    
    def test_task_field_existence(self):
        """Test that all expected fields exist."""
        task = Task(
            name="Field Test Task",
            description="Task for field testing",
            expected_output="Output"
        )
        
        # Check field existence
        assert hasattr(task, 'id')
        assert hasattr(task, 'name')
        assert hasattr(task, 'description')
        assert hasattr(task, 'agent_id')
        assert hasattr(task, 'expected_output')
        assert hasattr(task, 'tools')
        assert hasattr(task, 'async_execution')
        assert hasattr(task, 'context')
        assert hasattr(task, 'config')
        assert hasattr(task, 'group_id')
        assert hasattr(task, 'created_by_email')
        assert hasattr(task, 'output_json')
        assert hasattr(task, 'output_pydantic')
        assert hasattr(task, 'output_file')
        assert hasattr(task, 'output')
        assert hasattr(task, 'markdown')
        assert hasattr(task, 'callback')
        assert hasattr(task, 'human_input')
        assert hasattr(task, 'converter_cls')
        assert hasattr(task, 'guardrail')
        assert hasattr(task, 'created_at')
        assert hasattr(task, 'updated_at')
    
    def test_task_string_fields(self):
        """Test string field types."""
        task = Task(
            name="String Test Task",
            description="Task with string fields",
            expected_output="String output",
            agent_id="agent_123",
            output_json="output.json",
            callback="handler",
            group_id="group_123"
        )
        
        assert isinstance(task.name, str)
        assert isinstance(task.description, str)
        assert isinstance(task.expected_output, str)
        assert isinstance(task.agent_id, str)
        assert isinstance(task.output_json, str)
        assert isinstance(task.callback, str)
        assert isinstance(task.group_id, str)
    
    def test_task_json_fields(self):
        """Test JSON field types."""
        tools = ["tool1", "tool2"]
        context = ["context1", "context2"]
        config = {"key": "value"}
        output = {"result": "success"}
        
        task = Task(
            name="JSON Test Task",
            description="Task with JSON fields",
            expected_output="Output",
            tools=tools,
            context=context,
            config=config,
            output=output
        )
        
        assert isinstance(task.tools, list)
        assert isinstance(task.context, list)
        assert isinstance(task.config, dict)
        assert isinstance(task.output, dict)
    
    def test_task_boolean_fields(self):
        """Test boolean field types."""
        task = Task(
            name="Boolean Test Task",
            description="Task with boolean fields",
            expected_output="Output",
            async_execution=True,
            markdown=False,
            human_input=True
        )
        
        assert isinstance(task.async_execution, bool)
        assert isinstance(task.markdown, bool)
        assert isinstance(task.human_input, bool)
        assert task.async_execution is True
        assert task.markdown is False
        assert task.human_input is True
    
    def test_task_datetime_fields(self):
        """Test datetime field types."""
        task = Task(
            name="DateTime Test Task",
            description="Task with datetime fields",
            expected_output="Output"
        )
        
        assert isinstance(task.created_at, datetime)
        assert isinstance(task.updated_at, datetime)
    
    def test_task_nullable_fields(self):
        """Test nullable field behavior."""
        task = Task(
            name="Nullable Test Task",
            description="Task with nullable fields",
            expected_output="Output"
        )
        
        # These fields should be nullable
        assert task.agent_id is None
        assert task.group_id is None
        assert task.created_by_email is None
        assert task.output_json is None
        assert task.output_pydantic is None
        assert task.output_file is None
        assert task.output is None
        assert task.callback is None
        assert task.converter_cls is None
        assert task.guardrail is None


class TestTaskUsagePatterns:
    """Test cases for common Task usage patterns."""
    
    def test_task_data_analysis_workflow(self):
        """Test Task for data analysis workflow."""
        analysis_task = Task(
            name="Sales Data Analysis",
            description="Analyze quarterly sales data and identify trends",
            agent_id="data_analyst_agent",
            expected_output="Comprehensive sales analysis report with visualizations",
            tools=["sql_query", "data_visualization", "statistical_analysis"],
            context=["previous_quarter_data", "market_conditions"],
            config={
                "data_source": "sales_database",
                "time_period": "Q1_2023",
                "analysis_type": "trend_analysis"
            },
            output_file="sales_analysis_Q1_2023.pdf",
            markdown=True,
            async_execution=False
        )
        
        assert analysis_task.name == "Sales Data Analysis"
        assert "sql_query" in analysis_task.tools
        assert "previous_quarter_data" in analysis_task.context
        assert analysis_task.config["data_source"] == "sales_database"
        assert analysis_task.output_file == "sales_analysis_Q1_2023.pdf"
        assert analysis_task.markdown is True
    
    def test_task_async_processing(self):
        """Test Task for asynchronous processing."""
        async_task = Task(
            name="Large Dataset Processing",
            description="Process large dataset asynchronously",
            expected_output="Processed dataset with summary statistics",
            tools=["big_data_processor", "parallel_computing"],
            async_execution=True,
            config={
                "batch_size": 10000,
                "parallel_workers": 8,
                "timeout": 7200
            },
            callback="notify_completion",
            output_json="processed_data.json"
        )
        
        assert async_task.async_execution is True
        assert async_task.config["parallel_workers"] == 8
        assert async_task.callback == "notify_completion"
        assert async_task.output_json == "processed_data.json"
    
    def test_task_human_interaction(self):
        """Test Task requiring human interaction."""
        human_task = Task(
            name="Review and Approval Task",
            description="Review analysis results and provide approval",
            expected_output="Approved analysis with human feedback",
            human_input=True,
            config={
                "approval_required": True,
                "reviewer_role": "senior_analyst",
                "max_wait_time": 3600
            },
            callback="approval_handler"
        )
        
        assert human_task.human_input is True
        assert human_task.config["approval_required"] is True
        assert human_task.callback == "approval_handler"
    
    def test_task_output_configurations(self):
        """Test Task with various output configurations."""
        # JSON output task
        json_task = Task(
            name="JSON Output Task",
            description="Task with JSON output",
            expected_output="Structured data output",
            output_json="results.json",
            output_pydantic="ResultModel"
        )
        
        # File output task
        file_task = Task(
            name="File Output Task",
            description="Task with file output",
            expected_output="Report document",
            output_file="report.pdf",
            markdown=True
        )
        
        # Complex output task
        complex_task = Task(
            name="Complex Output Task",
            description="Task with multiple outputs",
            expected_output="Multi-format output",
            output_json="data.json",
            output_file="summary.pdf",
            output_pydantic="ComplexResult",
            output={"format": "multi", "types": ["json", "pdf", "object"]}
        )
        
        # Verify output configurations
        assert json_task.output_json == "results.json"
        assert json_task.output_pydantic == "ResultModel"
        
        assert file_task.output_file == "report.pdf"
        assert file_task.markdown is True
        
        assert complex_task.output_json == "data.json"
        assert complex_task.output_file == "summary.pdf"
        assert complex_task.output["format"] == "multi"
    
    def test_task_group_isolation(self):
        """Test Task group isolation pattern."""
        # Team A task
        team_a_task = Task(
            name="Team A Analysis Task",
            description="Analysis task for team A",
            expected_output="Team A analysis results",
            group_id="team_a",
            created_by_email="analyst@teama.com"
        )
        
        # Team B task
        team_b_task = Task(
            name="Team B Processing Task",
            description="Processing task for team B",
            expected_output="Team B processing results",
            group_id="team_b",
            created_by_email="processor@teamb.com"
        )
        
        # Verify group isolation
        assert team_a_task.group_id == "team_a"
        assert team_a_task.created_by_email == "analyst@teama.com"
        assert team_b_task.group_id == "team_b"
        assert team_b_task.created_by_email == "processor@teamb.com"
    
    def test_task_conditional_execution(self):
        """Test Task with conditional execution."""
        conditional_task = Task(
            name="Conditional Processing Task",
            description="Task that executes based on conditions",
            expected_output="Conditional results",
            condition={
                "type": "success",
                "parameters": {"min_confidence": 0.8},
                "dependent_task": "data_validation_task"
            },
            config={
                "fallback_action": "notify_failure",
                "retry_on_failure": True
            }
        )
        
        assert "condition" in conditional_task.config
        assert conditional_task.config["condition"]["type"] == "success"
        assert conditional_task.config["condition"]["parameters"]["min_confidence"] == 0.8
        assert conditional_task.config["fallback_action"] == "notify_failure"
    
    def test_task_guardrail_configuration(self):
        """Test Task with guardrail configuration."""
        guardrail_task = Task(
            name="Guardrail Protected Task",
            description="Task with data validation guardrails",
            expected_output="Validated processing results",
            guardrail="data_quality_validator",
            config={
                "validation_rules": ["no_null_values", "data_type_check"],
                "validation_threshold": 0.95,
                "fail_on_validation_error": True
            }
        )
        
        assert guardrail_task.guardrail == "data_quality_validator"
        assert "validation_rules" in guardrail_task.config
        assert guardrail_task.config["validation_threshold"] == 0.95
    
    def test_task_migration_compatibility(self):
        """Test Task migration compatibility - tenant concept removed."""
        # Test creating a task with group fields only
        group_task = Task(
            name="Group Task",
            description="Task with group isolation",
            expected_output="Group output",
            group_id="group_456",
            created_by_email="user@group.com"
        )
        
        # Verify group fields work correctly
        assert group_task.group_id == "group_456"
        assert group_task.created_by_email == "user@group.com"
    
    def test_task_advanced_configuration(self):
        """Test Task with advanced configuration options."""
        advanced_task = Task(
            name="Advanced Configuration Task",
            description="Task with advanced settings",
            expected_output="Advanced processing results",
            tools=["advanced_processor", "ml_predictor", "data_validator"],
            config={
                "processing_mode": "advanced",
                "ml_model": "bert-large",
                "confidence_threshold": 0.85,
                "batch_processing": True,
                "parallel_execution": True,
                "memory_limit": "8GB",
                "timeout": 10800,
                "retry_policy": {
                    "max_retries": 3,
                    "backoff_factor": 2.0
                }
            },
            converter_cls="AdvancedDataConverter",
            async_execution=True,
            human_input=False
        )
        
        assert advanced_task.config["processing_mode"] == "advanced"
        assert advanced_task.config["ml_model"] == "bert-large"
        assert advanced_task.config["retry_policy"]["max_retries"] == 3
        assert advanced_task.converter_cls == "AdvancedDataConverter"
        assert advanced_task.async_execution is True