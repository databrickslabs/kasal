"""
Comprehensive unit tests for Task SQLAlchemy model.

Tests all aspects of the Task model including complex initialization logic.
"""
import pytest
from datetime import datetime
from sqlalchemy import Column, String, JSON, Boolean, DateTime, ForeignKey
from uuid import UUID

from src.models.task import generate_uuid, Task
from src.db.base import Base


class TestGenerateUuidTask:
    """Test generate_uuid utility function for Task."""

    def test_generate_uuid_returns_string(self):
        """Test generate_uuid returns a string."""
        uuid_str = generate_uuid()
        
        assert isinstance(uuid_str, str)

    def test_generate_uuid_is_valid_uuid(self):
        """Test generate_uuid returns a valid UUID string."""
        uuid_str = generate_uuid()
        
        # Should be able to parse as UUID
        uuid_obj = UUID(uuid_str)
        assert str(uuid_obj) == uuid_str

    def test_generate_uuid_unique(self):
        """Test generate_uuid returns unique values."""
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        assert uuid1 != uuid2


class TestTask:
    """Test Task model."""

    def test_task_inherits_base(self):
        """Test Task inherits from Base."""
        assert issubclass(Task, Base)

    def test_task_tablename(self):
        """Test Task table name."""
        assert Task.__tablename__ == "tasks"

    def test_task_columns_exist(self):
        """Test Task has expected columns."""
        expected_columns = [
            'id', 'name', 'description', 'agent_id', 'expected_output', 'tools',
            'tool_configs', 'async_execution', 'context', 'config', 'group_id',
            'created_by_email', 'output_json', 'output_pydantic', 'output_file',
            'output', 'markdown', 'callback', 'callback_config', 'human_input',
            'converter_cls', 'guardrail', 'created_at', 'updated_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(Task, column_name)

    def test_task_id_column_properties(self):
        """Test id column properties."""
        id_column = Task.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].default is not None

    def test_task_required_columns_properties(self):
        """Test required column properties."""
        name_column = Task.name
        assert name_column.property.columns[0].nullable is False
        
        description_column = Task.description
        assert description_column.property.columns[0].nullable is False
        
        expected_output_column = Task.expected_output
        assert expected_output_column.property.columns[0].nullable is False

    def test_task_foreign_key_properties(self):
        """Test foreign key column properties."""
        agent_id_column = Task.agent_id
        column = agent_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is True
        assert len(column.foreign_keys) == 1
        fk = list(column.foreign_keys)[0]
        assert str(fk.column) == "agents.id"

    def test_task_json_columns_properties(self):
        """Test JSON column properties."""
        json_columns = ['tools', 'tool_configs', 'context', 'config', 'output', 'callback_config']
        
        for col_name in json_columns:
            column = getattr(Task, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, JSON)

    def test_task_boolean_columns_properties(self):
        """Test boolean column properties."""
        boolean_columns = ['async_execution', 'markdown', 'human_input']
        
        for col_name in boolean_columns:
            column = getattr(Task, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, Boolean)
            assert column.default.arg is False

    def test_task_string_columns_properties(self):
        """Test string column properties."""
        string_columns = ['output_json', 'output_pydantic', 'output_file', 'callback', 'converter_cls', 'guardrail']
        
        for col_name in string_columns:
            column = getattr(Task, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, String)

    def test_task_group_columns_properties(self):
        """Test group-related column properties."""
        group_id_column = Task.group_id
        column = group_id_column.property.columns[0]
        assert isinstance(column.type, String)
        assert column.type.length == 100
        assert column.index is True
        assert column.nullable is True

        created_by_email_column = Task.created_by_email
        column = created_by_email_column.property.columns[0]
        assert isinstance(column.type, String)
        assert column.type.length == 255
        assert column.nullable is True

    def test_task_datetime_columns_properties(self):
        """Test datetime column properties."""
        created_at_column = Task.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None

        updated_at_column = Task.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None
        assert column.onupdate is not None


class TestTaskInitialization:
    """Test Task initialization and default value handling."""

    def test_task_minimal_initialization(self):
        """Test Task initialization with minimal required fields."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output"
        )
        
        assert task.name == "Test Task"
        assert task.description == "Test Description"
        assert task.expected_output == "Test Output"
        
        # Check defaults are applied
        assert task.tools == []
        assert task.context == []
        assert task.config == {}
        assert task.async_execution is False
        assert task.markdown is False
        assert task.human_input is False
        assert task.id is not None
        assert isinstance(task.id, str)

    def test_task_initialization_with_all_fields(self):
        """Test Task initialization with all fields."""
        task = Task(
            name="Test Task",
            description="Test Description",
            agent_id="agent-123",
            expected_output="Test Output",
            tools=["tool1", "tool2"],
            tool_configs={"tool1": {"param": "value"}},
            async_execution=True,
            context=["context1"],
            config={"key": "value"},
            group_id="group-123",
            created_by_email="test@example.com",
            output_json="json_schema",
            output_pydantic="PydanticModel",
            output_file="output.txt",
            output={"result": "data"},
            markdown=True,
            callback="callback_func",
            callback_config={"callback_param": "value"},
            human_input=True,
            converter_cls="ConverterClass",
            guardrail="guardrail_config"
        )
        
        assert task.name == "Test Task"
        assert task.description == "Test Description"
        assert task.agent_id == "agent-123"
        assert task.expected_output == "Test Output"
        assert task.tools == ["tool1", "tool2"]
        assert task.tool_configs == {"tool1": {"param": "value"}}
        assert task.async_execution is True
        assert task.context == ["context1"]
        # Config gets modified by synchronization logic
        assert "key" in task.config
        assert task.config["key"] == "value"
        # Additional fields added by synchronization
        assert task.config["output_json"] == "json_schema"
        assert task.config["output_pydantic"] == "PydanticModel"
        assert task.config["output_file"] == "output.txt"
        assert task.config["callback"] == "callback_func"
        assert task.config["markdown"] is True
        assert task.group_id == "group-123"
        assert task.created_by_email == "test@example.com"
        assert task.output_json == "json_schema"
        assert task.output_pydantic == "PydanticModel"
        assert task.output_file == "output.txt"
        assert task.output == {"result": "data"}
        assert task.markdown is True
        assert task.callback == "callback_func"
        assert task.callback_config == {"callback_param": "value"}
        assert task.human_input is True
        assert task.converter_cls == "ConverterClass"
        assert task.guardrail == "guardrail_config"

    def test_task_initialization_with_none_values(self):
        """Test Task initialization handles None values correctly."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            tools=None,
            context=None,
            config=None,
            async_execution=None,
            markdown=None,
            human_input=None
        )
        
        # None values should be replaced with defaults
        assert task.tools == []
        assert task.context == []
        # Config gets markdown added due to explicit parameter
        assert task.config == {'markdown': False}
        assert task.async_execution is False
        assert task.markdown is False
        assert task.human_input is False

    def test_task_initialization_with_condition(self):
        """Test Task initialization with condition parameter."""
        condition = {
            'type': 'conditional',
            'parameters': {'param1': 'value1'},
            'dependent_task': 'task-123'
        }
        
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            condition=condition
        )
        
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'conditional'
        assert task.config['condition']['parameters'] == {'param1': 'value1'}
        assert task.config['condition']['dependent_task'] == 'task-123'

    def test_task_initialization_condition_with_missing_fields(self):
        """Test Task initialization with incomplete condition."""
        condition = {
            'type': 'conditional'
            # Missing parameters and dependent_task
        }
        
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            condition=condition
        )
        
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'conditional'
        assert task.config['condition']['parameters'] == {}
        assert task.config['condition']['dependent_task'] is None


class TestTaskConfigSynchronization:
    """Test Task config synchronization logic."""

    def test_output_pydantic_config_to_field_sync(self):
        """Test output_pydantic synchronization from config to field."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            config={'output_pydantic': 'ConfigModel'}
        )
        
        assert task.output_pydantic == 'ConfigModel'
        assert task.config['output_pydantic'] == 'ConfigModel'

    def test_output_pydantic_field_to_config_sync(self):
        """Test output_pydantic synchronization from field to config."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            output_pydantic='FieldModel'
        )
        
        assert task.output_pydantic == 'FieldModel'
        assert task.config['output_pydantic'] == 'FieldModel'

    def test_output_json_config_to_field_sync(self):
        """Test output_json synchronization from config to field."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            config={'output_json': 'json_schema'}
        )
        
        assert task.output_json == 'json_schema'
        assert task.config['output_json'] == 'json_schema'

    def test_output_json_field_to_config_sync(self):
        """Test output_json synchronization from field to config."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            output_json='field_schema'
        )
        
        assert task.output_json == 'field_schema'
        assert task.config['output_json'] == 'field_schema'

    def test_output_file_config_to_field_sync(self):
        """Test output_file synchronization from config to field."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            config={'output_file': 'config_file.txt'}
        )
        
        assert task.output_file == 'config_file.txt'
        assert task.config['output_file'] == 'config_file.txt'

    def test_output_file_field_to_config_sync(self):
        """Test output_file synchronization from field to config."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            output_file='field_file.txt'
        )
        
        assert task.output_file == 'field_file.txt'
        assert task.config['output_file'] == 'field_file.txt'

    def test_callback_config_to_field_sync(self):
        """Test callback synchronization from config to field."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            config={'callback': 'config_callback'}
        )
        
        assert task.callback == 'config_callback'
        assert task.config['callback'] == 'config_callback'

    def test_callback_field_to_config_sync(self):
        """Test callback synchronization from field to config."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            callback='field_callback'
        )
        
        assert task.callback == 'field_callback'
        assert task.config['callback'] == 'field_callback'

    def test_markdown_config_to_field_sync(self):
        """Test markdown synchronization from config to field."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            config={'markdown': True}
        )
        
        assert task.markdown is True
        assert task.config['markdown'] is True

    def test_markdown_field_to_config_sync_explicit(self):
        """Test markdown synchronization from field to config when explicitly provided."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            markdown=True
        )
        
        assert task.markdown is True
        assert task.config['markdown'] is True

    def test_markdown_field_to_config_sync_not_explicit(self):
        """Test markdown not synced to config when not explicitly provided."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output"
        )
        
        assert task.markdown is False
        assert 'markdown' not in task.config or task.config.get('markdown') is None

    def test_multiple_field_config_sync(self):
        """Test multiple field-config synchronizations work together."""
        task = Task(
            name="Test Task",
            description="Test Description",
            expected_output="Test Output",
            output_pydantic='Model1',
            output_json='schema1',
            callback='callback1',
            markdown=True,
            config={'output_file': 'config_file.txt'}
        )
        
        # Field to config sync
        assert task.config['output_pydantic'] == 'Model1'
        assert task.config['output_json'] == 'schema1'
        assert task.config['callback'] == 'callback1'
        assert task.config['markdown'] is True
        
        # Config to field sync
        assert task.output_file == 'config_file.txt'
        
        # All values preserved
        assert task.output_pydantic == 'Model1'
        assert task.output_json == 'schema1'
        assert task.callback == 'callback1'
        assert task.markdown is True


class TestTaskTableStructure:
    """Test Task table structure and metadata."""

    def test_task_table_exists(self):
        """Test Task table exists in metadata."""
        assert hasattr(Task, '__table__')
        assert Task.__table__.name == "tasks"

    def test_task_primary_key(self):
        """Test Task primary key."""
        table = Task.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        assert primary_key_columns == ['id']

    def test_task_indexes(self):
        """Test Task indexes."""
        table = Task.__table__
        indexed_columns = []
        
        for column in table.columns:
            if column.index:
                indexed_columns.append(column.name)
        
        assert 'group_id' in indexed_columns

    def test_task_foreign_keys(self):
        """Test Task foreign keys."""
        table = Task.__table__
        
        agent_id_column = table.columns['agent_id']
        foreign_keys = list(agent_id_column.foreign_keys)
        assert len(foreign_keys) == 1
        fk = foreign_keys[0]
        assert str(fk.column) == "agents.id"

    def test_task_nullable_columns(self):
        """Test Task nullable column configuration."""
        table = Task.__table__
        
        # Non-nullable columns
        non_nullable = ['id', 'name', 'description', 'expected_output', 'tools']
        for col_name in non_nullable:
            column = table.columns[col_name]
            assert not column.nullable, f"Column {col_name} should not be nullable"

    def test_task_column_defaults(self):
        """Test Task column default values."""
        table = Task.__table__
        
        # Columns with defaults
        columns_with_defaults = ['id', 'tools', 'async_execution', 'context', 'config', 
                               'markdown', 'human_input', 'created_at', 'updated_at']
        
        for col_name in columns_with_defaults:
            column = table.columns[col_name]
            assert column.default is not None, f"Column {col_name} should have a default"
