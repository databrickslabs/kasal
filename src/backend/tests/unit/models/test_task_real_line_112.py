"""
Unit test to achieve 100% coverage for task.py line 112.

Line 112 is: self.config['condition'] = {
This line executes when a Task is created with a condition parameter.
"""
import pytest

from src.models.task import Task


class TestTaskLine112Coverage:
    """Test to achieve 100% coverage for line 112 in Task.__init__ method."""

    def test_task_with_condition_basic(self):
        """Test that line 112 executes when task is created with a condition."""
        task = Task(
            name="test_task_with_condition",
            description="Test task with condition",
            expected_output="Test output",
            condition={
                'type': 'basic_condition',
                'parameters': {'required': True},
                'dependent_task': 'upstream_task'
            }
        )
        
        # Verify line 112 was executed - condition should be in config
        assert task.config is not None
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'basic_condition'
        assert task.config['condition']['parameters']['required'] is True
        assert task.config['condition']['dependent_task'] == 'upstream_task'

    def test_task_with_condition_minimal(self):
        """Test line 112 with minimal condition data."""
        task = Task(
            name="minimal_condition_task",
            description="Minimal condition test",
            expected_output="Output",
            condition={
                'type': 'minimal'
            }
        )
        
        # Verify line 112 execution
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'minimal'
        assert task.config['condition']['parameters'] == {}
        assert task.config['condition']['dependent_task'] is None

    def test_task_with_condition_full_parameters(self):
        """Test line 112 with all condition parameters."""
        task = Task(
            name="full_condition_task",
            description="Full condition test",
            expected_output="Output",
            condition={
                'type': 'complex_condition',
                'parameters': {
                    'timeout': 300,
                    'retry_count': 3,
                    'critical': True
                },
                'dependent_task': 'critical_upstream'
            }
        )
        
        # Verify line 112 execution with full parameters
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'complex_condition'
        assert task.config['condition']['parameters']['timeout'] == 300
        assert task.config['condition']['parameters']['retry_count'] == 3
        assert task.config['condition']['parameters']['critical'] is True
        assert task.config['condition']['dependent_task'] == 'critical_upstream'

    def test_task_with_empty_condition_dict(self):
        """Test line 112 with empty condition dict."""
        task = Task(
            name="empty_condition_task",
            description="Empty condition test",
            expected_output="Output",
            condition={}
        )
        
        # Verify line 112 execution with empty condition
        assert 'condition' in task.config
        assert task.config['condition']['type'] is None
        assert task.config['condition']['parameters'] == {}
        assert task.config['condition']['dependent_task'] is None

    def test_task_with_condition_no_parameters(self):
        """Test line 112 with condition missing parameters key."""
        task = Task(
            name="no_params_task",
            description="No parameters test",
            expected_output="Output",
            condition={
                'type': 'no_params_condition',
                'dependent_task': 'some_task'
            }
        )
        
        # Verify line 112 execution
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'no_params_condition'
        assert task.config['condition']['parameters'] == {}
        assert task.config['condition']['dependent_task'] == 'some_task'

    def test_task_with_condition_no_dependent_task(self):
        """Test line 112 with condition missing dependent_task key."""
        task = Task(
            name="no_dependent_task",
            description="No dependent task test",
            expected_output="Output",
            condition={
                'type': 'independent_condition',
                'parameters': {'standalone': True}
            }
        )
        
        # Verify line 112 execution
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'independent_condition'
        assert task.config['condition']['parameters']['standalone'] is True
        assert task.config['condition']['dependent_task'] is None

    def test_task_without_condition(self):
        """Test that line 112 is NOT executed when no condition is provided."""
        task = Task(
            name="no_condition_task",
            description="Task without condition",
            expected_output="Output"
        )
        
        # Verify line 112 was NOT executed - no condition in config
        assert task.config is not None
        assert 'condition' not in task.config

    def test_task_with_condition_and_existing_config(self):
        """Test line 112 when task has existing config and condition."""
        task = Task(
            name="existing_config_task",
            description="Task with existing config",
            expected_output="Output",
            config={'existing_key': 'existing_value'},
            condition={
                'type': 'additional_condition',
                'parameters': {'added': True},
                'dependent_task': 'dependency'
            }
        )
        
        # Verify line 112 execution - condition added to existing config
        assert 'existing_key' in task.config
        assert task.config['existing_key'] == 'existing_value'
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'additional_condition'
        assert task.config['condition']['parameters']['added'] is True
        assert task.config['condition']['dependent_task'] == 'dependency'

    def test_task_with_output_pydantic_in_config(self):
        """Test line 81: output_pydantic from config to field."""
        task = Task(
            name="output_pydantic_config_task",
            description="Task with output_pydantic in config",
            expected_output="Output",
            config={'output_pydantic': 'MyModel'}
        )
        
        # Verify line 81 execution
        assert task.output_pydantic == 'MyModel'
        assert task.config['output_pydantic'] == 'MyModel'

    def test_task_with_output_pydantic_field_to_config(self):
        """Test line 84: output_pydantic from field to config."""
        task = Task(
            name="output_pydantic_field_task",
            description="Task with output_pydantic field",
            expected_output="Output",
            output_pydantic='FieldModel'
        )
        
        # Verify line 84 execution
        assert task.output_pydantic == 'FieldModel'
        assert task.config['output_pydantic'] == 'FieldModel'

    def test_task_with_output_json_in_config(self):
        """Test line 88: output_json from config to field."""
        task = Task(
            name="output_json_config_task",
            description="Task with output_json in config",
            expected_output="Output",
            config={'output_json': 'schema.json'}
        )
        
        # Verify line 88 execution
        assert task.output_json == 'schema.json'
        assert task.config['output_json'] == 'schema.json'

    def test_task_with_output_json_field_to_config(self):
        """Test line 90: output_json from field to config."""
        task = Task(
            name="output_json_field_task",
            description="Task with output_json field",
            expected_output="Output",
            output_json='field_schema.json'
        )
        
        # Verify line 90 execution
        assert task.output_json == 'field_schema.json'
        assert task.config['output_json'] == 'field_schema.json'

    def test_task_with_output_file_in_config(self):
        """Test line 93: output_file from config to field."""
        task = Task(
            name="output_file_config_task",
            description="Task with output_file in config",
            expected_output="Output",
            config={'output_file': 'output.txt'}
        )
        
        # Verify line 93 execution
        assert task.output_file == 'output.txt'
        assert task.config['output_file'] == 'output.txt'

    def test_task_with_output_file_field_to_config(self):
        """Test line 95: output_file from field to config."""
        task = Task(
            name="output_file_field_task",
            description="Task with output_file field",
            expected_output="Output",
            output_file='field_output.txt'
        )
        
        # Verify line 95 execution
        assert task.output_file == 'field_output.txt'
        assert task.config['output_file'] == 'field_output.txt'

    def test_task_with_callback_in_config(self):
        """Test line 98: callback from config to field."""
        task = Task(
            name="callback_config_task",
            description="Task with callback in config",
            expected_output="Output",
            config={'callback': 'my_callback'}
        )
        
        # Verify line 98 execution
        assert task.callback == 'my_callback'
        assert task.config['callback'] == 'my_callback'

    def test_task_with_callback_field_to_config(self):
        """Test line 100: callback from field to config."""
        task = Task(
            name="callback_field_task",
            description="Task with callback field",
            expected_output="Output",
            callback='field_callback'
        )
        
        # Verify line 100 execution
        assert task.callback == 'field_callback'
        assert task.config['callback'] == 'field_callback'

    def test_task_with_markdown_in_config(self):
        """Test line 104: markdown from config to field."""
        task = Task(
            name="markdown_config_task",
            description="Task with markdown in config",
            expected_output="Output",
            config={'markdown': True}
        )
        
        # Verify line 104 execution
        assert task.markdown is True
        assert task.config['markdown'] is True

    def test_task_with_markdown_field_to_config(self):
        """Test line 107: markdown from field to config (explicit)."""
        task = Task(
            name="markdown_field_task",
            description="Task with explicit markdown field",
            expected_output="Output",
            markdown=True
        )
        
        # Verify line 107 execution
        assert task.markdown is True
        assert task.config['markdown'] is True

    def test_task_with_markdown_false_in_config(self):
        """Test markdown synchronization with False value."""
        task = Task(
            name="markdown_false_config_task",
            description="Task with markdown False in config",
            expected_output="Output",
            config={'markdown': False}
        )
        
        assert task.markdown is False
        assert task.config['markdown'] is False

    def test_task_with_none_values_initialization(self):
        """Test all None value initializations."""
        task = Task(
            name="none_values_task",
            description="Task with None values",
            expected_output="Output",
            id=None,
            tools=None,
            context=None,
            config=None,
            async_execution=None,
            markdown=None,
            human_input=None,
            created_at=None,
            updated_at=None
        )
        
        # Verify all None value initializations
        assert task.id is not None  # Generated UUID
        assert task.tools == []
        assert task.context == []
        assert task.config == {'markdown': False}  # markdown gets added to config
        assert task.async_execution is False
        assert task.markdown is False
        assert task.human_input is False
        assert task.created_at is not None
        assert task.updated_at is not None

    def test_task_config_markdown_none_explicit_kwargs(self):
        """Test markdown synchronization when config markdown is None and explicitly provided."""
        task = Task(
            name="explicit_markdown_task",
            description="Task with explicit markdown",
            expected_output="Output",
            config={'markdown': None},
            markdown=True
        )
        
        # This should trigger line 107
        assert task.markdown is True
        assert task.config['markdown'] is True

    def test_task_existing_config_values_preserved(self):
        """Test that existing config values don't get overwritten."""
        task = Task(
            name="preserve_config_task",
            description="Task preserving config",
            expected_output="Output",
            config={
                'output_pydantic': 'ExistingModel',
                'output_json': 'existing.json',
                'output_file': 'existing.txt',
                'callback': 'existing_callback',
                'markdown': True
            },
            output_pydantic='NewModel',
            output_json='new.json',
            output_file='new.txt',
            callback='new_callback',
            markdown=False
        )
        
        # Config values should be used when they exist and are truthy
        assert task.output_pydantic == 'ExistingModel'
        assert task.output_json == 'existing.json'
        assert task.output_file == 'existing.txt'
        assert task.callback == 'existing_callback'
        assert task.markdown is True

    def test_task_with_empty_string_config_values(self):
        """Test with empty string config values that should not override fields."""
        task = Task(
            name="empty_config_task",
            description="Task with empty config values",
            expected_output="Output",
            config={
                'output_pydantic': '',
                'output_json': '',
                'output_file': '',
                'callback': ''
            },
            output_pydantic='FieldModel',
            output_json='field.json',
            output_file='field.txt',
            callback='field_callback'
        )
        
        # Empty strings in config should not override field values
        assert task.output_pydantic == 'FieldModel'
        assert task.output_json == 'field.json'
        assert task.output_file == 'field.txt'
        assert task.callback == 'field_callback'
        
        # And field values should be added to config
        assert task.config['output_pydantic'] == 'FieldModel'
        assert task.config['output_json'] == 'field.json'
        assert task.config['output_file'] == 'field.txt'
        assert task.config['callback'] == 'field_callback'

    def test_task_with_predefined_values(self):
        """Test Task when values are already set (not None) to cover else branches."""
        from datetime import datetime
        import uuid
        
        predefined_id = str(uuid.uuid4())
        predefined_created = datetime(2023, 1, 1, 12, 0, 0)
        predefined_updated = datetime(2023, 1, 2, 12, 0, 0)
        
        task = Task(
            name="predefined_task",
            description="Task with predefined values",
            expected_output="Output",
            id=predefined_id,
            tools=['existing_tool'],
            context=['existing_context'],
            config={'existing': 'config'},
            async_execution=True,
            markdown=True,
            human_input=True,
            created_at=predefined_created,
            updated_at=predefined_updated
        )
        
        # Verify predefined values are preserved (covering else branches)
        assert task.id == predefined_id
        assert task.tools == ['existing_tool']
        assert task.context == ['existing_context']
        assert 'existing' in task.config
        assert task.config['existing'] == 'config'
        assert task.async_execution is True
        assert task.markdown is True
        assert task.human_input is True
        assert task.created_at == predefined_created
        assert task.updated_at == predefined_updated