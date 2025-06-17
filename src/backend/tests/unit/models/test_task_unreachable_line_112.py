"""
Test to achieve 100% coverage for task.py model.

This test specifically targets the missing lines identified in coverage reports.
"""
import pytest
from src.models.task import Task


class TestTaskFullCoverage:
    """Test to achieve 100% coverage for task.py model."""

    def test_config_synchronization_output_pydantic_from_config(self):
        """Test line 81: output_pydantic sync from config to field."""
        task = Task(
            name="test_output_pydantic_from_config",
            description="Test output_pydantic sync from config",
            expected_output="Test output",
            config={'output_pydantic': {'test': 'model'}}
        )
        
        # Line 81 should be executed
        assert task.output_pydantic == {'test': 'model'}

    def test_config_synchronization_output_pydantic_to_config(self):
        """Test line 84: output_pydantic sync from field to config."""
        task = Task(
            name="test_output_pydantic_to_config", 
            description="Test output_pydantic sync to config",
            expected_output="Test output",
            output_pydantic={'test': 'model'}
        )
        
        # Line 84 should be executed
        assert task.config['output_pydantic'] == {'test': 'model'}

    def test_config_synchronization_output_json_from_config(self):
        """Test line 88: output_json sync from config to field."""
        task = Task(
            name="test_output_json_from_config",
            description="Test output_json sync from config", 
            expected_output="Test output",
            config={'output_json': {'schema': 'test'}}
        )
        
        # Line 88 should be executed
        assert task.output_json == {'schema': 'test'}

    def test_config_synchronization_output_json_to_config(self):
        """Test line 90: output_json sync from field to config."""
        task = Task(
            name="test_output_json_to_config",
            description="Test output_json sync to config",
            expected_output="Test output", 
            output_json={'schema': 'test'}
        )
        
        # Line 90 should be executed
        assert task.config['output_json'] == {'schema': 'test'}

    def test_config_synchronization_output_file_from_config(self):
        """Test line 93: output_file sync from config to field."""
        task = Task(
            name="test_output_file_from_config",
            description="Test output_file sync from config",
            expected_output="Test output",
            config={'output_file': 'test.txt'}
        )
        
        # Line 93 should be executed
        assert task.output_file == 'test.txt'

    def test_config_synchronization_output_file_to_config(self):
        """Test line 95: output_file sync from field to config."""
        task = Task(
            name="test_output_file_to_config",
            description="Test output_file sync to config",
            expected_output="Test output",
            output_file='test.txt'
        )
        
        # Line 95 should be executed
        assert task.config['output_file'] == 'test.txt'

    def test_config_synchronization_callback_from_config(self):
        """Test line 98: callback sync from config to field."""
        task = Task(
            name="test_callback_from_config",
            description="Test callback sync from config",
            expected_output="Test output",
            config={'callback': 'test_callback'}
        )
        
        # Line 98 should be executed
        assert task.callback == 'test_callback'

    def test_config_synchronization_callback_to_config(self):
        """Test line 100: callback sync from field to config."""
        task = Task(
            name="test_callback_to_config",
            description="Test callback sync to config",
            expected_output="Test output",
            callback='test_callback'
        )
        
        # Line 100 should be executed
        assert task.config['callback'] == 'test_callback'

    def test_config_synchronization_markdown_from_config(self):
        """Test line 104: markdown sync from config to field."""
        task = Task(
            name="test_markdown_from_config",
            description="Test markdown sync from config",
            expected_output="Test output",
            config={'markdown': True}
        )
        
        # Line 104 should be executed
        assert task.markdown is True

    def test_config_synchronization_markdown_to_config_explicit(self):
        """Test line 107: markdown sync from field to config when explicitly provided."""
        task = Task(
            name="test_markdown_to_config",
            description="Test markdown sync to config",
            expected_output="Test output",
            markdown=True
        )
        
        # Line 107 should be executed
        assert task.config['markdown'] is True

    def test_condition_processing_line_112(self):
        """Test line 112: condition assignment to config."""
        task = Task(
            name="test_condition_processing",
            description="Test condition processing",
            expected_output="Test output",
            condition={
                'type': 'test_condition',
                'parameters': {'param1': 'value1'},
                'dependent_task': 'test_task'
            }
        )
        
        # Line 112 should be executed
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'test_condition'
        assert task.config['condition']['parameters'] == {'param1': 'value1'}
        assert task.config['condition']['dependent_task'] == 'test_task'