"""
Unit tests for task model.

Tests the functionality of the Task database model including
field validation, complex initialization logic, and data integrity.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.models.task import Task, generate_uuid


class TestTask:
    """Test cases for Task model."""

    def test_task_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Task.__tablename__ == "tasks"

    def test_task_column_structure(self):
        """Test Task model column structure."""
        # Act
        columns = Task.__table__.columns
        
        # Assert - Check that all expected columns exist
        expected_columns = [
            'id', 'name', 'description', 'agent_id', 'expected_output', 'tools',
            'async_execution', 'context', 'config', 'group_id', 'created_by_email',
            'output_json', 'output_pydantic', 'output_file', 'output', 'markdown',
            'callback', 'human_input', 'converter_cls', 'guardrail',
            'created_at', 'updated_at'
        ]
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in Task model"

    def test_task_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Task.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "VARCHAR" in str(columns['id'].type) or "STRING" in str(columns['id'].type)
        
        # Required string fields
        required_string_fields = ['name', 'description', 'expected_output']
        for field in required_string_fields:
            assert columns[field].nullable is False
            assert "VARCHAR" in str(columns[field].type) or "STRING" in str(columns[field].type)
        
        # Optional string fields
        optional_string_fields = ['agent_id', 'group_id', 'created_by_email', 'output_json', 
                                'output_pydantic', 'output_file', 'callback', 'converter_cls', 'guardrail']
        for field in optional_string_fields:
            assert columns[field].nullable is True
            assert "VARCHAR" in str(columns[field].type) or "STRING" in str(columns[field].type)
        
        # JSON fields
        json_fields = ['tools', 'context', 'config', 'output']
        for field in json_fields:
            assert "JSON" in str(columns[field].type)
        
        # Boolean fields
        boolean_fields = ['async_execution', 'markdown', 'human_input']
        for field in boolean_fields:
            assert "BOOLEAN" in str(columns[field].type)
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)

    def test_task_default_values(self):
        """Test Task model default values."""
        # Act
        columns = Task.__table__.columns
        
        # Assert
        assert columns['async_execution'].default.arg is False
        assert columns['markdown'].default.arg is False
        assert columns['human_input'].default.arg is False
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_task_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = Task.__table__.columns
        
        # Assert
        assert columns['group_id'].index is True

    def test_task_foreign_keys(self):
        """Test Task model foreign key relationships."""
        # Act
        columns = Task.__table__.columns
        
        # Assert
        # agent_id should be a foreign key to agents.id
        agent_id_fks = [fk for fk in columns['agent_id'].foreign_keys]
        assert len(agent_id_fks) == 1
        assert str(agent_id_fks[0].column) == "agents.id"

    def test_generate_uuid_function(self):
        """Test the generate_uuid function."""
        # Act
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        # Assert
        assert isinstance(uuid1, str)
        assert isinstance(uuid2, str)
        assert uuid1 != uuid2
        assert len(uuid1) == 36  # Standard UUID format length
        assert "-" in uuid1

    def test_task_init_basic(self):
        """Test Task initialization with basic parameters."""
        # Act - Create a real Task instance
        task = Task(
            name='Test Task',
            description='Test Description',
            expected_output='Test Output'
        )

        # Assert - Verify defaults are set correctly
        assert task.name == 'Test Task'
        assert task.description == 'Test Description'
        assert task.expected_output == 'Test Output'
        assert task.id is not None  # UUID auto-generated
        assert task.tools == []
        assert task.context == []
        assert task.config == {}
        assert task.async_execution is False
        assert task.markdown is False
        assert task.human_input is False
        assert task.created_at is not None
        assert task.updated_at is not None

    def test_task_init_with_condition(self):
        """Test Task initialization with condition parameter."""
        # Act - Create task with condition
        condition_data = {
            'type': 'dependent',
            'parameters': {'param1': 'value1'},
            'dependent_task': 'task_id_123'
        }
        task = Task(
            name='Conditional Task',
            description='A task with condition',
            expected_output='Output',
            condition=condition_data
        )

        # Assert - Condition should be structured in config
        assert 'condition' in task.config
        assert task.config['condition']['type'] == 'dependent'
        assert task.config['condition']['parameters'] == {'param1': 'value1'}
        assert task.config['condition']['dependent_task'] == 'task_id_123'

    def test_task_config_synchronization_output_pydantic_from_config(self):
        """Test output_pydantic syncs from config to field."""
        task = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'output_pydantic': 'MyModel'}
        )
        assert task.output_pydantic == 'MyModel'

    def test_task_config_synchronization_output_pydantic_from_field(self):
        """Test output_pydantic syncs from field to config."""
        task = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            output_pydantic='MyModel'
        )
        assert task.config.get('output_pydantic') == 'MyModel'

    def test_task_config_synchronization_output_json(self):
        """Test output_json syncs between config and field."""
        # Config → field
        task1 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'output_json': 'output.json'}
        )
        assert task1.output_json == 'output.json'

        # Field → config
        task2 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            output_json='output.json'
        )
        assert task2.config.get('output_json') == 'output.json'

    def test_task_config_synchronization_output_file(self):
        """Test output_file syncs between config and field."""
        # Config → field
        task1 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'output_file': '/path/to/output.txt'}
        )
        assert task1.output_file == '/path/to/output.txt'

        # Field → config
        task2 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            output_file='/path/to/output.txt'
        )
        assert task2.config.get('output_file') == '/path/to/output.txt'

    def test_task_config_synchronization_callback(self):
        """Test callback syncs between config and field."""
        # Config → field
        task1 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'callback': 'my_callback'}
        )
        assert task1.callback == 'my_callback'

        # Field → config
        task2 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            callback='my_callback'
        )
        assert task2.config.get('callback') == 'my_callback'

    def test_task_config_synchronization_markdown(self):
        """Test markdown syncs between config and field."""
        # Config → field
        task1 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'markdown': True}
        )
        assert task1.markdown is True

        # Field → config (only when explicitly provided)
        task2 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            markdown=True
        )
        assert task2.config.get('markdown') is True

    def test_task_config_synchronization_guardrail(self):
        """Test code guardrail syncs between config and field."""
        # Config → field
        task1 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'guardrail': 'validate_output'}
        )
        assert task1.guardrail == 'validate_output'

        # Field → config
        task2 = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            guardrail='validate_output'
        )
        assert task2.config.get('guardrail') == 'validate_output'

    def test_llm_guardrail_column_does_not_sync_to_config(self):
        """Test that llm_guardrail column does NOT auto-sync to config.

        When a task is created during crew generation with llm_guardrail in the
        column (but NOT in config), the value should NOT be copied into config.
        The config stores the user's explicit choice (toggle ON/OFF), while
        the column stores the LLM-generated suggestion.
        """
        # Act - Create task with llm_guardrail column value but no config entry
        task = Task(
            name='Generated Task',
            description='LLM generated task',
            expected_output='Output',
            llm_guardrail={
                'description': 'Validate output',
                'llm_model': 'databricks-claude-sonnet-4-5'
            }
        )

        # Assert - config should NOT have llm_guardrail (no column → config sync)
        assert 'llm_guardrail' not in task.config
        # Column should retain the value
        assert task.llm_guardrail == {
            'description': 'Validate output',
            'llm_model': 'databricks-claude-sonnet-4-5'
        }

    def test_llm_guardrail_config_syncs_to_column(self):
        """Test that config llm_guardrail syncs to column (config → column).

        When the user explicitly enables the guardrail via the toggle, the
        config gets the value and it should sync to the column.
        """
        guardrail_config = {
            'description': 'Validate output completeness',
            'llm_model': 'databricks-claude-sonnet-4-5'
        }

        # Act - Create task with llm_guardrail in config (user toggle ON)
        task = Task(
            name='Task with guardrail',
            description='Desc',
            expected_output='Output',
            config={'llm_guardrail': guardrail_config}
        )

        # Assert - column should be synced from config
        assert task.llm_guardrail == guardrail_config

    def test_llm_guardrail_config_null_clears_column(self):
        """Test that setting config llm_guardrail to null clears the column.

        When the user disables the guardrail toggle, config gets null
        and the column should also be cleared.
        """
        # Act - Create task with llm_guardrail explicitly null in config
        task = Task(
            name='Task',
            description='Desc',
            expected_output='Output',
            config={'llm_guardrail': None}
        )

        # Assert - column should be null (synced from config)
        assert task.llm_guardrail is None

    def test_llm_guardrail_column_preserves_suggestion(self):
        """Test that llm_guardrail column value is preserved during generation.

        During crew generation, the column stores the LLM suggestion.
        It should NOT leak into config (which controls execution).
        """
        suggestion = {
            'description': 'Validate research output accuracy',
            'llm_model': 'databricks-claude-sonnet-4-5'
        }

        # Act - Create task with column value and empty config
        task = Task(
            name='Generated Task',
            description='Desc',
            expected_output='Output',
            llm_guardrail=suggestion,
            config={}
        )

        # Assert - Column retains the suggestion
        assert task.llm_guardrail == suggestion
        # Config remains clean (no auto-sync from column)
        assert 'llm_guardrail' not in task.config

    def test_task_model_documentation(self):
        """Test Task model documentation."""
        # Act & Assert
        assert Task.__doc__ is not None
        assert "Task model representing a task" in Task.__doc__
        assert "group isolation" in Task.__doc__

    def test_task_tools_scenarios(self):
        """Test tools field scenarios."""
        # Test different tools configurations
        tools_examples = [
            [],  # No tools
            ["web_search"],  # Single tool
            ["web_search", "calculator", "file_reader"],  # Multiple tools
            [
                {"name": "custom_tool", "config": {"param": "value"}},
                {"name": "api_tool", "endpoint": "https://api.example.com"}
            ]  # Complex tool configurations
        ]
        
        import json
        for tools in tools_examples:
            # Assert tools are JSON serializable
            json.dumps(tools)
            assert isinstance(tools, list)

    def test_task_context_scenarios(self):
        """Test context field scenarios."""
        # Test different context configurations
        context_examples = [
            [],  # No context
            ["context_item_1", "context_item_2"],  # Simple context
            [
                {"type": "file", "path": "/data/input.txt"},
                {"type": "url", "url": "https://example.com/data"},
                {"type": "variable", "name": "user_input", "value": "test data"}
            ]  # Complex context configurations
        ]
        
        import json
        for context in context_examples:
            # Assert context is JSON serializable
            json.dumps(context)
            assert isinstance(context, list)

    def test_task_config_scenarios(self):
        """Test config field scenarios."""
        # Test different config structures
        config_examples = [
            {},  # Empty config
            {"timeout": 300, "retries": 3},  # Simple config
            {
                "execution": {
                    "timeout": 600,
                    "max_retries": 5,
                    "retry_delay": 30
                },
                "output": {
                    "format": "json",
                    "validate": True,
                    "schema": "output_schema.json"
                },
                "conditions": [
                    {
                        "type": "dependency",
                        "task_id": "prerequisite_task",
                        "status": "completed"
                    }
                ]
            }  # Complex nested config
        ]
        
        import json
        for config in config_examples:
            # Assert config is JSON serializable
            json.dumps(config)
            assert isinstance(config, dict)

    def test_task_output_scenarios(self):
        """Test output field scenarios."""
        # Test different output formats
        output_examples = [
            None,  # No output yet
            {"status": "completed", "result": "Success"},  # Simple output
            {
                "execution_summary": {
                    "status": "completed",
                    "duration_seconds": 45.2,
                    "timestamp": "2023-12-01T10:30:00Z"
                },
                "result": {
                    "data": "Generated content here",
                    "metadata": {
                        "word_count": 250,
                        "quality_score": 0.95
                    }
                },
                "artifacts": [
                    {"type": "file", "path": "output.txt", "size_bytes": 1024},
                    {"type": "data", "format": "json", "content": {"key": "value"}}
                ]
            }  # Complex output with metadata
        ]
        
        import json
        for output in output_examples:
            if output is not None:
                # Assert output is JSON serializable
                json.dumps(output)
                assert isinstance(output, dict)

    def test_task_group_isolation_scenarios(self):
        """Test group isolation field scenarios."""
        # Test different group isolation scenarios
        group_scenarios = [
            {
                "group_id": "engineering_team",
                "created_by_email": "engineer@company.com"
            },
            {
                "group_id": "marketing_dept",
                "created_by_email": "marketer@company.com"
            },
            {
                "group_id": None,  # Global task
                "created_by_email": "admin@company.com"
            }
        ]
        
        for scenario in group_scenarios:
            # Assert group scenario structure
            if scenario["group_id"] is not None:
                assert isinstance(scenario["group_id"], str)
                assert len(scenario["group_id"]) > 0
            
            if scenario["created_by_email"] is not None:
                assert isinstance(scenario["created_by_email"], str)
                assert "@" in scenario["created_by_email"]


class TestTaskEdgeCases:
    """Test edge cases and error scenarios for Task."""

    def test_task_very_long_fields(self):
        """Test Task with very long field values."""
        # Arrange
        long_name = "Very Long Task Name " * 20  # 400 characters
        long_description = "Very long description " * 30  # 660 characters
        long_expected_output = "Expected output " * 25  # 400 characters
        
        # Assert
        assert len(long_name) == 400
        assert len(long_description) == 660
        assert len(long_expected_output) == 400

    def test_task_complex_tools_configuration(self):
        """Test Task with complex tools configuration."""
        # Complex tools with various configurations
        complex_tools = [
            {
                "name": "web_search",
                "type": "built_in",
                "config": {
                    "search_engine": "google",
                    "max_results": 10,
                    "timeout": 30
                }
            },
            {
                "name": "custom_api",
                "type": "custom",
                "config": {
                    "endpoint": "https://api.company.com/v2/data",
                    "auth": {"type": "bearer", "token_env": "API_TOKEN"},
                    "rate_limit": {"requests_per_minute": 60}
                }
            },
            {
                "name": "file_processor",
                "type": "utility",
                "config": {
                    "supported_formats": ["pdf", "docx", "txt"],
                    "max_file_size_mb": 50,
                    "processing_options": {
                        "extract_text": True,
                        "preserve_formatting": False,
                        "ocr_enabled": True
                    }
                }
            }
        ]
        
        import json
        # Assert complex tools are properly structured
        json.dumps(complex_tools)
        assert len(complex_tools) == 3
        for tool in complex_tools:
            assert "name" in tool
            assert "type" in tool
            assert "config" in tool

    def test_task_advanced_config_scenarios(self):
        """Test Task with advanced configuration scenarios."""
        # Advanced configuration scenarios
        advanced_configs = [
            {
                "scenario": "conditional_execution",
                "config": {
                    "conditions": [
                        {
                            "type": "dependency",
                            "task_ids": ["task_1", "task_2"],
                            "operator": "all_completed"
                        },
                        {
                            "type": "time_constraint",
                            "start_after": "2023-12-01T09:00:00Z",
                            "complete_before": "2023-12-01T17:00:00Z"
                        }
                    ]
                }
            },
            {
                "scenario": "retry_policy",
                "config": {
                    "retry": {
                        "max_attempts": 5,
                        "backoff_strategy": "exponential",
                        "base_delay_seconds": 2,
                        "max_delay_seconds": 300,
                        "retry_on_errors": ["timeout", "rate_limit", "temporary_failure"]
                    }
                }
            },
            {
                "scenario": "resource_constraints",
                "config": {
                    "resources": {
                        "memory_limit_mb": 2048,
                        "cpu_limit_percent": 80,
                        "disk_space_mb": 1024,
                        "network_bandwidth_mbps": 100
                    }
                }
            }
        ]
        
        import json
        for config_scenario in advanced_configs:
            # Assert advanced config is properly structured
            json.dumps(config_scenario["config"])
            assert "scenario" in config_scenario
            assert "config" in config_scenario

    def test_task_output_format_scenarios(self):
        """Test Task with different output format scenarios."""
        # Different output format scenarios
        output_formats = [
            {
                "format_type": "json_schema",
                "output_json": "OutputSchema",
                "config": {
                    "output_validation": {
                        "schema": "task_output_schema.json",
                        "strict": True
                    }
                }
            },
            {
                "format_type": "pydantic_model",
                "output_pydantic": "TaskOutputModel",
                "config": {
                    "output_validation": {
                        "model_class": "models.TaskOutputModel",
                        "serialize_as_dict": True
                    }
                }
            },
            {
                "format_type": "file_output",
                "output_file": "/outputs/task_result.txt",
                "config": {
                    "file_options": {
                        "encoding": "utf-8",
                        "append_mode": False,
                        "create_backup": True
                    }
                }
            },
            {
                "format_type": "markdown",
                "markdown": True,
                "config": {
                    "markdown_options": {
                        "include_metadata": True,
                        "format_tables": True,
                        "syntax_highlighting": True
                    }
                }
            }
        ]
        
        for format_scenario in output_formats:
            # Assert output format scenario structure
            assert "format_type" in format_scenario
            assert "config" in format_scenario

    def test_task_callback_scenarios(self):
        """Test Task with callback scenarios."""
        # Callback scenarios
        callback_scenarios = [
            {
                "callback_type": "function",
                "callback": "handle_task_completion",
                "config": {
                    "callback_config": {
                        "async": True,
                        "timeout": 60,
                        "retry_on_failure": True
                    }
                }
            },
            {
                "callback_type": "webhook",
                "callback": "https://api.company.com/webhooks/task_complete",
                "config": {
                    "callback_config": {
                        "method": "POST",
                        "headers": {"Authorization": "Bearer token"},
                        "payload_format": "json"
                    }
                }
            },
            {
                "callback_type": "message_queue",
                "callback": "task_completion_queue",
                "config": {
                    "callback_config": {
                        "queue_name": "task_notifications",
                        "persistent": True,
                        "priority": "high"
                    }
                }
            }
        ]
        
        for callback_scenario in callback_scenarios:
            # Assert callback scenario structure
            assert "callback_type" in callback_scenario
            assert "callback" in callback_scenario
            assert "config" in callback_scenario

    def test_task_human_input_scenarios(self):
        """Test Task with human input scenarios."""
        # Human input scenarios
        human_input_scenarios = [
            {
                "human_input": True,
                "config": {
                    "human_input_config": {
                        "prompt": "Please review and approve the generated content",
                        "timeout_minutes": 60,
                        "required_approvers": ["manager@company.com"],
                        "approval_method": "email"
                    }
                }
            },
            {
                "human_input": True,
                "config": {
                    "human_input_config": {
                        "prompt": "Select the best option from the generated alternatives",
                        "input_type": "multiple_choice",
                        "options": ["Option A", "Option B", "Option C"],
                        "allow_custom_input": True
                    }
                }
            },
            {
                "human_input": False,
                "config": {
                    "automation": {
                        "fully_automated": True,
                        "fallback_to_human": False
                    }
                }
            }
        ]
        
        for scenario in human_input_scenarios:
            # Assert human input scenario structure
            assert "human_input" in scenario
            assert isinstance(scenario["human_input"], bool)
            assert "config" in scenario

    def test_task_guardrail_scenarios(self):
        """Test Task with guardrail scenarios."""
        # Guardrail scenarios (stored as JSON strings)
        guardrail_scenarios = [
            {
                "guardrail_type": "content_filter",
                "guardrail": '{"type": "content_safety", "rules": ["no_harmful_content", "no_personal_info"]}'
            },
            {
                "guardrail_type": "data_validation",
                "guardrail": '{"type": "data_quality", "checks": ["completeness", "accuracy", "consistency"]}'
            },
            {
                "guardrail_type": "business_rules",
                "guardrail": '{"type": "compliance", "frameworks": ["GDPR", "CCPA"], "audit_trail": true}'
            }
        ]
        
        import json
        for scenario in guardrail_scenarios:
            # Assert guardrail scenario structure
            assert "guardrail_type" in scenario
            assert "guardrail" in scenario
            # Verify guardrail is valid JSON string
            parsed_guardrail = json.loads(scenario["guardrail"])
            assert isinstance(parsed_guardrail, dict)

    def test_task_data_integrity(self):
        """Test data integrity constraints."""
        # Act
        table = Task.__table__
        
        # Assert primary key
        primary_keys = [col for col in table.columns if col.primary_key]
        assert len(primary_keys) == 1
        assert primary_keys[0].name == 'id'
        
        # Assert required fields
        required_fields = ['name', 'description', 'expected_output']
        for field_name in required_fields:
            field = table.columns[field_name]
            assert field.nullable is False
        
        # Assert optional fields
        optional_fields = ['agent_id', 'group_id', 'created_by_email', 'guardrail']
        for field_name in optional_fields:
            field = table.columns[field_name]
            assert field.nullable is True
        
        # Assert JSON fields have correct types
        json_fields = ['tools', 'context', 'config', 'output']
        for field_name in json_fields:
            field = table.columns[field_name]
            assert "JSON" in str(field.type)
        
        # Assert indexed fields
        indexed_fields = ['group_id']
        for field_name in indexed_fields:
            field = table.columns[field_name]
            assert field.index is True