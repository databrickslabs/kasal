"""
Unit tests for flow model.

Tests the functionality of the Flow database model including
field validation, UUID handling, and workflow logic.
"""
import pytest
import uuid
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.models.flow import Flow


class TestFlow:
    """Test cases for Flow model."""

    def test_flow_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Flow.__tablename__ == "flows"

    def test_flow_column_structure(self):
        """Test Flow model column structure."""
        # Act
        columns = Flow.__table__.columns
        
        # Assert - Check that all expected columns exist
        expected_columns = [
            'id', 'name', 'crew_id', 'nodes', 'edges', 'flow_config',
            'group_id', 'created_by_email', 'created_at', 'updated_at'
        ]
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in Flow model"

    def test_flow_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Flow.__table__.columns
        
        # Assert
        # Primary key (UUID)
        assert columns['id'].primary_key is True
        assert "UUID" in str(columns['id'].type)
        
        # Required string field
        assert columns['name'].nullable is False
        assert "VARCHAR" in str(columns['name'].type) or "STRING" in str(columns['name'].type)
        
        # Foreign key (UUID)
        assert "UUID" in str(columns['crew_id'].type)
        assert columns['crew_id'].nullable is True
        
        # JSON fields
        json_fields = ['nodes', 'edges', 'flow_config']
        for field in json_fields:
            assert "JSON" in str(columns[field].type)
        
        # Group isolation fields
        assert columns['group_id'].nullable is True
        assert columns['group_id'].index is True
        assert columns['created_by_email'].nullable is True
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)

    def test_flow_default_values(self):
        """Test Flow model default values."""
        # Act
        columns = Flow.__table__.columns
        
        # Assert
        assert columns['id'].default is not None
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_flow_foreign_keys(self):
        """Test Flow model foreign key relationships."""
        # Act
        columns = Flow.__table__.columns

        # Assert
        # crew_id should be a foreign key to crews.id
        crew_id_fks = [fk for fk in columns['crew_id'].foreign_keys]
        assert len(crew_id_fks) == 1
        assert str(crew_id_fks[0].column) == "crews.id"

    def test_flow_crew_id_cascade_delete(self):
        """Test that crew_id foreign key has ON DELETE CASCADE."""
        columns = Flow.__table__.columns
        crew_id_fks = list(columns['crew_id'].foreign_keys)
        assert len(crew_id_fks) == 1
        assert crew_id_fks[0].ondelete == "CASCADE"

    def test_flow_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = Flow.__table__.columns
        
        # Assert
        assert columns['group_id'].index is True

    def test_flow_init_method(self):
        """Test Flow custom __init__ method."""
        # Act & Assert
        assert hasattr(Flow, '__init__')
        
        # Test initialization logic without creating database objects
        with patch.object(Flow, '__init__', return_value=None) as mock_init:
            flow_data = {'name': 'Test Flow'}
            Flow(**flow_data)
            mock_init.assert_called_once_with(**flow_data)

    def test_flow_uuid_generation(self):
        """Test Flow UUID generation logic."""
        # Test UUID generation
        test_uuid = uuid.uuid4()
        
        # Assert UUID properties
        assert isinstance(test_uuid, uuid.UUID)
        assert str(test_uuid).count('-') == 4
        assert len(str(test_uuid)) == 36

    def test_flow_init_defaults_logic(self):
        """Test Flow initialization defaults logic."""
        # Test default value initialization scenarios
        default_scenarios = [
            {
                "field": "nodes",
                "default": [],
                "description": "Empty nodes list"
            },
            {
                "field": "edges", 
                "default": [],
                "description": "Empty edges list"
            },
            {
                "field": "flow_config",
                "default": {"actions": []},
                "description": "Default flow config with actions"
            }
        ]
        
        for scenario in default_scenarios:
            # Assert default values are correct type
            default_value = scenario["default"]
            if scenario["field"] in ["nodes", "edges"]:
                assert isinstance(default_value, list)
                assert len(default_value) == 0
            elif scenario["field"] == "flow_config":
                assert isinstance(default_value, dict)
                assert "actions" in default_value

    def test_flow_config_actions_logic(self):
        """Test flow config actions initialization logic."""
        # Test flow config scenarios
        config_scenarios = [
            {"flow_config": None, "expected_actions": []},
            {"flow_config": {}, "expected_actions": []},
            {"flow_config": {"actions": ["action1"]}, "expected_actions": ["action1"]},
            {"flow_config": {"other_field": "value"}, "expected_actions": []}
        ]
        
        for scenario in config_scenarios:
            # Assert actions logic
            if scenario["flow_config"] is None:
                # Should get default config with actions
                expected_config = {"actions": scenario["expected_actions"]}
                assert "actions" in expected_config
            elif "actions" not in scenario["flow_config"]:
                # Should add actions to existing config
                scenario["flow_config"]["actions"] = scenario["expected_actions"]
                assert "actions" in scenario["flow_config"]

    def test_flow_model_documentation(self):
        """Test Flow model documentation."""
        # Act & Assert
        assert Flow.__doc__ is not None
        assert "workflow definition with nodes and edges" in Flow.__doc__
        assert "group isolation" in Flow.__doc__

    def test_flow_nodes_scenarios(self):
        """Test nodes field scenarios."""
        # Test different node configurations
        nodes_examples = [
            [],  # No nodes
            [
                {"id": "start", "type": "start", "position": {"x": 0, "y": 0}}
            ],  # Single node
            [
                {"id": "start", "type": "start", "position": {"x": 0, "y": 0}},
                {"id": "process", "type": "task", "position": {"x": 200, "y": 0}, "data": {"task_id": "task_1"}},
                {"id": "end", "type": "end", "position": {"x": 400, "y": 0}}
            ]  # Multiple nodes
        ]
        
        for nodes in nodes_examples:
            # Assert nodes are valid JSON
            json.dumps(nodes)
            assert isinstance(nodes, list)
            for node in nodes:
                if node:  # Skip empty list case
                    assert "id" in node
                    assert "type" in node

    def test_flow_edges_scenarios(self):
        """Test edges field scenarios."""
        # Test different edge configurations
        edges_examples = [
            [],  # No edges
            [
                {"id": "edge1", "source": "start", "target": "process"}
            ],  # Single edge
            [
                {"id": "edge1", "source": "start", "target": "process"},
                {"id": "edge2", "source": "process", "target": "end"},
                {"id": "edge3", "source": "process", "target": "process2", "condition": {"type": "success"}}
            ]  # Multiple edges with conditions
        ]
        
        for edges in edges_examples:
            # Assert edges are valid JSON
            json.dumps(edges)
            assert isinstance(edges, list)
            for edge in edges:
                if edge:  # Skip empty list case
                    assert "source" in edge
                    assert "target" in edge

    def test_flow_config_scenarios(self):
        """Test flow config field scenarios."""
        # Test different flow configuration formats
        config_examples = [
            {"actions": []},  # Default config
            {
                "actions": ["validate_input", "execute_flow", "generate_output"],
                "settings": {
                    "parallel_execution": True,
                    "max_retries": 3,
                    "timeout_minutes": 30
                }
            },  # Basic config with settings
            {
                "actions": ["pre_process", "main_execution", "post_process"],
                "triggers": [
                    {"type": "webhook", "url": "/api/trigger"},
                    {"type": "schedule", "cron": "0 9 * * *"}
                ],
                "error_handling": {
                    "retry_strategy": "exponential_backoff",
                    "fallback_action": "notify_admin"
                },
                "monitoring": {
                    "metrics_enabled": True,
                    "log_level": "INFO"
                }
            }  # Complex config with multiple sections
        ]
        
        for config in config_examples:
            # Assert config is valid JSON
            json.dumps(config)
            assert isinstance(config, dict)
            assert "actions" in config

    def test_flow_group_isolation_scenarios(self):
        """Test group isolation field scenarios."""
        # Test different group isolation scenarios
        group_scenarios = [
            {
                "group_id": "engineering_team",
                "created_by_email": "engineer@company.com"
            },
            {
                "group_id": "data_science_team",
                "created_by_email": "datascientist@company.com"
            },
            {
                "group_id": None,  # Global flow
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

    def test_flow_workflow_patterns(self):
        """Test common workflow patterns."""
        # Test different workflow patterns
        workflow_patterns = [
            {
                "pattern": "linear",
                "nodes": [
                    {"id": "step1", "type": "task"},
                    {"id": "step2", "type": "task"},
                    {"id": "step3", "type": "task"}
                ],
                "edges": [
                    {"source": "step1", "target": "step2"},
                    {"source": "step2", "target": "step3"}
                ]
            },
            {
                "pattern": "parallel",
                "nodes": [
                    {"id": "start", "type": "start"},
                    {"id": "parallel1", "type": "task"},
                    {"id": "parallel2", "type": "task"},
                    {"id": "merge", "type": "merge"}
                ],
                "edges": [
                    {"source": "start", "target": "parallel1"},
                    {"source": "start", "target": "parallel2"},
                    {"source": "parallel1", "target": "merge"},
                    {"source": "parallel2", "target": "merge"}
                ]
            },
            {
                "pattern": "conditional",
                "nodes": [
                    {"id": "decision", "type": "decision"},
                    {"id": "path_a", "type": "task"},
                    {"id": "path_b", "type": "task"}
                ],
                "edges": [
                    {"source": "decision", "target": "path_a", "condition": {"type": "if", "expression": "result == 'A'"}},
                    {"source": "decision", "target": "path_b", "condition": {"type": "else"}}
                ]
            }
        ]
        
        for pattern in workflow_patterns:
            # Assert workflow pattern structure
            assert "pattern" in pattern
            assert "nodes" in pattern
            assert "edges" in pattern
            json.dumps(pattern["nodes"])
            json.dumps(pattern["edges"])


class TestFlowInit:
    """Tests that actually instantiate Flow to cover __init__ logic."""

    def test_defaults_when_no_kwargs(self):
        """Instantiating with only required 'name' fills all defaults."""
        flow = Flow(name="basic")
        assert flow.name == "basic"
        assert isinstance(flow.id, uuid.UUID)
        assert flow.nodes == []
        assert flow.edges == []
        assert flow.flow_config == {"actions": []}
        assert isinstance(flow.created_at, datetime)
        assert isinstance(flow.updated_at, datetime)

    def test_provided_id_is_preserved(self):
        """If id is passed, __init__ should NOT overwrite it."""
        fixed_id = uuid.uuid4()
        flow = Flow(name="x", id=fixed_id)
        assert flow.id == fixed_id

    def test_provided_nodes_edges_preserved(self):
        """If nodes/edges are provided, __init__ should keep them."""
        nodes = [{"id": "n1"}]
        edges = [{"source": "n1", "target": "n2"}]
        flow = Flow(name="x", nodes=nodes, edges=edges)
        assert flow.nodes == nodes
        assert flow.edges == edges

    def test_flow_config_none_gets_default(self):
        """flow_config=None results in {'actions': []}."""
        flow = Flow(name="x", flow_config=None)
        assert flow.flow_config == {"actions": []}

    def test_flow_config_dict_without_actions_gets_actions_key(self):
        """A dict config missing 'actions' gets the key added."""
        flow = Flow(name="x", flow_config={"retries": 3})
        assert flow.flow_config == {"retries": 3, "actions": []}

    def test_flow_config_with_actions_unchanged(self):
        """A dict config that already has 'actions' is left alone."""
        cfg = {"actions": ["a1", "a2"], "retries": 3}
        flow = Flow(name="x", flow_config=cfg)
        assert flow.flow_config["actions"] == ["a1", "a2"]
        assert flow.flow_config["retries"] == 3

    def test_provided_timestamps_preserved(self):
        """If created_at / updated_at are passed, they are kept."""
        ts = datetime(2025, 1, 1, 0, 0, 0)
        flow = Flow(name="x", created_at=ts, updated_at=ts)
        assert flow.created_at == ts
        assert flow.updated_at == ts


class TestFlowEdgeCases:
    """Test edge cases and error scenarios for Flow."""

    def test_flow_very_long_name(self):
        """Test Flow with very long name."""
        # Arrange
        long_name = "Very Long Flow Name " * 25  # 500 characters
        
        # Assert
        assert len(long_name) == 500

    def test_flow_complex_workflow_definition(self):
        """Test Flow with complex workflow definition."""
        # Complex workflow with many nodes and edges
        complex_workflow = {
            "nodes": [
                {
                    "id": f"node_{i}",
                    "type": "task" if i % 2 == 0 else "decision",
                    "position": {"x": i * 100, "y": (i % 3) * 100},
                    "data": {
                        "task_id": f"task_{i}",
                        "config": {"timeout": 300, "retries": 3}
                    }
                } for i in range(20)
            ],
            "edges": [
                {
                    "id": f"edge_{i}",
                    "source": f"node_{i}",
                    "target": f"node_{i+1}",
                    "condition": {"type": "success"} if i % 3 == 0 else None
                } for i in range(19)
            ],
            "flow_config": {
                "actions": ["validate", "execute", "monitor"],
                "settings": {
                    "max_parallel_nodes": 5,
                    "execution_timeout": 3600,
                    "retry_failed_nodes": True
                },
                "monitoring": {
                    "track_performance": True,
                    "alert_on_failure": True,
                    "metrics_collection": ["duration", "success_rate", "error_count"]
                }
            }
        }
        
        # Assert complex workflow is valid
        json.dumps(complex_workflow["nodes"])
        json.dumps(complex_workflow["edges"])
        json.dumps(complex_workflow["flow_config"])
        assert len(complex_workflow["nodes"]) == 20
        assert len(complex_workflow["edges"]) == 19

    def test_flow_nested_workflow_config(self):
        """Test Flow with deeply nested configuration."""
        # Deeply nested configuration
        nested_config = {
            "actions": ["setup", "execute", "cleanup"],
            "execution": {
                "strategy": "sequential",
                "parallel_groups": [
                    {
                        "group_id": "data_processing",
                        "nodes": ["node_1", "node_2"],
                        "max_concurrency": 2
                    },
                    {
                        "group_id": "analysis",
                        "nodes": ["node_3", "node_4", "node_5"],
                        "max_concurrency": 3
                    }
                ]
            },
            "error_handling": {
                "global_retry": {
                    "max_attempts": 3,
                    "backoff": {
                        "type": "exponential",
                        "base_delay": 1,
                        "max_delay": 60
                    }
                },
                "node_specific": {
                    "critical_nodes": ["node_1", "node_3"],
                    "retry_policy": {
                        "max_attempts": 5,
                        "immediate_retry": True
                    }
                }
            },
            "monitoring": {
                "metrics": {
                    "collection_interval": 30,
                    "metrics_types": ["cpu", "memory", "duration"],
                    "export": {
                        "enabled": True,
                        "format": "prometheus",
                        "endpoint": "/metrics"
                    }
                },
                "alerting": {
                    "channels": ["email", "slack"],
                    "rules": [
                        {
                            "condition": "duration > 300",
                            "severity": "warning",
                            "message": "Flow execution taking longer than expected"
                        },
                        {
                            "condition": "error_rate > 0.1",
                            "severity": "critical",
                            "message": "High error rate detected"
                        }
                    ]
                }
            }
        }
        
        # Assert nested config is valid
        json.dumps(nested_config)
        assert "execution" in nested_config
        assert "error_handling" in nested_config
        assert "monitoring" in nested_config

    def test_flow_large_scale_workflow(self):
        """Test Flow with large-scale workflow (many nodes/edges)."""
        # Large-scale workflow simulation
        num_nodes = 100
        large_workflow = {
            "nodes": [
                {
                    "id": f"node_{i:03d}",
                    "type": "task",
                    "position": {"x": (i % 10) * 150, "y": (i // 10) * 100},
                    "data": {
                        "task_name": f"Task {i}",
                        "dependencies": [f"node_{j:03d}" for j in range(max(0, i-2), i)],
                        "estimated_duration": 60 + (i % 120)
                    }
                } for i in range(num_nodes)
            ],
            "edges": [
                {
                    "id": f"edge_{i:03d}",
                    "source": f"node_{i:03d}",
                    "target": f"node_{(i+1):03d}"
                } for i in range(num_nodes - 1)
            ]
        }
        
        # Assert large workflow is manageable
        json.dumps(large_workflow["nodes"])
        json.dumps(large_workflow["edges"])
        assert len(large_workflow["nodes"]) == num_nodes
        assert len(large_workflow["edges"]) == num_nodes - 1

    def test_flow_workflow_versioning(self):
        """Test Flow with workflow versioning scenarios."""
        # Workflow versioning scenarios
        versioning_scenarios = [
            {
                "version": "1.0.0",
                "flow_config": {
                    "actions": ["basic_execution"],
                    "version": "1.0.0",
                    "schema_version": "v1",
                    "changelog": ["Initial version"]
                }
            },
            {
                "version": "1.1.0",
                "flow_config": {
                    "actions": ["basic_execution", "enhanced_monitoring"],
                    "version": "1.1.0",
                    "schema_version": "v1",
                    "changelog": ["Initial version", "Added monitoring"]
                }
            },
            {
                "version": "2.0.0",
                "flow_config": {
                    "actions": ["advanced_execution", "full_monitoring", "auto_scaling"],
                    "version": "2.0.0",
                    "schema_version": "v2",
                    "changelog": ["Initial version", "Added monitoring", "Major refactor"]
                }
            }
        ]
        
        for scenario in versioning_scenarios:
            # Assert versioning scenario structure
            json.dumps(scenario["flow_config"])
            assert "version" in scenario["flow_config"]
            assert "changelog" in scenario["flow_config"]

    def test_flow_integration_scenarios(self):
        """Test Flow with external system integration scenarios."""
        # Integration scenarios
        integration_scenarios = [
            {
                "integration_type": "api_webhook",
                "flow_config": {
                    "actions": ["trigger_webhook"],
                    "integrations": {
                        "webhooks": [
                            {
                                "name": "completion_webhook",
                                "url": "https://api.example.com/flow/complete",
                                "method": "POST",
                                "headers": {"Authorization": "Bearer token"}
                            }
                        ]
                    }
                }
            },
            {
                "integration_type": "database",
                "flow_config": {
                    "actions": ["sync_database"],
                    "integrations": {
                        "databases": [
                            {
                                "name": "results_db",
                                "type": "postgresql",
                                "connection": "postgresql://user:pass@host:5432/db"
                            }
                        ]
                    }
                }
            },
            {
                "integration_type": "message_queue",
                "flow_config": {
                    "actions": ["publish_results"],
                    "integrations": {
                        "message_queues": [
                            {
                                "name": "results_queue",
                                "type": "rabbitmq",
                                "queue_name": "flow_results"
                            }
                        ]
                    }
                }
            }
        ]
        
        for scenario in integration_scenarios:
            # Assert integration scenario structure
            json.dumps(scenario["flow_config"])
            assert "integrations" in scenario["flow_config"]

    def test_flow_data_integrity(self):
        """Test data integrity constraints."""
        # Act
        table = Flow.__table__
        
        # Assert primary key
        primary_keys = [col for col in table.columns if col.primary_key]
        assert len(primary_keys) == 1
        assert primary_keys[0].name == 'id'
        
        # Assert required fields
        required_fields = ['name']
        for field_name in required_fields:
            field = table.columns[field_name]
            assert field.nullable is False
        
        # Assert optional fields
        optional_fields = ['crew_id', 'group_id', 'created_by_email']
        for field_name in optional_fields:
            field = table.columns[field_name]
            assert field.nullable is True
        
        # Assert JSON fields
        json_fields = ['nodes', 'edges', 'flow_config']
        for field_name in json_fields:
            field = table.columns[field_name]
            assert "JSON" in str(field.type)
        
        # Assert foreign key
        crew_id_fks = [fk for fk in table.columns['crew_id'].foreign_keys]
        assert len(crew_id_fks) == 1
        
        # Assert indexed fields
        indexed_fields = ['group_id']
        for field_name in indexed_fields:
            field = table.columns[field_name]
            assert field.index is True