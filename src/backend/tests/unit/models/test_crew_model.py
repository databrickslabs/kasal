"""
Unit tests for crew model.

Tests the functionality of the Crew database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
import uuid
from unittest.mock import MagicMock

from src.models.crew import Crew, Plan


class TestCrew:
    """Test cases for Crew model."""

    def test_crew_creation_minimal(self):
        """Test basic Crew model creation with minimal fields."""
        # Arrange
        name = "Data Analysis Crew"
        
        # Act
        crew = Crew(name=name)
        
        # Assert
        assert crew.name == name
        assert crew.agent_ids == []  # Set by __init__ method
        assert crew.task_ids == []  # Set by __init__ method
        assert crew.nodes == []  # Set by __init__ method
        assert crew.edges == []  # Set by __init__ method
        assert crew.group_id is None
        assert crew.created_by_email is None

    def test_crew_creation_with_all_fields(self):
        """Test Crew model creation with all fields populated."""
        # Arrange
        name = "Marketing Research Crew"
        agent_ids = ["agent-123", "agent-456", "agent-789"]
        task_ids = ["task-abc", "task-def", "task-ghi"]
        nodes = [
            {"id": "node1", "type": "agent", "position": {"x": 100, "y": 200}},
            {"id": "node2", "type": "task", "position": {"x": 300, "y": 400}}
        ]
        edges = [
            {"id": "edge1", "source": "node1", "target": "node2", "type": "workflow"}
        ]
        group_id = "marketing-team"
        created_by_email = "manager@company.com"
        created_at = datetime.utcnow()
        updated_at = datetime.utcnow()
        
        # Act
        crew = Crew(
            name=name,
            agent_ids=agent_ids,
            task_ids=task_ids,
            nodes=nodes,
            edges=edges,
            group_id=group_id,
            created_by_email=created_by_email,
            created_at=created_at,
            updated_at=updated_at
        )
        
        # Assert
        assert crew.name == name
        assert crew.agent_ids == agent_ids
        assert crew.task_ids == task_ids
        assert crew.nodes == nodes
        assert crew.edges == edges
        assert crew.group_id == group_id
        assert crew.created_by_email == created_by_email
        assert crew.created_at == created_at
        assert crew.updated_at == updated_at

    def test_crew_init_method_logic(self):
        """Test the custom __init__ method logic."""
        # Test 1: When agent_ids is None
        crew1 = Crew(
            name="Test Crew 1",
            agent_ids=None
        )
        assert crew1.agent_ids == []
        
        # Test 2: When task_ids is None
        crew2 = Crew(
            name="Test Crew 2",
            task_ids=None
        )
        assert crew2.task_ids == []
        
        # Test 3: When nodes is None
        crew3 = Crew(
            name="Test Crew 3",
            nodes=None
        )
        assert crew3.nodes == []
        
        # Test 4: When edges is None
        crew4 = Crew(
            name="Test Crew 4",
            edges=None
        )
        assert crew4.edges == []
        
        # Test 5: When all arrays are provided
        crew5 = Crew(
            name="Test Crew 5",
            agent_ids=["agent1"],
            task_ids=["task1"],
            nodes=[{"id": "node1"}],
            edges=[{"id": "edge1"}]
        )
        assert crew5.agent_ids == ["agent1"]
        assert crew5.task_ids == ["task1"]
        assert crew5.nodes == [{"id": "node1"}]
        assert crew5.edges == [{"id": "edge1"}]

    def test_crew_with_multiple_agents_and_tasks(self):
        """Test Crew with multiple agents and tasks."""
        # Arrange
        name = "Complex Analysis Crew"
        agent_ids = [
            "research-agent-001",
            "analysis-agent-002", 
            "writing-agent-003",
            "review-agent-004"
        ]
        task_ids = [
            "data-collection-task",
            "data-analysis-task",
            "report-generation-task",
            "quality-review-task"
        ]
        
        # Act
        crew = Crew(
            name=name,
            agent_ids=agent_ids,
            task_ids=task_ids
        )
        
        # Assert
        assert crew.name == name
        assert len(crew.agent_ids) == 4
        assert len(crew.task_ids) == 4
        assert "research-agent-001" in crew.agent_ids
        assert "quality-review-task" in crew.task_ids

    def test_crew_with_workflow_nodes_and_edges(self):
        """Test Crew with complex workflow nodes and edges."""
        # Arrange
        name = "Workflow Crew"
        nodes = [
            {
                "id": "start_node",
                "type": "start",
                "position": {"x": 0, "y": 100},
                "data": {"label": "Start"}
            },
            {
                "id": "agent_node_1",
                "type": "agent",
                "position": {"x": 200, "y": 100},
                "data": {
                    "agent_id": "agent-123",
                    "name": "Research Agent",
                    "config": {"max_iter": 25}
                }
            },
            {
                "id": "task_node_1",
                "type": "task",
                "position": {"x": 400, "y": 100},
                "data": {
                    "task_id": "task-456",
                    "description": "Analyze data",
                    "expected_output": "Analysis report"
                }
            },
            {
                "id": "end_node",
                "type": "end",
                "position": {"x": 600, "y": 100},
                "data": {"label": "End"}
            }
        ]
        
        edges = [
            {
                "id": "edge_1",
                "source": "start_node",
                "target": "agent_node_1",
                "type": "workflow",
                "animated": False
            },
            {
                "id": "edge_2",
                "source": "agent_node_1",
                "target": "task_node_1",
                "type": "assignment",
                "animated": True
            },
            {
                "id": "edge_3",
                "source": "task_node_1",
                "target": "end_node",
                "type": "completion",
                "animated": False
            }
        ]
        
        # Act
        crew = Crew(
            name=name,
            nodes=nodes,
            edges=edges
        )
        
        # Assert
        assert crew.name == name
        assert len(crew.nodes) == 4
        assert len(crew.edges) == 3
        assert crew.nodes[0]["type"] == "start"
        assert crew.nodes[1]["data"]["agent_id"] == "agent-123"
        assert crew.edges[1]["type"] == "assignment"
        assert crew.edges[1]["animated"] is True

    def test_crew_multi_tenant_fields(self):
        """Test multi-tenant fields for group isolation."""
        # Arrange
        group_id = "research-division"
        created_by_email = "lead@research-division.com"
        
        # Act
        crew = Crew(
            name="Research Crew",
            group_id=group_id,
            created_by_email=created_by_email
        )
        
        # Assert
        assert crew.group_id == group_id
        assert crew.created_by_email == created_by_email

    def test_crew_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Crew.__tablename__ == "crews"

    def test_crew_primary_key_uuid(self):
        """Test that primary key uses UUID generation."""
        # Act
        crew = Crew(name="UUID Test Crew")
        
        # Assert
        # Note: The actual UUID is generated when saved to database
        # Here we test that the default function is set correctly
        id_column = Crew.__table__.columns['id']
        assert id_column.primary_key is True
        # Check that the default is a callable (the uuid.uuid4 function)
        assert callable(id_column.default.arg)
        assert id_column.default.arg.__name__ == 'uuid4'

    def test_crew_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = Crew.__table__.columns
        
        # Assert
        assert columns['id'].index is True
        assert columns['name'].index is True
        assert columns['group_id'].index is True

    def test_crew_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Crew.__table__.columns
        
        # Assert
        # UUID column
        assert "UUID" in str(columns['id'].type)
        
        # String columns
        assert "VARCHAR" in str(columns['name'].type) or "STRING" in str(columns['name'].type)
        assert "VARCHAR" in str(columns['group_id'].type) or "STRING" in str(columns['group_id'].type)
        assert "VARCHAR" in str(columns['created_by_email'].type) or "STRING" in str(columns['created_by_email'].type)
        
        # JSON columns
        assert "JSON" in str(columns['agent_ids'].type)
        assert "JSON" in str(columns['task_ids'].type)
        assert "JSON" in str(columns['nodes'].type)
        assert "JSON" in str(columns['edges'].type)
        
        # DateTime columns
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)
        
        # Nullable constraints
        assert columns['nodes'].nullable is True
        assert columns['edges'].nullable is True
        assert columns['group_id'].nullable is True
        assert columns['created_by_email'].nullable is True

    def test_crew_json_field_defaults(self):
        """Test that JSON fields have correct default configurations."""
        # Act
        columns = Crew.__table__.columns
        
        # Assert
        # Check that agent_ids and task_ids have lambda defaults
        assert columns['agent_ids'].default is not None
        assert columns['task_ids'].default is not None
        assert callable(columns['agent_ids'].default.arg)
        assert callable(columns['task_ids'].default.arg)

    def test_crew_timestamp_defaults(self):
        """Test timestamp column defaults."""
        # Act
        columns = Crew.__table__.columns
        
        # Assert
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_crew_repr(self):
        """Test string representation of Crew model."""
        # Arrange
        crew = Crew(name="Test Crew")
        
        # Act
        repr_str = repr(crew)
        
        # Assert
        assert "Crew" in repr_str

    def test_crew_with_empty_collections(self):
        """Test Crew with explicitly empty collections."""
        # Arrange & Act
        crew = Crew(
            name="Empty Crew",
            agent_ids=[],
            task_ids=[],
            nodes=[],
            edges=[]
        )
        
        # Assert
        assert crew.agent_ids == []
        assert crew.task_ids == []
        assert crew.nodes == []
        assert crew.edges == []

    def test_crew_with_complex_node_data(self):
        """Test Crew with complex node data structures."""
        # Arrange
        complex_nodes = [
            {
                "id": "complex_agent",
                "type": "agent",
                "position": {"x": 100, "y": 200},
                "data": {
                    "agent_config": {
                        "name": "Complex Agent",
                        "role": "Data Scientist",
                        "goal": "Analyze complex datasets",
                        "backstory": "Experienced data scientist with PhD",
                        "tools": ["python_repl", "sql_query", "data_viz"],
                        "llm": "gpt-4",
                        "max_iter": 50,
                        "verbose": True,
                        "memory": True,
                        "embedder_config": {
                            "provider": "openai",
                            "model": "text-embedding-ada-002"
                        }
                    }
                },
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",
                    "version": "1.0"
                }
            }
        ]
        
        # Act
        crew = Crew(
            name="Complex Node Crew",
            nodes=complex_nodes
        )
        
        # Assert
        assert len(crew.nodes) == 1
        node = crew.nodes[0]
        assert node["type"] == "agent"
        assert node["data"]["agent_config"]["name"] == "Complex Agent"
        assert node["data"]["agent_config"]["tools"] == ["python_repl", "sql_query", "data_viz"]
        assert node["metadata"]["version"] == "1.0"

    def test_crew_edge_types_and_properties(self):
        """Test different edge types and their properties."""
        # Arrange
        edges = [
            {
                "id": "sequential_edge",
                "source": "agent1",
                "target": "agent2",
                "type": "sequential",
                "properties": {"delay": 0, "condition": None}
            },
            {
                "id": "parallel_edge",
                "source": "start",
                "target": "agent_group",
                "type": "parallel",
                "properties": {"max_concurrent": 3}
            },
            {
                "id": "conditional_edge",
                "source": "decision_node",
                "target": "path_a",
                "type": "conditional",
                "properties": {
                    "condition": "output.success == True",
                    "priority": 1
                }
            }
        ]
        
        # Act
        crew = Crew(
            name="Edge Types Crew",
            edges=edges
        )
        
        # Assert
        assert len(crew.edges) == 3
        assert crew.edges[0]["type"] == "sequential"
        assert crew.edges[1]["properties"]["max_concurrent"] == 3
        assert crew.edges[2]["properties"]["condition"] == "output.success == True"


class TestPlan:
    """Test cases for Plan model (backward compatibility alias)."""

    def test_plan_is_crew_alias(self):
        """Test that Plan is an alias for Crew."""
        # Act & Assert
        assert issubclass(Plan, Crew)

    def test_plan_functionality(self):
        """Test that Plan alias works the same as Crew."""
        # Arrange
        name = "Test Plan"
        agent_ids = ["agent-1", "agent-2"]
        
        # Act
        plan_via_alias = Plan(
            name=name,
            agent_ids=agent_ids
        )
        
        crew_via_class = Crew(
            name=name + "_crew",
            agent_ids=agent_ids
        )
        
        # Assert
        assert plan_via_alias.agent_ids == crew_via_class.agent_ids
        assert isinstance(plan_via_alias, Crew)
        assert plan_via_alias.__tablename__ == crew_via_class.__tablename__

    def test_plan_uses_same_table(self):
        """Test that Plan uses the same database table as Crew."""
        # Act
        plan = Plan(name="Test Plan")
        crew = Crew(name="Test Crew")
        
        # Assert
        assert plan.__tablename__ == crew.__tablename__ == "crews"


class TestCrewEdgeCases:
    """Test edge cases and error scenarios for Crew."""

    def test_crew_with_very_long_name(self):
        """Test Crew with very long name."""
        # Arrange
        long_name = "Very Long Crew Name " * 20  # 400 characters
        
        # Act
        crew = Crew(name=long_name)
        
        # Assert
        assert crew.name == long_name
        assert len(crew.name) == 400

    def test_crew_with_large_agent_list(self):
        """Test Crew with large number of agents."""
        # Arrange
        large_agent_list = [f"agent-{i:04d}" for i in range(100)]
        
        # Act
        crew = Crew(
            name="Large Crew",
            agent_ids=large_agent_list
        )
        
        # Assert
        assert len(crew.agent_ids) == 100
        assert crew.agent_ids[0] == "agent-0000"
        assert crew.agent_ids[99] == "agent-0099"

    def test_crew_with_deeply_nested_nodes(self):
        """Test Crew with deeply nested node structures."""
        # Arrange
        deeply_nested_nodes = [
            {
                "id": "nested_node",
                "type": "complex",
                "data": {
                    "level1": {
                        "level2": {
                            "level3": {
                                "level4": {
                                    "config": {
                                        "settings": {
                                            "advanced": {
                                                "value": "deep_value"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
        
        # Act
        crew = Crew(
            name="Nested Crew",
            nodes=deeply_nested_nodes
        )
        
        # Assert
        assert len(crew.nodes) == 1
        deep_value = crew.nodes[0]["data"]["level1"]["level2"]["level3"]["level4"]["config"]["settings"]["advanced"]["value"]
        assert deep_value == "deep_value"

    def test_crew_common_use_cases(self):
        """Test Crew configurations for common use cases."""
        # Data Pipeline Crew
        data_pipeline_crew = Crew(
            name="Data Pipeline Crew",
            agent_ids=["data-collector", "data-processor", "data-validator"],
            task_ids=["collect-data", "process-data", "validate-results"],
            group_id="data-team"
        )
        
        # Content Creation Crew
        content_crew = Crew(
            name="Content Creation Crew",
            agent_ids=["researcher", "writer", "editor", "reviewer"],
            task_ids=["research-topic", "write-draft", "edit-content", "final-review"],
            group_id="marketing-team"
        )
        
        # Customer Support Crew
        support_crew = Crew(
            name="Customer Support Crew",
            agent_ids=["ticket-classifier", "issue-resolver", "escalation-handler"],
            task_ids=["classify-ticket", "resolve-issue", "escalate-complex"],
            group_id="support-team"
        )
        
        # Assert
        assert "data-collector" in data_pipeline_crew.agent_ids
        assert "process-data" in data_pipeline_crew.task_ids
        assert data_pipeline_crew.group_id == "data-team"
        
        assert len(content_crew.agent_ids) == 4
        assert "write-draft" in content_crew.task_ids
        
        assert support_crew.group_id == "support-team"
        assert "escalation-handler" in support_crew.agent_ids

    def test_crew_workflow_patterns(self):
        """Test common workflow patterns in crew configurations."""
        # Sequential workflow
        sequential_crew = Crew(
            name="Sequential Workflow",
            nodes=[
                {"id": "step1", "type": "agent"},
                {"id": "step2", "type": "agent"},
                {"id": "step3", "type": "agent"}
            ],
            edges=[
                {"id": "seq1", "source": "step1", "target": "step2", "type": "sequential"},
                {"id": "seq2", "source": "step2", "target": "step3", "type": "sequential"}
            ]
        )
        
        # Parallel workflow
        parallel_crew = Crew(
            name="Parallel Workflow",
            nodes=[
                {"id": "start", "type": "start"},
                {"id": "parallel1", "type": "agent"},
                {"id": "parallel2", "type": "agent"},
                {"id": "parallel3", "type": "agent"},
                {"id": "merge", "type": "merge"}
            ],
            edges=[
                {"id": "split1", "source": "start", "target": "parallel1", "type": "parallel"},
                {"id": "split2", "source": "start", "target": "parallel2", "type": "parallel"},
                {"id": "split3", "source": "start", "target": "parallel3", "type": "parallel"},
                {"id": "merge1", "source": "parallel1", "target": "merge", "type": "merge"},
                {"id": "merge2", "source": "parallel2", "target": "merge", "type": "merge"},
                {"id": "merge3", "source": "parallel3", "target": "merge", "type": "merge"}
            ]
        )
        
        # Assert
        assert len(sequential_crew.nodes) == 3
        assert len(sequential_crew.edges) == 2
        assert all(edge["type"] == "sequential" for edge in sequential_crew.edges)
        
        assert len(parallel_crew.nodes) == 5
        assert len(parallel_crew.edges) == 6
        parallel_edges = [edge for edge in parallel_crew.edges if edge["type"] == "parallel"]
        assert len(parallel_edges) == 3