"""
Unit tests for flow schemas.

Tests the functionality of Pydantic schemas for flow operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from uuid import uuid4, UUID
from pydantic import ValidationError

from src.schemas.flow import (
    Position, Style, NodeData, Node, Edge, FlowBase, FlowCreate, 
    FlowUpdate, FlowInDBBase, Flow, FlowResponse
)


class TestPosition:
    """Test cases for Position schema."""
    
    def test_valid_position(self):
        """Test Position with valid coordinates."""
        position_data = {"x": 100.5, "y": 200.75}
        position = Position(**position_data)
        assert position.x == 100.5
        assert position.y == 200.75
    
    def test_position_missing_coordinates(self):
        """Test Position validation with missing coordinates."""
        # Missing x coordinate
        with pytest.raises(ValidationError) as exc_info:
            Position(y=100)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "x" in missing_fields
        
        # Missing y coordinate
        with pytest.raises(ValidationError) as exc_info:
            Position(x=100)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "y" in missing_fields
    
    def test_position_negative_coordinates(self):
        """Test Position with negative coordinates."""
        position = Position(x=-50, y=-100)
        assert position.x == -50
        assert position.y == -100
    
    def test_position_zero_coordinates(self):
        """Test Position with zero coordinates."""
        position = Position(x=0, y=0)
        assert position.x == 0
        assert position.y == 0
    
    def test_position_large_coordinates(self):
        """Test Position with large coordinates."""
        position = Position(x=9999.99, y=10000.01)
        assert position.x == 9999.99
        assert position.y == 10000.01


class TestStyle:
    """Test cases for Style schema."""
    
    def test_valid_style_minimal(self):
        """Test Style with no styling properties."""
        style = Style()
        assert style.background is None
        assert style.border is None
        assert style.borderRadius is None
        assert style.padding is None
        assert style.boxShadow is None
    
    def test_valid_style_complete(self):
        """Test Style with all styling properties."""
        style_data = {
            "background": "#ffffff",
            "border": "2px solid #000000",
            "borderRadius": "8px",
            "padding": "10px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
        }
        style = Style(**style_data)
        assert style.background == "#ffffff"
        assert style.border == "2px solid #000000"
        assert style.borderRadius == "8px"
        assert style.padding == "10px"
        assert style.boxShadow == "0 2px 4px rgba(0,0,0,0.1)"
    
    def test_style_partial_styling(self):
        """Test Style with partial styling properties."""
        style_data = {
            "background": "#f0f0f0",
            "borderRadius": "4px"
        }
        style = Style(**style_data)
        assert style.background == "#f0f0f0"
        assert style.border is None
        assert style.borderRadius == "4px"
        assert style.padding is None
        assert style.boxShadow is None
    
    def test_style_css_values(self):
        """Test Style with various CSS values."""
        css_styles = [
            {"background": "linear-gradient(45deg, #ff0000, #00ff00)"},
            {"border": "1px dashed red"},
            {"borderRadius": "50%"},
            {"padding": "5px 10px 15px 20px"},
            {"boxShadow": "inset 0 1px 2px rgba(255,255,255,0.5)"}
        ]
        
        for css_style in css_styles:
            style = Style(**css_style)
            key, value = list(css_style.items())[0]
            assert getattr(style, key) == value


class TestNodeData:
    """Test cases for NodeData schema."""
    
    def test_valid_node_data_minimal(self):
        """Test NodeData with minimal required fields."""
        node_data = {"label": "Start Node"}
        data = NodeData(**node_data)
        assert data.label == "Start Node"
        assert data.crewName is None
        assert data.type is None
        assert data.decorator is None
        assert data.listenTo is None
        assert data.routerCondition is None
        assert data.stateType is None
        assert data.stateDefinition is None
        assert data.listener is None
    
    def test_valid_node_data_complete(self):
        """Test NodeData with all fields."""
        listener_data = {
            "event": "task_completed",
            "handler": "process_completion",
            "config": {"timeout": 30}
        }
        
        node_data = {
            "label": "Data Processing Node",
            "crewName": "data_analysis_crew",
            "type": "task",
            "decorator": "@task_decorator",
            "listenTo": ["node1", "node2"],
            "routerCondition": "input.status == 'ready'",
            "stateType": "processing",
            "stateDefinition": "{'status': 'active', 'progress': 0}",
            "listener": listener_data
        }
        data = NodeData(**node_data)
        assert data.label == "Data Processing Node"
        assert data.crewName == "data_analysis_crew"
        assert data.type == "task"
        assert data.decorator == "@task_decorator"
        assert data.listenTo == ["node1", "node2"]
        assert data.routerCondition == "input.status == 'ready'"
        assert data.stateType == "processing"
        assert data.stateDefinition == "{'status': 'active', 'progress': 0}"
        assert data.listener == listener_data
    
    def test_node_data_missing_label(self):
        """Test NodeData validation with missing label."""
        with pytest.raises(ValidationError) as exc_info:
            NodeData(type="task")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "label" in missing_fields
    
    def test_node_data_empty_label(self):
        """Test NodeData with empty label."""
        data = NodeData(label="")
        assert data.label == ""
    
    def test_node_data_listen_to_empty_list(self):
        """Test NodeData with empty listenTo list."""
        data = NodeData(label="Test Node", listenTo=[])
        assert data.listenTo == []
    
    def test_node_data_complex_listener(self):
        """Test NodeData with complex listener configuration."""
        complex_listener = {
            "events": ["start", "complete", "error"],
            "handlers": {
                "start": "on_start_handler",
                "complete": "on_complete_handler",
                "error": "on_error_handler"
            },
            "config": {
                "retry_count": 3,
                "timeout": 60,
                "async": True
            }
        }
        
        data = NodeData(label="Complex Node", listener=complex_listener)
        assert data.listener["events"] == ["start", "complete", "error"]
        assert data.listener["config"]["retry_count"] == 3


class TestNode:
    """Test cases for Node schema."""
    
    def test_valid_node_minimal(self):
        """Test Node with minimal required fields."""
        position = Position(x=100, y=200)
        node_data = NodeData(label="Test Node")
        
        node = Node(
            id="node_1",
            type="default",
            position=position,
            data=node_data
        )
        assert node.id == "node_1"
        assert node.type == "default"
        assert node.position.x == 100
        assert node.position.y == 200
        assert node.data.label == "Test Node"
        assert node.width is None
        assert node.height is None
        assert node.selected is None
        assert node.positionAbsolute is None
        assert node.dragging is None
        assert node.style is None
    
    def test_valid_node_complete(self):
        """Test Node with all fields."""
        position = Position(x=150, y=250)
        absolute_position = Position(x=160, y=260)
        node_data = NodeData(
            label="Complete Node",
            type="task",
            crewName="test_crew"
        )
        style = Style(
            background="#ffffff",
            border="1px solid #ccc"
        )
        
        node = Node(
            id="complete_node",
            type="custom",
            position=position,
            data=node_data,
            width=200.0,
            height=100.0,
            selected=True,
            positionAbsolute=absolute_position,
            dragging=False,
            style=style
        )
        assert node.id == "complete_node"
        assert node.type == "custom"
        assert node.width == 200.0
        assert node.height == 100.0
        assert node.selected is True
        assert node.positionAbsolute.x == 160
        assert node.dragging is False
        assert node.style.background == "#ffffff"
    
    def test_node_missing_required_fields(self):
        """Test Node validation with missing required fields."""
        required_fields = ["id", "type", "position", "data"]
        
        for missing_field in required_fields:
            node_data_dict = {
                "id": "test_node",
                "type": "default",
                "position": Position(x=0, y=0),
                "data": NodeData(label="Test")
            }
            del node_data_dict[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                Node(**node_data_dict)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_node_dimensions(self):
        """Test Node with various dimensions."""
        position = Position(x=0, y=0)
        node_data = NodeData(label="Dimension Test")
        
        # Zero dimensions
        node_zero = Node(
            id="zero_node",
            type="default",
            position=position,
            data=node_data,
            width=0.0,
            height=0.0
        )
        assert node_zero.width == 0.0
        assert node_zero.height == 0.0
        
        # Large dimensions
        node_large = Node(
            id="large_node",
            type="default",
            position=position,
            data=node_data,
            width=1000.5,
            height=800.25
        )
        assert node_large.width == 1000.5
        assert node_large.height == 800.25


class TestEdge:
    """Test cases for Edge schema."""
    
    def test_valid_edge_minimal(self):
        """Test Edge with minimal required fields."""
        edge = Edge(
            source="node1",
            target="node2",
            id="edge1"
        )
        assert edge.source == "node1"
        assert edge.target == "node2"
        assert edge.id == "edge1"
        assert edge.sourceHandle is None
        assert edge.targetHandle is None
    
    def test_valid_edge_complete(self):
        """Test Edge with all fields."""
        edge = Edge(
            source="start_node",
            target="end_node",
            id="connection_edge",
            sourceHandle="output",
            targetHandle="input"
        )
        assert edge.source == "start_node"
        assert edge.target == "end_node"
        assert edge.id == "connection_edge"
        assert edge.sourceHandle == "output"
        assert edge.targetHandle == "input"
    
    def test_edge_missing_required_fields(self):
        """Test Edge validation with missing required fields."""
        required_fields = ["source", "target", "id"]
        
        for missing_field in required_fields:
            edge_data = {
                "source": "node1",
                "target": "node2", 
                "id": "edge1"
            }
            del edge_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                Edge(**edge_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_edge_same_source_target(self):
        """Test Edge with same source and target (self-loop)."""
        edge = Edge(
            source="node1",
            target="node1", 
            id="self_loop"
        )
        assert edge.source == edge.target == "node1"
    
    def test_edge_various_handles(self):
        """Test Edge with various handle configurations."""
        handle_configs = [
            {"sourceHandle": "out1", "targetHandle": "in1"},
            {"sourceHandle": "output_port", "targetHandle": "input_port"},
            {"sourceHandle": "default", "targetHandle": "default"},
            {"sourceHandle": None, "targetHandle": "input"},
            {"sourceHandle": "output", "targetHandle": None}
        ]
        
        for i, handles in enumerate(handle_configs):
            edge = Edge(
                source=f"source_{i}",
                target=f"target_{i}",
                id=f"edge_{i}",
                **handles
            )
            assert edge.sourceHandle == handles["sourceHandle"]
            assert edge.targetHandle == handles["targetHandle"]


class TestFlowBase:
    """Test cases for FlowBase schema."""
    
    def test_valid_flow_base_minimal(self):
        """Test FlowBase with minimal required fields."""
        flow = FlowBase(name="Test Flow")
        assert flow.name == "Test Flow"
        assert flow.crew_id is None
        assert flow.nodes == []
        assert flow.edges == []
        assert flow.flow_config is None
    
    def test_valid_flow_base_complete(self):
        """Test FlowBase with all fields."""
        crew_id = uuid4()
        position = Position(x=100, y=100)
        node_data = NodeData(label="Flow Node")
        node = Node(id="node1", type="default", position=position, data=node_data)
        edge = Edge(source="node1", target="node2", id="edge1")
        config = {"version": "1.0", "description": "Test flow configuration"}
        
        flow = FlowBase(
            name="Complete Flow",
            crew_id=crew_id,
            nodes=[node],
            edges=[edge],
            flow_config=config
        )
        assert flow.name == "Complete Flow"
        assert flow.crew_id == crew_id
        assert len(flow.nodes) == 1
        assert len(flow.edges) == 1
        assert flow.flow_config == config
    
    def test_flow_base_missing_name(self):
        """Test FlowBase validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            FlowBase()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_flow_base_empty_name(self):
        """Test FlowBase with empty name."""
        flow = FlowBase(name="")
        assert flow.name == ""
    
    def test_flow_base_complex_config(self):
        """Test FlowBase with complex flow configuration."""
        complex_config = {
            "metadata": {
                "version": "2.1",
                "author": "test_user",
                "description": "Complex workflow for data processing",
                "tags": ["data", "processing", "analytics"]
            },
            "execution": {
                "parallel": True,
                "max_workers": 4,
                "timeout": 3600
            },
            "variables": {
                "input_path": "/data/input",
                "output_path": "/data/output",
                "batch_size": 1000
            }
        }
        
        flow = FlowBase(name="Complex Flow", flow_config=complex_config)
        assert flow.flow_config["metadata"]["version"] == "2.1"
        assert flow.flow_config["execution"]["parallel"] is True
        assert flow.flow_config["variables"]["batch_size"] == 1000


class TestFlowCreate:
    """Test cases for FlowCreate schema."""
    
    def test_flow_create_inheritance(self):
        """Test that FlowCreate inherits from FlowBase."""
        crew_id = uuid4()
        create_data = {
            "name": "Create Flow Test",
            "crew_id": crew_id,
            "flow_config": {"test": True}
        }
        create_flow = FlowCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_flow, 'name')
        assert hasattr(create_flow, 'crew_id')
        assert hasattr(create_flow, 'nodes')
        assert hasattr(create_flow, 'edges')
        assert hasattr(create_flow, 'flow_config')
        
        # Values should match
        assert create_flow.name == "Create Flow Test"
        assert create_flow.crew_id == crew_id
        assert create_flow.flow_config == {"test": True}
    
    def test_flow_create_model_config(self):
        """Test FlowCreate model configuration."""
        assert hasattr(FlowCreate, 'model_config')
        assert FlowCreate.model_config.get('from_attributes') is True


class TestFlowUpdate:
    """Test cases for FlowUpdate schema."""
    
    def test_valid_flow_update_minimal(self):
        """Test FlowUpdate with minimal required fields."""
        update = FlowUpdate(name="Updated Flow")
        assert update.name == "Updated Flow"
        assert update.flow_config is None
    
    def test_valid_flow_update_complete(self):
        """Test FlowUpdate with all fields."""
        config = {"updated": True, "version": "1.1"}
        update = FlowUpdate(
            name="Completely Updated Flow",
            flow_config=config
        )
        assert update.name == "Completely Updated Flow"
        assert update.flow_config == config
    
    def test_flow_update_missing_name(self):
        """Test FlowUpdate validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            FlowUpdate(flow_config={"test": True})
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_flow_update_model_config(self):
        """Test FlowUpdate model configuration."""
        assert hasattr(FlowUpdate, 'model_config')
        assert FlowUpdate.model_config.get('from_attributes') is True


class TestFlowInDBBase:
    """Test cases for FlowInDBBase schema."""
    
    def test_valid_flow_in_db_base(self):
        """Test FlowInDBBase with valid data."""
        flow_id = uuid4()
        crew_id = uuid4()
        now = datetime.now()
        
        db_flow = FlowInDBBase(
            name="DB Flow",
            crew_id=crew_id,
            id=flow_id,
            created_at=now,
            updated_at=now
        )
        assert db_flow.name == "DB Flow"
        assert db_flow.crew_id == crew_id
        assert db_flow.id == flow_id
        assert db_flow.created_at == now
        assert db_flow.updated_at == now
    
    def test_flow_in_db_base_missing_db_fields(self):
        """Test FlowInDBBase validation with missing database fields."""
        db_fields = ["id", "created_at", "updated_at"]
        
        for missing_field in db_fields:
            flow_data = {
                "name": "Test Flow",
                "id": uuid4(),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            del flow_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                FlowInDBBase(**flow_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_flow_in_db_base_model_config(self):
        """Test FlowInDBBase model configuration."""
        assert hasattr(FlowInDBBase, 'model_config')
        assert FlowInDBBase.model_config.get('from_attributes') is True


class TestFlow:
    """Test cases for Flow schema."""
    
    def test_flow_inheritance(self):
        """Test that Flow inherits from FlowInDBBase."""
        flow_id = uuid4()
        now = datetime.now()
        
        flow = Flow(
            name="Inherited Flow",
            id=flow_id,
            created_at=now,
            updated_at=now
        )
        
        # Should have all inherited attributes
        assert hasattr(flow, 'name')
        assert hasattr(flow, 'crew_id')
        assert hasattr(flow, 'nodes')
        assert hasattr(flow, 'edges')
        assert hasattr(flow, 'flow_config')
        assert hasattr(flow, 'id')
        assert hasattr(flow, 'created_at')
        assert hasattr(flow, 'updated_at')
        
        # Values should be accessible
        assert flow.name == "Inherited Flow"
        assert flow.id == flow_id
        assert flow.created_at == now
        assert flow.updated_at == now


class TestFlowResponse:
    """Test cases for FlowResponse schema."""
    
    def test_valid_flow_response_minimal(self):
        """Test FlowResponse with minimal required fields."""
        flow_id = uuid4()
        response = FlowResponse(
            id=flow_id,
            name="Response Flow",
            nodes=[],
            edges=[],
            created_at="2023-01-01T12:00:00",
            updated_at="2023-01-01T12:30:00"
        )
        assert response.id == flow_id
        assert response.name == "Response Flow"
        assert response.crew_id is None
        assert response.nodes == []
        assert response.edges == []
        assert response.flow_config == {}  # Default factory
        assert response.created_at == "2023-01-01T12:00:00"
        assert response.updated_at == "2023-01-01T12:30:00"
    
    def test_valid_flow_response_complete(self):
        """Test FlowResponse with all fields."""
        flow_id = uuid4()
        crew_id = uuid4()
        position = Position(x=50, y=75)
        node_data = NodeData(label="Response Node")
        node = Node(id="resp_node", type="response", position=position, data=node_data)
        edge = Edge(source="resp_node", target="end", id="resp_edge")
        config = {"response_config": True}
        
        response = FlowResponse(
            id=flow_id,
            name="Complete Response Flow",
            crew_id=crew_id,
            nodes=[node],
            edges=[edge],
            flow_config=config,
            created_at="2023-01-01T10:00:00Z",
            updated_at="2023-01-01T11:00:00Z"
        )
        assert response.id == flow_id
        assert response.name == "Complete Response Flow"
        assert response.crew_id == crew_id
        assert len(response.nodes) == 1
        assert len(response.edges) == 1
        assert response.flow_config == config
        assert response.created_at == "2023-01-01T10:00:00Z"
        assert response.updated_at == "2023-01-01T11:00:00Z"
    
    def test_flow_response_missing_required_fields(self):
        """Test FlowResponse validation with missing required fields."""
        required_fields = ["id", "name", "nodes", "edges", "created_at", "updated_at"]
        
        for missing_field in required_fields:
            response_data = {
                "id": uuid4(),
                "name": "Test Response",
                "nodes": [],
                "edges": [],
                "created_at": "2023-01-01T12:00:00",
                "updated_at": "2023-01-01T12:00:00"
            }
            del response_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                FlowResponse(**response_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_flow_response_string_id(self):
        """Test FlowResponse with string ID."""
        response = FlowResponse(
            id="string-flow-id-123",
            name="String ID Flow",
            nodes=[],
            edges=[],
            created_at="2023-01-01T12:00:00",
            updated_at="2023-01-01T12:00:00"
        )
        assert response.id == "string-flow-id-123"
    
    def test_flow_response_model_config(self):
        """Test FlowResponse model configuration."""
        assert hasattr(FlowResponse, 'model_config')
        assert FlowResponse.model_config.get('from_attributes') is True


class TestFlowSchemaIntegration:
    """Integration tests for flow schema interactions."""
    
    def test_complete_flow_creation_workflow(self):
        """Test complete flow creation workflow."""
        # Create nodes
        start_position = Position(x=100, y=100)
        start_data = NodeData(
            label="Start",
            type="start",
            stateType="initial"
        )
        start_node = Node(
            id="start",
            type="start",
            position=start_position,
            data=start_data,
            width=120.0,
            height=80.0
        )
        
        process_position = Position(x=300, y=100)
        process_data = NodeData(
            label="Process Data",
            type="task",
            crewName="data_processing_crew",
            listenTo=["start"]
        )
        process_node = Node(
            id="process",
            type="task",
            position=process_position,
            data=process_data,
            width=150.0,
            height=100.0
        )
        
        # Create edges
        start_to_process = Edge(
            source="start",
            target="process",
            id="start_to_process",
            sourceHandle="output",
            targetHandle="input"
        )
        
        # Create flow
        crew_id = uuid4()
        flow_config = {
            "execution_mode": "sequential",
            "timeout": 3600,
            "retry_policy": {"max_retries": 3, "backoff": "exponential"}
        }
        
        create_flow = FlowCreate(
            name="Data Processing Flow",
            crew_id=crew_id,
            nodes=[start_node, process_node],
            edges=[start_to_process],
            flow_config=flow_config
        )
        
        # Simulate database insertion
        flow_id = uuid4()
        now = datetime.now()
        db_flow = FlowInDBBase(
            **create_flow.model_dump(),
            id=flow_id,
            created_at=now,
            updated_at=now
        )
        
        # Create response
        flow_response = FlowResponse(
            id=str(flow_id),
            name=db_flow.name,
            crew_id=db_flow.crew_id,
            nodes=db_flow.nodes,
            edges=db_flow.edges,
            flow_config=db_flow.flow_config,
            created_at=now.isoformat(),
            updated_at=now.isoformat()
        )
        
        # Verify workflow
        assert create_flow.name == "Data Processing Flow"
        assert len(create_flow.nodes) == 2
        assert len(create_flow.edges) == 1
        assert db_flow.id == flow_id
        assert flow_response.name == create_flow.name
        assert len(flow_response.nodes) == len(create_flow.nodes)
    
    def test_flow_update_workflow(self):
        """Test flow update workflow."""
        # Original flow
        original_flow = FlowInDBBase(
            name="Original Flow",
            id=uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Update data
        new_config = {
            "updated": True,
            "version": "2.0",
            "new_features": ["validation", "monitoring"]
        }
        update_data = FlowUpdate(
            name="Updated Flow",
            flow_config=new_config
        )
        
        # Simulate update
        updated_flow = FlowInDBBase(
            **original_flow.model_dump(exclude={"name", "flow_config", "updated_at"}),
            name=update_data.name,
            flow_config=update_data.flow_config,
            updated_at=datetime.now()
        )
        
        # Verify update
        assert updated_flow.name == "Updated Flow"
        assert updated_flow.flow_config == new_config
        assert updated_flow.id == original_flow.id
        assert updated_flow.created_at == original_flow.created_at
        assert updated_flow.updated_at > original_flow.updated_at
    
    def test_complex_flow_structure(self):
        """Test complex flow with multiple nodes and edges."""
        # Create multiple nodes with different types
        nodes = []
        edges = []
        
        # Input node
        input_node = Node(
            id="input",
            type="input",
            position=Position(x=0, y=100),
            data=NodeData(label="Input", type="input"),
            style=Style(background="#e8f5e8")
        )
        nodes.append(input_node)
        
        # Router node
        router_node = Node(
            id="router",
            type="router",
            position=Position(x=200, y=100),
            data=NodeData(
                label="Router",
                type="router",
                routerCondition="data.type == 'batch'"
            ),
            style=Style(background="#fff5e6")
        )
        nodes.append(router_node)
        
        # Processing nodes
        batch_node = Node(
            id="batch_processor",
            type="processor",
            position=Position(x=400, y=50),
            data=NodeData(
                label="Batch Processor",
                type="processor",
                crewName="batch_crew"
            )
        )
        nodes.append(batch_node)
        
        stream_node = Node(
            id="stream_processor", 
            type="processor",
            position=Position(x=400, y=150),
            data=NodeData(
                label="Stream Processor",
                type="processor",
                crewName="stream_crew"
            )
        )
        nodes.append(stream_node)
        
        # Output node
        output_node = Node(
            id="output",
            type="output",
            position=Position(x=600, y=100),
            data=NodeData(label="Output", type="output"),
            style=Style(background="#ffe8e8")
        )
        nodes.append(output_node)
        
        # Create edges
        edges.extend([
            Edge(source="input", target="router", id="input_to_router"),
            Edge(source="router", target="batch_processor", id="router_to_batch"),
            Edge(source="router", target="stream_processor", id="router_to_stream"),
            Edge(source="batch_processor", target="output", id="batch_to_output"),
            Edge(source="stream_processor", target="output", id="stream_to_output")
        ])
        
        # Create complex flow
        complex_flow = FlowCreate(
            name="Complex Data Processing Flow",
            nodes=nodes,
            edges=edges,
            flow_config={
                "execution_strategy": "conditional_parallel",
                "error_handling": "continue_on_error",
                "monitoring": {
                    "metrics": ["throughput", "latency", "error_rate"],
                    "alerts": ["high_error_rate", "slow_processing"]
                }
            }
        )
        
        # Verify complex structure
        assert len(complex_flow.nodes) == 5
        assert len(complex_flow.edges) == 5
        
        # Verify node types
        node_types = [node.data.type for node in complex_flow.nodes]
        assert "input" in node_types
        assert "router" in node_types
        assert "processor" in node_types
        assert "output" in node_types
        
        # Verify connections
        edge_connections = [(edge.source, edge.target) for edge in complex_flow.edges]
        assert ("input", "router") in edge_connections
        assert ("router", "batch_processor") in edge_connections
        assert ("router", "stream_processor") in edge_connections
    
    def test_flow_styling_and_positioning(self):
        """Test flow with comprehensive styling and positioning."""
        # Create styled nodes
        styled_nodes = []
        
        for i in range(3):
            style = Style(
                background=f"#{hex(200 + i * 20)[2:]}f0f0",
                border="2px solid #333",
                borderRadius="12px",
                padding="8px",
                boxShadow="0 4px 8px rgba(0,0,0,0.1)"
            )
            
            node = Node(
                id=f"styled_node_{i}",
                type="styled",
                position=Position(x=i * 200, y=i * 50),
                data=NodeData(label=f"Styled Node {i}"),
                width=180.0,
                height=90.0,
                style=style,
                selected=i == 1,  # Middle node selected
                dragging=False
            )
            styled_nodes.append(node)
        
        # Create flow with styled nodes
        styled_flow = FlowCreate(
            name="Styled Flow",
            nodes=styled_nodes,
            edges=[
                Edge(source="styled_node_0", target="styled_node_1", id="style_edge_1"),
                Edge(source="styled_node_1", target="styled_node_2", id="style_edge_2")
            ]
        )
        
        # Verify styling
        assert len(styled_flow.nodes) == 3
        assert all(node.style is not None for node in styled_flow.nodes)
        assert styled_flow.nodes[1].selected is True
        assert all(node.dragging is False for node in styled_flow.nodes)
        
        # Verify positioning
        positions = [(node.position.x, node.position.y) for node in styled_flow.nodes]
        assert positions == [(0, 0), (200, 50), (400, 100)]