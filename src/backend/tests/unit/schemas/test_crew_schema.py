"""
Unit tests for crew schemas.

Tests the functionality of Pydantic schemas for crew operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from uuid import uuid4, UUID
from pydantic import ValidationError
from typing import List, Dict, Any

from src.schemas.crew import (
    Position, Style, TaskConfig, NodeData, Node, Edge, CrewBase, CrewCreate,
    CrewUpdate, CrewInDBBase, Crew, CrewResponse, CrewGenerationRequest,
    AgentConfig, Agent, Task, CrewGenerationResponse, CrewCreationResponse
)


class TestPosition:
    """Test cases for Position schema."""
    
    def test_valid_position(self):
        """Test valid Position creation."""
        position_data = {"x": 100.5, "y": 200.0}
        position = Position(**position_data)
        assert position.x == 100.5
        assert position.y == 200.0
    
    def test_position_negative_coordinates(self):
        """Test Position with negative coordinates."""
        position_data = {"x": -50.0, "y": -100.0}
        position = Position(**position_data)
        assert position.x == -50.0
        assert position.y == -100.0
    
    def test_position_zero_coordinates(self):
        """Test Position with zero coordinates."""
        position_data = {"x": 0.0, "y": 0.0}
        position = Position(**position_data)
        assert position.x == 0.0
        assert position.y == 0.0
    
    def test_position_missing_fields(self):
        """Test Position validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            Position(x=100.0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "y" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            Position(y=100.0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "x" in missing_fields
    
    def test_position_integer_coordinates(self):
        """Test Position with integer coordinates."""
        position_data = {"x": 100, "y": 200}
        position = Position(**position_data)
        assert position.x == 100.0
        assert position.y == 200.0
        assert isinstance(position.x, float)
        assert isinstance(position.y, float)


class TestStyle:
    """Test cases for Style schema."""
    
    def test_valid_style_empty(self):
        """Test valid Style creation with no styling."""
        style = Style()
        assert style.background is None
        assert style.border is None
        assert style.borderRadius is None
        assert style.padding is None
        assert style.boxShadow is None
    
    def test_valid_style_full(self):
        """Test valid Style creation with all fields."""
        style_data = {
            "background": "#ffffff",
            "border": "1px solid #ccc",
            "borderRadius": "8px",
            "padding": "16px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
        }
        style = Style(**style_data)
        assert style.background == "#ffffff"
        assert style.border == "1px solid #ccc"
        assert style.borderRadius == "8px"
        assert style.padding == "16px"
        assert style.boxShadow == "0 2px 4px rgba(0,0,0,0.1)"
    
    def test_style_partial_fields(self):
        """Test Style with partial fields."""
        style_data = {
            "background": "#f0f0f0",
            "borderRadius": "4px"
        }
        style = Style(**style_data)
        assert style.background == "#f0f0f0"
        assert style.borderRadius == "4px"
        assert style.border is None
        assert style.padding is None
        assert style.boxShadow is None


class TestTaskConfig:
    """Test cases for TaskConfig schema."""
    
    def test_valid_task_config_defaults(self):
        """Test TaskConfig with default values."""
        config = TaskConfig()
        assert config.cache_response is False
        assert config.cache_ttl == 3600
        assert config.retry_on_fail is False
        assert config.max_retries == 3
        assert config.timeout is None
        assert config.priority == 1
        assert config.error_handling == "default"
        assert config.output_file is None
        assert config.output_json is None
        assert config.output_pydantic is None
        assert config.validation_function is None
        assert config.callback_function is None
        assert config.human_input is False
        assert config.markdown is False
    
    def test_valid_task_config_full(self):
        """Test TaskConfig with all fields specified."""
        config_data = {
            "cache_response": True,
            "cache_ttl": 7200,
            "retry_on_fail": True,
            "max_retries": 5,
            "timeout": 300,
            "priority": 2,
            "error_handling": "custom",
            "output_file": "output.txt",
            "output_json": "result.json",
            "output_pydantic": "MyModel",
            "validation_function": "validate_output",
            "callback_function": "on_complete",
            "human_input": True,
            "markdown": True
        }
        config = TaskConfig(**config_data)
        assert config.cache_response is True
        assert config.cache_ttl == 7200
        assert config.retry_on_fail is True
        assert config.max_retries == 5
        assert config.timeout == 300
        assert config.priority == 2
        assert config.error_handling == "custom"
        assert config.output_file == "output.txt"
        assert config.output_json == "result.json"
        assert config.output_pydantic == "MyModel"
        assert config.validation_function == "validate_output"
        assert config.callback_function == "on_complete"
        assert config.human_input is True
        assert config.markdown is True


class TestNodeData:
    """Test cases for NodeData schema."""
    
    def test_valid_node_data_minimal(self):
        """Test NodeData with minimal required fields."""
        node_data = {"label": "Test Node"}
        data = NodeData(**node_data)
        assert data.label == "Test Node"
        assert data.role is None
        assert data.goal is None
        assert data.backstory is None
        assert data.tools == []
        assert data.agentId is None
        assert data.taskId is None
        assert data.memory is True
        assert data.context == []
        assert data.async_execution is False
        assert data.markdown is False
    
    def test_valid_node_data_agent(self):
        """Test NodeData for an agent node."""
        node_data = {
            "label": "Data Analyst",
            "role": "analyst",
            "goal": "Analyze data effectively",
            "backstory": "Expert in data analysis",
            "tools": ["pandas", "numpy"],
            "agentId": "agent-123",
            "llm": "gpt-4",
            "max_iter": 10,
            "verbose": True,
            "allow_delegation": False,
            "cache": True,
            "memory": True
        }
        data = NodeData(**node_data)
        assert data.label == "Data Analyst"
        assert data.role == "analyst"
        assert data.goal == "Analyze data effectively"
        assert data.backstory == "Expert in data analysis"
        assert data.tools == ["pandas", "numpy"]
        assert data.agentId == "agent-123"
        assert data.llm == "gpt-4"
        assert data.max_iter == 10
        assert data.verbose is True
        assert data.allow_delegation is False
        assert data.cache is True
        assert data.memory is True
    
    def test_valid_node_data_task(self):
        """Test NodeData for a task node."""
        task_config = TaskConfig(priority=2, human_input=True)
        node_data = {
            "label": "Analysis Task",
            "type": "task",
            "description": "Perform data analysis",
            "expected_output": "Analysis report",
            "taskId": "task-456",
            "config": task_config,
            "context": ["task-123"],
            "async_execution": True,
            "markdown": True
        }
        data = NodeData(**node_data)
        assert data.label == "Analysis Task"
        assert data.type == "task"
        assert data.description == "Perform data analysis"
        assert data.expected_output == "Analysis report"
        assert data.taskId == "task-456"
        assert isinstance(data.config, TaskConfig)
        assert data.config.priority == 2
        assert data.context == ["task-123"]
        assert data.async_execution is True
        assert data.markdown is True
    
    def test_node_data_missing_label(self):
        """Test NodeData validation with missing label."""
        with pytest.raises(ValidationError) as exc_info:
            NodeData()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "label" in missing_fields


class TestNode:
    """Test cases for Node schema."""
    
    def test_valid_node_minimal(self):
        """Test Node with minimal required fields."""
        position = Position(x=100, y=200)
        node_data = NodeData(label="Test Node")
        
        node = Node(
            id="node-1",
            type="agent",
            position=position,
            data=node_data
        )
        assert node.id == "node-1"
        assert node.type == "agent"
        assert node.position.x == 100
        assert node.position.y == 200
        assert node.data.label == "Test Node"
        assert node.width is None
        assert node.height is None
        assert node.selected is None
        assert node.positionAbsolute is None
        assert node.dragging is None
        assert node.style is None
    
    def test_valid_node_full(self):
        """Test Node with all fields."""
        position = Position(x=100, y=200)
        position_absolute = Position(x=150, y=250)
        node_data = NodeData(label="Full Node", role="agent")
        style = Style(background="#ffffff", border="1px solid #ccc")
        
        node = Node(
            id="node-1",
            type="agent",
            position=position,
            data=node_data,
            width=200.0,
            height=100.0,
            selected=True,
            positionAbsolute=position_absolute,
            dragging=False,
            style=style
        )
        assert node.id == "node-1"
        assert node.type == "agent"
        assert node.width == 200.0
        assert node.height == 100.0
        assert node.selected is True
        assert node.positionAbsolute.x == 150
        assert node.positionAbsolute.y == 250
        assert node.dragging is False
        assert node.style.background == "#ffffff"
    
    def test_node_missing_required_fields(self):
        """Test Node validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Node(id="node-1")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "type" in missing_fields
        assert "position" in missing_fields
        assert "data" in missing_fields


class TestEdge:
    """Test cases for Edge schema."""
    
    def test_valid_edge_minimal(self):
        """Test Edge with minimal required fields."""
        edge = Edge(
            source="node-1",
            target="node-2",
            id="edge-1"
        )
        assert edge.source == "node-1"
        assert edge.target == "node-2"
        assert edge.id == "edge-1"
        assert edge.sourceHandle is None
        assert edge.targetHandle is None
    
    def test_valid_edge_full(self):
        """Test Edge with all fields."""
        edge = Edge(
            source="node-1",
            target="node-2",
            id="edge-1",
            sourceHandle="output",
            targetHandle="input"
        )
        assert edge.source == "node-1"
        assert edge.target == "node-2"
        assert edge.id == "edge-1"
        assert edge.sourceHandle == "output"
        assert edge.targetHandle == "input"
    
    def test_edge_missing_required_fields(self):
        """Test Edge validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Edge(source="node-1")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "target" in missing_fields
        assert "id" in missing_fields


class TestCrewBase:
    """Test cases for CrewBase schema."""
    
    def test_valid_crew_base_minimal(self):
        """Test CrewBase with minimal required fields."""
        crew = CrewBase(name="Test Crew")
        assert crew.name == "Test Crew"
        assert crew.agent_ids == []
        assert crew.task_ids == []
        assert crew.nodes == []
        assert crew.edges == []
    
    def test_valid_crew_base_full(self):
        """Test CrewBase with all fields."""
        position = Position(x=100, y=200)
        node_data = NodeData(label="Agent Node")
        node = Node(id="node-1", type="agent", position=position, data=node_data)
        edge = Edge(source="node-1", target="node-2", id="edge-1")
        
        crew = CrewBase(
            name="Full Crew",
            agent_ids=["agent-1", "agent-2"],
            task_ids=["task-1", "task-2"],
            nodes=[node],
            edges=[edge]
        )
        assert crew.name == "Full Crew"
        assert crew.agent_ids == ["agent-1", "agent-2"]
        assert crew.task_ids == ["task-1", "task-2"]
        assert len(crew.nodes) == 1
        assert len(crew.edges) == 1
        assert crew.nodes[0].id == "node-1"
        assert crew.edges[0].id == "edge-1"
    
    def test_crew_base_missing_name(self):
        """Test CrewBase validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            CrewBase()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields


class TestCrewCreate:
    """Test cases for CrewCreate schema."""
    
    def test_crew_create_inheritance(self):
        """Test that CrewCreate inherits from CrewBase."""
        crew = CrewCreate(name="Test Crew")
        assert crew.name == "Test Crew"
        assert crew.agent_ids == []
        assert crew.task_ids == []
        assert crew.nodes == []
        assert crew.edges == []
        
        # Should have all base class attributes
        assert hasattr(crew, 'name')
        assert hasattr(crew, 'agent_ids')
        assert hasattr(crew, 'task_ids')
        assert hasattr(crew, 'nodes')
        assert hasattr(crew, 'edges')


class TestCrewUpdate:
    """Test cases for CrewUpdate schema."""
    
    def test_crew_update_all_optional(self):
        """Test that all CrewUpdate fields are optional."""
        update = CrewUpdate()
        assert update.name is None
        assert update.agent_ids is None
        assert update.task_ids is None
        assert update.nodes is None
        assert update.edges is None
    
    def test_crew_update_partial(self):
        """Test CrewUpdate with partial fields."""
        update = CrewUpdate(
            name="Updated Crew",
            agent_ids=["agent-1"]
        )
        assert update.name == "Updated Crew"
        assert update.agent_ids == ["agent-1"]
        assert update.task_ids is None
        assert update.nodes is None
        assert update.edges is None
    
    def test_crew_update_full(self):
        """Test CrewUpdate with all fields."""
        position = Position(x=100, y=200)
        node_data = NodeData(label="Updated Node")
        node = Node(id="node-1", type="agent", position=position, data=node_data)
        edge = Edge(source="node-1", target="node-2", id="edge-1")
        
        update = CrewUpdate(
            name="Updated Crew",
            agent_ids=["agent-1", "agent-2"],
            task_ids=["task-1"],
            nodes=[node],
            edges=[edge]
        )
        assert update.name == "Updated Crew"
        assert update.agent_ids == ["agent-1", "agent-2"]
        assert update.task_ids == ["task-1"]
        assert len(update.nodes) == 1
        assert len(update.edges) == 1


class TestCrewInDBBase:
    """Test cases for CrewInDBBase schema."""
    
    def test_valid_crew_in_db_base(self):
        """Test CrewInDBBase with all required fields."""
        crew_id = uuid4()
        now = datetime.now()
        
        crew = CrewInDBBase(
            name="DB Crew",
            id=crew_id,
            created_at=now,
            updated_at=now
        )
        assert crew.name == "DB Crew"
        assert crew.id == crew_id
        assert crew.created_at == now
        assert crew.updated_at == now
        assert crew.agent_ids == []
        assert crew.task_ids == []
        assert crew.nodes == []
        assert crew.edges == []
    
    def test_crew_in_db_base_config(self):
        """Test CrewInDBBase model configuration."""
        assert hasattr(CrewInDBBase, 'model_config')
        assert CrewInDBBase.model_config['from_attributes'] is True
    
    def test_crew_in_db_base_missing_fields(self):
        """Test CrewInDBBase validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewInDBBase(name="Test Crew")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields


class TestCrew:
    """Test cases for Crew schema."""
    
    def test_crew_inheritance(self):
        """Test that Crew inherits from CrewInDBBase."""
        crew_id = uuid4()
        now = datetime.now()
        
        crew = Crew(
            name="Test Crew",
            id=crew_id,
            created_at=now,
            updated_at=now
        )
        assert crew.name == "Test Crew"
        assert crew.id == crew_id
        assert crew.created_at == now
        assert crew.updated_at == now
        
        # Should have all base class attributes
        assert hasattr(crew, 'name')
        assert hasattr(crew, 'id')
        assert hasattr(crew, 'created_at')
        assert hasattr(crew, 'updated_at')
        assert hasattr(crew, 'agent_ids')
        assert hasattr(crew, 'task_ids')
        assert hasattr(crew, 'nodes')
        assert hasattr(crew, 'edges')


class TestCrewResponse:
    """Test cases for CrewResponse schema."""
    
    def test_valid_crew_response(self):
        """Test CrewResponse with all required fields."""
        crew_id = uuid4()
        
        response = CrewResponse(
            id=crew_id,
            name="Response Crew",
            agent_ids=["agent-1"],
            task_ids=["task-1"],
            nodes=[],
            edges=[],
            created_at="2023-01-01T12:00:00Z",
            updated_at="2023-01-01T12:00:00Z"
        )
        assert response.id == crew_id
        assert response.name == "Response Crew"
        assert response.agent_ids == ["agent-1"]
        assert response.task_ids == ["task-1"]
        assert response.nodes == []
        assert response.edges == []
        assert response.created_at == "2023-01-01T12:00:00Z"
        assert response.updated_at == "2023-01-01T12:00:00Z"
    
    def test_crew_response_config(self):
        """Test CrewResponse model configuration."""
        assert hasattr(CrewResponse, 'model_config')
        assert CrewResponse.model_config['from_attributes'] is True
    
    def test_crew_response_missing_fields(self):
        """Test CrewResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewResponse(name="Test Crew")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "agent_ids" in missing_fields
        assert "task_ids" in missing_fields
        assert "nodes" in missing_fields
        assert "edges" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields


class TestCrewGenerationRequest:
    """Test cases for CrewGenerationRequest schema."""
    
    def test_valid_crew_generation_request_minimal(self):
        """Test CrewGenerationRequest with minimal required fields."""
        request = CrewGenerationRequest(
            prompt="Create a data analysis crew"
        )
        assert request.prompt == "Create a data analysis crew"
        assert request.model is None
        assert request.tools == []
        assert request.api_key is None
    
    def test_valid_crew_generation_request_full(self):
        """Test CrewGenerationRequest with all fields."""
        request = CrewGenerationRequest(
            prompt="Create a comprehensive data analysis crew",
            model="gpt-4",
            tools=["pandas", "numpy", "matplotlib"],
            api_key="sk-test-key"
        )
        assert request.prompt == "Create a comprehensive data analysis crew"
        assert request.model == "gpt-4"
        assert request.tools == ["pandas", "numpy", "matplotlib"]
        assert request.api_key == "sk-test-key"
    
    def test_crew_generation_request_missing_prompt(self):
        """Test CrewGenerationRequest validation with missing prompt."""
        with pytest.raises(ValidationError) as exc_info:
            CrewGenerationRequest()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "prompt" in missing_fields


class TestAgentConfig:
    """Test cases for AgentConfig schema."""
    
    def test_agent_config_defaults(self):
        """Test AgentConfig with default values."""
        config = AgentConfig()
        assert config.llm == "databricks-llama-4-maverick"
        assert config.function_calling_llm is None
        assert config.max_iter == 25
        assert config.max_rpm is None
        assert config.max_execution_time is None
        assert config.verbose is False
        assert config.allow_delegation is False
        assert config.cache is True
        assert config.system_template is None
        assert config.prompt_template is None
        assert config.response_template is None
        assert config.allow_code_execution is False
        assert config.code_execution_mode == "safe"
        assert config.max_retry_limit == 2
        assert config.use_system_prompt is True
        assert config.respect_context_window is True
    
    def test_agent_config_custom_values(self):
        """Test AgentConfig with custom values."""
        config = AgentConfig(
            llm="gpt-4",
            function_calling_llm="gpt-3.5-turbo",
            max_iter=50,
            max_rpm=100,
            max_execution_time=600,
            verbose=True,
            allow_delegation=True,
            cache=False,
            system_template="Custom system template",
            prompt_template="Custom prompt template",
            response_template="Custom response template",
            allow_code_execution=True,
            code_execution_mode="unsafe",
            max_retry_limit=5,
            use_system_prompt=False,
            respect_context_window=False
        )
        assert config.llm == "gpt-4"
        assert config.function_calling_llm == "gpt-3.5-turbo"
        assert config.max_iter == 50
        assert config.max_rpm == 100
        assert config.max_execution_time == 600
        assert config.verbose is True
        assert config.allow_delegation is True
        assert config.cache is False
        assert config.system_template == "Custom system template"
        assert config.prompt_template == "Custom prompt template"
        assert config.response_template == "Custom response template"
        assert config.allow_code_execution is True
        assert config.code_execution_mode == "unsafe"
        assert config.max_retry_limit == 5
        assert config.use_system_prompt is False
        assert config.respect_context_window is False


class TestAgent:
    """Test cases for Agent schema."""
    
    def test_valid_agent_minimal(self):
        """Test Agent with minimal required fields."""
        agent = Agent(
            name="Data Analyst",
            role="analyst",
            goal="Analyze data",
            backstory="Expert analyst"
        )
        assert agent.name == "Data Analyst"
        assert agent.role == "analyst"
        assert agent.goal == "Analyze data"
        assert agent.backstory == "Expert analyst"
        assert agent.id is None
        assert agent.tools == []
        assert agent.llm == "databricks-llama-4-maverick"
        assert agent.max_iter == 25
        assert agent.verbose is False
        assert agent.allow_delegation is False
        assert agent.cache is True
    
    def test_valid_agent_full(self):
        """Test Agent with all fields."""
        agent = Agent(
            id="agent-123",
            name="Senior Data Analyst",
            role="senior_analyst",
            goal="Perform advanced data analysis",
            backstory="10 years of experience in data analysis",
            tools=["pandas", "numpy", "scipy"],
            llm="gpt-4",
            function_calling_llm="gpt-3.5-turbo",
            max_iter=50,
            max_rpm=200,
            max_execution_time=1200,
            verbose=True,
            allow_delegation=True,
            cache=False,
            system_template="Custom system template",
            prompt_template="Custom prompt template",
            response_template="Custom response template",
            allow_code_execution=True,
            code_execution_mode="unsafe",
            max_retry_limit=5,
            use_system_prompt=False,
            respect_context_window=False
        )
        assert agent.id == "agent-123"
        assert agent.name == "Senior Data Analyst"
        assert agent.role == "senior_analyst"
        assert agent.goal == "Perform advanced data analysis"
        assert agent.backstory == "10 years of experience in data analysis"
        assert agent.tools == ["pandas", "numpy", "scipy"]
        assert agent.llm == "gpt-4"
        assert agent.function_calling_llm == "gpt-3.5-turbo"
        assert agent.max_iter == 50
        assert agent.max_rpm == 200
        assert agent.max_execution_time == 1200
        assert agent.verbose is True
        assert agent.allow_delegation is True
        assert agent.cache is False
        assert agent.system_template == "Custom system template"
        assert agent.prompt_template == "Custom prompt template"
        assert agent.response_template == "Custom response template"
        assert agent.allow_code_execution is True
        assert agent.code_execution_mode == "unsafe"
        assert agent.max_retry_limit == 5
        assert agent.use_system_prompt is False
        assert agent.respect_context_window is False
    
    def test_agent_missing_required_fields(self):
        """Test Agent validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Agent(name="Test Agent")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "role" in missing_fields
        assert "goal" in missing_fields
        assert "backstory" in missing_fields


class TestTask:
    """Test cases for Task schema."""
    
    def test_valid_task_minimal(self):
        """Test Task with minimal required fields."""
        task = Task(
            name="Data Analysis",
            description="Analyze the dataset"
        )
        assert task.name == "Data Analysis"
        assert task.description == "Analyze the dataset"
        assert task.id is None
        assert task.expected_output is None
        assert task.tools == []
        assert task.assigned_agent is None
        assert task.async_execution is False
        assert task.context == []
        assert task.config == {}
        assert task.output_json is None
        assert task.output_pydantic is None
        assert task.output_file is None
        assert task.output is None
        assert task.callback is None
        assert task.human_input is False
        assert task.converter_cls is None
        assert task.markdown is False
    
    def test_valid_task_full(self):
        """Test Task with all fields."""
        task = Task(
            id="task-123",
            name="Advanced Data Analysis",
            description="Perform comprehensive data analysis",
            expected_output="Detailed analysis report",
            tools=["pandas", "numpy", "matplotlib"],
            assigned_agent="agent-456",
            async_execution=True,
            context=["task-001", "task-002"],
            config={"timeout": 300, "retries": 3},
            output_json=True,
            output_pydantic="AnalysisResult",
            output_file="analysis_report.pdf",
            output="analysis_results",
            callback="on_analysis_complete",
            human_input=True,
            converter_cls="AnalysisConverter",
            markdown=True
        )
        assert task.id == "task-123"
        assert task.name == "Advanced Data Analysis"
        assert task.description == "Perform comprehensive data analysis"
        assert task.expected_output == "Detailed analysis report"
        assert task.tools == ["pandas", "numpy", "matplotlib"]
        assert task.assigned_agent == "agent-456"
        assert task.async_execution is True
        assert task.context == ["task-001", "task-002"]
        assert task.config == {"timeout": 300, "retries": 3}
        assert task.output_json is True
        assert task.output_pydantic == "AnalysisResult"
        assert task.output_file == "analysis_report.pdf"
        assert task.output == "analysis_results"
        assert task.callback == "on_analysis_complete"
        assert task.human_input is True
        assert task.converter_cls == "AnalysisConverter"
        assert task.markdown is True
    
    def test_task_missing_required_fields(self):
        """Test Task validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Task(name="Test Task")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "description" in missing_fields


class TestCrewGenerationResponse:
    """Test cases for CrewGenerationResponse schema."""
    
    def test_valid_crew_generation_response(self):
        """Test CrewGenerationResponse with agents and tasks."""
        agents = [
            Agent(name="Analyst", role="analyst", goal="Analyze", backstory="Expert"),
            Agent(name="Writer", role="writer", goal="Write", backstory="Professional")
        ]
        tasks = [
            Task(name="Analysis", description="Analyze data"),
            Task(name="Report", description="Write report")
        ]
        
        response = CrewGenerationResponse(agents=agents, tasks=tasks)
        assert len(response.agents) == 2
        assert len(response.tasks) == 2
        assert response.agents[0].name == "Analyst"
        assert response.tasks[0].name == "Analysis"
    
    def test_crew_generation_response_empty(self):
        """Test CrewGenerationResponse with empty lists."""
        response = CrewGenerationResponse(agents=[], tasks=[])
        assert len(response.agents) == 0
        assert len(response.tasks) == 0
    
    def test_crew_generation_response_missing_fields(self):
        """Test CrewGenerationResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewGenerationResponse(agents=[])
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tasks" in missing_fields


class TestCrewCreationResponse:
    """Test cases for CrewCreationResponse schema."""
    
    def test_valid_crew_creation_response(self):
        """Test CrewCreationResponse with created entities."""
        mock_agents = [{"id": 1, "name": "Agent 1"}, {"id": 2, "name": "Agent 2"}]
        mock_tasks = [{"id": 1, "name": "Task 1"}, {"id": 2, "name": "Task 2"}]
        
        response = CrewCreationResponse(agents=mock_agents, tasks=mock_tasks)
        assert len(response.agents) == 2
        assert len(response.tasks) == 2
        assert response.agents[0]["id"] == 1
        assert response.tasks[0]["name"] == "Task 1"
    
    def test_crew_creation_response_config(self):
        """Test CrewCreationResponse model configuration."""
        assert hasattr(CrewCreationResponse, 'model_config')
        assert CrewCreationResponse.model_config['from_attributes'] is True
    
    def test_crew_creation_response_missing_fields(self):
        """Test CrewCreationResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewCreationResponse(agents=[])
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tasks" in missing_fields


class TestSchemaIntegration:
    """Integration tests for crew schema interactions."""
    
    def test_complete_crew_workflow(self):
        """Test a complete crew workflow with all schemas."""
        # Create a crew generation request
        request = CrewGenerationRequest(
            prompt="Create a data analysis crew with visualization",
            model="gpt-4",
            tools=["pandas", "matplotlib", "seaborn"]
        )
        
        # Create agents and tasks (simulating generation response)
        agents = [
            Agent(
                name="Data Analyst",
                role="analyst",
                goal="Analyze data thoroughly",
                backstory="Expert in statistical analysis",
                tools=["pandas", "numpy"]
            ),
            Agent(
                name="Data Visualizer",
                role="visualizer",
                goal="Create insightful visualizations",
                backstory="Specialist in data visualization",
                tools=["matplotlib", "seaborn"]
            )
        ]
        
        tasks = [
            Task(
                name="Data Analysis",
                description="Perform statistical analysis on the dataset",
                expected_output="Statistical summary and insights",
                tools=["pandas", "numpy"],
                assigned_agent="agent-1"
            ),
            Task(
                name="Data Visualization",
                description="Create visualizations based on analysis",
                expected_output="Charts and graphs",
                tools=["matplotlib", "seaborn"],
                assigned_agent="agent-2",
                context=["task-1"]
            )
        ]
        
        generation_response = CrewGenerationResponse(agents=agents, tasks=tasks)
        
        # Create crew with nodes and edges
        position1 = Position(x=100, y=100)
        position2 = Position(x=300, y=100)
        
        node1 = Node(
            id="node-1",
            type="agent",
            position=position1,
            data=NodeData(
                label="Data Analyst",
                role="analyst",
                goal="Analyze data thoroughly",
                backstory="Expert in statistical analysis",
                agentId="agent-1"
            )
        )
        
        node2 = Node(
            id="node-2",
            type="agent",
            position=position2,
            data=NodeData(
                label="Data Visualizer",
                role="visualizer",
                goal="Create insightful visualizations",
                backstory="Specialist in data visualization",
                agentId="agent-2"
            )
        )
        
        edge = Edge(source="node-1", target="node-2", id="edge-1")
        
        crew_create = CrewCreate(
            name="Data Analysis Crew",
            agent_ids=["agent-1", "agent-2"],
            task_ids=["task-1", "task-2"],
            nodes=[node1, node2],
            edges=[edge]
        )
        
        # Verify the complete workflow
        assert request.prompt == "Create a data analysis crew with visualization"
        assert len(generation_response.agents) == 2
        assert len(generation_response.tasks) == 2
        assert crew_create.name == "Data Analysis Crew"
        assert len(crew_create.nodes) == 2
        assert len(crew_create.edges) == 1
        assert crew_create.nodes[0].data.label == "Data Analyst"
        assert crew_create.nodes[1].data.label == "Data Visualizer"
        assert crew_create.edges[0].source == "node-1"
        assert crew_create.edges[0].target == "node-2"
        assert generation_response.tasks[1].context == ["task-1"]  # Task dependency
    
    def test_crew_update_workflow(self):
        """Test crew update workflow with partial updates."""
        # Create initial crew
        crew_create = CrewCreate(
            name="Initial Crew",
            agent_ids=["agent-1"],
            task_ids=["task-1"]
        )
        
        # Update crew with additional agents and tasks
        position = Position(x=200, y=200)
        new_node = Node(
            id="node-2",
            type="agent",
            position=position,
            data=NodeData(label="New Agent", role="new_role", agentId="agent-2")
        )
        
        crew_update = CrewUpdate(
            name="Updated Crew",
            agent_ids=["agent-1", "agent-2"],
            task_ids=["task-1", "task-2"],
            nodes=[new_node]
        )
        
        # Verify update
        assert crew_create.name == "Initial Crew"
        assert len(crew_create.agent_ids) == 1
        assert crew_update.name == "Updated Crew"
        assert len(crew_update.agent_ids) == 2
        assert len(crew_update.nodes) == 1
        assert crew_update.nodes[0].data.label == "New Agent"