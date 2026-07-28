"""
Unit tests for connection schemas.

Tests the functionality of Pydantic schemas for connection operations
including validation, serialization, and field constraints.
"""
import pytest
from typing import List
from pydantic import ValidationError

from src.schemas.connection import (
    Agent, TaskContext, Task, ConnectionRequest, TaskAssignment,
    AgentAssignment, Dependency, ConnectionResponse, ApiKeyTestResult,
    PythonInfo, ApiKeyTestResponse
)


class TestAgent:
    """Test cases for Agent schema."""
    
    def test_valid_agent_minimal(self):
        """Test valid Agent creation with minimal required fields."""
        agent_data = {
            "name": "Data Analyst",
            "role": "analyst",
            "goal": "Analyze data and provide insights"
        }
        agent = Agent(**agent_data)
        assert agent.name == "Data Analyst"
        assert agent.role == "analyst"
        assert agent.goal == "Analyze data and provide insights"
        assert agent.backstory is None
        assert agent.tools is None
    
    def test_valid_agent_full(self):
        """Test valid Agent creation with all fields."""
        agent_data = {
            "name": "Data Analyst",
            "role": "analyst",
            "goal": "Analyze data and provide insights",
            "backstory": "Expert in data analysis with 10 years experience",
            "tools": ["pandas", "numpy", "matplotlib"]
        }
        agent = Agent(**agent_data)
        assert agent.name == "Data Analyst"
        assert agent.role == "analyst"
        assert agent.goal == "Analyze data and provide insights"
        assert agent.backstory == "Expert in data analysis with 10 years experience"
        assert agent.tools == ["pandas", "numpy", "matplotlib"]
    
    def test_agent_missing_required_fields(self):
        """Test Agent validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Agent(name="Test Agent")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "role" in missing_fields
        assert "goal" in missing_fields
    
    def test_agent_empty_strings(self):
        """Test Agent with empty strings for required fields."""
        agent_data = {
            "name": "",
            "role": "",
            "goal": ""
        }
        agent = Agent(**agent_data)
        assert agent.name == ""
        assert agent.role == ""
        assert agent.goal == ""


class TestTaskContext:
    """Test cases for TaskContext schema."""
    
    def test_valid_task_context_defaults(self):
        """Test TaskContext with default values."""
        context = TaskContext()
        assert context.type == "general"
        assert context.priority == "medium"
        assert context.complexity == "medium"
        assert context.required_skills is None
    
    def test_valid_task_context_full(self):
        """Test TaskContext with all fields specified."""
        context_data = {
            "type": "analysis",
            "priority": "high",
            "complexity": "high",
            "required_skills": ["python", "sql", "statistics"]
        }
        context = TaskContext(**context_data)
        assert context.type == "analysis"
        assert context.priority == "high"
        assert context.complexity == "high"
        assert context.required_skills == ["python", "sql", "statistics"]
    
    def test_task_context_none_values(self):
        """Test TaskContext with None values."""
        context_data = {
            "type": None,
            "priority": None,
            "complexity": None,
            "required_skills": None
        }
        context = TaskContext(**context_data)
        assert context.type is None
        assert context.priority is None
        assert context.complexity is None
        assert context.required_skills is None


class TestTask:
    """Test cases for Task schema."""
    
    def test_valid_task_minimal(self):
        """Test valid Task creation with minimal required fields."""
        task_data = {
            "name": "Data Analysis",
            "description": "Analyze the provided dataset"
        }
        task = Task(**task_data)
        assert task.name == "Data Analysis"
        assert task.description == "Analyze the provided dataset"
        assert task.expected_output is None
        assert task.tools is None
        assert task.markdown is False
        assert task.context is None
        assert task.human_input is False
    
    def test_valid_task_full(self):
        """Test valid Task creation with all fields."""
        context = TaskContext(type="analysis", priority="high")
        task_data = {
            "name": "Data Analysis",
            "description": "Analyze the provided dataset",
            "expected_output": "Statistical report with insights",
            "tools": ["pandas", "matplotlib"],
            "markdown": True,
            "context": context,
            "human_input": True
        }
        task = Task(**task_data)
        assert task.name == "Data Analysis"
        assert task.description == "Analyze the provided dataset"
        assert task.expected_output == "Statistical report with insights"
        assert task.tools == ["pandas", "matplotlib"]
        assert task.markdown is True
        assert task.context == context
        assert task.human_input is True
    
    def test_task_missing_required_fields(self):
        """Test Task validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Task(name="Test Task")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "description" in missing_fields
    
    def test_task_with_context_dict(self):
        """Test Task with context provided as dictionary."""
        task_data = {
            "name": "Data Analysis",
            "description": "Analyze the provided dataset",
            "context": {
                "type": "analysis",
                "priority": "high",
                "complexity": "medium",
                "required_skills": ["python"]
            }
        }
        task = Task(**task_data)
        assert isinstance(task.context, TaskContext)
        assert task.context.type == "analysis"
        assert task.context.priority == "high"


class TestConnectionRequest:
    """Test cases for ConnectionRequest schema."""
    
    def test_valid_connection_request_minimal(self):
        """Test valid ConnectionRequest with minimal data."""
        agent = Agent(name="Analyst", role="analyst", goal="Analyze data")
        task = Task(name="Analysis", description="Perform analysis")
        
        request_data = {
            "agents": [agent],
            "tasks": [task]
        }
        request = ConnectionRequest(**request_data)
        assert len(request.agents) == 1
        assert len(request.tasks) == 1
        assert request.model == "gpt-4-turbo"
        assert request.instructions is None
    
    def test_valid_connection_request_full(self):
        """Test valid ConnectionRequest with all fields."""
        agents = [
            Agent(name="Analyst", role="analyst", goal="Analyze data"),
            Agent(name="Writer", role="writer", goal="Write reports")
        ]
        tasks = [
            Task(name="Analysis", description="Perform analysis"),
            Task(name="Report", description="Write report")
        ]
        
        request_data = {
            "agents": agents,
            "tasks": tasks,
            "model": "gpt-3.5-turbo",
            "instructions": "Focus on efficiency and accuracy"
        }
        request = ConnectionRequest(**request_data)
        assert len(request.agents) == 2
        assert len(request.tasks) == 2
        assert request.model == "gpt-3.5-turbo"
        assert request.instructions == "Focus on efficiency and accuracy"
    
    def test_connection_request_empty_lists(self):
        """Test ConnectionRequest with empty agent and task lists."""
        request_data = {
            "agents": [],
            "tasks": []
        }
        request = ConnectionRequest(**request_data)
        assert len(request.agents) == 0
        assert len(request.tasks) == 0
    
    def test_connection_request_missing_required_fields(self):
        """Test ConnectionRequest validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionRequest()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "agents" in missing_fields
        assert "tasks" in missing_fields


class TestTaskAssignment:
    """Test cases for TaskAssignment schema."""
    
    def test_valid_task_assignment(self):
        """Test valid TaskAssignment creation."""
        assignment_data = {
            "task_name": "Data Analysis",
            "reasoning": "Agent has strong analytical skills"
        }
        assignment = TaskAssignment(**assignment_data)
        assert assignment.task_name == "Data Analysis"
        assert assignment.reasoning == "Agent has strong analytical skills"
    
    def test_task_assignment_missing_fields(self):
        """Test TaskAssignment validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            TaskAssignment(task_name="Test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "reasoning" in missing_fields


class TestAgentAssignment:
    """Test cases for AgentAssignment schema."""
    
    def test_valid_agent_assignment(self):
        """Test valid AgentAssignment creation."""
        tasks = [
            TaskAssignment(task_name="Task1", reasoning="Good fit"),
            TaskAssignment(task_name="Task2", reasoning="Expertise match")
        ]
        assignment_data = {
            "agent_name": "Data Analyst",
            "tasks": tasks
        }
        assignment = AgentAssignment(**assignment_data)
        assert assignment.agent_name == "Data Analyst"
        assert len(assignment.tasks) == 2
        assert assignment.tasks[0].task_name == "Task1"
    
    def test_agent_assignment_empty_tasks(self):
        """Test AgentAssignment with empty task list."""
        assignment_data = {
            "agent_name": "Data Analyst",
            "tasks": []
        }
        assignment = AgentAssignment(**assignment_data)
        assert assignment.agent_name == "Data Analyst"
        assert len(assignment.tasks) == 0


class TestDependency:
    """Test cases for Dependency schema."""
    
    def test_valid_dependency(self):
        """Test valid Dependency creation."""
        dependency_data = {
            "task_name": "Report Writing",
            "depends_on": ["Data Analysis", "Data Cleaning"],
            "reasoning": "Report needs clean analyzed data"
        }
        dependency = Dependency(**dependency_data)
        assert dependency.task_name == "Report Writing"
        assert dependency.depends_on == ["Data Analysis", "Data Cleaning"]
        assert dependency.reasoning == "Report needs clean analyzed data"
    
    def test_dependency_empty_depends_on(self):
        """Test Dependency with empty depends_on list."""
        dependency_data = {
            "task_name": "Independent Task",
            "depends_on": [],
            "reasoning": "This task has no dependencies"
        }
        dependency = Dependency(**dependency_data)
        assert dependency.task_name == "Independent Task"
        assert dependency.depends_on == []
    
    def test_dependency_missing_fields(self):
        """Test Dependency validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            Dependency(task_name="Test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "depends_on" in missing_fields
        assert "reasoning" in missing_fields


class TestConnectionResponse:
    """Test cases for ConnectionResponse schema."""
    
    def test_valid_connection_response(self):
        """Test valid ConnectionResponse creation."""
        assignments = [
            AgentAssignment(
                agent_name="Analyst",
                tasks=[TaskAssignment(task_name="Analysis", reasoning="Good fit")]
            )
        ]
        dependencies = [
            Dependency(
                task_name="Report",
                depends_on=["Analysis"],
                reasoning="Report needs analysis results"
            )
        ]
        
        response_data = {
            "assignments": assignments,
            "dependencies": dependencies
        }
        response = ConnectionResponse(**response_data)
        assert len(response.assignments) == 1
        assert len(response.dependencies) == 1
        assert response.assignments[0].agent_name == "Analyst"
    
    def test_connection_response_empty_lists(self):
        """Test ConnectionResponse with empty lists."""
        response_data = {
            "assignments": [],
            "dependencies": []
        }
        response = ConnectionResponse(**response_data)
        assert len(response.assignments) == 0
        assert len(response.dependencies) == 0


class TestApiKeyTestResult:
    """Test cases for ApiKeyTestResult schema."""
    
    def test_valid_api_key_test_result_minimal(self):
        """Test valid ApiKeyTestResult with minimal fields."""
        result_data = {
            "has_key": True
        }
        result = ApiKeyTestResult(**result_data)
        assert result.has_key is True
        assert result.valid is None
        assert result.message is None
        assert result.key_prefix is None
    
    def test_valid_api_key_test_result_full(self):
        """Test valid ApiKeyTestResult with all fields."""
        result_data = {
            "has_key": True,
            "valid": True,
            "message": "API key is valid",
            "key_prefix": "sk-abc"
        }
        result = ApiKeyTestResult(**result_data)
        assert result.has_key is True
        assert result.valid is True
        assert result.message == "API key is valid"
        assert result.key_prefix == "sk-abc"
    
    def test_api_key_test_result_invalid_key(self):
        """Test ApiKeyTestResult for invalid key."""
        result_data = {
            "has_key": True,
            "valid": False,
            "message": "Invalid API key format"
        }
        result = ApiKeyTestResult(**result_data)
        assert result.has_key is True
        assert result.valid is False
        assert result.message == "Invalid API key format"


class TestPythonInfo:
    """Test cases for PythonInfo schema."""
    
    def test_valid_python_info(self):
        """Test valid PythonInfo creation."""
        info_data = {
            "version": "3.9.7",
            "executable": "/usr/bin/python3",
            "platform": "linux"
        }
        info = PythonInfo(**info_data)
        assert info.version == "3.9.7"
        assert info.executable == "/usr/bin/python3"
        assert info.platform == "linux"
    
    def test_python_info_missing_fields(self):
        """Test PythonInfo validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            PythonInfo(version="3.9.7")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "executable" in missing_fields
        assert "platform" in missing_fields


class TestApiKeyTestResponse:
    """Test cases for ApiKeyTestResponse schema."""
    
    def test_valid_api_key_test_response(self):
        """Test valid ApiKeyTestResponse creation."""
        openai_result = ApiKeyTestResult(has_key=True, valid=True)
        anthropic_result = ApiKeyTestResult(has_key=False)
        deepseek_result = ApiKeyTestResult(has_key=True, valid=False)
        python_info = PythonInfo(version="3.9.7", executable="/usr/bin/python3", platform="linux")
        
        response_data = {
            "openai": openai_result,
            "anthropic": anthropic_result,
            "deepseek": deepseek_result,
            "python_info": python_info
        }
        response = ApiKeyTestResponse(**response_data)
        assert response.openai.has_key is True
        assert response.anthropic.has_key is False
        assert response.deepseek.valid is False
        assert response.python_info.version == "3.9.7"
    
    def test_api_key_test_response_with_dicts(self):
        """Test ApiKeyTestResponse creation with dict inputs."""
        response_data = {
            "openai": {"has_key": True, "valid": True},
            "anthropic": {"has_key": False},
            "deepseek": {"has_key": True, "valid": False},
            "python_info": {
                "version": "3.9.7",
                "executable": "/usr/bin/python3",
                "platform": "linux"
            }
        }
        response = ApiKeyTestResponse(**response_data)
        assert isinstance(response.openai, ApiKeyTestResult)
        assert isinstance(response.python_info, PythonInfo)
        assert response.openai.has_key is True
        assert response.python_info.version == "3.9.7"
    
    def test_api_key_test_response_missing_fields(self):
        """Test ApiKeyTestResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyTestResponse()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "openai" in missing_fields
        assert "anthropic" in missing_fields
        assert "deepseek" in missing_fields
        assert "python_info" in missing_fields


class TestSchemaIntegration:
    """Integration tests for schema interactions."""
    
    def test_full_connection_workflow(self):
        """Test a complete connection workflow with all schemas."""
        # Create agents
        agents = [
            Agent(
                name="Data Analyst",
                role="analyst",
                goal="Analyze datasets",
                backstory="Expert in data analysis",
                tools=["pandas", "numpy"]
            ),
            Agent(
                name="Report Writer",
                role="writer",
                goal="Create reports",
                tools=["markdown", "charts"]
            )
        ]
        
        # Create tasks
        tasks = [
            Task(
                name="Data Analysis",
                description="Analyze the customer data",
                expected_output="Statistical insights",
                context=TaskContext(type="analysis", priority="high"),
                tools=["pandas"]
            ),
            Task(
                name="Report Writing",
                description="Write analysis report",
                expected_output="Formatted report",
                markdown=True,
                human_input=True
            )
        ]
        
        # Create connection request
        request = ConnectionRequest(
            agents=agents,
            tasks=tasks,
            model="gpt-4-turbo",
            instructions="Focus on accuracy"
        )
        
        # Create response
        assignments = [
            AgentAssignment(
                agent_name="Data Analyst",
                tasks=[TaskAssignment(task_name="Data Analysis", reasoning="Perfect match")]
            ),
            AgentAssignment(
                agent_name="Report Writer",
                tasks=[TaskAssignment(task_name="Report Writing", reasoning="Writing expertise")]
            )
        ]
        
        dependencies = [
            Dependency(
                task_name="Report Writing",
                depends_on=["Data Analysis"],
                reasoning="Report needs analysis results"
            )
        ]
        
        response = ConnectionResponse(
            assignments=assignments,
            dependencies=dependencies
        )
        
        # Verify the complete workflow
        assert len(request.agents) == 2
        assert len(request.tasks) == 2
        assert len(response.assignments) == 2
        assert len(response.dependencies) == 1
        assert response.dependencies[0].task_name == "Report Writing"
        assert "Data Analysis" in response.dependencies[0].depends_on