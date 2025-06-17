"""
Unit tests for agent schemas.

Tests the functionality of Pydantic schemas for agent operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import List, Dict, Any

from src.schemas.agent import (
    AgentBase, AgentCreate, AgentUpdate, AgentLimitedUpdate,
    AgentInDBBase, Agent
)


class TestAgentBase:
    """Test cases for AgentBase schema."""
    
    def test_valid_agent_base_minimal(self):
        """Test AgentBase with minimal required fields."""
        agent_data = {
            "role": "analyst",
            "goal": "Analyze data effectively",
            "backstory": "Expert in data analysis"
        }
        agent = AgentBase(**agent_data)
        assert agent.name == "Unnamed Agent"  # Default value
        assert agent.role == "analyst"
        assert agent.goal == "Analyze data effectively"
        assert agent.backstory == "Expert in data analysis"
        assert agent.llm == "databricks-llama-4-maverick"  # Default
        assert agent.tools == []  # Default empty list
        assert agent.function_calling_llm is None
        assert agent.max_iter == 25  # Default
        assert agent.max_rpm is None
        assert agent.max_execution_time is None
        assert agent.verbose is False  # Default
        assert agent.allow_delegation is False  # Default
        assert agent.cache is True  # Default
        assert agent.memory is True  # Default
        assert agent.embedder_config is None
        assert agent.system_template is None
        assert agent.prompt_template is None
        assert agent.response_template is None
        assert agent.allow_code_execution is False  # Default
        assert agent.code_execution_mode == "safe"  # Default
        assert agent.max_retry_limit == 2  # Default
        assert agent.use_system_prompt is True  # Default
        assert agent.respect_context_window is True  # Default
        assert agent.knowledge_sources == []  # Default empty list
    
    def test_valid_agent_base_full(self):
        """Test AgentBase with all fields specified."""
        agent_data = {
            "name": "Senior Data Analyst",
            "role": "senior_analyst",
            "goal": "Perform advanced data analysis and insights",
            "backstory": "10 years of experience in data science",
            "llm": "gpt-4",
            "tools": ["pandas", "numpy", "scipy"],
            "function_calling_llm": "gpt-3.5-turbo",
            "max_iter": 50,
            "max_rpm": 100,
            "max_execution_time": 1200,
            "verbose": True,
            "allow_delegation": True,
            "cache": False,
            "memory": False,
            "embedder_config": {"model": "sentence-transformers", "dimension": 384},
            "system_template": "You are a data analyst",
            "prompt_template": "Analyze: {data}",
            "response_template": "Result: {result}",
            "allow_code_execution": True,
            "code_execution_mode": "unsafe",
            "max_retry_limit": 5,
            "use_system_prompt": False,
            "respect_context_window": False,
            "knowledge_sources": [{"type": "document", "path": "/docs"}]
        }
        agent = AgentBase(**agent_data)
        assert agent.name == "Senior Data Analyst"
        assert agent.role == "senior_analyst"
        assert agent.goal == "Perform advanced data analysis and insights"
        assert agent.backstory == "10 years of experience in data science"
        assert agent.llm == "gpt-4"
        assert agent.tools == ["pandas", "numpy", "scipy"]
        assert agent.function_calling_llm == "gpt-3.5-turbo"
        assert agent.max_iter == 50
        assert agent.max_rpm == 100
        assert agent.max_execution_time == 1200
        assert agent.verbose is True
        assert agent.allow_delegation is True
        assert agent.cache is False
        assert agent.memory is False
        assert agent.embedder_config == {"model": "sentence-transformers", "dimension": 384}
        assert agent.system_template == "You are a data analyst"
        assert agent.prompt_template == "Analyze: {data}"
        assert agent.response_template == "Result: {result}"
        assert agent.allow_code_execution is True
        assert agent.code_execution_mode == "unsafe"
        assert agent.max_retry_limit == 5
        assert agent.use_system_prompt is False
        assert agent.respect_context_window is False
        assert agent.knowledge_sources == [{"type": "document", "path": "/docs"}]
    
    def test_agent_base_missing_required_fields(self):
        """Test AgentBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            AgentBase(name="Test Agent")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "role" in missing_fields
        assert "goal" in missing_fields
        assert "backstory" in missing_fields
    
    def test_agent_base_empty_strings(self):
        """Test AgentBase with empty strings for required fields."""
        agent_data = {
            "name": "",
            "role": "",
            "goal": "",
            "backstory": ""
        }
        agent = AgentBase(**agent_data)
        assert agent.name == ""
        assert agent.role == ""
        assert agent.goal == ""
        assert agent.backstory == ""
    
    def test_agent_base_boolean_conversions(self):
        """Test AgentBase boolean field conversions."""
        agent_data = {
            "role": "analyst",
            "goal": "Analyze data",
            "backstory": "Expert analyst",
            "verbose": "true",
            "allow_delegation": 1,
            "cache": 0,
            "memory": "false"
        }
        agent = AgentBase(**agent_data)
        assert agent.verbose is True
        assert agent.allow_delegation is True
        assert agent.cache is False
        assert agent.memory is False
    
    def test_agent_base_integer_validations(self):
        """Test AgentBase integer field validations."""
        agent_data = {
            "role": "analyst",
            "goal": "Analyze data",
            "backstory": "Expert analyst",
            "max_iter": "30",  # String that can be converted
            "max_retry_limit": 3.0  # Float that can be converted
        }
        agent = AgentBase(**agent_data)
        assert agent.max_iter == 30
        assert agent.max_retry_limit == 3
        assert isinstance(agent.max_iter, int)
        assert isinstance(agent.max_retry_limit, int)


class TestAgentCreate:
    """Test cases for AgentCreate schema."""
    
    def test_agent_create_inheritance(self):
        """Test that AgentCreate inherits from AgentBase."""
        agent_data = {
            "name": "New Agent",
            "role": "developer",
            "goal": "Develop software",
            "backstory": "Software developer"
        }
        agent = AgentCreate(**agent_data)
        
        # Should have all base class attributes
        assert hasattr(agent, 'name')
        assert hasattr(agent, 'role')
        assert hasattr(agent, 'goal')
        assert hasattr(agent, 'backstory')
        assert hasattr(agent, 'llm')
        assert hasattr(agent, 'tools')
        assert hasattr(agent, 'max_iter')
        assert hasattr(agent, 'verbose')
        assert hasattr(agent, 'cache')
        assert hasattr(agent, 'memory')
        
        # Should behave like base class
        assert agent.name == "New Agent"
        assert agent.role == "developer"
        assert agent.goal == "Develop software"
        assert agent.backstory == "Software developer"
        assert agent.llm == "databricks-llama-4-maverick"  # Default
        assert agent.max_iter == 25  # Default
    
    def test_agent_create_with_custom_values(self):
        """Test AgentCreate with custom values."""
        agent_data = {
            "name": "Custom Agent",
            "role": "researcher",
            "goal": "Research topics",
            "backstory": "Academic researcher",
            "llm": "claude-3",
            "tools": ["search", "summarize"],
            "max_iter": 40,
            "verbose": True
        }
        agent = AgentCreate(**agent_data)
        assert agent.name == "Custom Agent"
        assert agent.role == "researcher"
        assert agent.llm == "claude-3"
        assert agent.tools == ["search", "summarize"]
        assert agent.max_iter == 40
        assert agent.verbose is True


class TestAgentUpdate:
    """Test cases for AgentUpdate schema."""
    
    def test_agent_update_all_optional(self):
        """Test that all AgentUpdate fields are optional."""
        update = AgentUpdate()
        assert update.name is None
        assert update.role is None
        assert update.goal is None
        assert update.backstory is None
        assert update.llm is None
        assert update.tools is None
        assert update.function_calling_llm is None
        assert update.max_iter is None
        assert update.max_rpm is None
        assert update.max_execution_time is None
        assert update.verbose is None
        assert update.allow_delegation is None
        assert update.cache is None
        assert update.memory is None
        assert update.embedder_config is None
        assert update.system_template is None
        assert update.prompt_template is None
        assert update.response_template is None
        assert update.allow_code_execution is None
        assert update.code_execution_mode is None
        assert update.max_retry_limit is None
        assert update.use_system_prompt is None
        assert update.respect_context_window is None
        assert update.knowledge_sources is None
    
    def test_agent_update_partial(self):
        """Test AgentUpdate with partial fields."""
        update_data = {
            "name": "Updated Agent",
            "llm": "gpt-4",
            "verbose": True
        }
        update = AgentUpdate(**update_data)
        assert update.name == "Updated Agent"
        assert update.llm == "gpt-4"
        assert update.verbose is True
        assert update.role is None
        assert update.goal is None
        assert update.backstory is None
    
    def test_agent_update_full(self):
        """Test AgentUpdate with all fields."""
        update_data = {
            "name": "Fully Updated Agent",
            "role": "updated_role",
            "goal": "Updated goal",
            "backstory": "Updated backstory",
            "llm": "claude-3",
            "tools": ["new_tool"],
            "function_calling_llm": "gpt-3.5-turbo",
            "max_iter": 60,
            "max_rpm": 200,
            "max_execution_time": 1800,
            "verbose": False,
            "allow_delegation": True,
            "cache": False,
            "memory": False,
            "embedder_config": {"updated": True},
            "system_template": "Updated system template",
            "prompt_template": "Updated prompt template",
            "response_template": "Updated response template",
            "allow_code_execution": True,
            "code_execution_mode": "restricted",
            "max_retry_limit": 7,
            "use_system_prompt": False,
            "respect_context_window": False,
            "knowledge_sources": [{"updated": "source"}]
        }
        update = AgentUpdate(**update_data)
        assert update.name == "Fully Updated Agent"
        assert update.role == "updated_role"
        assert update.goal == "Updated goal"
        assert update.backstory == "Updated backstory"
        assert update.llm == "claude-3"
        assert update.tools == ["new_tool"]
        assert update.function_calling_llm == "gpt-3.5-turbo"
        assert update.max_iter == 60
        assert update.max_rpm == 200
        assert update.max_execution_time == 1800
        assert update.verbose is False
        assert update.allow_delegation is True
        assert update.cache is False
        assert update.memory is False
        assert update.embedder_config == {"updated": True}
        assert update.system_template == "Updated system template"
        assert update.prompt_template == "Updated prompt template"
        assert update.response_template == "Updated response template"
        assert update.allow_code_execution is True
        assert update.code_execution_mode == "restricted"
        assert update.max_retry_limit == 7
        assert update.use_system_prompt is False
        assert update.respect_context_window is False
        assert update.knowledge_sources == [{"updated": "source"}]
    
    def test_agent_update_none_values(self):
        """Test AgentUpdate with explicit None values."""
        update_data = {
            "name": None,
            "role": None,
            "goal": None,
            "backstory": None,
            "llm": None,
            "tools": None
        }
        update = AgentUpdate(**update_data)
        assert update.name is None
        assert update.role is None
        assert update.goal is None
        assert update.backstory is None
        assert update.llm is None
        assert update.tools is None
    
    def test_agent_update_empty_strings(self):
        """Test AgentUpdate with empty strings."""
        update_data = {
            "name": "",
            "role": "",
            "goal": "",
            "backstory": ""
        }
        update = AgentUpdate(**update_data)
        assert update.name == ""
        assert update.role == ""
        assert update.goal == ""
        assert update.backstory == ""


class TestAgentLimitedUpdate:
    """Test cases for AgentLimitedUpdate schema."""
    
    def test_agent_limited_update_all_optional(self):
        """Test that all AgentLimitedUpdate fields are optional."""
        update = AgentLimitedUpdate()
        assert update.name is None
        assert update.role is None
        assert update.goal is None
        assert update.backstory is None
    
    def test_agent_limited_update_partial(self):
        """Test AgentLimitedUpdate with partial fields."""
        update_data = {
            "name": "Limited Update Agent",
            "role": "limited_role"
        }
        update = AgentLimitedUpdate(**update_data)
        assert update.name == "Limited Update Agent"
        assert update.role == "limited_role"
        assert update.goal is None
        assert update.backstory is None
    
    def test_agent_limited_update_full(self):
        """Test AgentLimitedUpdate with all fields."""
        update_data = {
            "name": "Complete Limited Update",
            "role": "complete_role",
            "goal": "Complete goal",
            "backstory": "Complete backstory"
        }
        update = AgentLimitedUpdate(**update_data)
        assert update.name == "Complete Limited Update"
        assert update.role == "complete_role"
        assert update.goal == "Complete goal"
        assert update.backstory == "Complete backstory"
    
    def test_agent_limited_update_restricted_fields(self):
        """Test that AgentLimitedUpdate only has basic fields defined."""
        # Check that model only has the expected fields in model_fields
        expected_fields = {"name", "role", "goal", "backstory"}
        actual_fields = set(AgentLimitedUpdate.model_fields.keys())
        assert actual_fields == expected_fields
        
        # Test that it doesn't have configuration fields in its model definition
        config_fields = {"llm", "tools", "max_iter", "verbose", "cache", "memory"}
        assert config_fields.isdisjoint(actual_fields)
        
        # Test that a limited update can be created successfully
        limited_update = AgentLimitedUpdate(
            name="Test",
            role="test",
            goal="test goal",
            backstory="test backstory"
        )
        assert limited_update.name == "Test"
        assert limited_update.role == "test"
        assert limited_update.goal == "test goal"
        assert limited_update.backstory == "test backstory"


class TestAgentInDBBase:
    """Test cases for AgentInDBBase schema."""
    
    def test_valid_agent_in_db_base(self):
        """Test AgentInDBBase with all required fields."""
        now = datetime.now()
        agent_data = {
            "id": "agent-123",
            "name": "DB Agent",
            "role": "db_analyst",
            "goal": "Analyze database",
            "backstory": "Database expert",
            "created_at": now,
            "updated_at": now
        }
        agent = AgentInDBBase(**agent_data)
        assert agent.id == "agent-123"
        assert agent.name == "DB Agent"
        assert agent.role == "db_analyst"
        assert agent.goal == "Analyze database"
        assert agent.backstory == "Database expert"
        assert agent.created_at == now
        assert agent.updated_at == now
        
        # Should inherit all base class defaults
        assert agent.llm == "databricks-llama-4-maverick"
        assert agent.tools == []
        assert agent.max_iter == 25
        assert agent.verbose is False
        assert agent.cache is True
        assert agent.memory is True
    
    def test_agent_in_db_base_config(self):
        """Test AgentInDBBase Config class."""
        assert hasattr(AgentInDBBase, 'model_config')
        assert AgentInDBBase.model_config.get('from_attributes') is True
    
    def test_agent_in_db_base_missing_fields(self):
        """Test AgentInDBBase validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            AgentInDBBase(
                name="Test Agent",
                role="test",
                goal="test",
                backstory="test"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields
    
    def test_agent_in_db_base_datetime_conversion(self):
        """Test AgentInDBBase with datetime string conversion."""
        agent_data = {
            "id": "agent-456",
            "name": "DateTime Agent",
            "role": "datetime_analyst",
            "goal": "Handle datetime",
            "backstory": "Time expert",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00"
        }
        agent = AgentInDBBase(**agent_data)
        assert agent.id == "agent-456"
        assert isinstance(agent.created_at, datetime)
        assert isinstance(agent.updated_at, datetime)


class TestAgent:
    """Test cases for Agent schema."""
    
    def test_agent_inheritance(self):
        """Test that Agent inherits from AgentInDBBase."""
        now = datetime.now()
        agent_data = {
            "id": "agent-789",
            "name": "Inherited Agent",
            "role": "inherited_analyst",
            "goal": "Test inheritance",
            "backstory": "Inheritance expert",
            "created_at": now,
            "updated_at": now
        }
        agent = Agent(**agent_data)
        
        # Should have all AgentInDBBase attributes
        assert hasattr(agent, 'id')
        assert hasattr(agent, 'created_at')
        assert hasattr(agent, 'updated_at')
        
        # Should have all AgentBase attributes
        assert hasattr(agent, 'name')
        assert hasattr(agent, 'role')
        assert hasattr(agent, 'goal')
        assert hasattr(agent, 'backstory')
        assert hasattr(agent, 'llm')
        assert hasattr(agent, 'tools')
        assert hasattr(agent, 'max_iter')
        assert hasattr(agent, 'verbose')
        assert hasattr(agent, 'cache')
        assert hasattr(agent, 'memory')
        
        # Verify values
        assert agent.id == "agent-789"
        assert agent.name == "Inherited Agent"
        assert agent.role == "inherited_analyst"
        assert agent.goal == "Test inheritance"
        assert agent.backstory == "Inheritance expert"
        assert agent.created_at == now
        assert agent.updated_at == now
        assert agent.llm == "databricks-llama-4-maverick"  # Default from base
        assert agent.max_iter == 25  # Default from base
    
    def test_agent_with_full_configuration(self):
        """Test Agent with full configuration."""
        now = datetime.now()
        agent_data = {
            "id": "agent-full",
            "name": "Fully Configured Agent",
            "role": "full_analyst",
            "goal": "Full analysis",
            "backstory": "Comprehensive background",
            "llm": "gpt-4",
            "tools": ["tool1", "tool2", "tool3"],
            "function_calling_llm": "gpt-3.5-turbo",
            "max_iter": 100,
            "max_rpm": 500,
            "max_execution_time": 3600,
            "verbose": True,
            "allow_delegation": True,
            "cache": False,
            "memory": False,
            "embedder_config": {"model": "bert", "size": 768},
            "system_template": "System: {prompt}",
            "prompt_template": "User: {input}",
            "response_template": "Assistant: {output}",
            "allow_code_execution": True,
            "code_execution_mode": "sandbox",
            "max_retry_limit": 10,
            "use_system_prompt": False,
            "respect_context_window": False,
            "knowledge_sources": [{"type": "wiki", "url": "wikipedia.org"}],
            "created_at": now,
            "updated_at": now
        }
        agent = Agent(**agent_data)
        
        # Verify all fields are set correctly
        assert agent.id == "agent-full"
        assert agent.name == "Fully Configured Agent"
        assert agent.role == "full_analyst"
        assert agent.goal == "Full analysis"
        assert agent.backstory == "Comprehensive background"
        assert agent.llm == "gpt-4"
        assert agent.tools == ["tool1", "tool2", "tool3"]
        assert agent.function_calling_llm == "gpt-3.5-turbo"
        assert agent.max_iter == 100
        assert agent.max_rpm == 500
        assert agent.max_execution_time == 3600
        assert agent.verbose is True
        assert agent.allow_delegation is True
        assert agent.cache is False
        assert agent.memory is False
        assert agent.embedder_config == {"model": "bert", "size": 768}
        assert agent.system_template == "System: {prompt}"
        assert agent.prompt_template == "User: {input}"
        assert agent.response_template == "Assistant: {output}"
        assert agent.allow_code_execution is True
        assert agent.code_execution_mode == "sandbox"
        assert agent.max_retry_limit == 10
        assert agent.use_system_prompt is False
        assert agent.respect_context_window is False
        assert agent.knowledge_sources == [{"type": "wiki", "url": "wikipedia.org"}]
        assert agent.created_at == now
        assert agent.updated_at == now


class TestSchemaIntegration:
    """Integration tests for agent schema interactions."""
    
    def test_agent_creation_workflow(self):
        """Test complete agent creation workflow."""
        # Create agent
        create_data = {
            "name": "Workflow Agent",
            "role": "workflow_analyst",
            "goal": "Test workflow",
            "backstory": "Workflow expert",
            "llm": "claude-3",
            "tools": ["workflow_tool"],
            "verbose": True
        }
        create_schema = AgentCreate(**create_data)
        
        # Update agent
        update_data = {
            "name": "Updated Workflow Agent",
            "max_iter": 50,
            "cache": False
        }
        update_schema = AgentUpdate(**update_data)
        
        # Limited update
        limited_update_data = {
            "role": "senior_workflow_analyst",
            "goal": "Advanced workflow testing"
        }
        limited_update_schema = AgentLimitedUpdate(**limited_update_data)
        
        # Simulate database entity
        now = datetime.now()
        db_data = {
            "id": "workflow-agent-1",
            "name": update_data["name"],  # Updated name
            "role": limited_update_data["role"],  # Limited update role
            "goal": limited_update_data["goal"],  # Limited update goal
            "backstory": create_schema.backstory,  # Original backstory
            "llm": create_schema.llm,  # Original llm
            "tools": create_schema.tools,  # Original tools
            "verbose": create_schema.verbose,  # Original verbose
            "max_iter": update_data["max_iter"],  # Updated max_iter
            "cache": update_data["cache"],  # Updated cache
            "created_at": now,
            "updated_at": now
        }
        agent_response = Agent(**db_data)
        
        # Verify the complete workflow
        assert create_schema.name == "Workflow Agent"
        assert create_schema.llm == "claude-3"
        assert update_schema.name == "Updated Workflow Agent"
        assert update_schema.max_iter == 50
        assert limited_update_schema.role == "senior_workflow_analyst"
        assert agent_response.id == "workflow-agent-1"
        assert agent_response.name == "Updated Workflow Agent"  # From update
        assert agent_response.role == "senior_workflow_analyst"  # From limited update
        assert agent_response.goal == "Advanced workflow testing"  # From limited update
        assert agent_response.backstory == "Workflow expert"  # From creation
        assert agent_response.llm == "claude-3"  # From creation
        assert agent_response.max_iter == 50  # From update
        assert agent_response.cache is False  # From update
    
    def test_agent_configuration_scenarios(self):
        """Test different agent configuration scenarios."""
        # Basic agent
        basic_agent = AgentCreate(
            role="basic",
            goal="Basic tasks",
            backstory="Basic background"
        )
        assert basic_agent.name == "Unnamed Agent"
        assert basic_agent.llm == "databricks-llama-4-maverick"
        assert basic_agent.max_iter == 25
        assert basic_agent.verbose is False
        
        # Advanced agent
        advanced_agent = AgentCreate(
            name="Advanced AI",
            role="advanced",
            goal="Complex analysis",
            backstory="PhD in AI",
            llm="gpt-4",
            tools=["research", "analysis", "visualization"],
            max_iter=200,
            verbose=True,
            allow_code_execution=True,
            memory=True
        )
        assert advanced_agent.name == "Advanced AI"
        assert advanced_agent.llm == "gpt-4"
        assert advanced_agent.tools == ["research", "analysis", "visualization"]
        assert advanced_agent.max_iter == 200
        assert advanced_agent.verbose is True
        assert advanced_agent.allow_code_execution is True
        assert advanced_agent.memory is True
        
        # Specialized agent with templates
        specialized_agent = AgentCreate(
            name="Specialized Bot",
            role="specialist",
            goal="Domain-specific tasks",
            backstory="Domain expert",
            system_template="You are a specialist in {domain}",
            prompt_template="Analyze this {domain} problem: {problem}",
            response_template="Solution: {solution}",
            knowledge_sources=[{"type": "domain_docs", "path": "/domain"}]
        )
        assert specialized_agent.system_template == "You are a specialist in {domain}"
        assert specialized_agent.prompt_template == "Analyze this {domain} problem: {problem}"
        assert specialized_agent.response_template == "Solution: {solution}"
        assert specialized_agent.knowledge_sources == [{"type": "domain_docs", "path": "/domain"}]
    
    def test_agent_update_scenarios(self):
        """Test different agent update scenarios."""
        # Performance tuning update
        performance_update = AgentUpdate(
            max_iter=300,
            max_rpm=1000,
            max_execution_time=7200,
            cache=True,
            respect_context_window=True
        )
        assert performance_update.max_iter == 300
        assert performance_update.max_rpm == 1000
        assert performance_update.max_execution_time == 7200
        assert performance_update.cache is True
        assert performance_update.respect_context_window is True
        
        # Security update
        security_update = AgentUpdate(
            allow_code_execution=False,
            code_execution_mode="safe",
            allow_delegation=False,
            use_system_prompt=True
        )
        assert security_update.allow_code_execution is False
        assert security_update.code_execution_mode == "safe"
        assert security_update.allow_delegation is False
        assert security_update.use_system_prompt is True
        
        # Content update
        content_update = AgentUpdate(
            system_template="Updated system template",
            prompt_template="Updated prompt template",
            response_template="Updated response template",
            knowledge_sources=[{"type": "updated_docs", "path": "/updated"}]
        )
        assert content_update.system_template == "Updated system template"
        assert content_update.prompt_template == "Updated prompt template"
        assert content_update.response_template == "Updated response template"
        assert content_update.knowledge_sources == [{"type": "updated_docs", "path": "/updated"}]