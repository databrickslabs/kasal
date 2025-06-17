"""
Unit tests for agent model.

Tests the functionality of the Agent database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.models.agent import Agent, generate_uuid


class TestAgent:
    """Test cases for Agent model."""

    def test_agent_creation_minimal(self):
        """Test basic Agent model creation with minimal required fields."""
        # Arrange
        name = "Research Agent"
        role = "Senior Researcher"
        goal = "Conduct thorough research on given topics"
        
        # Act
        agent = Agent(
            name=name,
            role=role,
            goal=goal
        )
        
        # Assert
        assert agent.name == name
        assert agent.role == role
        assert agent.goal == goal
        assert agent.backstory is None
        # Note: SQLAlchemy defaults are applied when saved to database
        # __init__ method sets tools and knowledge_sources to empty lists
        assert agent.tools == []  # Set by __init__ method
        assert agent.knowledge_sources == []  # Set by __init__ method
        # Test column defaults are configured correctly
        assert Agent.__table__.columns['llm'].default.arg == "databricks-llama-4-maverick"
        assert Agent.__table__.columns['max_iter'].default.arg == 25
        assert Agent.__table__.columns['verbose'].default.arg is False

    def test_agent_creation_with_all_fields(self):
        """Test Agent model creation with all fields populated."""
        # Arrange
        name = "Data Analysis Agent"
        role = "Data Analyst"
        goal = "Analyze data and provide insights"
        backstory = "Experienced in statistical analysis and data visualization"
        group_id = "analytics-team"
        created_by_email = "analyst@company.com"
        llm = "gpt-4"
        tools = ["python_repl", "sql_query", "data_viz"]
        function_calling_llm = "gpt-4-function-calling"
        max_iter = 50
        max_rpm = 10
        max_execution_time = 3600
        verbose = True
        allow_delegation = True
        cache = False
        memory = True
        embedder_config = {"model": "text-embedding-ada-002", "dimensions": 1536}
        system_template = "You are a data analyst agent"
        prompt_template = "Analyze the following data: {data}"
        response_template = "Analysis results: {results}"
        allow_code_execution = True
        code_execution_mode = "docker"
        max_retry_limit = 5
        use_system_prompt = True
        respect_context_window = False
        knowledge_sources = ["company_database", "market_research"]
        
        # Act
        agent = Agent(
            name=name,
            role=role,
            goal=goal,
            backstory=backstory,
            group_id=group_id,
            created_by_email=created_by_email,
            llm=llm,
            tools=tools,
            function_calling_llm=function_calling_llm,
            max_iter=max_iter,
            max_rpm=max_rpm,
            max_execution_time=max_execution_time,
            verbose=verbose,
            allow_delegation=allow_delegation,
            cache=cache,
            memory=memory,
            embedder_config=embedder_config,
            system_template=system_template,
            prompt_template=prompt_template,
            response_template=response_template,
            allow_code_execution=allow_code_execution,
            code_execution_mode=code_execution_mode,
            max_retry_limit=max_retry_limit,
            use_system_prompt=use_system_prompt,
            respect_context_window=respect_context_window,
            knowledge_sources=knowledge_sources
        )
        
        # Assert
        assert agent.name == name
        assert agent.role == role
        assert agent.goal == goal
        assert agent.backstory == backstory
        assert agent.group_id == group_id
        assert agent.created_by_email == created_by_email
        assert agent.llm == llm
        assert agent.tools == tools
        assert agent.function_calling_llm == function_calling_llm
        assert agent.max_iter == max_iter
        assert agent.max_rpm == max_rpm
        assert agent.max_execution_time == max_execution_time
        assert agent.verbose == verbose
        assert agent.allow_delegation == allow_delegation
        assert agent.cache == cache
        assert agent.memory == memory
        assert agent.embedder_config == embedder_config
        assert agent.system_template == system_template
        assert agent.prompt_template == prompt_template
        assert agent.response_template == response_template
        assert agent.allow_code_execution == allow_code_execution
        assert agent.code_execution_mode == code_execution_mode
        assert agent.max_retry_limit == max_retry_limit
        assert agent.use_system_prompt == use_system_prompt
        assert agent.respect_context_window == respect_context_window
        assert agent.knowledge_sources == knowledge_sources

    def test_agent_defaults(self):
        """Test Agent model with default values."""
        # Arrange & Act
        agent = Agent(
            name="Test Agent",
            role="Test Role",
            goal="Test Goal"
        )
        
        # Assert
        # Note: SQLAlchemy defaults are applied when saved to database
        # Test that __init__ method sets lists to empty when None
        assert agent.tools == []
        assert agent.knowledge_sources == []
        # Test column defaults are configured correctly
        assert Agent.__table__.columns['llm'].default.arg == "databricks-llama-4-maverick"
        assert Agent.__table__.columns['max_iter'].default.arg == 25
        assert Agent.__table__.columns['verbose'].default.arg is False
        assert Agent.__table__.columns['allow_delegation'].default.arg is False
        assert Agent.__table__.columns['cache'].default.arg is True
        assert Agent.__table__.columns['memory'].default.arg is True
        assert Agent.__table__.columns['allow_code_execution'].default.arg is False
        assert Agent.__table__.columns['code_execution_mode'].default.arg == "safe"
        assert Agent.__table__.columns['max_retry_limit'].default.arg == 2
        assert Agent.__table__.columns['use_system_prompt'].default.arg is True
        assert Agent.__table__.columns['respect_context_window'].default.arg is True

    def test_agent_init_method_logic(self):
        """Test the custom __init__ method logic."""
        # Test 1: When tools is None
        agent1 = Agent(
            name="Agent 1",
            role="Role 1",
            goal="Goal 1",
            tools=None
        )
        assert agent1.tools == []
        
        # Test 2: When knowledge_sources is None
        agent2 = Agent(
            name="Agent 2",
            role="Role 2",
            goal="Goal 2",
            knowledge_sources=None
        )
        assert agent2.knowledge_sources == []
        
        # Test 3: When tools and knowledge_sources are provided
        agent3 = Agent(
            name="Agent 3",
            role="Role 3",
            goal="Goal 3",
            tools=["tool1", "tool2"],
            knowledge_sources=["source1"]
        )
        assert agent3.tools == ["tool1", "tool2"]
        assert agent3.knowledge_sources == ["source1"]

    def test_agent_llm_configurations(self):
        """Test different LLM configurations."""
        # Test with different LLM providers
        llm_configs = [
            "gpt-4",
            "gpt-3.5-turbo",
            "claude-3-sonnet",
            "databricks-llama-4-maverick",
            "custom-model"
        ]
        
        for llm in llm_configs:
            agent = Agent(
                name=f"Agent for {llm}",
                role="Test Role",
                goal="Test Goal",
                llm=llm
            )
            assert agent.llm == llm

    def test_agent_tools_configuration(self):
        """Test different tools configurations."""
        # Arrange
        tools_configs = [
            [],  # No tools
            ["python_repl"],  # Single tool
            ["python_repl", "sql_query", "web_search"],  # Multiple tools
            ["custom_tool_1", "custom_tool_2", "custom_tool_3", "custom_tool_4"]  # Many tools
        ]
        
        for tools in tools_configs:
            # Act
            agent = Agent(
                name="Tool Test Agent",
                role="Tool Tester",
                goal="Test different tool configurations",
                tools=tools
            )
            
            # Assert
            assert agent.tools == tools
            assert isinstance(agent.tools, list)

    def test_agent_execution_settings(self):
        """Test agent execution settings configurations."""
        # Test low resource configuration
        low_resource_agent = Agent(
            name="Low Resource Agent",
            role="Basic Assistant",
            goal="Simple tasks",
            max_iter=5,
            max_rpm=1,
            max_execution_time=60,
            verbose=False,
            allow_delegation=False,
            cache=True
        )
        
        # Test high resource configuration
        high_resource_agent = Agent(
            name="High Resource Agent",
            role="Complex Analyst",
            goal="Complex analysis tasks",
            max_iter=100,
            max_rpm=60,
            max_execution_time=7200,
            verbose=True,
            allow_delegation=True,
            cache=False
        )
        
        # Assert
        assert low_resource_agent.max_iter == 5
        assert low_resource_agent.max_rpm == 1
        assert low_resource_agent.max_execution_time == 60
        assert low_resource_agent.verbose is False
        assert low_resource_agent.allow_delegation is False
        
        assert high_resource_agent.max_iter == 100
        assert high_resource_agent.max_rpm == 60
        assert high_resource_agent.max_execution_time == 7200
        assert high_resource_agent.verbose is True
        assert high_resource_agent.allow_delegation is True

    def test_agent_memory_and_embedder_config(self):
        """Test agent memory and embedder configurations."""
        # Arrange
        embedder_config = {
            "provider": "openai",
            "model": "text-embedding-ada-002",
            "dimensions": 1536,
            "max_tokens": 8000
        }
        
        # Act
        agent = Agent(
            name="Memory Agent",
            role="Memory Specialist",
            goal="Remember and recall information",
            memory=True,
            embedder_config=embedder_config
        )
        
        # Assert
        assert agent.memory is True
        assert agent.embedder_config == embedder_config
        assert agent.embedder_config["provider"] == "openai"
        assert agent.embedder_config["dimensions"] == 1536

    def test_agent_templates(self):
        """Test agent template configurations."""
        # Arrange
        system_template = "You are a helpful AI assistant specialized in {domain}."
        prompt_template = "User query: {query}\nContext: {context}\nPlease provide a detailed response."
        response_template = "Response: {response}\nConfidence: {confidence}\nSources: {sources}"
        
        # Act
        agent = Agent(
            name="Template Agent",
            role="Template Specialist",
            goal="Use custom templates",
            system_template=system_template,
            prompt_template=prompt_template,
            response_template=response_template
        )
        
        # Assert
        assert agent.system_template == system_template
        assert agent.prompt_template == prompt_template
        assert agent.response_template == response_template
        assert "{domain}" in agent.system_template
        assert "{query}" in agent.prompt_template
        assert "{response}" in agent.response_template

    def test_agent_code_execution_settings(self):
        """Test agent code execution configurations."""
        # Test safe mode
        safe_agent = Agent(
            name="Safe Agent",
            role="Safe Executor",
            goal="Execute safe code only",
            allow_code_execution=True,
            code_execution_mode="safe"
        )
        
        # Test docker mode
        docker_agent = Agent(
            name="Docker Agent",
            role="Docker Executor",
            goal="Execute code in docker",
            allow_code_execution=True,
            code_execution_mode="docker"
        )
        
        # Test disabled code execution
        no_code_agent = Agent(
            name="No Code Agent",
            role="Text Only",
            goal="No code execution",
            allow_code_execution=False
        )
        
        # Assert
        assert safe_agent.allow_code_execution is True
        assert safe_agent.code_execution_mode == "safe"
        
        assert docker_agent.allow_code_execution is True
        assert docker_agent.code_execution_mode == "docker"
        
        assert no_code_agent.allow_code_execution is False
        # Note: code_execution_mode default is applied by database
        assert no_code_agent.code_execution_mode is None or no_code_agent.code_execution_mode == "safe"

    def test_agent_knowledge_sources(self):
        """Test agent knowledge sources configurations."""
        # Arrange
        knowledge_sources = [
            "company_wiki",
            "technical_documentation", 
            "previous_conversations",
            "external_apis",
            "database_schemas"
        ]
        
        # Act
        agent = Agent(
            name="Knowledge Agent",
            role="Knowledge Specialist",
            goal="Access and use various knowledge sources",
            knowledge_sources=knowledge_sources
        )
        
        # Assert
        assert agent.knowledge_sources == knowledge_sources
        assert len(agent.knowledge_sources) == 5
        assert "company_wiki" in agent.knowledge_sources
        assert "database_schemas" in agent.knowledge_sources

    def test_agent_multi_tenant_fields(self):
        """Test multi-tenant fields for group isolation."""
        # Arrange
        group_id = "team-alpha"
        created_by_email = "leader@team-alpha.com"
        
        # Act
        agent = Agent(
            name="Team Agent",
            role="Team Assistant",
            goal="Help team members",
            group_id=group_id,
            created_by_email=created_by_email
        )
        
        # Assert
        assert agent.group_id == group_id
        assert agent.created_by_email == created_by_email

    def test_agent_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Agent.__tablename__ == "agents"

    def test_agent_primary_key_generation(self):
        """Test that primary key uses UUID generation."""
        # Act
        agent = Agent(
            name="UUID Test",
            role="Test Role",
            goal="Test UUID generation"
        )
        
        # Note: The actual UUID is generated when saved to database
        # Here we just test that the default function is set correctly
        # Check that the default is a callable (the generate_uuid function)
        assert callable(Agent.__table__.columns['id'].default.arg)
        assert Agent.__table__.columns['id'].default.arg.__name__ == 'generate_uuid'

    def test_agent_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = Agent.__table__.columns
        
        # Assert - Check that group_id has index
        group_id_column = columns['group_id']
        assert group_id_column.index is True

    def test_agent_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Agent.__table__.columns
        
        # Assert required fields
        assert columns['name'].nullable is False
        assert columns['role'].nullable is False
        assert columns['goal'].nullable is False
        
        # Assert optional fields
        assert columns['backstory'].nullable is True
        assert columns['group_id'].nullable is True
        assert columns['created_by_email'].nullable is True
        
        # Assert JSON columns
        assert "JSON" in str(columns['tools'].type)
        assert "JSON" in str(columns['embedder_config'].type)
        assert "JSON" in str(columns['knowledge_sources'].type)
        
        # Assert Boolean columns
        assert "BOOLEAN" in str(columns['verbose'].type)
        assert "BOOLEAN" in str(columns['allow_delegation'].type)
        assert "BOOLEAN" in str(columns['cache'].type)
        assert "BOOLEAN" in str(columns['memory'].type)

    def test_agent_timestamp_behavior(self):
        """Test timestamp behavior in Agent."""
        # Arrange
        before_creation = datetime.utcnow()
        
        # Act
        agent = Agent(
            name="Timestamp Test",
            role="Time Keeper",
            goal="Test timestamps"
        )
        
        after_creation = datetime.utcnow()
        
        # Assert
        # Note: created_at and updated_at are set by database defaults
        # Here we just verify the column configurations
        created_at_column = Agent.__table__.columns['created_at']
        updated_at_column = Agent.__table__.columns['updated_at']
        
        assert created_at_column.default is not None
        assert updated_at_column.default is not None
        assert updated_at_column.onupdate is not None

    def test_agent_repr(self):
        """Test string representation of Agent model."""
        # Arrange
        agent = Agent(
            name="Repr Test Agent",
            role="Test Role",
            goal="Test representation"
        )
        
        # Act
        repr_str = repr(agent)
        
        # Assert
        assert "Agent" in repr_str

    def test_agent_complex_json_fields(self):
        """Test Agent with complex JSON field configurations."""
        # Arrange
        complex_embedder_config = {
            "provider": "custom",
            "model_config": {
                "name": "custom-embedder-v2",
                "dimensions": 2048,
                "context_window": 4096,
                "batch_size": 32
            },
            "preprocessing": {
                "normalize": True,
                "remove_stopwords": False,
                "tokenizer": "custom-tokenizer"
            }
        }
        
        complex_tools = [
            {
                "name": "advanced_calculator",
                "config": {"precision": "high", "mode": "scientific"}
            },
            {
                "name": "database_connector",
                "config": {"host": "localhost", "timeout": 30}
            }
        ]
        
        # Act
        agent = Agent(
            name="Complex JSON Agent",
            role="JSON Specialist",
            goal="Handle complex JSON configurations",
            embedder_config=complex_embedder_config,
            tools=complex_tools
        )
        
        # Assert
        assert agent.embedder_config["provider"] == "custom"
        assert agent.embedder_config["model_config"]["dimensions"] == 2048
        assert agent.tools[0]["name"] == "advanced_calculator"
        assert agent.tools[1]["config"]["timeout"] == 30


class TestGenerateUuid:
    """Test cases for generate_uuid function."""

    def test_generate_uuid_function(self):
        """Test the generate_uuid function."""
        # Act
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        # Assert
        assert uuid1 is not None
        assert uuid2 is not None
        assert uuid1 != uuid2
        assert isinstance(uuid1, str)
        assert isinstance(uuid2, str)
        assert len(uuid1) == 36  # Standard UUID length
        assert len(uuid2) == 36

    def test_generate_uuid_uniqueness(self):
        """Test that generate_uuid generates unique IDs."""
        # Act
        uuids = [generate_uuid() for _ in range(100)]
        
        # Assert
        assert len(set(uuids)) == 100  # All UUIDs should be unique


class TestAgentEdgeCases:
    """Test edge cases and error scenarios for Agent."""

    def test_agent_with_empty_strings(self):
        """Test Agent with empty string values."""
        # Act
        agent = Agent(
            name="",
            role="",
            goal="",
            backstory=""
        )
        
        # Assert
        assert agent.name == ""
        assert agent.role == ""
        assert agent.goal == ""
        assert agent.backstory == ""

    def test_agent_with_very_long_strings(self):
        """Test Agent with very long string values."""
        # Arrange
        long_name = "Agent " * 100  # 600 characters
        long_role = "Role " * 100   # 500 characters
        long_goal = "Goal " * 100   # 500 characters
        long_backstory = "Backstory " * 200  # 2000 characters
        
        # Act
        agent = Agent(
            name=long_name,
            role=long_role,
            goal=long_goal,
            backstory=long_backstory
        )
        
        # Assert
        assert len(agent.name) == 600
        assert len(agent.role) == 500
        assert len(agent.goal) == 500
        assert len(agent.backstory) == 2000

    def test_agent_extreme_execution_settings(self):
        """Test Agent with extreme execution settings."""
        # Act
        extreme_agent = Agent(
            name="Extreme Agent",
            role="Stress Tester",
            goal="Test extreme values",
            max_iter=0,  # Minimum
            max_rpm=0,   # Minimum  
            max_execution_time=0,  # Minimum
            max_retry_limit=0  # Minimum
        )
        
        # Assert
        assert extreme_agent.max_iter == 0
        assert extreme_agent.max_rpm == 0
        assert extreme_agent.max_execution_time == 0
        assert extreme_agent.max_retry_limit == 0

    def test_agent_common_use_cases(self):
        """Test Agent configurations for common use cases."""
        # Research Agent
        research_agent = Agent(
            name="Research Specialist",
            role="Senior Researcher",
            goal="Conduct comprehensive research on any topic",
            backstory="PhD in Information Sciences with 10 years of research experience",
            tools=["web_search", "academic_database", "citation_manager"],
            max_iter=50,
            verbose=True,
            memory=True
        )
        
        # Code Assistant Agent
        code_agent = Agent(
            name="Code Assistant",
            role="Senior Software Engineer",
            goal="Help with coding tasks and code review",
            backstory="Full-stack developer with expertise in multiple programming languages",
            tools=["python_repl", "code_analyzer", "documentation_search"],
            allow_code_execution=True,
            code_execution_mode="docker",
            max_iter=30
        )
        
        # Data Analysis Agent
        data_agent = Agent(
            name="Data Analyst",
            role="Data Science Expert",
            goal="Analyze data and provide insights",
            backstory="Statistics PhD with expertise in machine learning and data visualization",
            tools=["python_repl", "sql_query", "data_viz", "statistical_analysis"],
            allow_code_execution=True,
            max_execution_time=3600
        )
        
        # Assert
        assert "Research" in research_agent.name
        assert research_agent.verbose is True
        assert "web_search" in research_agent.tools
        
        assert code_agent.allow_code_execution is True
        assert code_agent.code_execution_mode == "docker"
        assert "python_repl" in code_agent.tools
        
        assert data_agent.max_execution_time == 3600
        assert "statistical_analysis" in data_agent.tools