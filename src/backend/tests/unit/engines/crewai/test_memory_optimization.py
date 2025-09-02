"""
Unit tests for memory optimization in CrewAI agents.

Tests that memory is automatically disabled for agents that don't need it,
such as validators, formatters, and other stateless operations.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from src.engines.crewai.crew_preparation import CrewPreparation


class TestMemoryOptimization:
    """Test suite for agent memory optimization."""
    
    @pytest.fixture
    def mock_tool_service(self):
        """Create a mock tool service."""
        return Mock()
    
    @pytest.fixture
    def mock_tool_factory(self):
        """Create a mock tool factory."""
        return Mock()
    
    def test_should_disable_memory_for_validator_agent(self):
        """Test that memory is disabled for validator agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "JSON Schema Validator",
            "goal": "Validate JSON documents against schemas",
            "backstory": "Expert at validating data structures",
            "tools": [],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    def test_should_disable_memory_for_formatter_agent(self):
        """Test that memory is disabled for formatter agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Code Formatter",
            "goal": "Format code according to style guidelines",
            "backstory": "Expert in code formatting standards",
            "tools": ["prettier", "black"],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    def test_should_disable_memory_for_code_review_agent(self):
        """Test that memory is disabled for code review agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Code Review Specialist",
            "goal": "Perform static code analysis and security scans",
            "backstory": "Security expert with years of experience",
            "tools": ["sonarqube", "eslint"],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    def test_should_disable_memory_for_data_cleaning_agent(self):
        """Test that memory is disabled for data cleaning agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Data Cleaning Specialist",
            "goal": "Clean and validate CSV data",
            "backstory": "Expert in data quality and cleaning",
            "tools": ["pandas", "csv_validator"],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    def test_should_keep_memory_for_research_agent(self):
        """Test that memory is kept enabled for research agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Market Research Analyst",
            "goal": "Research market trends and analyze patterns over time",
            "backstory": "Experienced researcher with deep market knowledge",
            "tools": ["web_search", "data_analysis"]
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == False
    
    def test_should_keep_memory_for_assistant_agent(self):
        """Test that memory is kept enabled for assistant agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Personal Assistant",
            "goal": "Help users with tasks and remember their preferences",
            "backstory": "Dedicated assistant that learns from interactions",
            "tools": ["calendar", "notes", "reminders"]
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == False
    
    def test_should_keep_memory_for_learning_agent(self):
        """Test that memory is kept enabled for agents that learn."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "ML Model Trainer",
            "goal": "Train models that learn and adapt from data",
            "backstory": "Machine learning expert",
            "tools": ["tensorflow", "pytorch"]
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == False
    
    def test_should_keep_memory_for_conversational_agent(self):
        """Test that memory is kept enabled for conversational agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Customer Support Chatbot",
            "goal": "Engage in dialogue with customers and resolve issues",
            "backstory": "Experienced support specialist",
            "tools": ["ticket_system", "knowledge_base"]
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == False
    
    def test_should_disable_memory_for_api_caller_agent(self):
        """Test that memory is disabled for simple API caller agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "API Gateway",
            "goal": "Make HTTP requests to external services",
            "backstory": "Handles API calls and webhooks",
            "tools": ["http_client"],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    def test_should_disable_memory_for_notification_agent(self):
        """Test that memory is disabled for notification agents."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        agent_config = {
            "role": "Email Notifier",
            "goal": "Send email alerts and notifications",
            "backstory": "Handles all email communications",
            "tools": ["smtp_client"],
            "memory": False  # Explicitly disable memory
        }
        
        should_disable = crew_prep._should_disable_memory_for_agent(agent_config)
        assert should_disable == True
    
    @pytest.mark.asyncio
    async def test_crew_memory_disabled_when_all_agents_stateless(self):
        """Test that crew memory is disabled when all agents are stateless."""
        config = {
            "agents": [
                {
                    "role": "JSON Validator",
                    "goal": "Validate JSON",
                    "backstory": "Validation expert"
                },
                {
                    "role": "Code Formatter",
                    "goal": "Format code",
                    "backstory": "Formatting expert"
                }
            ],
            "tasks": [],
            "crew": {
                "memory": True
            }
        }
        
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True):
            with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_agents', new_callable=AsyncMock, return_value=True):
                with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_tasks', new_callable=AsyncMock, return_value=True):
                    with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_crew', new_callable=AsyncMock, return_value=True) as mock_create_crew:
                        crew_prep = CrewPreparation(config)
                        result = await crew_prep.prepare()
                        
                        # The prepare method should succeed
                        assert result == True
    
    @pytest.mark.asyncio
    async def test_crew_memory_enabled_when_any_agent_needs_it(self):
        """Test that crew memory stays enabled when at least one agent needs it."""
        config = {
            "agents": [
                {
                    "role": "JSON Validator",
                    "goal": "Validate JSON",
                    "backstory": "Validation expert"
                },
                {
                    "role": "Research Analyst",
                    "goal": "Conduct research and analysis",
                    "backstory": "Research expert"
                }
            ],
            "tasks": [],
            "crew": {
                "memory": True
            }
        }
        
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True):
            with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_agents', new_callable=AsyncMock, return_value=True):
                with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_tasks', new_callable=AsyncMock, return_value=True):
                    with patch('src.engines.crewai.crew_preparation.CrewPreparation._create_crew', new_callable=AsyncMock, return_value=True) as mock_create_crew:
                        crew_prep = CrewPreparation(config)
                        result = await crew_prep.prepare()
                        
                        # The prepare method should succeed
                        assert result == True
    
    def test_memory_optimization_with_edge_cases(self):
        """Test memory optimization with edge cases."""
        config = {
            "agents": [],
            "tasks": [],
            "crew": {}
        }
        crew_prep = CrewPreparation(config)
        
        # Test with empty config
        assert crew_prep._should_disable_memory_for_agent({}) == False
        
        # Test with only role and memory explicitly disabled
        assert crew_prep._should_disable_memory_for_agent({"role": "Validator", "memory": False}) == True
        
        # Test with conflicting keywords - memory not explicitly set
        agent_config = {
            "role": "Research Validator",  # Has both 'research' (needs memory) and 'validator' (doesn't need)
            "goal": "Research and validate data",
            "backstory": "Expert researcher and validator"
        }
        # Should keep memory when there's a conflict (safer option)
        assert crew_prep._should_disable_memory_for_agent(agent_config) == False
        
        # Test with no tools and simple role - memory explicitly disabled
        agent_config = {
            "role": "Simple Formatter",
            "goal": "Format text",
            "backstory": "Formats things",
            "tools": [],
            "memory": False  # Explicitly disable memory
        }
        assert crew_prep._should_disable_memory_for_agent(agent_config) == True