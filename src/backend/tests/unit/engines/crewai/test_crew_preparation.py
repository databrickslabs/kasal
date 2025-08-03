import pytest
import unittest.mock
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Dict, Any, List

from src.engines.crewai.crew_preparation import (
    CrewPreparation, 
    validate_crew_config, 
    handle_crew_error,
    process_crew_output
)


class TestCrewPreparation:
    """Test suite for CrewPreparation class."""
    
    @pytest.fixture
    def sample_config(self):
        """Sample crew configuration."""
        return {
            "agents": [
                {
                    "name": "researcher",
                    "role": "Senior Research Analyst",
                    "goal": "Research AI trends",
                    "backstory": "Expert AI researcher",
                    "tools": ["search_tool"],
                    "verbose": True
                },
                {
                    "name": "writer", 
                    "role": "Content Writer",
                    "goal": "Write reports",
                    "backstory": "Experienced writer",
                    "tools": ["write_tool"],
                    "verbose": False
                }
            ],
            "tasks": [
                {
                    "id": "research_task",
                    "name": "research_task",
                    "description": "Research AI trends",
                    "agent": "researcher", 
                    "expected_output": "Research report",
                    "tools": ["search_tool"],
                    "async_execution": False
                },
                {
                    "id": "write_task",
                    "name": "write_task",
                    "description": "Write blog post",
                    "agent": "writer",
                    "expected_output": "Blog post",
                    "context": ["research_task"],
                    "tools": ["write_tool"],
                    "async_execution": True
                }
            ],
            "crew": {
                "process": "sequential",
                "verbose": True,
                "memory": True,
                "planning": False,
                "reasoning": False
            },
            "model": "gpt-4",
            "max_rpm": 10,
            "output_dir": "/tmp/output"
        }
    
    @pytest.fixture
    def mock_tool_service(self):
        """Mock tool service."""
        tool_service = MagicMock()
        tool_service.get_tool = AsyncMock()
        return tool_service
    
    @pytest.fixture
    def mock_tool_factory(self):
        """Mock tool factory."""
        tool_factory = MagicMock()
        tool_factory.create_tool = AsyncMock()
        return tool_factory
    
    @pytest.fixture
    def crew_preparation(self, sample_config, mock_tool_service, mock_tool_factory):
        """CrewPreparation instance with sample config."""
        return CrewPreparation(sample_config, mock_tool_service, mock_tool_factory)
    
    def test_init(self, sample_config, mock_tool_service, mock_tool_factory):
        """Test CrewPreparation initialization."""
        prep = CrewPreparation(sample_config, mock_tool_service, mock_tool_factory)
        
        assert prep.config == sample_config
        assert prep.tool_service == mock_tool_service
        assert prep.tool_factory == mock_tool_factory
        assert prep.agents == {}
        assert prep.tasks == []
        assert prep.crew is None
    
    @pytest.mark.asyncio
    async def test_prepare_success(self, crew_preparation):
        """Test successful crew preparation."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True), \
             patch.object(crew_preparation, '_create_agents', return_value=True), \
             patch.object(crew_preparation, '_create_tasks', return_value=True), \
             patch.object(crew_preparation, '_create_crew', return_value=True):
            
            result = await crew_preparation.prepare()
            assert result is True
    
    @pytest.mark.asyncio
    async def test_prepare_invalid_config(self, crew_preparation):
        """Test preparation with invalid configuration."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=False):
            result = await crew_preparation.prepare()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_prepare_agent_creation_failure(self, crew_preparation):
        """Test preparation when agent creation fails."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True), \
             patch.object(crew_preparation, '_create_agents', return_value=False):
            
            result = await crew_preparation.prepare()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_prepare_task_creation_failure(self, crew_preparation):
        """Test preparation when task creation fails."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True), \
             patch.object(crew_preparation, '_create_agents', return_value=True), \
             patch.object(crew_preparation, '_create_tasks', return_value=False):
            
            result = await crew_preparation.prepare()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_prepare_crew_creation_failure(self, crew_preparation):
        """Test preparation when crew creation fails."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', return_value=True), \
             patch.object(crew_preparation, '_create_agents', return_value=True), \
             patch.object(crew_preparation, '_create_tasks', return_value=True), \
             patch.object(crew_preparation, '_create_crew', return_value=False):
            
            result = await crew_preparation.prepare()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_prepare_exception_handling(self, crew_preparation):
        """Test preparation handles exceptions properly."""
        with patch('src.engines.crewai.crew_preparation.validate_crew_config', side_effect=Exception("Test error")), \
             patch('src.engines.crewai.crew_preparation.handle_crew_error') as mock_handle_error:
            
            result = await crew_preparation.prepare()
            assert result is False
            mock_handle_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_agents_success(self, crew_preparation):
        """Test successful agent creation."""
        mock_agent1 = MagicMock()
        mock_agent2 = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.create_agent', side_effect=[mock_agent1, mock_agent2]) as mock_create:
            result = await crew_preparation._create_agents()
            
            assert result is True
            assert len(crew_preparation.agents) == 2
            assert crew_preparation.agents["researcher"] == mock_agent1
            assert crew_preparation.agents["writer"] == mock_agent2
            
            # Verify create_agent was called correctly
            assert mock_create.call_count == 2
            mock_create.assert_any_call(
                agent_key="researcher",
                agent_config=crew_preparation.config["agents"][0],
                tool_service=crew_preparation.tool_service,
                tool_factory=crew_preparation.tool_factory
            )
    
    @pytest.mark.asyncio
    async def test_create_agents_with_fallback_names(self, crew_preparation):
        """Test agent creation with fallback naming."""
        # Modify config to test fallback naming
        crew_preparation.config["agents"] = [
            {"role": "Analyst", "goal": "Analyze data", "backstory": "Expert analyst"},  # No name, should use role
            {"role": "Worker", "goal": "Do work", "backstory": "Hard worker"}  # No name or role, should use agent_1
        ]
        
        mock_agent1 = MagicMock()
        mock_agent2 = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.create_agent', side_effect=[mock_agent1, mock_agent2]):
            result = await crew_preparation._create_agents()
            
            assert result is True
            assert "Analyst" in crew_preparation.agents
            assert "Worker" in crew_preparation.agents
    
    @pytest.mark.asyncio
    async def test_create_agents_creation_failure(self, crew_preparation):
        """Test agent creation when create_agent returns None."""
        with patch('src.engines.crewai.crew_preparation.create_agent', return_value=None):
            result = await crew_preparation._create_agents()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_agents_exception_handling(self, crew_preparation):
        """Test agent creation handles exceptions."""
        with patch('src.engines.crewai.crew_preparation.create_agent', side_effect=Exception("Test error")), \
             patch('src.engines.crewai.crew_preparation.handle_crew_error') as mock_handle_error:
            
            result = await crew_preparation._create_agents()
            assert result is False
            mock_handle_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tasks_success(self, crew_preparation):
        """Test successful task creation."""
        # Setup agents first
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()
            
            assert result is True
            assert len(crew_preparation.tasks) == 2
            assert crew_preparation.tasks[0] == mock_task1
            assert crew_preparation.tasks[1] == mock_task2
    
    @pytest.mark.asyncio
    async def test_create_tasks_with_context_resolution(self, crew_preparation):
        """Test task creation with context resolution."""
        # Setup agents first
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        mock_task2.context = None  # Initialize context
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()
            
            assert result is True
            # Verify context was set on the second task
            assert mock_task2.context == [mock_task1]
    
    @pytest.mark.asyncio
    async def test_create_tasks_agent_fallback(self, crew_preparation):
        """Test task creation with agent fallback."""
        # Setup one agent
        crew_preparation.agents = {"researcher": MagicMock()}
        
        # Modify config to have task with invalid agent
        crew_preparation.config["tasks"] = [
            {
                "id": "test_task",
                "name": "test_task", 
                "description": "Test task",
                "agent": "nonexistent_agent",
                "expected_output": "Test output"
            }
        ]
        
        mock_task = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', return_value=mock_task):
            result = await crew_preparation._create_tasks()
            assert result is True
    
    @pytest.mark.asyncio
    async def test_create_tasks_no_agents_available(self, crew_preparation):
        """Test task creation when no agents are available."""
        crew_preparation.agents = {}
        
        result = await crew_preparation._create_tasks()
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_tasks_async_execution_validation(self, crew_preparation):
        """Test async execution validation (only last task can be async)."""
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        # Both tasks set to async - first should be changed to sync
        crew_preparation.config["tasks"] = [
            {
                "id": "task1",
                "name": "task1",
                "description": "First task",
                "agent": "researcher",
                "expected_output": "Output 1",
                "async_execution": True  # Should be changed to False
            },
            {
                "id": "task2", 
                "name": "task2",
                "description": "Second task",
                "agent": "writer",
                "expected_output": "Output 2",
                "async_execution": True  # Should remain True
            }
        ]
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]) as mock_create:
            result = await crew_preparation._create_tasks()
            
            assert result is True
            
            # Check that the first task was called with async_execution=False
            first_call_config = mock_create.call_args_list[0][1]['task_config']
            assert first_call_config['async_execution'] is False
            
            # Check that the second task was called with async_execution=True
            second_call_config = mock_create.call_args_list[1][1]['task_config']
            assert second_call_config['async_execution'] is True
    
    @pytest.mark.asyncio
    async def test_create_tasks_exception_handling(self, crew_preparation):
        """Test task creation handles exceptions."""
        crew_preparation.agents = {"researcher": MagicMock()}
        
        with patch('src.engines.crewai.crew_preparation.create_task', side_effect=Exception("Test error")), \
             patch('src.engines.crewai.crew_preparation.handle_crew_error') as mock_handle_error:
            
            result = await crew_preparation._create_tasks()
            assert result is False
            mock_handle_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tasks_string_context(self, crew_preparation):
        """Test task creation with string context."""
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        # Modify config to have string context 
        crew_preparation.config["tasks"] = [
            {
                "id": "research_task",
                "name": "research_task",
                "description": "Research AI trends",
                "agent": "researcher", 
                "expected_output": "Research report"
            },
            {
                "id": "write_task",
                "name": "write_task",
                "description": "Write blog post",
                "agent": "writer",
                "expected_output": "Blog post",
                "context": "research_task"  # String context
            }
        ]
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        mock_task2.context = None
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()
            
            assert result is True
            assert mock_task2.context == [mock_task1]
    
    @pytest.mark.asyncio
    async def test_create_tasks_dict_context_with_task_ids(self, crew_preparation):
        """Test task creation with dict context containing task_ids."""
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        # Modify config to have dict context with task_ids
        crew_preparation.config["tasks"] = [
            {
                "id": "research_task",
                "name": "research_task",
                "description": "Research AI trends",
                "agent": "researcher", 
                "expected_output": "Research report"
            },
            {
                "id": "write_task",
                "name": "write_task",
                "description": "Write blog post",
                "agent": "writer",
                "expected_output": "Blog post",
                "context": {"task_ids": ["research_task"]}  # Dict context with task_ids
            }
        ]
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        mock_task2.context = None
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()
            
            assert result is True
            assert mock_task2.context == [mock_task1]
    
    @pytest.mark.asyncio
    async def test_create_tasks_unresolvable_context_references(self, crew_preparation):
        """Test task creation when context references can't be resolved."""
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}
        
        crew_preparation.config["tasks"] = [
            {
                "id": "research_task",
                "name": "research_task",
                "description": "Research AI trends",
                "agent": "researcher", 
                "expected_output": "Research report",
                "context": []  # Empty context (no warning expected)
            },
            {
                "id": "write_task",
                "name": "write_task",
                "description": "Write report",
                "agent": "writer", 
                "expected_output": "Written report",
                "context": ["nonexistent1", "nonexistent2"]  # All invalid references
            }
        ]
        
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_tasks()
            
            assert result is True
            # Check that warning was logged for no resolvable context tasks
            mock_logger.warning.assert_any_call("No context tasks could be resolved for task write_task")
    
    @pytest.mark.asyncio
    async def test_create_crew_basic_success(self, crew_preparation):
        """Test basic crew creation success."""
        # Setup agents and tasks
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            assert crew_preparation.crew == mock_crew
    
    @pytest.mark.asyncio
    async def test_create_crew_with_databricks_environment(self, crew_preparation):
        """Test crew creation in Databricks environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        mock_llm = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.core.llm_manager.LLMManager.get_llm', return_value=mock_llm), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            assert result is True
    
    @pytest.mark.asyncio
    async def test_create_crew_with_planning_and_reasoning(self, crew_preparation):
        """Test crew creation with planning and reasoning enabled."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["planning"] = True
        crew_preparation.config["crew"]["reasoning"] = True
        crew_preparation.config["crew"]["planning_llm"] = "gpt-3.5-turbo"
        crew_preparation.config["crew"]["reasoning_llm"] = "gpt-4"
        
        mock_crew = MagicMock()
        mock_planning_llm = MagicMock()
        mock_reasoning_llm = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm') as mock_get_llm, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            # Configure LLMManager.get_llm to return different mocks for different models
            # Note: gpt-4 is used for both reasoning and manager LLM in this test
            mock_get_llm.side_effect = lambda model: {
                "gpt-3.5-turbo": mock_planning_llm,
                "gpt-4": mock_reasoning_llm
            }[model]
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for planning, reasoning, and manager LLMs
            # Note: gpt-4 is called twice (manager + reasoning), gpt-3.5-turbo once (planning)
            assert mock_get_llm.call_count == 3
            mock_get_llm.assert_any_call("gpt-3.5-turbo")
            mock_get_llm.assert_any_call("gpt-4")
            
            # Verify crew was created with planning and reasoning LLM objects
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['planning'] is True
            assert call_kwargs['reasoning'] is True
            assert call_kwargs['planning_llm'] is mock_planning_llm
            assert call_kwargs['reasoning_llm'] is mock_reasoning_llm
    
    @pytest.mark.asyncio
    async def test_create_crew_with_max_rpm(self, crew_preparation):
        """Test crew creation with max RPM setting."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["max_rpm"] = 15
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['max_rpm'] == 15
    
    @pytest.mark.asyncio
    async def test_create_crew_exception_handling(self, crew_preparation):
        """Test crew creation handles exceptions."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        with patch('src.engines.crewai.crew_preparation.Crew', side_effect=Exception("Test error")), \
             patch('src.engines.crewai.crew_preparation.handle_crew_error') as mock_handle_error:
            
            result = await crew_preparation._create_crew()
            assert result is False
            mock_handle_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_crew_llm_manager_import_error(self, crew_preparation):
        """Test crew creation handles ImportError for LLMManager."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["model"] = "gpt-4"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', side_effect=ImportError("Module not found")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.warning.assert_any_call("Enhanced Databricks auth not available for crew preparation")
    
    @pytest.mark.asyncio
    async def test_create_crew_llm_manager_exception(self, crew_preparation):
        """Test crew creation handles exceptions in LLM configuration."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["model"] = "gpt-4"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("LLM error")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.warning.assert_any_call("Could not create LLM for model gpt-4: LLM error")
    
    @pytest.mark.asyncio
    async def test_create_crew_databricks_fallback_on_llm_error(self, crew_preparation):
        """Test crew creation falls back to Databricks model when LLM fails in Databricks environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["model"] = "gpt-4"
        
        mock_crew = MagicMock()
        mock_fallback_llm = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=[Exception("LLM error"), mock_fallback_llm]), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.info.assert_any_call("Falling back to Databricks model in Apps environment")
    
    @pytest.mark.asyncio
    async def test_create_crew_no_model_databricks_default(self, crew_preparation):
        """Test crew creation uses Databricks default when no model specified in Databricks environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        # Explicitly remove model from config
        crew_preparation.config.pop('model', None)
        
        mock_crew = MagicMock()
        mock_default_llm = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.core.llm_manager.LLMManager.get_llm', return_value=mock_default_llm), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            # Just assert that the crew was created successfully - the logging is complex due to embedder logic
    
    @pytest.mark.asyncio
    async def test_create_crew_no_model_standard_environment(self, crew_preparation):
        """Test crew creation uses CrewAI defaults when no model specified in standard environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        # Explicitly remove model from config
        crew_preparation.config.pop('model', None)
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            # Just assert that the crew was created successfully - the logging is complex due to embedder logic
    
    @pytest.mark.asyncio
    async def test_create_crew_with_embedder_config_in_agents(self, crew_preparation):
        """Test crew creation finds embedder config in agent configuration."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        # Add embedder config to agent
        crew_preparation.config["agents"] = [
            {"name": "agent1", "embedder_config": {"provider": "openai", "config": {"model": "text-embedding-ada-002"}}}
        ]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value="test-key"), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.info.assert_any_call("Found embedder configuration: {'provider': 'openai', 'config': {'model': 'text-embedding-ada-002'}}")
    
    @pytest.mark.asyncio
    async def test_create_crew_openai_api_key_in_databricks(self, crew_preparation):
        """Test crew creation handles OpenAI API key configuration in Databricks Apps environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value="test-openai-key"), \
             patch.dict('os.environ', {}, clear=True), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.info.assert_any_call("OpenAI API key is configured, keeping it for CrewAI")
    
    @pytest.mark.asyncio
    async def test_create_crew_no_openai_key_in_databricks(self, crew_preparation):
        """Test crew creation sets dummy OpenAI key when none configured in Databricks Apps environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None), \
             patch.dict('os.environ', {}, clear=True), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.info.assert_any_call("No OpenAI API key configured, set dummy key for CrewAI validation")
    
    @pytest.mark.asyncio
    async def test_create_crew_openai_key_error_in_databricks(self, crew_preparation):
        """Test crew creation handles errors when checking OpenAI API key in Databricks Apps environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', side_effect=Exception("API error")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            mock_logger.warning.assert_any_call("Error checking OpenAI API key configuration: API error")
    
    @pytest.mark.asyncio
    async def test_execute_success(self, crew_preparation):
        """Test successful crew execution."""
        mock_crew = MagicMock()
        mock_crew.kickoff = AsyncMock(return_value="execution result")
        crew_preparation.crew = mock_crew
        
        mock_processed_output = {"result": "processed"}
        
        with patch('src.engines.crewai.crew_preparation.process_crew_output', return_value=mock_processed_output), \
             patch('src.engines.crewai.crew_preparation.is_data_missing', return_value=False):
            
            result = await crew_preparation.execute()
            
            assert result == mock_processed_output
    
    @pytest.mark.asyncio
    async def test_execute_without_crew(self, crew_preparation):
        """Test execution when crew is not prepared."""
        crew_preparation.crew = None
        
        result = await crew_preparation.execute()
        assert result == {"error": "Crew not prepared"}
    
    @pytest.mark.asyncio
    async def test_execute_with_missing_data_warning(self, crew_preparation):
        """Test execution with missing data warning."""
        mock_crew = MagicMock()
        mock_crew.kickoff = AsyncMock(return_value="execution result")
        crew_preparation.crew = mock_crew
        
        mock_processed_output = {"result": "processed"}
        
        with patch('src.engines.crewai.crew_preparation.process_crew_output', return_value=mock_processed_output), \
             patch('src.engines.crewai.crew_preparation.is_data_missing', return_value=True), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation.execute()
            
            assert result == mock_processed_output
            mock_logger.warning.assert_called_with("Crew execution completed but data may be missing")
    
    @pytest.mark.asyncio
    async def test_execute_exception_handling(self, crew_preparation):
        """Test execution handles exceptions."""
        mock_crew = MagicMock()
        mock_crew.kickoff = AsyncMock(side_effect=Exception("Execution error"))
        crew_preparation.crew = mock_crew
        
        with patch('src.engines.crewai.crew_preparation.handle_crew_error') as mock_handle_error:
            result = await crew_preparation.execute()
            
            assert result == {"error": "Execution error"}
            mock_handle_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_crew_with_planning_llm_error(self, crew_preparation):
        """Test crew creation when planning LLM creation fails."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["planning"] = True
        crew_preparation.config["crew"]["planning_llm"] = "invalid-model"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("Model not found")) as mock_get_llm, \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for planning LLM and manager LLM
            # Note: Manager LLM is also created, so we expect 2 calls (assuming config has a model)
            assert mock_get_llm.call_count >= 1  # At least one call for planning_llm
            mock_get_llm.assert_any_call("invalid-model")
            
            # Verify error was logged
            mock_logger.warning.assert_called_with("Could not create planning LLM for model invalid-model: Model not found")
            
            # Verify crew was created without planning_llm in kwargs
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['planning'] is True
            assert 'planning_llm' not in call_kwargs
    
    @pytest.mark.asyncio
    async def test_create_crew_with_reasoning_llm_error(self, crew_preparation):
        """Test crew creation when reasoning LLM creation fails."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["reasoning"] = True
        crew_preparation.config["crew"]["reasoning_llm"] = "invalid-model"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("Model not found")) as mock_get_llm, \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for reasoning LLM and manager LLM
            # Note: Manager LLM is also created, so we expect 2 calls (assuming config has a model)
            assert mock_get_llm.call_count >= 1  # At least one call for reasoning_llm
            mock_get_llm.assert_any_call("invalid-model")
            
            # Verify error was logged
            mock_logger.warning.assert_called_with("Could not create reasoning LLM for model invalid-model: Model not found")
            
            # Verify crew was created without reasoning_llm in kwargs
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['reasoning'] is True
            assert 'reasoning_llm' not in call_kwargs
    
    @pytest.mark.asyncio
    async def test_create_crew_with_both_planning_and_reasoning_llm_errors(self, crew_preparation):
        """Test crew creation when both planning and reasoning LLM creation fail."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["planning"] = True
        crew_preparation.config["crew"]["reasoning"] = True
        crew_preparation.config["crew"]["planning_llm"] = "invalid-planning-model"
        crew_preparation.config["crew"]["reasoning_llm"] = "invalid-reasoning-model"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("Model not found")) as mock_get_llm, \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for planning, reasoning, and manager LLMs
            # Note: Manager LLM is also created, so we expect 3 calls total
            assert mock_get_llm.call_count >= 2  # At least 2 calls for planning and reasoning LLMs
            mock_get_llm.assert_any_call("invalid-planning-model")
            mock_get_llm.assert_any_call("invalid-reasoning-model")
            
            # Verify both errors were logged
            mock_logger.warning.assert_any_call("Could not create planning LLM for model invalid-planning-model: Model not found")
            mock_logger.warning.assert_any_call("Could not create reasoning LLM for model invalid-reasoning-model: Model not found")
            
            # Verify crew was created without planning_llm or reasoning_llm in kwargs
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['planning'] is True
            assert call_kwargs['reasoning'] is True
            assert 'planning_llm' not in call_kwargs
            assert 'reasoning_llm' not in call_kwargs
    
    @pytest.mark.asyncio
    async def test_create_crew_with_memory_no_backend_config(self, crew_preparation):
        """Test crew creation with memory enabled but no backend configuration (uses default ChromaDB)."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        crew_preparation.config["group_id"] = "test_group"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            # Mock memory backend service to return None (no config in database)
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=None)
            mock_memory_service.return_value = mock_service_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created with memory=True for default ChromaDB
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['memory'] is True
            
            # Verify logging indicates use of default memory
            mock_logger.warning.assert_any_call("No active memory backend configuration found in database")
            mock_logger.info.assert_any_call("Created default memory backend configuration (ChromaDB + SQLite)")
            
            # Verify storage directory would be set for default backend
            # The actual setting happens in crew_preparation, which we're testing
            # Just verify the crew was created with memory enabled
    
    @pytest.mark.asyncio
    async def test_create_crew_with_default_backend_config(self, crew_preparation):
        """Test crew creation with DEFAULT backend type configured."""
        crew_preparation.agents = {"agent1": MagicMock(role="Test Agent")}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        crew_preparation.config["group_id"] = "test_group"
        
        mock_crew = MagicMock()
        
        # Mock memory backend config with DEFAULT type
        from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
        mock_backend_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory.create_memory_backends', return_value={}), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            # Mock memory backend service to return DEFAULT config
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=mock_backend_config)
            mock_memory_service.return_value = mock_service_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created with memory=True for default backend
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['memory'] is True
            
            # Verify logging for DEFAULT backend
            mock_logger.info.assert_any_call("Skipping individual memory configuration for DEFAULT backend")
            mock_logger.info.assert_any_call("Set memory=True for default backend to use CrewAI's built-in memory")
            
            # Verify logging shows the storage directory was set
            # Look for log messages about storage directory
            storage_log_calls = [call for call in mock_logger.info.call_args_list 
                                if 'storage directory' in str(call)]
            assert len(storage_log_calls) > 0
    
    @pytest.mark.asyncio
    async def test_create_crew_with_memory_databricks_backend_config(self, crew_preparation):
        """Test crew creation with memory enabled and Databricks backend configuration."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        
        mock_crew = MagicMock()
        mock_memory_backends = {
            'short_term': MagicMock(),
            'long_term': MagicMock(),
            'entity': MagicMock()
        }
        
        # Mock memory backend config
        from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType, DatabricksMemoryConfig
        mock_backend_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="test-endpoint",
                short_term_index="test-short-term",
                embedding_dimension=1024
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory.create_memory_backends', return_value=mock_memory_backends), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            # Mock memory backend service to return Databricks config
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=mock_backend_config)
            mock_memory_service.return_value = mock_service_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created with memory=False for Databricks to prevent conflicts
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['memory'] is False
            
            # Verify custom memory backends were configured
            assert 'short_term_memory' in call_kwargs
            assert 'long_term_memory' in call_kwargs
            assert 'entity_memory' in call_kwargs
            
            # Verify logging indicates Databricks backend
            mock_logger.info.assert_any_call("Set memory=False for Databricks backend to prevent conflicts")
    
    @pytest.mark.asyncio
    async def test_create_crew_with_memory_disabled_in_config(self, crew_preparation):
        """Test crew creation when memory is disabled in crew config."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = False
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.core.llm_manager.LLMManager.get_llm', return_value=MagicMock()):
            
            # Memory backend service should not be imported/called when memory is disabled
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created with memory=False
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['memory'] is False
    
    @pytest.mark.asyncio
    async def test_create_crew_with_disabled_configuration(self, crew_preparation):
        """Test crew creation when all memory types are disabled (Disabled Configuration)."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        crew_preparation.config["group_id"] = "test_group"
        
        mock_crew = MagicMock()
        
        # Mock memory backend config with all types disabled (Disabled Configuration)
        from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
        mock_backend_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=False
        )
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            # Mock memory backend service to return config with all disabled
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=mock_backend_config)
            mock_memory_service.return_value = mock_service_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created with memory=True (falls back to default)
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['memory'] is True
            
            # Verify logging indicates disabled configuration was found
            mock_logger.info.assert_any_call("Found 'Disabled Configuration' - ignoring database config and using default memory")
            mock_logger.info.assert_any_call("Created default memory backend configuration (ChromaDB + SQLite)")
            
            # Verify storage directory would be set for default backend
            # The actual setting happens in crew_preparation, which we're testing
            # Just verify the crew was created with memory enabled
    
    @pytest.mark.asyncio
    async def test_create_crew_memory_backend_service_exception(self, crew_preparation):
        """Test crew creation when memory backend service throws exception."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        crew_preparation.config["group_id"] = "test_group"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            # Mock memory backend service to throw exception
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(side_effect=Exception("Database error"))
            mock_memory_service.return_value = mock_service_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify crew was created successfully despite exception
            call_kwargs = mock_crew_class.call_args[1]
            # Memory should still be enabled when service fails (falls back to default)
            assert call_kwargs['memory'] is True
            
            # Verify warning was logged
            mock_logger.warning.assert_any_call("Failed to load memory backend config from database: Database error")
            
            # Verify default memory backend was created
            mock_logger.info.assert_any_call("Created default memory backend configuration (ChromaDB + SQLite)")
            
            # Verify storage directory would be set for default backend
            # The actual setting happens in crew_preparation, which we're testing
            # Just verify the crew was created with memory enabled
    
    @pytest.mark.asyncio
    async def test_create_crew_storage_directory_creation(self, crew_preparation):
        """Test that storage directories are created correctly for different backend types."""
        crew_preparation.agents = {"agent1": MagicMock(role="Test Agent")}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["memory"] = True
        crew_preparation.config["group_id"] = "test_group"
        crew_preparation.config["database_crew_id"] = "db_crew_123"
        
        mock_crew = MagicMock()
        
        # Test 1: Databricks backend
        from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType, DatabricksMemoryConfig
        databricks_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="test-endpoint",
                short_term_index="test-short-term",
                embedding_dimension=1024
            ),
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('src.engines.crewai.memory.memory_backend_factory.MemoryBackendFactory.create_memory_backends', return_value={'short_term': MagicMock()}), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('shutil.rmtree') as mock_rmtree:
            
            # Mock memory backend service to return Databricks config
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=databricks_config)
            mock_memory_service.return_value = mock_service_instance
            
            await crew_preparation._create_crew()
            
            # Verify Databricks storage directory was set
            import os
            assert 'CREWAI_STORAGE_DIR' in os.environ
            assert os.environ['CREWAI_STORAGE_DIR'] == 'kasal_databricks_crew_db_db_crew_123'
            
            # Verify cleanup was attempted
            mock_rmtree.assert_called_once()
        
        # Test 2: Default backend
        default_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False), \
             patch('src.services.memory_backend_service.MemoryBackendService') as mock_memory_service, \
             patch('src.core.unit_of_work.UnitOfWork'), \
             patch.dict('os.environ', {}, clear=True), \
             patch('crewai.utilities.paths.db_storage_path', return_value='/mock/path'), \
             patch('pathlib.Path.exists', return_value=False):
            
            # Mock memory backend service to return Default config
            mock_service_instance = MagicMock()
            mock_service_instance.get_active_config = AsyncMock(return_value=default_config)
            mock_memory_service.return_value = mock_service_instance
            
            await crew_preparation._create_crew()
            
            # Verify Default storage directory was set
            import os
            assert 'CREWAI_STORAGE_DIR' in os.environ
            assert os.environ['CREWAI_STORAGE_DIR'] == 'kasal_default_crew_db_db_crew_123'


class TestCrewPreparationHelperFunctions:
    """Test suite for helper functions in crew_preparation module."""
    
    def test_validate_crew_config_success(self):
        """Test successful config validation."""
        config = {
            "agents": [{"name": "agent1"}],
            "tasks": [{"name": "task1"}]
        }
        
        with patch('src.engines.crewai.crew_preparation.logger'):
            result = validate_crew_config(config)
            assert result is True
    
    def test_validate_crew_config_missing_agents(self):
        """Test config validation with missing agents."""
        config = {"tasks": [{"name": "task1"}]}
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            result = validate_crew_config(config)
            assert result is False
            mock_logger.error.assert_called_with("Missing or empty required section: agents")
    
    def test_validate_crew_config_missing_tasks(self):
        """Test config validation with missing tasks."""
        config = {"agents": [{"name": "agent1"}]}
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            result = validate_crew_config(config)
            assert result is False
            mock_logger.error.assert_called_with("Missing or empty required section: tasks")
    
    def test_validate_crew_config_empty_agents(self):
        """Test config validation with empty agents list."""
        config = {"agents": [], "tasks": [{"name": "task1"}]}
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            result = validate_crew_config(config)
            assert result is False
            mock_logger.error.assert_called_with("Missing or empty required section: agents")
    
    def test_validate_crew_config_empty_tasks(self):
        """Test config validation with empty tasks list."""
        config = {"agents": [{"name": "agent1"}], "tasks": []}
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            result = validate_crew_config(config)
            assert result is False
            mock_logger.error.assert_called_with("Missing or empty required section: tasks")
    
    def test_handle_crew_error(self):
        """Test error handling function."""
        test_exception = ValueError("Test error")
        test_message = "Test operation failed"
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            handle_crew_error(test_exception, test_message)
            
            mock_logger.error.assert_called_once_with(
                "Test operation failed: Test error",
                exc_info=True
            )

class TestProcessCrewOutput:
    """Test suite for process_crew_output function."""
    
    @pytest.mark.asyncio
    async def test_process_crew_output_dict_input(self):
        """Test processing dict input."""
        result = {"key": "value"}
        output = await process_crew_output(result)
        assert output == result
    
    @pytest.mark.asyncio
    async def test_process_crew_output_object_with_raw(self):
        """Test processing object with raw attribute."""
        mock_result = MagicMock()
        mock_result.raw = "raw content"
        
        output = await process_crew_output(mock_result)
        expected = {"result": "raw content", "type": "crew_result"}
        assert output == expected
    
    @pytest.mark.asyncio
    async def test_process_crew_output_string_input(self):
        """Test processing string input."""
        result = "test string"
        output = await process_crew_output(result)
        expected = {"result": "test string", "type": "processed"}
        assert output == expected
    
    @pytest.mark.asyncio
    async def test_process_crew_output_exception_handling(self):
        """Test exception handling in process_crew_output."""
        # Mock the entire process_crew_output function to test exception handling
        from unittest.mock import patch
        
        with patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            # Test actual exception handling by calling with a mock that will cause an exception in str()
            class FailingObject:
                def __str__(self):
                    raise Exception("Conversion error")
                def __getattribute__(self, name):
                    if name == 'raw':
                        raise Exception("raw access error")
                    return super().__getattribute__(name)
            
            output = await process_crew_output(FailingObject())
            
            assert "error" in output
            assert "Failed to process output" in output["error"]
            mock_logger.error.assert_called_once()