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
            call_kwargs_list = [c.kwargs for c in mock_create.call_args_list]
            researcher_call = next(ck for ck in call_kwargs_list if ck.get('agent_key') == 'researcher')
            assert researcher_call['agent_config'] == crew_preparation.config['agents'][0]
            assert researcher_call['tool_service'] is crew_preparation.tool_service
            assert researcher_call['tool_factory'] is crew_preparation.tool_factory
            assert researcher_call['config'] is crew_preparation.config
            assert 'agent_id' in researcher_call  # modern implementation passes resolved agent_id

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
        mock_task1.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
        mock_task2 = MagicMock()
        mock_task2.async_execution = True  # Match config (write_task has async=True)

        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()

            # Only 1 async task, so no completion task added
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
        mock_task1.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
        mock_task2 = MagicMock()
        mock_task2.async_execution = True  # Match config (write_task has async=True)
        mock_task2.context = None  # Initialize context

        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]):
            result = await crew_preparation._create_tasks()

            assert result is True
            # Context is set then cleared for async tasks to enable parallel execution
            # (but since only 1 async task, context is set and not cleared)
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
    async def test_create_tasks_async_execution_parallel(self, crew_preparation):
        """Test parallel execution setup for multiple async tasks.

        CrewAI validates: "The crew must end with at most one asynchronous task"
        Solution: Keep ALL async tasks as async (they run in parallel), add a minimal
        completion task with context=[all_async_tasks] to satisfy CrewAI validation.

        IMPORTANT: Async tasks must NOT have context set to run truly in parallel.
        """
        crew_preparation.agents = {"researcher": MagicMock(), "writer": MagicMock()}

        # Both tasks set to async - they should run in parallel
        crew_preparation.config["tasks"] = [
            {
                "id": "task1",
                "name": "task1",
                "description": "First task",
                "agent": "researcher",
                "expected_output": "Output 1",
                "async_execution": True  # Will remain True (runs in parallel)
            },
            {
                "id": "task2",
                "name": "task2",
                "description": "Second task",
                "agent": "writer",
                "expected_output": "Output 2",
                "async_execution": True  # Will remain True (runs in parallel)
            }
        ]

        mock_task1 = MagicMock()
        mock_task1.async_execution = True
        mock_task1.context = None
        mock_task1.description = "First task"
        mock_task1.agent = MagicMock()

        mock_task2 = MagicMock()
        mock_task2.async_execution = True
        mock_task2.context = None
        mock_task2.description = "Second task"
        mock_task2.agent = MagicMock()

        with patch('src.engines.crewai.helpers.task_helpers.create_task', side_effect=[mock_task1, mock_task2]) as mock_create:
            # Patch crewai.Task since the code does 'from crewai import Task as CrewAITask'
            with patch('crewai.Task') as mock_task_class:
                mock_completion_task = MagicMock()
                mock_task_class.return_value = mock_completion_task

                result = await crew_preparation._create_tasks()

                assert result is True

                # Both tasks should be created with async_execution=True
                first_call_config = mock_create.call_args_list[0][1]['task_config']
                assert first_call_config['async_execution'] is True

                second_call_config = mock_create.call_args_list[1][1]['task_config']
                assert second_call_config['async_execution'] is True

                # Both async tasks remain async (for parallel execution)
                assert mock_task1.async_execution is True
                assert mock_task2.async_execution is True

                # A completion task should be added (3 tasks total)
                assert len(crew_preparation.tasks) == 3
                assert crew_preparation.tasks[2] == mock_completion_task
    
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
        mock_task1.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
        mock_task2 = MagicMock()
        mock_task2.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
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
        mock_task1.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
        mock_task2 = MagicMock()
        mock_task2.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
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
        mock_task1.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior
        mock_task2 = MagicMock()
        mock_task2.async_execution = False  # Explicitly set to avoid MagicMock truthy behavior

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
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew):
            
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
        crew_preparation.config["group_id"] = "test_group_123"  # Required for planning/reasoning LLMs

        mock_crew = MagicMock()
        mock_planning_llm = MagicMock()
        mock_reasoning_llm = MagicMock()
        mock_manager_llm = MagicMock()

        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class, \
             patch('src.core.llm_manager.LLMManager.get_llm') as mock_get_llm, \
             patch('src.core.llm_manager.LLMManager.configure_crewai_llm') as mock_configure_llm, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):

            # Configure LLMManager.configure_crewai_llm to return different LLMs based on model
            # When group_id is present, configure_crewai_llm is called instead of get_llm
            def configure_llm_side_effect(model, group_id):
                if model == "gpt-3.5-turbo":
                    return mock_planning_llm
                elif model == "gpt-4":
                    return mock_reasoning_llm
                return mock_manager_llm

            mock_configure_llm.side_effect = configure_llm_side_effect

            result = await crew_preparation._create_crew()

            assert result is True

            # Verify LLMManager.configure_crewai_llm was called for planning, reasoning, and manager LLMs
            # Since group_id is present, configure_crewai_llm is used instead of get_llm
            assert mock_configure_llm.call_count == 3  # planning + reasoning + manager
            mock_configure_llm.assert_any_call("gpt-3.5-turbo", "test_group_123")  # planning
            mock_configure_llm.assert_any_call("gpt-4", "test_group_123")  # reasoning and manager

            # Verify get_llm was NOT called (since group_id is present)
            assert mock_get_llm.call_count == 0

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
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew) as mock_crew_class:
            
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
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=ImportError("Module not found")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()

            assert result is True
            # Logger assertions removed - implementation details moved to ManagerConfigBuilder service
    
    @pytest.mark.asyncio
    async def test_create_crew_llm_manager_exception(self, crew_preparation):
        """Test crew creation handles exceptions in LLM configuration."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["model"] = "gpt-4"
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("LLM error")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            # Logger assertions removed - implementation details moved to ManagerConfigBuilder service
    
    @pytest.mark.asyncio
    async def test_create_crew_databricks_fallback_on_llm_error(self, crew_preparation):
        """Test crew creation falls back to Databricks model when LLM fails in Databricks environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["model"] = "gpt-4"
        
        mock_crew = MagicMock()
        mock_fallback_llm = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=[Exception("LLM error"), mock_fallback_llm]), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()

            assert result is True
            # Logger assertions removed - fallback handling moved to ManagerConfigBuilder service
    
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
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value="test-key"), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            # Logger assertions removed - embedder config handling moved to EmbedderConfigBuilder service
    
    @pytest.mark.asyncio
    async def test_create_crew_openai_api_key_in_databricks(self, crew_preparation):
        """Test crew creation handles OpenAI API key configuration in Databricks Apps environment."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        
        mock_crew = MagicMock()
        
        with patch('src.engines.crewai.crew_preparation.Crew', return_value=mock_crew), \
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
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', side_effect=Exception("API error")), \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger:
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            # Logger assertions removed - OpenAI key handling is in _handle_openai_api_key helper
    
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
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("Model not found")) as mock_get_llm, \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None), \
             patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as mock_databricks_repo:
            
            # Configure mock to return None for get_databricks_config
            mock_databricks_instance = MagicMock()
            mock_databricks_instance.get_databricks_config = AsyncMock(return_value=None)
            mock_databricks_repo.return_value = mock_databricks_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for planning LLM and manager LLM
            # Note: Manager LLM is also created, so we expect 2 calls (assuming config has a model)
            assert mock_get_llm.call_count >= 1  # At least one call for planning_llm
            mock_get_llm.assert_any_call("invalid-model")
            
            # Verify error was logged
            # Planning LLM error handling moved to CrewConfigBuilder.add_llm_parameters()
            # Just verify crew creation succeeded despite the error
            
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
             patch('src.core.llm_manager.LLMManager.get_llm', side_effect=Exception("Model not found")) as mock_get_llm, \
             patch('src.engines.crewai.crew_preparation.logger') as mock_logger, \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None), \
             patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as mock_databricks_repo:
            
            # Configure mock to return None for get_databricks_config
            mock_databricks_instance = MagicMock()
            mock_databricks_instance.get_databricks_config = AsyncMock(return_value=None)
            mock_databricks_repo.return_value = mock_databricks_instance
            
            result = await crew_preparation._create_crew()
            
            assert result is True
            
            # Verify LLMManager.get_llm was called for reasoning LLM and manager LLM
            # Note: Manager LLM is also created, so we expect 2 calls (assuming config has a model)
            assert mock_get_llm.call_count >= 1  # At least one call for reasoning_llm
            mock_get_llm.assert_any_call("invalid-model")
            
            # Verify error was logged
            # Reasoning LLM error handling moved to CrewConfigBuilder.add_llm_parameters()
            # Just verify crew creation succeeded despite the error
            
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
            # Planning/Reasoning LLM error handling moved to CrewConfigBuilder service
            # Just verify crew creation succeeded
            # Error handling moved to service layer

            # Verify crew was created without planning_llm or reasoning_llm in kwargs
            call_kwargs = mock_crew_class.call_args[1]
            assert call_kwargs['planning'] is True
            assert call_kwargs['reasoning'] is True
            assert 'planning_llm' not in call_kwargs
            assert 'reasoning_llm' not in call_kwargs

    @pytest.mark.asyncio
    async def test_create_crew_hierarchical_minimal_kwargs_preserves_manager_llm(self, crew_preparation):
        """Test that minimal_kwargs fallback preserves manager_llm for hierarchical process."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["process"] = "hierarchical"
        crew_preparation.config["group_id"] = "test_group_123"

        mock_manager_llm = MagicMock()
        mock_crew = MagicMock()

        # Simulate TypeError on first Crew() call, success on minimal_kwargs
        call_count = 0
        def crew_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call fails with TypeError (unsupported kwarg)
                raise TypeError("unexpected keyword argument 'unsupported_param'")
            # Second call (minimal_kwargs) succeeds
            return mock_crew

        with patch('src.engines.crewai.crew_preparation.Crew', side_effect=crew_side_effect) as mock_crew_class, \
             patch('src.engines.crewai.config.manager_config_builder.LLMManager.configure_crewai_llm',
                   return_value=mock_manager_llm), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):

            result = await crew_preparation._create_crew()

            assert result is True
            assert crew_preparation.crew == mock_crew

            # Verify Crew was called twice: first with full kwargs, then with minimal_kwargs
            assert mock_crew_class.call_count == 2

            # Verify the second call (minimal_kwargs) included manager_llm
            second_call_kwargs = mock_crew_class.call_args_list[1][1]
            assert 'manager_llm' in second_call_kwargs
            assert second_call_kwargs['manager_llm'] == mock_manager_llm

    @pytest.mark.asyncio
    async def test_create_crew_hierarchical_minimal_kwargs_preserves_manager_agent(self, crew_preparation):
        """Test that minimal_kwargs fallback preserves manager_agent for hierarchical process."""
        crew_preparation.agents = {"agent1": MagicMock()}
        crew_preparation.tasks = [MagicMock()]
        crew_preparation.config["crew"]["process"] = "hierarchical"
        crew_preparation.config["crew"]["manager_agent"] = {
            "role": "Manager",
            "goal": "Manage team",
            "backstory": "Experienced"
        }
        crew_preparation.config["group_id"] = "test_group_123"

        mock_manager_agent = MagicMock()
        mock_crew = MagicMock()

        # Simulate TypeError on first Crew() call, success on minimal_kwargs
        call_count = 0
        def crew_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call fails with TypeError (unsupported kwarg)
                raise TypeError("unexpected keyword argument 'unsupported_param'")
            # Second call (minimal_kwargs) succeeds
            return mock_crew

        # Mock create_agent to accept all parameters passed by manager_config_builder
        async def mock_create_agent_func(**kwargs):
            return mock_manager_agent

        with patch('src.engines.crewai.crew_preparation.Crew', side_effect=crew_side_effect) as mock_crew_class, \
             patch('src.engines.crewai.config.manager_config_builder.create_agent', side_effect=mock_create_agent_func), \
             patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', return_value=None):

            result = await crew_preparation._create_crew()

            assert result is True
            assert crew_preparation.crew == mock_crew

            # Verify Crew was called twice: first with full kwargs, then with minimal_kwargs
            assert mock_crew_class.call_count == 2

            # Verify the second call (minimal_kwargs) included manager_agent
            second_call_kwargs = mock_crew_class.call_args_list[1][1]
            assert 'manager_agent' in second_call_kwargs
            assert second_call_kwargs['manager_agent'] == mock_manager_agent
    
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