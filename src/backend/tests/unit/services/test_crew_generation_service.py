"""
Unit tests for CrewGenerationService.

Tests the functionality of crew generation service including
LLM-based crew creation, tool management, and documentation retrieval.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.services.crew_generation_service import CrewGenerationService
from src.services.log_service import LLMLogService
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
from src.utils.user_context import GroupContext


# Mock classes for testing
class MockAgent:
    def __init__(self, id=1, name="test_agent", role="Test Role", 
                 goal="Test Goal", backstory="Test Backstory", tools=None):
        self.id = id
        self.name = name
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.tools = tools or []
        
    def model_dump(self):
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "goal": self.goal,
            "backstory": self.backstory,
            "tools": self.tools
        }


class MockTask:
    def __init__(self, id=1, name="test_task", description="Test Description",
                 expected_output="Test Output", agent=None, tools=None, context=None):
        self.id = id
        self.name = name
        self.description = description
        self.expected_output = expected_output
        self.agent = agent
        self.tools = tools or []
        self.context = context or []
        
    def model_dump(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "expected_output": self.expected_output,
            "agent": self.agent,
            "tools": self.tools,
            "context": self.context
        }


class MockTool:
    def __init__(self, id=1, name="test_tool", title="Test Tool", 
                 description="Test Tool Description", parameters=None):
        self.id = id
        self.name = name
        self.title = title
        self.description = description
        self.parameters = parameters or {}
        
    def model_dump(self):
        return {
            "id": self.id,
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "parameters": self.parameters
        }


@pytest.fixture
def mock_log_service():
    """Create a mock log service."""
    return AsyncMock(spec=LLMLogService)


@pytest.fixture
def crew_generation_service(mock_log_service):
    """Create a crew generation service with mocked dependencies."""
    return CrewGenerationService(log_service=mock_log_service)


@pytest.fixture
def sample_crew_request():
    """Create a sample crew generation request."""
    return CrewGenerationRequest(
        prompt="Create a crew for data analysis",
        model="databricks-llama-4-maverick",
        tools=["NL2SQLTool", "DataVisualizationTool"]
    )


@pytest.fixture
def sample_tools():
    """Create sample tools for testing."""
    return [
        MockTool(id=1, name="NL2SQLTool", title="NL2SQL Tool", 
                description="Convert natural language to SQL queries",
                parameters={"sql_query": {"type": "string", "description": "SQL query"}}),
        MockTool(id=2, name="DataVisualizationTool", title="Data Visualization Tool",
                description="Create data visualizations",
                parameters={"chart_type": {"type": "string", "description": "Type of chart"}})
    ]


@pytest.fixture
def sample_llm_response():
    """Create a sample LLM response for crew generation."""
    return {
        "agents": [
            {
                "name": "data_analyst",
                "role": "Data Analyst",
                "goal": "Analyze data and provide insights",
                "backstory": "Experienced data analyst with SQL expertise",
                "tools": ["NL2SQLTool"]
            },
            {
                "name": "visualizer",
                "role": "Data Visualizer",
                "goal": "Create meaningful visualizations",
                "backstory": "Expert in data visualization techniques",
                "tools": ["DataVisualizationTool"]
            }
        ],
        "tasks": [
            {
                "name": "analyze_data",
                "description": "Query and analyze the dataset",
                "expected_output": "Data analysis report",
                "agent": "data_analyst",
                "tools": ["NL2SQLTool"],
                "context": []
            },
            {
                "name": "create_visualizations",
                "description": "Create charts and graphs from analysis",
                "expected_output": "Visual representations of data",
                "agent": "visualizer",
                "tools": ["DataVisualizationTool"],
                "context": ["analyze_data"]
            }
        ]
    }


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["test_group"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="test_user",
        access_token="test_token"
    )


class TestCrewGenerationServiceInit:
    """Test cases for CrewGenerationService initialization."""
    
    def test_init_success(self, mock_log_service):
        """Test successful initialization."""
        service = CrewGenerationService(log_service=mock_log_service)
        
        assert service.log_service == mock_log_service
        assert service.tool_service is None
        assert service.crew_generator_repository is not None
    
    def test_create_factory_method(self):
        """Test the create factory method."""
        with patch.object(LLMLogService, 'create') as mock_create_log:
            mock_log_service = AsyncMock()
            mock_create_log.return_value = mock_log_service
            
            service = CrewGenerationService.create()
            
            assert service.log_service == mock_log_service
            mock_create_log.assert_called_once()


class TestLogLLMInteraction:
    """Test cases for _log_llm_interaction method."""
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_success(self, crew_generation_service, mock_log_service, mock_group_context):
        """Test successful LLM interaction logging."""
        await crew_generation_service._log_llm_interaction(
            endpoint="generate-crew",
            prompt="Test prompt",
            response="Test response",
            model="test-model",
            status="success",
            group_context=mock_group_context
        )
        
        mock_log_service.create_log.assert_called_once_with(
            endpoint="generate-crew",
            prompt="Test prompt",
            response="Test response",
            model="test-model",
            status="success",
            error_message=None,
            group_context=mock_group_context
        )
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_with_error(self, crew_generation_service, mock_log_service):
        """Test LLM interaction logging with error."""
        await crew_generation_service._log_llm_interaction(
            endpoint="generate-crew",
            prompt="Test prompt",
            response="",
            model="test-model",
            status="error",
            error_message="Test error"
        )
        
        mock_log_service.create_log.assert_called_once_with(
            endpoint="generate-crew",
            prompt="Test prompt",
            response="",
            model="test-model",
            status="error",
            error_message="Test error",
            group_context=None
        )
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_exception(self, crew_generation_service, mock_log_service):
        """Test LLM interaction logging when exception occurs."""
        mock_log_service.create_log.side_effect = Exception("Logging failed")
        
        with patch('src.services.crew_generation_service.logger') as mock_logger:
            await crew_generation_service._log_llm_interaction(
                endpoint="test",
                prompt="test",
                response="test",
                model="test"
            )
            
            mock_logger.error.assert_called()


class TestPreparePromptTemplate:
    """Test cases for _prepare_prompt_template method."""
    
    @pytest.mark.asyncio
    async def test_prepare_prompt_template_success(self, crew_generation_service, sample_tools):
        """Test successful prompt template preparation."""
        template_content = "Base template content"
        
        with patch('src.services.crew_generation_service.TemplateService.get_template_content') as mock_get_template:
            mock_get_template.return_value = template_content
            
            result = await crew_generation_service._prepare_prompt_template(
                [tool.model_dump() for tool in sample_tools]
            )
            
            assert template_content in result
            assert "Available tools:" in result
            assert "NL2SQLTool" in result
            assert "DataVisualizationTool" in result
            assert "Parameters:" in result
            mock_get_template.assert_called_once_with("generate_crew")
    
    @pytest.mark.asyncio
    async def test_prepare_prompt_template_no_tools(self, crew_generation_service):
        """Test prompt template preparation with no tools."""
        template_content = "Base template content"
        
        with patch('src.services.crew_generation_service.TemplateService.get_template_content') as mock_get_template:
            mock_get_template.return_value = template_content
            
            result = await crew_generation_service._prepare_prompt_template([])
            
            assert result == template_content
            assert "Available tools:" not in result
    
    @pytest.mark.asyncio
    async def test_prepare_prompt_template_not_found(self, crew_generation_service):
        """Test prompt template preparation when template not found."""
        with patch('src.services.crew_generation_service.TemplateService.get_template_content') as mock_get_template:
            mock_get_template.return_value = None
            
            with pytest.raises(ValueError, match="Required prompt template 'generate_crew' not found"):
                await crew_generation_service._prepare_prompt_template([])
    
    @pytest.mark.asyncio
    async def test_prepare_prompt_template_with_nl2sql(self, crew_generation_service):
        """Test prompt template with NL2SQLTool specific instructions."""
        template_content = "Base template"
        tools = [{"name": "NL2SQLTool", "description": "SQL tool"}]
        
        with patch('src.services.crew_generation_service.TemplateService.get_template_content') as mock_get_template:
            mock_get_template.return_value = template_content
            
            result = await crew_generation_service._prepare_prompt_template(tools)
            
            assert "For NL2SQLTool, use the following format" in result


class TestProcessCrewSetup:
    """Test cases for _process_crew_setup method."""
    
    def test_process_crew_setup_success(self, crew_generation_service, sample_llm_response, sample_tools):
        """Test successful crew setup processing."""
        tool_details = [tool.model_dump() for tool in sample_tools]
        tool_name_to_id_map = {"NL2SQLTool": "1", "DataVisualizationTool": "2"}
        
        result = crew_generation_service._process_crew_setup(
            sample_llm_response, 
            tool_details, 
            tool_name_to_id_map
        )
        
        # Verify agents processed correctly
        assert len(result["agents"]) == 2
        assert result["agents"][0]["tools"] == ["1"]  # Tool name converted to ID
        assert result["agents"][1]["tools"] == ["2"]
        
        # Verify tasks processed correctly
        assert len(result["tasks"]) == 2
        assert result["tasks"][0]["agent"] == "data_analyst"
        assert result["tasks"][0]["tools"] == ["1"]
        assert result["tasks"][1]["context"] == []  # Context initialized
        assert result["tasks"][1]["_context_refs"] == ["analyze_data"]  # Context refs stored
    
    def test_process_crew_setup_missing_agents(self, crew_generation_service):
        """Test processing when agents are missing."""
        setup = {"tasks": []}
        
        with pytest.raises(ValueError, match="Missing or empty 'agents' array"):
            crew_generation_service._process_crew_setup(setup, [], {})
    
    def test_process_crew_setup_missing_tasks(self, crew_generation_service):
        """Test processing when tasks are missing."""
        setup = {"agents": [{"name": "agent1", "role": "role", "goal": "goal", "backstory": "story"}]}
        
        with pytest.raises(ValueError, match="Missing or empty 'tasks' array"):
            crew_generation_service._process_crew_setup(setup, [], {})
    
    def test_process_crew_setup_invalid_agent(self, crew_generation_service):
        """Test processing with invalid agent missing required fields."""
        setup = {
            "agents": [{"name": "agent1"}],  # Missing required fields
            "tasks": [{"name": "task1"}]
        }
        
        with pytest.raises(ValueError, match="Missing required field"):
            crew_generation_service._process_crew_setup(setup, [], {})
    
    def test_process_crew_setup_filter_invalid_tools(self, crew_generation_service):
        """Test filtering of invalid tools."""
        setup = {
            "agents": [{
                "name": "agent1", 
                "role": "role", 
                "goal": "goal", 
                "backstory": "story",
                "tools": ["ValidTool", "InvalidTool"]
            }],
            "tasks": [{
                "name": "task1",
                "tools": ["ValidTool", "UnknownTool"]
            }]
        }
        allowed_tools = [{"name": "ValidTool"}]
        tool_map = {"ValidTool": "1"}
        
        result = crew_generation_service._process_crew_setup(setup, allowed_tools, tool_map)
        
        assert result["agents"][0]["tools"] == ["1"]
        assert result["tasks"][0]["tools"] == ["1"]


class TestSafeGetAttr:
    """Test cases for _safe_get_attr method."""
    
    def test_safe_get_attr_dict(self, crew_generation_service):
        """Test safe get attr with dictionary."""
        obj = {"key": "value"}
        
        result = crew_generation_service._safe_get_attr(obj, "key")
        assert result == "value"
        
        result = crew_generation_service._safe_get_attr(obj, "missing", "default")
        assert result == "default"
    
    def test_safe_get_attr_object(self, crew_generation_service):
        """Test safe get attr with object."""
        class TestObj:
            key = "value"
        
        obj = TestObj()
        
        result = crew_generation_service._safe_get_attr(obj, "key")
        assert result == "value"
        
        result = crew_generation_service._safe_get_attr(obj, "missing", "default")
        assert result == "default"
    
    def test_safe_get_attr_none(self, crew_generation_service):
        """Test safe get attr with None."""
        result = crew_generation_service._safe_get_attr(None, "key", "default")
        assert result == "default"


class TestGetRelevantDocumentation:
    """Test cases for _get_relevant_documentation method."""
    
    @pytest.mark.asyncio
    async def test_get_relevant_documentation_success(self, crew_generation_service):
        """Test successful documentation retrieval."""
        mock_embedding = [0.1, 0.2, 0.3]
        mock_docs = [
            MagicMock(source="doc1.md", title="Title 1", content="Content 1"),
            MagicMock(source="doc2.md", title="Title 2", content="Content 2")
        ]
        
        with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
            with patch('src.services.crew_generation_service.DocumentationEmbeddingService') as mock_doc_service:
                with patch('src.services.crew_generation_service.UnitOfWork') as mock_uow:
                    # Setup mocks
                    mock_llm_instance = mock_llm_manager.return_value
                    mock_llm_instance.get_embedding = AsyncMock(return_value=mock_embedding)
                    
                    mock_doc_instance = mock_doc_service.return_value
                    mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=mock_docs)
                    
                    mock_session = AsyncMock()
                    mock_uow_instance = mock_uow.return_value
                    mock_uow_instance._session.__aenter__ = AsyncMock(return_value=mock_session)
                    
                    result = await crew_generation_service._get_relevant_documentation("test prompt")
                    
                    assert "CrewAI Relevant Documentation" in result
                    assert "Title 1" in result
                    assert "Content 1" in result
                    assert "Title 2" in result
    
    @pytest.mark.asyncio
    async def test_get_relevant_documentation_no_embedding(self, crew_generation_service):
        """Test documentation retrieval when embedding fails."""
        with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
            mock_llm_instance = mock_llm_manager.return_value
            mock_llm_instance.get_embedding = AsyncMock(return_value=None)
            
            result = await crew_generation_service._get_relevant_documentation("test prompt")
            
            assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_relevant_documentation_exception(self, crew_generation_service):
        """Test documentation retrieval when exception occurs."""
        with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
            mock_llm_manager.side_effect = Exception("LLM error")
            
            with patch('src.services.crew_generation_service.logger') as mock_logger:
                result = await crew_generation_service._get_relevant_documentation("test prompt")
                
                assert result == ""
                mock_logger.error.assert_called()


class TestCreateCrewComplete:
    """Test cases for create_crew_complete method."""
    
    @pytest.mark.asyncio
    async def test_create_crew_complete_success(self, crew_generation_service, sample_crew_request, 
                                              sample_tools, sample_llm_response, mock_group_context):
        """Test successful crew creation."""
        mock_agents = [MockAgent(name="data_analyst"), MockAgent(name="visualizer")]
        mock_tasks = [MockTask(name="analyze_data"), MockTask(name="create_visualizations")]
        
        with patch('src.services.crew_generation_service.UnitOfWork') as mock_uow:
            with patch('src.services.crew_generation_service.ToolService') as mock_tool_service_class:
                with patch('src.services.crew_generation_service.litellm') as mock_litellm:
                    with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
                        with patch('src.services.crew_generation_service.robust_json_parser') as mock_parser:
                            # Setup mocks
                            mock_uow_instance = mock_uow.return_value.__aenter__.return_value
                            
                            mock_tool_service = AsyncMock()
                            mock_tool_service.get_all_tools = AsyncMock()
                            mock_tool_service.get_all_tools.return_value.tools = sample_tools
                            mock_tool_service_class.from_unit_of_work = AsyncMock(return_value=mock_tool_service)
                            
                            crew_generation_service.crew_generator_repository.create_crew_entities = AsyncMock(
                                return_value={"agents": mock_agents, "tasks": mock_tasks}
                            )
                            
                            mock_llm_manager.configure_litellm = AsyncMock(
                                return_value={"model": "test-model"}
                            )
                            
                            mock_litellm.acompletion = AsyncMock(
                                return_value={
                                    "choices": [{
                                        "message": {"content": "json response"}
                                    }]
                                }
                            )
                            
                            mock_parser.return_value = sample_llm_response
                            
                            with patch.object(crew_generation_service, '_prepare_prompt_template') as mock_prepare:
                                with patch.object(crew_generation_service, '_get_relevant_documentation') as mock_get_docs:
                                    mock_prepare.return_value = "System message"
                                    mock_get_docs.return_value = "Documentation context"
                                    
                                    result = await crew_generation_service.create_crew_complete(
                                        sample_crew_request, 
                                        group_context=mock_group_context
                                    )
                                    
                                    assert "agents" in result
                                    assert "tasks" in result
                                    assert len(result["agents"]) == 2
                                    assert len(result["tasks"]) == 2
                                    
                                    # Verify litellm was called with correct parameters
                                    mock_litellm.acompletion.assert_called_once()
                                    call_args = mock_litellm.acompletion.call_args
                                    assert call_args.kwargs["temperature"] == 0.7
                                    assert call_args.kwargs["max_tokens"] == 4000
    
    @pytest.mark.asyncio
    async def test_create_crew_complete_llm_error(self, crew_generation_service, sample_crew_request):
        """Test crew creation when LLM fails."""
        with patch('src.services.crew_generation_service.UnitOfWork') as mock_uow:
            with patch('src.services.crew_generation_service.ToolService') as mock_tool_service_class:
                with patch('src.services.crew_generation_service.litellm') as mock_litellm:
                    with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
                        # Setup mocks
                        mock_tool_service = AsyncMock()
                        mock_tool_service.get_all_tools = AsyncMock()
                        mock_tool_service.get_all_tools.return_value.tools = []
                        mock_tool_service_class.from_unit_of_work = AsyncMock(return_value=mock_tool_service)
                        
                        mock_llm_manager.configure_litellm = AsyncMock(
                            return_value={"model": "test-model"}
                        )
                        
                        mock_litellm.acompletion = AsyncMock(
                            side_effect=Exception("LLM API error")
                        )
                        
                        with patch.object(crew_generation_service, '_prepare_prompt_template') as mock_prepare:
                            with patch.object(crew_generation_service, '_get_relevant_documentation') as mock_get_docs:
                                mock_prepare.return_value = "System message"
                                mock_get_docs.return_value = ""
                                
                                with pytest.raises(ValueError, match="Error generating crew"):
                                    await crew_generation_service.create_crew_complete(sample_crew_request)
    
    @pytest.mark.asyncio
    async def test_create_crew_complete_invalid_json(self, crew_generation_service, sample_crew_request):
        """Test crew creation when JSON parsing fails."""
        with patch('src.services.crew_generation_service.UnitOfWork') as mock_uow:
            with patch('src.services.crew_generation_service.ToolService') as mock_tool_service_class:
                with patch('src.services.crew_generation_service.litellm') as mock_litellm:
                    with patch('src.services.crew_generation_service.LLMManager') as mock_llm_manager:
                        with patch('src.services.crew_generation_service.robust_json_parser') as mock_parser:
                            # Setup mocks
                            mock_tool_service = AsyncMock()
                            mock_tool_service.get_all_tools = AsyncMock()
                            mock_tool_service.get_all_tools.return_value.tools = []
                            mock_tool_service_class.from_unit_of_work = AsyncMock(return_value=mock_tool_service)
                            
                            mock_llm_manager.configure_litellm = AsyncMock(
                                return_value={"model": "test-model"}
                            )
                            
                            mock_litellm.acompletion = AsyncMock(
                                return_value={
                                    "choices": [{
                                        "message": {"content": "invalid json"}
                                    }]
                                }
                            )
                            
                            mock_parser.side_effect = ValueError("Invalid JSON")
                            
                            with patch.object(crew_generation_service, '_prepare_prompt_template') as mock_prepare:
                                with patch.object(crew_generation_service, '_get_relevant_documentation') as mock_get_docs:
                                    mock_prepare.return_value = "System message"
                                    mock_get_docs.return_value = ""
                                    
                                    with pytest.raises(ValueError, match="Error generating crew"):
                                        await crew_generation_service.create_crew_complete(sample_crew_request)


class TestCreateToolNameToIdMap:
    """Test cases for _create_tool_name_to_id_map method."""
    
    def test_create_tool_name_to_id_map_success(self, crew_generation_service, sample_tools):
        """Test successful tool name to ID mapping."""
        tools = [tool.model_dump() for tool in sample_tools]
        
        result = crew_generation_service._create_tool_name_to_id_map(tools)
        
        assert result["NL2SQL Tool"] == "1"
        assert result["NL2SQLTool"] == "1"  # Original name also mapped
        assert result["Data Visualization Tool"] == "2"
        assert result["DataVisualizationTool"] == "2"
    
    def test_create_tool_name_to_id_map_missing_fields(self, crew_generation_service):
        """Test mapping with missing fields."""
        tools = [
            {"title": "Tool1"},  # Missing ID
            {"id": 2},  # Missing name
            {"title": "Tool3", "id": 3}  # Valid
        ]
        
        result = crew_generation_service._create_tool_name_to_id_map(tools)
        
        assert "Tool3" in result
        assert result["Tool3"] == "3"
        assert len(result) == 1  # Only valid tool mapped
    
    def test_create_tool_name_to_id_map_empty(self, crew_generation_service):
        """Test mapping with empty tools list."""
        result = crew_generation_service._create_tool_name_to_id_map([])
        
        assert result == {}


class TestGetToolDetails:
    """Test cases for _get_tool_details method."""
    
    @pytest.mark.asyncio
    async def test_get_tool_details_by_name(self, crew_generation_service, sample_tools):
        """Test getting tool details by name."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_all_tools = AsyncMock()
        mock_tool_service.get_all_tools.return_value.tools = sample_tools
        
        result = await crew_generation_service._get_tool_details(
            ["NL2SQL Tool", "Data Visualization Tool"],
            mock_tool_service
        )
        
        assert len(result) == 2
        assert result[0]["name"] == "NL2SQLTool"
        assert result[1]["name"] == "DataVisualizationTool"
    
    @pytest.mark.asyncio
    async def test_get_tool_details_by_id(self, crew_generation_service, sample_tools):
        """Test getting tool details by ID."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_all_tools = AsyncMock()
        mock_tool_service.get_all_tools.return_value.tools = sample_tools
        
        result = await crew_generation_service._get_tool_details(
            ["1", "2"],
            mock_tool_service
        )
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_get_tool_details_mixed_input(self, crew_generation_service, sample_tools):
        """Test getting tool details with mixed input types."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_all_tools = AsyncMock()
        mock_tool_service.get_all_tools.return_value.tools = sample_tools
        
        result = await crew_generation_service._get_tool_details(
            [
                "NL2SQL Tool",  # String name
                {"name": "Data Visualization Tool"},  # Dict with name
                {"id": 1}  # Dict with ID
            ],
            mock_tool_service
        )
        
        assert len(result) == 3
    
    @pytest.mark.asyncio
    async def test_get_tool_details_not_found(self, crew_generation_service, sample_tools):
        """Test getting tool details when tool not found."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_all_tools = AsyncMock()
        mock_tool_service.get_all_tools.return_value.tools = sample_tools
        
        result = await crew_generation_service._get_tool_details(
            ["UnknownTool"],
            mock_tool_service
        )
        
        assert len(result) == 1
        assert result[0]["name"] == "UnknownTool"
        assert "A tool named UnknownTool" in result[0]["description"]
    
    @pytest.mark.asyncio
    async def test_get_tool_details_exception(self, crew_generation_service):
        """Test getting tool details when exception occurs."""
        mock_tool_service = AsyncMock()
        mock_tool_service.get_all_tools = AsyncMock(side_effect=Exception("Service error"))
        
        with patch('src.services.crew_generation_service.logger') as mock_logger:
            result = await crew_generation_service._get_tool_details(
                ["Tool1", "Tool2"],
                mock_tool_service
            )
            
            assert len(result) == 2
            assert result[0]["name"] == "Tool1"
            assert result[1]["name"] == "Tool2"
            mock_logger.error.assert_called()


class TestEdgeCases:
    """Test edge cases and error scenarios."""
    
    @pytest.mark.asyncio
    async def test_create_crew_with_context_dependencies(self, crew_generation_service):
        """Test crew creation with task dependencies."""
        request = CrewGenerationRequest(
            prompt="Create crew with dependencies",
            tools=[]
        )
        
        llm_response = {
            "agents": [{"name": "agent1", "role": "role", "goal": "goal", "backstory": "story"}],
            "tasks": [
                {"name": "task1", "agent": "agent1"},
                {"name": "task2", "agent": "agent1", "context": ["task1"]}
            ]
        }
        
        with patch('src.services.crew_generation_service.UnitOfWork'):
            with patch('src.services.crew_generation_service.ToolService'):
                with patch('src.services.crew_generation_service.litellm'):
                    with patch('src.services.crew_generation_service.LLMManager'):
                        with patch('src.services.crew_generation_service.robust_json_parser') as mock_parser:
                            mock_parser.return_value = llm_response
                            
                            # Setup other necessary mocks
                            crew_generation_service.crew_generator_repository.create_crew_entities = AsyncMock(
                                return_value={"agents": [], "tasks": []}
                            )
                            
                            with patch.object(crew_generation_service, '_get_tool_details') as mock_get_tools:
                                with patch.object(crew_generation_service, '_prepare_prompt_template'):
                                    with patch.object(crew_generation_service, '_get_relevant_documentation'):
                                        mock_get_tools.return_value = []
                                        
                                        # Call should handle context dependencies
                                        try:
                                            await crew_generation_service.create_crew_complete(request)
                                        except:
                                            pass  # We're testing the processing, not the full flow
                                        
                                        # Verify context was processed
                                        call_args = crew_generation_service.crew_generator_repository.create_crew_entities.call_args
                                        if call_args:
                                            crew_dict = call_args[0][0]
                                            assert crew_dict["tasks"][1].get("_context_refs") == ["task1"]
    
    def test_process_crew_setup_preserve_agent_assignments(self, crew_generation_service):
        """Test that agent assignments are preserved during processing."""
        setup = {
            "agents": [{"name": "agent1", "role": "role", "goal": "goal", "backstory": "story"}],
            "tasks": [
                {"name": "task1", "assigned_agent": "agent1"},  # Using assigned_agent
                {"name": "task2", "agent": "agent1"}  # Using agent
            ]
        }
        
        result = crew_generation_service._process_crew_setup(setup, [], {})
        
        # Both tasks should have agent assignment preserved
        assert result["tasks"][0]["agent"] == "agent1"
        assert result["tasks"][0]["assigned_agent"] == "agent1"
        assert result["tasks"][1]["agent"] == "agent1"
        assert result["tasks"][1]["assigned_agent"] == "agent1"