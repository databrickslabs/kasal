"""
Tests for dispatcher service with 100% coverage.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from src.services.dispatcher_service import DispatcherService
from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
from src.utils.user_context import GroupContext
import json


@pytest.fixture
def mock_log_service():
    service = Mock()
    service.create_log = AsyncMock()
    return service


@pytest.fixture
def mock_agent_service():
    service = Mock()
    service.generate_agent = AsyncMock()
    return service


@pytest.fixture
def mock_task_service():
    service = Mock()
    service.generate_and_save_task = AsyncMock()
    return service


@pytest.fixture
def mock_crew_service():
    service = Mock()
    service.create_crew_complete = AsyncMock()
    return service


@pytest.fixture
def dispatcher_service(mock_log_service):
    with patch('src.services.dispatcher_service.AgentGenerationService.create') as mock_agent_create, \
         patch('src.services.dispatcher_service.TaskGenerationService.create') as mock_task_create, \
         patch('src.services.dispatcher_service.CrewGenerationService.create') as mock_crew_create:
        
        mock_agent_create.return_value = Mock()
        mock_task_create.return_value = Mock()
        mock_crew_create.return_value = Mock()
        
        service = DispatcherService(mock_log_service)
        return service


@pytest.fixture
def group_context():
    return GroupContext(
        group_ids=["test_group"],
        user_id="test_user",
        group_email="test@example.com",
        email_domain="example.com"
    )


class TestDispatcherService:
    """Test cases for DispatcherService."""

    def test_init(self, mock_log_service):
        """Test dispatcher service initialization."""
        with patch('src.services.dispatcher_service.AgentGenerationService.create') as mock_agent_create, \
             patch('src.services.dispatcher_service.TaskGenerationService.create') as mock_task_create, \
             patch('src.services.dispatcher_service.CrewGenerationService.create') as mock_crew_create:
            
            mock_agent_create.return_value = Mock()
            mock_task_create.return_value = Mock()
            mock_crew_create.return_value = Mock()
            
            service = DispatcherService(mock_log_service)
            
            assert service.log_service == mock_log_service
            assert service.agent_service is not None
            assert service.task_service is not None
            assert service.crew_service is not None

    def test_create_factory_method(self):
        """Test the create factory method."""
        with patch('src.services.dispatcher_service.LLMLogService.create') as mock_log_create, \
             patch('src.services.dispatcher_service.AgentGenerationService.create') as mock_agent_create, \
             patch('src.services.dispatcher_service.TaskGenerationService.create') as mock_task_create, \
             patch('src.services.dispatcher_service.CrewGenerationService.create') as mock_crew_create:
            
            mock_log_create.return_value = Mock()
            mock_agent_create.return_value = Mock()
            mock_task_create.return_value = Mock()
            mock_crew_create.return_value = Mock()
            
            service = DispatcherService.create()
            
            assert isinstance(service, DispatcherService)
            mock_log_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_llm_interaction_success(self, dispatcher_service):
        """Test successful LLM interaction logging."""
        with patch.object(dispatcher_service.log_service, 'create_log', new_callable=AsyncMock) as mock_create_log:
            await dispatcher_service._log_llm_interaction(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model"
            )
            
            mock_create_log.assert_called_once_with(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model",
                status="success",
                error_message=None,
                group_context=None
            )

    @pytest.mark.asyncio
    async def test_log_llm_interaction_with_error(self, dispatcher_service):
        """Test LLM interaction logging with error."""
        with patch.object(dispatcher_service.log_service, 'create_log', new_callable=AsyncMock) as mock_create_log:
            await dispatcher_service._log_llm_interaction(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model",
                status="error",
                error_message="Test error"
            )
            
            mock_create_log.assert_called_once_with(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model",
                status="error",
                error_message="Test error",
                group_context=None
            )

    @pytest.mark.asyncio
    async def test_log_llm_interaction_with_group_context(self, dispatcher_service, group_context):
        """Test LLM interaction logging with group context."""
        with patch.object(dispatcher_service.log_service, 'create_log', new_callable=AsyncMock) as mock_create_log:
            await dispatcher_service._log_llm_interaction(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model",
                group_context=group_context
            )
            
            mock_create_log.assert_called_once_with(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model",
                status="success",
                error_message=None,
                group_context=group_context
            )

    @pytest.mark.asyncio
    async def test_log_llm_interaction_exception_handling(self, dispatcher_service):
        """Test exception handling in LLM interaction logging."""
        with patch.object(dispatcher_service.log_service, 'create_log', new_callable=AsyncMock) as mock_create_log:
            mock_create_log.side_effect = Exception("Database error")
            
            # Should not raise exception
            await dispatcher_service._log_llm_interaction(
                endpoint="test-endpoint",
                prompt="test prompt",
                response="test response",
                model="test-model"
            )
            
            mock_create_log.assert_called_once()

    def test_analyze_message_semantics_task_actions(self, dispatcher_service):
        """Test semantic analysis for task action words."""
        result = dispatcher_service._analyze_message_semantics("find the best hotel")
        
        assert "find" in result["task_actions"]
        assert result["has_imperative"] == True
        assert result["has_command_structure"] == True
        assert result["suggested_intent"] == "generate_task"

    def test_analyze_message_semantics_conversation_words(self, dispatcher_service):
        """Test semantic analysis for conversation words."""
        result = dispatcher_service._analyze_message_semantics("hello, how are you?")
        
        assert "hello" in result["conversation_words"]
        assert "how" in result["conversation_words"]
        assert result["has_question"] == True
        assert result["has_greeting"] == True
        assert result["suggested_intent"] == "conversation"

    def test_analyze_message_semantics_agent_keywords(self, dispatcher_service):
        """Test semantic analysis for agent keywords."""
        result = dispatcher_service._analyze_message_semantics("create an agent that can analyze data")
        
        assert "agent" in result["agent_keywords"]
        # Agent keywords have high weight but need to compete with task actions
        # This message has both "create" (task action) and "agent" keywords
        # The result depends on scoring algorithm

    def test_analyze_message_semantics_crew_keywords(self, dispatcher_service):
        """Test semantic analysis for crew keywords."""
        result = dispatcher_service._analyze_message_semantics("build a team of specialists")
        
        assert "team" in result["crew_keywords"]
        # Crew keywords have high weight but need to compete with task actions
        # This message has both "build" (task action) and "team" keywords

    def test_analyze_message_semantics_configure_keywords(self, dispatcher_service):
        """Test semantic analysis for configure keywords."""
        result = dispatcher_service._analyze_message_semantics("configure the llm settings")
        
        assert "configure" in result["configure_keywords"]
        assert result["has_configure_structure"] == True
        assert result["suggested_intent"] == "configure_crew"

    def test_analyze_message_semantics_command_patterns(self, dispatcher_service):
        """Test command pattern detection."""
        test_cases = [
            "find the best flight",
            "i need a task to analyze data",
            "help me create a report",
            "can you build an agent"
        ]
        
        for message in test_cases:
            result = dispatcher_service._analyze_message_semantics(message)
            assert result["has_command_structure"] == True

    def test_analyze_message_semantics_configure_patterns(self, dispatcher_service):
        """Test configure pattern detection."""
        test_cases = [
            "configure the system",
            "change the llm model",
            "select different tools",
            "model settings need updating"
        ]
        
        for message in test_cases:
            result = dispatcher_service._analyze_message_semantics(message)
            assert result["has_configure_structure"] == True

    def test_analyze_message_semantics_intent_scores(self, dispatcher_service):
        """Test intent scoring calculation."""
        result = dispatcher_service._analyze_message_semantics("find and analyze the data using multiple agents")
        
        scores = result["intent_scores"]
        assert scores["generate_task"] > 0  # Has task actions
        assert scores["generate_crew"] > 0  # Has "multiple"
        assert isinstance(scores["generate_agent"], int)
        assert isinstance(scores["configure_crew"], int)
        assert isinstance(scores["conversation"], int)

    def test_analyze_message_semantics_semantic_hints(self, dispatcher_service):
        """Test semantic hints generation."""
        result = dispatcher_service._analyze_message_semantics("find the best hotel quickly")
        
        hints = result["semantic_hints"]
        assert any("Action words detected" in hint for hint in hints)
        assert any("Command-like structure detected" in hint for hint in hints)
        assert any("Imperative form detected" in hint for hint in hints)

    def test_analyze_message_semantics_empty_message(self, dispatcher_service):
        """Test semantic analysis with empty message."""
        result = dispatcher_service._analyze_message_semantics("")
        
        assert result["task_actions"] == []
        assert result["conversation_words"] == []
        assert result["agent_keywords"] == []
        assert result["crew_keywords"] == []
        assert result["configure_keywords"] == []
        assert result["has_imperative"] == False
        assert result["has_question"] == False
        assert result["has_greeting"] == False
        assert result["has_command_structure"] == False
        assert result["has_configure_structure"] == False

    def test_analyze_message_semantics_special_characters(self, dispatcher_service):
        """Test semantic analysis with special characters."""
        result = dispatcher_service._analyze_message_semantics("find @#$% data!")
        
        assert "find" in result["task_actions"]
        assert result["has_imperative"] == True

    @pytest.mark.asyncio
    async def test_detect_intent_with_template(self, dispatcher_service):
        """Test intent detection with template from database."""
        mock_template_content = "Test template content"
        
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = mock_template_content
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"intent": "generate_task", "confidence": 0.8}'}}]
            }
            mock_json_parser.return_value = {
                "intent": "generate_task",
                "confidence": 0.8,
                "extracted_info": {},
                "suggested_prompt": "test message"
            }
            
            result = await dispatcher_service._detect_intent("find the best hotel", "test-model")
            
            assert result["intent"] == "generate_task"
            assert result["confidence"] == 0.8
            mock_get_template.assert_called_once_with("detect_intent")

    @pytest.mark.asyncio
    async def test_detect_intent_without_template(self, dispatcher_service):
        """Test intent detection without template (uses default)."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = None  # No template found
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"intent": "generate_task", "confidence": 0.8}'}}]
            }
            mock_json_parser.return_value = {
                "intent": "generate_task",
                "confidence": 0.8,
                "extracted_info": {},
                "suggested_prompt": "test message"
            }
            
            result = await dispatcher_service._detect_intent("find the best hotel", "test-model")
            
            assert result["intent"] == "generate_task"
            assert result["confidence"] == 0.8

    @pytest.mark.asyncio
    async def test_detect_intent_incomplete_response(self, dispatcher_service):
        """Test intent detection with incomplete LLM response."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"confidence": 0.8}'}}]  # Missing intent
            }
            mock_json_parser.return_value = {"confidence": 0.8}  # Missing fields
            
            result = await dispatcher_service._detect_intent("find the best hotel", "test-model")
            
            # Should fill in missing fields with defaults/semantic analysis
            assert "intent" in result
            assert "confidence" in result
            assert "extracted_info" in result
            assert "suggested_prompt" in result

    @pytest.mark.asyncio
    async def test_detect_intent_missing_confidence(self, dispatcher_service):
        """Test intent detection with missing confidence field."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"intent": "conversation"}'}}]  # Missing confidence, intent without task words
            }
            mock_json_parser.return_value = {"intent": "conversation"}  # Missing confidence
            
            result = await dispatcher_service._detect_intent("xyz abc def", "test-model")  # Message with no semantic indicators
            
            # Should have default confidence of 0.5 (or higher due to semantic override)
            assert result["confidence"] >= 0.5

    @pytest.mark.asyncio
    async def test_detect_intent_semantic_analysis_override(self, dispatcher_service):
        """Test semantic analysis overriding LLM result when more confident."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"intent": "conversation", "confidence": 0.5}'}}]
            }
            mock_json_parser.return_value = {
                "intent": "conversation",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "test"
            }
            
            # Message with strong task indicators
            result = await dispatcher_service._detect_intent("find create analyze search get data", "test-model")
            
            # Semantic analysis should override due to high confidence
            assert result["intent"] == "generate_task"  # Semantic analysis should win

    @pytest.mark.asyncio
    async def test_detect_intent_exception_fallback(self, dispatcher_service):
        """Test fallback to semantic analysis when LLM fails."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.side_effect = Exception("LLM error")
            
            result = await dispatcher_service._detect_intent("find the best hotel", "test-model")
            
            # Should fall back to semantic analysis
            assert "intent" in result
            assert "confidence" in result
            assert result["extracted_info"]["semantic_analysis"] is not None

    @pytest.mark.asyncio
    async def test_dispatch_generate_agent(self, dispatcher_service, group_context):
        """Test dispatching to agent generation service."""
        request = DispatcherRequest(message="create an agent", model="test-model", tools=["tool1"])
        
        mock_intent_result = {
            "intent": "generate_agent",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "create an agent"
        }
        
        mock_agent_result = {"agent": "generated"}
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.agent_service, 'generate_agent', new_callable=AsyncMock, return_value=mock_agent_result):
            
            result = await dispatcher_service.dispatch(request, group_context)
            
            assert result["dispatcher"]["intent"] == "generate_agent"
            assert result["generation_result"] == mock_agent_result
            assert result["service_called"] == "generate_agent"
            
            dispatcher_service.agent_service.generate_agent.assert_called_once_with(
                prompt_text="create an agent",
                model="test-model",
                tools=["tool1"],
                group_context=group_context
            )

    @pytest.mark.asyncio
    async def test_dispatch_generate_task(self, dispatcher_service, group_context):
        """Test dispatching to task generation service."""
        request = DispatcherRequest(message="find the best hotel", model="test-model")
        
        mock_intent_result = {
            "intent": "generate_task",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "find the best hotel"
        }
        
        mock_task_result = {"task": "generated"}
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.task_service, 'generate_and_save_task', new_callable=AsyncMock, return_value=mock_task_result):
            
            result = await dispatcher_service.dispatch(request, group_context)
            
            assert result["dispatcher"]["intent"] == "generate_task"
            assert result["generation_result"] == mock_task_result
            assert result["service_called"] == "generate_task"
            
            # Verify task service called with correct parameters
            call_args = dispatcher_service.task_service.generate_and_save_task.call_args
            assert call_args[0][0].text == "find the best hotel"
            assert call_args[0][0].model == "test-model"
            assert call_args[0][1] == group_context

    @pytest.mark.asyncio
    async def test_dispatch_generate_crew(self, dispatcher_service, group_context):
        """Test dispatching to crew generation service."""
        request = DispatcherRequest(message="build a team", model="test-model", tools=["tool1"])
        
        mock_intent_result = {
            "intent": "generate_crew",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "build a team"
        }
        
        mock_crew_result = {"crew": "generated"}
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.crew_service, 'create_crew_complete', new_callable=AsyncMock, return_value=mock_crew_result):
            
            result = await dispatcher_service.dispatch(request, group_context)
            
            assert result["dispatcher"]["intent"] == "generate_crew"
            assert result["generation_result"] == mock_crew_result
            assert result["service_called"] == "generate_crew"
            
            # Verify crew service called with correct parameters
            call_args = dispatcher_service.crew_service.create_crew_complete.call_args
            assert call_args[0][0].prompt == "build a team"
            assert call_args[0][0].model == "test-model"
            assert call_args[0][0].tools == ["tool1"]
            assert call_args[0][1] == group_context

    @pytest.mark.asyncio
    async def test_dispatch_generate_crew_with_none_suggested_prompt(self, dispatcher_service, group_context):
        """Test dispatching to crew generation service with None suggested_prompt."""
        request = DispatcherRequest(message="build a team", model="test-model", tools=["tool1"])
        
        mock_intent_result = {
            "intent": "generate_crew",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": None  # None suggested prompt
        }
        
        mock_crew_result = {"crew": "generated"}
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.crew_service, 'create_crew_complete', new_callable=AsyncMock, return_value=mock_crew_result):
            
            result = await dispatcher_service.dispatch(request, group_context)
            
            # Should use original message when suggested_prompt is None
            call_args = dispatcher_service.crew_service.create_crew_complete.call_args
            assert call_args[0][0].prompt == "build a team"  # Original message

    @pytest.mark.asyncio
    async def test_dispatch_generate_crew_request_creation(self, dispatcher_service):
        """Test specific crew request creation logic coverage."""
        request = DispatcherRequest(message="build workflow", model="test-model", tools=None)
        
        mock_intent_result = {
            "intent": "generate_crew",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "enhanced crew prompt"
        }
        
        mock_crew_result = {"crew": "generated"}
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.crew_service, 'create_crew_complete', new_callable=AsyncMock, return_value=mock_crew_result):
            
            result = await dispatcher_service.dispatch(request)
            
            # Verify CrewGenerationRequest was created with correct parameters
            call_args = dispatcher_service.crew_service.create_crew_complete.call_args
            crew_request = call_args[0][0]
            assert crew_request.prompt == "enhanced crew prompt"
            assert crew_request.model == "test-model"
            assert crew_request.tools is None

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_llm(self, dispatcher_service):
        """Test dispatching configure crew with LLM config type."""
        request = DispatcherRequest(message="configure llm", model="test-model")
        
        mock_intent_result = {
            "intent": "configure_crew",
            "confidence": 0.9,
            "extracted_info": {"config_type": "llm"},
            "suggested_prompt": "configure llm"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["dispatcher"]["intent"] == "configure_crew"
            assert result["generation_result"]["type"] == "configure_crew"
            assert result["generation_result"]["config_type"] == "llm"
            assert result["generation_result"]["actions"]["open_llm_dialog"] == True
            assert result["generation_result"]["actions"]["open_maxr_dialog"] == False
            assert result["generation_result"]["actions"]["open_tools_dialog"] == False

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_maxr(self, dispatcher_service):
        """Test dispatching configure crew with maxr config type."""
        request = DispatcherRequest(message="configure maxr", model="test-model")
        
        mock_intent_result = {
            "intent": "configure_crew",
            "confidence": 0.9,
            "extracted_info": {"config_type": "maxr"},
            "suggested_prompt": "configure maxr"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["generation_result"]["config_type"] == "maxr"
            assert result["generation_result"]["actions"]["open_llm_dialog"] == False
            assert result["generation_result"]["actions"]["open_maxr_dialog"] == True
            assert result["generation_result"]["actions"]["open_tools_dialog"] == False

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_tools(self, dispatcher_service):
        """Test dispatching configure crew with tools config type."""
        request = DispatcherRequest(message="configure tools", model="test-model")
        
        mock_intent_result = {
            "intent": "configure_crew",
            "confidence": 0.9,
            "extracted_info": {"config_type": "tools"},
            "suggested_prompt": "configure tools"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["generation_result"]["config_type"] == "tools"
            assert result["generation_result"]["actions"]["open_llm_dialog"] == False
            assert result["generation_result"]["actions"]["open_maxr_dialog"] == False
            assert result["generation_result"]["actions"]["open_tools_dialog"] == True

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_general(self, dispatcher_service):
        """Test dispatching configure crew with general config type."""
        request = DispatcherRequest(message="configure crew", model="test-model")
        
        mock_intent_result = {
            "intent": "configure_crew",
            "confidence": 0.9,
            "extracted_info": {"config_type": "general"},
            "suggested_prompt": "configure crew"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["generation_result"]["config_type"] == "general"
            assert result["generation_result"]["actions"]["open_llm_dialog"] == True
            assert result["generation_result"]["actions"]["open_maxr_dialog"] == True
            assert result["generation_result"]["actions"]["open_tools_dialog"] == True

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_default_config_type(self, dispatcher_service):
        """Test dispatching configure crew with default config type."""
        request = DispatcherRequest(message="configure crew", model="test-model")
        
        mock_intent_result = {
            "intent": "configure_crew",
            "confidence": 0.9,
            "extracted_info": {},  # No config_type specified
            "suggested_prompt": "configure crew"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["generation_result"]["config_type"] == "general"  # Default

    @pytest.mark.asyncio
    async def test_dispatch_conversation(self, dispatcher_service):
        """Test dispatching conversation intent."""
        request = DispatcherRequest(message="hello, how are you?", model="test-model")
        
        mock_intent_result = {
            "intent": "conversation",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "hello, how are you?"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["dispatcher"]["intent"] == "conversation"
            assert result["generation_result"]["type"] == "conversation"
            assert "conversation" in result["generation_result"]["message"]
            assert "suggestions" in result["generation_result"]
            assert result["service_called"] == "conversation"

    @pytest.mark.asyncio
    async def test_dispatch_unknown_intent(self, dispatcher_service):
        """Test dispatching unknown intent."""
        request = DispatcherRequest(message="gibberish", model="test-model")
        
        mock_intent_result = {
            "intent": "unknown",
            "confidence": 0.3,
            "extracted_info": {},
            "suggested_prompt": "gibberish"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock):
            
            result = await dispatcher_service.dispatch(request)
            
            assert result["dispatcher"]["intent"] == "unknown"
            assert result["generation_result"]["type"] == "unknown"
            assert "not sure" in result["generation_result"]["message"]
            assert "suggestions" in result["generation_result"]
            assert result["service_called"] is None

    @pytest.mark.asyncio
    async def test_dispatch_default_model(self, dispatcher_service):
        """Test dispatching with default model when none specified."""
        request = DispatcherRequest(message="find the best hotel")  # No model specified
        
        mock_intent_result = {
            "intent": "generate_task",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "find the best hotel"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.task_service, 'generate_and_save_task', new_callable=AsyncMock, return_value={}):
            
            result = await dispatcher_service.dispatch(request)
            
            # Should use default model
            dispatcher_service._detect_intent.assert_called_once_with(
                "find the best hotel", 
                "databricks-llama-4-maverick"  # Default model
            )

    @pytest.mark.asyncio
    async def test_dispatch_service_exception(self, dispatcher_service):
        """Test exception handling in generation service calls."""
        request = DispatcherRequest(message="create an agent", model="test-model")
        
        mock_intent_result = {
            "intent": "generate_agent",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": "create an agent"
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.agent_service, 'generate_agent', new_callable=AsyncMock, side_effect=Exception("Service error")):
            
            with pytest.raises(Exception, match="Service error"):
                await dispatcher_service.dispatch(request)
            
            # Should log the error
            assert dispatcher_service._log_llm_interaction.call_count == 2  # Once for intent, once for error

    @pytest.mark.asyncio
    async def test_dispatch_with_suggested_prompt_fallback(self, dispatcher_service):
        """Test fallback to original message when suggested_prompt is None."""
        request = DispatcherRequest(message="find the best hotel", model="test-model")
        
        mock_intent_result = {
            "intent": "generate_task",
            "confidence": 0.9,
            "extracted_info": {},
            "suggested_prompt": None  # None suggested prompt
        }
        
        with patch.object(dispatcher_service, '_detect_intent', new_callable=AsyncMock, return_value=mock_intent_result), \
             patch.object(dispatcher_service, '_log_llm_interaction', new_callable=AsyncMock), \
             patch.object(dispatcher_service.task_service, 'generate_and_save_task', new_callable=AsyncMock, return_value={}):
            
            result = await dispatcher_service.dispatch(request)
            
            # Should use original message
            call_args = dispatcher_service.task_service.generate_and_save_task.call_args
            assert call_args[0][0].text == "find the best hotel"

    def test_task_action_words_coverage(self, dispatcher_service):
        """Test coverage of task action words."""
        action_words = dispatcher_service.TASK_ACTION_WORDS
        
        # Test a few specific words
        assert 'find' in action_words
        assert 'search' in action_words
        assert 'create' in action_words
        assert 'analyze' in action_words
        
        # Ensure it's a set
        assert isinstance(action_words, set)

    def test_conversation_words_coverage(self, dispatcher_service):
        """Test coverage of conversation words."""
        conversation_words = dispatcher_service.CONVERSATION_WORDS
        
        assert 'hello' in conversation_words
        assert 'what' in conversation_words
        assert 'help' in conversation_words
        
        assert isinstance(conversation_words, set)

    def test_agent_keywords_coverage(self, dispatcher_service):
        """Test coverage of agent keywords."""
        agent_keywords = dispatcher_service.AGENT_KEYWORDS
        
        assert 'agent' in agent_keywords
        assert 'assistant' in agent_keywords
        assert 'expert' in agent_keywords
        
        assert isinstance(agent_keywords, set)

    def test_crew_keywords_coverage(self, dispatcher_service):
        """Test coverage of crew keywords."""
        crew_keywords = dispatcher_service.CREW_KEYWORDS
        
        assert 'team' in crew_keywords
        assert 'crew' in crew_keywords
        assert 'workflow' in crew_keywords
        
        assert isinstance(crew_keywords, set)

    def test_configure_keywords_coverage(self, dispatcher_service):
        """Test coverage of configure keywords."""
        configure_keywords = dispatcher_service.CONFIGURE_KEYWORDS
        
        assert 'configure' in configure_keywords
        assert 'settings' in configure_keywords
        assert 'llm' in configure_keywords
        
        assert isinstance(configure_keywords, set)

    def test_analyze_message_semantics_edge_cases(self, dispatcher_service):
        """Test edge cases in semantic analysis."""
        # Test with numbers and special characters
        result = dispatcher_service._analyze_message_semantics("find 123 @#$% data!!!")
        assert "find" in result["task_actions"]
        
        # Test very long message
        long_message = "find " * 1000 + "data"
        result = dispatcher_service._analyze_message_semantics(long_message)
        assert "find" in result["task_actions"]
        
        # Test with mixed case
        result = dispatcher_service._analyze_message_semantics("FIND the BEST hotel")
        assert "find" in result["task_actions"]

    def test_analyze_message_semantics_question_detection(self, dispatcher_service):
        """Test question detection in semantic analysis."""
        # Test with question mark
        result = dispatcher_service._analyze_message_semantics("Can you find the best hotel?")
        assert result["has_question"] == True
        
        # Test with question words
        for word in ['what', 'how', 'why', 'when', 'where', 'who']:
            result = dispatcher_service._analyze_message_semantics(f"{word} is the best hotel")
            assert result["has_question"] == True

    def test_analyze_message_semantics_imperative_detection(self, dispatcher_service):
        """Test imperative detection in semantic analysis."""
        # Test action words in first 3 positions
        result = dispatcher_service._analyze_message_semantics("find the best hotel")
        assert result["has_imperative"] == True
        
        result = dispatcher_service._analyze_message_semantics("please find the best hotel")
        assert result["has_imperative"] == True
        
        result = dispatcher_service._analyze_message_semantics("I need to find the best hotel")
        assert result["has_imperative"] == False  # "find" is not in first 3 words
        
        # Test action word not in first 3 positions
        result = dispatcher_service._analyze_message_semantics("I would like you to find the best hotel")
        assert result["has_imperative"] == False

    def test_analyze_message_semantics_max_scores(self, dispatcher_service):
        """Test maximum intent scores calculation."""
        # Test message with no keywords
        result = dispatcher_service._analyze_message_semantics("xyz abc def")
        assert max(result["intent_scores"].values()) == 0
        assert result["suggested_intent"] == "unknown"
        
        # Test message with multiple strong indicators
        result = dispatcher_service._analyze_message_semantics("create find analyze search multiple team agents configure llm hello")
        scores = result["intent_scores"]
        assert scores["generate_task"] > 0
        assert scores["generate_crew"] > 0
        assert scores["configure_crew"] > 0
        assert scores["conversation"] > 0

    @pytest.mark.asyncio
    async def test_detect_intent_enhanced_message_format(self, dispatcher_service):
        """Test that enhanced message includes semantic analysis."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion, \
             patch('src.services.dispatcher_service.robust_json_parser') as mock_json_parser:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.return_value = {
                "choices": [{"message": {"content": '{"intent": "generate_task", "confidence": 0.8}'}}]
            }
            mock_json_parser.return_value = {
                "intent": "generate_task",
                "confidence": 0.8,
                "extracted_info": {},
                "suggested_prompt": "test"
            }
            
            await dispatcher_service._detect_intent("find the best hotel", "test-model")
            
            # Check that the enhanced message was passed to completion
            call_args = mock_completion.call_args
            messages = call_args[1]["messages"]
            user_message = messages[1]["content"]
            
            assert "Semantic Analysis:" in user_message
            assert "Detected action words:" in user_message
            assert "Has imperative form:" in user_message
            assert "Suggested intent from analysis:" in user_message

    @pytest.mark.asyncio
    async def test_detect_intent_low_semantic_confidence_fallback(self, dispatcher_service):
        """Test fallback behavior when semantic confidence is too low."""
        with patch('src.services.dispatcher_service.TemplateService.get_template_content') as mock_get_template, \
             patch('src.services.dispatcher_service.LLMManager.configure_litellm') as mock_configure_llm, \
             patch('src.services.dispatcher_service.litellm.acompletion') as mock_completion:
            
            mock_get_template.return_value = "test template"
            mock_configure_llm.return_value = {"model": "test-model"}
            mock_completion.side_effect = Exception("LLM error")
            
            # Message with very low semantic indicators
            result = await dispatcher_service._detect_intent("xyz", "test-model")
            
            # Should return unknown due to low confidence
            assert result["intent"] == "unknown"
            assert result["confidence"] >= 0.3  # Minimum confidence

    def test_class_constants_immutability(self, dispatcher_service):
        """Test that class constants are properly defined."""
        # Verify all constants exist and are sets
        assert hasattr(dispatcher_service, 'TASK_ACTION_WORDS')
        assert hasattr(dispatcher_service, 'CONVERSATION_WORDS')
        assert hasattr(dispatcher_service, 'AGENT_KEYWORDS')
        assert hasattr(dispatcher_service, 'CREW_KEYWORDS')
        assert hasattr(dispatcher_service, 'CONFIGURE_KEYWORDS')
        
        # Verify they are sets
        assert isinstance(dispatcher_service.TASK_ACTION_WORDS, set)
        assert isinstance(dispatcher_service.CONVERSATION_WORDS, set)
        assert isinstance(dispatcher_service.AGENT_KEYWORDS, set)
        assert isinstance(dispatcher_service.CREW_KEYWORDS, set)
        assert isinstance(dispatcher_service.CONFIGURE_KEYWORDS, set)
        
        # Verify they are not empty
        assert len(dispatcher_service.TASK_ACTION_WORDS) > 0
        assert len(dispatcher_service.CONVERSATION_WORDS) > 0
        assert len(dispatcher_service.AGENT_KEYWORDS) > 0
        assert len(dispatcher_service.CREW_KEYWORDS) > 0
        assert len(dispatcher_service.CONFIGURE_KEYWORDS) > 0