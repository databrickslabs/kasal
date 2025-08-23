"""
Unit tests for dispatcher schemas.

Tests the functionality of Pydantic schemas for dispatcher service operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.dispatcher import (
    IntentType, DispatcherRequest, DispatcherResponse
)


class TestIntentType:
    """Test cases for IntentType enum."""
    
    def test_intent_type_values(self):
        """Test all IntentType enum values."""
        expected_values = {
            "generate_agent",
            "generate_task", 
            "generate_crew",
            "generate_plan",  # Added to match the actual enum
            "execute_crew",
            "configure_crew",
            "conversation",
            "unknown"
        }
        actual_values = {intent.value for intent in IntentType}
        assert actual_values == expected_values
    
    def test_intent_type_enum_members(self):
        """Test IntentType enum members."""
        assert IntentType.GENERATE_AGENT == "generate_agent"
        assert IntentType.GENERATE_TASK == "generate_task"
        assert IntentType.GENERATE_CREW == "generate_crew"
        assert IntentType.GENERATE_PLAN == "generate_plan"  # Added to match the actual enum
        assert IntentType.EXECUTE_CREW == "execute_crew"
        assert IntentType.CONFIGURE_CREW == "configure_crew"
        assert IntentType.CONVERSATION == "conversation"
        assert IntentType.UNKNOWN == "unknown"
    
    def test_intent_type_string_inheritance(self):
        """Test that IntentType inherits from str."""
        assert isinstance(IntentType.GENERATE_AGENT, str)
        assert isinstance(IntentType.CONVERSATION, str)
        assert isinstance(IntentType.UNKNOWN, str)
    
    def test_intent_type_iteration(self):
        """Test iterating over IntentType enum."""
        intent_list = list(IntentType)
        assert len(intent_list) == 8  # Updated count to include GENERATE_PLAN
        assert IntentType.GENERATE_AGENT in intent_list
        assert IntentType.EXECUTE_CREW in intent_list
        assert IntentType.UNKNOWN in intent_list


class TestDispatcherRequest:
    """Test cases for DispatcherRequest schema."""
    
    def test_valid_dispatcher_request_minimal(self):
        """Test DispatcherRequest with minimal required fields."""
        request_data = {
            "message": "Create an agent for data analysis"
        }
        request = DispatcherRequest(**request_data)
        assert request.message == "Create an agent for data analysis"
        assert request.model is None
        assert request.tools == []
    
    def test_valid_dispatcher_request_full(self):
        """Test DispatcherRequest with all fields."""
        request_data = {
            "message": "Generate a crew that can handle customer support",
            "model": "databricks-llama-4-maverick",
            "tools": ["EmailTool", "KnowledgeBaseTool", "TicketingTool"]
        }
        request = DispatcherRequest(**request_data)
        assert request.message == "Generate a crew that can handle customer support"
        assert request.model == "databricks-llama-4-maverick"
        assert request.tools == ["EmailTool", "KnowledgeBaseTool", "TicketingTool"]
    
    def test_dispatcher_request_missing_message(self):
        """Test DispatcherRequest validation with missing message."""
        with pytest.raises(ValidationError) as exc_info:
            DispatcherRequest(model="test-model")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields
    
    def test_dispatcher_request_empty_message(self):
        """Test DispatcherRequest with empty message."""
        request_data = {"message": ""}
        request = DispatcherRequest(**request_data)
        assert request.message == ""
        assert request.model is None
        assert request.tools == []
    
    def test_dispatcher_request_default_tools(self):
        """Test DispatcherRequest with default tools behavior."""
        request_data = {"message": "Test message"}
        request = DispatcherRequest(**request_data)
        assert isinstance(request.tools, list)
        assert len(request.tools) == 0
        
        # Test explicit empty list
        request_data = {"message": "Test message", "tools": []}
        request = DispatcherRequest(**request_data)
        assert request.tools == []
    
    def test_dispatcher_request_various_models(self):
        """Test DispatcherRequest with various model names."""
        models = [
            "databricks-llama-4-maverick",
            "openai-gpt-4",
            "claude-3-sonnet",
            "custom-model-v1.0",
            "local-llm-7b"
        ]
        
        for model in models:
            request_data = {
                "message": f"Test message for {model}",
                "model": model
            }
            request = DispatcherRequest(**request_data)
            assert request.model == model
            assert request.message == f"Test message for {model}"
    
    def test_dispatcher_request_various_tools(self):
        """Test DispatcherRequest with various tool configurations."""
        tool_configurations = [
            [],
            ["SingleTool"],
            ["Tool1", "Tool2", "Tool3"],
            ["NL2SQLTool", "FileReadTool", "WebSearchTool", "EmailTool"],
            ["CustomTool_v1", "LegacyTool-2.0", "NewTool123"]
        ]
        
        for tools in tool_configurations:
            request_data = {
                "message": f"Test with {len(tools)} tools",
                "tools": tools
            }
            request = DispatcherRequest(**request_data)
            assert request.tools == tools
            assert len(request.tools) == len(tools)
    
    def test_dispatcher_request_long_message(self):
        """Test DispatcherRequest with long message."""
        long_message = "Create a sophisticated multi-agent system that can handle complex business workflows including data processing, analysis, reporting, communication with stakeholders, and automated decision making based on predefined business rules and machine learning models." * 10
        request_data = {
            "message": long_message,
            "model": "advanced-model",
            "tools": ["AdvancedTool1", "AdvancedTool2"]
        }
        request = DispatcherRequest(**request_data)
        assert request.message == long_message
        assert len(request.message) > 1000
    
    def test_dispatcher_request_special_characters(self):
        """Test DispatcherRequest with special characters in message."""
        special_messages = [
            "Create an agent with SQL: SELECT * FROM users WHERE status = 'active'",
            "Generate a crew for handling files like: file_name_v1.2.3.json",
            "Build a task that processes data: {'key': 'value', 'items': [1, 2, 3]}",
            "Create an agent that can handle URLs: https://api.example.com/v1/data",
            "Make a crew for emails: user@domain.com and support@company.org"
        ]
        
        for message in special_messages:
            request_data = {"message": message}
            request = DispatcherRequest(**request_data)
            assert request.message == message
    
    def test_dispatcher_request_config_example(self):
        """Test DispatcherRequest with config example data."""
        example_data = {
            "message": "Create an agent that can analyze financial data",
            "model": "databricks-llama-4-maverick",
            "tools": ["NL2SQLTool", "FileReadTool"]
        }
        request = DispatcherRequest(**example_data)
        assert request.message == "Create an agent that can analyze financial data"
        assert request.model == "databricks-llama-4-maverick"
        assert request.tools == ["NL2SQLTool", "FileReadTool"]


class TestDispatcherResponse:
    """Test cases for DispatcherResponse schema."""
    
    def test_valid_dispatcher_response_minimal(self):
        """Test DispatcherResponse with minimal required fields."""
        response_data = {
            "intent": IntentType.GENERATE_AGENT,
            "confidence": 0.85
        }
        response = DispatcherResponse(**response_data)
        assert response.intent == IntentType.GENERATE_AGENT
        assert response.confidence == 0.85
        assert response.extracted_info == {}
        assert response.suggested_prompt is None
    
    def test_valid_dispatcher_response_full(self):
        """Test DispatcherResponse with all fields."""
        response_data = {
            "intent": IntentType.GENERATE_CREW,
            "confidence": 0.92,
            "extracted_info": {
                "crew_type": "customer_support",
                "agents_needed": ["support_agent", "escalation_agent"],
                "capabilities": ["email", "chat", "knowledge_base"]
            },
            "suggested_prompt": "Create a customer support crew with agents for handling emails, chat, and knowledge base queries"
        }
        response = DispatcherResponse(**response_data)
        assert response.intent == IntentType.GENERATE_CREW
        assert response.confidence == 0.92
        assert response.extracted_info["crew_type"] == "customer_support"
        assert response.extracted_info["agents_needed"] == ["support_agent", "escalation_agent"]
        assert response.suggested_prompt.startswith("Create a customer support crew")
    
    def test_dispatcher_response_missing_required_fields(self):
        """Test DispatcherResponse validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            DispatcherResponse(intent=IntentType.CONVERSATION)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "confidence" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            DispatcherResponse(confidence=0.8)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "intent" in missing_fields
    
    def test_dispatcher_response_confidence_validation(self):
        """Test DispatcherResponse confidence field validation."""
        # Valid confidence values
        valid_confidences = [0.0, 0.1, 0.5, 0.85, 0.99, 1.0]
        for confidence in valid_confidences:
            response_data = {
                "intent": IntentType.UNKNOWN,
                "confidence": confidence
            }
            response = DispatcherResponse(**response_data)
            assert response.confidence == confidence
        
        # Invalid confidence values (below 0.0)
        with pytest.raises(ValidationError) as exc_info:
            DispatcherResponse(
                intent=IntentType.CONVERSATION,
                confidence=-0.1
            )
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "greater_than_equal" for error in errors)
        
        # Invalid confidence values (above 1.0)
        with pytest.raises(ValidationError) as exc_info:
            DispatcherResponse(
                intent=IntentType.CONVERSATION,
                confidence=1.1
            )
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "less_than_equal" for error in errors)
    
    def test_dispatcher_response_all_intent_types(self):
        """Test DispatcherResponse with all intent types."""
        for intent in IntentType:
            response_data = {
                "intent": intent,
                "confidence": 0.75
            }
            response = DispatcherResponse(**response_data)
            assert response.intent == intent
            assert response.confidence == 0.75
    
    def test_dispatcher_response_intent_string_values(self):
        """Test DispatcherResponse with intent as string values."""
        for intent in IntentType:
            response_data = {
                "intent": intent.value,  # Use string value
                "confidence": 0.6
            }
            response = DispatcherResponse(**response_data)
            assert response.intent == intent
            assert response.confidence == 0.6
    
    def test_dispatcher_response_extracted_info_scenarios(self):
        """Test DispatcherResponse with various extracted_info scenarios."""
        scenarios = [
            {
                "intent": IntentType.GENERATE_AGENT,
                "info": {
                    "agent_type": "data_analyst",
                    "domain": "finance",
                    "skills": ["python", "sql", "statistics"]
                }
            },
            {
                "intent": IntentType.GENERATE_TASK,
                "info": {
                    "task_type": "data_processing",
                    "input_format": "csv",
                    "output_format": "json",
                    "complexity": "medium"
                }
            },
            {
                "intent": IntentType.CONFIGURE_CREW,
                "info": {
                    "crew_id": "crew_123",
                    "modifications": ["add_agent", "update_tools"],
                    "target_agents": ["agent_1", "agent_2"]
                }
            },
            {
                "intent": IntentType.CONVERSATION,
                "info": {
                    "context": "user_question",
                    "topic": "general_help",
                    "requires_clarification": True
                }
            }
        ]
        
        for scenario in scenarios:
            response_data = {
                "intent": scenario["intent"],
                "confidence": 0.88,
                "extracted_info": scenario["info"]
            }
            response = DispatcherResponse(**response_data)
            assert response.intent == scenario["intent"]
            assert response.extracted_info == scenario["info"]
    
    def test_dispatcher_response_suggested_prompt_scenarios(self):
        """Test DispatcherResponse with various suggested prompt scenarios."""
        prompt_scenarios = [
            {
                "intent": IntentType.GENERATE_AGENT,
                "prompt": "Create a financial analyst agent with expertise in data analysis, capable of processing financial reports and generating insights"
            },
            {
                "intent": IntentType.GENERATE_CREW,
                "prompt": "Build a customer support crew with multiple agents handling different channels: email, chat, phone, and escalation management"
            },
            {
                "intent": IntentType.GENERATE_TASK,
                "prompt": "Design a data processing task that can clean, transform, and validate customer data from multiple sources"
            },
            {
                "intent": IntentType.CONFIGURE_CREW,
                "prompt": "Modify the existing marketing crew by adding a social media agent and updating the content creation tools"
            }
        ]
        
        for scenario in prompt_scenarios:
            response_data = {
                "intent": scenario["intent"],
                "confidence": 0.91,
                "suggested_prompt": scenario["prompt"]
            }
            response = DispatcherResponse(**response_data)
            assert response.intent == scenario["intent"]
            assert response.suggested_prompt == scenario["prompt"]
    
    def test_dispatcher_response_empty_extracted_info(self):
        """Test DispatcherResponse with empty extracted_info."""
        response_data = {
            "intent": IntentType.UNKNOWN,
            "confidence": 0.1,
            "extracted_info": {}
        }
        response = DispatcherResponse(**response_data)
        assert response.extracted_info == {}
        assert len(response.extracted_info) == 0
    
    def test_dispatcher_response_complex_extracted_info(self):
        """Test DispatcherResponse with complex nested extracted_info."""
        complex_info = {
            "request_analysis": {
                "keywords": ["machine learning", "prediction", "classification"],
                "entities": {
                    "technologies": ["python", "scikit-learn", "pandas"],
                    "domains": ["healthcare", "diagnosis"],
                    "methods": ["supervised_learning", "cross_validation"]
                },
                "complexity_score": 0.8
            },
            "recommendations": {
                "primary_intent": "generate_agent",
                "alternative_intents": ["generate_task", "generate_crew"],
                "confidence_breakdown": {
                    "keyword_match": 0.9,
                    "context_analysis": 0.85,
                    "pattern_recognition": 0.8
                }
            },
            "metadata": {
                "processing_time_ms": 150,
                "model_version": "v2.1",
                "language_detected": "en"
            }
        }
        
        response_data = {
            "intent": IntentType.GENERATE_AGENT,
            "confidence": 0.85,
            "extracted_info": complex_info
        }
        response = DispatcherResponse(**response_data)
        assert response.extracted_info == complex_info
        assert response.extracted_info["request_analysis"]["complexity_score"] == 0.8
        assert response.extracted_info["recommendations"]["confidence_breakdown"]["keyword_match"] == 0.9
    
    def test_dispatcher_response_config_example(self):
        """Test DispatcherResponse with config example data."""
        example_data = {
            "intent": "generate_agent",
            "confidence": 0.95,
            "extracted_info": {
                "agent_type": "financial analyst",
                "capabilities": ["analyze data", "financial analysis"]
            },
            "suggested_prompt": "Create a financial analyst agent that can analyze financial data with expertise in data analysis"
        }
        response = DispatcherResponse(**example_data)
        assert response.intent == IntentType.GENERATE_AGENT
        assert response.confidence == 0.95
        assert response.extracted_info["agent_type"] == "financial analyst"
        assert response.suggested_prompt.startswith("Create a financial analyst agent")


class TestSchemaIntegration:
    """Integration tests for dispatcher schema interactions."""
    
    def test_dispatcher_workflow_generate_agent(self):
        """Test complete dispatcher workflow for generating an agent."""
        # User request
        request = DispatcherRequest(
            message="I need an agent that can analyze sales data and create reports",
            model="databricks-llama-4-maverick",
            tools=["DataAnalysisTool", "ReportingTool", "SQLTool"]
        )
        
        # Dispatcher response
        response = DispatcherResponse(
            intent=IntentType.GENERATE_AGENT,
            confidence=0.92,
            extracted_info={
                "agent_type": "sales_analyst",
                "capabilities": ["data_analysis", "report_generation", "sql_queries"],
                "domain": "sales",
                "tools_required": ["DataAnalysisTool", "ReportingTool", "SQLTool"]
            },
            suggested_prompt="Create a sales analyst agent capable of analyzing sales data, generating comprehensive reports, and executing SQL queries for data extraction"
        )
        
        # Verify workflow
        assert "agent" in request.message.lower()
        assert "analyze" in request.message.lower()
        assert "DataAnalysisTool" in request.tools
        assert response.intent == IntentType.GENERATE_AGENT
        assert response.confidence > 0.9
        assert response.extracted_info["agent_type"] == "sales_analyst"
        assert "sql_queries" in response.extracted_info["capabilities"]
        assert response.suggested_prompt.startswith("Create a sales analyst agent")
    
    def test_dispatcher_workflow_generate_crew(self):
        """Test complete dispatcher workflow for generating a crew."""
        # User request
        request = DispatcherRequest(
            message="Build a team of agents to handle customer support across multiple channels",
            model="claude-3-sonnet",
            tools=["EmailTool", "ChatTool", "KnowledgeBaseTool", "TicketingTool"]
        )
        
        # Dispatcher response
        response = DispatcherResponse(
            intent=IntentType.GENERATE_CREW,
            confidence=0.89,
            extracted_info={
                "crew_type": "customer_support",
                "channels": ["email", "chat", "phone"],
                "agent_roles": ["first_line_support", "specialist", "escalation_manager"],
                "tools_distribution": {
                    "first_line_support": ["ChatTool", "KnowledgeBaseTool"],
                    "specialist": ["EmailTool", "TicketingTool"],
                    "escalation_manager": ["EmailTool", "TicketingTool"]
                }
            },
            suggested_prompt="Create a customer support crew with specialized agents for handling multiple communication channels including email, chat, and escalation management"
        )
        
        # Verify workflow
        assert "team" in request.message.lower() or "crew" in request.message.lower()
        assert "customer support" in request.message.lower()
        assert len(request.tools) == 4
        assert response.intent == IntentType.GENERATE_CREW
        assert response.extracted_info["crew_type"] == "customer_support"
        assert len(response.extracted_info["agent_roles"]) == 3
        assert "ChatTool" in response.extracted_info["tools_distribution"]["first_line_support"]
    
    def test_dispatcher_workflow_configure_crew(self):
        """Test complete dispatcher workflow for configuring an existing crew."""
        # User request
        request = DispatcherRequest(
            message="Update my marketing crew by adding a social media agent and removing the email agent",
            tools=["SocialMediaTool", "ContentTool", "AnalyticsTool"]
        )
        
        # Dispatcher response
        response = DispatcherResponse(
            intent=IntentType.CONFIGURE_CREW,
            confidence=0.87,
            extracted_info={
                "action_type": "modify_crew",
                "crew_context": "marketing",
                "modifications": {
                    "add_agents": [{"type": "social_media_agent", "tools": ["SocialMediaTool"]}],
                    "remove_agents": [{"type": "email_agent"}]
                },
                "affected_tools": ["SocialMediaTool", "EmailTool"]
            },
            suggested_prompt="Modify the marketing crew by adding a social media agent equipped with social media tools and removing the email agent"
        )
        
        # Verify workflow
        assert "update" in request.message.lower() or "modify" in request.message.lower()
        assert "marketing crew" in request.message.lower()
        assert response.intent == IntentType.CONFIGURE_CREW
        assert response.extracted_info["action_type"] == "modify_crew"
        assert "add_agents" in response.extracted_info["modifications"]
        assert "remove_agents" in response.extracted_info["modifications"]
    
    def test_dispatcher_workflow_conversation(self):
        """Test complete dispatcher workflow for general conversation."""
        # User request
        request = DispatcherRequest(
            message="What are the different types of agents I can create?"
        )
        
        # Dispatcher response
        response = DispatcherResponse(
            intent=IntentType.CONVERSATION,
            confidence=0.78,
            extracted_info={
                "question_type": "information_request",
                "topic": "agent_types",
                "requires_explanation": True,
                "context": "user_education"
            },
            suggested_prompt="Provide information about the different types of agents that can be created in the system"
        )
        
        # Verify workflow
        assert request.message.startswith("What")
        assert "types of agents" in request.message
        assert response.intent == IntentType.CONVERSATION
        assert response.extracted_info["question_type"] == "information_request"
        assert response.extracted_info["requires_explanation"] is True
    
    def test_dispatcher_confidence_scenarios(self):
        """Test dispatcher responses across different confidence levels."""
        confidence_scenarios = [
            {
                "message": "Create a data analysis agent",
                "intent": IntentType.GENERATE_AGENT,
                "confidence": 0.95,
                "reason": "clear_intent"
            },
            {
                "message": "Maybe build something for data processing?",
                "intent": IntentType.GENERATE_TASK,
                "confidence": 0.65,
                "reason": "ambiguous_language"
            },
            {
                "message": "I want agents or tasks or something",
                "intent": IntentType.UNKNOWN,
                "confidence": 0.25,
                "reason": "very_unclear"
            },
            {
                "message": "Hello",
                "intent": IntentType.CONVERSATION,
                "confidence": 0.8,
                "reason": "clear_greeting"
            }
        ]
        
        for scenario in confidence_scenarios:
            request = DispatcherRequest(message=scenario["message"])
            response = DispatcherResponse(
                intent=scenario["intent"],
                confidence=scenario["confidence"],
                extracted_info={"classification_reason": scenario["reason"]}
            )
            
            # Verify confidence correlates with intent clarity
            if scenario["reason"] == "clear_intent":
                assert response.confidence >= 0.9
            elif scenario["reason"] == "ambiguous_language":
                assert 0.5 <= response.confidence < 0.8
            elif scenario["reason"] == "very_unclear":
                assert response.confidence < 0.5
            
            assert response.intent == scenario["intent"]
            assert response.extracted_info["classification_reason"] == scenario["reason"]
    
    def test_dispatcher_tool_based_intent_detection(self):
        """Test how available tools influence intent detection."""
        # Request with agent-focused tools
        agent_request = DispatcherRequest(
            message="Create something for data processing",
            tools=["DataProcessingTool", "AnalyticsTool", "ReportingTool"]
        )
        
        agent_response = DispatcherResponse(
            intent=IntentType.GENERATE_AGENT,
            confidence=0.85,
            extracted_info={
                "tool_influence": True,
                "suggested_agent_tools": ["DataProcessingTool", "AnalyticsTool"],
                "reasoning": "tools_suggest_single_agent"
            }
        )
        
        # Request with crew-focused tools
        crew_request = DispatcherRequest(
            message="Create something for data processing",
            tools=["EmailTool", "ChatTool", "KnowledgeBaseTool", "EscalationTool", "TicketingTool"]
        )
        
        crew_response = DispatcherResponse(
            intent=IntentType.GENERATE_CREW,
            confidence=0.82,
            extracted_info={
                "tool_influence": True,
                "suggested_crew_structure": {
                    "communication_agents": ["EmailTool", "ChatTool"],
                    "support_agents": ["KnowledgeBaseTool", "TicketingTool"]
                },
                "reasoning": "tools_suggest_multi_agent_crew"
            }
        )
        
        # Verify tool influence on intent detection
        assert agent_request.tools != crew_request.tools
        assert agent_response.intent != crew_response.intent
        assert agent_response.extracted_info["tool_influence"] is True
        assert crew_response.extracted_info["tool_influence"] is True