import pytest
from unittest.mock import MagicMock, patch

from src.engines.crewai.utils.agent_utils import (
    extract_agent_name_from_event,
    extract_agent_name_from_object
)


class TestExtractAgentNameFromEvent:
    """Test suite for extract_agent_name_from_event function."""
    
    def test_extract_from_event_with_agent_role(self):
        """Test extracting agent name from event with agent.role."""
        mock_event = MagicMock()
        mock_agent = MagicMock()
        mock_agent.role = "Data Analyst"
        mock_event.agent = mock_agent
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            assert result == "Data Analyst"
            mock_logger.info.assert_any_call("TEST DEBUG: Extracting agent name from MagicMock")
    
    def test_extract_from_event_with_agent_name_fallback(self):
        """Test extracting agent name using name fallback."""
        mock_event = MagicMock()
        mock_agent = MagicMock()
        mock_agent.role = None  # No role
        mock_agent.name = "Agent Smith"
        mock_event.agent = mock_agent
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            assert result == "Agent Smith"
            mock_logger.info.assert_any_call("TEST Using agent.name as fallback: Agent Smith")
    
    def test_extract_from_event_with_agent_id_fallback(self):
        """Test extracting agent name using id fallback."""
        mock_event = MagicMock()
        mock_agent = MagicMock()
        mock_agent.role = None
        mock_agent.name = None
        mock_agent.id = "12345"
        mock_event.agent = mock_agent
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            assert result == "Agent-12345"
            mock_logger.info.assert_any_call("TEST Using agent.id as fallback: Agent-12345")
    
    def test_extract_from_event_context_agent(self):
        """Test extracting agent name from event.context.agent."""
        mock_event = MagicMock()
        mock_event.agent = None  # No direct agent
        
        mock_context = MagicMock()
        mock_agent = MagicMock()
        mock_agent.role = "Context Agent"
        mock_context.agent = mock_agent
        mock_event.context = mock_context
        
        with patch('src.engines.crewai.utils.agent_utils.logger'):
            result = extract_agent_name_from_event(mock_event)
            
            assert result == "Context Agent"
    
    def test_extract_from_crew_event(self):
        """Test extracting from crew-level events."""
        # Create a mock object that will return correct type name
        class CrewKickoffStartedEvent:
            def __init__(self):
                self.agent = None
        
        mock_event = CrewKickoffStartedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        
        assert result == "Crew"
    
    def test_extract_from_task_event_without_agent(self):
        """Test extracting from task events without agent info."""
        # Create a mock object that will return correct type name
        class TaskCompletedEvent:
            pass
        
        mock_event = TaskCompletedEvent()
        # Don't add agent attribute
        
        result = extract_agent_name_from_event(mock_event)
        
        assert result == "System"
    
    def test_extract_from_llm_event_with_system_message(self):
        """Test extracting agent name from LLM event system messages."""
        # Create a mock object that will return correct type name
        class LLMCallStartedEvent:
            def __init__(self):
                self.agent = None
                self.messages = [
                    {"role": "system", "content": "You are Data Scientist. Analyze the data carefully."},
                    {"role": "user", "content": "Please analyze this dataset."}
                ]
        
        mock_event = LLMCallStartedEvent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            assert result == "Data Scientist"
            mock_logger.info.assert_any_call("TEST DEBUG: Extracted agent from system message: Data Scientist")
    
    def test_extract_from_llm_event_with_multiline_system_message(self):
        """Test extracting from system message with newlines."""
        # Create a mock object that will return correct type name
        class LLMCallCompletedEvent:
            def __init__(self):
                self.agent = None
                self.messages = [
                    {
                        "role": "system", 
                        "content": "You are Research Assistant\nYour job is to conduct thorough research."
                    }
                ]
        
        mock_event = LLMCallCompletedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        
        # The function extracts up to newline or end - in this case there's no period so it gets more text
        assert "Research Assistant" in result
    
    def test_extract_fallback_unknown_agent(self):
        """Test fallback to unknown agent format."""
        # Create a mock object that will return correct type name
        class CustomEvent:
            def __init__(self):
                self.agent = None
        
        mock_event = CustomEvent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            assert result == "UnknownAgent-CustomEvent"
            mock_logger.info.assert_any_call("TEST DEBUG: No agent information found in CustomEvent event")
    
    def test_extract_with_empty_agent_object(self):
        """Test with agent object that exists but is None."""
        # Create a simple class to avoid MagicMock behavior that interferes with testing
        class SimpleEvent:
            def __init__(self):
                self.agent = None
        
        mock_event = SimpleEvent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            # Should generate the expected log and fallback to UnknownAgent pattern
            assert result.startswith("UnknownAgent-")
            mock_logger.info.assert_any_call("TEST DEBUG: Event has agent attribute but agent is: None")
    
    def test_extract_with_debug_logging(self):
        """Test debug logging for agent without role."""
        mock_event = MagicMock()
        mock_agent = MagicMock()
        mock_agent.role = None
        mock_agent.name = None
        mock_agent.id = None
        mock_event.agent = mock_agent
        
        # Mock dir() to return some attributes
        with patch('builtins.dir', return_value=['public_attr', 'another_attr', '_private']), \
             patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            
            result = extract_agent_name_from_event(mock_event, "TEST")
            
            # Should log available attributes
            mock_logger.debug.assert_any_call(
                "TEST Agent exists but missing 'role'. Available attributes: ['public_attr', 'another_attr']"
            )
    
    def test_extract_with_empty_system_message(self):
        """Test LLM event with empty system message."""
        # Create a mock object that will return correct type name
        class LLMCallStartedEvent:
            def __init__(self):
                self.agent = None
                self.messages = [
                    {"role": "system", "content": ""},
                    {"role": "user", "content": "Test message"}
                ]
        
        mock_event = LLMCallStartedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        
        assert result == "UnknownAgent-LLMCallStartedEvent"
    
    def test_extract_with_malformed_system_message(self):
        """Test system message that doesn't match expected pattern."""
        # Create a mock object that will return correct type name
        class LLMCallStartedEvent:
            def __init__(self):
                self.agent = None
                self.messages = [
                    {"role": "system", "content": "This is not the right pattern"},
                    {"role": "user", "content": "Test"}
                ]
        
        mock_event = LLMCallStartedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        
        assert result == "UnknownAgent-LLMCallStartedEvent"
    
    def test_extract_with_no_messages(self):
        """Test LLM event with no messages."""
        # Create a mock object that will return correct type name
        class LLMCallStartedEvent:
            def __init__(self):
                self.agent = None
                self.messages = []
        
        mock_event = LLMCallStartedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        
        assert result == "UnknownAgent-LLMCallStartedEvent"


class TestExtractAgentNameFromObject:
    """Test suite for extract_agent_name_from_object function."""
    
    def test_extract_from_none_agent(self):
        """Test extracting from None agent."""
        result = extract_agent_name_from_object(None)
        assert result == "NoAgent"
    
    def test_extract_from_agent_with_role(self):
        """Test extracting from agent with role."""
        mock_agent = MagicMock()
        mock_agent.role = "Senior Developer"
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "Senior Developer"
    
    def test_extract_from_agent_with_name_fallback(self):
        """Test extracting using name fallback."""
        mock_agent = MagicMock()
        mock_agent.role = None
        mock_agent.name = "Alice"
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_object(mock_agent, "TEST")
            
            assert result == "Alice"
            mock_logger.info.assert_called_with("TEST Using agent.name as fallback: Alice")
    
    def test_extract_from_agent_with_id_fallback(self):
        """Test extracting using id fallback."""
        mock_agent = MagicMock()
        mock_agent.role = None
        mock_agent.name = None
        mock_agent.id = "agent-456"
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_object(mock_agent, "TEST")
            
            assert result == "Agent-agent-456"
            mock_logger.info.assert_called_with("TEST Using agent.id as fallback: Agent-agent-456")
    
    def test_extract_from_agent_no_identifiers(self):
        """Test extracting from agent with no identifiers."""
        # Create a custom class to avoid MagicMock type issues
        class CustomAgent:
            def __init__(self):
                self.role = None
                self.name = None
                self.id = None
                self.method1 = "test"
                self.method2 = "test"
                self._private = "test"
        
        mock_agent = CustomAgent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_object(mock_agent, "TEST")
            
            assert result == "UnknownAgent-CustomAgent"
            # Check that warning was called with agent attributes (method1, method2 but not _private)
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args[0][0]
            assert "TEST Agent object missing role/name/id" in call_args
            assert "method1" in call_args
            assert "method2" in call_args
            assert "_private" not in call_args
    
    def test_extract_with_empty_role(self):
        """Test with empty string role."""
        mock_agent = MagicMock()
        mock_agent.role = ""
        mock_agent.name = "Backup Name"
        
        with patch('src.engines.crewai.utils.agent_utils.logger'):
            result = extract_agent_name_from_object(mock_agent)
            
            assert result == "Backup Name"
    
    def test_extract_with_empty_name(self):
        """Test with empty string name."""
        mock_agent = MagicMock()
        mock_agent.role = None
        mock_agent.name = ""
        mock_agent.id = "backup-id"
        
        with patch('src.engines.crewai.utils.agent_utils.logger'):
            result = extract_agent_name_from_object(mock_agent)
            
            assert result == "Agent-backup-id"
    
    def test_extract_with_zero_id(self):
        """Test with zero as id (should not work since 0 is falsy)."""
        # Create a custom class to avoid MagicMock type issues
        class MockAgent:
            def __init__(self):
                self.role = None
                self.name = None
                self.id = 0
        
        mock_agent = MockAgent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger'):
            result = extract_agent_name_from_object(mock_agent)
            
            # 0 is falsy, so should fall back to UnknownAgent pattern
            assert result == "UnknownAgent-MockAgent"
    
    def test_extract_with_numeric_role(self):
        """Test with numeric role value."""
        mock_agent = MagicMock()
        mock_agent.role = 123
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "123"
    
    def test_extract_with_complex_object_attributes(self):
        """Test with agent having complex object attributes."""
        # Create a custom class to avoid MagicMock type issues
        class CrewAIAgent:
            def __init__(self):
                self.role = None
                self.name = None
                self.id = None
                self.config = {"setting": "value"}
                self.tools = ["tool1", "tool2"]
                self.memory = MagicMock()
                self._internal = "private"
        
        mock_agent = CrewAIAgent()
        
        result = extract_agent_name_from_object(mock_agent)
        
        assert result == "UnknownAgent-CrewAIAgent"
    
    def test_extract_preserves_string_conversion(self):
        """Test that result is properly converted to string."""
        mock_agent = MagicMock()
        
        # Mock role as a custom object
        class CustomRole:
            def __str__(self):
                return "Custom Role Object"
        
        mock_agent.role = CustomRole()
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "Custom Role Object"
        assert isinstance(result, str)