import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from src.engines.crewai.utils.agent_utils import (
    extract_agent_name_from_event,
    extract_agent_name_from_object
)


class TestExtractAgentNameFromEvent:
    """Test suite for extract_agent_name_from_event function."""
    
    def test_extract_from_event_with_agent_role_field(self):
        """Test extracting agent name from event with agent_role field."""
        # Create a simple class to avoid MagicMock auto-attribute creation
        class TestEvent:
            def __init__(self):
                self.agent_role = "Data Analyst"
        
        mock_event = TestEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Data Analyst"
    
    def test_extract_from_event_with_agent_object(self):
        """Test extracting agent name from event with agent.role."""
        # Create simple classes to avoid MagicMock issues
        class TestAgent:
            def __init__(self):
                self.role = "Senior Developer"
        
        class TestEvent:
            def __init__(self):
                self.agent = TestAgent()
        
        mock_event = TestEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Senior Developer"
    
    def test_extract_from_crew_event(self):
        """Test extracting from crew-level events."""
        class CrewKickoffStartedEvent:
            pass
        
        mock_event = CrewKickoffStartedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Crew Manager"
    
    def test_extract_from_event_without_agent_raises_error(self):
        """Test that events without agent info raise ValueError."""
        class CustomEvent:
            pass
        
        mock_event = CustomEvent()
        
        with pytest.raises(ValueError, match="Cannot determine agent for event type"):
            extract_agent_name_from_event(mock_event)
    
    def test_extract_with_none_agent_raises_error(self):
        """Test that event with None agent raises ValueError."""
        class TestEvent:
            def __init__(self):
                self.agent = None
        
        mock_event = TestEvent()
        
        with pytest.raises(ValueError, match="Cannot determine agent for event type"):
            extract_agent_name_from_event(mock_event)
    
    def test_extract_with_agent_missing_role_raises_error(self):
        """Test that agent without role attribute raises ValueError."""
        class TestAgent:
            def __init__(self):
                self.name = "Test Agent"  # Has name but no role
        
        class TestEvent:
            def __init__(self):
                self.agent = TestAgent()
        
        mock_event = TestEvent()
        
        # The implementation checks for agent.role and if not found, falls through to the general error
        with pytest.raises(ValueError, match="Cannot determine agent for event type"):
            extract_agent_name_from_event(mock_event)
    
    def test_extract_with_empty_agent_role_field(self):
        """Test event with empty agent_role field falls back to agent object."""
        class TestAgent:
            def __init__(self):
                self.role = "Backup Role"
        
        class TestEvent:
            def __init__(self):
                self.agent_role = None  # Empty agent_role
                self.agent = TestAgent()
        
        mock_event = TestEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Backup Role"
    
    def test_extract_with_empty_agent_role_string(self):
        """Test event with empty string agent_role field falls back to agent object."""
        class TestAgent:
            def __init__(self):
                self.role = "Fallback Role"
        
        class TestEvent:
            def __init__(self):
                self.agent_role = ""  # Empty string
                self.agent = TestAgent()
        
        mock_event = TestEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Fallback Role"
    
    def test_crew_completion_event(self):
        """Test CrewKickoffCompletedEvent returns Crew Manager."""
        class CrewKickoffCompletedEvent:
            pass
        
        mock_event = CrewKickoffCompletedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Crew Manager"
    
    def test_agent_execution_completed_event(self):
        """Test AgentExecutionCompletedEvent with agent.role."""
        class TestAgent:
            def __init__(self):
                self.role = "Research Assistant"
        
        class AgentExecutionCompletedEvent:
            def __init__(self):
                self.agent = TestAgent()
        
        mock_event = AgentExecutionCompletedEvent()
        
        result = extract_agent_name_from_event(mock_event)
        assert result == "Research Assistant"


class TestExtractAgentNameFromObject:
    """Test suite for extract_agent_name_from_object function."""
    
    def test_extract_from_none_agent_raises_error(self):
        """Test extracting from None agent raises ValueError."""
        with pytest.raises(ValueError, match="Agent object is None"):
            extract_agent_name_from_object(None)
    
    def test_extract_from_agent_with_role(self):
        """Test extracting from agent with role."""
        class TestAgent:
            def __init__(self):
                self.role = "Senior Developer"
        
        mock_agent = TestAgent()
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "Senior Developer"
    
    def test_extract_from_agent_without_role_raises_error(self):
        """Test extracting from agent without role raises ValueError."""
        class TestAgent:
            def __init__(self):
                self.name = "Alice"  # Has name but no role
                self.id = "123"
        
        mock_agent = TestAgent()
        
        with pytest.raises(ValueError, match="Agent object missing 'role' attribute"):
            extract_agent_name_from_object(mock_agent)
    
    def test_extract_with_empty_role(self):
        """Test agent with empty/None role raises ValueError."""
        class TestAgent:
            def __init__(self):
                self.role = None
                self.name = "Backup Name"
        
        mock_agent = TestAgent()
        
        with pytest.raises(ValueError, match="Agent object missing 'role' attribute"):
            extract_agent_name_from_object(mock_agent)
    
    def test_extract_with_empty_string_role(self):
        """Test agent with empty string role raises ValueError."""
        class TestAgent:
            def __init__(self):
                self.role = ""
                self.name = "Backup Name"
        
        mock_agent = TestAgent()
        
        with pytest.raises(ValueError, match="Agent object missing 'role' attribute"):
            extract_agent_name_from_object(mock_agent)
    
    def test_extract_with_numeric_role(self):
        """Test with numeric role value."""
        class TestAgent:
            def __init__(self):
                self.role = 123
        
        mock_agent = TestAgent()
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "123"
    
    def test_extract_preserves_string_conversion(self):
        """Test that result is properly converted to string."""
        class CustomRole:
            def __str__(self):
                return "Custom Role Object"
        
        class TestAgent:
            def __init__(self):
                self.role = CustomRole()
        
        mock_agent = TestAgent()
        
        result = extract_agent_name_from_object(mock_agent)
        assert result == "Custom Role Object"
        assert isinstance(result, str)
    
    def test_extract_with_log_prefix(self):
        """Test logging with prefix when agent has role."""
        class TestAgent:
            def __init__(self):
                self.role = "Test Role"
        
        mock_agent = TestAgent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            result = extract_agent_name_from_object(mock_agent, "TEST_PREFIX")
            assert result == "Test Role"
            # No error logging should occur for valid agent
            mock_logger.error.assert_not_called()
    
    def test_extract_error_logging_for_none(self):
        """Test error logging when agent is None."""
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            with pytest.raises(ValueError):
                extract_agent_name_from_object(None, "TEST_PREFIX")
            
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args[0][0]
            assert "TEST_PREFIX" in call_args
            assert "CRITICAL" in call_args
            assert "Agent object is None" in call_args
    
    def test_extract_error_logging_for_missing_role(self):
        """Test error logging when agent missing role."""
        class TestAgent:
            def __init__(self):
                self.name = "Test"
                self.id = "123"
        
        mock_agent = TestAgent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            with pytest.raises(ValueError):
                extract_agent_name_from_object(mock_agent, "TEST_PREFIX")
            
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args[0][0]
            assert "TEST_PREFIX" in call_args
            assert "CRITICAL" in call_args
            assert "missing 'role' attribute" in call_args


class TestEventWithLogPrefix:
    """Test log_prefix parameter usage."""
    
    def test_event_log_prefix_in_error(self):
        """Test log_prefix appears in error logs."""
        class TestEvent:
            pass
        
        mock_event = TestEvent()
        
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            with pytest.raises(ValueError):
                extract_agent_name_from_event(mock_event, "MY_PREFIX")
            
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args[0][0]
            assert "MY_PREFIX" in call_args
    
    def test_object_log_prefix_in_error(self):
        """Test log_prefix appears in object error logs."""
        with patch('src.engines.crewai.utils.agent_utils.logger') as mock_logger:
            with pytest.raises(ValueError):
                extract_agent_name_from_object(None, "OBJ_PREFIX")
            
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args[0][0]
            assert "OBJ_PREFIX" in call_args


class TestSourceParameter:
    """Test that source parameter is accepted but unused."""
    
    def test_source_parameter_ignored(self):
        """Test source parameter doesn't affect behavior."""
        class TestAgent:
            def __init__(self):
                self.role = "Test Role"
        
        class TestEvent:
            def __init__(self):
                self.agent = TestAgent()
        
        mock_event = TestEvent()
        
        # Should work the same with or without source
        result1 = extract_agent_name_from_event(mock_event)
        result2 = extract_agent_name_from_event(mock_event, source="some_source")
        result3 = extract_agent_name_from_event(mock_event, "", "another_source")
        
        assert result1 == "Test Role"
        assert result2 == "Test Role"
        assert result3 == "Test Role"