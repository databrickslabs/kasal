"""
Utility functions for extracting agent information from CrewAI events.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

def extract_agent_name_from_event(event: Any, log_prefix: str = "", source: Any = None) -> str:
    """
    Extract agent name from CrewAI event.
    
    Based on CrewAI documentation:
    - Memory/LLM events have agent_role field directly on the event
    - AgentExecutionCompletedEvent has agent.role through the agent object
    - All operations in CrewAI are agent-initiated
    
    Args:
        event: CrewAI event object
        log_prefix: Logging prefix for debugging
        source: Optional source parameter (unused but kept for compatibility)
        
    Returns:
        Agent name string
    """
    # Memory, LLM, and other events with direct agent_role field
    if hasattr(event, 'agent_role') and event.agent_role:
        return str(event.agent_role)
    
    # AgentExecutionCompletedEvent and similar with agent.role
    if hasattr(event, 'agent') and event.agent is not None:
        if hasattr(event.agent, 'role') and event.agent.role:
            return str(event.agent.role)
    
    # For crew-level events, these are orchestration events managed by CrewAI
    event_type = type(event).__name__
    if 'Crew' in event_type:
        return "Crew Manager"
    
    # This should never happen - log error and raise exception
    logger.error(f"{log_prefix} CRITICAL: Could not extract agent name from {event_type}. Event attributes: {dir(event)}")
    raise ValueError(f"Cannot determine agent for event type {event_type}. This indicates a bug in the event handling.")

def extract_agent_name_from_object(agent: Any, log_prefix: str = "") -> str:
    """
    Extract agent name from a direct agent object.
    
    Args:
        agent: CrewAI agent object
        log_prefix: Logging prefix for debugging
        
    Returns:
        Agent name string
    """
    if agent is None:
        logger.error(f"{log_prefix} CRITICAL: Agent object is None. This should never happen in CrewAI.")
        raise ValueError("Agent object is None. All CrewAI operations must have an associated agent.")
    
    # Primary: agent.role (standard CrewAI agent attribute)
    if hasattr(agent, 'role') and agent.role:
        return str(agent.role)
    
    # This should never happen - log error and raise exception
    logger.error(f"{log_prefix} CRITICAL: Agent object missing 'role' attribute. Agent attributes: {dir(agent)}")
    raise ValueError("Agent object missing 'role' attribute. This indicates a malformed agent object.")