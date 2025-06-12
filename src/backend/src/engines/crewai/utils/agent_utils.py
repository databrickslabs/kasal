"""
Utility functions for extracting agent information from CrewAI events.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

def extract_agent_name_from_event(event: Any, log_prefix: str = "", source: Any = None) -> str:
    """
    Extract agent name from CrewAI event with debugging and multiple fallback strategies.
    Based on CrewAI documentation: event handlers receive (source, event) parameters.
    
    Args:
        event: CrewAI event object
        log_prefix: Logging prefix for debugging
        
    Returns:
        Agent name string with descriptive fallback
    """
    event_type = type(event).__name__
    logger.info(f"{log_prefix} DEBUG: Extracting agent name from {event_type}")
    
    # Strategy 1: Direct agent.role access (primary CrewAI pattern)
    if hasattr(event, 'agent') and event.agent is not None:
        agent = event.agent
        if hasattr(agent, 'role') and agent.role:
            return str(agent.role)
        else:
            # Debug missing role
            agent_attrs = [attr for attr in dir(agent) if not attr.startswith('_')]
            logger.debug(f"{log_prefix} Agent exists but missing 'role'. Available attributes: {agent_attrs}")
            
            # Try alternative agent identifiers
            if hasattr(agent, 'name') and agent.name:
                logger.info(f"{log_prefix} Using agent.name as fallback: {agent.name}")
                return str(agent.name)
            elif hasattr(agent, 'id') and agent.id:
                logger.info(f"{log_prefix} Using agent.id as fallback: Agent-{agent.id}")
                return f"Agent-{agent.id}"
    
    # Strategy 2: Context-based agent access  
    elif hasattr(event, 'context') and hasattr(event.context, 'agent') and event.context.agent is not None:
        agent = event.context.agent
        if hasattr(agent, 'role') and agent.role:
            return str(agent.role)
    
    # Strategy 3: Check if this is a CrewAI event without agent info (like crew-level events)
    event_type = type(event).__name__
    if 'Crew' in event_type:
        return "Crew"
    elif 'Task' in event_type and not hasattr(event, 'agent'):
        return "System"
    
    # Final fallback with improved debugging - use INFO level to ensure it appears in logs
    logger.info(f"{log_prefix} DEBUG: No agent information found in {event_type} event")
    if hasattr(event, 'agent'):
        logger.info(f"{log_prefix} DEBUG: Event has agent attribute but agent is: {event.agent}")
    
    # Enhanced debugging for specific event types that commonly lack agent info
    if 'LLM' in event_type:
        # For LLM events, try to extract from messages
        if hasattr(event, 'messages') and event.messages:
            logger.info(f"{log_prefix} DEBUG: LLM event has messages, checking for agent info")
            # Look for system message that contains agent role
            for msg in event.messages:
                if isinstance(msg, dict) and msg.get('role') == 'system':
                    content = msg.get('content', '')
                    # Extract agent name from "You are [Agent Name]." pattern
                    if content.startswith('You are '):
                        # Find the end of the agent name (typically ends with a period or description)
                        end_idx = content.find('.')
                        if end_idx == -1:
                            end_idx = content.find('\n')
                        if end_idx > 8:  # "You are " is 8 chars
                            agent_name = content[8:end_idx].strip()
                            logger.info(f"{log_prefix} DEBUG: Extracted agent from system message: {agent_name}")
                            return agent_name
    
    return f"UnknownAgent-{event_type}"

def extract_agent_name_from_object(agent: Any, log_prefix: str = "") -> str:
    """
    Extract agent name from a direct agent object.
    
    Args:
        agent: CrewAI agent object
        log_prefix: Logging prefix for debugging
        
    Returns:
        Agent name string with descriptive fallback
    """
    if agent is None:
        return "NoAgent"
    
    # Try role first (primary CrewAI pattern)
    if hasattr(agent, 'role') and agent.role:
        return str(agent.role)
    
    # Try name as fallback
    if hasattr(agent, 'name') and agent.name:
        logger.info(f"{log_prefix} Using agent.name as fallback: {agent.name}")
        return str(agent.name)
    
    # Try ID as last resort
    if hasattr(agent, 'id') and agent.id:
        logger.info(f"{log_prefix} Using agent.id as fallback: Agent-{agent.id}")
        return f"Agent-{agent.id}"
    
    # Debug and fallback
    agent_attrs = [attr for attr in dir(agent) if not attr.startswith('_')]
    logger.warning(f"{log_prefix} Agent object missing role/name/id. Available attributes: {agent_attrs}")
    return f"UnknownAgent-{type(agent).__name__}"