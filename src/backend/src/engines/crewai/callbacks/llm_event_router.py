"""
LLM Event Router for capturing CrewAI LLM events.

This module provides a router that captures LLM events from the global CrewAI
event bus and routes them to the appropriate execution based on agent context.
"""

import logging
import threading
from typing import Dict, Set, Any, Optional
from datetime import datetime, timezone
import weakref

from crewai.utilities.events import crewai_event_bus, LLMCallCompletedEvent
from src.services.trace_queue import get_trace_queue
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class LLMEventRouter:
    """
    Routes LLM events to appropriate executions based on agent context.
    
    This is a singleton that registers once with the global CrewAI event bus
    and routes events to the appropriate execution based on agent ownership.
    """
    
    _instance: Optional['LLMEventRouter'] = None
    _lock = threading.Lock()
    _initialized = False
    _active_executions: Dict[str, Dict[str, Any]] = {}
    
    def __new__(cls):
        """Ensure singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def register_execution(cls, execution_id: str, crew: Any, group_context: Optional[GroupContext] = None):
        """
        Register an execution with its agents for LLM event routing.
        
        Args:
            execution_id: Unique identifier for the execution
            crew: The CrewAI crew instance
            group_context: Optional group context for multi-tenant isolation
        """
        router = cls()
        
        # Extract agent roles from crew
        agent_roles = set()
        if crew and hasattr(crew, 'agents'):
            for agent in crew.agents:
                if hasattr(agent, 'role'):
                    agent_roles.add(agent.role)
        
        # Store execution data
        cls._active_executions[execution_id] = {
            'agents': agent_roles,
            'group_context': group_context,
            'trace_queue': get_trace_queue()
        }
        
        log_prefix = f"[LLMEventRouter][{execution_id}]"
        logger.info(f"{log_prefix} Registered execution with agents: {agent_roles}")
        
        # Initialize global handler if needed
        if not cls._initialized:
            router._setup_global_handler()
            cls._initialized = True
    
    @classmethod
    def unregister_execution(cls, execution_id: str):
        """
        Remove an execution from routing.
        
        Args:
            execution_id: The execution to unregister
        """
        if execution_id in cls._active_executions:
            agent_roles = cls._active_executions[execution_id]['agents']
            del cls._active_executions[execution_id]
            logger.info(f"[LLMEventRouter][{execution_id}] Unregistered execution with agents: {agent_roles}")
    
    def _setup_global_handler(self):
        """Set up the global LLM event handler once."""
        logger.info("[LLMEventRouter] Setting up global LLM event handler")
        
        @crewai_event_bus.on(LLMCallCompletedEvent)
        def handle_llm_event(source: Any, event: LLMCallCompletedEvent):
            """Handle LLM events and route to appropriate execution."""
            try:
                # Skip if no agent role
                if not hasattr(event, 'agent_role') or not event.agent_role:
                    return
                
                agent_role = event.agent_role
                
                # Find which execution(s) this belongs to
                # Note: If multiple executions have the same agent role, the event
                # will be routed to the first matching one (limitation)
                for exec_id, exec_data in self._active_executions.items():
                    if agent_role in exec_data['agents']:
                        # Extract response content
                        output_content = "LLM call completed"
                        if hasattr(event, 'response') and event.response:
                            output_content = str(event.response)
                        
                        # Create trace data
                        trace_data = {
                            "job_id": exec_id,
                            "event_source": agent_role,
                            "event_context": "llm_call",
                            "event_type": "llm_call",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "output_content": output_content,
                            "extra_data": {
                                "type": "llm_event",
                                "agent_role": agent_role,
                                "model": str(event.model) if hasattr(event, 'model') else "unknown",
                                "call_type": str(event.call_type.value) if hasattr(event, 'call_type') else "unknown"
                            }
                        }
                        
                        # Add group context if available
                        if exec_data['group_context']:
                            trace_data["group_id"] = exec_data['group_context'].primary_group_id
                            trace_data["group_email"] = exec_data['group_context'].group_email
                        
                        # Enqueue trace
                        exec_data['trace_queue'].put_nowait(trace_data)
                        
                        logger.debug(f"[LLMEventRouter] Routed LLM event from {agent_role} to execution {exec_id}")
                        break  # Event processed, don't route to other executions
                
            except Exception as e:
                logger.error(f"[LLMEventRouter] Error handling LLM event: {e}")
        
        logger.info("[LLMEventRouter] Global LLM event handler registered")
    
    @classmethod
    def get_active_execution_count(cls) -> int:
        """Get the number of active executions being tracked."""
        return len(cls._active_executions)
    
    @classmethod
    def get_active_agents(cls) -> Set[str]:
        """Get all agent roles currently being tracked across all executions."""
        all_agents = set()
        for exec_data in cls._active_executions.values():
            all_agents.update(exec_data['agents'])
        return all_agents


# Convenience functions
def register_execution_for_llm_events(execution_id: str, crew: Any, group_context: Optional[GroupContext] = None):
    """
    Register an execution to receive LLM events.
    
    Args:
        execution_id: Unique identifier for the execution
        crew: The CrewAI crew instance
        group_context: Optional group context
    """
    LLMEventRouter.register_execution(execution_id, crew, group_context)


def unregister_execution_from_llm_events(execution_id: str):
    """
    Unregister an execution from receiving LLM events.
    
    Args:
        execution_id: The execution to unregister
    """
    LLMEventRouter.unregister_execution(execution_id)