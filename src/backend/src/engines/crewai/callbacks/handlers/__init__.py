"""Event handler registration modules for AgentTraceEventListener.

Each module exports a `register_*_handlers(listener, crewai_event_bus, log_prefix)`
function that registers the appropriate event handlers on the CrewAI event bus.
"""

from src.engines.crewai.callbacks.handlers.core_handlers import register_core_handlers
from src.engines.crewai.callbacks.handlers.memory_knowledge_handlers import (
    register_memory_handlers,
    register_knowledge_handlers,
)
from src.engines.crewai.callbacks.handlers.tool_handlers import register_tool_handlers
from src.engines.crewai.callbacks.handlers.task_handlers import register_task_handlers
from src.engines.crewai.callbacks.handlers.advanced_handlers import (
    register_llm_stream_handler,
    register_reasoning_handlers,
    register_guardrail_handlers,
)

__all__ = [
    "register_core_handlers",
    "register_memory_handlers",
    "register_knowledge_handlers",
    "register_tool_handlers",
    "register_task_handlers",
    "register_llm_stream_handler",
    "register_reasoning_handlers",
    "register_guardrail_handlers",
]
