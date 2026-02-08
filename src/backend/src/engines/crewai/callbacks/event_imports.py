"""Conditional CrewAI event imports with version compatibility.

This module centralizes all CrewAI event imports with graceful fallbacks
for different CrewAI versions. Each event category has an availability
flag that handlers can check before registering.

Compatibility: CrewAI 0.177+
"""

import logging

logger = logging.getLogger(__name__)

# Import CrewAI's event system
from crewai.events import BaseEventListener, crewai_event_bus

# Core Crew Events
try:
    from crewai.events import (
        CrewKickoffStartedEvent,
        CrewKickoffCompletedEvent,
    )

    CREW_EVENTS_AVAILABLE = True
except ImportError:
    CREW_EVENTS_AVAILABLE = False
    logger.info("Some crew events not available in this CrewAI version")

# Agent Events
try:
    from crewai.events import (
        AgentExecutionStartedEvent,
        AgentExecutionCompletedEvent,
        AgentExecutionErrorEvent,
    )

    AGENT_EVENTS_AVAILABLE = True
except ImportError:
    AGENT_EVENTS_AVAILABLE = False
    logger.info("Some agent events not available in this CrewAI version")

# Task Events
try:
    from crewai.events.types.task_events import (
        TaskStartedEvent,
        TaskCompletedEvent,
        TaskFailedEvent,
    )

    TASK_EVENTS_AVAILABLE = True
    logger.info("Task events loaded successfully from CrewAI 0.177")
except ImportError:
    TASK_EVENTS_AVAILABLE = False
    logger.info("Task events not available in this CrewAI version")

# Tool Usage Events
try:
    from crewai.events.types.tool_usage_events import (
        ToolUsageStartedEvent,
        ToolUsageFinishedEvent,
        ToolUsageErrorEvent,
    )

    TOOL_EVENTS_AVAILABLE = True
    logger.info("Tool usage events loaded successfully from CrewAI 0.177")
except ImportError:
    TOOL_EVENTS_AVAILABLE = False
    logger.info("Tool events not available in this CrewAI version")

# LLM Events - Only LLMStreamChunkEvent exists in 0.177
try:
    from crewai.events import LLMStreamChunkEvent

    LLM_EVENTS_AVAILABLE = True
    logger.info("LLM events loaded successfully for CrewAI 0.177")
except ImportError as e:
    LLM_EVENTS_AVAILABLE = False
    logger.warning(f"LLM events not available: {e}")

# Memory Events
try:
    from crewai.events import (
        MemoryQueryStartedEvent,
        MemoryQueryCompletedEvent,
        MemoryQueryFailedEvent,
        MemorySaveStartedEvent,
        MemorySaveCompletedEvent,
        MemorySaveFailedEvent,
        MemoryRetrievalCompletedEvent,
    )

    MEMORY_EVENTS_AVAILABLE = True
    logger.info("Memory events loaded successfully for CrewAI 0.177")
except ImportError as e:
    MEMORY_EVENTS_AVAILABLE = False
    logger.warning(f"Memory events not available: {e}")

# Knowledge Events
try:
    from crewai.events import (
        KnowledgeRetrievalStartedEvent,
        KnowledgeRetrievalCompletedEvent,
    )

    KNOWLEDGE_EVENTS_AVAILABLE = True
    logger.info("Knowledge events loaded successfully for CrewAI 0.177")
except ImportError as e:
    KNOWLEDGE_EVENTS_AVAILABLE = False
    logger.warning(f"Knowledge events not available: {e}")

# Reasoning Events
try:
    from crewai.events.types.reasoning_events import (
        AgentReasoningStartedEvent,
        AgentReasoningCompletedEvent,
        AgentReasoningFailedEvent,
    )

    REASONING_EVENTS_AVAILABLE = True
    logger.info("Reasoning events loaded successfully from CrewAI 0.177")
except ImportError:
    REASONING_EVENTS_AVAILABLE = False
    logger.info("Reasoning events not available in this CrewAI version")

# LLM Guardrail Events
try:
    from crewai.events.types.llm_guardrail_events import (
        LLMGuardrailStartedEvent,
        LLMGuardrailCompletedEvent,
        LLMGuardrailFailedEvent,
    )

    LLM_GUARDRAIL_EVENTS_AVAILABLE = True
    logger.info("LLM Guardrail events loaded successfully from CrewAI 0.177+")
except ImportError:
    LLM_GUARDRAIL_EVENTS_AVAILABLE = False
    logger.info("LLM Guardrail events not available in this CrewAI version")

# Flow Events
try:
    from crewai.events.types.flow_events import (
        FlowStartedEvent,
        FlowFinishedEvent,
        FlowCreatedEvent,
    )

    FLOW_EVENTS_AVAILABLE = True
    logger.info("Flow events loaded successfully from CrewAI 0.177")
except ImportError:
    FLOW_EVENTS_AVAILABLE = False
    logger.info("Flow events not available in this CrewAI version")
