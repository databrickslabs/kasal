"""Memory and knowledge event handlers.

Handles MemorySave, MemoryQuery, MemoryRetrieval, and KnowledgeRetrieval events.
"""

import logging

from src.engines.crewai.utils.agent_utils import extract_agent_name_from_event

logger = logging.getLogger(__name__)


def register_memory_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register all memory event handlers (save, query, retrieval).

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        MemorySaveStartedEvent,
        MemorySaveCompletedEvent,
        MemoryQueryStartedEvent,
        MemoryQueryCompletedEvent,
        MemoryRetrievalCompletedEvent,
    )

    def _resolve_memory_type(source_type):
        """Convert CrewAI source_type to friendly memory type name."""
        if not source_type:
            return "memory"
        if "short_term" in source_type:
            return "short_term"
        elif "long_term" in source_type:
            return "long_term"
        elif "entity" in source_type:
            return "entity"
        return source_type.replace("_memory", "")

    @crewai_event_bus.on(MemorySaveStartedEvent)
    def on_memory_save_started(source, event):
        """Handle memory save start events."""
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            source_type = event.source_type if hasattr(event, "source_type") else None
            memory_type = _resolve_memory_type(source_type)

            memory_value = (
                str(event.value) if hasattr(event, "value") and event.value else None
            )

            task_name, task_id, task_description = listener._extract_task_info(event)

            logger.info(
                f"{log_prefix} Event: MemorySaveStarted | Agent: {agent_name} | Type: {memory_type} | source_type: {source_type} | Task: {task_name}"
            )
            if memory_value:
                logger.info(f"{log_prefix} Memory value (first 200 chars): {memory_value[:200]}")

            # Store value for use in completed event
            context_key = f"pending_save_{agent_name}_{memory_type}"
            if listener.job_id not in listener._active_context:
                listener._active_context[listener.job_id] = {}
            listener._active_context[listener.job_id][context_key] = {
                "value": memory_value,
                "memory_type": memory_type,
                "task_name": task_name,
                "task_id": task_id,
            }
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_memory_save_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(MemorySaveCompletedEvent)
    def on_memory_save_completed(source, event):
        """Handle memory save completion events."""
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            source_type = event.source_type if hasattr(event, "source_type") else None
            memory_type = _resolve_memory_type(source_type)

            task_name, task_id, task_description = listener._extract_task_info(event)

            context_key = f"pending_save_{agent_name}_{memory_type}"
            pending_save = None
            if listener.job_id in listener._active_context:
                pending_save = listener._active_context[listener.job_id].pop(context_key, None)

            saved_value = pending_save.get("value") if pending_save else None

            event_value = (
                str(event.value) if hasattr(event, "value") and event.value else None
            )
            event_metadata = (
                event.metadata if hasattr(event, "metadata") and event.metadata else {}
            )

            logger.info(
                f"{log_prefix} Event: MemorySaveCompleted | Agent: {agent_name} | Type: {memory_type} | source_type: {source_type} | Task: {task_name}"
            )

            output_content = (
                event_value or saved_value or f"Saved to {memory_type} memory"
            )

            entity_count = None
            if memory_type == "entity" and event_metadata:
                entity_count = event_metadata.get("entity_count")
                if entity_count and "Saved" in (output_content or ""):
                    logger.info(f"{log_prefix} Entity batch save: {entity_count} entities")

            if output_content and len(output_content) > 200:
                logger.info(f"{log_prefix} Output content (first 200 chars): {output_content[:200]}")
            else:
                logger.info(f"{log_prefix} Output content: {output_content}")

            extra_data = {
                "operation": "save_completed",
                "memory_type": memory_type,
                "backend": "default",
                "task_name": task_name,
                "task_id": task_id,
            }

            if entity_count:
                extra_data["entity_count"] = entity_count

            if event_metadata:
                for key in ["quality", "expected_output", "agent"]:
                    if key in event_metadata:
                        extra_data[key] = event_metadata[key]

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=f"saved_{memory_type}",
                event_type="memory_write",
                output_content=output_content,
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_memory_save_completed: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(MemoryQueryStartedEvent)
    def on_memory_query_started(source, event):
        """Handle memory query start events.

        NOTE: We don't save to database - only the completed event is saved.
        """
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            source_type = event.source_type if hasattr(event, "source_type") else None
            memory_type = _resolve_memory_type(source_type)

            query = str(event.query) if hasattr(event, "query") else "query"
            query_limit = event.limit if hasattr(event, "limit") else None

            task_name, task_id, task_description = listener._extract_task_info(event)

            logger.info(
                f"{log_prefix} Event: MemoryQueryStarted | Agent: {agent_name} | Type: {memory_type} | Task: {task_name}"
            )
            logger.info(f"{log_prefix} Query: {query[:200] if len(query) > 200 else query}")

            context_key = f"pending_query_{agent_name}_{memory_type}"
            if listener.job_id not in listener._active_context:
                listener._active_context[listener.job_id] = {}
            listener._active_context[listener.job_id][context_key] = {
                "query": query,
                "limit": query_limit,
                "memory_type": memory_type,
                "task_name": task_name,
                "task_id": task_id,
            }
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_memory_query_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(MemoryQueryCompletedEvent)
    def on_memory_query_completed(source, event):
        """Handle memory query completion events."""
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            source_type = event.source_type if hasattr(event, "source_type") else None
            memory_type = _resolve_memory_type(source_type)

            task_name, task_id, task_description = listener._extract_task_info(event)

            context_key = f"pending_query_{agent_name}_{memory_type}"
            pending_query = None
            if listener.job_id in listener._active_context:
                pending_query = listener._active_context[listener.job_id].pop(context_key, None)

            query = (
                pending_query.get("query") if pending_query
                else (str(event.query) if hasattr(event, "query") else None)
            )

            actual_results = event.results if hasattr(event, "results") else None
            results_count = len(actual_results) if actual_results and hasattr(actual_results, "__len__") else 0

            # Format results for display
            if actual_results is None:
                results_str = "No results"
            elif isinstance(actual_results, list) and len(actual_results) == 0:
                results_str = f"No matching memories found in {memory_type.replace('_', ' ')} memory"
            elif isinstance(actual_results, list) and len(actual_results) > 0:
                formatted_results = []
                for i, result in enumerate(actual_results, 1):
                    if isinstance(result, dict):
                        content = result.get("context", result.get("memory", result.get("data", str(result))))
                        score = result.get("score", result.get("relevance", None))
                        if score is not None:
                            formatted_results.append(f"[{i}] (score: {score:.2f}) {content}")
                        else:
                            formatted_results.append(f"[{i}] {content}")
                    else:
                        formatted_results.append(f"[{i}] {result}")
                results_str = "\n".join(formatted_results)
            else:
                results_str = str(actual_results)

            query_time_ms = event.query_time_ms if hasattr(event, "query_time_ms") else None

            logger.info(
                f"{log_prefix} Event: MemoryQueryCompleted | Agent: {agent_name} | Type: {memory_type} | Task: {task_name}"
            )
            logger.info(
                f"{log_prefix} Results count: {results_count}, Query time: {query_time_ms}ms"
            )
            if results_str and results_str != "No results":
                logger.info(f"{log_prefix} Results (first 300 chars): {results_str[:300]}")

            if results_count == 0 and query:
                query_preview = query[:200] + "..." if len(query) > 200 else query
                output_with_context = f"{results_str}\n\nSearch query: {query_preview}"
            else:
                output_with_context = results_str

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=f"memory_query[{memory_type}]",
                event_type="memory_retrieval",
                output_content=output_with_context,
                extra_data={
                    "operation": "query_completed",
                    "memory_type": memory_type,
                    "backend": "default",
                    "task_name": task_name,
                    "task_id": task_id,
                    "query": query,
                    "results_count": results_count,
                    "query_time_ms": query_time_ms,
                },
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_memory_query_completed: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(MemoryRetrievalCompletedEvent)
    def on_memory_retrieval_completed(source, event):
        """Handle aggregated memory retrieval completion events.

        This event contains the AGGREGATED memory content from ALL memory types
        combined. This is the actual memory that gets injected into the task prompt.
        """
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            task_name, task_id, task_description = listener._extract_task_info(event)

            memory_content = (
                str(event.memory_content)
                if hasattr(event, "memory_content") and event.memory_content
                else None
            )
            retrieval_time_ms = event.retrieval_time_ms if hasattr(event, "retrieval_time_ms") else None

            logger.info(
                f"{log_prefix} Event: MemoryRetrievalCompleted | Agent: {agent_name} | Task: {task_name} | Time: {retrieval_time_ms}ms"
            )
            if memory_content:
                content_preview = memory_content[:500] if len(memory_content) > 500 else memory_content
                logger.info(f"{log_prefix} Aggregated memory content (first 500 chars): {content_preview}")

            if memory_content and memory_content.strip():
                listener._enqueue_trace(
                    event_source=agent_name,
                    event_context="memory_context",
                    event_type="memory_context_retrieved",
                    output_content=memory_content,
                    extra_data={
                        "operation": "context_retrieval_completed",
                        "memory_type": "aggregated",
                        "retrieval_time_ms": retrieval_time_ms,
                        "task_name": task_name,
                        "task_id": task_id,
                        "content_length": len(memory_content),
                    },
                )
            else:
                logger.info(f"{log_prefix} No aggregated memory content to save (empty)")
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_memory_retrieval_completed: {e}",
                exc_info=True,
            )


def register_knowledge_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register knowledge retrieval event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        KnowledgeRetrievalStartedEvent,
        KnowledgeRetrievalCompletedEvent,
    )

    logger.info(f"{log_prefix} Registering knowledge event handlers for CrewAI")

    @crewai_event_bus.on(KnowledgeRetrievalStartedEvent)
    def on_knowledge_retrieval_started(source, event):
        """Handle knowledge retrieval start events."""
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            query = (
                str(event.query) if hasattr(event, "query") else "knowledge query"
            )

            logger.info(
                f"{log_prefix} Event: KnowledgeRetrievalStarted | Agent: {agent_name}"
            )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="knowledge_retrieval",
                event_type="knowledge_retrieval_started",
                output_content=query,
                extra_data={"operation": "retrieval_started", "query": query},
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_knowledge_retrieval_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(KnowledgeRetrievalCompletedEvent)
    def on_knowledge_retrieval_completed(source, event):
        """Handle knowledge retrieval completion events."""
        try:
            agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
            results = (
                str(event.results) if hasattr(event, "results") else "knowledge retrieved"
            )

            logger.info(
                f"{log_prefix} Event: KnowledgeRetrievalCompleted | Agent: {agent_name}"
            )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="knowledge_retrieval",
                event_type="knowledge_retrieval",
                output_content=results,
                extra_data={"operation": "retrieval_completed"},
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_knowledge_retrieval_completed: {e}",
                exc_info=True,
            )
