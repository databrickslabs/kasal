"""Advanced event handlers: LLM streaming, reasoning, and guardrail events.

Handles LLMStreamChunk, AgentReasoning*, and LLMGuardrail* events.
"""

import logging

from src.engines.crewai.utils.agent_utils import extract_agent_name_from_event

logger = logging.getLogger(__name__)


def register_llm_stream_handler(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register LLM streaming chunk handler (always available).

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import LLMStreamChunkEvent

    @crewai_event_bus.on(LLMStreamChunkEvent)
    def on_llm_stream_chunk(source, event):
        """Handle LLM streaming chunk events."""
        try:
            chunk = str(event.chunk) if hasattr(event, "chunk") else ""

            if not chunk.strip():
                return

            agent_name = extract_agent_name_from_event(event)

            # Aggregate chunks to avoid too many individual traces
            if not hasattr(listener, "_llm_stream_buffer"):
                listener._llm_stream_buffer = {}

            if agent_name not in listener._llm_stream_buffer:
                listener._llm_stream_buffer[agent_name] = []

            listener._llm_stream_buffer[agent_name].append(chunk)

            buffer_content = "".join(listener._llm_stream_buffer[agent_name])
            if len(buffer_content) > 500 or any(
                marker in chunk
                for marker in ["\n\n", "Final Answer:", "Tool:", "Action:"]
            ):
                logger.info(
                    f"{log_prefix} Event: LLMStream | Agent: {agent_name} | Size: {len(buffer_content)}"
                )

                event_type = "llm_stream"
                if any(
                    pattern in buffer_content
                    for pattern in ["Tool:", "Action:", "Using tool:"]
                ):
                    event_type = "llm_tool_stream"

                listener._enqueue_trace(
                    event_source=agent_name,
                    event_context="llm_streaming",
                    event_type=event_type,
                    output_content=buffer_content[:1000],
                    extra_data={
                        "stream_size": len(buffer_content),
                        "chunk_count": len(listener._llm_stream_buffer[agent_name]),
                    },
                )

                listener._llm_stream_buffer[agent_name] = []

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_llm_stream_chunk: {e}", exc_info=True
            )


def register_reasoning_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register agent reasoning event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        AgentReasoningStartedEvent,
        AgentReasoningCompletedEvent,
        AgentReasoningFailedEvent,
    )

    logger.info(f"{log_prefix} Registering reasoning event handlers for CrewAI 0.177")

    @crewai_event_bus.on(AgentReasoningStartedEvent)
    def on_reasoning_started(source, event):
        """Handle agent reasoning start events."""
        try:
            agent_name = (
                event.agent_role if hasattr(event, "agent_role") else "Unknown Agent"
            )
            task_id = event.task_id if hasattr(event, "task_id") else None

            logger.info(
                f"{log_prefix} Event: ReasoningStarted | Agent: {agent_name}"
            )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="reasoning",
                event_type="agent_reasoning",
                output_content=f"Agent {agent_name} starting reasoning process",
                extra_data={
                    "agent_role": agent_name,
                    "task_id": task_id,
                    "operation": "reasoning_started",
                },
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_reasoning_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(AgentReasoningCompletedEvent)
    def on_reasoning_completed(source, event):
        """Handle agent reasoning completion events."""
        try:
            agent_name = (
                event.agent_role if hasattr(event, "agent_role") else "Unknown Agent"
            )
            task_id = event.task_id if hasattr(event, "task_id") else None
            plan = str(event.plan) if hasattr(event, "plan") else None
            ready = event.ready if hasattr(event, "ready") else False

            logger.info(
                f"{log_prefix} Event: ReasoningCompleted | Agent: {agent_name} | Ready: {ready}"
            )

            output_content = (
                plan if plan else f"Agent {agent_name} completed reasoning"
            )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="reasoning",
                event_type="agent_reasoning",
                output_content=output_content,
                extra_data={
                    "agent_role": agent_name,
                    "task_id": task_id,
                    "ready": ready,
                    "operation": "reasoning_completed",
                },
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_reasoning_completed: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(AgentReasoningFailedEvent)
    def on_reasoning_failed(source, event):
        """Handle agent reasoning failure events."""
        try:
            agent_name = (
                event.agent_role if hasattr(event, "agent_role") else "Unknown Agent"
            )
            task_id = event.task_id if hasattr(event, "task_id") else None
            error = (
                str(event.error) if hasattr(event, "error") else "Unknown error"
            )

            logger.error(
                f"{log_prefix} Event: ReasoningFailed | Agent: {agent_name} | Error: {error}"
            )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="reasoning",
                event_type="agent_reasoning_error",
                output_content=f"Reasoning failed: {error}",
                extra_data={
                    "agent_role": agent_name,
                    "task_id": task_id,
                    "error": error,
                    "operation": "reasoning_failed",
                },
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_reasoning_failed: {e}", exc_info=True
            )


def register_guardrail_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register LLM guardrail event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        LLMGuardrailStartedEvent,
        LLMGuardrailCompletedEvent,
        LLMGuardrailFailedEvent,
    )

    logger.info(f"{log_prefix} Registering LLM guardrail event handlers for CrewAI 0.177")

    @crewai_event_bus.on(LLMGuardrailStartedEvent)
    def on_guardrail_started(source, event):
        """Handle LLM guardrail start events."""
        try:
            guardrail_obj = event.guardrail if hasattr(event, "guardrail") else None
            guardrail_name = str(guardrail_obj) if guardrail_obj else "guardrail"
            guardrail_description = None

            if guardrail_obj:
                if hasattr(guardrail_obj, "description"):
                    guardrail_description = guardrail_obj.description
                elif hasattr(guardrail_obj, "guardrail") and hasattr(guardrail_obj.guardrail, "config"):
                    config = guardrail_obj.guardrail.config
                    if isinstance(config, dict):
                        guardrail_description = config.get("description", config.get("type", ""))

            retry_count = event.retry_count if hasattr(event, "retry_count") else 0

            task_name = event.task_name if hasattr(event, "task_name") else None
            task_id = event.task_id if hasattr(event, "task_id") else None

            agent_name = "System"
            if hasattr(event, "agent_role") and event.agent_role:
                agent_name = event.agent_role
            elif hasattr(source, "role"):
                agent_name = source.role

            logger.info(
                f"{log_prefix} Event: GuardrailStarted | Task: {task_name} | Guardrail: {guardrail_name} | Retry: {retry_count}"
            )

            extra_data = {
                "guardrail": guardrail_name,
                "retry_count": retry_count,
                "operation": "guardrail_started",
            }

            if guardrail_description:
                extra_data["guardrail_description"] = guardrail_description
            if task_name:
                extra_data["task_name"] = task_name
            if task_id:
                extra_data["task_id"] = task_id

            output_content = "Starting guardrail validation"
            if task_name:
                output_content = f"Validating output for task: {task_name}"
            if guardrail_description:
                output_content += f"\n\nValidation Criteria:\n{guardrail_description}"
            if retry_count > 0:
                output_content += f"\n\n(Retry attempt #{retry_count})"

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="guardrail_check",
                event_type="llm_guardrail",
                output_content=output_content,
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_guardrail_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(LLMGuardrailCompletedEvent)
    def on_guardrail_completed(source, event):
        """Handle LLM guardrail completion events."""
        try:
            success = event.success if hasattr(event, "success") else False
            result = event.result if hasattr(event, "result") else None
            error = str(event.error) if hasattr(event, "error") and event.error else None
            retry_count = event.retry_count if hasattr(event, "retry_count") else 0

            task_name = event.task_name if hasattr(event, "task_name") else None
            task_id = event.task_id if hasattr(event, "task_id") else None

            agent_name = "System"
            if hasattr(event, "agent_role") and event.agent_role:
                agent_name = event.agent_role
            elif hasattr(source, "role"):
                agent_name = source.role

            status = "PASSED" if success else "FAILED"
            logger.info(
                f"{log_prefix} Event: GuardrailCompleted | Task: {task_name} | Status: {status} | Retries: {retry_count}"
            )

            extra_data = {
                "success": success,
                "retry_count": retry_count,
                "operation": "guardrail_completed",
                "status": status,
            }

            if error:
                extra_data["error"] = error
            if task_name:
                extra_data["task_name"] = task_name
            if task_id:
                extra_data["task_id"] = task_id

            output_lines = []
            if task_name:
                output_lines.append(f"Guardrail Validation Result for: {task_name}")
            else:
                output_lines.append("Guardrail Validation Result")
            output_lines.append(f"Status: {status}")

            if result is not None:
                if isinstance(result, tuple) and len(result) >= 2:
                    output_lines.append(f"\nValidation Output:")
                    output_lines.append(f"  Valid: {result[0]}")
                    output_lines.append(f"  Message: {result[1]}")
                    extra_data["validation_valid"] = result[0]
                    extra_data["validation_message"] = str(result[1])
                else:
                    result_str = str(result)
                    if result_str and result_str != "None":
                        output_lines.append(f"\nResult Details:")
                        output_lines.append(result_str)
                        extra_data["result"] = result_str

            if error:
                output_lines.append(f"\nError: {error}")
            if retry_count > 0:
                output_lines.append(f"\n(After {retry_count} retry attempts)")

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="guardrail_check",
                event_type="llm_guardrail",
                output_content="\n".join(output_lines),
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_guardrail_completed: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(LLMGuardrailFailedEvent)
    def on_guardrail_failed(source, event):
        """Handle LLM guardrail failure events (technical errors)."""
        try:
            error = str(event.error) if hasattr(event, "error") else "Unknown error"
            retry_count = event.retry_count if hasattr(event, "retry_count") else 0

            task_name = event.task_name if hasattr(event, "task_name") else None
            task_id = event.task_id if hasattr(event, "task_id") else None

            agent_name = "unknown_agent"
            if hasattr(event, "agent_role") and event.agent_role:
                agent_name = event.agent_role
            elif hasattr(event, "from_agent") and event.from_agent:
                agent_name = getattr(event.from_agent, "role", "unknown_agent")

            logger.error(
                f"{log_prefix} Event: GuardrailFailed | Task: {task_name} | Agent: {agent_name} | Error: {error} | Retries: {retry_count}"
            )

            extra_data = {
                "error": error,
                "retry_count": retry_count,
                "operation": "guardrail_failed",
                "status": "ERROR",
            }

            if task_name:
                extra_data["task_name"] = task_name
            if task_id:
                extra_data["task_id"] = task_id

            output_lines = []
            if task_name:
                output_lines.append(f"Guardrail Error for task: {task_name}")
            else:
                output_lines.append("Guardrail Error")
            output_lines.append("Status: ERROR")
            output_lines.append(f"\nError Details: {error}")
            if retry_count > 0:
                output_lines.append(f"\n(After {retry_count} retry attempts)")

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="guardrail_check",
                event_type="llm_guardrail",
                output_content="\n".join(output_lines),
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_guardrail_failed: {e}",
                exc_info=True,
            )
