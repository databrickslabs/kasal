"""ChatMode's 'chat' answer mode.

A single Agent.kickoff_async. The bespoke plan + agent + task generations of
the progressive path add ~3 LLM round-trips with no benefit for what is just
a default assistant answering a message, so this skips them entirely."""

import logging
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from src.core.sse_manager import SSEEvent, sse_manager
from src.utils.user_context import GroupContext

if TYPE_CHECKING:  # imported for the annotation only, no runtime cost
    from src.schemas.crew import CrewStreamingRequest

logger = logging.getLogger(__name__)


class ChatFastPathMixin:
    """ChatMode's 'chat' answer mode.

    A single Agent.kickoff_async. The bespoke plan + agent + task generations of
    the progressive path add ~3 LLM round-trips with no benefit for what is just
    a default assistant answering a message, so this skips them entirely."""

    async def _run_chat_fast_path(
        self,
        request: "CrewStreamingRequest",
        group_context: Optional[GroupContext],
        generation_id: str,
        root_span: Any,
    ) -> None:
        """ChatMode 'chat' fast path — no crew generation.

        Builds a default single assistant + one task carrying the user's request
        (plus any explicitly-attached tools / MCP servers / Agent Bricks
        endpoints from the chat "+" menu — no LLM needed to pick those) and
        auto-executes the light agent. Emits ONLY the terminal
        ``generation_complete`` event the chat UI requires (with ``agents``,
        ``tasks`` and the ``execution_id``); no plan/agent/task cards, since
        nothing was generated. Cuts chat latency from ~3 generation LLM calls +
        the answer down to just the answer.
        """
        from src.schemas.execution import CrewConfig
        from src.services.execution.service import ExecutionService

        user_request = request.original_prompt or request.prompt or ""
        attached_tools = list(getattr(request, "tools", None) or [])

        # A default lightweight assistant + a single task. The config builder
        # injects the attached MCP servers / Agent Bricks endpoints and grounds the
        # task with the user's request, sets execution_type='agent' (light) and carries
        # session_id / memory_workspace_scope — identical to a generated chat agent,
        # only without the LLM generation.
        agent_results = [
            {
                "id": "chat",
                "name": "Assistant",
                "role": "Assistant",
                "goal": "Answer the user's request helpfully, accurately and concisely.",
                "backstory": "You are a helpful AI assistant.",
                "tools": attached_tools,
            }
        ]
        clean_tasks = [
            {
                "id": "chat",
                "name": "Chat response",
                "description": "Respond directly and helpfully to the user's request.",
                "expected_output": "A helpful, complete answer to the user's request.",
                "agent_id": "chat",
                "tools": attached_tools,
                "context": [],
            }
        ]

        gen_complete_data: Dict[str, Any] = {
            "type": "generation_complete",
            "status": "completed",
            "agents": agent_results,
            "tasks": clean_tasks,
            "user_request": user_request,
        }

        try:
            crew_config = CrewConfig(
                **self.build_crew_config_from_generated(
                    request, agent_results, clean_tasks
                )
            )
            exec_result = await ExecutionService(session=None).create_execution(
                config=crew_config,
                background_tasks=None,
                group_context=group_context,
            )
            gen_complete_data["execution_id"] = exec_result.get("execution_id")
            gen_complete_data["run_name"] = exec_result.get("run_name")
            logger.info(
                f"PROGRESSIVE [{generation_id}]: chat fast-path launched "
                f"execution {exec_result.get('execution_id')} (no generation)"
            )
        except Exception as exec_err:
            logger.error(
                f"PROGRESSIVE [{generation_id}]: chat fast-path execute "
                f"failed: {exec_err}"
            )
            logger.error(traceback.format_exc())
            gen_complete_data["execution_error"] = str(exec_err)

        await sse_manager.broadcast_to_job(
            generation_id,
            SSEEvent(
                data=gen_complete_data,
                event="generation_complete",
            ),
        )
        logger.info(f"PROGRESSIVE [{generation_id}]: chat fast-path complete")

        try:
            from src.services.otel_tracing.mlflow_parent_setup import (
                set_root_span_outputs,
            )

            set_root_span_outputs(root_span, gen_complete_data)
        except Exception:
            pass
