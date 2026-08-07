"""Streaming crew generation (``POST /crew/create-crew-streaming``).

Plan first, then agents, then tasks, broadcasting SSE as each entity lands.
This is the path BOTH the canvas chat input and ChatMode use.

It runs as a background task after the HTTP response is already sent, so the
request-scoped session is closed by then — every database touch here opens a
session of its own, on a PRIVATE connection."""

import logging
import os
import re
import traceback
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from src.core.exceptions import BadRequestError, KasalError
from src.core.sse_manager import SSEEvent, sse_manager
from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.repositories.log_repository import LLMLogRepository
from src.schemas.crew import (
    CrewGenerationRequest,
    CrewGenerationResponse,
    CrewPlan,
    CrewStreamingRequest,
)
from src.schemas.task_generation import Agent as TaskGenAgent
from src.schemas.task_generation import TaskGenerationRequest
from src.services.catalog.templates import TemplateService
from src.services.execution.logs.llm_log_service import LLMLogService
from src.services.generation.agents import AgentGenerationService
from src.services.generation.tasks import TaskGenerationService
from src.services.llm.manager import LLMManager
from src.services.tools.tool_service import ToolService
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class ProgressiveGenerationMixin:
    """Streaming crew generation (``POST /crew/create-crew-streaming``).

    Plan first, then agents, then tasks, broadcasting SSE as each entity lands.
    This is the path BOTH the canvas chat input and ChatMode use.

    It runs as a background task after the HTTP response is already sent, so the
    request-scoped session is closed by then — every database touch here opens a
    session of its own, on a PRIVATE connection."""

    async def create_crew_progressive(
        self,
        request: CrewStreamingRequest,
        group_context: Optional[GroupContext],
        generation_id: str,
        mlflow_enabled: bool = False,
    ) -> None:
        """
        Progressively generate a crew, broadcasting SSE events as each entity
        is created.

        Phase 1 — Plan: Fast LLM call returning agent names/roles + task names.
        Phase 2 — Agent details: Reuse AgentGenerationService per agent.
        Phase 3 — Task details: Reuse TaskGenerationService per task.

        IMPORTANT: This method runs as a background task after the HTTP response
        has already been sent. The request-scoped DB session is closed by then,
        so all database work uses an independent session created here.
        """
        from contextlib import asynccontextmanager, nullcontext

        from src.db.database_router import (
            get_lakebase_config_from_db,
            is_lakebase_enabled,
        )
        from src.db.lakebase_session import get_lakebase_session
        from src.db.session import (
            detach_request_session,
            get_isolated_db_session,
        )

        # This runs via asyncio.create_task, so it inherited a COPY of the
        # dispatch request's context — including the request-scoped DB session,
        # which FastAPI has already closed. Detach it so every
        # routed_scoped_session() below (notably the model-config read inside
        # LLMManager.configure_kasal_llm during planning) opens a fresh session
        # instead of failing with "Cannot operate on a closed database".
        detach_request_session()

        if mlflow_enabled:
            try:
                from src.services.mlflow.tracing import start_root_trace

                trace_ctx = start_root_trace(
                    "crew_generation",
                    inputs={
                        "prompt": request.prompt,
                        "model": request.model or "default",
                    },
                )
            except Exception:
                trace_ctx = nullcontext()
        else:
            trace_ctx = nullcontext()

        with trace_ctx as root_span:
            try:
                # ── ChatMode 'chat' fast path ─────────────────────────────
                # 'chat' answer mode runs a SINGLE Agent.kickoff_async. The
                # bespoke plan + agent + task LLM generations below add ~3 LLM
                # round-trips with no benefit for what is just a default
                # assistant answering the user's message. Skip them entirely:
                # synthesize a default agent + a task from the raw prompt and go
                # straight to auto-execute. 'research'/'deep' still generate a
                # full crew (they need the plan/agents/tasks).
                if (
                    getattr(request, "chat_mode_type", "chat") or "chat"
                ) == "chat" and getattr(request, "auto_execute", False):
                    await self._run_chat_fast_path(
                        request, group_context, generation_id, root_span
                    )
                    return

                model = request.model or os.getenv(
                    "CREW_MODEL", "databricks-gpt-5-3-codex"
                )

                # ── Compute caps BEFORE planning so the LLM knows the limits ──
                # Caps are UPPER BOUNDS, not predictions: the PLAN LLM decides the
                # actual counts by mapping the user's distinct actions to tasks
                # (the generate_crew_plan template + few-shots own that logic, and
                # "use the minimum agents needed" keeps simple prompts small).
                # NEVER derive ceilings from keyword heuristics here — a hardcoded
                # verb lexicon capped "list data products, understand the
                # contracts, …" to ONE task because none of its verbs were in the
                # list (the "always 1 agent / 1 task" regression vs v1.3.0). Only
                # an EXPLICIT numeric request tightens/raises the caps.
                ABSOLUTE_MAX_AGENTS = 10
                ABSOLUTE_MAX_TASKS = 10
                # Defaults mirror the generate_crew template LIMITS ("at most 3
                # agents and 6 tasks unless the user explicitly asks for more").
                DEFAULT_MAX_TASKS = 6
                DEFAULT_MAX_AGENTS = 3

                # Check BOTH the (possibly LLM-rewritten) prompt AND the original
                # user message for explicit count requests.
                user_prompt = (request.prompt or "").lower()
                original_prompt = (
                    getattr(request, "original_prompt", None) or ""
                ).lower()
                combined = user_prompt + " " + original_prompt
                # Bounded gap (≤3 words, e.g. "4 specialized research agents") and a
                # lookahead so a count can't be claimed ACROSS the other noun —
                # "4 agents and 8 tasks" must read tasks=8, not greedily tasks=4.
                # Spelled-out counts matter as much as digits: the dispatcher's
                # prompt rewrite turns "create 4 agents" into "four specialized
                # agents", and a digit-only pattern silently dropped the user's
                # count back to DEFAULT_MAX_AGENTS.
                _NUMBER_WORDS = {
                    "one": 1,
                    "two": 2,
                    "three": 3,
                    "four": 4,
                    "five": 5,
                    "six": 6,
                    "seven": 7,
                    "eight": 8,
                    "nine": 9,
                    "ten": 10,
                }
                _num = r"\d+|" + "|".join(_NUMBER_WORDS)
                _count_re = (
                    r"\b(%s)\s+(?:(?!agents?\b|tasks?\b)\w+\s+){0,3}%%s\b" % _num
                )

                def _parse_count(noun: str) -> Optional[int]:
                    m = re.search(_count_re % noun, combined)
                    if not m:
                        return None
                    token = m.group(1)
                    return int(token) if token.isdigit() else _NUMBER_WORDS[token]

                requested_tasks = _parse_count("tasks?")
                requested_agents = _parse_count("agents?")

                if requested_tasks is not None:
                    max_tasks = min(requested_tasks, ABSOLUTE_MAX_TASKS)
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: User requested {max_tasks} tasks"
                    )
                else:
                    max_tasks = DEFAULT_MAX_TASKS
                if requested_agents is not None:
                    max_agents = min(requested_agents, ABSOLUTE_MAX_AGENTS)
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: User requested {max_agents} agents"
                    )
                    # An explicit agent count can exceed the default task ceiling
                    # ("5 agents" with no task count): every agent needs a task of
                    # its own or the orphan sweep below deletes it again.
                    if requested_tasks is None and max_agents > max_tasks:
                        max_tasks = min(max_agents, ABSOLUTE_MAX_TASKS)
                else:
                    max_agents = min(DEFAULT_MAX_AGENTS, max_tasks)

                # Chat (light agent) ANSWER mode runs a SINGLE Agent.kickoff_async —
                # force exactly one agent + one task so there is one agent to kick
                # off and one grounded task description to use as its prompt. This
                # applies ONLY when this generation IS the chat answer run
                # (auto_execute) — that path normally short-circuits into
                # _run_chat_fast_path above, so this is a defensive guard. A
                # GENERATE-ONLY request (the AgentBuilder canvas chat, which leaves
                # auto_execute False and renders the plan as nodes) must plan the
                # full crew like research/deep: chat_mode_type defaults to "chat"
                # in the schema, and clamping on it alone collapsed every canvas
                # generation to 1 agent / 1 task (regression vs v1.3.0).
                if (
                    getattr(request, "chat_mode_type", "chat") or "chat"
                ) == "chat" and getattr(request, "auto_execute", False):
                    max_agents = 1
                    max_tasks = 1
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: chat (light agent) answer mode — capping to 1 agent / 1 task"
                    )

                # ── Phase 1: Planning (LLM only, no DB writes) ───────────
                # Inject the computed cap into the request so the LLM generates
                # the correct number from the start (instead of generating many
                # and truncating, which loses the user's actual goal).
                logger.info(
                    f"PROGRESSIVE [{generation_id}]: Phase 1 — Planning (max {max_agents} agents, {max_tasks} tasks)"
                )

                # Crews this workspace already built and a human marked good.
                # The PLAN call is where they belong: it decides the shape —
                # how many agents, how the work splits — which is exactly what a
                # past crew is evidence about. Injecting them into the later
                # per-agent/per-task calls would arrive after those decisions
                # were already made. Empty and inert until someone curates.
                recipe_decision = await self._recipe_decision_isolated(
                    request, group_context
                )
                if recipe_decision is not None and recipe_decision.injected_labels:
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: reusing "
                        f"{len(recipe_decision.injected_labels)} curated recipe(s)"
                    )

                try:
                    plan = await self._generate_crew_plan(
                        request,
                        group_context,
                        model,
                        max_agents=max_agents,
                        max_tasks=max_tasks,
                        explicit_agents=(
                            max_agents if requested_agents is not None else None
                        ),
                        exemplars=(recipe_decision.text if recipe_decision else ""),
                    )
                except Exception as e:
                    logger.error(f"PROGRESSIVE [{generation_id}]: Planning failed: {e}")
                    await sse_manager.broadcast_to_job(
                        generation_id,
                        SSEEvent(
                            data={"type": "generation_failed", "error": str(e)},
                            event="generation_failed",
                        ),
                    )
                    return

                plan_agents = plan.get("agents", [])
                plan_tasks = plan.get("tasks", [])
                process_type = plan.get("process_type", "sequential")
                complexity = plan.get("complexity", "standard")

                # Safety net: if LLM still exceeded caps, truncate as last resort.
                # For single-agent (max=1), keep the LAST agent/task since in a
                # sequential pipeline the final step produces the user's deliverable
                # (e.g., dashboard builder > scraper). For multi-agent, keep the first N.
                if len(plan_agents) > max_agents:
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Truncating agents from "
                        f"{len(plan_agents)} to {max_agents}"
                    )
                    if max_agents == 1 and process_type == "sequential":
                        plan_agents = plan_agents[-1:]
                    else:
                        plan_agents = plan_agents[:max_agents]
                if len(plan_tasks) > max_tasks:
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Truncating tasks from "
                        f"{len(plan_tasks)} to {max_tasks}"
                    )
                    if max_tasks == 1 and process_type == "sequential":
                        plan_tasks = plan_tasks[-1:]
                    else:
                        plan_tasks = plan_tasks[:max_tasks]
                if not plan_agents:
                    await sse_manager.broadcast_to_job(
                        generation_id,
                        SSEEvent(
                            data={
                                "type": "generation_failed",
                                "error": "Plan returned no agents",
                            },
                            event="generation_failed",
                        ),
                    )
                    return

                # Re-assign orphaned tasks to valid agents and clean stale context refs
                valid_agent_names = {a.get("name") for a in plan_agents}
                valid_task_names = {t.get("name") for t in plan_tasks}
                for task in plan_tasks:
                    if task.get("assigned_agent") not in valid_agent_names:
                        task["assigned_agent"] = plan_agents[0].get("name", "")
                    # Remove context references to tasks that were truncated
                    if task.get("context"):
                        task["context"] = [
                            c for c in task["context"] if c in valid_task_names
                        ]

                # Remove orphan agents that have no tasks assigned
                assigned_agents = {t.get("assigned_agent") for t in plan_tasks}
                orphan_agents = [
                    a for a in plan_agents if a.get("name") not in assigned_agents
                ]
                if orphan_agents:
                    orphan_names = [a.get("name") for a in orphan_agents]
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Removing "
                        f"{len(orphan_agents)} orphan agent(s) with no tasks: "
                        f"{orphan_names}"
                    )
                    plan_agents = [
                        a for a in plan_agents if a.get("name") in assigned_agents
                    ]

                # ── Enforce sequential dependency chain ────────────────
                if process_type == "sequential":
                    for i, task in enumerate(plan_tasks):
                        if i > 0 and not task.get("context"):
                            prev_name = plan_tasks[i - 1].get("name", "")
                            if prev_name:
                                task["context"] = [prev_name]
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Auto-chained "
                                    f"task '{task.get('name')}' → depends on '{prev_name}'"
                                )

                logger.info(
                    f"PROGRESSIVE [{generation_id}]: Plan — complexity={complexity}, "
                    f"process={process_type}, {len(plan_agents)} agents, {len(plan_tasks)} tasks"
                )

                # Broadcast plan_ready
                await sse_manager.broadcast_to_job(
                    generation_id,
                    SSEEvent(
                        data={
                            "type": "plan_ready",
                            "agents": plan_agents,
                            "tasks": plan_tasks,
                            "process_type": process_type,
                            "complexity": complexity,
                        },
                        event="plan_ready",
                    ),
                )

                # ── Phases 2-4: DB writes use an independent session ──────
                # The request-scoped session is already closed by FastAPI DI,
                # so we create a standalone session for all database operations.
                # IMPORTANT: Route to Lakebase when enabled, matching
                # get_smart_db_session() so reads/writes hit the same DB.
                #
                # On SQLite, use a PRIVATE connection (get_isolated_db_session)
                # rather than the shared StaticPool one. This flow commits an agent,
                # then makes a seconds-long LLM call, then inserts a task referencing
                # it — and on the shared connection a concurrent request's
                # commit/rollback in that window can silently discard the committed
                # agent, making the task's agent_id FK fail. A private connection is
                # immune to that interference. (Lakebase/Postgres pooled checkouts
                # are already per-connection, so the helper falls through to them.)
                if await is_lakebase_enabled():
                    lb_config = await get_lakebase_config_from_db()
                    lb_instance = (lb_config or {}).get(
                        "instance_name"
                    ) or os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
                    _session_ctx = get_lakebase_session(lb_instance)
                else:
                    _session_ctx = get_isolated_db_session()

                async with _session_ctx as session:
                    try:
                        repo = CrewGeneratorRepository(session)
                        agent_gen_service = AgentGenerationService(session)
                        task_gen_service = TaskGenerationService(session)

                        # ── Resolve workspace tools ───────────────────────
                        tool_name_to_id_map: Dict[str, str] = {}
                        available_tools_for_llm: List[Dict[str, str]] = []
                        if request.tools:
                            try:
                                tool_service = ToolService(session)
                                tools_with_details = await self._get_tool_details(
                                    request.tools, tool_service
                                )
                                tool_name_to_id_map = self._create_tool_name_to_id_map(
                                    tools_with_details
                                )
                                available_tools_for_llm = [
                                    {
                                        "name": t.get("title") or t.get("name", ""),
                                        "description": t.get("description", ""),
                                    }
                                    for t in tools_with_details
                                    if t.get("title") or t.get("name")
                                ]
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Resolved "
                                    f"{len(available_tools_for_llm)} workspace tools"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"PROGRESSIVE [{generation_id}]: "
                                    f"Tool resolution failed, continuing without tools: {e}"
                                )

                        # ── Build reverse map: tool_id → tool_title ──────
                        tool_id_to_title: Dict[str, str] = {
                            v: k for k, v in tool_name_to_id_map.items()
                        }

                        # ── Group tasks by assigned agent for interleaved generation ──
                        tasks_by_agent: Dict[str, List[Dict]] = defaultdict(list)
                        unassigned_tasks: List[Dict] = []
                        for task_plan in plan_tasks:
                            assigned = task_plan.get("assigned_agent", "")
                            if assigned:
                                tasks_by_agent[assigned.lower()].append(task_plan)
                            else:
                                unassigned_tasks.append(task_plan)

                        # ── Interleaved Phase: Agent → its Tasks → next Agent → its Tasks ──
                        logger.info(
                            f"PROGRESSIVE [{generation_id}]: Interleaved agent→task generation"
                        )
                        agent_results: List[Dict[str, Any]] = []
                        task_results: List[Dict[str, Any]] = []
                        # Plan-task name -> tool ids it received, so a task cannot
                        # re-take a tool one of its dependencies already holds.
                        tools_by_task: Dict[str, List[Any]] = {}
                        global_task_index = 0

                        # If no persistent memory backend is configured, disable memory on
                        # generated agents (otherwise memory silently writes to ephemeral
                        # local storage that doesn't survive in a deployed app).
                        has_memory = await self._has_persistent_memory_backend(
                            session, group_context
                        )
                        logger.info(
                            f"PROGRESSIVE [{generation_id}]: persistent memory backend present = {has_memory}"
                        )

                        for i, agent_plan in enumerate(plan_agents):
                            agent_name = agent_plan.get("name", f"Agent {i+1}")
                            agent_role = agent_plan.get("role", "Specialist")
                            try:
                                prompt = (
                                    f"Create an agent named '{agent_name}' with role "
                                    f"'{agent_role}' for a crew that: {request.prompt}"
                                )
                                agent_config = await agent_gen_service.generate_agent(
                                    prompt_text=prompt,
                                    model=model,
                                    tools=[],
                                    group_context=group_context,
                                )

                                # Tools are assigned at the task level, not agent level
                                agent_tool_ids: List[str] = []

                                agent_data = {
                                    "name": agent_config.get("name", agent_name),
                                    "role": agent_config.get("role", agent_role),
                                    "goal": agent_config.get("goal", ""),
                                    "backstory": agent_config.get("backstory", ""),
                                    "llm": model,
                                    "tools": agent_tool_ids,
                                }
                                # No persistent memory backend → disable memory so the
                                # crew doesn't write to ephemeral local storage.
                                if not has_memory:
                                    agent_data["memory"] = False
                                adv = agent_config.get("advanced_config", {})
                                for key in (
                                    "function_calling_llm",
                                    "max_iter",
                                    "max_rpm",
                                    "verbose",
                                    "allow_delegation",
                                    "cache",
                                    "code_execution_mode",
                                    "max_retry_limit",
                                    "use_system_prompt",
                                    "respect_context_window",
                                ):
                                    if key in adv:
                                        agent_data[key] = adv[key]

                                saved = await repo.create_single_agent(
                                    agent_data, group_context
                                )
                                # Commit each agent so it exists for FK constraints
                                await session.commit()
                                agent_results.append(saved)

                                await sse_manager.broadcast_to_job(
                                    generation_id,
                                    SSEEvent(
                                        data={
                                            "type": "agent_detail",
                                            "index": i,
                                            "agent": saved,
                                        },
                                        event="agent_detail",
                                    ),
                                )
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Agent {i+1}/{len(plan_agents)} done — {saved.get('name')}"
                                )

                            except Exception as e:
                                logger.error(
                                    f"PROGRESSIVE [{generation_id}]: Agent '{agent_name}' failed: {e}"
                                )
                                await session.rollback()
                                await sse_manager.broadcast_to_job(
                                    generation_id,
                                    SSEEvent(
                                        data={
                                            "type": "entity_error",
                                            "index": i,
                                            "entity_type": "agent",
                                            "name": agent_name,
                                            "error": str(e),
                                        },
                                        event="entity_error",
                                    ),
                                )
                                continue

                            # ── Generate tasks assigned to this agent ──────
                            agent_tasks = tasks_by_agent.get(agent_name.lower(), [])
                            for task_plan in agent_tasks:
                                task_name = task_plan.get(
                                    "name", f"Task {global_task_index+1}"
                                )
                                try:
                                    agent_context = self._find_agent_context(
                                        task_plan, agent_results
                                    )

                                    task_request = TaskGenerationRequest(
                                        text=self._task_brief(
                                            task_plan, task_name, request.prompt
                                        ),
                                        model=model,
                                        agent=agent_context,
                                        available_tools=available_tools_for_llm or None,
                                    )
                                    task_response = (
                                        await task_gen_service.generate_task(
                                            task_request, group_context
                                        )
                                    )

                                    agent_id = self._resolve_agent_id(
                                        task_plan, agent_results
                                    )

                                    task_tool_ids = self._task_tool_ids(
                                        task_response.tools,
                                        tool_name_to_id_map,
                                        self._ancestor_tool_ids(
                                            task_plan, plan_tasks, tools_by_task
                                        ),
                                    )
                                    tools_by_task[str(task_plan.get("name") or "")] = (
                                        task_tool_ids
                                    )

                                    task_data = {
                                        "name": task_response.name,
                                        "description": task_response.description,
                                        "expected_output": task_response.expected_output,
                                        "tools": task_tool_ids,
                                        "tool_configs": {},
                                        "async_execution": False,
                                        "human_input": False,
                                        "llm_guardrail": (
                                            task_response.llm_guardrail.model_dump()
                                            if task_response.llm_guardrail
                                            else None
                                        ),
                                    }

                                    task_saved = await repo.create_single_task(
                                        task_data, agent_id, group_context
                                    )
                                    await session.commit()
                                    task_results.append(
                                        {**task_saved, "_plan": task_plan}
                                    )

                                    await sse_manager.broadcast_to_job(
                                        generation_id,
                                        SSEEvent(
                                            data={
                                                "type": "task_detail",
                                                "index": global_task_index,
                                                "task": task_saved,
                                            },
                                            event="task_detail",
                                        ),
                                    )
                                    logger.info(
                                        f"PROGRESSIVE [{generation_id}]: Task {global_task_index+1}/{len(plan_tasks)} done — {task_saved.get('name')}"
                                    )

                                    # ── Detect GenieTool and suggest space ──
                                    needs_genie_config = any(
                                        tool_id_to_title.get(tid) == "GenieTool"
                                        for tid in task_tool_ids
                                    )
                                    if needs_genie_config:
                                        suggested = await self._suggest_genie_space(
                                            task_name=task_saved["name"],
                                            task_description=task_saved.get(
                                                "description", ""
                                            ),
                                        )
                                        await sse_manager.broadcast_to_job(
                                            generation_id,
                                            SSEEvent(
                                                data={
                                                    "type": "tool_config_needed",
                                                    "task_id": task_saved["id"],
                                                    "task_name": task_saved["name"],
                                                    "tool_name": "GenieTool",
                                                    "config_fields": ["spaceId"],
                                                    "suggested_space": suggested,
                                                },
                                                event="tool_config_needed",
                                            ),
                                        )

                                except Exception as e:
                                    logger.error(
                                        f"PROGRESSIVE [{generation_id}]: Task '{task_name}' failed: {e}"
                                    )
                                    await session.rollback()
                                    await sse_manager.broadcast_to_job(
                                        generation_id,
                                        SSEEvent(
                                            data={
                                                "type": "entity_error",
                                                "index": global_task_index,
                                                "entity_type": "task",
                                                "name": task_name,
                                                "error": str(e),
                                            },
                                            event="entity_error",
                                        ),
                                    )
                                global_task_index += 1

                        # ── Handle unassigned tasks at the end ──────────
                        for task_plan in unassigned_tasks:
                            task_name = task_plan.get(
                                "name", f"Task {global_task_index+1}"
                            )
                            try:
                                agent_context = self._find_agent_context(
                                    task_plan, agent_results
                                )

                                task_request = TaskGenerationRequest(
                                    text=self._task_brief(
                                        task_plan, task_name, request.prompt
                                    ),
                                    model=model,
                                    agent=agent_context,
                                    available_tools=available_tools_for_llm or None,
                                )
                                task_response = await task_gen_service.generate_task(
                                    task_request, group_context
                                )

                                agent_id = self._resolve_agent_id(
                                    task_plan, agent_results
                                )

                                task_tool_ids = self._task_tool_ids(
                                    task_response.tools,
                                    tool_name_to_id_map,
                                    self._ancestor_tool_ids(
                                        task_plan, plan_tasks, tools_by_task
                                    ),
                                )
                                tools_by_task[str(task_plan.get("name") or "")] = (
                                    task_tool_ids
                                )

                                task_data = {
                                    "name": task_response.name,
                                    "description": task_response.description,
                                    "expected_output": task_response.expected_output,
                                    "tools": task_tool_ids,
                                    "tool_configs": {},
                                    "async_execution": False,
                                    "human_input": False,
                                    "llm_guardrail": (
                                        task_response.llm_guardrail.model_dump()
                                        if task_response.llm_guardrail
                                        else None
                                    ),
                                }

                                task_saved = await repo.create_single_task(
                                    task_data, agent_id, group_context
                                )
                                await session.commit()
                                task_results.append({**task_saved, "_plan": task_plan})

                                await sse_manager.broadcast_to_job(
                                    generation_id,
                                    SSEEvent(
                                        data={
                                            "type": "task_detail",
                                            "index": global_task_index,
                                            "task": task_saved,
                                        },
                                        event="task_detail",
                                    ),
                                )
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Task {global_task_index+1}/{len(plan_tasks)} done — {task_saved.get('name')}"
                                )

                                # ── Detect GenieTool and suggest space ──
                                needs_genie_config = any(
                                    tool_id_to_title.get(tid) == "GenieTool"
                                    for tid in task_tool_ids
                                )
                                if needs_genie_config:
                                    suggested = await self._suggest_genie_space(
                                        task_name=task_saved["name"],
                                        task_description=task_saved.get(
                                            "description", ""
                                        ),
                                    )
                                    await sse_manager.broadcast_to_job(
                                        generation_id,
                                        SSEEvent(
                                            data={
                                                "type": "tool_config_needed",
                                                "task_id": task_saved["id"],
                                                "task_name": task_saved["name"],
                                                "tool_name": "GenieTool",
                                                "config_fields": ["spaceId"],
                                                "suggested_space": suggested,
                                            },
                                            event="tool_config_needed",
                                        ),
                                    )

                            except Exception as e:
                                logger.error(
                                    f"PROGRESSIVE [{generation_id}]: Task '{task_name}' failed: {e}"
                                )
                                await session.rollback()
                                await sse_manager.broadcast_to_job(
                                    generation_id,
                                    SSEEvent(
                                        data={
                                            "type": "entity_error",
                                            "index": global_task_index,
                                            "entity_type": "task",
                                            "name": task_name,
                                            "error": str(e),
                                        },
                                        event="entity_error",
                                    ),
                                )
                            global_task_index += 1

                        # ── Fallback: synthesize tasks when generation produced none ──
                        # Per-task LLM generation occasionally fails for EVERY task
                        # (small models returning malformed JSON). Reaching save /
                        # auto-execute with agents but ZERO tasks dies in crew
                        # preparation ("Failed to prepare crew"). Synthesize a minimal
                        # task per planned task — from the plan name + the user's
                        # request — so the crew stays runnable.
                        if not task_results and plan_tasks and agent_results:
                            logger.warning(
                                f"PROGRESSIVE [{generation_id}]: all task generation failed — "
                                f"synthesizing {len(plan_tasks)} task(s) from the plan"
                            )
                            for task_plan in plan_tasks:
                                task_name = task_plan.get(
                                    "name", f"Task {global_task_index + 1}"
                                )
                                try:
                                    agent_id = self._resolve_agent_id(
                                        task_plan, agent_results
                                    )
                                    task_data = {
                                        "name": task_name,
                                        "description": (
                                            f"{task_name} — complete this task for a crew that: "
                                            f"{request.prompt}"
                                        ),
                                        "expected_output": "A complete, well-structured result for this task.",
                                        "tools": [],
                                        "tool_configs": {},
                                        "async_execution": False,
                                        "human_input": False,
                                        "llm_guardrail": None,
                                    }
                                    task_saved = await repo.create_single_task(
                                        task_data, agent_id, group_context
                                    )
                                    await session.commit()
                                    task_results.append(
                                        {**task_saved, "_plan": task_plan}
                                    )
                                    await sse_manager.broadcast_to_job(
                                        generation_id,
                                        SSEEvent(
                                            data={
                                                "type": "task_detail",
                                                "index": global_task_index,
                                                "task": task_saved,
                                            },
                                            event="task_detail",
                                        ),
                                    )
                                    logger.info(
                                        f"PROGRESSIVE [{generation_id}]: Synthesized fallback task — "
                                        f"{task_saved.get('name')}"
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"PROGRESSIVE [{generation_id}]: Fallback task '{task_name}' failed: {e}"
                                    )
                                    await session.rollback()
                                    await sse_manager.broadcast_to_job(
                                        generation_id,
                                        SSEEvent(
                                            data={
                                                "type": "entity_error",
                                                "index": global_task_index,
                                                "entity_type": "task",
                                                "name": task_name,
                                                "error": str(e),
                                            },
                                            event="entity_error",
                                        ),
                                    )
                                global_task_index += 1

                        # ── Phase 4: Resolve task dependencies ────────────
                        await self._resolve_progressive_dependencies(
                            task_results, generation_id, repo
                        )
                        await session.commit()

                    except Exception as e:
                        await session.rollback()
                        raise

                # Broadcast resolved dependencies so frontend can create
                # task-to-task edges with real DB IDs.
                for t in task_results:
                    resolved = t.get("context", [])
                    if resolved:
                        await sse_manager.broadcast_to_job(
                            generation_id,
                            SSEEvent(
                                data={
                                    "type": "dependencies_resolved",
                                    "task_id": t["id"],
                                    "task_name": t.get("name", ""),
                                    "context": resolved,
                                },
                                event="dependencies_resolved",
                            ),
                        )

                # ── Done ──────────────────────────────────────────────────
                clean_tasks = [
                    {k: v for k, v in t.items() if k != "_plan"} for t in task_results
                ]
                gen_complete_data = {
                    "type": "generation_complete",
                    "status": "completed",
                    "agents": agent_results,
                    "tasks": clean_tasks,
                }

                # Measurement ledger — written with the ids that were actually
                # persisted, which later link this generation to the run it
                # becomes.
                await self._record_recipe_trial_isolated(
                    recipe_decision, agent_results, clean_tasks, group_context
                )

                # Tell the user what this drew on, AFTER the fact. Reuse is
                # surfaced, never gated: an approval step on every generation
                # would slow the common case down to re-confirm a judgement
                # already made when the recipe was marked good.
                if recipe_decision is not None and recipe_decision.injected_labels:
                    gen_complete_data["reused_recipes"] = (
                        recipe_decision.injected_labels
                    )

                # ── ChatMode auto-execute ─────────────────────────────────
                # ChatMode generates AND runs in one backend flow so the run
                # survives the user switching sessions before the plan finishes
                # — the frontend never has to round-trip a createExecution call
                # (which is what used to drop the run on session switch). The
                # execution id is FOLDED INTO generation_complete (a single
                # terminal event) so the frontend can stop the generation stream
                # immediately — no open-window for SSE reconnect/replay to
                # re-deliver and cross-route trace events. AgentBuilder leaves
                # auto_execute False and only renders the plan as nodes.
                if getattr(request, "auto_execute", False) and not clean_tasks:
                    # A crew with zero tasks cannot run — crew preparation requires
                    # at least one task. Don't launch it (it would crash in
                    # preparation); surface an actionable error on the
                    # generation_complete event instead. This is the terminal guard
                    # after the synthesize-tasks fallback also came up empty.
                    msg = (
                        "Auto-execute skipped: the crew has no runnable tasks "
                        "(task generation and the fallback both produced none)."
                    )
                    logger.error(f"PROGRESSIVE [{generation_id}]: {msg}")
                    gen_complete_data["execution_error"] = msg
                elif getattr(request, "auto_execute", False):
                    try:
                        from src.schemas.execution import CrewConfig
                        from src.services.execution.service import ExecutionService

                        crew_config = CrewConfig(
                            **self.build_crew_config_from_generated(
                                request, agent_results, clean_tasks
                            )
                        )
                        # session=None: the whole execution stack opens its own
                        # routed_scoped_session() (already detached above), so a
                        # request-scoped session would only be a closed handle.
                        # background_tasks=None launches via asyncio.create_task.
                        exec_result = await ExecutionService(
                            session=None
                        ).create_execution(
                            config=crew_config,
                            background_tasks=None,
                            group_context=group_context,
                        )
                        gen_complete_data["execution_id"] = exec_result.get(
                            "execution_id"
                        )
                        gen_complete_data["run_name"] = exec_result.get("run_name")
                        logger.info(
                            f"PROGRESSIVE [{generation_id}]: Auto-execute launched "
                            f"execution {exec_result.get('execution_id')}"
                        )
                    except Exception as exec_err:
                        logger.error(
                            f"PROGRESSIVE [{generation_id}]: Auto-execute failed: "
                            f"{exec_err}"
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
                logger.info(f"PROGRESSIVE [{generation_id}]: Generation complete")

                # Populate the trace Response (otherwise it shows null).
                try:
                    from src.services.otel_tracing.mlflow_parent_setup import (
                        set_root_span_outputs,
                    )

                    set_root_span_outputs(root_span, gen_complete_data)
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"PROGRESSIVE [{generation_id}]: Unexpected error: {e}")
                logger.error(traceback.format_exc())
                await sse_manager.broadcast_to_job(
                    generation_id,
                    SSEEvent(
                        data={
                            "type": "generation_failed",
                            "status": "failed",
                            "error": str(e),
                        },
                        event="generation_failed",
                    ),
                )

    async def _generate_crew_plan(
        self,
        request: CrewStreamingRequest,
        group_context: Optional[GroupContext],
        model: str,
        max_agents: int = 1,
        max_tasks: int = 1,
        explicit_agents: Optional[int] = None,
        exemplars: str = "",
    ) -> Dict[str, Any]:
        """Fast LLM call to get crew outline (names/roles only).

        NOTE: This method is called from create_crew_progressive which runs as
        a background task after the request-scoped session is closed. It uses
        an independent session to log the LLM interaction.

        Args:
            max_agents: Maximum number of agents to generate. Injected into
                the user message so the LLM plans within the limit rather
                than generating excess agents that get truncated (which would
                lose the user's actual goal).
            max_tasks: Maximum number of tasks to generate.
            explicit_agents: The agent count the user asked for IN WORDS
                ("create 4 agents"), when they did. Turns the cap from an upper
                bound into an exact target — otherwise "use the minimum number
                of agents" wins and a five-topic fan-out collapses to one agent.
        """
        # Dedicated lightweight plan template (~1.4k chars). The old approach
        # sent the full 9.4k-char generate_crew template and then told the
        # model to IGNORE most of it (descriptions/backstories/tools) — ~2k
        # wasted prompt tokens with contradictory instructions on every
        # "create a crew" chat message.
        system_message = await TemplateService.get_effective_template_content(
            "generate_crew_plan", group_context
        )
        if not system_message:
            # Fallback for DBs seeded before the plan template existed.
            system_message = await TemplateService.get_effective_template_content(
                "generate_crew", group_context
            )
            if not system_message:
                raise KasalError(
                    "Required prompt template 'generate_crew_plan' not found"
                )
            planning_prefix = (
                "You are generating a PLAN OUTLINE only. Return a lightweight JSON with:\n"
                '{"complexity": "light|standard|complex", "process_type": "sequential|parallel", '
                '"agents": [{"name": "...", "role": "..."}], '
                '"tasks": [{"name": "...", "assigned_agent": "...", "context": []}]}\n'
                "Do NOT include descriptions, goals, backstories, or tools — those will be generated separately.\n\n"
            )
            system_message = planning_prefix + system_message

        # Inject cap constraints based on verb-counted max_tasks. When the user
        # named a count, it is a TARGET, not a ceiling — say so explicitly and
        # suppress the minimise-agents rule, which otherwise overrides it.
        if explicit_agents:
            system_cap = (
                f"OUTPUT CONSTRAINT: The user explicitly asked for "
                f"{explicit_agents} agents — generate EXACTLY {explicit_agents} "
                f"agent(s), one per distinct subject the user enumerated, and at "
                f"least one task per agent (max {max_tasks} tasks). Do NOT merge "
                f"them into fewer agents and do NOT apply the "
                f"minimum-agents rule here.\n\n"
            )
            cap_instruction = (
                f"\n\nCONSTRAINT: Generate EXACTLY {explicit_agents} agent(s) — "
                f"one per subject the user listed — and give every agent its own "
                f"task (up to {max_tasks} tasks). An agent with no task is dropped."
            )
        else:
            system_cap = (
                f"OUTPUT CONSTRAINT: Generate up to {max_agents} agent(s) and "
                f"up to {max_tasks} task(s). Each distinct action verb in the user's "
                f"message should map to a separate task. Use the minimum number of "
                f"agents needed to cover the tasks.\n\n"
            )
            cap_instruction = (
                f"\n\nCONSTRAINT: Generate up to {max_agents} agent(s) and "
                f"up to {max_tasks} task(s). Match task count to the number of "
                f"distinct action verbs in the message."
            )

        # Plan against what the USER actually asked, not a restatement of it.
        #
        # ChatMode reaches this through the dispatcher, which passes its LLM
        # rewrite (``suggested_prompt``) as ``prompt`` and keeps the real message
        # in ``original_prompt``. The rewrite reliably grows steps: "gather the
        # latest innovation around agent memory and give me a table" came back as
        # "Research AND compile a table …, INCLUDING the innovation name and
        # citation links", and since the constraint below asks the planner to
        # match task count to the distinct action verbs in the message, those
        # manufactured verbs became manufactured tasks — three agents, three
        # tasks, two of them separately searching the web for the same thing.
        # The crew canvas plans against the typed message and produces the right
        # crew, which is the same code doing better work on better input.
        #
        # The rewrite is not discarded: it still grounds the RUN (crews.py's
        # "USER REQUEST" block) and still shapes each task's write-up. It just no
        # longer decides how many tasks there are.
        typed = getattr(request, "original_prompt", None)
        planning_prompt = (
            typed if isinstance(typed, str) and typed.strip() else request.prompt
        )
        user_message = planning_prompt + cap_instruction

        messages = [
            # Exemplars go on the SYSTEM message so they cannot displace the
            # hardcoded verb-to-task few-shots below, which own the output format.
            {"role": "system", "content": system_cap + system_message + exemplars},
        ]

        # Few-shot examples showing verb-to-task mapping AND both agent-count
        # outcomes: consolidation (one specialist covers related tasks) and
        # escalation (genuinely different specialisms get their own agent).
        # Without the second example the model over-consolidated to one agent
        # even when the cap allowed more.
        messages.extend(
            [
                {
                    "role": "user",
                    "content": (
                        "gather swiss news, create a presentation, and send an email to the team\n\n"
                        "CONSTRAINT: Generate up to 2 agent(s) and up to 3 task(s). "
                        "Match task count to the number of distinct action verbs in the message."
                    ),
                },
                {
                    "role": "assistant",
                    "content": '{"complexity":"complex","process_type":"sequential","agents":[{"name":"Swiss News Specialist","role":"News Research and Content Creation Expert"}],"tasks":[{"name":"Gather Swiss News","assigned_agent":"Swiss News Specialist","context":[]},{"name":"Create News Presentation","assigned_agent":"Swiss News Specialist","context":["Gather Swiss News"]},{"name":"Send Email to Team","assigned_agent":"Swiss News Specialist","context":["Create News Presentation"]}]}',
                },
                {
                    "role": "user",
                    "content": (
                        "research our top competitors, analyze their pricing, and write a summary report\n\n"
                        "CONSTRAINT: Generate up to 2 agent(s) and up to 3 task(s). "
                        "Match task count to the number of distinct action verbs in the message."
                    ),
                },
                {
                    "role": "assistant",
                    "content": '{"complexity":"standard","process_type":"sequential","agents":[{"name":"Market Research Analyst","role":"Competitive research and pricing analysis specialist"},{"name":"Report Writer","role":"Business report composition specialist"}],"tasks":[{"name":"Research Competitors","assigned_agent":"Market Research Analyst","context":[]},{"name":"Analyze Pricing","assigned_agent":"Market Research Analyst","context":["Research Competitors"]},{"name":"Write Summary Report","assigned_agent":"Report Writer","context":["Analyze Pricing"]}]}',
                },
            ]
        )

        messages.append(
            {"role": "user", "content": user_message},
        )

        # 4000 (was 2000): reasoning models (Qwen3-thinking, gpt-oss, R1-style)
        # spend part of the budget on hidden reasoning tokens, so 2000 could
        # exhaust before the plan JSON closed → truncated, unparseable output.
        content = await LLMManager.completion(
            messages=messages,
            model=model,
            temperature=0.3,
            max_tokens=4000,
            # The shape is REQUIRED of the model, not merely described to it.
            # Asked in prose for scope/produces/needs_tools, gpt-5-3-codex read a
            # 4.3k-char instruction carrying all three and answered with the old
            # three-field shape and six tasks for a two-task request. A field a
            # model may decline to emit cannot be built on.
            response_format=CrewPlan,
        )

        # Log via an independent session (the request-scoped session is closed
        # by the time this background task runs). ROUTED: this runs IN-PROCESS
        # under asyncio.create_task, where the raw factory is a snapshot only a
        # subprocess ever swaps to Lakebase — so the log row would have landed in
        # the local database while every other write from this generation went to
        # Lakebase.
        from src.db.session import routed_scoped_session

        try:
            async with routed_scoped_session() as log_session:
                log_service = LLMLogService(LLMLogRepository(log_session))
                await log_service.create_log(
                    endpoint="generate-crew-plan",
                    prompt=f"System: {system_message}\nUser: {user_message}",
                    response=content,
                    model=model,
                    status="success",
                    group_context=group_context,
                )
                await log_session.commit()
                logger.info("Logged generate-crew-plan interaction to database")
        except Exception as e:
            logger.error(f"Failed to log crew plan LLM interaction: {e}")

        plan = robust_json_parser(content)

        if not isinstance(plan.get("agents"), list) or len(plan["agents"]) == 0:
            raise BadRequestError("Plan returned no agents")

        if not isinstance(plan.get("tasks"), list) or len(plan["tasks"]) == 0:
            raise BadRequestError("Plan returned no tasks")

        return plan

    async def _suggest_genie_space(
        self, task_name: str, task_description: str
    ) -> Optional[Dict]:
        """Query Genie spaces and suggest the best match based on task context."""
        try:
            from src.repositories.genie_repository import GenieRepository

            genie_repo = GenieRepository()

            # Search using task name as query
            response = await genie_repo.get_spaces(
                search_query=task_name,
                page_size=5,
                enabled_only=True,
            )

            if response.spaces:
                best = response.spaces[0]
                return {
                    "id": best.id,
                    "name": best.name,
                    "description": best.description or "",
                }

            # Fallback: get first available space if search returned nothing
            response = await genie_repo.get_spaces(page_size=1, enabled_only=True)
            if response.spaces:
                best = response.spaces[0]
                return {
                    "id": best.id,
                    "name": best.name,
                    "description": best.description or "",
                }

            return None
        except Exception as e:
            logger.warning(f"Failed to suggest Genie space: {e}")
            return None

    @staticmethod
    def _find_agent_context(
        task_plan: Dict[str, Any],
        agent_results: List[Dict[str, Any]],
    ) -> Optional[TaskGenAgent]:
        """Build a TaskGenAgent for the task's assigned agent, if found."""
        assigned = task_plan.get("assigned_agent", "")
        if not assigned:
            return None

        for agent in agent_results:
            if agent.get("name", "").lower() == assigned.lower():
                return TaskGenAgent(
                    name=agent["name"],
                    role=agent.get("role", ""),
                    goal=agent.get("goal", ""),
                    backstory=agent.get("backstory", ""),
                )
        return None

    @staticmethod
    def _task_brief(task_plan: Dict[str, Any], task_name: str, crew_prompt: str) -> str:
        """What to tell the LLM that writes ONE task's description.

        This used to be the task's name plus the WHOLE user request, and nothing
        about the other tasks. Each task is written by its own call, so N calls
        were each asked to satisfy the entire request — and each did. Asked to
        "gather the latest innovations in agent memory and give me a table", all
        three tasks came back describing gathering, all three took
        PerplexityTool, and run 499624b2 spent 4 searches on one question, two of
        them differing only by a hyphen.

        The plan already decided the division of work; it just was not passed on.
        So the brief now leads with this task's SCOPE, names what it RECEIVES
        from earlier tasks and what it must PRODUCE, and carries the user's
        request only as background. The template's SCOPE section tells the model
        what to do with those.

        Only data goes in here — the instructions live in the generate_task
        template, which is DB-backed and editable. When the plan supplies no
        scope (an older plan, or a model that dropped the field) the wording
        falls back to exactly what it was before, so nothing regresses.
        """
        scope = " ".join(str(task_plan.get("scope") or "").split())
        produces = " ".join(str(task_plan.get("produces") or "").split())
        context_names = [
            str(name).strip()
            for name in (task_plan.get("context") or [])
            if str(name).strip()
        ]

        if not scope and not produces and not context_names:
            return (
                f"Create a task named '{task_name}' for a crew that: "
                f"{crew_prompt}. THIS SPECIFIC TASK is '{task_name}'."
            )

        lines = [f"Create a task named '{task_name}'."]
        if scope:
            lines.append(f"THIS TASK'S SCOPE — its only responsibility: {scope}")
        if context_names:
            lines.append(
                "It RECEIVES the output of these earlier tasks, already done: "
                + ", ".join(context_names)
            )
        if produces:
            lines.append(f"It PRODUCES: {produces}")
        # Last, and labelled as background: leading with it is what made every
        # task restate the whole job.
        lines.append(f"For background, the crew as a whole: {crew_prompt}")
        return "\n".join(lines)

    @staticmethod
    def _ancestor_tool_ids(
        task_plan: Dict[str, Any],
        plan_tasks: List[Dict[str, Any]],
        tools_by_task: Dict[str, List[Any]],
    ) -> set:
        """Tool ids already held by the tasks this one depends on, transitively.

        The one thing neither the prompt nor the schema could fix. Told that
        "find the sources" and "collect their links" are the same lookup, given a
        schema that REQUIRED an answer, and finally planning against the user's
        own words with no rewrite in the way, the planner still returned:

            {"name": "Extract Citation Links",
             "context": ["Gather Latest Innovation Information"],
             "needs_tools": true}

        A second web search for links the first search had already returned. Runs
        97ff3d3a and after spent PerplexityTool on both tasks.

        A dependency is the crew stating "this task receives that task's output",
        so re-taking the same capability along that chain is doing the upstream
        task's work again. That is a structural fact about the plan, not a
        judgement, so it is settled here instead of argued about in a prompt.

        Keyed on the TOOL, not on the kind of work — which is what keeps it
        general. A "fetch the numbers -> publish the dashboard" chain is
        untouched, because the publisher's tool is a different one. And it follows
        only dependencies, so independent tasks in a fan-out ("one for sports, one
        for politics") still share a tool: they act on different subjects.
        """
        by_name = {
            str(t.get("name") or ""): t for t in plan_tasks if isinstance(t, dict)
        }
        seen: set = set()
        frontier = [
            str(name).strip() for name in (task_plan.get("context") or []) if name
        ]
        held: set = set()
        while frontier:
            name = frontier.pop()
            if not name or name in seen:
                continue
            seen.add(name)
            held.update(tools_by_task.get(name, []))
            ancestor = by_name.get(name)
            if isinstance(ancestor, dict):
                frontier.extend(
                    str(n).strip() for n in (ancestor.get("context") or []) if n
                )
        return held

    @staticmethod
    def _task_tool_ids(
        tools: Optional[List[Any]],
        name_to_id: Dict[str, Any],
        exclude: Optional[set] = None,
    ) -> List[Any]:
        """Map generated tool NAMES onto workspace tool ids.

        ``exclude`` carries the tools this task's dependencies already hold (see
        ``_ancestor_tool_ids``) — the one structural rule applied here, because a
        prompt rule is a preference and a model can name a tool it was never
        offered. An unknown name is dropped rather than failing the whole
        generation: the model is told to use only the names it was given, but it
        is a model.

        There is deliberately no "this task may not use tools" flag. One existed,
        driven by the plan's ``needs_tools``, and it turned a planner misjudgement
        into a capability loss: asked to "gather swiss news and create a
        presentation and send it to <address>", the planner marked *Gather Swiss
        News* as needing no tools, so the search task was never shown the tool
        list and ran blind, while *Send Presentation via Email* — the only task
        the plan marked — took the workspace's only tool, PerplexityTool, which
        cannot send anything. The dependency rule alone gets that plan right:
        gather keeps the tool, the two tasks downstream of it do not.
        """
        already_held = exclude or set()
        resolved: List[Any] = []
        for entry in tools or []:
            name = entry.get("name") if isinstance(entry, dict) else str(entry)
            if not name or name not in name_to_id:
                continue
            tool_id = name_to_id[name]
            if tool_id in already_held:
                logger.info(
                    "Dropping %s: a task this one depends on already holds it", name
                )
                continue
            resolved.append(tool_id)
        return resolved

    @staticmethod
    def _resolve_agent_id(
        task_plan: Dict[str, Any],
        agent_results: List[Dict[str, Any]],
    ) -> Optional[str]:
        """Resolve the assigned_agent name to a database agent ID."""
        assigned = task_plan.get("assigned_agent", "")
        if not assigned:
            return agent_results[0]["id"] if agent_results else None

        for agent in agent_results:
            if agent.get("name", "").lower() == assigned.lower():
                return agent["id"]

        # Fallback: first agent
        return agent_results[0]["id"] if agent_results else None

    async def _resolve_progressive_dependencies(
        self,
        task_results: List[Dict[str, Any]],
        generation_id: str,
        repo: Optional["CrewGeneratorRepository"] = None,
    ) -> None:
        """Resolve task context references (names) to database IDs."""
        effective_repo = repo or self.crew_generator_repository

        task_name_to_id: Dict[str, str] = {}
        for t in task_results:
            name = t.get("name", "")
            tid = t.get("id", "")
            if name and tid:
                task_name_to_id[name] = tid

        for t in task_results:
            plan = t.get("_plan", {})
            context_refs = plan.get("context", [])
            if not context_refs:
                continue

            resolved_ids = []
            for ref in context_refs:
                dep_id = task_name_to_id.get(ref)
                if dep_id and dep_id != t.get("id"):
                    resolved_ids.append(dep_id)

            if resolved_ids:
                try:
                    await effective_repo.update_task_dependencies(t["id"], resolved_ids)
                    t["context"] = resolved_ids
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: "
                        f"Task '{t.get('name')}' dependencies: {resolved_ids}"
                    )
                except Exception as e:
                    logger.error(
                        f"PROGRESSIVE [{generation_id}]: "
                        f"Failed to set deps for '{t.get('name')}': {e}"
                    )
