"""Light/chat single-agent execution service.

Dedicated engine-level service for the "chat" (light) answer mode — the
single-agent counterpart to :class:`CrewPreparation` + ``run_crew_in_process``
(crews) and :class:`CrewAIFlowService` (flows). It owns the single-agent build,
cognitive-memory wiring, tool/agent-lifecycle tracing, ``Agent.kickoff_async``,
and terminal status. Runs IN-PROCESS for sub-second latency.

The module-level ``run_light_agent`` entry point lives at the bottom of this
module (mirroring ``run_crew_in_process`` in the crew path and
``run_flow_in_process`` in the flow path) and delegates to
:class:`LightAgentService`. It was previously misplaced in
``paths/crew/execution_runner`` and moved here as part of the engine-path
refactor, so existing callers/tests keep working.
"""

import asyncio
import logging
import os
from typing import Any, Dict, Optional

from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class _RunTraceWriter:
    """Persists one light-agent run's trace events on a single private session.

    Perf: the previous per-event ``get_isolated_db_session()`` opened a fresh
    connection (full TCP+TLS+auth on Lakebase NullPool) for EVERY tool/LLM
    event — ~10-20 handshakes per chat answer, all on the critical path since
    completion awaits the persists. This writer opens ONE private session
    lazily on the first persist and reuses it until ``close()``.

    Concurrency: handlers schedule persists onto the main loop via
    ``run_coroutine_threadsafe``, so several (tool_usage, <tool>_run, llm_call,
    llm_response, response_run) run as concurrent tasks. Interleaving their DB
    work corrupts the async connection's greenlet state (the symptom is
    ``MissingGreenlet`` mid-run), so the internal lock makes writes strictly
    one-at-a-time — which is also what makes single-session reuse safe.

    The parent ExecutionHistory row is verified once by the first successful
    write; later writes skip that SELECT and carry the resolved ``run_id``
    forward. A failed write rolls back and drops the session so the next
    persist reopens a fresh connection.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._ctx: Any = None
        self._session: Any = None
        self._verified = False
        self._run_id: Optional[int] = None

    async def _get_session(self) -> Any:
        if self._session is None:
            from src.db.session import get_isolated_db_session
            self._ctx = get_isolated_db_session()
            self._session = await self._ctx.__aenter__()
        return self._session

    async def close(self) -> None:
        """Release the private session. Idempotent; never raises."""
        ctx = self._ctx
        self._ctx = None
        self._session = None
        if ctx is not None:
            try:
                await ctx.__aexit__(None, None, None)
            except Exception as close_err:  # noqa: BLE001
                logger.debug(f"[light_agent] trace session close skipped: {close_err}")

    async def persist(self, trace_data: Dict[str, Any]) -> None:
        """Write one trace event. Never raises — a lost trace must not fail the run."""
        try:
            from src.services.execution_trace_service import ExecutionTraceService
            async with self._lock:
                session = await self._get_session()
                try:
                    if self._run_id is not None:
                        trace_data.setdefault("run_id", self._run_id)
                    item = await ExecutionTraceService(session).create_trace(
                        trace_data,
                        verify_execution_exists=not self._verified,
                    )
                    await session.commit()
                    self._verified = True
                    if self._run_id is None:
                        self._run_id = getattr(item, "run_id", None)
                except Exception:
                    # The connection may be poisoned — roll back and drop it so
                    # the next persist reopens a fresh one.
                    try:
                        await session.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    await self.close()
                    raise
            logger.debug(
                f"[light_agent] trace persisted: job_id={trace_data.get('job_id')} "
                f"event_type={trace_data.get('event_type')}"
            )
        except Exception as persist_err:  # noqa: BLE001
            logger.warning(
                f"[light_agent] trace persist FAILED "
                f"(event_type={trace_data.get('event_type')}): {persist_err}",
                exc_info=True,
            )


class LightAgentService:
    """Engine-level service for the single-agent ("chat"/light) path."""

    async def run_light_agent_execution(
        self,
        execution_id: str,
        config: Any,
        group_context: GroupContext = None,
        session=None,
    ) -> Dict[str, Any]:
        """Run a SINGLE agent via CrewAI ``Agent.kickoff_async`` — the "chat"
        (light) answer mode. No crew, no tasks/process, no planning/reasoning.

        Engine-level counterpart of the crew runners (:func:`run_crew_in_process`):
        the service layer resolves the engine and delegates here, so all
        CrewAI-specific work (agent build, kickoff, trace emission) stays in the
        engine. Runs IN-PROCESS (no subprocess spin-up) for sub-second latency and
        writes its own terminal status, so a fast answer is fetchable via the REST
        poller even when the SSE listener attaches late.

        Tool activity is surfaced to the chat trace pane through a per-agent
        ``step_callback`` (scoped to THIS agent instance, not the global event bus,
        so concurrent in-process runs never cross-talk — tenant-safe). It emits a
        ``<tool>_run`` trace per tool step via ``ExecutionTraceService.create_trace``,
        which persists the trace and broadcasts it over SSE (in-process, so not the
        subprocess no-op path).

        Args:
            execution_id: Execution/job ID (already has a RUNNING row).
            config: ``CrewConfig`` with exactly one agent + one task (chat mode).
            group_context: Multi-tenant context (group + optional OBO token).
            session: Unused; kept for signature parity with the crew runners.

        Returns:
            ``{"execution_id", "status"[, "error"]}``.
        """
        import re
        from datetime import datetime, UTC
        from src.utils.user_context import UserContext
        from src.engines.crewai.kernel.agent_tools import build_agent_with_tools
        from src.services.execution_status_service import ExecutionStatusService
        from src.db.session import request_scoped_session
        from src.services.agent_service import AgentService
        from src.services.execution_logs_queue import enqueue_log

        def _log(msg: str) -> None:
            """Best-effort execution log line. The main-process logs writer drains
            the queue to the DB, surfacing these in the run's Logs tab. Thread-safe
            (plain queue) so it is also callable from the bus's handler threads;
            never raises."""
            try:
                enqueue_log(execution_id=execution_id, content=msg, group_context=group_context)
            except Exception:  # noqa: BLE001
                pass

        # Detached background task: re-establish auth context (group + OBO token).
        # build_agent_llm REQUIRES a group_id and OBO-protected Databricks models
        # need the user token in UserContext, so this MUST run before the build.
        if group_context:
            try:
                UserContext.set_group_context(group_context)
                if getattr(group_context, "access_token", None):
                    UserContext.set_user_token(group_context.access_token)
            except Exception as ctx_err:  # noqa: BLE001
                logger.warning(f"[light_agent] Could not set user context: {ctx_err}")

        try:
            logger.info(f"[light_agent] Starting light agent execution {execution_id}")

            # ── Resolve the single agent spec from the config ─────────────────
            agent_spec: Optional[Dict[str, Any]] = None
            if getattr(config, "agents_yaml", None) and isinstance(config.agents_yaml, dict):
                for agent_key, agent_config in config.agents_yaml.items():
                    agent_spec = dict(agent_config)
                    agent_spec.setdefault("id", agent_key)
                    break
            elif getattr(config, "agents", None) and isinstance(config.agents, list) and config.agents:
                agent_spec = dict(config.agents[0])
            if not agent_spec:
                raise ValueError("Light agent execution requires exactly one agent in the config")

            # Best-effort: merge DB tool_configs when the spec lacks them (catalog
            # agents). Chat-generated agents already carry their own tool_configs.
            if not agent_spec.get("tool_configs"):
                try:
                    async with request_scoped_session() as db_session:
                        db_id = str(agent_spec.get("id", "")).replace("agent_", "").replace("agent-", "")
                        db_agent = await AgentService(db_session).get(db_id) if db_id else None
                        if db_agent and getattr(db_agent, "tool_configs", None):
                            agent_spec["tool_configs"] = db_agent.tool_configs
                except Exception as enrich_err:  # noqa: BLE001
                    logger.debug(f"[light_agent] tool_configs enrichment skipped: {enrich_err}")

            # ── Prompt = the (already user-grounded) first task description ────
            prompt = ""
            if getattr(config, "tasks_yaml", None) and isinstance(config.tasks_yaml, dict):
                for _tid, task_config in config.tasks_yaml.items():
                    prompt = str(task_config.get("description") or "")
                    expected = str(task_config.get("expected_output") or "")
                    if expected:
                        prompt = f"{prompt}\n\nExpected output: {expected}"
                    break
            elif getattr(config, "tasks", None) and isinstance(config.tasks, list) and config.tasks:
                prompt = str(config.tasks[0].get("description") or "")
            if not prompt.strip():
                _inputs = getattr(config, "inputs", None) or {}
                prompt = str(_inputs.get("user_request") or _inputs.get("prompt") or "")
            if not prompt.strip():
                raise ValueError("Light agent execution requires a prompt (task description)")

            group_id = self._resolve_group_id(config, group_context)
            group_email = getattr(group_context, "group_email", None)
            # Diagnostic: MCP servers are workspace-scoped, so the resolved group_id
            # must match the workspace where a server was enabled or MCP resolves to
            # 0. Log every input so a mismatch (e.g. resolving to a domain/team group
            # instead of the personal workspace) is visible in the run logs.
            logger.info(
                "[light_agent] group resolution: resolved=%r config.group_id=%r "
                "primary_group_id=%r group_ids=%r group_email=%r",
                group_id,
                getattr(config, "group_id", None),
                getattr(group_context, "primary_group_id", None),
                getattr(group_context, "group_ids", None),
                group_email,
            )
            role = str(agent_spec.get("role") or agent_spec.get("name") or "Assistant")
            # Short label for the trace's event_context (the user's ask, one line).
            trace_context = (prompt.strip().splitlines()[0] if prompt.strip() else "chat")[:120]

            # Prior turns of THIS chat session. Each light-agent turn runs as an
            # isolated kickoff with no built-in history, so without this the agent
            # cannot recall what was just said (e.g. the user's name). Prepended to
            # the kickoff prompt ONLY — trace + memory keep the clean current ask.
            conversation_preamble = await self._conversation_preamble(
                config, group_context, group_id, _log
            )

            # Make sure the main-process logs writer is draining the queue, then log
            # the run start so the Logs tab is populated for this light run too.
            try:
                from src.engines.crewai.infra.trace_management import TraceManager
                await TraceManager.ensure_writer_started()
            except Exception as w_err:  # noqa: BLE001
                logger.debug(f"[light_agent] logs writer ensure skipped: {w_err}")
            _log(f"Chat agent '{role}' started")
            _log(f"Prompt: {trace_context}")

            # ── Tool-activity tracing via CrewAI's event bus ──────────────────
            # The agent's per-instance ``step_callback`` only fires on the ReAct
            # path; native/function-calling tool calls (e.g. the vLLM function
            # caller + MCP tools) go through ``execute_native_tool``, which does NOT
            # invoke it. The event bus, by contrast, emits ToolUsage{Started,Finished}
            # for BOTH paths, so it is the reliable source. Handlers are filtered by
            # this run's agent id, so concurrent in-process light runs never
            # cross-talk — tenant-safe (each run only traces its own agent's tools)
            # — and are unregistered in a ``finally`` so nothing leaks on the bus.
            #
            # ``emit`` runs sync handlers in a worker thread (and parallel native
            # tool calls emit from a pool thread), so the async DB write + SSE
            # broadcast is bridged onto THIS run's loop — the same loop the SSE
            # clients live on — via ``run_coroutine_threadsafe``.
            try:
                _main_loop = asyncio.get_running_loop()
            except RuntimeError:
                _main_loop = None

            # Futures for every scheduled trace write, awaited before the run is
            # marked COMPLETED so a fire-and-forget write is never dropped (the
            # crew/flow OTel exporter flushes before the subprocess exits — this
            # is the in-process equivalent).
            _pending_trace_futures: list = []
            # Per-run trace writer: one private session for all of this run's
            # trace writes, serialized internally (see _RunTraceWriter).
            _trace_writer = _RunTraceWriter()

            async def _flush_and_close_traces(timeout: float) -> None:
                """Await pending persists (bounded), then release the session."""
                if _pending_trace_futures:
                    logger.info(
                        f"[light_agent] flushing {len(_pending_trace_futures)} pending "
                        f"trace write(s) for {execution_id}"
                    )
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(
                                *(asyncio.wrap_future(f) for f in _pending_trace_futures),
                                return_exceptions=True,
                            ),
                            timeout=timeout,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[light_agent] trace flush timed out for {execution_id}; "
                            "some traces may be written after completion"
                        )
                await _trace_writer.close()

            def _schedule_trace(trace_data: Dict[str, Any]) -> None:
                if _main_loop is None:
                    logger.warning("[light_agent] no main loop — trace dropped")
                    return
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        _trace_writer.persist(trace_data), _main_loop
                    )
                    _pending_trace_futures.append(fut)
                    fut.add_done_callback(lambda f: f.exception())  # drain, never raise
                except Exception as sched_err:  # noqa: BLE001
                    logger.warning(
                        f"[light_agent] trace schedule FAILED: {sched_err}", exc_info=True
                    )

            def _base_trace(event_type: str, output: Dict[str, Any], tool_name: str) -> Dict[str, Any]:
                td: Dict[str, Any] = {
                    "job_id": execution_id,
                    "event_source": role,
                    "event_context": trace_context,
                    "event_type": event_type,
                    "created_at": datetime.now(UTC).replace(tzinfo=None),
                    "output": output,
                    "trace_metadata": {"agent_role": role, "tool_name": tool_name},
                }
                if group_id and group_id != "default":
                    td["group_id"] = group_id
                if group_email:
                    td["group_email"] = group_email
                return td

            # ── Build the ToolFactory (DB-backed API keys), then the agent, then
            # kickoff — all within one DB session so MCP/tool resources stay live.
            from src.engines.crewai.tools.tool_factory import ToolFactory
            from src.services.api_keys_service import ApiKeysService

            _factory_group = group_id if group_id != "default" else None
            async with request_scoped_session() as db_session:
                try:
                    api_keys_service = ApiKeysService(db_session, group_id=_factory_group)
                    tool_factory = await ToolFactory.create(
                        config={"group_id": group_id} if _factory_group else {},
                        api_keys_service=api_keys_service,
                    )
                except Exception as tf_err:  # noqa: BLE001
                    logger.warning(f"[light_agent] ToolFactory.create failed, using basic factory: {tf_err}")
                    tool_factory = ToolFactory({"group_id": group_id} if _factory_group else {})
                    try:
                        await tool_factory.initialize()
                    except Exception:  # noqa: BLE001
                        pass

                # Thread the requesting user's OBO token into MCP creation (see
                # _build_mcp_configs) so Databricks-managed MCP servers authenticate
                # on behalf of the user, not the app service principal.
                user_obo_token = getattr(group_context, "access_token", None)
                mcp_config, mcp_call_config = self._build_mcp_configs(
                    agent_spec, group_id, user_obo_token
                )

                agent = await build_agent_with_tools(
                    agent_spec,
                    group_id=group_id,
                    default_model="databricks-llama-4-maverick",
                    label=str(agent_spec.get("role") or agent_spec.get("name") or "chat-agent"),
                    tool_ids=agent_spec.get("tools") or None,
                    tool_factory=tool_factory,
                    tool_configs=agent_spec.get("tool_configs", {}),
                    tool_service=None,
                    mcp_config=mcp_config,
                    mcp_call_config=mcp_call_config,
                )

                # ── Genie MCP fixups — parity with the crew/flow task_builder ─────
                # The light agent has no Task, so it skips build_task_args() and the
                # MCP/Genie fixups it performs. Apply them here against the agent's
                # tools so a Genie MCP server picked in the chat "+" menu actually
                # works (without this, a co-assigned GenieTool errors "Genie space ID
                # is not configured"). Output formatting is intentionally NOT done —
                # the answer flows through the shared A2UI composer like any other.
                self._apply_genie_mcp_fixups(agent)

                # ── Cognitive memory (recall + persist) — chat parity w/ crews ──
                # Attach a unified Memory so kickoff_async auto-recalls relevant
                # context and persists this turn. Best-effort: never breaks the run.
                await self._attach_memory(
                    agent, agent_spec, config, group_context,
                    group_id, prompt, execution_id, _log,
                )
                # The unified Memory instance attached above is the bus ``source``
                # for this run's MemoryQuery/Save/Retrieval events (CrewAI emits them
                # with source=<the Memory>, and — unlike tool/LLM events — WITHOUT an
                # agent_id). Hold a reference so the memory handlers can scope by
                # identity (``source is _agent_memory``), tenant-safe for concurrent
                # in-process light runs. ``None``/``True``/``False`` means memory was
                # not attached (disabled / no backend) → no memory traces.
                _attached_mem = getattr(agent, "memory", None)
                _agent_memory = _attached_mem if _attached_mem not in (None, True, False) else None

                # This agent's own LLM instance. CrewAI emits tool/LLM events with
                # ``source = <the LLM>`` (crewai/llm.py, llms/base_llm.py), and native
                # function-calling / MCP tool events can arrive with ``from_agent``
                # already nulled by ToolUsageEvent.__init__ (it copies agent_id/role
                # off from_agent then clears it) — or, for a direct LLM tool call,
                # with NO agent attribution at all. Matching ``source is _agent_llm``
                # catches those, and is tenant-safe: build_agent builds a fresh LLM
                # per agent, so each in-process light run has its own instance.
                _agent_llm = getattr(agent, "llm", None)

                # Register tool-activity handlers scoped to THIS agent's id. A
                # ``tool_usage`` trace marks the call; a ``<tool>_run`` trace carries
                # the result (the chat pane renders both and restores ``_run`` from
                # the DB on refresh). Both are matched by agent id so a concurrent
                # light run's tools never bleed into this run's timeline.
                import json
                from crewai.events import crewai_event_bus
                from crewai.events.types.tool_usage_events import (
                    ToolUsageStartedEvent,
                    ToolUsageFinishedEvent,
                    ToolUsageErrorEvent,
                )
                # ``Agent.kickoff_async`` runs as a CrewAI "LiteAgent" and emits these
                # lifecycle events on the SAME bus — they fire on every run, even one
                # that calls no tools, which is what gives the chat a trace when the
                # answer is pure prose (the tool handlers above never fire then).
                from crewai.events.types.agent_events import (
                    LiteAgentExecutionStartedEvent,
                    LiteAgentExecutionCompletedEvent,
                    LiteAgentExecutionErrorEvent,
                )
                from crewai.events.types.llm_events import (
                    LLMCallStartedEvent,
                    LLMCallCompletedEvent,
                    LLMCallFailedEvent,
                )
                # Memory recall/persist events — emitted by the unified Memory with
                # source=<the Memory> (no agent_id), so they're matched by identity
                # (source is _agent_memory). These give the chat trace the same
                # "Memory Read / Memory Context Retrieved / Memory Write" rows the
                # crew/flow OTel timeline shows — homogeneous across paths.
                from crewai.events.types.memory_events import (
                    MemoryQueryCompletedEvent,
                    MemoryRetrievalCompletedEvent,
                    MemorySaveCompletedEvent,
                )

                _agent_id = str(getattr(agent, "id", "") or "")
                # Mutable holder so the (sync, possibly worker-thread) started/completed
                # handlers can share the kickoff start time to compute a duration.
                _agent_started_at: list = []

                _role_lower = str(role or "").strip().lower()

                def _matches(event, source=None) -> bool:
                    if self._event_matches_run(
                        event, source,
                        agent=agent, agent_id=_agent_id,
                        role_lower=_role_lower, agent_llm=_agent_llm,
                    ):
                        return True
                    # Nothing matched — log once so a dropped MCP/tool event is
                    # visible instead of silently vanishing from the trace.
                    logger.info(
                        "[light_agent] tool event NOT matched to this run "
                        "(tool=%s event_agent_id=%s our_agent_id=%s source=%s) — dropped",
                        getattr(event, "tool_name", "?"),
                        getattr(event, "agent_id", None), _agent_id,
                        type(source).__name__ if source is not None else None,
                    )
                    return False

                def _args_str(event) -> str:
                    ta = getattr(event, "tool_args", None)
                    if ta is None:
                        return ""
                    if isinstance(ta, str):
                        return ta
                    try:
                        return json.dumps(ta, default=str)
                    except Exception:  # noqa: BLE001
                        return str(ta)

                def _on_tool_started(source, event) -> None:
                    try:
                        if not _matches(event, source):
                            return
                        tool_name = str(getattr(event, "tool_name", "") or "tool")
                        args = _args_str(event)
                        _log(f"Using tool: {tool_name}({args[:200]})")
                        _schedule_trace(_base_trace(
                            "tool_usage",
                            {"tool_name": tool_name,
                             "extra_data": {"tool_name": tool_name, "tool_args": args}},
                            tool_name,
                        ))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] tool-start trace skipped: {h_err}")

                def _on_tool_finished(source, event) -> None:
                    try:
                        if not _matches(event, source):
                            return
                        tool_name = str(getattr(event, "tool_name", "") or "tool")
                        out_val = getattr(event, "output", None)
                        content = "" if out_val is None else str(out_val)
                        # Cap the stored/streamed trace so a large tool dump (e.g. a
                        # full web-search payload) doesn't bloat the run record. The
                        # frontend renders a clamped body gracefully — it decodes any
                        # escapes and unwraps a JSON envelope even when the cut left
                        # invalid JSON — so this only bounds size, it doesn't garble.
                        max_len = 20000
                        if len(content) > max_len:
                            content = content[:max_len] + "…[truncated]"
                        output: Dict[str, Any] = {
                            "tool_name": tool_name,
                            "input": _args_str(event),
                            "content": content,
                        }
                        try:
                            sa = getattr(event, "started_at", None)
                            fa = getattr(event, "finished_at", None)
                            if sa and fa:
                                output["duration_ms"] = int((fa - sa).total_seconds() * 1000)
                        except Exception:  # noqa: BLE001
                            pass
                        norm = re.sub(r"[^a-z0-9]+", "", tool_name.lower()) or "tool"
                        _log(f"Tool {tool_name} returned ({len(content)} chars)")
                        _schedule_trace(_base_trace(f"{norm}_run", output, tool_name))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] tool-finish trace skipped: {h_err}")

                def _on_tool_error(source, event) -> None:
                    # Without this a tool that ERRORS (e.g. an MCP server timeout or
                    # 4xx) fires ToolUsageErrorEvent — NOT Finished — so the chat
                    # showed "using tool" then nothing. Surface the failure as a
                    # ``<tool>_error`` trace so the result is never silently missing.
                    try:
                        if not _matches(event, source):
                            return
                        tool_name = str(getattr(event, "tool_name", "") or "tool")
                        err = str(getattr(event, "error", "") or "Tool error")
                        norm = re.sub(r"[^a-z0-9]+", "", tool_name.lower()) or "tool"
                        _log(f"Tool {tool_name} failed: {err[:200]}")
                        _schedule_trace(_base_trace(
                            f"{norm}_error",
                            {"tool_name": tool_name, "input": _args_str(event),
                             "content": err, "error": err},
                            tool_name,
                        ))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] tool-error trace skipped: {h_err}")

                # ── LLM call tracing ────────────────────────────────────────
                # Each LLM round-trip (the reasoning behind the answer and every
                # tool-calling turn) fires LLMCall{Started,Completed,Failed}. The
                # crew/flow OTel bridge maps these to ``llm_call`` / ``llm_response``;
                # mirror that here so the chat trace shows the model calls too.
                def _msgs_str(event) -> str:
                    """Flatten the request messages to readable text for the trace
                    detail (so 'LLM Request' → View shows the actual prompt)."""
                    msgs = getattr(event, "messages", None)
                    if msgs is None:
                        return ""
                    if isinstance(msgs, str):
                        return msgs
                    try:
                        parts = []
                        for m in msgs:
                            if isinstance(m, dict):
                                parts.append(f"{m.get('role', '?')}: {m.get('content', '')}")
                            else:
                                parts.append(str(m))
                        return "\n\n".join(parts)
                    except Exception:  # noqa: BLE001
                        return str(msgs)

                def _on_llm_started(source, event) -> None:
                    try:
                        if not _matches(event, source):
                            return
                        model_name = str(getattr(event, "model", "") or "llm")
                        prompt_text = _msgs_str(event)
                        max_len = 20000
                        if len(prompt_text) > max_len:
                            prompt_text = prompt_text[:max_len] + "…[truncated]"
                        _log(f"LLM call ({model_name})")
                        _schedule_trace(_base_trace(
                            "llm_call",
                            {"tool_name": "LLM",
                             "input": model_name,
                             # The Jobs timeline reads output.content; the prompt here
                             # makes 'LLM Request → View' show the real request.
                             "content": prompt_text,
                             "extra_data": {"model": model_name,
                                            "prompt": prompt_text,
                                            "prompt_length": len(prompt_text)}},
                            "LLM",
                        ))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] llm-start trace skipped: {h_err}")

                def _on_llm_completed(source, event) -> None:
                    try:
                        if not _matches(event, source):
                            return
                        model_name = str(getattr(event, "model", "") or "llm")
                        resp = getattr(event, "response", None)
                        content = "" if resp is None else str(resp)
                        max_len = 20000
                        if len(content) > max_len:
                            content = content[:max_len] + "…[truncated]"
                        extra: Dict[str, Any] = {"model": model_name,
                                                 "output_length": len(content)}
                        usage = getattr(event, "usage", None)
                        if usage is not None:
                            try:
                                extra["usage"] = json.loads(json.dumps(usage, default=str))
                            except Exception:  # noqa: BLE001
                                pass
                        output: Dict[str, Any] = {
                            "tool_name": "LLM",
                            "input": model_name,
                            "content": content,
                            "extra_data": extra,
                        }
                        td = _base_trace("llm_response", output, "LLM")
                        # The Jobs timeline's llm_response label reads output_length
                        # from trace_metadata; mirror it there too.
                        td["trace_metadata"]["output_length"] = len(content)
                        td["trace_metadata"]["model"] = model_name
                        _log(f"LLM responded ({len(content)} chars, {model_name})")
                        _schedule_trace(td)
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] llm-complete trace skipped: {h_err}")

                def _on_llm_failed(source, event) -> None:
                    try:
                        if not _matches(event, source):
                            return
                        err = str(getattr(event, "error", "") or "LLM error")
                        _log(f"LLM call failed: {err[:200]}")
                        _schedule_trace(_base_trace(
                            "llm_call_failed",
                            {"tool_name": "LLM", "content": err, "error": err},
                            "LLM",
                        ))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] llm-fail trace skipped: {h_err}")

                # ── Memory tracing (Memory Read / Context Retrieved / Write) ──
                # Scoped to THIS run's Memory instance (the bus source), mirroring
                # the field extraction the crew/flow OTel bridge uses so the Jobs
                # timeline renders identical rows.
                def _matches_memory(source) -> bool:
                    return _agent_memory is not None and source is _agent_memory

                def _cap(text: str, n: int = 8000) -> str:
                    return text if len(text) <= n else text[:n] + "…[truncated]"

                def _on_memory_query(source, event) -> None:
                    try:
                        if not _matches_memory(source):
                            return
                        results = getattr(event, "results", None)
                        count = len(results) if isinstance(results, (list, tuple)) else None
                        qms = getattr(event, "query_time_ms", None)
                        content = "" if results is None else _cap(str(results))
                        extra: Dict[str, Any] = {}
                        if count is not None:
                            extra["results_count"] = count
                        if qms is not None:
                            extra["query_time_ms"] = float(qms)
                        out = {"tool_name": "Memory", "content": content, "extra_data": extra}
                        td = _base_trace("memory_retrieval", out, "Memory")
                        td["trace_metadata"].update(extra)
                        _log(f"Memory read: {count if count is not None else '?'} result(s)")
                        _schedule_trace(td)
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] memory-query trace skipped: {h_err}")

                def _on_memory_retrieval(source, event) -> None:
                    try:
                        if not _matches_memory(source):
                            return
                        mc = getattr(event, "memory_content", None)
                        content = str(mc).strip() if mc else ""
                        if not content:
                            content = "(no memories matched the query)"
                        content = _cap(content)
                        rms = getattr(event, "retrieval_time_ms", None)
                        extra: Dict[str, Any] = {}
                        if rms is not None:
                            extra["retrieval_time_ms"] = float(rms)
                        out = {"tool_name": "Memory", "content": content, "extra_data": extra}
                        td = _base_trace("memory_retrieval_completed", out, "Memory")
                        td["trace_metadata"].update(extra)
                        _schedule_trace(td)
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] memory-retrieval trace skipped: {h_err}")

                def _on_memory_save(source, event) -> None:
                    try:
                        if not _matches_memory(source):
                            return
                        val = getattr(event, "value", None)
                        content = "" if val is None else _cap(str(val))
                        sms = getattr(event, "save_time_ms", None)
                        extra: Dict[str, Any] = {}
                        if sms is not None:
                            extra["save_time_ms"] = float(sms)
                        out = {"tool_name": "Memory", "content": content, "extra_data": extra}
                        td = _base_trace("memory_write", out, "Memory")
                        td["trace_metadata"].update(extra)
                        _log("Memory write")
                        _schedule_trace(td)
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] memory-save trace skipped: {h_err}")

                # ── Agent lifecycle tracing (fires even with NO tools) ──────
                # The LiteAgent events are emitted with the AGENT instance as the bus
                # ``source`` (see crewai.agent.core kickoff_async / _finalize_kickoff /
                # _emit_kickoff_error), so ``source is agent`` scopes them to THIS run
                # — tenant-safe for concurrent in-process light runs, the same
                # guarantee the tool handlers get from agent-id matching.
                #
                # The completed event becomes a ``response_run`` trace: the ``_run``
                # suffix is the convention the chat renders as a timeline step (live
                # AND restored from the DB on refresh), so a tool-less chat answer
                # still shows a "Response" step instead of an empty trace pane.
                # NB: chat mode does NOT reason or plan (that's the 'research'/'deep'
                # crew modes) — this is just the agent's single answer generation.
                def _on_agent_started(source, event) -> None:
                    try:
                        if source is not agent:
                            return
                        _agent_started_at.append(datetime.now(UTC))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] agent-start trace skipped: {h_err}")

                def _on_agent_completed(source, event) -> None:
                    try:
                        if source is not agent:
                            return
                        out_val = getattr(event, "output", None)
                        answer_text = "" if out_val is None else str(out_val)
                        # A one-line preview as the step's expandable detail — informative
                        # without dumping the whole answer (which already renders in chat).
                        preview = " ".join(answer_text.split())[:280]
                        note = preview or f"Generated {len(answer_text)} chars"
                        output: Dict[str, Any] = {
                            "tool_name": "Response",
                            "input": "",
                            "content": note,
                        }
                        if _agent_started_at:
                            try:
                                elapsed = datetime.now(UTC) - _agent_started_at[0]
                                output["duration_ms"] = int(elapsed.total_seconds() * 1000)
                            except Exception:  # noqa: BLE001
                                pass
                        _log(f"Response generated ({len(answer_text)} chars)")
                        _schedule_trace(_base_trace("response_run", output, "Response"))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] agent-complete trace skipped: {h_err}")

                def _on_agent_error(source, event) -> None:
                    try:
                        if source is not agent:
                            return
                        err = str(getattr(event, "error", "") or "Agent error")
                        _schedule_trace(_base_trace(
                            "tool_error",
                            {"tool_name": "Response", "content": err, "error": err},
                            "Response",
                        ))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] agent-error trace skipped: {h_err}")

                crewai_event_bus.register_handler(ToolUsageStartedEvent, _on_tool_started)
                crewai_event_bus.register_handler(ToolUsageFinishedEvent, _on_tool_finished)
                crewai_event_bus.register_handler(ToolUsageErrorEvent, _on_tool_error)
                crewai_event_bus.register_handler(LLMCallStartedEvent, _on_llm_started)
                crewai_event_bus.register_handler(LLMCallCompletedEvent, _on_llm_completed)
                crewai_event_bus.register_handler(LLMCallFailedEvent, _on_llm_failed)
                crewai_event_bus.register_handler(MemoryQueryCompletedEvent, _on_memory_query)
                crewai_event_bus.register_handler(MemoryRetrievalCompletedEvent, _on_memory_retrieval)
                crewai_event_bus.register_handler(MemorySaveCompletedEvent, _on_memory_save)
                crewai_event_bus.register_handler(LiteAgentExecutionStartedEvent, _on_agent_started)
                crewai_event_bus.register_handler(LiteAgentExecutionCompletedEvent, _on_agent_completed)
                crewai_event_bus.register_handler(LiteAgentExecutionErrorEvent, _on_agent_error)

                logger.info(f"[light_agent] Kicking off single agent for execution {execution_id}")
                try:
                    kickoff_prompt = (
                        f"{conversation_preamble}\n\nCurrent message:\n{prompt}"
                        if conversation_preamble else prompt
                    )
                    kicked = await self._kickoff_with_mlflow_trace(
                        agent, kickoff_prompt, config, execution_id,
                        trace_context, group_context, group_id,
                    )
                finally:
                    # Always unregister so handlers never leak on the global bus.
                    try:
                        crewai_event_bus.off(ToolUsageStartedEvent, _on_tool_started)
                        crewai_event_bus.off(ToolUsageFinishedEvent, _on_tool_finished)
                        crewai_event_bus.off(ToolUsageErrorEvent, _on_tool_error)
                        crewai_event_bus.off(LLMCallStartedEvent, _on_llm_started)
                        crewai_event_bus.off(LLMCallCompletedEvent, _on_llm_completed)
                        crewai_event_bus.off(LLMCallFailedEvent, _on_llm_failed)
                        crewai_event_bus.off(MemoryQueryCompletedEvent, _on_memory_query)
                        crewai_event_bus.off(MemoryRetrievalCompletedEvent, _on_memory_retrieval)
                        crewai_event_bus.off(MemorySaveCompletedEvent, _on_memory_save)
                        crewai_event_bus.off(LiteAgentExecutionStartedEvent, _on_agent_started)
                        crewai_event_bus.off(LiteAgentExecutionCompletedEvent, _on_agent_completed)
                        crewai_event_bus.off(LiteAgentExecutionErrorEvent, _on_agent_error)
                    except Exception as off_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] handler unregister skipped: {off_err}")
                answer = getattr(kicked, "raw", None)
                if answer is None:
                    answer = str(kicked) if kicked is not None else ""

            _log(f"Chat agent '{role}' completed ({len(answer or '')} chars)")

            # Compose a renderable A2UI surface from the answer — the SAME shared
            # composer the exported app uses (src.shared.a2ui), run post-answer so the
            # local model only has to answer in prose, not also emit UI JSON inline.
            # Persisted as a {text, a2ui} envelope; the chat renders the surface inline
            # by default. Never blocks completion (returns None / markdown on any issue).
            result_payload: Any = answer
            try:
                from src.engines.crewai.kernel.a2ui_runner import compose_surface
                # Bounded: this is an auxiliary LLM call for the UI surface. If it
                # hangs it must NOT block the terminal status — the prose answer has
                # already streamed, so on timeout we complete with the plain answer.
                surface = await asyncio.wait_for(
                    compose_surface(
                        answer,
                        purpose=str(agent_spec.get("goal") or agent_spec.get("role") or ""),
                        query=prompt,
                        model=getattr(config, "model", None),
                        group_id=group_id,
                    ),
                    # The local model needs headroom to emit a full surface (e.g. a
                    # multi-slide presentation deck) as valid JSON; 30s was too tight
                    # and silently dropped to plain text. Still bounded so a hung
                    # compose never wedges the terminal status.
                    timeout=60,
                )
                if surface:
                    result_payload = {"text": answer, "a2ui": surface}
                    _log(f"Composed A2UI surface: {surface.get('surfaceKind', 'conversation')}")
            except asyncio.TimeoutError:
                logger.warning(
                    f"[light_agent] a2ui compose timed out for {execution_id}; "
                    "completing with plain answer"
                )
                _log("UI compose timed out — returning plain answer")
            except Exception as a2ui_err:  # noqa: BLE001
                logger.debug(f"[light_agent] a2ui compose skipped: {a2ui_err}")

            # Flush trace writes before terminal status — guarantees the
            # fire-and-forget persists (response_run + every tool trace) actually
            # land, instead of racing the request teardown — then release the
            # run's private trace session. Bounded so a slow/hung write never
            # wedges completion.
            await _flush_and_close_traces(timeout=10)

            await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=ExecutionStatus.COMPLETED.value,
                message="Light agent execution completed",
                result=result_payload,
            )
            logger.info(f"[light_agent] Completed light agent execution {execution_id}")
            return {"execution_id": execution_id, "status": ExecutionStatus.COMPLETED.value}

        except Exception as e:  # noqa: BLE001
            logger.error(f"[light_agent] Error in light agent execution {execution_id}: {e}", exc_info=True)
            _log(f"Chat agent failed: {e}")
            # Flush + release the run's private trace session. Guarded: the
            # failure may predate the persister closures being defined.
            try:
                await _flush_and_close_traces(timeout=5)
            except NameError:
                pass
            except Exception as flush_err:  # noqa: BLE001
                logger.debug(f"[light_agent] trace flush on failure skipped: {flush_err}")
            try:
                await ExecutionStatusService.update_status(
                    job_id=execution_id,
                    status=ExecutionStatus.FAILED.value,
                    message=f"Light agent execution failed: {e}",
                    result=None,
                )
            except Exception as status_err:  # noqa: BLE001
                logger.error(f"[light_agent] Could not mark execution {execution_id} FAILED: {status_err}")
            return {"execution_id": execution_id, "status": ExecutionStatus.FAILED.value, "error": str(e)}

    @staticmethod
    def _event_matches_run(
        event: Any,
        source: Any,
        *,
        agent: Any,
        agent_id: str,
        role_lower: str,
        agent_llm: Any,
    ) -> bool:
        """True if a bus event belongs to THIS in-process light run.

        Tool events reach the shared bus from several sources — the agent executor
        (``from_agent`` → ``agent_id``), the LLM's inline function caller, and MCP
        wrappers — so match on ANY reliable identity signal. Concurrent chat users
        share this in-process bus, so every clause must be run-unique (tenant-safe).

        ``source is agent_llm`` is checked FIRST: CrewAI emits tool/LLM events with
        ``source=<the LLM>``, and ``ToolUsageEvent.__init__`` copies agent_id/role off
        ``from_agent`` then nulls it — so a native function-calling / MCP tool call
        can arrive with NO agent attribution at all. The LLM instance is unique per
        run (``build_agent`` builds a fresh LLM per agent), so this is safe.
        """
        if source is not None and agent_llm is not None and source is agent_llm:
            return True
        eid = getattr(event, "agent_id", None)
        if eid is not None and agent_id and str(eid) == agent_id:
            return True
        if agent is not None and getattr(event, "agent", None) is agent:
            return True
        fa = getattr(event, "from_agent", None)
        if fa is not None and agent_id and str(getattr(fa, "id", "")) == agent_id:
            return True
        erole = getattr(event, "agent_role", None)
        if erole and role_lower and str(erole).strip().lower() == role_lower:
            return True
        return False

    @staticmethod
    def _resolve_group_id(config: Any, group_context: Optional[GroupContext]) -> str:
        """Resolve the workspace group_id for this light-agent (chat) run.

        Prefers the execution's authoritative ``config.group_id`` (set by the
        pipeline — the SAME value the crew/task path uses) over the runtime
        ``group_context``. The group_context can reach the execution worker
        without ``primary_group_id``/``group_ids`` and would otherwise fall back
        to ``"default"``, which breaks workspace-scoped lookups: e.g. an MCP
        server enabled only for this workspace resolves to 0 servers under
        ``"default"`` (find_by_names_group_scope matches group_id exactly), so
        chat "answer mode" silently dropped MCP tools while crew/research kept them.
        """
        resolved = (
            getattr(config, "group_id", None)
            or getattr(group_context, "primary_group_id", None)
            or (group_context.group_ids[0] if group_context and getattr(group_context, "group_ids", None) else None)
        )
        if resolved:
            return resolved
        # Personal-workspace fallback: when no workspace is selected (no group_id
        # header), the request carries no primary_group_id/group_ids — but the
        # user's OWN workspace id is deterministically derived from their email,
        # the SAME id GroupContext.from_email uses and that workspace-scoped MCP
        # overrides are stored under (e.g. user_nehme_tohme_databricks_com).
        # Without this the chat "answer mode" path falls back to "default", which
        # matches no MCP rows, so workspace-enabled servers resolve to 0.
        email = getattr(group_context, "group_email", None)
        if email and "@" in email:
            return GroupContext.generate_individual_group_id(email)
        return "default"

    @staticmethod
    def _build_mcp_configs(
        agent_spec: Dict[str, Any], group_id: str, user_token: Optional[str]
    ) -> tuple:
        """Build (mcp_config, mcp_call_config) for the chat agent's MCP tools.

        Threads the requesting user's OBO token through so Databricks-managed MCP
        servers (Genie, UC functions) authenticate ON BEHALF OF THE USER rather
        than the app service principal — without it MCPIntegration falls back to
        SPN and per-user Genie spaces fail with PERMISSION_DENIED. The crew/flow
        paths already pass user_token; the in-process chat path previously did not.
        ``user_token`` is omitted when absent so service-level (PAT/SPN) auth still
        applies for non-OBO runs.
        """
        mcp_config = dict(agent_spec)
        mcp_config["group_id"] = group_id
        mcp_call_config: Dict[str, Any] = {"group_id": group_id}
        if user_token:
            mcp_config["user_token"] = user_token
            mcp_call_config["user_token"] = user_token
        return mcp_config, mcp_call_config

    @staticmethod
    def _apply_genie_mcp_fixups(agent: Any) -> None:
        """Bridge a selected Genie MCP server's space id into a co-assigned
        GenieTool — the functional fixup the crew/flow ``build_task_args`` performs.

        The light agent has no Task, so it never runs ``build_task_args`` and would
        otherwise miss this (a co-assigned GenieTool errors "Genie space ID is not
        configured"). Output FORMATTING is intentionally not done here: the answer
        flows through the shared A2UI composer like every other deliverable.
        """
        from src.engines.crewai.kernel.genie_formatting import apply_genie_mcp_space_id

        # Copy the picked Genie MCP server's space id into any co-assigned GenieTool
        # (scans agent.tools) so it doesn't error "Genie space ID is not configured".
        applied_space = apply_genie_mcp_space_id([], agent)
        if applied_space:
            logger.info(
                "[light_agent] configured GenieTool spaceId '%s' from the selected "
                "Genie MCP server",
                applied_space,
            )

    async def _kickoff_with_mlflow_trace(
        self,
        agent: Any,
        kickoff_prompt: str,
        config: Any,
        execution_id: str,
        trace_context: str,
        group_context: Optional[GroupContext],
        group_id: str,
    ) -> Any:
        """Run ``agent.kickoff_async`` inside an MLflow root trace that lands in
        the SAME UC trace experiment crew/flow use, so chat LLM spans show up
        alongside crew/flow runs.

        Crew/flow run in a subprocess and call ``configure_mlflow_in_subprocess``
        (which ``set_experiment(name, trace_location=UnityCatalog(...))`` and
        flips global env). The light path runs IN-PROCESS, so instead of mutating
        the shared process we route THIS run's trace **context-locally** to the
        configured UC experiment (``mlflow.tracing.set_destination(..., context_local=True)``)
        — isolated per async task, concurrency-safe. The experiment name comes
        from the workspace's Databricks config (``mlflow_experiment_name``) + the
        ``-uc`` suffix, exactly like ``configure_mlflow_in_subprocess`` derives it.
        MLflow autolog is already enabled process-wide by ``LLMManager``, so the
        root trace is what groups the kickoff's LLM call(s) into one trace.

        CHAT-ONLY: also tags the trace with the chat ``session_id`` (and user) via
        MLflow's session metadata keys, so the MLflow UI groups a conversation's
        turns into one session. Crew/flow deliberately do NOT set this.

        Fully best-effort: any MLflow problem falls back to a plain kickoff, and
        ``kickoff_async`` runs exactly once (a real kickoff error propagates).
        """
        async def _do() -> Any:
            return await agent.kickoff_async(kickoff_prompt)

        try:
            import logging as _logging
            import mlflow
            from src.services.mlflow_tracing_service import start_root_trace
            from src.services.otel_tracing.mlflow_setup import (
                configure_mlflow_in_subprocess,
                set_trace_attributes,
                extract_trace_outputs,
            )
            from src.db.session import async_session_factory
            from src.services.databricks_service import DatabricksService

            # Load the workspace's Databricks config (same source crew/flow use).
            async with async_session_factory() as _session:
                db_config = await DatabricksService(
                    session=_session, group_id=group_id
                ).get_databricks_config()

            # Run the EXACT MLflow setup crew/flow use — auth → bind the `-uc`
            # experiment to the UC Delta-table trace location → enable native
            # autolog. The lightweight set_experiment-only variant did NOT work
            # (autolog stayed bound to the startup experiment), so we use the same
            # function crew/flow use. It configures the CURRENT process (the light
            # path is in-process), which is acceptable: traces route to the same UC
            # experiment crew/flow write to. Its verbose diagnostics are routed to
            # the "crew" logger (crew.log) — NOT the main app log — to keep the app
            # console clean.
            mlflow_result = await configure_mlflow_in_subprocess(
                db_config=db_config,
                job_id=execution_id,
                execution_id=execution_id,
                group_id=group_id,
                group_context=group_context,
                async_logger=_logging.getLogger("crew"),
            )
        except Exception as setup_err:  # noqa: BLE001
            logger.warning(
                f"[light_agent] MLflow setup failed; running without trace: {setup_err}",
                exc_info=True,
            )
            return await _do()

        if not mlflow_result or not getattr(mlflow_result, "tracing_ready", False):
            logger.info(
                "[light_agent] MLflow tracing not ready (enabled=%s, error=%s) — "
                "running chat kickoff without an MLflow trace",
                getattr(mlflow_result, "enabled", None),
                getattr(mlflow_result, "error", None),
            )
            return await _do()
        uc_name = getattr(mlflow_result, "experiment_name", None)

        run_name = (getattr(config, "inputs", None) or {}).get("run_name")
        flow_config = {
            "model": getattr(config, "model", None),
            "execution_type": "agent",
            "run_name": run_name,
        }
        # Show the ACTUAL prompt sent to the model (conversation preamble + the
        # user's current message) on the trace, not the generic one-line task
        # label — so the MLflow trace's Inputs reflect the real request. Capped so
        # a long conversation doesn't bloat the trace.
        _prompt_for_trace = kickoff_prompt if isinstance(kickoff_prompt, str) else str(kickoff_prompt)
        if len(_prompt_for_trace) > 20000:
            _prompt_for_trace = _prompt_for_trace[:20000] + "…[truncated]"
        inputs = {"run_name": run_name or trace_context, "prompt": _prompt_for_trace}
        # session_id can ride on the config top-level OR inside inputs depending on
        # the entry path (the service copies it into execution_config["session_id"]).
        # Read both so the MLflow session tag is set consistently across runs.
        session_id = (
            getattr(config, "session_id", None)
            or (getattr(config, "inputs", None) or {}).get("session_id")
        )
        user = getattr(group_context, "group_email", None)

        logger.info(
            "[light_agent] MLflow tracing chat kickoff → experiment=%s session=%s",
            uc_name,
            session_id,
        )
        with start_root_trace(f"chat_kickoff:{run_name or trace_context}", inputs) as root_span:
            # CHAT-ONLY: group this conversation's turns into one MLflow session.
            if session_id:
                try:
                    metadata = {"mlflow.trace.session": str(session_id)}
                    if user:
                        metadata["mlflow.trace.user"] = str(user)
                    mlflow.update_current_trace(metadata=metadata)
                except Exception as sess_err:  # noqa: BLE001
                    logger.debug(f"[light_agent] mlflow session tag skipped: {sess_err}")
            try:
                set_trace_attributes(root_span, flow_config, logger, run_name=run_name)
            except Exception:  # noqa: BLE001
                pass
            result = await _do()
            try:
                outputs = extract_trace_outputs(result, logger)
                if outputs and root_span is not None and hasattr(root_span, "set_outputs"):
                    root_span.set_outputs(outputs)
            except Exception:  # noqa: BLE001
                pass
            return result

    async def _conversation_preamble(
        self,
        config: Any,
        group_context: Optional[GroupContext],
        group_id: str,
        log,
    ) -> str:
        """Recent turns of THIS chat session as a short transcript, weighted so
        the user's own statements survive a long/bloated conversation.

        Each light-agent turn is an isolated ``kickoff_async`` with no built-in
        conversation history, so without this the assistant cannot recall what was
        said earlier (e.g. the user's name). Read-only and best-effort: returns
        ``""`` on any issue. The current turn (the just-written user row + its
        ``Thinking...`` / ``[ui-card]`` placeholder rows) is excluded.

        Scoring (why this beats a flat "last N turns" window): a long chat is
        dominated by large ASSISTANT outputs (decks, reports) that would otherwise
        push the short, high-signal USER facts out of the window. So USER turns are
        prioritized — every user turn is kept (capped per-turn), and only the most
        recent assistant turns are kept and hard-truncated. Under the overall
        character budget the OLDEST assistant turns are dropped first; user turns
        are never dropped. The header also tells the model to treat the user's
        statements as authoritative.
        """
        session_id = getattr(config, "session_id", None)
        if not session_id:
            return ""
        group_ids = list(getattr(group_context, "group_ids", None) or [])
        if not group_ids and group_id and group_id != "default":
            group_ids = [group_id]
        if not group_ids:
            return ""

        # Tunables (env-overridable). Defaults favor keeping user facts.
        recent_limit = int(os.getenv("CHAT_HISTORY_RECENT_LIMIT", "120"))
        user_cap = int(os.getenv("CHAT_HISTORY_USER_CHAR_CAP", "500"))
        assistant_cap = int(os.getenv("CHAT_HISTORY_ASSISTANT_CHAR_CAP", "240"))
        max_assistant_turns = int(os.getenv("CHAT_HISTORY_MAX_ASSISTANT_TURNS", "8"))
        max_chars = int(os.getenv("CHAT_HISTORY_MAX_CHARS", "6000"))

        try:
            from src.db.session import request_scoped_session
            from src.repositories.chat_history_repository import (
                ChatHistoryRepository,
            )
            async with request_scoped_session() as db_session:
                # MOST RECENT window (not the oldest page) — a session longer than
                # one page must still recall what was just said.
                messages = await ChatHistoryRepository(
                    db_session
                ).get_recent_by_session_and_group(session_id, group_ids, limit=recent_limit)
        except Exception as hist_err:  # noqa: BLE001
            logger.debug(f"[light_agent] chat history fetch skipped: {hist_err}")
            return ""

        # Drop the current turn: everything from the LAST 'user' row onward is
        # this run (that user message + its placeholder assistant rows).
        last_user = -1
        for i, m in enumerate(messages):
            if getattr(m, "message_type", "") == "user":
                last_user = i
        prior = messages[:last_user] if last_user >= 0 else list(messages)

        placeholders = {"thinking...", "[ui-card]", ""}
        # Build (role, "User: ..."/"Assistant: ..." line) keeping chronological order.
        entries: list = []  # list of (role, line)
        for m in prior:
            mtype = getattr(m, "message_type", "")
            if mtype not in ("user", "assistant"):
                continue
            content = (getattr(m, "content", "") or "").strip()
            if content.lower() in placeholders or content.startswith("[ui-card]"):
                continue
            cap = user_cap if mtype == "user" else assistant_cap
            if len(content) > cap:
                content = content[:cap] + "…"
            label = "User" if mtype == "user" else "Assistant"
            entries.append((mtype, f"{label}: {content}"))

        if not entries:
            return ""

        # Keep ALL user turns; keep only the most recent N assistant turns.
        assistant_positions = [i for i, (role, _) in enumerate(entries) if role == "assistant"]
        keep_assistant = set(assistant_positions[-max_assistant_turns:])
        selected = [
            (role, line)
            for i, (role, line) in enumerate(entries)
            if role == "user" or i in keep_assistant
        ]

        # Enforce the character budget by dropping the OLDEST assistant turns
        # first; user turns are never dropped (they carry the facts to recall).
        def _total(items) -> int:
            return sum(len(line) + 1 for _, line in items)

        while selected and _total(selected) > max_chars:
            drop_at = next((i for i, (role, _) in enumerate(selected) if role == "assistant"), None)
            if drop_at is None:
                break  # only user turns remain — keep them even if slightly over
            selected.pop(drop_at)

        lines = [line for _, line in selected]
        user_count = sum(1 for role, _ in selected if role == "user")
        log(
            f"Recalling {len(lines)} prior message(s) from this chat session "
            f"({user_count} from you)"
        )
        return (
            "Conversation so far in THIS chat session (most recent last). The "
            "User's statements below are authoritative facts about the user and "
            "their request — rely on them directly when answering (e.g. the user's "
            "name, preferences, and earlier instructions):\n" + "\n".join(lines)
        )

    async def _attach_memory(
        self,
        agent: Any,
        agent_spec: Dict[str, Any],
        config: Any,
        group_context: Optional[GroupContext],
        group_id: str,
        prompt: str,
        execution_id: str,
        log,
    ) -> None:
        """Attach a unified cognitive ``Memory`` to the single agent so
        ``Agent.kickoff_async`` auto-recalls relevant context and persists the
        turn — chat-mode parity with crews.

        Composes the EXISTING public ``CrewMemoryService`` building blocks (same
        backend selection, embedder, group/session scoping and crew-id rules the
        crew path uses) — it does NOT modify the crew path. Best-effort: any
        failure leaves the agent memory-less so the chat still answers.

        Memory is ON by default; the chat "No memory" toggle arrives as
        ``agent_spec['memory'] is False`` and is honored here.
        """
        if agent_spec.get("memory") is False:
            log("Memory disabled for this run")
            return
        try:
            from src.engines.crewai.memory.crew_memory_service import CrewMemoryService
            from src.engines.crewai.config.crew_config_builder import CrewConfigBuilder
            from src.engines.crewai.config.embedder_config_builder import EmbedderConfigBuilder
            from src.schemas.memory_backend import MemoryBackendConfig

            user_token = getattr(group_context, "access_token", None)

            # Config the memory services read: group is the tenant boundary;
            # session_id + memory_workspace_scope drive recall scope; agents/tasks
            # feed the deterministic crew-id hash + the embedder's agent scan.
            workspace_scope = getattr(config, "memory_workspace_scope", None)
            mem_config: Dict[str, Any] = {
                "group_id": group_id,
                "session_id": getattr(config, "session_id", None),
                "memory_workspace_scope": True if workspace_scope is None else bool(workspace_scope),
                "model": getattr(config, "model", None),
                "name": "chat",
                "execution_id": execution_id,
                "job_id": execution_id,
                "agents": [agent_spec],
                "tasks": [{"description": prompt}],
            }

            memory_service = CrewMemoryService(mem_config, user_token)
            config_builder = CrewConfigBuilder(mem_config)

            # crew_kwargs-shaped dict so the existing helpers operate on our agent.
            crew_kwargs: Dict[str, Any] = {"agents": [agent], "memory": True}

            # Embedder (Databricks/Lakebase need a custom one; default → None).
            embedder_builder = EmbedderConfigBuilder(mem_config, user_token)
            crew_kwargs, custom_embedder, _embedder_config = await embedder_builder.configure_embedder(crew_kwargs)

            # Backend config from DB → default (LanceDB) fallback.
            memory_backend_config = await memory_service.fetch_memory_backend_config()
            if not memory_backend_config:
                memory_backend_config = {"backend_type": "default"}

            crew_id = memory_service.generate_crew_id()
            memory_service.setup_storage_directory(crew_id, memory_backend_config)

            # "Disabled Configuration" → all memory types off → no memory.
            if config_builder.check_memory_disabled_by_backend_config(memory_backend_config):
                log("Memory backend is the 'Disabled Configuration' — no memory")
                return

            backend_type = memory_backend_config.get("backend_type")
            embedder_for_backend = (
                custom_embedder if backend_type in ("databricks", "lakebase")
                else crew_kwargs.get("embedder")
            )
            unified_storage = await memory_service.create_unified_storage(
                memory_backend_config, crew_id, embedder_for_backend
            )
            memory_config = MemoryBackendConfig(**memory_backend_config)
            memory_llm_override = await memory_service.resolve_memory_llm_override(memory_config)

            # Builds crew_kwargs['memory'] = Memory(...) AND sets agent.memory via
            # _attach_crew_memory_to_agents (it iterates crew_kwargs['agents']).
            memory_service.configure_crew_memory_components(
                crew_kwargs,
                memory_config,
                unified_storage,
                crew_id,
                custom_embedder,
                memory_llm_override=memory_llm_override,
            )

            attached = getattr(agent, "memory", None)
            if attached not in (None, True, False):
                scope = (
                    "session"
                    if (mem_config["session_id"] and not mem_config["memory_workspace_scope"])
                    else "workspace"
                )
                log(f"Memory enabled ({backend_type}, {scope} scope) — recall + persist")
            else:
                log("Memory unavailable for this run (no backend/embedder)")
        except Exception as mem_err:  # noqa: BLE001
            logger.warning(f"[light_agent] memory setup skipped: {mem_err}")


async def run_light_agent(
    execution_id: str,
    config: Any,
    group_context: Optional[GroupContext] = None,
    session=None,
) -> Dict[str, Any]:
    """Module-level entry point for the single-agent ("chat"/light) run.

    Mirrors ``run_crew_in_process`` (crew path) and ``run_flow_in_process``
    (flow path): the light path exposes its run entry point here, in the light
    agent module itself, alongside the :class:`LightAgentService` that holds the
    logic. The crewai-engine path refactor split the engine into
    ``paths/{crew,flow,light_agent}``; this previously lived (misplaced) in
    ``paths/crew/execution_runner`` and is now back where it belongs. Kept as a
    thin function so existing imports/tests referencing ``run_light_agent`` work.
    """
    return await LightAgentService().run_light_agent_execution(
        execution_id, config, group_context, session
    )
