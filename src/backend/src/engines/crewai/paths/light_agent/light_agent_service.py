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

            group_id = (
                getattr(group_context, "primary_group_id", None)
                or (group_context.group_ids[0] if group_context and group_context.group_ids else None)
                or "default"
            )
            group_email = getattr(group_context, "group_email", None)
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
            _log(f"🚀 Chat agent '{role}' started")
            _log(f"📝 Prompt: {trace_context}")

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

            async def _persist_trace(trace_data: Dict[str, Any]) -> None:
                try:
                    from src.services.execution_trace_service import ExecutionTraceService
                    async with request_scoped_session() as trace_session:
                        await ExecutionTraceService(trace_session).create_trace(trace_data)
                        await trace_session.commit()
                except Exception as persist_err:  # noqa: BLE001
                    logger.debug(f"[light_agent] trace persist skipped: {persist_err}")

            def _schedule_trace(trace_data: Dict[str, Any]) -> None:
                if _main_loop is None:
                    return
                try:
                    fut = asyncio.run_coroutine_threadsafe(_persist_trace(trace_data), _main_loop)
                    fut.add_done_callback(lambda f: f.exception())  # drain, never raise
                except Exception as sched_err:  # noqa: BLE001
                    logger.debug(f"[light_agent] trace schedule skipped: {sched_err}")

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

                mcp_config = dict(agent_spec)
                mcp_config["group_id"] = group_id

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
                    mcp_call_config={"group_id": group_id},
                )

                # ── Cognitive memory (recall + persist) — chat parity w/ crews ──
                # Attach a unified Memory so kickoff_async auto-recalls relevant
                # context and persists this turn. Best-effort: never breaks the run.
                await self._attach_memory(
                    agent, agent_spec, config, group_context,
                    group_id, prompt, execution_id, _log,
                )

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

                _agent_id = str(getattr(agent, "id", "") or "")
                # Mutable holder so the (sync, possibly worker-thread) started/completed
                # handlers can share the kickoff start time to compute a duration.
                _agent_started_at: list = []

                def _matches(event) -> bool:
                    eid = getattr(event, "agent_id", None)
                    if eid is not None and _agent_id:
                        return str(eid) == _agent_id
                    return getattr(event, "agent", None) is agent  # identity fallback

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
                        if not _matches(event):
                            return
                        tool_name = str(getattr(event, "tool_name", "") or "tool")
                        args = _args_str(event)
                        _log(f"🔧 Using tool: {tool_name}({args[:200]})")
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
                        if not _matches(event):
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
                        _log(f"✅ Tool {tool_name} returned ({len(content)} chars)")
                        _schedule_trace(_base_trace(f"{norm}_run", output, tool_name))
                    except Exception as h_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] tool-finish trace skipped: {h_err}")

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
                        _log(f"✅ Response generated ({len(answer_text)} chars)")
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
                crewai_event_bus.register_handler(LiteAgentExecutionStartedEvent, _on_agent_started)
                crewai_event_bus.register_handler(LiteAgentExecutionCompletedEvent, _on_agent_completed)
                crewai_event_bus.register_handler(LiteAgentExecutionErrorEvent, _on_agent_error)

                logger.info(f"[light_agent] Kicking off single agent for execution {execution_id}")
                try:
                    kickoff_prompt = (
                        f"{conversation_preamble}\n\nCurrent message:\n{prompt}"
                        if conversation_preamble else prompt
                    )
                    kicked = await agent.kickoff_async(kickoff_prompt)
                finally:
                    # Always unregister so handlers never leak on the global bus.
                    try:
                        crewai_event_bus.off(ToolUsageStartedEvent, _on_tool_started)
                        crewai_event_bus.off(ToolUsageFinishedEvent, _on_tool_finished)
                        crewai_event_bus.off(LiteAgentExecutionStartedEvent, _on_agent_started)
                        crewai_event_bus.off(LiteAgentExecutionCompletedEvent, _on_agent_completed)
                        crewai_event_bus.off(LiteAgentExecutionErrorEvent, _on_agent_error)
                    except Exception as off_err:  # noqa: BLE001
                        logger.debug(f"[light_agent] handler unregister skipped: {off_err}")
                answer = getattr(kicked, "raw", None)
                if answer is None:
                    answer = str(kicked) if kicked is not None else ""

            _log(f"✅ Chat agent '{role}' completed ({len(answer or '')} chars)")

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
                    _log(f"🎨 Composed A2UI surface: {surface.get('surfaceKind', 'conversation')}")
            except asyncio.TimeoutError:
                logger.warning(
                    f"[light_agent] a2ui compose timed out for {execution_id}; "
                    "completing with plain answer"
                )
                _log("⚠️ UI compose timed out — returning plain answer")
            except Exception as a2ui_err:  # noqa: BLE001
                logger.debug(f"[light_agent] a2ui compose skipped: {a2ui_err}")

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
            _log(f"❌ Chat agent failed: {e}")
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
            f"🧠 Recalling {len(lines)} prior message(s) from this chat session "
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
            log("🧠 Memory disabled for this run")
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
                log("🧠 Memory backend is the 'Disabled Configuration' — no memory")
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
                log(f"🧠 Memory enabled ({backend_type}, {scope} scope) — recall + persist")
            else:
                log("🧠 Memory unavailable for this run (no backend/embedder)")
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
