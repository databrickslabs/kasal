"""
Flow methods module for CrewAI flow execution.

This module handles dynamic creation of flow methods (starting points, listeners, routers).
"""

import asyncio
import logging
import uuid
from typing import Any, Callable, Dict, List, Optional

from src.core.llm.output_cap import output_cap
from src.core.logger import LoggerManager
from src.services.execution.runtime import Crew, Process, Task
from src.services.flow_builder.runtime import and_, listen, or_, router, start

from .flow_state import FlowStateManager

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow

# Per-crew kickoff timeout for flow execution. Large Power BI models (hundreds of
# measures, dozens of fact tables + opt-in LLM DAX translation) can legitimately
# take longer than the original 10 min. Bumped to 20 min as headroom; the DAX LLM
# fallback is also now bounded-concurrent so it finishes far faster than before.
CREW_KICKOFF_TIMEOUT_SECONDS = 1200.0


def extract_final_answer(results) -> str:
    """
    Extract only the final answer from flow results, excluding the thinking process.

    CrewAI agent outputs often include the full thinking process followed by "Final Answer:".
    This function extracts only the final answer portion for cleaner context passing between
    crews in a flow.

    Args:
        results: Flow results which can be:
            - List of dicts with 'content' key
            - CrewOutput/TaskOutput object with 'raw' attribute
            - String
            - Other iterable

    Returns:
        str: The extracted final answer, or the full content if no "Final Answer:" marker found
    """
    if not results:
        return ""

    # Get the first result
    first_result = results[0] if hasattr(results, "__getitem__") else results

    # Handle list of dicts with 'content' key
    if isinstance(first_result, list):
        # Multiple content items - extract final answer from each and join
        contents = []
        for item in first_result:
            if isinstance(item, dict) and "content" in item:
                content = item["content"]
                # Extract only the Final Answer portion if present
                if "Final Answer:" in content:
                    # Get everything after "Final Answer:"
                    final_answer_part = content.split("Final Answer:")[-1].strip()
                    contents.append(final_answer_part)
                elif "Final Answer" in content:
                    # Handle case without colon
                    final_answer_part = content.split("Final Answer")[-1].strip()
                    # Remove leading colon or newline if present
                    final_answer_part = final_answer_part.lstrip(":").strip()
                    contents.append(final_answer_part)
                else:
                    contents.append(content)
            elif isinstance(item, str):
                contents.append(item)
        return "\n\n".join(contents)

    # Handle dict with 'content' key
    if isinstance(first_result, dict) and "content" in first_result:
        content = first_result["content"]
    # Handle objects with 'raw' attribute (TaskOutput, CrewOutput)
    elif hasattr(first_result, "raw") and first_result.raw:
        content = str(first_result.raw)
    # Handle string
    elif isinstance(first_result, str):
        content = first_result
    else:
        # Fallback to string conversion
        content = str(first_result)

    # Extract only the Final Answer portion if present
    if "Final Answer:" in content:
        return content.split("Final Answer:")[-1].strip()
    elif "Final Answer" in content:
        final_answer_part = content.split("Final Answer")[-1].strip()
        return final_answer_part.lstrip(":").strip()

    return content


async def get_model_context_limits(agent, group_context) -> tuple[int, int]:
    """
    Get the context window and max output tokens for the agent's model using ModelConfigService.

    Args:
        agent: CrewAI Agent instance with llm attribute
        group_context: Group context for multi-tenant isolation

    Returns:
        tuple[int, int]: (context_window_tokens, max_output_tokens), defaults to (128000, 16000) if not found
    """
    default_context_window = 128000
    default_max_output = 16000

    try:
        # Get the model name from agent's llm attribute
        model_name = None
        if hasattr(agent, "llm") and agent.llm:
            # The agent.llm could be a LiteLLM instance or string
            if isinstance(agent.llm, str):
                model_name = agent.llm
            elif hasattr(agent.llm, "model"):
                model_name = agent.llm.model
            else:
                logger.warning(f"Agent LLM has unknown type: {type(agent.llm)}")
                return default_context_window, default_max_output

        if not model_name:
            logger.info("No model name found for agent, using defaults")
            return default_context_window, default_max_output

        # Extract group_id from group_context
        group_id = None
        if group_context:
            if hasattr(group_context, "primary_group_id"):
                group_id = group_context.primary_group_id
            elif hasattr(group_context, "group_ids") and group_context.group_ids:
                group_id = group_context.group_ids[0]

        if not group_id:
            logger.info("No group_id found, using defaults")
            return default_context_window, default_max_output

        # Use ModelConfigService to get model configuration
        from src.db.session import request_scoped_session
        from src.services.settings.models import ModelConfigService

        async with request_scoped_session() as session:
            model_config_service = ModelConfigService(session, group_id)
            model_config = await model_config_service.find_by_key(model_name)

            if model_config:
                context_window = (
                    model_config.context_window
                    if hasattr(model_config, "context_window")
                    and model_config.context_window
                    else default_context_window
                )
                max_output = (
                    model_config.max_output_tokens
                    if hasattr(model_config, "max_output_tokens")
                    and model_config.max_output_tokens
                    else default_max_output
                )

                logger.info(
                    f"Model {model_name}: context_window={context_window}, max_output_tokens={max_output}"
                )
                return context_window, max_output

            logger.info(f"No model config found for {model_name}, using defaults")
            return default_context_window, default_max_output

    except Exception as e:
        logger.warning(f"Error getting model config: {e}, using defaults")
        return default_context_window, default_max_output


async def configure_flow_crew_memory(
    crew_kwargs: Dict[str, Any],
    agents: List[Any],
    task_list: List[Any],
    crew_name: str,
    group_id: Optional[str],
    user_token: Optional[str],
) -> Dict[str, Any]:
    """Wire the unified Databricks/Lakebase memory backend into a flow crew.

    Crews built inside a Flow never went through ``CrewMemoryService`` like the
    regular crew path (crew_preparation step 8), so they fell back to CrewAI's
    default LanceDB + OpenAI embedder and failed with
    ``CHROMA_OPENAI_API_KEY is not set``. This mirrors the crew-mode setup:
    fetch the backend config, build the Databricks embedder, create the unified
    storage, and attach the configured ``Memory`` to the crew + its agents.
    Falls back gracefully (CrewAI default) when no backend is configured.
    """
    from src.schemas.memory_backend import MemoryBackendConfig as MemBackConfig
    from src.services.execution.config.embedder_config_builder import (
        EmbedderConfigBuilder,
    )
    from src.services.memory.crew_memory import CrewMemoryService

    model = None
    if agents and getattr(agents[0], "llm", None) is not None:
        model = getattr(agents[0].llm, "model", None)

    mem_service_config = {
        "group_id": group_id,
        "agents": [{"role": getattr(a, "role", "")} for a in agents],
        "tasks": [
            {"description": getattr(t, "description", "") or getattr(t, "name", "")}
            for t in task_list
        ],
        "name": crew_name,
        "model": model,
    }
    memory_service = CrewMemoryService(mem_service_config, user_token=user_token)

    try:
        memory_backend_config = await memory_service.fetch_memory_backend_config()
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(f"[FLOW MEMORY] Could not fetch memory backend config: {e}")
        memory_backend_config = None

    # Build the embedder (callable for databricks/lakebase, provider dict otherwise).
    custom_embedder = None
    try:
        embedder_build_config = {
            "agents": [{"embedder_config": None}],
            "group_id": group_id,
        }
        embedder_builder = EmbedderConfigBuilder(embedder_build_config, user_token)
        crew_kwargs, custom_embedder, _ = await embedder_builder.configure_embedder(
            crew_kwargs
        )
    except Exception as e:
        logger.warning(f"[FLOW MEMORY] Embedder configuration failed: {e}")

    if not memory_backend_config:
        # Mirror the crew path (crew_preparation step 8): when no ACTIVE backend
        # config exists (e.g. the "Disabled Configuration" row → get_active_config
        # returns None), fall back to the DEFAULT local backend and STILL run the
        # memory wiring below — so the Databricks embedder is attached to the crew
        # + agents. Returning here would leave crew_kwargs["memory"]=True, so
        # CrewAI builds its OWN default Memory (ChromaDB + OpenAI embedder) and the
        # save_to_memory / search_memory tools fail with
        # "CHROMA_OPENAI_API_KEY is not set". (If no embedder can be built,
        # configure_crew_memory_components disables memory gracefully.)
        memory_backend_config = {"backend_type": "default"}

    try:
        crew_id = memory_service.generate_crew_id()
        # Align flow memory with the crew path: point CREWAI_STORAGE_DIR at the
        # deterministic local store so flow DEFAULT memory writes/reads the same
        # place crews do (the known KASAL_MEMORY_DIR root), not CrewAI's default
        # location. Crews do this in crew_preparation right after generate_crew_id.
        memory_service.setup_storage_directory(crew_id, memory_backend_config)
        backend_type = memory_backend_config.get("backend_type")
        embedder_for_backend = (
            custom_embedder
            if backend_type in ("databricks", "lakebase")
            else crew_kwargs.get("embedder")
        )
        unified_storage = await memory_service.create_unified_storage(
            memory_backend_config, crew_id, embedder_for_backend
        )
        memory_config = MemBackConfig(**memory_backend_config)
        # Resolve the optional memory-analysis LLM override into a configured
        # instance so CrewAI Memory doesn't fall back to OpenAI and 401.
        memory_llm_override = await memory_service.resolve_memory_llm_override(
            memory_config
        )
        crew_kwargs = memory_service.configure_crew_memory_components(
            crew_kwargs,
            memory_config,
            unified_storage,
            crew_id,
            custom_embedder,
            memory_llm_override=memory_llm_override,
        )
        # Register for end-of-run maintenance. A flow builds a Memory per crew
        # here, deep inside the flow's wiring, and the subprocess teardown that
        # drains the write pool has no reference to any of them — so without
        # this a flow-only workspace would never consolidate its memory.
        try:
            from src.services.memory.maintenance import (
                register_memory_for_maintenance,
            )

            register_memory_for_maintenance(crew_kwargs.get("memory"))
        except Exception as reg_err:  # noqa: BLE001 — maintenance is best-effort
            logger.debug(f"[FLOW MEMORY] Maintenance registration skipped: {reg_err}")
        logger.info(
            f"[FLOW MEMORY] Configured unified memory (backend={backend_type}, crew_id={crew_id})"
        )
    except Exception as e:
        logger.warning(f"[FLOW MEMORY] Failed to configure unified memory backend: {e}")
        # Disable memory rather than leave crew_kwargs["memory"]=True — a bare
        # True makes CrewAI build its own ChromaDB+OpenAI Memory and fail with
        # "CHROMA_OPENAI_API_KEY is not set". Graceful degradation = no memory.
        crew_kwargs["memory"] = False

    return crew_kwargs


def _dedupe_flow_agent_task_tools(agents: List[Any], task_list: List[Any]) -> None:
    """Strip an agent's tool when the SAME tool (by name) is on a task it runs.

    Flow crews are assembled from already-built CrewAI ``Agent``/``Task`` objects,
    so we de-dupe on the instantiated tools' ``.name`` rather than config ids.
    For each task with an assigned agent, any agent tool whose name also appears
    among the task's tools is removed from the agent — the task-level instance is
    the configured, authoritative one. Best-effort: never raises (a flow must run
    even if this normalization can't be applied).

    BEHAVIOR-PRESERVING GUARD: CrewAI lets a tool-less task inherit its agent's
    tools. So an agent that also runs a task with NO tools is skipped entirely —
    stripping its tools would strand that inheriting task. Identical tool set to
    before for every crew except the exact duplicate case being fixed.
    """
    try:
        # Agents (by identity) that have at least one tool-less task → skip them.
        agents_with_inheriting_task = set()
        for task in task_list:
            agent = getattr(task, "agent", None)
            if agent is None:
                continue
            if not (getattr(task, "tools", None) or []):
                agents_with_inheriting_task.add(id(agent))

        for task in task_list:
            agent = getattr(task, "agent", None)
            if agent is None:
                continue
            if id(agent) in agents_with_inheriting_task:
                continue
            task_tools = getattr(task, "tools", None) or []
            agent_tools = getattr(agent, "tools", None) or []
            if not task_tools or not agent_tools:
                continue
            task_tool_names = {
                getattr(t, "name", None) for t in task_tools if getattr(t, "name", None)
            }
            if not task_tool_names:
                continue
            kept = [
                t
                for t in agent_tools
                if getattr(t, "name", None) not in task_tool_names
            ]
            if len(kept) != len(agent_tools):
                removed = [
                    getattr(t, "name", "?")
                    for t in agent_tools
                    if getattr(t, "name", None) in task_tool_names
                ]
                agent.tools = kept
                agent_role = getattr(agent, "role", "Unknown")
                logger.info(
                    f"[FLOW] Agent '{agent_role}': removed tool(s) {removed} also "
                    f"present on its task — built at task level with the task's "
                    f"config (avoids an empty-config duplicate tool instance)."
                )
    except Exception as e:  # noqa: BLE001 — normalization must never break a flow
        logger.debug(f"[FLOW] agent∩task tool de-dupe skipped: {e}")


def crew_inputs_from_state(flow: Any) -> Dict[str, Any]:
    """The flow's state, as inputs for a crew it is about to run.

    The missing link between a flow receiving a value and that value MEANING
    anything. A flow merges its inputs into state, but a crew interpolates
    `{placeholders}` from the inputs passed to ITS kickoff — and both crew call
    sites passed none. Observed on a real run: `topic="swiss news"` reached flow
    state correctly, and the crew then executed a task that still read
    "related to a specified {topic}" literally, searched for whatever its memory
    suggested, and returned a raw tool payload.

    ``id`` is dropped: it is the checkpoint handle, not something a task should
    ever interpolate.

    Best-effort — a flow with no state, or a state that is not a mapping, simply
    passes nothing, exactly as before.
    """
    state = getattr(flow, "state", None)
    if isinstance(state, dict):
        return {k: v for k, v in state.items() if k != "id"}
    if state is None:
        return {}

    # A typed state dumps itself. `vars()` would work for the declared fields
    # but miss everything written at runtime — pydantic keeps extras off
    # `__dict__` — and those are exactly the values a downstream task
    # interpolates: a state operation's output variable is written, never
    # declared. `model_dump` includes them.
    dump = getattr(state, "model_dump", None)
    if callable(dump):
        try:
            return {k: v for k, v in dump().items() if k != "id"}
        except Exception as exc:  # noqa: BLE001 — inputs must not fail a kickoff
            logger.debug(f"could not dump flow state for crew inputs: {exc}")
    if hasattr(state, "__dict__"):
        return {
            k: v for k, v in vars(state).items() if k != "id" and not k.startswith("_")
        }
    return {}


def attach_memory_seams(crew: Any, crew_label: str, request: str | None = None) -> None:
    """Wire runtime recall + per-task persistence onto an already-built crew.

    MUST be called for every flow crew that has memory configured. The engine
    carries ``Crew.memory`` but its execution loop never consults it (see
    ``memory/memory_hooks.py``): recall happens through ``crew.context_providers``
    and persistence through ``crew.output_sinks``. A crew built with
    ``memory=True`` and a fully configured backend but without these seams is
    silently memory-less — it neither reads nor writes, with no error to show it.

    Best-effort by design: a broken memory backend must never break a flow.
    """
    try:
        from src.services.memory.hooks import (
            make_memory_context_provider,
            make_memory_output_sink,
        )

        crew_memory = getattr(crew, "memory", None)
        # ``request`` leads the recall query so a saved crew does not match its
        # own history on template text alone. Always None here today: a flow
        # cannot receive inputs at all yet (kickoff_async takes none), so there
        # is nothing carrying the run's request. Wire it when that lands.
        memory_provider = make_memory_context_provider(crew_memory, request)
        if memory_provider is not None:
            crew.context_providers.append(memory_provider)
        memory_sink = make_memory_output_sink(crew_memory)
        if memory_sink is not None:
            crew.output_sinks.append(memory_sink)
        if memory_provider is not None or memory_sink is not None:
            logger.info(
                f"Memory recall provider + persist sink attached to {crew_label}"
            )
        else:
            logger.info(
                f"No usable memory instance on {crew_label} — recall/persist not attached"
            )
    except Exception as mem_err:
        logger.warning(f"Flow crew memory wiring skipped (non-fatal): {mem_err}")


def collect_task_agents(task_list: List[Any]) -> List[Any]:
    """Unique agents assigned to ``task_list``, in first-seen task order.

    Deliberately not ``list(set(t.agent for t in task_list))``: the engine's
    ``Agent`` is a non-frozen Pydantic model, so Pydantic sets ``__hash__ = None``
    and ``set()`` raises ``TypeError: unhashable type: 'Agent'`` — every flow with
    a listener or router died there. Identity de-dupe via ``id()`` matches
    ``_dedupe_flow_agent_task_tools`` above, and preserving task order makes the
    derived crew name (``agents[0].role``) deterministic rather than dependent on
    set iteration order. Tasks with no agent are skipped instead of contributing a
    ``None`` to the crew's agent list.
    """
    agents: List[Any] = []
    seen = set()
    for task in task_list:
        agent = getattr(task, "agent", None)
        if agent is None or id(agent) in seen:
            continue
        seen.add(id(agent))
        agents.append(agent)
    return agents


def _emit_checkpoint_restored(crew_name: str, output: Any) -> None:
    """Put a restored crew on the trace, marked as restored.

    A skipped crew used to emit nothing, so a resumed run's timeline showed
    only the part that re-ran and read as a partial job. This gives it a row
    of its own — ``crew_checkpoint_restored`` — rather than a synthetic
    completion event, so the trace shows the whole flow WITHOUT claiming the
    crew executed.

    Fail-open: a trace row is never worth failing a run for.
    """
    try:
        from src.core.events import event_bus
        from src.core.events.types import CrewCheckpointRestoredEvent

        event_bus.emit(
            None,
            CrewCheckpointRestoredEvent(crew_name=crew_name, output=output),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            f"Could not emit checkpoint-restored event for '{crew_name}' "
            f"(non-fatal): {e}"
        )


def _crew_identity(crew_name: Optional[str], task_list: Any) -> Optional[str]:
    """The content hash of the crew about to run, or None if it cannot be taken.

    The SAME identity the resume path uses, computed from the same runtime
    objects — so "has this crew changed" cannot be answered two different ways
    depending on which feature is asking.
    """
    try:
        from src.services.flow_builder.checkpoint_identity import (
            compute_crew_identity,
        )

        return compute_crew_identity(crew_name, task_list or [])
    except Exception as exc:  # noqa: BLE001 — identity is a guard, not a gate
        logger.debug(f"[flow-reuse] could not compute crew identity: {exc}")
        return None


def _reuse_check(flow: Any, crew_name: Optional[str], task_list: Any) -> Optional[Any]:
    """A previous turn's answer for this crew, when reusing it is safe.

    None means run. Every doubtful case returns None — see `flow_reuse`.
    """
    if not crew_name:
        return None
    try:
        from src.services.flow_builder.conversation.reuse import (
            crews_that_answer,
            emit_reused,
            reusable_output,
            reuse_enabled,
        )

        config = getattr(flow, "_kasal_flow_config", None) or {}
        state_config = config.get("state") if isinstance(config, dict) else None
        refresh = (
            bool(config.get("refresh_outputs")) if isinstance(config, dict) else False
        )
        if not reuse_enabled(state_config, refresh):
            return None

        # Prefer the hash taken when the flow was compiled: it is the one the
        # outcome registry carries, so "has this crew changed" is answered the
        # same way wherever it is asked.
        identity = (getattr(flow, "_kasal_crew_identities", {}) or {}).get(
            crew_name
        ) or _crew_identity(crew_name, task_list)
        state = getattr(flow, "state", None)
        output = reusable_output(
            state,
            crew_name,
            identity,
            # Terminal crews AND the one this turn selected. Passing only the
            # terminal set let narrowing choose a crew and reuse skip it, so the
            # turn ran nothing and the chat got no answer.
            crews_that_answer(config, flow),
            refresh,
        )
        if output is None:
            return None
        logger.info(
            f"♻️  Reusing '{crew_name}' output from an earlier turn — not running it again"
        )
        emit_reused(crew_name, output)
        return output
    except Exception as exc:  # noqa: BLE001 — never block a run over reuse
        logger.debug(f"[flow-reuse] check failed, running the crew: {exc}")
        return None


def _store_output_slug(flow: Any, crew_name: Optional[str], result: Any) -> None:
    """Store a crew's output under an interpolatable name as well."""
    try:
        from src.services.flow_builder.conversation.reuse import output_slug

        slug = output_slug(crew_name or "")
        if slug and slug != crew_name:
            flow.state[slug] = result
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[flow-reuse] could not store output slug: {exc}")


def _record_crew_identity(flow: Any, crew_name: Optional[str], task_list: Any) -> None:
    """Stamp which crew produced the output just written into state."""
    if not crew_name:
        return
    try:
        from src.services.flow_builder.conversation.reuse import record_identity

        record_identity(
            getattr(flow, "state", None),
            crew_name,
            _crew_identity(crew_name, task_list),
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[flow-reuse] could not record identity: {exc}")


class FlowMethodFactory:
    """
    Factory for creating dynamic flow methods (starting points, listeners, routers).
    """

    @staticmethod
    def create_starting_point_crew_method(
        method_name: str,
        task_list: List[Any],
        crew_name: str,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable,
        crew_data: Optional[Any] = None,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Callable:
        """
        Create a starting point method that executes multiple tasks as a crew.

        Args:
            method_name: Name of the method
            task_list: List of Task objects to execute sequentially (with task.context dependencies set)
            crew_name: Name of the crew
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks
            crew_data: Crew data from database for configuration inheritance
            user_token: User access token for OBO authentication (optional)
            group_id: Group ID for multi-tenant isolation (optional)

        Returns:
            Async function decorated with @start()
        """

        @start()
        async def starting_point_crew_method(self):
            """Starting point method - executes crew with multiple sequential tasks."""
            # Already answered on an earlier turn? A conversational flow
            # restores its state before every turn, so the crews upstream of the
            # one answering THIS question already have their results in memory.
            # Running them again spends minutes reproducing what is sitting
            # right there. `_reuse_check` decides — terminal crews, edited crews
            # and refreshes all fall through and run.
            reused = _reuse_check(self, crew_name, task_list)
            if reused is not None:
                return reused

            logger.info("=" * 80)
            logger.info(f"START CREW METHOD CALLED - Crew: {crew_name}")
            logger.info(f"Number of tasks: {len(task_list)}")
            logger.info("=" * 80)

            # Collect all unique agents from tasks
            agents = []
            agent_roles_seen = set()

            for task in task_list:
                if hasattr(task, "agent") and task.agent:
                    agent_role = (
                        task.agent.role if hasattr(task.agent, "role") else "Unknown"
                    )
                    if agent_role not in agent_roles_seen:
                        agents.append(task.agent)
                        agent_roles_seen.add(agent_role)
                        logger.info(f"  Agent: {agent_role}")

                        # Log if agent has no tools
                        if not hasattr(task.agent, "tools") or not task.agent.tools:
                            logger.info(
                                f"  Agent {agent_role} has no tools assigned but will continue with execution"
                            )

            # De-dupe agent∩task tools (flow path): a tool present on BOTH an
            # agent and a task it runs is built twice — the agent copy carries the
            # agent-level config (often empty) and the task copy carries the task's
            # config. CrewAI then shows the LLM two same-named tools and it may pick
            # the empty one → e.g. "workspace_id ... required". The crew path fixes
            # this in CrewPreparation, but flow crews bypass that, so apply the
            # same normalization here on the built Agent/Task objects (match by
            # tool .name; the task instance is authoritative, so strip the agent's).
            _dedupe_flow_agent_task_tools(agents, task_list)

            logger.info(f"Total unique agents: {len(agents)}")
            logger.info(f"Total tasks: {len(task_list)}")

            # Log task dependencies
            for idx, task in enumerate(task_list):
                task_desc = (
                    task.description[:50] + "..."
                    if len(task.description) > 50
                    else task.description
                )
                if (
                    hasattr(task, "context")
                    and task.context
                    and isinstance(task.context, list)
                ):
                    logger.info(
                        f"  Task {idx}: {task_desc} (depends on {len(task.context)} previous task(s))"
                    )
                else:
                    logger.info(f"  Task {idx}: {task_desc} (no dependencies)")

            # Create crew with all tasks
            # Determine memory setting based on agent configuration
            # This follows the same pattern as CrewPreparation in regular crew execution
            logger.info(f"Creating Crew instance: {crew_name}")

            # Determine crew memory setting - check both crew config AND agent settings
            crew_memory = True  # Default

            # First, get crew-level memory setting if available
            crew_memory_from_config = None
            if (
                crew_data
                and hasattr(crew_data, "memory")
                and crew_data.memory is not None
            ):
                crew_memory_from_config = crew_data.memory
                logger.info(
                    f"Crew memory setting from configuration: {crew_memory_from_config}"
                )

            # Then check agent memory settings - this is ALWAYS checked, not just as fallback
            # We check our custom _kasal_memory_disabled attribute since CrewAI Agent doesn't store memory as an attribute
            agents_with_memory_enabled = []
            agents_with_memory_disabled = []
            logger.info(
                f"Checking memory settings for {len(agents)} agents in crew {crew_name}"
            )
            for agent in agents:
                agent_role = agent.role if hasattr(agent, "role") else "Unknown"
                # Check our custom attribute that was set during agent configuration
                has_kasal_attr = hasattr(agent, "_kasal_memory_disabled")
                kasal_memory_disabled = getattr(agent, "_kasal_memory_disabled", False)
                logger.info(
                    f"  Agent '{agent_role}': has_kasal_attr={has_kasal_attr}, _kasal_memory_disabled={kasal_memory_disabled}"
                )
                if has_kasal_attr and kasal_memory_disabled:
                    agents_with_memory_disabled.append(agent_role)
                    logger.info(
                        f"  → Agent '{agent_role}' has memory DISABLED (via _kasal_memory_disabled)"
                    )
                else:
                    agents_with_memory_enabled.append(agent_role)
                    logger.info(f"  → Agent '{agent_role}' has memory ENABLED")

            # Determine final crew memory setting:
            # 1. If ALL agents have memory disabled, crew memory should be False (regardless of crew config)
            # 2. If crew config explicitly sets memory=False, use that
            # 3. Otherwise use crew config or default to True
            all_agents_memory_disabled = (
                agents_with_memory_disabled and not agents_with_memory_enabled
            )

            if all_agents_memory_disabled:
                crew_memory = False
                logger.info(
                    f"All agents have memory disabled ({agents_with_memory_disabled}) - setting crew memory to False"
                )
            elif crew_memory_from_config is False:
                crew_memory = False
                logger.info("Crew memory explicitly disabled in configuration")
            elif crew_memory_from_config is True:
                crew_memory = True
                logger.info("Using crew memory setting from configuration: True")
            else:
                # Default: at least one agent has memory enabled
                crew_memory = True
                logger.info(
                    f"At least one agent has memory enabled ({agents_with_memory_enabled}) - setting crew memory to True"
                )

            # Determine process type from crew_data
            process_type = Process.sequential  # Default
            if crew_data and hasattr(crew_data, "process") and crew_data.process:
                if crew_data.process.lower() == "hierarchical":
                    process_type = Process.hierarchical
                    logger.info("Using hierarchical process from crew configuration")
                else:
                    logger.info("Using sequential process from crew configuration")
            else:
                logger.info("Using default sequential process")

            # Determine verbose setting from crew_data
            crew_verbose = True  # Default
            if (
                crew_data
                and hasattr(crew_data, "verbose")
                and crew_data.verbose is not None
            ):
                crew_verbose = crew_data.verbose

            crew_kwargs = {
                "name": crew_name,
                "agents": agents,
                "tasks": task_list,  # Pass ALL tasks - CrewAI will respect task.context for sequential execution
                "verbose": crew_verbose,
                "process": process_type,
                "memory": crew_memory,
            }

            # NOTE: no planning / planning_llm. The CrewAI-style prose planner was
            # removed (the engine has no planner), so a legacy crew row carrying
            # planning=True must NOT resolve a planner LLM — that was a wasted
            # model lookup on every flow crew build.

            # NOTE: reasoning is NOT a Crew parameter and cannot be applied after the
            # agents exist. It is the MODEL's native reasoning budget, stamped onto
            # each agent's LLM while that LLM is built (see
            # kernel/agent_builder._apply_reasoning_effort) from the agent node's own
            # `reasoning` / `reasoning_config` fields.

            # Configure the unified memory backend (Databricks/Lakebase) for the
            # flow crew — same wiring as the regular crew path. Without this the
            # crew falls back to CrewAI's default LanceDB + OpenAI embedder and
            # fails with "CHROMA_OPENAI_API_KEY is not set".
            if crew_memory:
                crew_kwargs = await configure_flow_crew_memory(
                    crew_kwargs, agents, task_list, crew_name, group_id, user_token
                )

            # Log crew configuration for debugging
            logger.info(
                f"📋 Crew configuration: memory={crew_memory}, process={process_type}"
            )

            crew = Crew(**crew_kwargs)
            logger.info(
                f"Crew instance '{crew_name}' created successfully with {len(task_list)} tasks, kwargs: {list(crew_kwargs.keys())}"
            )

            # ── Memory seams — same wiring as the crew path: runtime recall via
            # context provider, per-task persistence via output sink. The flow
            # subprocess flushes pending writes at flow end.
            attach_memory_seams(crew, f"flow crew '{crew_name}'")

            # SECURITY: Run all assembly-time security checks (spotlighting, trifecta,
            # mixed-task anti-pattern, destructive tools).  Flow crews are built here
            # directly — they bypass CrewPreparation — so we must call the shared helper
            # explicitly to ensure identical protection on both execution paths.
            try:
                from src.services.security.tool_capability_manifest import (
                    run_crew_security_checks as _run_security_checks,
                )

                _run_security_checks(crew, context=f"flow crew '{crew_name}'")
            except Exception as _sec_err:
                logger.debug(
                    "[SECURITY] Flow crew security checks skipped: %s", _sec_err
                )

            # Set up execution callbacks
            job_id = None
            if callbacks:
                job_id = callbacks.get("job_id")
                if job_id:
                    logger.info(f"Extracted job_id from callbacks: {job_id}")

            # Create and set synchronous step and task callbacks
            if job_id:
                try:
                    step_callback, task_callback = create_execution_callbacks(
                        job_id=job_id, config={}, group_context=group_context, crew=crew
                    )
                    crew.step_callback = step_callback
                    crew.task_callback = task_callback
                    logger.info(
                        f"✅ Set synchronous execution callbacks on crew for job {job_id}"
                    )
                except Exception as callback_error:
                    logger.warning(
                        f"Failed to set execution callbacks: {callback_error}"
                    )
            else:
                logger.warning(
                    "No job_id available, skipping execution callbacks setup"
                )

            # Attach execution trace context to the crew's tools + memory (parity
            # with the crew path) so flow tool/memory traces carry job_id + group
            # attribution (e.g. custom llm_call trace events). Shared entry point
            # with the crew path; builds a minimal service from group_id+job_id.
            from src.services.execution.kernel.trace_context import (
                attach_execution_trace_context,
            )

            attach_execution_trace_context(
                crew, crew_kwargs, group_id=group_id, job_id=job_id
            )

            try:
                # Enhanced logging for truncation diagnosis
                import time

                start_time = time.time()

                # Log LLM configuration details for first agent
                first_agent = agents[0] if agents else None
                if first_agent and hasattr(first_agent, "llm") and first_agent.llm:
                    llm = first_agent.llm
                    llm_info = {
                        "model": getattr(llm, "model", "unknown"),
                        # BOTH fields: a GPT-5 model carries its ceiling as
                        # max_completion_tokens, so reading only max_tokens
                        # reported "None" for a model capped at 128k — and read
                        # that way, a configured limit looks like no limit.
                        "max_output": output_cap(llm),
                        "timeout": getattr(llm, "timeout", "not set"),
                    }
                    logger.info(f"📊 LLM Configuration: {llm_info}")

                logger.info(f"📝 Total tasks: {len(task_list)}")
                logger.info(
                    f"⏱️ Calling crew.kickoff_async() with {CREW_KICKOFF_TIMEOUT_SECONDS/60:.0f} minute timeout..."
                )

                # Flow state IS the crew's inputs — this is what interpolates
                # {placeholders} in the crew's tasks. Passing nothing is why a
                # flow given `topic` ran a task that still said "{topic}".
                crew_inputs = crew_inputs_from_state(self)
                if crew_inputs:
                    logger.info(
                        f"Passing {sorted(crew_inputs)} from flow state into crew "
                        f"'{crew_name}'"
                    )
                result = await asyncio.wait_for(
                    crew.kickoff_async(inputs=crew_inputs),
                    timeout=CREW_KICKOFF_TIMEOUT_SECONDS,
                )

                elapsed_time = time.time() - start_time
                logger.info(
                    f"⏱️ Crew '{crew_name}' execution took {elapsed_time:.2f} seconds"
                )

                # Log result details for truncation diagnosis
                if result:
                    if hasattr(result, "raw") and result.raw:
                        result_length = len(str(result.raw))
                        logger.info(
                            f"✅ kickoff_async completed - result.raw length: {result_length} chars"
                        )
                        raw_str = str(result.raw)
                        if result_length > 400:
                            logger.info(
                                f"📄 Result preview - First 200 chars: {raw_str[:200]}"
                            )
                            logger.info(
                                f"📄 Result preview - Last 200 chars: {raw_str[-200:]}"
                            )
                        else:
                            logger.info(f"📄 Full result: {raw_str}")
                    else:
                        logger.info(
                            f"✅ kickoff_async completed - result type: {type(result)}, str length: {len(str(result))}"
                        )
                else:
                    logger.warning("⚠️ kickoff_async returned None or empty result")

                # Return serializable value for @persist compatibility
                # CrewOutput objects are not JSON-serializable, so extract raw content
                serializable_result = None
                if hasattr(result, "raw") and result.raw:
                    serializable_result = result.raw
                elif result is not None:
                    serializable_result = str(result)
                else:
                    serializable_result = result

                # Store result in state for checkpoint resume support
                # This allows skipped crews to retrieve the output when resuming
                if serializable_result is not None:
                    if hasattr(self, "state"):
                        self.state[method_name] = serializable_result
                        self.state[crew_name] = serializable_result
                        # Also under a name a task can interpolate: the display
                        # name has spaces, so `{agentic ai frameworks}` is not a
                        # placeholder and a later crew had no way to ASK for
                        # what an earlier one produced.
                        _store_output_slug(self, crew_name, serializable_result)
                        # Stamp WHICH crew produced it: an edited crew must not
                        # have its old answer replayed on a later turn.
                        _record_crew_identity(self, crew_name, task_list)
                        logger.info(
                            f"📦 Stored crew output in state['{method_name}'] and state['{crew_name}'] for checkpoint support"
                        )

                        # ── CI/CD artifact aggregation ─────────────────────────
                        # If this crew produced a cicd_download_url, append it to
                        # the shared _cicd_artifacts list in the flow state so
                        # backend_flow.py can inject ALL artifacts into the final
                        # result (not just the last crew's output).
                        try:
                            _parsed_result = None
                            if isinstance(serializable_result, dict):
                                _parsed_result = serializable_result
                            elif isinstance(
                                serializable_result, str
                            ) and serializable_result.strip().startswith("{"):
                                import json as _json

                                _parsed_result = _json.loads(serializable_result)

                            if _parsed_result and isinstance(_parsed_result, dict):
                                _url = _parsed_result.get("cicd_download_url")
                                if _url:
                                    _artifact = {
                                        "cicd_download_url": _url,
                                        "cicd_type": _parsed_result.get(
                                            "cicd_type", ""
                                        ),
                                        "cicd_name": _parsed_result.get(
                                            "cicd_name", ""
                                        ),
                                    }
                                    if "cicd_serialized_space" in _parsed_result:
                                        _artifact["cicd_serialized_space"] = (
                                            _parsed_result["cicd_serialized_space"]
                                        )

                                    # Initialise or extend the shared list
                                    existing = self.state.get("_cicd_artifacts", [])
                                    if not isinstance(existing, list):
                                        existing = []
                                    # Deduplicate by URL
                                    if not any(
                                        a.get("cicd_download_url") == _url
                                        for a in existing
                                    ):
                                        existing.append(_artifact)
                                    self.state["_cicd_artifacts"] = existing
                                    logger.info(
                                        f"📥 [CI/CD] Captured artifact from '{crew_name}': {_artifact.get('cicd_type')} — {_url}"
                                    )
                        except Exception as _ce:
                            logger.debug(
                                f"[CI/CD] Could not capture artifact from '{crew_name}': {_ce}"
                            )
                        # ── end CI/CD aggregation ──────────────────────────────

                return serializable_result
            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time if "start_time" in dir() else 0
                logger.error(
                    f"❌ Crew '{crew_name}' execution timed out after {elapsed_time:.2f} seconds (limit: {CREW_KICKOFF_TIMEOUT_SECONDS:.0f}s)"
                )
                raise TimeoutError(
                    f"Crew '{crew_name}' execution timed out after {CREW_KICKOFF_TIMEOUT_SECONDS/60:.0f} minutes"
                )
            except Exception as e:
                elapsed_time = time.time() - start_time if "start_time" in dir() else 0
                logger.error(
                    f"❌ Error during crew '{crew_name}' kickoff after {elapsed_time:.2f} seconds: {e}",
                    exc_info=True,
                )
                raise

        # Set metadata on both wrapper AND wrapped function
        # CRITICAL: Must also set _meth.__name__ because StartMethod.__get__ creates a new
        # bound wrapper from _meth, which inherits __name__ from _meth (not the outer wrapper).
        # Without this, Flow.__init__ stores the method under the wrong name in _methods,
        # causing KeyError when kickoff_async tries to find it by the name in _start_methods.
        starting_point_crew_method.__name__ = method_name
        starting_point_crew_method.__qualname__ = method_name
        starting_point_crew_method._meth.__name__ = method_name
        starting_point_crew_method._meth.__qualname__ = method_name

        return starting_point_crew_method

    @staticmethod
    def create_listener_method(
        method_name: str,
        listener_tasks: List[Any],
        method_condition: Any,
        condition_type: str,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable,
        crew_name: Optional[str] = None,
        crew_data: Optional[Any] = None,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Callable:
        """
        Create a listener method for the flow.

        Args:
            method_name: Name of the method
            listener_tasks: List of task objects to execute
            method_condition: Condition for @listen() decorator (method name, and_(), or or_())
            condition_type: Type of condition (NONE, AND, OR)
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks
            crew_name: Name of the crew from flow configuration (for trace tracking)
            crew_data: Crew data from database containing memory and configuration settings
            user_token: User access token for OBO authentication (optional)
            group_id: Group ID for multi-tenant isolation (optional)

        Returns:
            Async function decorated with @listen()
        """
        decorator = listen(method_condition)

        @decorator
        async def listener_method(self, *results):
            """Listener method - executes when listening to a specific event."""
            # Already answered on an earlier turn? A conversational flow
            # restores its state before every turn, so the crews upstream of the
            # one answering THIS question already have their results in memory.
            # Running them again spends minutes reproducing what is sitting
            # right there. `_reuse_check` decides — terminal crews, edited crews
            # and refreshes all fall through and run.
            reused = _reuse_check(self, crew_name, listener_tasks)
            if reused is not None:
                return reused

            logger.info("=" * 80)
            condition_desc = (
                f"{condition_type} conditional "
                if condition_type in ["AND", "OR"]
                else ""
            )
            logger.info(
                f"LISTENER METHOD CALLED - Executing {condition_desc}listener with {len(listener_tasks)} tasks"
            )

            # Log and store previous outputs from preceding methods
            if results:
                logger.info(f"📥 RECEIVED {len(results)} PREVIOUS OUTPUT(S):")
                for i, result in enumerate(results):
                    result_str = str(result)
                    logger.info(f"  Output {i}: {result_str[:200]}...")
                    # Store each result in state. Serialize CrewOutput first: the
                    # @persist decorator JSON-serializes the entire flow state after
                    # this method runs, and a raw CrewOutput is not JSON-serializable
                    # (would raise "Object of type CrewOutput is not JSON serializable").
                    serialized_result = (
                        result.raw
                        if hasattr(result, "raw") and result.raw
                        else (str(result) if result is not None else result)
                    )
                    self.state[f"previous_output_{i}"] = serialized_result
                    if i == 0:
                        # Also store first output as 'previous_output' for easy access
                        self.state["previous_output"] = serialized_result
            else:
                logger.info("📭 No previous outputs received")

            logger.info("=" * 80)

            # Create runtime tasks with previous output injected into descriptions
            # This follows the official CrewAI Flow pattern of creating tasks at runtime
            runtime_tasks = []
            previous_output_context = ""

            if results:
                previous_output_str = extract_final_answer(results)
                full_len = len(previous_output_str)

                # For large outputs (>2K chars), skip task-description injection entirely.
                # The data is already injected directly into tool._default_config (below),
                # so the LLM doesn't need to see it — it just needs to call the tool.
                # Injecting large context causes LLM timeouts (297s) with no benefit.
                MAX_CONTEXT_CHARS = 2_000

                if full_len > MAX_CONTEXT_CHARS:
                    previous_output_context = (
                        f"\n\nContext from previous step: {full_len:,} chars of data are "
                        f"pre-loaded into your tool's config_json parameter. "
                        f"Call the tool without passing config_json — it already has the data."
                    )
                else:
                    previous_output_context = (
                        f"\n\nContext from previous step:\n{previous_output_str}"
                    )
                logger.info(
                    f"📤 Context injection: {len(previous_output_context)} chars in task description (original: {full_len:,} chars)"
                )

            # Create new Task objects with modified descriptions.
            # CRITICAL: carry over tools/output_pydantic/output_json/converter_cls/
            # context. Rebuilding with only description/agent/expected_output (the
            # prior behavior) silently dropped them, so listener crews lost their
            # tools (no MCP/tool calls) AND their structured-output schema (no
            # .pydantic → routers fall back to flaky raw-text parsing). The
            # starting crew keeps its original Task objects, which is why only
            # listener (@listen) crews were affected.
            for task in listener_tasks:
                # Create new task with injected context, preserving execution config.
                runtime_task = Task(
                    description=f"{task.description}{previous_output_context}",
                    agent=task.agent,
                    expected_output=(
                        task.expected_output
                        if hasattr(task, "expected_output")
                        else "Task completed successfully"
                    ),
                    tools=getattr(task, "tools", None) or [],
                    output_pydantic=getattr(task, "output_pydantic", None),
                    output_json=getattr(task, "output_json", None),
                    converter_cls=getattr(task, "converter_cls", None),
                )
                runtime_tasks.append(runtime_task)
                logger.info(
                    f"Created runtime task with injected context for agent: {task.agent.role} "
                    f"(tools={len(getattr(task, 'tools', None) or [])}, "
                    f"output_pydantic={'yes' if getattr(task, 'output_pydantic', None) else 'no'})"
                )

            # Create a crew with runtime tasks
            agents = collect_task_agents(runtime_tasks)
            logger.info(f"Number of agents in listener: {len(agents)}")

            # Log if agents have no tools
            for agent in agents:
                if not hasattr(agent, "tools") or not agent.tools:
                    logger.info(
                        f"Agent {agent.role} has no tools assigned but will continue with execution"
                    )

            logger.info("Creating Crew instance for listener method")
            logger.info(
                f"Listener has {len(agents)} agents and {len(runtime_tasks)} tasks"
            )

            # Use provided crew name from flow config, fallback to first agent role
            listener_crew_name = (
                crew_name
                if crew_name
                else (
                    agents[0].role
                    if agents and hasattr(agents[0], "role") and agents[0].role
                    else "Listener Crew"
                )
            )
            logger.info(f"Creating listener crew with name: {listener_crew_name}")

            # Determine crew memory setting - check both crew config AND agent settings
            crew_memory = True  # Default

            # First, get crew-level memory setting if available
            crew_memory_from_config = None
            if (
                crew_data
                and hasattr(crew_data, "memory")
                and crew_data.memory is not None
            ):
                crew_memory_from_config = crew_data.memory
                logger.info(
                    f"Listener crew memory setting from configuration: {crew_memory_from_config}"
                )

            # Then check agent memory settings - this is ALWAYS checked, not just as fallback
            # We check our custom _kasal_memory_disabled attribute since CrewAI Agent doesn't store memory as an attribute
            agents_with_memory_enabled = []
            agents_with_memory_disabled = []
            logger.info(
                f"Checking memory settings for {len(agents)} agents in listener crew {listener_crew_name}"
            )
            for agent in agents:
                agent_role = agent.role if hasattr(agent, "role") else "Unknown"
                # Check our custom attribute that was set during agent configuration
                has_kasal_attr = hasattr(agent, "_kasal_memory_disabled")
                kasal_memory_disabled = getattr(agent, "_kasal_memory_disabled", False)
                logger.info(
                    f"  Agent '{agent_role}': has_kasal_attr={has_kasal_attr}, _kasal_memory_disabled={kasal_memory_disabled}"
                )
                if has_kasal_attr and kasal_memory_disabled:
                    agents_with_memory_disabled.append(agent_role)
                    logger.info(
                        f"  → Agent '{agent_role}' has memory DISABLED (via _kasal_memory_disabled)"
                    )
                else:
                    agents_with_memory_enabled.append(agent_role)
                    logger.info(f"  → Agent '{agent_role}' has memory ENABLED")

            # Determine final crew memory setting:
            # 1. If ALL agents have memory disabled, crew memory should be False (regardless of crew config)
            # 2. If crew config explicitly sets memory=False, use that
            # 3. Otherwise use crew config or default to True
            all_agents_memory_disabled = (
                agents_with_memory_disabled and not agents_with_memory_enabled
            )

            if all_agents_memory_disabled:
                crew_memory = False
                logger.info(
                    f"All agents have memory disabled ({agents_with_memory_disabled}) - setting listener crew memory to False"
                )
            elif crew_memory_from_config is False:
                crew_memory = False
                logger.info("Listener crew memory explicitly disabled in configuration")
            elif crew_memory_from_config is True:
                crew_memory = True
                logger.info(
                    "Using listener crew memory setting from configuration: True"
                )
            else:
                # Default: at least one agent has memory enabled
                crew_memory = True
                logger.info(
                    f"At least one agent has memory enabled ({agents_with_memory_enabled}) - setting listener crew memory to True"
                )

            # Determine process type from crew_data
            process_type = Process.sequential  # Default
            if crew_data and hasattr(crew_data, "process") and crew_data.process:
                if crew_data.process.lower() == "hierarchical":
                    process_type = Process.hierarchical
                    logger.info(
                        "Using hierarchical process for listener crew from configuration"
                    )
                else:
                    logger.info(
                        "Using sequential process for listener crew from configuration"
                    )
            else:
                logger.info("Using default sequential process for listener crew")

            # Determine verbose setting from crew_data
            crew_verbose = True  # Default
            if (
                crew_data
                and hasattr(crew_data, "verbose")
                and crew_data.verbose is not None
            ):
                crew_verbose = crew_data.verbose

            # Create crew with configuration from crew_data
            crew_kwargs = {
                "name": listener_crew_name,
                "agents": agents,
                "tasks": runtime_tasks,
                "verbose": crew_verbose,
                "process": process_type,
                "memory": crew_memory,
            }

            # NOTE: no planning / planning_llm and no crew-level reasoning — same
            # reasons as the crew builder above: the prose planner is gone, and the
            # reasoning budget is applied to each agent's LLM at build time from the
            # agent node's own reasoning / reasoning_config fields.

            # Configure the unified memory backend (Databricks/Lakebase) for the
            # listener crew — same wiring as the regular crew path (avoids the
            # CrewAI default LanceDB + OpenAI embedder / CHROMA_OPENAI_API_KEY).
            if crew_memory:
                crew_kwargs = await configure_flow_crew_memory(
                    crew_kwargs,
                    agents,
                    runtime_tasks,
                    listener_crew_name,
                    group_id,
                    user_token,
                )

            # Log crew configuration for debugging
            logger.info(
                f"Listener crew configuration: memory={crew_memory}, process={process_type}"
            )

            crew = Crew(**crew_kwargs)
            logger.info(
                f"Crew instance '{listener_crew_name}' created for listener, kwargs: {list(crew_kwargs.keys())}"
            )

            # ── Memory seams — REQUIRED, same as starting-point crews above.
            # Listener crews were configuring a memory backend and then never
            # attaching these hooks, so every @listen step ran with no recall and
            # no persistence despite reporting memory=True.
            attach_memory_seams(crew, f"flow listener crew '{listener_crew_name}'")

            # SECURITY: Same assembly-time checks as starting-point crews.
            try:
                from src.services.security.tool_capability_manifest import (
                    run_crew_security_checks as _run_security_checks,
                )

                _run_security_checks(
                    crew, context=f"flow listener crew '{listener_crew_name}'"
                )
            except Exception as _sec_err:
                logger.debug(
                    "[SECURITY] Flow listener crew security checks skipped: %s",
                    _sec_err,
                )

            # Set up execution callbacks
            job_id = None
            if callbacks:
                job_id = callbacks.get("job_id")
                if job_id:
                    logger.info(
                        f"Extracted job_id from callbacks for listener: {job_id}"
                    )

            # Create and set synchronous step and task callbacks
            if job_id:
                try:
                    step_callback, task_callback = create_execution_callbacks(
                        job_id=job_id, config={}, group_context=group_context, crew=crew
                    )
                    crew.step_callback = step_callback
                    crew.task_callback = task_callback
                    logger.info(
                        f"✅ Set synchronous execution callbacks on listener crew for job {job_id}"
                    )
                except Exception as callback_error:
                    logger.warning(
                        f"Failed to set execution callbacks on listener: {callback_error}"
                    )
            else:
                logger.warning(
                    "No job_id available for listener, skipping execution callbacks setup"
                )

            # Attach execution trace context to the crew's tools + memory (parity
            # with the crew path) so flow tool/memory traces carry job_id + group
            # attribution (e.g. custom llm_call trace events). Shared entry point
            # with the crew path; builds a minimal service from group_id+job_id.
            from src.services.execution.kernel.trace_context import (
                attach_execution_trace_context,
            )

            attach_execution_trace_context(
                crew, crew_kwargs, group_id=group_id, job_id=job_id
            )

            try:
                # Enhanced logging for truncation diagnosis
                import time

                start_time = time.time()

                # Log LLM configuration details for first agent
                first_agent = agents[0] if agents else None
                if first_agent and hasattr(first_agent, "llm") and first_agent.llm:
                    llm = first_agent.llm
                    llm_info = {
                        "model": getattr(llm, "model", "unknown"),
                        "max_output": output_cap(llm),
                        "timeout": getattr(llm, "timeout", "not set"),
                    }
                    logger.info(f"📊 Listener LLM Configuration: {llm_info}")

                logger.info(f"📝 Listener tasks: {len(runtime_tasks)}")

                # ── Inject previous crew output into tool _default_config ──
                # Use the most recent previous crew output from flow state.
                # When multiple crews are chained, `results` may contain stale
                # output from an earlier crew (passed through HITL gates).
                # The flow state stores each crew's output by name and by
                # listener index — use the latest one.
                _inject_source = None
                if results:
                    _inject_source = extract_final_answer(results)
                # Also check flow state for a more recent output from the
                # immediately preceding crew (stored at listener_N keys)
                if hasattr(self, "state") and isinstance(self.state, dict):
                    # Find the highest listener_N key that has data
                    _latest_listener = None
                    for _sk, _sv in self.state.items():
                        if _sk.startswith("listener_") and _sv:
                            idx = _sk.replace("listener_", "")
                            if idx.isdigit():
                                if (
                                    _latest_listener is None
                                    or int(idx) > _latest_listener[0]
                                ):
                                    _latest_listener = (int(idx), str(_sv))
                    if _latest_listener and _latest_listener[1]:
                        _candidate = _latest_listener[1]
                        # Extract raw string from CrewOutput if needed
                        if hasattr(_candidate, "raw"):
                            _candidate = _candidate.raw
                        _candidate = (
                            str(_candidate)
                            if not isinstance(_candidate, str)
                            else _candidate
                        )
                        # Prefer the state output if it contains UCMV data (yaml key)
                        # or is from a more recent crew than what results provided
                        _is_ucmv_output = (
                            '"yaml"' in _candidate[:500] or "'yaml'" in _candidate[:500]
                        )
                        _is_different = (
                            _inject_source is None or _candidate != _inject_source
                        )
                        if _is_different and (
                            _is_ucmv_output
                            or len(_candidate) > len(_inject_source or "")
                        ):
                            logger.info(
                                f"📥 Using flow state listener_{_latest_listener[0]} output "
                                f"({len(_candidate):,} chars, has_yaml={_is_ucmv_output}) "
                                f"instead of results ({len(_inject_source or ''):,} chars)"
                            )
                            _inject_source = _candidate

                # ── Collect ALL parseable JSON outputs from the flow state ──
                # In multi-crew flows (A→B→C), crew C needs data from crew A,
                # not just from the immediately preceding crew B.
                import json as _json

                def _extract_json(raw: str) -> dict | None:
                    """Try to extract a JSON dict from a string (handles agent narrative wrapping)."""
                    if not raw or not isinstance(raw, str):
                        return None
                    s = raw.strip()
                    # Direct JSON
                    if s.startswith("{"):
                        try:
                            return _json.loads(s)
                        except _json.JSONDecodeError:
                            pass
                    # Extract JSON from "Final Answer: {...}" or narrative wrapping
                    import re as _re

                    match = _re.search(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", s)
                    if match:
                        try:
                            return _json.loads(match.group(0))
                        except _json.JSONDecodeError:
                            pass
                    return None

                # Build a list of all parsed outputs from flow state + current results
                all_outputs: list[dict] = []
                all_output_sources: list[str] = []

                # Add immediate predecessor output
                if _inject_source:
                    parsed = _extract_json(_inject_source)
                    if parsed:
                        all_outputs.append(parsed)
                        all_output_sources.append(
                            f"immediate predecessor ({len(_inject_source):,} chars)"
                        )

                # Add all stored state outputs (from earlier crews in the chain)
                if hasattr(self, "state") and isinstance(self.state, dict):
                    for _sk, _sv in sorted(self.state.items()):
                        if not _sv or _sk.startswith("_"):
                            continue
                        sv_str = str(_sv) if not isinstance(_sv, str) else _sv
                        parsed = _extract_json(sv_str)
                        if parsed and parsed not in all_outputs:
                            all_outputs.append(parsed)
                            all_output_sources.append(f"state[{_sk}]")

                logger.info(
                    f"📦 Collected {len(all_outputs)} parseable JSON outputs from flow chain"
                )

                try:
                    injected_count = 0
                    for agent in agents:
                        for tool in agent.tools or []:
                            if not hasattr(tool, "_default_config") or not isinstance(
                                tool._default_config, dict
                            ):
                                continue
                            tool_name = type(tool).__name__
                            cfg = tool._default_config

                            for out_idx, prev_data in enumerate(all_outputs):
                                source_label = (
                                    all_output_sources[out_idx]
                                    if out_idx < len(all_output_sources)
                                    else "unknown"
                                )

                                # ── Pipeline Config Generator output → UCMV inputs ──
                                # config-gen emits a WRAPPER dict:
                                #   {proposed_config: {...join_key_map, measure_resolutions...},
                                #    measures_json: [...], mquery_json: [...],
                                #    relationships_json: [...], summary: {...}}
                                # UCMV (JSON mode) builds views from measures_json +
                                # mquery_json and reads join maps from config_json — so
                                # unwrap the wrapper and map each piece to the matching
                                # UCMV field. Detect by the wrapper's own keys OR by a
                                # bare (unwrapped) pipeline-config dict.
                                inner_cfg = (
                                    prev_data.get("proposed_config")
                                    if isinstance(
                                        prev_data.get("proposed_config"), dict
                                    )
                                    else None
                                )
                                has_ucmv_handoff = any(
                                    k in prev_data
                                    for k in (
                                        "proposed_config",
                                        "measures_json",
                                        "mquery_json",
                                    )
                                )
                                is_bare_pipeline_config = inner_cfg is None and any(
                                    k in prev_data
                                    for k in (
                                        "join_key_map",
                                        "enrichment_joins",
                                        "filter_sets",
                                        "measure_resolutions",
                                    )
                                )
                                if has_ucmv_handoff or is_bare_pipeline_config:
                                    # config_json ← proposed_config (or the bare dict itself)
                                    if "config_json" in cfg:
                                        existing = cfg.get("config_json") or ""
                                        if (
                                            not existing
                                            or existing in ("{}", "null")
                                            or len(existing) <= 10
                                        ):
                                            config_payload = (
                                                inner_cfg
                                                if inner_cfg is not None
                                                else prev_data
                                            )
                                            cfg["config_json"] = _json.dumps(
                                                config_payload
                                            )
                                            injected_count += 1
                                            logger.info(
                                                f"📥 Injected pipeline config from {source_label} into {tool_name}.config_json"
                                            )
                                    # measures_json / mquery_json / relationships_json ← handoff arrays.
                                    # NOTE: do NOT require the key to already exist in cfg —
                                    # the UCMV crew is seeded in JSON mode with these fields
                                    # absent (the tool's __init__ only stores non-None keys),
                                    # so gating on `_hk in cfg` would silently skip the very
                                    # data UCMV needs. They are valid UCMV config fields; adding
                                    # them to _default_config is exactly what JSON mode expects.
                                    # Only fill when the current value is empty so a manually
                                    # provided value is never clobbered.
                                    for _hk in (
                                        "measures_json",
                                        "mquery_json",
                                        "relationships_json",
                                    ):
                                        payload = prev_data.get(_hk)
                                        if not payload:
                                            continue
                                        existing = cfg.get(_hk) or ""
                                        if (
                                            not existing
                                            or existing in ("[]", "{}", "null")
                                            or len(str(existing)) <= 2
                                        ):
                                            cfg[_hk] = (
                                                payload
                                                if isinstance(payload, str)
                                                else _json.dumps(payload)
                                            )
                                            injected_count += 1
                                            _n = (
                                                len(payload)
                                                if isinstance(payload, list)
                                                else len(_json.dumps(payload))
                                            )
                                            logger.info(
                                                f"📥 Injected {_hk} ({_n} items) from {source_label} into {tool_name}"
                                            )
                                    continue

                                # UCMV output (has 'yaml' key) → ucmv_output
                                if "ucmv_output" in cfg and "yaml" in prev_data:
                                    has_manual_yaml = bool(
                                        cfg.get("yaml_specs_json")
                                        and cfg["yaml_specs_json"]
                                        not in ("{}", None, "")
                                    )
                                    if not has_manual_yaml and (
                                        not cfg.get("ucmv_output")
                                        or cfg["ucmv_output"] in (None, "null", "")
                                    ):
                                        cfg["ucmv_output"] = _json.dumps(prev_data)
                                        injected_count += 1
                                        logger.info(
                                            f"📥 Injected UCMV output from {source_label} ({len(_json.dumps(prev_data)):,} chars) into {tool_name}.ucmv_output"
                                        )
                                    continue

                                # Visual mappings (has 'visual_mappings' or 'visual_mappings_json') → visual_mappings_json
                                if "visual_mappings_json" in cfg and (
                                    "visual_mappings" in prev_data
                                    or "visual_mappings_json" in prev_data
                                ):
                                    if not cfg.get("visual_mappings_json") or cfg[
                                        "visual_mappings_json"
                                    ] in (None, "null", ""):
                                        raw_mappings = prev_data.get(
                                            "visual_mappings_json"
                                        ) or _json.dumps(
                                            prev_data.get("visual_mappings", [])
                                        )
                                        cfg["visual_mappings_json"] = (
                                            raw_mappings
                                            if isinstance(raw_mappings, str)
                                            else _json.dumps(raw_mappings)
                                        )
                                        injected_count += 1
                                        logger.info(
                                            f"📥 Injected visual_mappings_json from {source_label} into {tool_name}"
                                        )
                                    continue

                                # Generic: inject matching keys from immediate predecessor only
                                if out_idx == 0:
                                    for key, value in prev_data.items():
                                        if key in cfg and (
                                            not cfg.get(key)
                                            or cfg[key] in (None, "null", "")
                                        ):
                                            cfg[key] = (
                                                value
                                                if isinstance(value, str)
                                                else _json.dumps(value)
                                            )
                                            injected_count += 1

                    if injected_count > 0:
                        logger.info(
                            f"📥 Total: injected {injected_count} key(s) from flow chain into tool _default_config(s)"
                        )
                except Exception as inject_err:
                    logger.debug(
                        f"Flow chain injection error (non-fatal): {inject_err}"
                    )

                logger.info(
                    f"⏱️ Calling listener crew.kickoff_async() with {CREW_KICKOFF_TIMEOUT_SECONDS/60:.0f} minute timeout..."
                )

                # The SECOND crew call site (listener crews). Same reason.
                crew_inputs = crew_inputs_from_state(self)
                result = await asyncio.wait_for(
                    crew.kickoff_async(inputs=crew_inputs),
                    timeout=CREW_KICKOFF_TIMEOUT_SECONDS,
                )

                elapsed_time = time.time() - start_time
                logger.info(
                    f"⏱️ Listener crew execution took {elapsed_time:.2f} seconds"
                )

                # Log result details for truncation diagnosis
                if result:
                    if hasattr(result, "raw") and result.raw:
                        result_length = len(str(result.raw))
                        logger.info(
                            f"✅ Listener kickoff completed - result.raw length: {result_length} chars"
                        )
                        raw_str = str(result.raw)
                        if result_length > 400:
                            logger.info(
                                f"📄 Listener result preview - First 200 chars: {raw_str[:200]}"
                            )
                            logger.info(
                                f"📄 Listener result preview - Last 200 chars: {raw_str[-200:]}"
                            )
                        else:
                            logger.info(f"📄 Listener full result: {raw_str}")
                    else:
                        logger.info(
                            f"✅ Listener kickoff completed - result type: {type(result)}, str length: {len(str(result))}"
                        )
                else:
                    logger.warning("⚠️ Listener kickoff returned None or empty result")

                # Return serializable value for @persist compatibility
                # CrewOutput objects are not JSON-serializable, so extract raw content
                serializable_result = None
                if hasattr(result, "raw") and result.raw:
                    serializable_result = result.raw
                elif result is not None:
                    serializable_result = str(result)
                else:
                    serializable_result = result

                # Store result in state for checkpoint resume support
                # This allows skipped crews to retrieve the output when resuming
                if serializable_result is not None:
                    if hasattr(self, "state"):
                        self.state[method_name] = serializable_result
                        self.state[crew_name] = serializable_result
                        # Also under a name a task can interpolate: the display
                        # name has spaces, so `{agentic ai frameworks}` is not a
                        # placeholder and a later crew had no way to ASK for
                        # what an earlier one produced.
                        _store_output_slug(self, crew_name, serializable_result)
                        # Stamp WHICH crew produced it: an edited crew must not
                        # have its old answer replayed on a later turn.
                        _record_crew_identity(self, crew_name, listener_tasks)
                        logger.info(
                            f"📦 Stored listener output in state['{method_name}'] and state['{crew_name}'] for checkpoint support"
                        )

                return serializable_result
            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time if "start_time" in dir() else 0
                logger.error(
                    f"❌ Listener crew execution timed out after {elapsed_time:.2f} seconds (limit: {CREW_KICKOFF_TIMEOUT_SECONDS:.0f}s)"
                )
                raise TimeoutError("Listener crew execution timed out")
            except Exception as e:
                elapsed_time = time.time() - start_time if "start_time" in dir() else 0
                logger.error(
                    f"❌ Error during listener crew kickoff after {elapsed_time:.2f} seconds: {e}",
                    exc_info=True,
                )
                raise

        # Set metadata on both wrapper AND wrapped function
        # CRITICAL: Must also set _meth.__name__ because ListenMethod.__get__ creates a new
        # bound wrapper from _meth, which inherits __name__ from _meth (not the outer wrapper).
        listener_method.__name__ = method_name
        listener_method.__qualname__ = method_name
        listener_method._meth.__name__ = method_name
        listener_method._meth.__qualname__ = method_name

        return listener_method

    @staticmethod
    def create_skipped_crew_method(
        method_name: str,
        crew_name: str,
        crew_sequence: int,
        is_starting_point: bool = True,
        method_condition: Any = None,
        condition_type: str = "NONE",
        checkpoint_output: Any = None,
    ) -> Callable:
        """
        Create a stub method for a crew that should be skipped during checkpoint resume.

        When resuming from a checkpoint, crews that have already completed (sequence < resume_from)
        are replaced with stub methods that return the checkpoint output from the database,
        allowing the flow to continue with proper context for downstream crews.
        Note: resume_from is the sequence of the crew TO RUN, not the last completed.

        Args:
            method_name: Name of the flow method
            crew_name: Name of the crew being skipped
            crew_sequence: The sequence number of this crew
            is_starting_point: True if this is a starting point method, False for listener
            method_condition: For listeners, the method(s) to listen to
            condition_type: For listeners, the condition type (AND, OR, NONE)
            checkpoint_output: The actual output from the previous execution (from database traces)

        Returns:
            A decorated async method that returns the checkpoint output
        """
        logger.info(
            f"Creating SKIP method '{method_name}' for crew '{crew_name}' (sequence: {crew_sequence})"
        )
        if checkpoint_output is not None:
            logger.info(
                f"  📦 Checkpoint output provided: {str(checkpoint_output)[:200]}..."
            )

        def get_cached_output(flow_instance, method_nm, crew_nm, prev_output=None):
            """
            Retrieve cached output from persistence layer.

            Checks multiple sources for cached crew output:
            1. _method_outputs (set by @persist decorator)
            2. state dictionary with method_name key
            3. state dictionary with crew_name key
            4. state dictionary with 'crew_{sequence}_output' key
            5. Falls back to previous_output if provided
            """
            cached_output = None

            # DIAGNOSTIC: Log what's available in the flow instance
            logger.info(
                f"  🔍 DIAGNOSTIC - Looking for cached output for '{method_nm}' / '{crew_nm}'"
            )
            if hasattr(flow_instance, "_method_outputs"):
                logger.info(
                    f"  🔍 DIAGNOSTIC - _method_outputs exists: {bool(flow_instance._method_outputs)}"
                )
                if flow_instance._method_outputs:
                    logger.info(
                        f"  🔍 DIAGNOSTIC - _method_outputs keys: {list(flow_instance._method_outputs.keys()) if isinstance(flow_instance._method_outputs, dict) else 'not a dict'}"
                    )
            else:
                logger.info("  🔍 DIAGNOSTIC - _method_outputs does not exist")

            if hasattr(flow_instance, "state"):
                state = flow_instance.state
                logger.info(f"  🔍 DIAGNOSTIC - state exists, type: {type(state)}")
                if hasattr(state, "keys"):
                    logger.info(f"  🔍 DIAGNOSTIC - state keys: {list(state.keys())}")
                elif hasattr(state, "__dict__"):
                    logger.info(
                        f"  🔍 DIAGNOSTIC - state attrs: {list(vars(state).keys())}"
                    )
            else:
                logger.info("  🔍 DIAGNOSTIC - state does not exist")

            # Try to get from _method_outputs (CrewAI @persist stores outputs here)
            if (
                hasattr(flow_instance, "_method_outputs")
                and flow_instance._method_outputs
            ):
                if (
                    isinstance(flow_instance._method_outputs, dict)
                    and method_nm in flow_instance._method_outputs
                ):
                    cached_output = flow_instance._method_outputs[method_nm]
                    logger.info(
                        f"  📦 Found cached output in _method_outputs['{method_nm}']"
                    )
                    return cached_output

            # Try to get from state with various key patterns
            if hasattr(flow_instance, "state"):
                state = flow_instance.state

                # Check for method_name as key
                if hasattr(state, "get"):
                    # Dict-like state
                    if method_nm in state:
                        cached_output = state.get(method_nm)
                        logger.info(f"  📦 Found cached output in state['{method_nm}']")
                        return cached_output

                    # Check for crew_name as key
                    if crew_nm in state:
                        cached_output = state.get(crew_nm)
                        logger.info(f"  📦 Found cached output in state['{crew_nm}']")
                        return cached_output

                    # Check for crew_{sequence}_output pattern
                    seq_key = f"crew_{crew_sequence}_output"
                    if seq_key in state:
                        cached_output = state.get(seq_key)
                        logger.info(f"  📦 Found cached output in state['{seq_key}']")
                        return cached_output

                    # Check for {method_name}_output pattern
                    output_key = f"{method_nm}_output"
                    if output_key in state:
                        cached_output = state.get(output_key)
                        logger.info(
                            f"  📦 Found cached output in state['{output_key}']"
                        )
                        return cached_output

                    # Check previous_output key
                    if "previous_output" in state:
                        cached_output = state.get("previous_output")
                        logger.info(
                            "  📦 Found cached output in state['previous_output']"
                        )
                        return cached_output
                else:
                    # Object-like state
                    if hasattr(state, method_nm):
                        cached_output = getattr(state, method_nm)
                        logger.info(f"  📦 Found cached output in state.{method_nm}")
                        return cached_output
                    if hasattr(state, crew_nm):
                        cached_output = getattr(state, crew_nm)
                        logger.info(f"  📦 Found cached output in state.{crew_nm}")
                        return cached_output

            # Fall back to previous_output if provided (pass-through for listeners)
            if prev_output is not None:
                logger.info(
                    "  📦 Using previous_output as fallback (pass-through mode)"
                )
                return prev_output

            logger.warning(
                f"  ⚠️ No cached output found for '{method_nm}' / '{crew_nm}'"
            )
            return None

        if is_starting_point:
            # Create a starting point stub method that returns checkpoint output
            @start()
            async def skipped_starting_method(self):
                logger.info("=" * 80)
                logger.info(
                    f"⏭️  CHECKPOINT RESUME: Skipping crew '{crew_name}' (sequence: {crew_sequence})"
                )
                logger.info(f"Method: {method_name}")
                logger.info("This crew was already completed in a previous execution")

                # Primary source: checkpoint_output from database traces (passed from flow builder)
                result_output = checkpoint_output

                if result_output is not None:
                    logger.info(
                        f"  ✅ Using checkpoint output from database: {str(result_output)[:200]}..."
                    )
                else:
                    # Fallback: try to get from persistence layer (in case @persist loaded state)
                    result_output = get_cached_output(self, method_name, crew_name)

                    if result_output is not None:
                        logger.info(
                            f"  ✅ Using cached output from persistence: {str(result_output)[:200]}..."
                        )
                    else:
                        logger.warning(
                            "  ⚠️ No checkpoint output found, returning placeholder"
                        )
                        # Create a placeholder output to allow flow to continue
                        result_output = {
                            "status": "skipped",
                            "crew_name": crew_name,
                            "message": f"Crew '{crew_name}' was skipped during checkpoint resume",
                        }

                # Store in state for downstream propagation
                if hasattr(self, "state"):
                    self.state[method_name] = result_output
                    self.state[crew_name] = result_output
                    logger.info(
                        f"  📦 Stored output in state['{method_name}'] and state['{crew_name}']"
                    )

                _emit_checkpoint_restored(crew_name, result_output)

                logger.info("=" * 80)
                return result_output

            # Set metadata on both wrapper AND wrapped function
            skipped_starting_method.__name__ = method_name
            skipped_starting_method.__qualname__ = method_name
            skipped_starting_method._meth.__name__ = method_name
            skipped_starting_method._meth.__qualname__ = method_name
            return skipped_starting_method
        else:
            # Create a listener stub method that returns checkpoint output
            @listen(method_condition)
            async def skipped_listener_method(self, previous_output=None):
                logger.info("=" * 80)
                logger.info(
                    f"⏭️  CHECKPOINT RESUME: Skipping listener crew '{crew_name}' (sequence: {crew_sequence})"
                )
                logger.info(f"Method: {method_name}")
                logger.info(f"Listening to: {method_condition}")
                logger.info(
                    f"Previous output received: {str(previous_output)[:200] if previous_output else 'None'}..."
                )
                logger.info("This crew was already completed in a previous execution")

                # Primary source: checkpoint_output from database traces (passed from flow builder)
                result_output = checkpoint_output

                if result_output is not None:
                    logger.info(
                        f"  ✅ Using checkpoint output from database: {str(result_output)[:200]}..."
                    )
                else:
                    # Fallback: try to get from persistence layer, with previous_output as last resort
                    result_output = get_cached_output(
                        self, method_name, crew_name, previous_output
                    )

                    if result_output is not None:
                        logger.info(
                            f"  ✅ Using cached/fallback output: {str(result_output)[:200]}..."
                        )
                    else:
                        logger.warning(
                            "  ⚠️ No checkpoint output found and no previous_output, returning placeholder"
                        )
                        # Create a placeholder output to allow flow to continue
                        result_output = {
                            "status": "skipped",
                            "crew_name": crew_name,
                            "message": f"Crew '{crew_name}' was skipped during checkpoint resume",
                        }

                # Store in state to propagate to downstream crews
                if hasattr(self, "state"):
                    self.state[method_name] = result_output
                    self.state[crew_name] = result_output
                    self.state["previous_output"] = result_output
                    logger.info(
                        f"  📦 Stored output in state['{method_name}'] and state['{crew_name}']"
                    )

                _emit_checkpoint_restored(crew_name, result_output)

                logger.info("=" * 80)
                return result_output

            # Set metadata on both wrapper AND wrapped function
            skipped_listener_method.__name__ = method_name
            skipped_listener_method.__qualname__ = method_name
            skipped_listener_method._meth.__name__ = method_name
            skipped_listener_method._meth.__qualname__ = method_name
            return skipped_listener_method

    @staticmethod
    def create_hitl_gate_method(
        method_name: str,
        gate_node_id: str,
        gate_config: Dict[str, Any],
        previous_method_name: str,
        crew_sequence: int,
        callbacks: Optional[Dict[str, Any]] = None,
        group_context: Optional[Any] = None,
    ) -> Callable:
        """
        Create an HITL gate method that pauses flow for human approval.

        This method listens to the previous crew's completion, then:
        1. Creates an HITLApproval record in the database
        2. Updates execution status to WAITING_FOR_APPROVAL
        3. Sends webhook notifications
        4. Raises FlowPausedForApprovalException to pause flow

        Args:
            method_name: Name of the gate method
            gate_node_id: ID of the HITL gate node in the flow
            gate_config: Gate configuration dict with:
                - message: Display message for approver
                - timeout_seconds: Seconds before timeout
                - timeout_action: Action on timeout (auto_reject, fail)
                - require_comment: Whether comment is required
                - allowed_approvers: List of allowed approver emails
            previous_method_name: Name of the method this gate listens to
            crew_sequence: Sequence number of the previous crew
            callbacks: Callbacks dict with job_id and other metadata
            group_context: Group context for multi-tenant isolation

        Returns:
            Async function decorated with @listen() that pauses for approval
        """

        @listen(previous_method_name)
        async def hitl_gate_method(self, previous_output=None):
            """HITL gate method - pauses flow for human approval."""
            from src.db.session import request_scoped_session
            from src.models.hitl_approval import HITLApprovalStatus
            from src.repositories.hitl_repository import HITLApprovalRepository
            from src.services.flow_builder.exceptions import (
                FlowPausedForApprovalException,
            )
            from src.services.hitl.service import HITLService
            from src.services.hitl.webhook import HITLWebhookService

            logger.info("=" * 80)
            logger.info(f"🚦 HITL GATE REACHED: {gate_node_id}")
            logger.info(f"Method: {method_name}")
            logger.info(f"Listening to: {previous_method_name}")
            logger.info(f"Gate config: {gate_config}")
            logger.info("=" * 80)

            # Extract execution context
            job_id = callbacks.get("job_id") if callbacks else None
            flow_id = callbacks.get("flow_id") if callbacks else None

            logger.info("📋 Extracted from callbacks:")
            logger.info(f"   job_id: {job_id}")
            logger.info(f"   flow_id: {flow_id}")
            logger.info(
                f"   callbacks keys: {list(callbacks.keys()) if callbacks else 'None'}"
            )

            if not job_id:
                logger.error(
                    "No job_id found in callbacks - cannot create HITL approval"
                )
                raise ValueError("HITL gate requires job_id in callbacks")

            # Get group_id from context
            group_id = None
            if group_context:
                if hasattr(group_context, "primary_group_id"):
                    group_id = group_context.primary_group_id
                elif hasattr(group_context, "group_ids") and group_context.group_ids:
                    group_id = group_context.group_ids[0]

            if not group_id:
                logger.error(
                    "No group_id found in group_context - cannot create HITL approval"
                )
                raise ValueError("HITL gate requires group_id in context")

            # Check if there's already an APPROVED approval for this gate
            # This happens when resuming after approval
            async with request_scoped_session() as session:
                hitl_repo = HITLApprovalRepository(session)
                existing_approvals = await hitl_repo.get_all_for_execution(
                    job_id, group_id
                )

                # Look for an approved approval for this specific gate
                approved_for_gate = None
                for approval in existing_approvals:
                    if (
                        approval.gate_node_id == gate_node_id
                        and approval.status == HITLApprovalStatus.APPROVED
                    ):
                        approved_for_gate = approval
                        break

                if approved_for_gate:
                    logger.info("=" * 80)
                    logger.info(f"✅ HITL GATE ALREADY APPROVED: {gate_node_id}")
                    logger.info(f"   Approval ID: {approved_for_gate.id}")
                    logger.info(f"   Approved by: {approved_for_gate.responded_by}")
                    logger.info(f"   Approved at: {approved_for_gate.responded_at}")
                    logger.info("   Passing through to next step...")
                    logger.info("=" * 80)

                    # Check if the user edited the config in the Config Editor.
                    # If so, pass the edited version to the next crew instead of
                    # the original crew output.
                    try:
                        from src.repositories.execution_history_repository import (
                            ExecutionHistoryRepository,
                        )

                        exec_repo = ExecutionHistoryRepository(session)
                        execution = await exec_repo.get_execution_by_job_id(job_id)
                        if execution and execution.checkpoint_data:
                            edited_config = execution.checkpoint_data.get(
                                "edited_config"
                            )
                            if edited_config:
                                import json

                                logger.info(
                                    "   📝 Found edited_config in checkpoint_data — using user edits"
                                )
                                return json.dumps(edited_config)
                    except Exception as e:
                        logger.warning(f"   Could not check for edited_config: {e}")

                    # No edited config — pass original output
                    return previous_output

            # Get previous crew name and output
            previous_crew_name = previous_method_name
            previous_crew_output = None
            if previous_output:
                if isinstance(previous_output, str):
                    previous_crew_output = previous_output
                elif hasattr(previous_output, "raw"):
                    previous_crew_output = str(previous_output.raw)
                else:
                    previous_crew_output = str(previous_output)

            # Get flow state snapshot
            flow_state_snapshot = {}
            if hasattr(self, "state"):
                try:
                    if hasattr(self.state, "model_dump"):
                        flow_state_snapshot = self.state.model_dump()
                    elif isinstance(self.state, dict):
                        flow_state_snapshot = dict(self.state)
                except Exception as e:
                    logger.warning(f"Could not serialize flow state: {e}")

            # Get flow_uuid for checkpoint
            flow_uuid = None
            logger.info("🔍 HITL gate checkpoint extraction:")
            logger.info(f"   hasattr(self, 'state'): {hasattr(self, 'state')}")
            if hasattr(self, "state"):
                state = self.state
                logger.info(f"   self.state type: {type(state)}")
                logger.info(f"   hasattr(self.state, 'id'): {hasattr(state, 'id')}")
                if hasattr(state, "id"):
                    flow_uuid = getattr(state, "id", None)
                    logger.info(f"   ✅ Extracted flow_uuid from state.id: {flow_uuid}")
                elif isinstance(state, dict) and "id" in state:
                    # Try to get id from dict-like state
                    flow_uuid = state["id"]
                    logger.info(
                        f"   ✅ Extracted flow_uuid from dict state['id']: {flow_uuid}"
                    )

            # Fallback: Generate a UUID if none was found
            # This ensures checkpoint functionality works even if @persist state.id is not available
            if not flow_uuid:
                flow_uuid = str(uuid.uuid4())
                logger.warning(
                    f"   ⚠️ No state.id found - generated fallback flow_uuid: {flow_uuid}"
                )
                # Store in state for future reference if possible
                if hasattr(self, "state"):
                    try:
                        if hasattr(self.state, "id"):
                            setattr(self.state, "id", flow_uuid)
                        elif isinstance(self.state, dict):
                            self.state["id"] = flow_uuid
                        logger.info("   ✅ Stored generated flow_uuid in state")
                    except Exception as e:
                        logger.warning(f"   Could not store flow_uuid in state: {e}")

            # Create HITL approval request
            async with request_scoped_session() as session:
                hitl_service = HITLService(session)
                webhook_service = HITLWebhookService(session)

                approval = await hitl_service.create_approval_request(
                    execution_id=job_id,
                    flow_id=flow_id or "",
                    gate_node_id=gate_node_id,
                    crew_sequence=crew_sequence,
                    gate_config=gate_config,
                    group_id=group_id,
                    previous_crew_name=previous_crew_name,
                    previous_crew_output=previous_crew_output,
                    flow_state_snapshot=flow_state_snapshot,
                )

                await session.commit()

                logger.info(f"✅ Created HITL approval {approval.id}")
                logger.info(f"   Execution: {job_id}")
                logger.info(f"   Gate: {gate_node_id}")
                logger.info(f"   Expires at: {approval.expires_at}")

                # Send webhook notification
                try:
                    # Build approval URL (this would be configured in settings)
                    approval_url = f"/flows/approvals/{approval.id}"

                    await webhook_service.send_gate_reached_notification(
                        approval=approval, approval_url=approval_url
                    )
                except Exception as e:
                    logger.warning(f"Failed to send webhook notification: {e}")

            # Raise exception to pause flow
            logger.info("🛑 PAUSING FLOW FOR HUMAN APPROVAL")
            logger.info("=" * 80)

            raise FlowPausedForApprovalException(
                approval_id=approval.id,
                gate_node_id=gate_node_id,
                message=gate_config.get("message", "Approval required to proceed"),
                execution_id=job_id,
                crew_sequence=crew_sequence,
                flow_uuid=flow_uuid,
            )

        # Set metadata on both wrapper AND wrapped function
        hitl_gate_method.__name__ = method_name
        hitl_gate_method.__qualname__ = method_name
        hitl_gate_method._meth.__name__ = method_name
        hitl_gate_method._meth.__qualname__ = method_name
        return hitl_gate_method
