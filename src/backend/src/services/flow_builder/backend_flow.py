from src.utils.model_config import DEFAULT_ENGINE_MODEL

"""
Base BackendFlow class for handling flow execution.

Handles the creation and execution of CrewAI flows.
"""
import asyncio
import json
import logging
import os
import time
import traceback
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from src.core.llm.transport import LLM
from src.core.logger import LoggerManager
from src.repositories.flow_repository import FlowRepository
from src.services.flow_builder.conversation.interrupt import (
    APPROVAL_CONFIG_KEY,
    interrupt_inputs,
)
from src.services.flow_builder.conversation.thread import thread_state_uuid
from src.services.flow_builder.conversation.turn import (
    close_turn_async,
    is_conversational,
    turn_inputs,
)
from src.services.flow_builder.exceptions import FlowPausedForApprovalException

# Import the refactored modules
from src.services.flow_builder.modules.flow_builder import FlowBuilder
from src.services.flow_builder.runtime import Flow as CrewAIFlow
from src.services.llm.manager import LLMManager
from src.services.tools.tool_factory import ToolFactory

# Initialize logger manager - use flow logger for flow execution
logger = LoggerManager.get_instance().flow

#: Keys the UI puts in a run's ``inputs`` that describe the RUN, not the flow's
#: state. They travel in the same dict as the user's actual inputs, and on an
#: untyped dict state they simply became stray keys nobody read.
#:
#: A TYPED state refuses a key it has no channel for — which is the behaviour
#: that makes a misspelled input visible — so these have to be filtered at the
#: boundary or every conversational flow fails its kickoff with
#: ``Flow state has no channel(s) ['flow_id', 'run_name']``. They are metadata
#: about the request, and a flow's state is not where a request's metadata
#: belongs.
RUN_METADATA_INPUTS = frozenset({"flow_id", "run_name", "execution_id", "job_id"})


def _extract_flow_uuid(engine_flow) -> Optional[str]:
    """Extract CrewAI's flow state id — used as the checkpoint/resume ``flow_uuid``.

    CrewAI wraps flow state in a ``StateProxy`` whose ``id`` is NOT exposed as an
    attribute (``hasattr(state, 'id')`` is False) and which is not a ``dict``, so the
    naive ``getattr``/``isinstance`` checks miss it and ``flow_uuid`` comes back None —
    which means ``set_checkpoint_active`` never fires and no resumable checkpoint is
    recorded. Try ``model_dump()['id']``, subscript, then attribute access to cover
    StateProxy, dict, and plain pydantic states.
    """
    try:
        state = getattr(engine_flow, "state", None)
    except Exception:
        return None
    if state is None:
        return None

    sid = None
    # 1) Direct attribute — plain pydantic states / objects exposing .id.
    #    StateProxy raises AttributeError for 'id' (handled), so we fall through.
    try:
        sid = getattr(state, "id", None)
    except Exception:
        sid = None
    # 2) model_dump() — CrewAI StateProxy surfaces 'id' here, not as an attribute.
    if sid is None and hasattr(state, "model_dump"):
        try:
            sid = state.model_dump().get("id")
        except Exception:
            sid = None
    # 3) Subscript — dict-like / StateProxy.__getitem__.
    if sid is None:
        try:
            sid = state["id"]
        except Exception:
            sid = None
    return str(sid) if sid is not None else None


class BackendFlow:
    """Base BackendFlow class for handling flow execution"""

    def __init__(
        self,
        job_id: Optional[str] = None,
        flow_id: Optional[Union[uuid.UUID, str]] = None,
        tracing: bool = False,
    ):
        """
        Initialize a new BackendFlow instance.

        Args:
            job_id: Optional job ID for tracking
            flow_id: Optional flow ID to load from database
            tracing: Enable MLflow tracing for this flow execution
        """
        self._job_id = job_id
        self._tracing_enabled = tracing

        # Handle flow_id conversion more safely
        if flow_id is None:
            self._flow_id = None
        elif isinstance(flow_id, uuid.UUID):
            self._flow_id = flow_id
        else:
            try:
                self._flow_id = uuid.UUID(flow_id)
            except (ValueError, AttributeError, TypeError):
                logger.error(f"Invalid flow_id format: {flow_id}")
                raise ValueError(f"Invalid flow_id format: {flow_id}")

        self._flow_data = None
        # Set when a turn was answered from state and no crew ran.
        self._state_answer: Optional[str] = None
        #: The outcome this turn selected, if narrowing chose one.
        self._turn_outcome: Optional[str] = None
        # Don't store API keys directly, just other configuration
        self._config = {}
        # Repository container
        self._repositories = {}
        logger.info(f"Initializing BackendFlow{' for job ' + job_id if job_id else ''}")

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def repositories(self):
        return self._repositories

    @repositories.setter
    def repositories(self, value):
        self._repositories = value

    async def load_flow(self, repository: Optional[FlowRepository] = None) -> Dict:
        """
        Load flow data from the database using repository if provided,
        otherwise get one from the factory.

        Args:
            repository: Optional FlowRepository instance

        Returns:
            Dictionary containing flow data
        """
        logger.info(f"Loading flow with ID: {self._flow_id}")

        if not self._flow_id:
            logger.error("No flow_id provided")
            raise ValueError("No flow_id provided")

        try:
            # Use provided repository or get one from the factory
            if repository:
                flow = await repository.get(self._flow_id)
            else:
                # Log error if no repository provided
                logger.error(f"No flow repository provided for flow_id {self._flow_id}")
                raise ValueError(
                    f"No flow repository provided for flow_id {self._flow_id}"
                )

            if not flow:
                logger.error(f"Flow with ID {self._flow_id} not found")
                raise ValueError(f"Flow with ID {self._flow_id} not found")

            self._flow_data = {
                "id": flow.id,
                "name": flow.name,
                "crew_id": flow.crew_id,
                "nodes": flow.nodes,
                "edges": flow.edges,
                "flow_config": flow.flow_config,
            }
            logger.info(f"Successfully loaded flow: {flow.name}")
            logger.info(f"Flow configuration: {flow.flow_config}")
            return self._flow_data
        except Exception as e:
            logger.error(f"Error loading flow data: {e}", exc_info=True)
            raise

    async def _get_llm(self) -> LLM:
        """
        Get a properly configured LLM for CrewAI using LLMManager.
        This ensures API keys are properly set from the database.
        """
        try:
            # Get the default model name from environment or use a default
            model_name = os.getenv("DEFAULT_LLM_MODEL", DEFAULT_ENGINE_MODEL)
            logger.info(f"Getting LLM model: {model_name} for flow execution")

            # Use LLMManager to get a properly configured LLM
            llm = await LLMManager.get_llm(model_name)
            logger.info(f"Successfully configured LLM: {model_name}")
            return llm
        except Exception as e:
            logger.error(f"Error configuring LLM: {e}", exc_info=True)
            raise

    async def flow(self) -> CrewAIFlow:
        """Creates and returns a CrewAI Flow instance based on the loaded flow configuration"""
        logger.info("Creating CrewAI Flow")

        # CRITICAL: Set group context for multi-tenant isolation before ANY LLM calls
        group_context = self._config.get("group_context")
        if group_context:
            try:
                from src.utils.user_context import UserContext

                UserContext.set_group_context(group_context)
                logger.info(
                    f"Set group context for flow execution: {getattr(group_context, 'primary_group_id', 'unknown')}"
                )
            except Exception as e:
                logger.warning(f"Could not set group context: {e}")

        if not self._flow_data:
            # Check if this is an unsaved flow with data in config
            config_has_nodes = (
                "nodes" in self._config
                and self._config["nodes"]
                and len(self._config["nodes"]) > 0
            )

            if config_has_nodes:
                # Unsaved flow - populate _flow_data from config
                logger.info(
                    f"[flow] Unsaved flow detected - populating _flow_data from config with {len(self._config['nodes'])} nodes"
                )
                self._flow_data = {
                    "id": self._flow_id,
                    "name": self._config.get("name", "Unsaved Flow"),
                    "crew_id": self._config.get("crew_id"),
                    "nodes": self._config["nodes"],
                    "edges": self._config.get("edges", []),
                    "flow_config": self._config.get("flow_config", {}),
                }
                logger.info(
                    "[flow] Successfully populated _flow_data from config for unsaved flow"
                )
            else:
                # Saved flow - load from database
                flow_repo = self._repositories.get("flow")
                await self.load_flow(repository=flow_repo)

        if not self._flow_data:
            logger.error("Flow data could not be loaded")
            raise ValueError("Flow data could not be loaded")

        try:
            # Initialize callbacks for this flow execution
            self._init_callbacks()

            # Extract checkpoint resume parameters from config
            resume_from_flow_uuid = self._config.get("resume_from_flow_uuid")
            resume_from_crew_sequence = self._config.get("resume_from_crew_sequence")
            resume_from_execution_id = self._config.get("resume_from_execution_id")
            if resume_from_flow_uuid:
                logger.info(f"Resuming flow from checkpoint: {resume_from_flow_uuid}")
                if resume_from_crew_sequence is not None:
                    logger.info(
                        f"Resuming from crew sequence: {resume_from_crew_sequence} (will skip crews up to this sequence)"
                    )
                if resume_from_execution_id is not None:
                    logger.info(
                        f"Resume from execution ID: {resume_from_execution_id} (will query traces for checkpoint data)"
                    )

            # Build the flow using the FlowBuilder module
            dynamic_flow = await FlowBuilder.build_flow(
                flow_data=self._flow_data,
                repositories=self._repositories,
                callbacks=self._config.get("callbacks", {}),
                group_context=self._config.get("group_context"),
                restore_uuid=resume_from_flow_uuid,
                resume_from_crew_sequence=resume_from_crew_sequence,
                resume_from_execution_id=resume_from_execution_id,
                user_token=self._config.get("user_token"),
                group_id=self._config.get("group_id"),
            )

            logger.info("Flow created successfully")
            return dynamic_flow

        except Exception as e:
            logger.error(f"Error creating flow: {e}", exc_info=True)
            raise ValueError(f"Failed to create flow: {str(e)}")

    def _init_callbacks(self):
        """
        Initialize callbacks for flow execution.

        Note: For flows, we don't use JobOutputCallback (async) like regular crews.
        Instead, we rely on:
        1. The OTel event bridge for execution traces (registered in the subprocess)
        2. Synchronous step_callback and task_callback set on each Crew instance
        """
        # Set group context in UserContext for multi-tenant isolation
        group_context = self._config.get("group_context")
        if group_context:
            try:
                from src.utils.user_context import UserContext

                UserContext.set_group_context(group_context)
                logger.info("Set group context for flow execution callbacks")
            except Exception as e:
                logger.warning(f"Could not set group context in _init_callbacks: {e}")

        # For flows, we only need minimal callback setup with job_id and flow_id
        # The actual logging/tracing is handled by:
        # 1. LogWriterTask + the OTel event bridge (initialized in subprocess)
        # 2. Synchronous callbacks set on each Crew instance in flow methods
        flow_id_for_callbacks = str(self._flow_id) if self._flow_id else None
        self._config["callbacks"] = {
            "handlers": [],  # No async handlers for flows
            "job_id": self._job_id,  # Pass job_id directly for sync callbacks
            "flow_id": flow_id_for_callbacks,  # Pass flow_id for HITL webhooks
            "start_trace_writer": True,  # Signal to start trace writer in subprocess
        }
        logger.info(
            f"Initialized flow callbacks with job_id={self._job_id}, flow_id={self._flow_id} (type: {type(self._flow_id).__name__})"
        )
        logger.info(
            f"Callbacks dict flow_id: {flow_id_for_callbacks} (type: {type(flow_id_for_callbacks).__name__ if flow_id_for_callbacks else 'None'})"
        )

    def _state_config_source(self) -> Dict[str, Any]:
        """The flow's whole config, from wherever this run carries it."""
        for source in (getattr(self, "_flow_data", None) or {}, self._config or {}):
            flow_config = source.get("flow_config")
            if isinstance(flow_config, dict) and flow_config:
                return flow_config
        return {}

    def _state_config(self) -> Dict[str, Any]:
        """The flow's declared state block, from wherever this run carries it.

        Both places, because both happen: a SAVED flow's config arrives on
        ``_flow_data`` via ``load_flow``, while an unsaved one runs straight
        from ``_config``. Reading only one of them would make a conversational
        flow behave differently depending on whether it had been saved.
        """
        # `getattr` rather than direct access: this runs on the config path,
        # which is reachable before `load_flow` has populated `_flow_data`.
        for source in (getattr(self, "_flow_data", None) or {}, self._config or {}):
            flow_config = source.get("flow_config")
            if isinstance(flow_config, dict):
                state = flow_config.get("state")
                if isinstance(state, dict) and state:
                    return state
        return {}

    def _thread_id(self) -> Optional[str]:
        """The checkpoint lineage this run belongs to, if any.

        An explicit resume wins: the caller named a lineage and must get that
        one. Otherwise the lineage is DERIVED from the conversation and the
        flow, so a second message in the same chat session continues the first
        instead of starting over. No session means no thread, which is how every
        flow runs today.
        """
        explicit = self._config.get("resume_from_flow_uuid")
        if explicit:
            return explicit
        return thread_state_uuid(
            self._config.get("session_id"),
            self._config.get("flow_id"),
            self._config.get("group_id"),
        )

    def _kickoff_inputs(self) -> Dict[str, Any]:
        """What this run passes into flow state.

        The user's inputs, the lineage id, and — for a conversational flow —
        this turn's user line.

        Inputs AND id, not either: the two call sites below used to pass
        ``{"id": resume_uuid}`` when resuming and nothing otherwise, so a
        resumed run silently dropped every input and a normal run had no way to
        send one at all. ``id`` wins on a collision — it addresses the
        checkpoint, and a flow whose own variable is called ``id`` must not be
        able to redirect a restore.

        The turn writes go through the same inputs path on purpose. That path
        already merges through each channel's reducer, so the user line APPENDS
        to the history restored a moment earlier; a separate write would have to
        re-implement merging and would eventually disagree with it.
        """
        inputs = self._config.get("inputs")
        merged: Dict[str, Any] = {
            key: value
            for key, value in (inputs or {}).items()
            if key not in RUN_METADATA_INPUTS
        }
        state_config = self._state_config()

        if is_conversational(state_config):
            user_message = self._config.get("user_message") or merged.pop(
                "user_message", None
            )
            merged.update(turn_inputs(user_message, intent=self._config.get("intent")))

        # A human's decision from a HITL gate, when this run is the resume of one
        # and the flow declared somewhere to put it. Same path as everything
        # else, so it merges through its channel's reducer.
        merged.update(
            interrupt_inputs(state_config, self._config.get(APPROVAL_CONFIG_KEY))
        )

        thread_id = self._thread_id()
        if thread_id:
            merged["id"] = thread_id
        return merged

    async def _plan_turn(self, engine_flow: Any) -> Optional[str]:
        """Decide what this turn does. Returns an answer when nothing needs to run.

        Three outcomes, and the third is the point: a turn asking ABOUT work
        already done is answered from state and no crew executes. The material
        is in memory, put there by crews already paid for, and re-running one to
        retell it is both slower and worse — a fresh run gathers again and may
        not find the same things.
        """
        await self._narrow_to_outcome(engine_flow)
        return getattr(self, "_state_answer", None)

    async def _narrow_to_outcome(self, engine_flow: Any) -> None:
        """Run only what produces what THIS turn asked for.

        A conversational flow is asked for different things on different turns.
        Running the whole graph every time re-does work the conversation already
        has — and produces artefacts nobody asked for. So the turn picks an OUTCOME
        (what to produce) and the runtime narrows to the methods that produce it;
        the reuse layer then covers the material upstream.

        Distinct from a ROUTER, which is unchanged: a router decides which branch
        the DATA implies, during execution. This decides what the TURN wants,
        before it. Silent on every failure — no outcome, no model, an unreachable
        target — and the flow runs exactly as it does today.
        """
        if not is_conversational(self._state_config()):
            return
        question = self._config.get("user_message")
        if not question:
            return
        try:
            from src.services.flow_builder.conversation.outcomes import (
                build_registry,
                select_outcome,
                trigger_for,
            )

            flow_config = self._state_config_source()
            state = getattr(engine_flow, "state", None)
            choice = await select_outcome(
                str(question),
                flow_config,
                self._config.get("group_context"),
                self._config.get("model"),
                # The conversation, so a fragment can be matched. "and for
                # Germany?" names no outcome on its own and would otherwise
                # decline into running the whole flow.
                getattr(state, "messages", None),
            )

            # Nothing to run: the turn is about work already done.
            if choice.answer_from_state:
                from src.services.flow_builder.conversation.retrieval import (
                    answer_from_state,
                )

                answer = await answer_from_state(
                    str(question), state, self._config.get("model")
                )
                if answer:
                    self._state_answer = answer
                    logger.info(f"[flow-outcome] no crew needed: {choice.reason}")
                    return
                # Could not answer from what is there after all — run the flow
                # rather than return nothing.
                logger.info(
                    "[flow-outcome] state could not answer the turn; running the flow"
                )
                return

            outcome, confidence, reason = (
                choice.outcome,
                choice.confidence,
                choice.reason,
            )
            if not outcome:
                logger.info(f"[flow-outcome] running the whole flow: {reason}")
                return

            registry = build_registry(
                getattr(engine_flow, "_kasal_method_crews", {}) or {},
                getattr(engine_flow, "_kasal_crew_identities", {}) or {},
            )
            method = trigger_for(registry, outcome)
            targets = {method} if method else set()
            # On the flow OBJECT, not in state. `_plan_turn` runs before
            # `kickoff_async`, and kickoff restores the checkpoint over the
            # state — `_restore_state` writes back every stored channel, which
            # is what makes a crew's output survive and also means anything
            # written here is replaced by the PREVIOUS turn's value. Reuse asks
            # which crew this turn selected; reading it from state gave it the
            # last turn's answer, so it protected the wrong crew and re-ran the
            # material it should have reused.
            setattr(engine_flow, "_kasal_selected_outcome", outcome)
            # And into state through the INPUTS, which kickoff merges AFTER the
            # restore — so the `last_outcome` channel a condition or the next
            # turn reads is finally this turn's choice rather than the last
            # one's. It has been wrong since the channel was added.
            self._turn_outcome = outcome

            if engine_flow.narrow_to(targets):
                logger.info(
                    f"[flow-outcome] this turn produces '{outcome}' "
                    f"(confidence {confidence:.2f}): {reason}"
                )
            else:
                logger.info(
                    f"[flow-outcome] '{outcome}' is not reachable in this graph; "
                    "running the whole flow"
                )
        except Exception as exc:  # noqa: BLE001 — narrowing is an optimisation
            logger.warning(f"[flow-outcome] narrowing skipped: {exc}")

    async def _close_turn(self, engine_flow: Any, result: Any) -> None:
        """End a conversational turn on the state the next one will restore.

        A turn does not end at its last method: the answer has to be recorded
        and the history bounded, and both happen AFTER the graph finishes — so
        the checkpoint written during the run does not have them. Hence the
        explicit save.

        Silent for a non-conversational flow, which is every flow today.
        """
        if not is_conversational(self._state_config()):
            return
        state = getattr(engine_flow, "state", None)
        if state is None or not hasattr(state, "merge"):
            return
        try:
            await close_turn_async(state, result, self._config.get("model"))
            engine_flow.save_checkpoint("turn_end")
        except Exception as exc:  # noqa: BLE001 — a bookkeeping failure must
            # not fail a turn the user already got an answer from.
            logger.warning(f"[flow-thread] could not close turn: {exc}")

    async def kickoff_async(self) -> Dict[str, Any]:
        """
        Async version of kickoff for better performance.
        Uses CrewAI's native kickoff_async() when available.
        """
        logger.info(f"Kicking off async flow execution for job {self._job_id}")

        # CRITICAL: Set group context for multi-tenant isolation before ANY operations
        group_context = self._config.get("group_context")
        if group_context:
            try:
                from src.utils.user_context import UserContext

                UserContext.set_group_context(group_context)
                logger.info(
                    f"Set group context for kickoff_async: {getattr(group_context, 'primary_group_id', 'unknown')}"
                )
            except Exception as e:
                logger.warning(f"Could not set group context in kickoff_async: {e}")

        # Get callbacks for use in finally block
        callbacks = self._config.get("callbacks", {})

        try:
            # Start the trace writer if tracing is enabled
            if self._tracing_enabled or callbacks.get("start_trace_writer", False):
                try:
                    from src.services.execution.logs.writer_task import LogWriterTask

                    await LogWriterTask.ensure_writer_started()
                    logger.info(
                        "Successfully started trace writer for event processing"
                    )
                except Exception as e:
                    logger.warning(f"Error starting trace writer: {e}", exc_info=True)

            # Load flow data if needed
            if not self._flow_data:
                # Check if this is an unsaved flow with data in config
                config_has_nodes = (
                    "nodes" in self._config
                    and self._config["nodes"]
                    and len(self._config["nodes"]) > 0
                )

                if config_has_nodes:
                    # Unsaved flow - populate _flow_data from config
                    logger.info(
                        f"[kickoff_async] Unsaved flow detected - populating _flow_data from config with {len(self._config['nodes'])} nodes"
                    )
                    self._flow_data = {
                        "id": self._flow_id,
                        "name": self._config.get("name", "Unsaved Flow"),
                        "crew_id": self._config.get("crew_id"),
                        "nodes": self._config["nodes"],
                        "edges": self._config.get("edges", []),
                        "flow_config": self._config.get("flow_config", {}),
                    }
                    logger.info(
                        "[kickoff_async] Successfully populated _flow_data from config for unsaved flow"
                    )
                else:
                    # Saved flow - load from database
                    try:
                        flow_repo = self._repositories.get("flow")
                        await self.load_flow(repository=flow_repo)
                        logger.info(
                            "Successfully loaded flow data during kickoff_async"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error loading flow data during kickoff_async: {e}",
                            exc_info=True,
                        )
                        return {
                            "success": False,
                            "error": f"Failed to load flow data: {str(e)}",
                            "flow_id": self._flow_id,
                        }

            # Merge config data into flow_data (frontend config takes precedence)
            # This ensures frontend-provided flow_config, nodes, and edges are used
            if "flow_config" in self._config:
                logger.info(
                    "[kickoff_async] Using flow_config from self._config (has latest updates)"
                )
                self._flow_data["flow_config"] = self._config["flow_config"]
            if "nodes" in self._config:
                self._flow_data["nodes"] = self._config["nodes"]
            if "edges" in self._config:
                self._flow_data["edges"] = self._config["edges"]
                logger.info(
                    f"[kickoff_async] Merged {len(self._config['edges'])} edges from config"
                )

            # Create the CrewAI flow instance
            try:
                engine_flow = await self.flow()
                logger.info(
                    "Successfully created CrewAI flow instance for async execution"
                )
            except Exception as e:
                logger.error(f"Error creating CrewAI flow: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": f"Failed to create CrewAI flow: {str(e)}",
                    "flow_id": self._flow_id,
                }

            # Execute using CrewAI's native kickoff_async if available
            logger.info("Starting async flow execution")
            logger.info(f"Flow instance type: {type(engine_flow)}")
            logger.info(
                f"Flow has kickoff_async: {hasattr(engine_flow, 'kickoff_async')}"
            )

            try:
                if hasattr(engine_flow, "kickoff_async"):
                    logger.info("Using CrewAI's native kickoff_async method")
                    logger.info("About to call kickoff_async() on flow instance")

                    # The run's inputs, plus the checkpoint id when resuming.
                    # The engine loads persisted state when 'id' is present.
                    kickoff_inputs = self._kickoff_inputs()
                    state_answer = await self._plan_turn(engine_flow)
                    # Planning decides the outcome; the inputs path is what puts
                    # it into state, because that merge happens after the
                    # checkpoint restore (see _narrow_to_outcome).
                    if getattr(self, "_turn_outcome", None):
                        kickoff_inputs["last_outcome"] = self._turn_outcome
                    if state_answer is not None:
                        result = state_answer
                        await self._close_turn(engine_flow, result)
                    elif kickoff_inputs:
                        logger.info(
                            f"Passing {sorted(kickoff_inputs)} to kickoff_async"
                        )
                        result = await engine_flow.kickoff_async(inputs=kickoff_inputs)
                    else:
                        result = await engine_flow.kickoff_async()
                    logger.info(f"kickoff_async() returned: {type(result)}")
                    await self._close_turn(engine_flow, result)

                    # DIAGNOSTIC: Log method_outputs from CrewAI Flow
                    if hasattr(engine_flow, "_method_outputs"):
                        logger.info(
                            f"🔍 DIAGNOSTIC - Flow._method_outputs count: {len(engine_flow._method_outputs)}"
                        )
                        for idx, output in enumerate(engine_flow._method_outputs):
                            if hasattr(output, "raw") and output.raw:
                                logger.info(
                                    f"🔍 DIAGNOSTIC - _method_outputs[{idx}].raw length: {len(str(output.raw))}"
                                )
                                raw_str = str(output.raw)
                                if len(raw_str) > 400:
                                    logger.info(
                                        f"🔍 DIAGNOSTIC - _method_outputs[{idx}] END: ...{raw_str[-200:]}"
                                    )
                            else:
                                logger.info(
                                    f"🔍 DIAGNOSTIC - _method_outputs[{idx}] type: {type(output)}, str length: {len(str(output))}"
                                )
                else:
                    logger.info(
                        "kickoff_async not available, using synchronous kickoff"
                    )
                    logger.info("About to call kickoff() on flow instance")
                    # CRITICAL: Pass restore_uuid as 'id' in inputs for checkpoint resume
                    resume_from_flow_uuid = self._config.get("resume_from_flow_uuid")
                    if resume_from_flow_uuid:
                        logger.info(
                            f"Passing id={resume_from_flow_uuid} to kickoff for checkpoint resume"
                        )
                        result = engine_flow.kickoff(
                            inputs={"id": resume_from_flow_uuid}
                        )
                    else:
                        result = engine_flow.kickoff()
                    logger.info(f"kickoff() returned: {type(result)}")

                logger.info("Flow executed successfully via kickoff_async")

                # Process result the same way as crew results
                # Extract raw content directly without wrapping
                logger.info(f"Processing flow result, type: {type(result)}")
                if result is None:
                    result_value = None
                elif hasattr(result, "raw") and result.raw:
                    # CrewOutput object with raw attribute - extract raw content directly
                    # This matches how crew execution captures results
                    result_value = result.raw
                    logger.info(
                        f"Extracted raw output from flow result, length: {len(str(result_value))}"
                    )
                elif isinstance(result, dict):
                    result_value = result
                    logger.info("Flow result is already a dictionary")
                elif isinstance(result, str):
                    result_value = result
                    logger.info(f"Flow result is a string, length: {len(result_value)}")
                elif hasattr(result, "to_dict"):
                    result_value = result.to_dict()
                    logger.info("Used to_dict() for flow result conversion")
                elif hasattr(result, "__dict__"):
                    result_value = result.__dict__
                    logger.info("Used __dict__ for flow result conversion")
                else:
                    result_value = str(result)
                    logger.info("Used string fallback for flow result conversion")

                logger.info(f"Flow result processed, type: {type(result_value)}")

                # ── CI/CD artifact aggregation ─────────────────────────────────
                # flow_methods.py accumulates cicd_download_url entries from every
                # crew into state['_cicd_artifacts'] as the flow runs.  Read that
                # list here and inject it into result_value so ShowResult shows
                # download buttons for ALL artifacts (Genie Space + Dashboard),
                # not just the last crew's output.
                try:
                    cicd_artifacts: List[Dict[str, Any]] = []
                    if hasattr(engine_flow, "state") and engine_flow.state is not None:
                        try:
                            cicd_artifacts = (
                                engine_flow.state.get("_cicd_artifacts") or []
                            )
                        except Exception:
                            cicd_artifacts = []

                    if cicd_artifacts:
                        if isinstance(result_value, dict):
                            result_value["_cicd_all"] = cicd_artifacts
                        elif isinstance(result_value, str):
                            try:
                                _rv = json.loads(result_value)
                                _rv["_cicd_all"] = cicd_artifacts
                                result_value = json.dumps(_rv, indent=2)
                            except Exception:
                                result_value = json.dumps(
                                    {
                                        "_result": result_value,
                                        "_cicd_all": cicd_artifacts,
                                    },
                                    indent=2,
                                )
                        logger.info(
                            f"[BackendFlow] Injected {len(cicd_artifacts)} CI/CD artifacts "
                            f"into flow result: {[a.get('cicd_type') for a in cicd_artifacts]}"
                        )
                    else:
                        logger.info(
                            "[BackendFlow] No CI/CD artifacts found in flow state."
                        )
                except Exception as _cicd_err:
                    logger.warning(
                        f"[BackendFlow] CI/CD artifact injection failed: {_cicd_err}"
                    )
                # ── end CI/CD aggregation ──────────────────────────────────────

                # Extract flow_uuid (state.id) for checkpoint/resume functionality
                # This is CrewAI's internal state identifier when using @persist
                flow_uuid = _extract_flow_uuid(engine_flow)
                if flow_uuid:
                    logger.info(
                        f"Extracted flow_uuid (state.id) for checkpoint: {flow_uuid}"
                    )
                else:
                    logger.warning(
                        "Could not extract flow_uuid from flow state for checkpoint"
                    )

                return {
                    "success": True,
                    "result": result_value,
                    "flow_id": self._flow_id,
                    "flow_uuid": flow_uuid,  # CrewAI state.id for checkpoint resume
                }
            except FlowPausedForApprovalException:
                # Re-raise HITL pause exception so it's handled by flow_runner_service
                logger.info(
                    "Re-raising FlowPausedForApprovalException from inner handler"
                )
                raise
            except Exception as exec_error:
                logger.error(
                    f"Error during async flow execution: {exec_error}", exc_info=True
                )
                return {
                    "success": False,
                    "error": str(exec_error),
                    "flow_id": self._flow_id,
                }

        except FlowPausedForApprovalException:
            # Re-raise HITL pause exception so it's handled by flow_runner_service
            logger.info("Re-raising FlowPausedForApprovalException from outer handler")
            raise
        except Exception as e:
            logger.error(f"Error during flow kickoff_async: {e}", exc_info=True)
            return {"success": False, "error": str(e), "flow_id": self._flow_id}
        finally:
            # Nothing to tear down: flows register no async callback objects.
            # Live events and traces come off the event bus in the subprocess.
            pass

    async def kickoff(self) -> Dict[str, Any]:
        """Execute the flow and return the result"""
        logger.info(f"Kicking off flow execution for job {self._job_id}")

        # CRITICAL: Set group context for multi-tenant isolation before ANY operations
        group_context = self._config.get("group_context")
        if group_context:
            try:
                from src.utils.user_context import UserContext

                UserContext.set_group_context(group_context)
                logger.info(
                    f"Set group context for kickoff: {getattr(group_context, 'primary_group_id', 'unknown')}"
                )
            except Exception as e:
                logger.warning(f"Could not set group context in kickoff: {e}")

        # Get callbacks for use in finally block
        callbacks = self._config.get("callbacks", {})

        try:
            # Start the trace writer if tracing is enabled
            if self._tracing_enabled or callbacks.get("start_trace_writer", False):
                try:
                    from src.services.execution.logs.writer_task import LogWriterTask

                    await LogWriterTask.ensure_writer_started()
                    logger.info(
                        "Successfully started trace writer for event processing"
                    )
                except Exception as e:
                    logger.warning(f"Error starting trace writer: {e}", exc_info=True)
                    # Continue execution even if trace writer fails

            # Make sure we have flow data loaded
            if not self._flow_data:
                # Check if this is an unsaved flow with data in config
                config_has_nodes = (
                    "nodes" in self._config
                    and self._config["nodes"]
                    and len(self._config["nodes"]) > 0
                )

                if config_has_nodes:
                    # Unsaved flow - populate _flow_data from config
                    logger.info(
                        f"Unsaved flow detected - populating _flow_data from config with {len(self._config['nodes'])} nodes"
                    )
                    self._flow_data = {
                        "id": self._flow_id,  # Use the generated flow_id
                        "name": self._config.get("name", "Unsaved Flow"),
                        "crew_id": self._config.get("crew_id"),
                        "nodes": self._config["nodes"],
                        "edges": self._config.get("edges", []),
                        "flow_config": self._config.get("flow_config", {}),
                    }
                    logger.info(
                        "Successfully populated _flow_data from config for unsaved flow"
                    )
                else:
                    # Saved flow - load from database
                    try:
                        # Use the repository from the service if provided
                        flow_repo = self._repositories.get("flow")
                        await self.load_flow(repository=flow_repo)
                        logger.info("Successfully loaded flow data during kickoff")
                    except Exception as e:
                        logger.error(
                            f"Error loading flow data during kickoff: {e}",
                            exc_info=True,
                        )
                        return {
                            "success": False,
                            "error": f"Failed to load flow data: {str(e)}",
                            "flow_id": self._flow_id,
                        }

            # CRITICAL: If config has an updated flow_config (with startingPoints), use it
            # This ensures frontend-provided flow_config takes precedence over database version
            if "flow_config" in self._config:
                logger.info(
                    "[kickoff] Using flow_config from self._config (has latest updates)"
                )
                self._flow_data["flow_config"] = self._config["flow_config"]

                # Also update nodes/edges if they're in config
                if "nodes" in self._config:
                    self._flow_data["nodes"] = self._config["nodes"]
                if "edges" in self._config:
                    self._flow_data["edges"] = self._config["edges"]

                logger.info("[kickoff] Updated flow_data with flow_config from config")
                if "startingPoints" in self._config.get("flow_config", {}):
                    logger.info(
                        f"[kickoff] flow_config has {len(self._config['flow_config']['startingPoints'])} startingPoints"
                    )

            # Create the CrewAI flow
            try:
                # Create the flow instance by awaiting the coroutine
                engine_flow = await self.flow()
                logger.info("Successfully created CrewAI flow instance")
            except Exception as e:
                logger.error(f"Error creating CrewAI flow: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": f"Failed to create CrewAI flow: {str(e)}",
                    "flow_id": self._flow_id,
                }

            # Execute the flow asynchronously - find start methods
            logger.info("=" * 100)
            logger.info("STARTING FLOW EXECUTION - Looking for start methods")
            logger.info("=" * 100)

            # Get all methods of the flow instance that are decorated with @start.
            # Skip dunders BEFORE calling getattr: pydantic v2 exposes
            # ``__signature__`` as a class-only descriptor on model instances, so
            # ``getattr(instance, '__signature__')`` raises AttributeError. The
            # ``starting_point_*`` methods we care about are never dunders.
            start_methods = []
            all_methods = []
            for attr_name in dir(engine_flow):
                if attr_name.startswith("_"):
                    continue
                if callable(getattr(engine_flow, attr_name, None)):
                    all_methods.append(attr_name)
                    if attr_name.startswith("starting_point_"):
                        start_methods.append(attr_name)

            logger.info(f"Flow instance type: {type(engine_flow)}")
            logger.info(f"Total callable methods: {len(all_methods)}")
            logger.info(
                f"All methods: {[m for m in all_methods if not m.startswith('_')]}"
            )
            logger.info(f"Found {len(start_methods)} start methods: {start_methods}")

            if not start_methods:
                logger.error("❌ NO START METHODS FOUND! Flow cannot execute.")
                logger.error(
                    "This usually means FlowBuilder._create_dynamic_flow() didn't create start methods properly"
                )
                return {
                    "success": False,
                    "error": "No start methods found in flow. Check flow configuration and startingPoints.",
                    "flow_id": self._flow_id,
                }

            # Execute the flow using CrewAI's kickoff mechanism
            # Use kickoff_async() for async flow execution
            # Do NOT call start methods directly - this bypasses the event system!
            logger.info(
                "Calling flow.kickoff_async() to execute the flow with proper event handling"
            )

            # CRITICAL: Clear the request-scoped session ContextVar before concurrent
            # execution.  CrewAI's kickoff_async() runs all @start() methods via
            # asyncio.gather(), meaning multiple starting-point coroutines execute
            # concurrently.  If they inherit the parent's _request_session, every call
            # to routed_scoped_session() returns the SAME AsyncSession — which is NOT
            # safe for concurrent coroutine use with PostgreSQL (Lakebase).
            # By clearing the ContextVar, each @start()/@listen() method that needs
            # DB access will create its own independent session via
            # routed_scoped_session() → the router (which in this subprocess resolves
            # to Lakebase, already activated by activate_lakebase_in_subprocess()).
            try:
                from src.db.session import _request_session

                _request_session.set(None)
                logger.info(
                    "Cleared _request_session ContextVar before concurrent kickoff"
                )
            except Exception as ctx_err:
                logger.warning(
                    f"Could not clear _request_session ContextVar: {ctx_err}"
                )

            try:
                # THE SECOND call site. Both branches are live — which one runs
                # depends on how the flow was loaded — so a fix applied to one
                # ships and appears not to work.
                kickoff_inputs = self._kickoff_inputs()
                state_answer = await self._plan_turn(engine_flow)
                if state_answer is not None:
                    flow_result = state_answer
                    await self._close_turn(engine_flow, flow_result)
                elif kickoff_inputs:
                    logger.info(f"Passing {sorted(kickoff_inputs)} to kickoff_async")
                    flow_result = await engine_flow.kickoff_async(inputs=kickoff_inputs)
                else:
                    flow_result = await engine_flow.kickoff_async()
                logger.info(
                    f"Flow kickoff_async completed, result type: {type(flow_result)}"
                )
                await self._close_turn(engine_flow, flow_result)

                # DIAGNOSTIC: Log method_outputs from CrewAI Flow
                if hasattr(engine_flow, "_method_outputs"):
                    logger.info(
                        f"🔍 DIAGNOSTIC - Flow._method_outputs count: {len(engine_flow._method_outputs)}"
                    )
                    for idx, output in enumerate(engine_flow._method_outputs):
                        if hasattr(output, "raw") and output.raw:
                            logger.info(
                                f"🔍 DIAGNOSTIC - _method_outputs[{idx}].raw length: {len(str(output.raw))}"
                            )
                            raw_str = str(output.raw)
                            if len(raw_str) > 400:
                                logger.info(
                                    f"🔍 DIAGNOSTIC - _method_outputs[{idx}] END: ...{raw_str[-200:]}"
                                )
                        else:
                            logger.info(
                                f"🔍 DIAGNOSTIC - _method_outputs[{idx}] type: {type(output)}, str length: {len(str(output))}"
                            )
            except Exception as flow_error:
                logger.error(
                    f"Error during flow kickoff_async: {flow_error}", exc_info=True
                )
                raise

            # Process flow result the same way as crew results
            # Extract raw content directly without wrapping in flow_output
            processed_result = None
            try:
                if flow_result is None:
                    logger.warning("Flow kickoff_async returned None")
                    processed_result = None
                elif hasattr(flow_result, "raw") and flow_result.raw:
                    # CrewOutput object with raw attribute - extract raw content directly
                    # This matches how crew execution captures results
                    processed_result = flow_result.raw
                    logger.info(
                        f"Extracted raw output from flow result, length: {len(str(processed_result))}"
                    )
                elif isinstance(flow_result, dict):
                    # Already a dictionary - use as is
                    processed_result = flow_result
                    logger.info("Flow result is already a dictionary")
                elif isinstance(flow_result, str):
                    # String result - use directly
                    processed_result = flow_result
                    logger.info(
                        f"Flow result is a string, length: {len(processed_result)}"
                    )
                elif hasattr(flow_result, "to_dict"):
                    # Use to_dict method if available
                    processed_result = flow_result.to_dict()
                    logger.info("Used to_dict() for flow result conversion")
                elif hasattr(flow_result, "__dict__"):
                    # Use __dict__ as fallback
                    processed_result = flow_result.__dict__
                    logger.info("Used __dict__ for flow result conversion")
                else:
                    # Fallback to string representation
                    processed_result = str(flow_result)
                    logger.info("Used string fallback for flow result conversion")
            except Exception as conv_error:
                logger.error(
                    f"Error processing flow result: {conv_error}", exc_info=True
                )
                # Use string representation as fallback
                processed_result = str(flow_result) if flow_result else None

            logger.info("Flow executed successfully, result processed")

            # Extract flow_uuid (state.id) for checkpoint/resume functionality
            # This is CrewAI's internal state identifier when using @persist
            flow_uuid = _extract_flow_uuid(engine_flow)
            if flow_uuid:
                logger.info(
                    f"Extracted flow_uuid (state.id) for checkpoint: {flow_uuid}"
                )
            else:
                logger.warning(
                    "Could not extract flow_uuid from flow state for checkpoint"
                )

            return {
                "success": True,
                "result": processed_result,
                "flow_id": self._flow_id,
                "flow_uuid": flow_uuid,  # CrewAI state.id for checkpoint resume
            }
        except FlowPausedForApprovalException:
            # Re-raise HITL pause exception so it's handled by flow_runner_service
            logger.info("Re-raising FlowPausedForApprovalException for proper handling")
            raise
        except Exception as e:
            logger.error(f"Error during flow kickoff: {e}", exc_info=True)
            return {"success": False, "error": str(e), "flow_id": self._flow_id}
        finally:
            # Nothing to tear down: flows register no async callback objects.
            # Live events and traces come off the event bus in the subprocess.
            pass

    async def plot(self, filename: str = "flow_diagram") -> Optional[str]:
        """
        Generate flow visualization using CrewAI's plot functionality.

        Args:
            filename: Name of the output file (without extension)

        Returns:
            Path to the generated visualization file, or None if plot is not available
        """
        logger.info(f"Generating flow visualization: {filename}")

        try:
            # Create the CrewAI flow instance
            engine_flow = await self.flow()

            # Check if plot method is available
            if hasattr(engine_flow, "plot"):
                logger.info("Using CrewAI's native plot method")
                output_path = os.path.join(".", filename)
                engine_flow.plot(filename=output_path)
                logger.info(f"Flow visualization saved to: {output_path}")
                return output_path
            else:
                logger.warning("CrewAI flow does not support plot() method")
                return None

        except Exception as e:
            logger.error(f"Error generating flow visualization: {e}", exc_info=True)
            return None
