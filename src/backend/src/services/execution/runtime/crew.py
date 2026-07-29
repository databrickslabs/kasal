"""Crew.

Authored module; surface validated against the kasal_engine datamodel.
Sequential process runs tasks in declared order (async_execution tasks run
in threads and are awaited when a later sync task needs them, or at the
end). Hierarchical process gives a manager agent delegation/question tools
over the crew's agents. Kickoff emits CrewKickoffStarted/Completed on the
engine bus, so ambient context (event_context) lands on every event.

Engine notes:
- memory/embedder/knowledge_sources/planning fields are accepted and carried
  (kasal configures them) but wiring is the memory subsystem's job.
- token_usage aggregates from any agent LLM exposing get_usage_metrics();
  the src.core.llm.transport subsystem provides that.
- stream/prompt_to_print_output/tracing are accepted for compatibility and
  inert here.
"""

import asyncio
import concurrent.futures
import hashlib
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from pydantic import UUID4, BaseModel, ConfigDict, Field, PrivateAttr

from src.core.events.bus import event_bus
from src.core.events.types import CrewKickoffCompletedEvent, CrewKickoffStartedEvent

from .agent import Agent, BaseAgent
from .executor import delegation_tools, interpolate_text
from .task import Task
from .types import CrewOutput, Process, TaskOutput, UsageMetrics

logger = logging.getLogger(__name__)

_MANAGER_GOAL = "Manage the team to complete the task in the best way possible."
_MANAGER_BACKSTORY = (
    "You're a long time manager of a team of agents. You excel at breaking "
    "work down, delegating to the right coworker, and reviewing their results "
    "until the task is complete."
)


class Crew(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    _inputs: dict[str, Any] | None = PrivateAttr(default=None)
    _seeded_outputs: dict[int, TaskOutput] | None = PrivateAttr(default=None)

    id: UUID4 = Field(default_factory=uuid.uuid4, frozen=True)
    name: str | None = "crew"
    agents: list[BaseAgent] = Field(default_factory=list)
    tasks: list[Task] = Field(default_factory=list)
    process: Process = Process.sequential
    verbose: bool = False
    cache: bool = False
    memory: Any = Field(
        default=False,
        description="Accepted and carried; wiring happens in the memory subsystem.",
    )
    embedder: dict[str, Any] | None = None
    knowledge_sources: list[Any] | None = None
    manager_llm: Any | None = None
    manager_agent: BaseAgent | None = None
    function_calling_llm: Any | None = Field(
        default=None, description="Deprecated; accepted for compatibility and unused."
    )
    config: dict[str, Any] | None = None
    step_callback: Callable[..., Any] | None = None
    task_callback: Callable[..., Any] | None = None
    max_rpm: int | None = None
    #: Wall-clock ceiling for the whole run. Without it the only clock is
    #: per-agent-call and starts afresh on every call — including every
    #: guardrail retry — so a six-task crew with three retries each had an
    #: effective ceiling of 24× the per-call cap and no way to state how long
    #: "deep research" can take. Set from the mode's budget profile.
    run_max_seconds: float | None = None
    planning: bool | None = False
    planning_llm: Any | None = None
    stream: bool = False
    tracing: bool | None = None
    prompt_to_print_output: bool = Field(
        default=False,
        description="kasal compatibility flag (suppressed interactive prompt); inert.",
    )
    context_providers: list[Any] = Field(
        default_factory=list,
        exclude=True,
        description=(
            "Callables ``(task, agent, context) -> str | None`` invoked when a "
            "task's context is assembled; non-empty returns are appended. The "
            "memory subsystem wires recall here. Provider errors are logged "
            "and never break the run."
        ),
    )
    output_sinks: list[Any] = Field(
        default_factory=list,
        exclude=True,
        description=(
            "Callables ``(task, output)`` invoked after every finished task "
            "(all processes, sync and async). The memory subsystem wires "
            "persistence here. Sink errors are logged and never break the run."
        ),
    )
    token_usage: UsageMetrics | None = None

    @property
    def key(self) -> str:
        source = "|".join(
            [self.process.value]
            + [agent.key for agent in self.agents]
            + [task.key for task in self.tasks]
        )
        return hashlib.md5(source.encode(), usedforsecurity=False).hexdigest()

    def kickoff(
        self,
        inputs: dict[str, Any] | None = None,
        input_files: dict[str, Any] | None = None,
        from_checkpoint: Any | None = None,
    ) -> CrewOutput:
        self._inputs = inputs
        if inputs:
            self._interpolate_inputs(inputs)
        # One deadline for the whole run, computed HERE rather than at agent
        # build time so the clock starts when work does.
        run_deadline = (
            time.monotonic() + float(self.run_max_seconds)
            if self.run_max_seconds
            else None
        )
        for agent in self.agents:
            agent.crew = self
            agent.run_deadline = run_deadline
        if self.manager_agent is not None:
            self.manager_agent.run_deadline = run_deadline

        # Checkpoint load happens AFTER input interpolation so restored task
        # keys (hashed from interpolated description|expected_output) only
        # match when the resume run uses the same inputs as the original.
        self._seeded_outputs = self._load_checkpoint(from_checkpoint)

        event_bus.emit(
            self, CrewKickoffStartedEvent(crew_name=self.name, inputs=inputs)
        )
        try:
            if self.process == Process.hierarchical:
                task_outputs = self._run_hierarchical()
            else:
                task_outputs = self._run_sequential()
        except Exception:
            logger.exception("crew %r kickoff failed", self.name)
            raise
        finally:
            self._seeded_outputs = None

        usage = self._aggregate_token_usage()
        self.token_usage = usage
        last = (
            task_outputs[-1]
            if task_outputs
            else TaskOutput(description="", raw="", agent="")
        )
        crew_output = CrewOutput(
            raw=last.raw,
            pydantic=last.pydantic,
            json_dict=last.json_dict,
            tasks_output=task_outputs,
            token_usage=usage,
        )
        event_bus.emit(
            self,
            CrewKickoffCompletedEvent(
                crew_name=self.name,
                output=crew_output,
                total_tokens=usage.total_tokens,
            ),
        )
        return crew_output

    async def kickoff_async(
        self,
        inputs: dict[str, Any] | None = None,
        input_files: dict[str, Any] | None = None,
        from_checkpoint: Any | None = None,
    ) -> CrewOutput:
        return await asyncio.to_thread(
            self.kickoff, inputs, input_files, from_checkpoint
        )

    def copy(self) -> "Crew":
        cloned_agents = [
            agent.model_copy(update={"id": uuid.uuid4()}) for agent in self.agents
        ]
        task_mapping: dict[str, Task] = {}
        cloned_tasks: list[Task] = []
        for task in self.tasks:
            cloned = task.copy(cloned_agents, task_mapping)
            task_mapping[task.key] = cloned
            cloned_tasks.append(cloned)
        # second pass: context lists referring to later tasks
        for original, cloned in zip(self.tasks, cloned_tasks):
            if isinstance(original.context, list):
                cloned.context = [task_mapping.get(t.key, t) for t in original.context]
        data = self.model_dump(
            exclude={"id", "agents", "tasks", "manager_agent", "token_usage"}
        )
        data.update(
            manager_llm=self.manager_llm,
            step_callback=self.step_callback,
            context_providers=list(self.context_providers),
            output_sinks=list(self.output_sinks),
            task_callback=self.task_callback,
            planning_llm=self.planning_llm,
            memory=self.memory,
            knowledge_sources=self.knowledge_sources,
            process=self.process,
        )
        return Crew(
            agents=cloned_agents,
            tasks=cloned_tasks,
            manager_agent=(
                self.manager_agent.model_copy(update={"id": uuid.uuid4()})
                if self.manager_agent
                else None
            ),
            **data,
        )

    # ------------------------- internals -------------------------

    def _interpolate_inputs(self, inputs: dict[str, Any]) -> None:
        for task in self.tasks:
            task.interpolate_inputs(inputs)
        for agent in self.agents:
            agent.role = interpolate_text(agent.role, inputs)
            agent.goal = interpolate_text(agent.goal, inputs)
            agent.backstory = interpolate_text(agent.backstory, inputs)

    def _context_for(
        self,
        task: Task,
        completed: list[tuple[Task, TaskOutput]],
        futures: dict[int, tuple[Task, concurrent.futures.Future]],
    ) -> str | None:
        if isinstance(task.context, list):
            outputs = []
            for context_task in task.context:
                if id(context_task) in futures:
                    _, future = futures.pop(id(context_task))
                    outputs.append(future.result())
                    completed.append((context_task, context_task.output))
                elif context_task.output is not None:
                    outputs.append(context_task.output)
            base = self._join([o.raw for o in outputs]) or None
        elif task.context is None:
            base = None
        else:
            # NOT_SPECIFIED: all prior outputs
            base = self._join([output.raw for _, output in completed]) or None
        return self._apply_context_providers(task, base)

    def _apply_context_providers(self, task: Task, base: str | None) -> str | None:
        """Append each provider's contribution to the task context (best-effort)."""
        if not self.context_providers:
            return base
        chunks: list[str] = [base] if base else []
        for provider in self.context_providers:
            try:
                extra = provider(task=task, agent=task.agent, context=base)
            except Exception:  # noqa: BLE001 — providers must never break a run
                logger.exception("context provider %r failed; skipping", provider)
                continue
            if extra:
                chunks.append(str(extra))
        return self._join(chunks) or None

    @staticmethod
    def _join(chunks: list[str]) -> str:
        return "\n\n----------\n\n".join(chunk for chunk in chunks if chunk)

    def _finish_task(self, task: Task, output: TaskOutput) -> None:
        if self.task_callback:
            try:
                self.task_callback(output)
            except Exception:
                logger.exception("task_callback failed for crew %r", self.name)
        for sink in self.output_sinks:
            try:
                sink(task=task, output=output)
            except Exception:  # noqa: BLE001 — sinks must never break a run
                logger.exception("output sink %r failed; skipping", sink)

    def _load_checkpoint(self, from_checkpoint: Any) -> dict[int, TaskOutput] | None:
        """Validate checkpoint data and rebuild TaskOutputs for completed tasks.

        Expected shape: ``{"completed": [{"index": int, "task_key": str,
        "output_raw": str, ...}, ...], "task_count": int}`` (``completed`` may
        also be a dict keyed by stringified index). Only the contiguous prefix
        of completed tasks is restored so context chaining ("all prior
        outputs") is byte-identical to an uninterrupted run. Any validation
        failure logs and returns None — the crew then runs from scratch.
        """
        if not from_checkpoint:
            return None
        if self.process != Process.sequential:
            logger.warning(
                "crew %r: checkpoint resume only supports the sequential "
                "process (got %s); running from scratch",
                self.name,
                self.process,
            )
            return None
        if not isinstance(from_checkpoint, dict):
            logger.warning(
                "crew %r: checkpoint is %s, expected dict; running from scratch",
                self.name,
                type(from_checkpoint).__name__,
            )
            return None

        completed = from_checkpoint.get("completed")
        if isinstance(completed, dict):
            entries = list(completed.values())
        elif isinstance(completed, list):
            entries = completed
        else:
            logger.warning(
                "crew %r: checkpoint has no completed entries; running from scratch",
                self.name,
            )
            return None

        task_count = from_checkpoint.get("task_count")
        if task_count is not None and int(task_count) != len(self.tasks):
            logger.warning(
                "crew %r: checkpoint task_count %s != current %d; running from scratch",
                self.name,
                task_count,
                len(self.tasks),
            )
            return None

        by_index: dict[int, dict[str, Any]] = {}
        for entry in entries:
            try:
                index = int(entry["index"])
            except (KeyError, TypeError, ValueError):
                logger.warning(
                    "crew %r: malformed checkpoint entry %r; running from scratch",
                    self.name,
                    entry,
                )
                return None
            if not 0 <= index < len(self.tasks):
                logger.warning(
                    "crew %r: checkpoint index %d out of range; running from scratch",
                    self.name,
                    index,
                )
                return None
            entry_key = entry.get("task_key")
            if entry_key and entry_key != self.tasks[index].key:
                logger.warning(
                    "crew %r: checkpoint task_key mismatch at index %d "
                    "(task list or inputs changed); running from scratch",
                    self.name,
                    index,
                )
                return None
            by_index[index] = entry

        seeded: dict[int, TaskOutput] = {}
        for index in range(len(self.tasks)):
            entry = by_index.get(index)
            if entry is None:
                break
            seeded[index] = self._restore_output(self.tasks[index], entry)
        if not seeded:
            return None
        logger.info(
            "crew %r: resuming from checkpoint — %d/%d task(s) restored",
            self.name,
            len(seeded),
            len(self.tasks),
        )
        return seeded

    @staticmethod
    def _restore_output(task: Task, entry: dict[str, Any]) -> TaskOutput:
        json_dict = entry.get("output_json")
        return TaskOutput(
            description=task.description,
            name=entry.get("name") or task.name,
            expected_output=task.expected_output,
            summary=entry.get("summary"),
            raw=entry.get("output_raw") or "",
            json_dict=json_dict if isinstance(json_dict, dict) else None,
            agent=entry.get("agent")
            or (task.agent.role if task.agent is not None else ""),
        )

    def _run_sequential(self) -> list[TaskOutput]:
        completed: list[tuple[Task, TaskOutput]] = []
        futures: dict[int, tuple[Task, concurrent.futures.Future]] = {}
        seeded = self._seeded_outputs or {}

        for index, task in enumerate(self.tasks):
            restored = seeded.get(index)
            if restored is not None:
                # Restored from checkpoint: no execution, no events, no
                # callbacks/sinks (they already ran in the original attempt).
                task.output = restored
                completed.append((task, restored))
                continue
            agent = task.agent
            if agent is None:
                raise ValueError(
                    f"Sequential process requires an agent on every task; "
                    f"task {task.description[:50]!r} has none."
                )
            if task.async_execution:
                context = self._context_for(task, completed, futures)
                futures[id(task)] = (
                    task,
                    task.execute_async(agent, context, task.tools),
                )
                continue
            # a sync task waits for every still-pending async task
            for task_key, (pending_task, future) in list(futures.items()):
                output = future.result()
                completed.append((pending_task, output))
                self._finish_task(pending_task, output)
                del futures[task_key]
            context = self._context_for(task, completed, futures)
            output = task.execute_sync(agent, context, task.tools)
            completed.append((task, output))
            self._finish_task(task, output)

        for pending_task, future in futures.values():
            output = future.result()
            completed.append((pending_task, output))
            self._finish_task(pending_task, output)

        ordered = {id(t): o for t, o in completed}
        return [ordered[id(t)] for t in self.tasks if id(t) in ordered]

    def _manager(self) -> BaseAgent:
        if self.manager_agent is not None:
            if self.manager_agent.tools:
                raise ValueError("Manager agent should not have tools")
            return self.manager_agent
        if self.manager_llm is None:
            raise ValueError(
                "Hierarchical process requires manager_llm or manager_agent."
            )
        manager = Agent(
            role="Crew Manager",
            goal=_MANAGER_GOAL,
            backstory=_MANAGER_BACKSTORY,
            llm=self.manager_llm,
            verbose=self.verbose,
        )
        self.manager_agent = manager
        return manager

    def _run_hierarchical(self) -> list[TaskOutput]:
        manager = self._manager()
        tools = delegation_tools(list(self.agents))
        completed: list[tuple[Task, TaskOutput]] = []
        for task in self.tasks:
            context = self._context_for(task, completed, {})
            output = task.execute_sync(manager, context, tools)
            completed.append((task, output))
            self._finish_task(task, output)
        return [output for _, output in completed]

    def _aggregate_token_usage(self) -> UsageMetrics:
        total = UsageMetrics()
        seen: set[int] = set()
        candidates = [agent.llm for agent in self.agents if getattr(agent, "llm", None)]
        if self.manager_agent is not None and self.manager_agent is not None:
            candidates.append(getattr(self.manager_agent, "llm", None))
        for llm in candidates:
            if llm is None or id(llm) in seen:
                continue
            seen.add(id(llm))
            getter = getattr(llm, "get_usage_metrics", None)
            if getter is None:
                continue
            try:
                total.add_usage_metrics(UsageMetrics.model_validate(getter()))
            except Exception:
                logger.debug("could not read usage metrics from %r", llm)
        return total
