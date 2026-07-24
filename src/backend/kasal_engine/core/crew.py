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
  the kasal_engine.llm subsystem provides that.
- stream/prompt_to_print_output/tracing are accepted for compatibility and
  inert here.
"""

import asyncio
import concurrent.futures
import hashlib
import logging
import uuid
from collections.abc import Callable
from typing import Any

from pydantic import UUID4, BaseModel, ConfigDict, Field, PrivateAttr

from ..events.bus import crewai_event_bus
from ..events.types import CrewKickoffCompletedEvent, CrewKickoffStartedEvent
from .agent import Agent, BaseAgent
from .executor import delegation_tools, interpolate_text
from .task import NOT_SPECIFIED, Task
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
    planning: bool | None = False
    planning_llm: Any | None = None
    stream: bool = False
    tracing: bool | None = None
    prompt_to_print_output: bool = Field(
        default=False,
        description="kasal compatibility flag (suppressed interactive prompt); inert.",
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
        for agent in self.agents:
            agent.crew = self

        crewai_event_bus.emit(
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

        usage = self._aggregate_token_usage()
        self.token_usage = usage
        last = task_outputs[-1] if task_outputs else TaskOutput(
            description="", raw="", agent=""
        )
        crew_output = CrewOutput(
            raw=last.raw,
            pydantic=last.pydantic,
            json_dict=last.json_dict,
            tasks_output=task_outputs,
            token_usage=usage,
        )
        crewai_event_bus.emit(
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
        return await asyncio.to_thread(self.kickoff, inputs, input_files, from_checkpoint)

    def copy(self) -> "Crew":
        cloned_agents = [agent.model_copy(update={"id": uuid.uuid4()}) for agent in self.agents]
        task_mapping: dict[str, Task] = {}
        cloned_tasks: list[Task] = []
        for task in self.tasks:
            cloned = task.copy(cloned_agents, task_mapping)
            task_mapping[task.key] = cloned
            cloned_tasks.append(cloned)
        # second pass: context lists referring to later tasks
        for original, cloned in zip(self.tasks, cloned_tasks):
            if isinstance(original.context, list):
                cloned.context = [
                    task_mapping.get(t.key, t) for t in original.context
                ]
        data = self.model_dump(
            exclude={"id", "agents", "tasks", "manager_agent", "token_usage"}
        )
        data.update(
            manager_llm=self.manager_llm,
            step_callback=self.step_callback,
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
            return self._join([o.raw for o in outputs]) or None
        if task.context is None:
            return None
        # NOT_SPECIFIED: all prior outputs
        return self._join([output.raw for _, output in completed]) or None

    @staticmethod
    def _join(chunks: list[str]) -> str:
        return "\n\n----------\n\n".join(chunk for chunk in chunks if chunk)

    def _finish_task(self, task: Task, output: TaskOutput) -> None:
        if self.task_callback:
            try:
                self.task_callback(output)
            except Exception:
                logger.exception("task_callback failed for crew %r", self.name)

    def _run_sequential(self) -> list[TaskOutput]:
        completed: list[tuple[Task, TaskOutput]] = []
        futures: dict[int, tuple[Task, concurrent.futures.Future]] = {}

        for task in self.tasks:
            agent = task.agent
            if agent is None:
                raise ValueError(
                    f"Sequential process requires an agent on every task; "
                    f"task {task.description[:50]!r} has none."
                )
            if task.async_execution:
                context = self._context_for(task, completed, futures)
                futures[id(task)] = (task, task.execute_async(agent, context, task.tools))
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
