"""Task.

Authored module; surface validated against the kasal_engine datamodel.
Task owns output shaping (structured output, guardrails, output_file,
callback); the agent produces the raw text. Members excluded from the
datamodel surface (include=false) — human input review, converters,
conversation history — are not implemented.
"""

import concurrent.futures
import datetime
import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from src.core.events.bus import event_bus
from src.core.events.types import (
    LLMGuardrailCompletedEvent,
    LLMGuardrailFailedEvent,
    LLMGuardrailStartedEvent,
    TaskCompletedEvent,
    TaskFailedEvent,
    TaskStartedEvent,
)
from src.services.tools.base import BaseTool

from .agent import BaseAgent
from .plan import reset_plan
from .executor import (
    interpolate_text,
    json_schema_instruction,
    reset_tool_ledger,
    structured_from_raw,
    tool_failure_summary,
    wholly_failed_tools,
)
from .types import OutputFormat, TaskOutput

logger = logging.getLogger(__name__)

_TASK_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="kasal-engine-task"
)


class _NotSpecified:
    def __repr__(self) -> str:
        return "NOT_SPECIFIED"


NOT_SPECIFIED = _NotSpecified()


class Task(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    logger: ClassVar[logging.Logger] = logging.getLogger(__name__)

    id: uuid.UUID = Field(default_factory=uuid.uuid4, frozen=True)
    name: str | None = None
    description: str = Field(description="Description of the actual task.")
    expected_output: str = Field(
        description="Clear definition of expected output for the task."
    )
    config: dict[str, Any] | None = None
    callback: Callable[..., Any] | None = None
    agent: BaseAgent | None = Field(
        default=None, description="Agent responsible for executing the task."
    )
    context: Any = Field(
        default=NOT_SPECIFIED,
        description="Other tasks whose output is used as context for this task.",
    )
    async_execution: bool | None = False
    output_json: type[BaseModel] | None = None
    output_pydantic: type[BaseModel] | None = None
    response_model: type[BaseModel] | None = None
    output_file: str | None = None
    output: TaskOutput | None = None
    tools: list[BaseTool] | None = Field(
        default_factory=list,
        description="Tools the agent is limited to use for this task.",
    )
    human_input: bool | None = Field(
        default=False,
        description="Accepted for compatibility; interactive review is not part of the engine.",
    )
    markdown: bool | None = False
    converter_cls: Any | None = None
    guardrail: Any | None = None
    guardrails: Any | None = None
    max_retries: int | None = Field(
        default=None, description="Deprecated alias for guardrail_max_retries."
    )
    guardrail_max_retries: int = 3
    #: What to do when a guardrail still rejects after the last retry.
    #: ``raise`` (default, unchanged behaviour) kills the run; ``degrade``
    #: accepts the best attempt with the reviewer's objection appended, so an
    #: expensive multi-task run is not lost to one task failing a judge.
    guardrail_on_exhausted: str = "raise"
    #: Same choice for a blown execution budget (tool rounds / wall clock).
    on_budget_exceeded: str = "raise"
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None

    @property
    def key(self) -> str:
        source = f"{self.description}|{self.expected_output}"
        return hashlib.md5(source.encode(), usedforsecurity=False).hexdigest()

    def prompt(self) -> str:
        parts = [self.description]
        parts.append(
            f"This is the expected criteria for your final answer: {self.expected_output}\n"
            "you MUST return the actual complete content as the final answer, not a summary."
        )
        if self.markdown:
            parts.append("Your final answer MUST be formatted in Markdown syntax.")
        return "\n\n".join(parts)

    def interpolate_inputs(self, inputs: dict[str, Any]) -> None:
        self.description = interpolate_text(self.description, inputs)
        self.expected_output = interpolate_text(self.expected_output, inputs)
        if self.output_file:
            self.output_file = interpolate_text(self.output_file, inputs)

    def execute_sync(
        self,
        agent: Any | None = None,
        context: str | None = None,
        tools: Sequence[Any] | None = None,
    ) -> TaskOutput:
        executing_agent = agent or self.agent
        if executing_agent is None:
            raise ValueError(
                f"Task {self.description[:50]!r} has no agent assigned and none was provided."
            )
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        # Per-task, not per-run: the guardrail and the degradation notice below
        # both ask "did THIS task's tools work?", and a previous task's 503s
        # would otherwise be held against an answer they had nothing to do with.
        reset_tool_ledger()
        # Same scope, same reason. The plan describes HOW THIS TASK gets done,
        # so it starts empty; carrying the previous task's items forward would
        # have the agent reporting work it never did here.
        reset_plan()
        event_bus.emit(self, TaskStartedEvent(context=context, task=self))
        try:
            return self._execute_core(executing_agent, context, tools)
        except Exception as e:
            event_bus.emit(self, TaskFailedEvent(error=str(e), task=self))
            raise

    def _execute_core(
        self,
        executing_agent: Any,
        context: str | None,
        tools: Sequence[Any] | None,
    ) -> TaskOutput:
        structured_model = (
            self.response_model or self.output_pydantic or self.output_json
        )
        if structured_model is not None:
            original_expected = self.expected_output
            self.expected_output = original_expected + json_schema_instruction(
                structured_model
            )
            try:
                raw = self._run_agent(executing_agent, context, tools)
            finally:
                self.expected_output = original_expected
        else:
            raw = self._run_agent(executing_agent, context, tools)

        raw, pydantic_output, json_output, output_format = self._shape_output(
            raw, structured_model
        )
        output = TaskOutput(
            description=self.description,
            name=self.name,
            expected_output=self.expected_output,
            summary=self._summary(),
            raw=raw,
            pydantic=pydantic_output,
            json_dict=json_output,
            agent=executing_agent.role,
            output_format=output_format,
        )

        output = self._apply_guardrails(
            output, executing_agent, context, tools, structured_model
        )
        output = self._flag_unavailable_sources(output)
        self.output = output
        self.end_time = datetime.datetime.now(datetime.timezone.utc)

        if self.output_file:
            path = Path(self.output_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(output.json_dict, indent=2)
                if output.json_dict is not None and self.output_json
                else output.raw
            )
        if self.callback:
            self.callback(output)
        event_bus.emit(self, TaskCompletedEvent(output=output, task=self))
        return output

    def execute_async(
        self,
        agent: Any | None = None,
        context: str | None = None,
        tools: Sequence[Any] | None = None,
    ) -> concurrent.futures.Future:
        return _TASK_EXECUTOR.submit(self.execute_sync, agent, context, tools)

    def copy(self, agents: Sequence[Any], task_mapping: dict[str, "Task"]) -> "Task":
        """Clone for a copied crew: fresh id/output, agent and context remapped."""
        data = self.model_dump(
            exclude={"id", "output", "agent", "context", "start_time", "end_time"},
            exclude_unset=False,
        )
        data.update(
            callback=self.callback,
            tools=list(self.tools or []),
            output_json=self.output_json,
            output_pydantic=self.output_pydantic,
            response_model=self.response_model,
            converter_cls=self.converter_cls,
            guardrail=self.guardrail,
            guardrails=self.guardrails,
        )
        cloned = Task(**data)
        if self.agent is not None:
            cloned.agent = next(
                (a for a in agents if a.role == self.agent.role), self.agent
            )
        if isinstance(self.context, list):
            cloned.context = [task_mapping.get(t.key, t) for t in self.context]
        return cloned

    def _summary(self) -> str:
        return f"{' '.join(self.description.split(' ')[:10])}..."

    def _run_agent(
        self,
        executing_agent: Any,
        context: str | None,
        tools: Sequence[Any] | None,
    ) -> str:
        """One agent turn, with the execution budget optionally soft.

        A blown budget (tool rounds or wall clock) raises out of the LLM
        transport and — caught nowhere — destroys the run along with every task
        that already succeeded. For long research runs that is the wrong trade:
        with ``on_budget_exceeded='degrade'`` the partial answer is kept and
        annotated so the failure is visible downstream rather than fatal.
        """
        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError

        try:
            return executing_agent.execute_task(
                self, context, list(tools) if tools else None
            )
        except ExecutionBudgetExceededError as exc:
            if self.on_budget_exceeded != "degrade":
                raise
            logger.warning(
                "task %r exceeded its execution budget (%s); degrading",
                self.name or self.description[:40],
                exc,
            )
            partial = getattr(exc, "partial", "") or ""
            return (
                f"{partial}\n\n> ⚠️ Truncated: exceeded the execution budget "
                f"({exc}). This answer is incomplete."
            ).strip()

    def _shape_output(
        self, raw: str, structured_model: type[BaseModel] | None
    ) -> tuple[str, BaseModel | None, dict[str, Any] | None, OutputFormat]:
        if structured_model is None:
            return raw, None, None, OutputFormat.RAW
        instance = structured_from_raw(structured_model, raw)
        if instance is None:
            logger.warning(
                "task %r: could not parse structured output, keeping raw",
                self.name or self.description[:40],
            )
            return raw, None, None, OutputFormat.RAW
        if self.output_json and not (self.output_pydantic or self.response_model):
            return (
                instance.model_dump_json(),
                None,
                instance.model_dump(),
                OutputFormat.JSON,
            )
        return raw, instance, instance.model_dump(), OutputFormat.PYDANTIC

    def _normalized_guardrails(self, agent: Any) -> list[Callable[..., Any]]:
        from .guardrail import LLMGuardrail

        raw_guardrails: list[Any] = []
        if self.guardrails is not None:
            raw_guardrails = (
                list(self.guardrails)
                if isinstance(self.guardrails, (list, tuple))
                else [self.guardrails]
            )
        elif self.guardrail is not None:
            raw_guardrails = [self.guardrail]

        normalized: list[Callable[..., Any]] = []
        for entry in raw_guardrails:
            if isinstance(entry, str):
                normalized.append(LLMGuardrail(description=entry, llm=agent.llm))
            elif callable(entry):
                normalized.append(entry)
            else:
                raise TypeError(f"Unsupported guardrail: {entry!r}")
        return normalized

    @staticmethod
    def _guardrail_label(guardrail: Any) -> str:
        """Human-readable guardrail name for events/trace rows."""
        description = getattr(guardrail, "description", None)
        if description:
            return str(description)[:200]
        owner = getattr(guardrail, "__self__", None)  # bound wrapper methods
        if owner is not None:
            inner = getattr(owner, "guardrail", None)
            return type(inner).__name__ if inner is not None else type(owner).__name__
        name = getattr(guardrail, "__qualname__", None)
        return str(name) if name else type(guardrail).__name__

    def _flag_unavailable_sources(self, output: TaskOutput) -> TaskOutput:
        """Mark an output whose tools never once worked.

        The guardrail path already reports this, but only a task that HAS a
        guardrail goes through it — and most do not. Without this, a task whose
        every source call returned 503 produces a confident answer built on
        nothing and is reported as a plain success. That is the failure this
        whole seam exists to stop, and it is the common case rather than the
        exceptional one.

        FLAGGED, not raised. The engine cannot know that a dead tool was
        essential: an agent with a search tool and a database tool may have been
        asked something the database alone answers. Raising would fail runs that
        legitimately succeeded, which is how a guard gets switched off. Marking
        the output degraded gives every caller — the next task, the UI, recipe
        mining — the fact, and lets each decide.
        """
        if output.degraded:
            return output  # the guardrail path already said so, with more detail
        dead = wholly_failed_tools()
        if not dead:
            return output
        cause = (
            f"every call to {', '.join(dead)} failed, so this answer was produced "
            "without the information those tools were meant to supply"
        )
        logger.warning(
            "task %r completed with wholly unavailable tool(s): %s",
            self.name or self.description[:40],
            ", ".join(dead),
        )
        return output.model_copy(
            update={
                "raw": f"{output.raw}\n\n> ⚠️ Unverified: {cause}",
                "degraded": True,
                "degradation_reason": cause,
            }
        )

    def _apply_guardrails(
        self,
        output: TaskOutput,
        agent: Any,
        context: str | None,
        tools: Sequence[Any] | None,
        structured_model: type[BaseModel] | None = None,
    ) -> TaskOutput:
        guardrails = self._normalized_guardrails(agent)
        if not guardrails:
            return output

        retries = self.guardrail_max_retries
        if self.max_retries is not None:
            retries = self.max_retries

        for guardrail in guardrails:
            label = self._guardrail_label(guardrail)
            for attempt in range(retries + 1):
                event_bus.emit(
                    self,
                    LLMGuardrailStartedEvent(
                        guardrail=label,
                        retry_count=attempt,
                        task=self,
                        task_id=str(self.id),
                        task_name=self.name,
                    ),
                )
                ok, result = guardrail(output)
                event_bus.emit(
                    self,
                    LLMGuardrailCompletedEvent(
                        guardrail=label,
                        success=bool(ok),
                        result=str(result)[:500] if result is not None else None,
                        error=None if ok else str(result)[:500],
                        retry_count=attempt,
                        task=self,
                        task_id=str(self.id),
                        task_name=self.name,
                    ),
                )
                if ok:
                    if isinstance(result, TaskOutput):
                        output = result
                    elif isinstance(result, str) and result:
                        output = output.model_copy(update={"raw": result})
                    break
                if attempt == retries:
                    event_bus.emit(
                        self,
                        LLMGuardrailFailedEvent(
                            guardrail=label,
                            error=str(result)[:500],
                            retry_count=attempt,
                            task=self,
                            task_id=str(self.id),
                            task_name=self.name,
                        ),
                    )
                    if self.guardrail_on_exhausted == "degrade":
                        # Keep the best attempt, flagged. Losing a six-task
                        # research run because task four could not satisfy a
                        # judge on the third try is the wrong trade; the next
                        # task needs to know the input is soft, not to never
                        # receive it.
                        #
                        # But say WHY it is soft. A run whose every source call
                        # returned 503 was annotated "Unverified: does not
                        # define named agents" — the judge's guess, not the
                        # cause — and a reader had no way to tell a badly
                        # written answer from one that never had any data.
                        dead = wholly_failed_tools()
                        cause = (
                            f"every call to {', '.join(dead)} failed, so the "
                            f"information this task needed was never available"
                            if dead
                            else str(result)
                        )
                        logger.warning(
                            "task %r failed guardrail %r after %d retries; "
                            "degrading (%s)",
                            self.name or self.description[:40],
                            label,
                            retries,
                            (
                                f"tools wholly failed: {', '.join(dead)}"
                                if dead
                                else "output rejected"
                            ),
                        )
                        return output.model_copy(
                            update={
                                "raw": f"{output.raw}\n\n> ⚠️ Unverified: {cause}",
                                "degraded": True,
                                "degradation_reason": cause,
                            }
                        )
                    raise ValueError(
                        f"Task guardrail failed after {retries} retries: {result}"
                    )
                feedback = (
                    f"{context}\n\nYour previous answer was rejected by a reviewer "
                    f"with this feedback:\n{result}\nPrevious answer:\n{output.raw}"
                    if context
                    else f"Your previous answer was rejected by a reviewer with this "
                    f"feedback:\n{result}\nPrevious answer:\n{output.raw}"
                )
                raw = self._run_agent(agent, feedback, tools)
                # Re-shape, do not merely patch ``raw``. The first pass shaped
                # the REJECTED answer, so patching raw alone leaves .pydantic /
                # .json_dict holding it — and with output_json set, raw itself
                # reverts from the JSON dump to the agent's unshaped prose. The
                # retry that was supposed to fix the output would otherwise be
                # what breaks the downstream JSON contract.
                raw, pydantic_output, json_output, output_format = self._shape_output(
                    raw, structured_model
                )
                output = output.model_copy(
                    update={
                        "raw": raw,
                        "pydantic": pydantic_output,
                        "json_dict": json_output,
                        "output_format": output_format,
                    }
                )
        return output
