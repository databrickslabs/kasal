"""Internal execution machinery for the orchestration core.

Not a datamodel component: helpers shared by Agent/Task/Crew. The LLM
contract is duck-typed — anything with
``call(messages, tools=..., available_functions=..., from_task=..., from_agent=...) -> str``
works (the src.core.llm.transport subsystem provides the real one; tests use fakes).
Tool usage is emitted on the engine event bus, so kasal's tracing sees
ToolUsageStarted/Finished/Error exactly as with crewAI.
"""

import contextvars
import inspect
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.core.events.bus import event_bus
from src.core.events.types import (
    ToolUsageErrorEvent,
    ToolUsageFinishedEvent,
    ToolUsageStartedEvent,
)
from src.core.llm.json_extraction import extract_json_dict
from src.core.llm.transport.exceptions import (
    ExecutionBudgetExceededError,
    ToolExecutionBlockedError,
)
from src.services.tools.base import BaseTool, sanitize_tool_name

logger = logging.getLogger(__name__)

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


# --------------------------------------------------------------------------
# Tool outcome ledger
#
# What a task's tools actually DID, so something other than the model can see
# it. Nothing could, before: a tool that RETURNS an error string — which most
# first-party tools and every MCP server do — emits ToolUsageFinishedEvent and
# is indistinguishable from success at every layer above. A run whose every
# source call returned 404/503 therefore completed, and the guardrail, seeing
# only the output text, rejected it three times for "not defining named agents"
# while the actual blocker was that the source was down.
#
# Scoped with a ContextVar rather than a module global: a crew runs its tasks
# in a worker thread, and asyncio tasks branch their own copy, so concurrent
# runs cannot bleed into each other's counts.
# --------------------------------------------------------------------------


@dataclass
class ToolOutcome:
    """Per-tool tally for one task."""

    calls: int = 0
    failures: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def wholly_failed(self) -> bool:
        """Every call to this tool failed — the capability was unavailable."""
        return self.calls > 0 and self.failures == self.calls


#: Markers that identify a tool result which REPORTS a failure instead of
#: raising one. Deliberately narrow, and matched only at the start of the
#: result: these are the shapes Kasal's own tools produce ("Error: …",
#: "Error executing …") and the shape an MCP server's isError result arrives as
#: ("Tool error: …"). A broad match would classify a successful search for the
#: word "error" as a failure, which is worse than missing one.
_FAILURE_PREFIXES = ("tool error:", "error:", "error executing", "error from")

_tool_ledger: contextvars.ContextVar[dict[str, ToolOutcome] | None] = (
    contextvars.ContextVar("kasal_tool_ledger", default=None)
)


def reset_tool_ledger() -> None:
    """Start a fresh tally. Called at the top of each task."""
    _tool_ledger.set({})


def tool_ledger() -> dict[str, ToolOutcome]:
    """The current task's tally (empty when nothing has run)."""
    return dict(_tool_ledger.get() or {})


def _record_tool_outcome(name: str, failed: bool, error: str | None = None) -> None:
    ledger = _tool_ledger.get()
    if ledger is None:
        return  # outside a task scope — nothing is watching
    outcome = ledger.setdefault(name, ToolOutcome())
    outcome.calls += 1
    if failed:
        outcome.failures += 1
        if error and len(outcome.errors) < 3:
            outcome.errors.append(error[:200])


def looks_like_failure(output: Any) -> bool:
    """Whether a tool RETURNED a failure rather than raising one."""
    if not isinstance(output, str):
        return False
    head = output.lstrip()[:60].lower()
    return any(head.startswith(marker) for marker in _FAILURE_PREFIXES)


def tool_failure_summary() -> str:
    """One line per tool that failed at all, or "" when everything worked.

    Written for a reader who is deciding whether an answer can be trusted —
    the guardrail, and the run's own report.
    """
    lines = []
    for name, outcome in sorted(tool_ledger().items()):
        if not outcome.failures:
            continue
        verdict = "ALL FAILED" if outcome.wholly_failed else "partly failed"
        detail = f" — {outcome.errors[0]}" if outcome.errors else ""
        lines.append(
            f"- {name}: {outcome.failures}/{outcome.calls} calls failed "
            f"({verdict}){detail}"
        )
    return "\n".join(lines)


def wholly_failed_tools() -> list[str]:
    """Tools that were called and never once succeeded."""
    return sorted(n for n, o in tool_ledger().items() if o.wholly_failed)


def interpolate_text(text: str | None, inputs: dict[str, Any]) -> str | None:
    """Replace {key} placeholders present in inputs; leave others untouched."""
    if text is None or not inputs:
        return text

    def replace(match: re.Match) -> str:
        key = match.group(1)
        return str(inputs[key]) if key in inputs else match.group(0)

    return _PLACEHOLDER_RE.sub(replace, text)


def tool_schema(tool: BaseTool) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": sanitize_tool_name(tool.name),
            "description": tool.description,
            "parameters": tool.args_schema.model_json_schema(),
        },
    }


# Tool lifecycle hooks — the enforcement seam beneath guardrails. Every tool
# call in every path flows through wrap_tool, so a hook registered here can
# audit, rewrite arguments, cache, or BLOCK any call (raise
# ToolExecutionBlockedError, or block synchronously while awaiting a human
# decision — tool calls already run on LLM worker threads in all paths).
# Global by design (one execution per subprocess; in-process hooks scope
# themselves by the agent/tool identity they captured, exactly like bus
# handlers do). Hook failures are isolated: a broken *observer* never breaks
# the call, but ToolExecutionBlockedError always propagates.
_TOOL_PRE_HOOKS: list[Callable[..., Any]] = []
_TOOL_POST_HOOKS: list[Callable[..., Any]] = []


def register_tool_hooks(
    pre: Callable[..., Any] | None = None,
    post: Callable[..., Any] | None = None,
) -> None:
    """Register tool hooks.

    pre(tool, kwargs, agent, task) -> dict | None — return a dict to REPLACE
    the tool kwargs; raise ToolExecutionBlockedError to block the call.
    post(tool, kwargs, result, agent, task) -> Any | None — return non-None
    to replace the tool result.
    """
    if pre is not None and pre not in _TOOL_PRE_HOOKS:
        _TOOL_PRE_HOOKS.append(pre)
    if post is not None and post not in _TOOL_POST_HOOKS:
        _TOOL_POST_HOOKS.append(post)


def unregister_tool_hooks(
    pre: Callable[..., Any] | None = None,
    post: Callable[..., Any] | None = None,
) -> None:
    if pre is not None and pre in _TOOL_PRE_HOOKS:
        _TOOL_PRE_HOOKS.remove(pre)
    if post is not None and post in _TOOL_POST_HOOKS:
        _TOOL_POST_HOOKS.remove(post)


def wrap_tool(
    tool: BaseTool, agent: Any = None, task: Any = None
) -> Callable[..., Any]:
    """Wrap tool.run with engine tool-usage events (native context applies)."""

    def run(**kwargs: Any) -> Any:
        common = {
            "tool_name": tool.name,
            "tool_args": kwargs,
            "tool_class": type(tool).__name__,
            "agent_role": getattr(agent, "role", None),
            "agent": agent,
            "task_name": getattr(task, "name", None),
            "task_id": str(task.id) if getattr(task, "id", None) else None,
        }
        started_at = datetime.now(timezone.utc)
        event_bus.emit(tool, ToolUsageStartedEvent(**common))
        try:
            for pre_hook in list(_TOOL_PRE_HOOKS):
                try:
                    replacement = pre_hook(tool, kwargs, agent, task)
                except ToolExecutionBlockedError:
                    raise
                except Exception:
                    logger.exception("tool pre-hook %r failed (ignored)", pre_hook)
                else:
                    if isinstance(replacement, dict):
                        kwargs = replacement
                        common["tool_args"] = kwargs
            output = tool.run(**kwargs)
            for post_hook in list(_TOOL_POST_HOOKS):
                try:
                    replaced = post_hook(tool, kwargs, output, agent, task)
                except Exception:
                    logger.exception("tool post-hook %r failed (ignored)", post_hook)
                else:
                    if replaced is not None:
                        output = replaced
        except Exception as e:
            _record_tool_outcome(tool.name, failed=True, error=str(e))
            event_bus.emit(tool, ToolUsageErrorEvent(error=str(e), **common))
            raise
        # A tool that RETURNS "Error: ..." has failed just as surely as one that
        # raised, and it still emits ToolUsageFinishedEvent — which is why no
        # layer above could tell. Record the real outcome even though the event
        # stream, for compatibility, keeps saying "finished".
        returned_failure = looks_like_failure(output)
        _record_tool_outcome(
            tool.name,
            failed=returned_failure,
            error=str(output)[:200] if returned_failure else None,
        )
        event_bus.emit(
            tool,
            ToolUsageFinishedEvent(
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                output=output,
                **common,
            ),
        )
        return output

    run.__name__ = sanitize_tool_name(tool.name)
    # Carried on the wrapper because the transport only ever sees these
    # callables, never the BaseTool instances. `result_as_answer` has been a
    # field on BaseTool and seeded per tool since the engine was vendored, and
    # was plumbed all the way to the instance without anything reading it; the
    # tool-call loop reads it here (see transport/tool_rounds).
    run.result_as_answer = bool(getattr(tool, "result_as_answer", False))
    return run


def build_tool_context(
    tools: list[BaseTool] | None, agent: Any = None, task: Any = None
) -> tuple[list[dict[str, Any]] | None, dict[str, Callable[..., Any]] | None]:
    if not tools:
        return None, None
    schemas = [tool_schema(t) for t in tools]
    functions = {sanitize_tool_name(t.name): wrap_tool(t, agent, task) for t in tools}
    return schemas, functions


def call_llm(
    llm: Any,
    messages: list[dict[str, str]],
    *,
    tools: list[dict[str, Any]] | None = None,
    available_functions: dict[str, Callable[..., Any]] | None = None,
    from_task: Any = None,
    from_agent: Any = None,
) -> str:
    """Invoke an LLM's call(), passing only the keywords it accepts."""
    if llm is None:
        raise ValueError("No LLM configured. Set agent.llm before executing.")
    if isinstance(llm, str):
        raise ValueError(
            f"Agent LLM is the string {llm!r}; the engine expects a configured "
            "LLM object (kasal builds these via its LLM manager)."
        )
    call = llm.call
    optional = {
        "tools": tools,
        "available_functions": available_functions,
        "from_task": from_task,
        "from_agent": from_agent,
    }
    try:
        accepted = set(inspect.signature(call).parameters)
    except (TypeError, ValueError):
        accepted = set(optional)
    kwargs = {k: v for k, v in optional.items() if k in accepted}
    result = call(messages, **kwargs)
    return result if isinstance(result, str) else str(result)


def structured_from_raw(model: type[BaseModel], raw: str) -> BaseModel | None:
    parsed = extract_json_dict(raw)
    if parsed is None:
        return None
    try:
        return model.model_validate(parsed)
    except Exception:
        logger.warning("structured output failed validation for %s", model.__name__)
        return None


def json_schema_instruction(model: type[BaseModel]) -> str:
    schema = json.dumps(model.model_json_schema(), indent=2)
    return (
        "\n\nReturn ONLY a valid JSON object matching this schema "
        "(no prose, no markdown fences):\n" + schema
    )


def default_system_prompt(agent: Any) -> str:
    return (
        f"You are {agent.role}. {agent.backstory}\n"
        f"Your personal goal is: {agent.goal}"
    )


def build_messages(agent: Any, user_prompt: str) -> list[dict[str, str]]:
    system = (
        agent.system_template.replace("{role}", agent.role)
        .replace("{goal}", agent.goal)
        .replace("{backstory}", agent.backstory)
        if agent.system_template
        else default_system_prompt(agent)
    )
    if getattr(agent, "inject_date", False):
        date_format = getattr(agent, "date_format", "%Y-%m-%d") or "%Y-%m-%d"
        system += f"\nCurrent date: {datetime.now(timezone.utc).strftime(date_format)}"
    if agent.prompt_template:
        user_prompt = agent.prompt_template.replace("{input}", user_prompt)
    if agent.use_system_prompt is False:
        return [{"role": "user", "content": f"{system}\n\n{user_prompt}"}]
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user_prompt},
    ]


def run_agent(
    agent: Any,
    user_prompt: str,
    tools: list[BaseTool] | None,
    *,
    task: Any = None,
    messages: list[dict[str, str]] | None = None,
) -> str:
    """One agent turn: prompt → LLM (which drives tool execution) → text."""
    if messages is None:
        messages = build_messages(agent, user_prompt)
    schemas, functions = build_tool_context(tools, agent, task)

    last_error: Exception | None = None
    for _attempt in range(max(1, agent.max_retry_limit + 1)):
        try:
            raw = call_llm(
                agent.llm,
                messages,
                tools=schemas,
                available_functions=functions,
                from_task=task,
                from_agent=agent,
            )
            break
        except ExecutionBudgetExceededError:
            # NOT transient, so not retryable. `messages` is built once above
            # and never touched in the loop, and the deadline is recomputed
            # fresh inside every `call_llm` — so a retry sends the identical
            # prompt, the model makes the identical (slow) tool calls, and the
            # identical budget blows again. Observed: three full rounds of
            # deep-research searches spent to reach the outcome the first
            # attempt already had.
            #
            # Hand it straight to Task._run_agent, which keeps the partial when
            # on_budget_exceeded is 'degrade'. Modes that raise would have
            # raised anyway, just three attempts later.
            raise
        except Exception as e:
            last_error = e
            logger.warning("agent %r LLM call failed: %s", agent.role, e)
    else:
        raise last_error  # type: ignore[misc]

    if agent.step_callback:
        try:
            agent.step_callback({"agent": agent.role, "output": raw})
        except Exception:
            logger.exception("step_callback failed for agent %r", agent.role)
    return raw


# --------------------------- delegation tools ---------------------------


def _find_coworker(agents: list[Any], coworker: str) -> Any:
    wanted = coworker.strip().casefold()
    for candidate in agents:
        if candidate.role.strip().casefold() == wanted:
            return candidate
    for candidate in agents:
        if wanted in candidate.role.casefold() or candidate.role.casefold() in wanted:
            return candidate
    raise ValueError(
        f"Coworker {coworker!r} not found. Available: "
        + ", ".join(a.role for a in agents)
    )


class DelegateWorkToolSchema(BaseModel):
    task: str = Field(description="The task to delegate")
    context: str = Field(description="The context for the task")
    coworker: str = Field(description="The role/name of the coworker to delegate to")


class DelegateWorkTool(BaseTool):
    name: str = "Delegate work to coworker"
    description: str = "Delegate a specific task to one of your coworkers."
    args_schema: type[BaseModel] = DelegateWorkToolSchema
    agents: list[Any] = Field(default_factory=list, exclude=True)

    def _run(self, task: str, context: str, coworker: str) -> str:
        from .task import Task

        agent = _find_coworker(self.agents, coworker)
        delegated = Task(
            description=task,
            expected_output="Your best complete final answer to the task.",
            agent=agent,
        )
        return delegated.execute_sync(agent=agent, context=context).raw


class AskQuestionToolSchema(BaseModel):
    question: str = Field(description="The question to ask")
    context: str = Field(description="The context for the question")
    coworker: str = Field(description="The role/name of the coworker to ask")


class AskQuestionTool(BaseTool):
    name: str = "Ask question to coworker"
    description: str = "Ask a specific question to one of your coworkers."
    args_schema: type[BaseModel] = AskQuestionToolSchema
    agents: list[Any] = Field(default_factory=list, exclude=True)

    def _run(self, question: str, context: str, coworker: str) -> str:
        from .task import Task

        agent = _find_coworker(self.agents, coworker)
        question_task = Task(
            description=question,
            expected_output="Your best complete answer to the question.",
            agent=agent,
        )
        return question_task.execute_sync(agent=agent, context=context).raw


def delegation_tools(agents: list[Any]) -> list[BaseTool]:
    roles = ", ".join(a.role for a in agents)
    delegate = DelegateWorkTool(agents=list(agents))
    delegate.description = (
        "Delegate a specific task to one of the following coworkers: " + roles
    )
    ask = AskQuestionTool(agents=list(agents))
    ask.description = (
        "Ask a specific question to one of the following coworkers: " + roles
    )
    return [delegate, ask]
