"""The agent's own plan for the task it is currently doing.

Kasal has two plans and they are not the same thing. The **task graph** is what
the user asked for — gather the data, build the dashboard, write the deck — and
it is decided before the run and does not change. This is the other one: how
*this* task gets done, written by the agent doing it, revised whenever the
evidence says the first guess was wrong.

Without it a long task has only the conversation to remember itself by, and the
conversation gets trimmed. What that looks like in practice, from a real run:
the same sub-task delegated five times because nothing recorded that it had
already been done, and 8192-token answers that were mostly the agent restating
its own progress because there was nowhere else to put it.

**This is engine machinery, not a tool a user grants.** There is no seed row, no
entry in the tool picker and nothing to configure — the same status as the
delegate/ask tools built in ``executor.py``. It is exposed to the model as a
callable only because a callable is the sole way a model can write anything;
that is a calling convention, not a product surface.

Scoping mirrors the tool outcome ledger next door, for the same reason: a crew
runs its tasks in worker threads and asyncio tasks branch their own context, so
a ContextVar keeps concurrent tasks from bleeding into each other's plans. It
holds no session, repository or GroupContext, so ``runtime/`` keeps its rule of
never depending on ``services/``.

The one thing the ledger does NOT do and this must: **delegation nests**. A
hierarchical manager executing a task delegates to a coworker, whose task runs
``execute_sync`` on the *same thread and context*. A bare reset there destroys
the manager's plan and hands it the coworker's leftovers. :func:`plan_scope`
pushes a child plan and restores the parent on exit.
"""

from __future__ import annotations

import contextvars
import json
import logging
from contextlib import contextmanager
from typing import Any, Iterator

from pydantic import BaseModel, Field

from src.core.events.bus import event_bus
from src.core.events.types import PlanUpdatedEvent

from src.services.tools.base import BaseTool

logger = logging.getLogger(__name__)

#: The only statuses an item may hold. ``cancelled`` matters as much as the
#: others: an approach that turned out to be wrong should be recorded as
#: abandoned, not quietly deleted, or the agent re-proposes it two rounds later.
VALID_STATUSES = ("pending", "in_progress", "completed", "cancelled")

#: Bounds. A plan is a handful of short lines; anything beyond this is the model
#: using the plan as a scratchpad for content, which defeats the purpose and
#: inflates every subsequent prompt.
MAX_ITEMS = 60
MAX_CONTENT_CHARS = 500


class PlanItem(BaseModel):
    """One step. Order in the list is priority."""

    id: str
    content: str
    status: str = "pending"

    def render(self) -> str:
        marker = {
            "completed": "[x]",
            "in_progress": "[>]",
            "pending": "[ ]",
            "cancelled": "[~]",
        }.get(self.status, "[?]")
        return f"{marker} {self.id}. {self.content}"


_plan: contextvars.ContextVar[list[PlanItem] | None] = contextvars.ContextVar(
    "kasal_task_plan", default=None
)


# ------------------------------------------------------------------ scoping


def reset_plan() -> None:
    """Start a fresh plan. Called at the top of each task."""
    _plan.set([])


def plan() -> list[PlanItem]:
    """The current task's plan (empty when none has been written)."""
    return list(_plan.get() or [])


@contextmanager
def plan_scope() -> Iterator[None]:
    """Give the enclosed work its own plan, restoring the caller's on exit.

    For delegation. A hierarchical manager holds the plan for the task it is
    executing and then hands work to a coworker whose task calls
    ``execute_sync`` — on the same thread, in the same context. Without this the
    coworker's ``reset_plan()`` overwrites the manager's plan, and when control
    returns the manager reads the coworker's leftovers as its own.
    """
    token = _plan.set([])
    try:
        yield
    finally:
        _plan.reset(token)


# ------------------------------------------------------------------ writing


def write_plan(items: list[dict[str, Any]], merge: bool = False) -> list[PlanItem]:
    """Replace the plan, or merge items into it by id.

    ``merge=False`` is a full rewrite and is the normal way to replan: the
    agent has learned something and the old decomposition is wrong. ``merge``
    updates named items and appends unknown ones, for the common case of ticking
    one thing off without restating the rest.
    """
    incoming: list[PlanItem] = []
    for raw in items[:MAX_ITEMS]:
        if not isinstance(raw, dict):
            continue
        item_id = str(raw.get("id") or "").strip()
        content = str(raw.get("content") or "").strip()
        if not item_id or not content:
            continue
        status = str(raw.get("status") or "pending").strip()
        if status not in VALID_STATUSES:
            status = "pending"
        incoming.append(
            PlanItem(id=item_id, content=content[:MAX_CONTENT_CHARS], status=status)
        )

    if merge:
        current = {item.id: item for item in plan()}
        order = [item.id for item in plan()]
        for item in incoming:
            if item.id not in current:
                order.append(item.id)
            current[item.id] = item
        result = [current[key] for key in order][:MAX_ITEMS]
    else:
        result = _dedupe(incoming)

    _plan.set(result)
    _emit(result)
    return result


def _dedupe(items: list[PlanItem]) -> list[PlanItem]:
    """Last write wins per id, original order preserved."""
    seen: dict[str, PlanItem] = {}
    order: list[str] = []
    for item in items:
        if item.id not in seen:
            order.append(item.id)
        seen[item.id] = item
    return [seen[key] for key in order]


def _emit(items: list[PlanItem]) -> None:
    """Put the plan on the run's event bus.

    Not decoration. Two of the three execution paths build inside a spawned
    interpreter, so a plan that lives only in that process's memory is invisible
    to the parent — and the trace is where "2 of 5 done, stuck on the metric
    view" has to show up, since the alternative is reading forty tool_usage rows
    to guess it.
    """
    try:
        counts = plan_counts(items)
        event_bus.emit(
            None,
            PlanUpdatedEvent(
                items=[item.model_dump() for item in items],
                rendered=render_plan(items),
                total=len(items),
                pending=counts["pending"],
                in_progress=counts["in_progress"],
                completed=counts["completed"],
                cancelled=counts["cancelled"],
            ),
        )
    except Exception:  # noqa: BLE001
        # A plan that cannot be traced is still a usable plan.
        logger.debug("could not emit PlanUpdatedEvent", exc_info=True)


# ------------------------------------------------------------------ reading


def plan_counts(items: list[PlanItem] | None = None) -> dict[str, int]:
    rows = plan() if items is None else items
    return {
        status: sum(1 for item in rows if item.status == status)
        for status in VALID_STATUSES
    }


def render_plan(items: list[PlanItem] | None = None) -> str:
    rows = plan() if items is None else items
    return "\n".join(item.render() for item in rows)


def unfinished_plan_items(items: list[PlanItem] | None = None) -> list[PlanItem]:
    """Items still open — what a completion check asks about.

    ``cancelled`` is deliberately NOT unfinished: abandoning an approach is a
    decision, not an omission.
    """
    rows = plan() if items is None else items
    return [item for item in rows if item.status in ("pending", "in_progress")]


def plan_summary() -> str:
    """One line for a guardrail or a degradation notice, or "" when no plan."""
    rows = plan()
    if not rows:
        return ""
    counts = plan_counts(rows)
    open_items = unfinished_plan_items(rows)
    done = counts["completed"]
    cancelled = counts["cancelled"]

    summary = f"{done}/{len(rows)} plan items completed"
    if cancelled:
        summary += f", {cancelled} cancelled"
    if open_items:
        still_open = "; ".join(
            f"{item.id} ({item.content[:60]})" for item in open_items[:5]
        )
        summary += f" — still open: {still_open}"
    return summary


# ------------------------------------------------------------------ the tool


class PlanToolSchema(BaseModel):
    todos: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Items to write, each {id, content, status}. Omit entirely to read "
            "the current list."
        ),
    )
    merge: bool = Field(
        default=False,
        description=(
            "False (default) replaces the whole list with a fresh plan. True "
            "updates existing items by id and appends new ones."
        ),
    )


class PlanTool(BaseTool):
    """The model's handle on its own plan.

    All behavioural guidance lives in ``description`` so it is part of the
    static tool schema — stable across the conversation and therefore cacheable,
    rather than a system-prompt block that shifts every turn.
    """

    name: str = "todo"
    description: str = (
        "Track your plan for the task you are working on. Use it for anything "
        "with 3+ steps.\n\n"
        "Call with no arguments to read the current list.\n"
        "Provide 'todos' to write: each item is {id, content, status} where "
        "status is pending, in_progress, completed or cancelled.\n"
        "merge=false (default) replaces the whole list — use it to REPLAN when "
        "you have learned something that makes the old plan wrong.\n"
        "merge=true updates items by id and adds new ones.\n\n"
        "List order is priority. Keep exactly ONE item in_progress. Mark an "
        "item completed as soon as it is done, not at the end. If an approach "
        "fails, cancel that item and add a revised one rather than deleting it "
        "— a cancelled item is what stops you proposing the same dead end "
        "again.\n\n"
        "Always returns the full current list."
    )
    args_schema: type[BaseModel] = PlanToolSchema

    def _run(self, **kwargs: Any) -> str:
        todos = kwargs.get("todos")
        merge = bool(kwargs.get("merge"))

        if todos is not None:
            # Models intermittently send the array as a JSON string.
            if isinstance(todos, str):
                try:
                    todos = json.loads(todos)
                except (json.JSONDecodeError, TypeError):
                    return (
                        "Error: 'todos' must be a list of objects; the string "
                        "supplied could not be parsed as JSON."
                    )
            if not isinstance(todos, list):
                return f"Error: 'todos' must be a list, got {type(todos).__name__}."
            items = write_plan(todos, merge=merge)
        else:
            items = plan()

        counts = plan_counts(items)
        if not items:
            return (
                "The plan is empty. Write one with the 'todos' argument before "
                "starting multi-step work."
            )
        return f"Plan ({counts['completed']}/{len(items)} completed):\n" + render_plan(
            items
        )


def build_plan_tool() -> PlanTool:
    """The plan tool, built by the engine.

    Flagged always-available for the same reason the skill tools are: a task
    that selects its own tools REPLACES the agent's, and this is not a
    selection. Dropping it would leave the model told to keep a plan it has no
    way to write.
    """
    tool = PlanTool()
    object.__setattr__(tool, "_kasal_always_available", True)
    return tool
