"""Resolve "auto" in a run config before the run is persisted or launched.

Called from ``ExecutionService.create_execution`` (through ``run_freeze``),
which every run passes through: the Runs API, chat answers, scheduled and
external runs. After it, the config holds only concrete model keys, so the
execution history, the crew and flow subprocesses and anything exported from
the run never see "auto", and it never reaches LiteLLM.

One decision per run: every "auto" in the config gets the same model.
"""

import asyncio
import logging
from typing import Any, Iterator, Optional, Set, Tuple

from src.schemas.execution import CrewConfig
from src.services.decisions.model_selection import (
    ModelSelection,
    current_selection,
    is_auto,
    select_for_workspace,
    trace_row,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

#: Trace writes in flight; held so the event loop does not drop them.
_pending: Set["asyncio.Task[None]"] = set()

#: Agent fields that name a model.
_AGENT_MODEL_FIELDS = ("llm", "function_calling_llm")


def _slots(config: CrewConfig) -> Iterator[Tuple[dict, str]]:
    """Every (container, field) in the config whose model asks for Auto.

    An agent's ``llm`` may be a key or a ``{"model": ...}`` dict; both count.
    """
    agents = config.agents_yaml if isinstance(config.agents_yaml, dict) else {}
    for spec in agents.values():
        if not isinstance(spec, dict):
            continue
        for field in _AGENT_MODEL_FIELDS:
            value = spec.get(field)
            if is_auto(value) or (
                isinstance(value, dict) and is_auto(value.get("model"))
            ):
                yield spec, field
    for node in config.nodes if isinstance(config.nodes, list) else []:
        data = node.get("data") if isinstance(node, dict) else None
        if isinstance(data, dict) and is_auto(data.get("llm")):
            yield data, "llm"


def wants_auto(config: CrewConfig) -> bool:
    return is_auto(config.model) or next(_slots(config), None) is not None


def run_prompt(config: CrewConfig) -> str:
    """What the run is asked to do, as the decision model should read it."""
    inputs = config.inputs if isinstance(config.inputs, dict) else {}
    for candidate in (
        config.user_message,
        inputs.get("user_request"),
        inputs.get("instruction"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    tasks = config.tasks_yaml if isinstance(config.tasks_yaml, dict) else {}
    return "\n\n".join(
        str(t.get("description") or "")
        for t in list(tasks.values())[:3]
        if isinstance(t, dict)
    )


def apply_selection(config: CrewConfig, model: Optional[str]) -> None:
    """Write ``model`` into every Auto slot; None removes them (the run default)."""
    if is_auto(config.model):
        config.model = model
    for container, field in list(_slots(config)):
        value = container[field]
        if model is None:
            container.pop(field, None)
        elif isinstance(value, dict):
            container[field] = {**value, "model": model}
        else:
            container[field] = model


async def resolve_run_models(
    config: CrewConfig, session: Any, group_context: Optional[GroupContext]
) -> Optional[ModelSelection]:
    """Replace every "auto" in ``config``; None when the run did not ask for Auto."""
    if not wants_auto(config):
        return None
    prompt = run_prompt(config)
    if session is not None:
        selection = await select_for_workspace(session, group_context, prompt)
    else:
        # Internal callers (the chat fast path, deck refinement) have no request
        # session; resolve on a routed one like any other code outside a request.
        from src.db.session import routed_scoped_session

        async with routed_scoped_session() as scoped:
            selection = await select_for_workspace(scoped, group_context, prompt)
    apply_selection(config, selection.model)
    return selection


def selection_for(
    config: CrewConfig, resolved: Optional[ModelSelection]
) -> Optional[ModelSelection]:
    """The Auto pick behind this run: made here, or earlier in the same request.

    A chat message resolves Auto at the dispatcher, before intent detection and
    generation, so the run it starts arrives here with a concrete model. The
    request's selection is attributed to the run only when the run's model is
    the one Auto picked.
    """
    if resolved is not None:
        return resolved
    earlier = current_selection.get()
    if earlier is not None and earlier.model and earlier.model == config.model:
        return earlier
    return None


async def _write_trace(
    selection: ModelSelection, job_id: str, group_id: Optional[str]
) -> None:
    try:
        from src.db.session import routed_scoped_session
        from src.services.trace.service import ExecutionTraceService

        async with routed_scoped_session() as session:
            await ExecutionTraceService(session).create_trace(
                trace_row(selection, job_id, group_id)
            )
            await session.commit()
    except Exception as exc:  # noqa: BLE001 — observability must never fail a run
        logger.warning("Could not record Auto model selection (%s)", type(exc).__name__)


def record_selection(
    selection: Optional[ModelSelection],
    job_id: str,
    group_context: Optional[GroupContext],
) -> None:
    """Write the run's Auto trace row off the request path (fire and forget).

    A task of its own gets its own routed session (see ``routed_scoped_session``),
    so a failed insert can never poison the request's transaction.
    """
    if selection is None:
        return
    group_id = getattr(group_context, "primary_group_id", None)
    task = asyncio.create_task(_write_trace(selection, job_id, group_id))
    _pending.add(task)
    task.add_done_callback(_pending.discard)
