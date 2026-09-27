"""Resolve "auto" in a run config before the run is persisted or launched.

Called through ``run_freeze`` by every path that starts a run:
``ExecutionService.create_execution`` (the Runs API, chat answers, A2A, deck
refinement), the scheduler, queue triggers, the MCP ``ask`` tool and
``/flow-execution``. After it, the config holds only concrete model keys, so the
crew and flow subprocesses and anything exported from the run never see "auto".
Should one slip through anyway, ``LLMManager.configure_kasal_llm`` turns it into
the workspace default and logs the path (``model_selection.resolve_leaked_auto``).

One decision per run: every "auto" in the config gets the same model.
"""

import asyncio
import logging
from typing import Iterable, Iterator, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

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

#: Fields that name a model, wherever they sit in a config: an agent's ``llm``
#: (a key or a ``{"model": ...}`` dict) and ``function_calling_llm``, a flow
#: node's ``data.llm``, ``inputs.manager_llm`` / ``reasoning_llm``, a task's
#: ``llm_guardrail.llm_model``, and agents nested in a flow's ``flow_config``.
_MODEL_FIELDS = frozenset(
    {
        "model",
        "llm",
        "function_calling_llm",
        "manager_llm",
        "planning_llm",
        "reasoning_llm",
        "llm_model",
    }
)


def _slots(roots: Iterable[object]) -> Iterator[Tuple[dict, str]]:
    """Every (container, field) under ``roots`` whose model asks for Auto."""
    stack = list(roots)
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            for key, value in node.items():
                if key in _MODEL_FIELDS and is_auto(value):
                    yield node, key
                elif isinstance(value, (dict, list)):
                    stack.append(value)


def _roots(config: CrewConfig) -> Tuple[object, ...]:
    return (
        config.agents_yaml,
        config.tasks_yaml,
        config.inputs,
        config.nodes,
        config.flow_config,
    )


def wants_auto(config: CrewConfig) -> bool:
    return is_auto(config.model) or next(_slots(_roots(config)), None) is not None


def _prompt(user_message: object, inputs: object, tasks: object) -> str:
    """What the run is asked to do, as the decision model should read it."""
    inputs = inputs if isinstance(inputs, dict) else {}
    for candidate in (
        user_message,
        inputs.get("user_request"),
        inputs.get("instruction"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    tasks = tasks if isinstance(tasks, dict) else {}
    return "\n\n".join(
        str(t.get("description") or "")
        for t in list(tasks.values())[:3]
        if isinstance(t, dict)
    )


def run_prompt(config: CrewConfig) -> str:
    return _prompt(config.user_message, config.inputs, config.tasks_yaml)


def _fill(slots: Iterable[Tuple[dict, str]], model: Optional[str]) -> None:
    """Write ``model`` into each slot; None removes them (the run default)."""
    for container, field in list(slots):
        if model is None:
            container.pop(field, None)
        else:
            container[field] = model


def apply_selection(config: CrewConfig, model: Optional[str]) -> None:
    """Write ``model`` into every Auto slot; None removes them (the run default)."""
    if is_auto(config.model):
        config.model = model
    _fill(_slots(_roots(config)), model)


async def _select(
    prompt: str,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> ModelSelection:
    if session is not None:
        return await select_for_workspace(session, group_context, prompt)
    # Internal callers (the chat fast path, deck refinement) have no request
    # session; resolve on a routed one like any other code outside a request.
    from src.db.session import routed_scoped_session

    async with routed_scoped_session() as scoped:
        return await select_for_workspace(scoped, group_context, prompt)


async def resolve_run_models(
    config: CrewConfig,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> Optional[ModelSelection]:
    """Replace every "auto" in ``config``; None when the run did not ask for Auto."""
    if not wants_auto(config):
        return None
    selection = await _select(run_prompt(config), session, group_context)
    apply_selection(config, selection.model)
    return selection


async def resolve_mapping_models(
    config: dict,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> Optional[ModelSelection]:
    """``resolve_run_models`` for a plain-dict config (``/flow-execution``)."""
    slots = list(_slots([config]))
    if not slots:
        return None
    inputs = config.get("inputs")
    prompt = _prompt(config.get("user_message"), inputs, config.get("tasks_yaml"))
    selection = await _select(prompt, session, group_context)
    _fill(slots, selection.model)
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
        from src.db.session import get_isolated_db_session
        from src.services.trace.service import ExecutionTraceService

        # A PRIVATE connection, like every other out-of-band trace writer. On
        # SQLite all routed sessions share one StaticPool connection: the
        # request's session returning it (rollback on return) discarded this
        # row's uncommitted INSERT, the refresh then failed, and validating the
        # expired row raised "15 validation errors ... MissingGreenlet".
        async with get_isolated_db_session() as session:
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

    The task writes on its own connection, so a failed insert can never poison
    the request's transaction, and the request's cannot discard this one.
    """
    if selection is None:
        return
    group_id = getattr(group_context, "primary_group_id", None)
    task = asyncio.create_task(_write_trace(selection, job_id, group_id))
    _pending.add(task)
    task.add_done_callback(_pending.discard)
