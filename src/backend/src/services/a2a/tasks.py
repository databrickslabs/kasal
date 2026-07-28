"""A2A task operations, over the shared External Invocation Layer.

SendMessage / GetTask / ListTasks / CancelTask. Every one of them is a
translation: the work is done by ``services/external/``, which the MCP adapter
calls too. If a decision is being made in this file that an MCP caller would
also need, it belongs one layer down.

The A2A shape differs from MCP's in one substantive way: a message can either
START a task or CONTINUE one. Continuing a task that is waiting for a human is
how A2A expresses answering an approval gate — the same round-trip MCP reaches
through ``respond_to_run``.
"""

import logging
from typing import Any, List, Optional

from src.schemas.a2a import Message, Task
from src.services.a2a import render
from src.services.external import artifacts as canonical_artifacts
from src.services.external import interaction
from src.services.external.identity import ExternalCaller
from src.services.external.invocation import (
    InvocationResult,
    cancel_run,
    run_status,
    start_run,
)
from src.services.external.publication import PublicationService
from src.services.external.state import ExternalTaskState

logger = logging.getLogger(__name__)


class UnknownSkillError(Exception):
    """The caller named a skill that is not on its card."""


class UnknownTaskError(Exception):
    """The caller named a task it may not see, or that does not exist."""


async def send_message(
    caller: ExternalCaller,
    message: Message,
    skill_id: Optional[str] = None,
    task_id: Optional[str] = None,
    session: Any = None,
) -> Task:
    """Start a task, or continue one that is waiting for input.

    With ``task_id``, this is an ANSWER to a paused task — the A2A way of
    completing a human-in-the-loop gate. Without one, it starts the named skill.
    """
    text = render.text_of(message)

    if task_id:
        return await _continue_task(caller, task_id, text, session=session)

    if not skill_id:
        raise UnknownSkillError(
            "Name a skill from the agent card, or supply taskId to continue a task."
        )

    service = PublicationService(session)
    publication = await service.resolve_capability(caller, skill_id)
    if publication is None:
        # Covers "no such skill" and "another tenant's skill" identically — the
        # caller must not be able to tell them apart.
        raise UnknownSkillError(f"No skill named {skill_id!r}")

    result = await start_run(
        caller=caller,
        publication=publication,
        inputs={"request": text} if text else None,
        session=session,
    )
    return render.to_task(result)


async def _continue_task(
    caller: ExternalCaller, task_id: str, text: str, session: Any = None
) -> Task:
    """Answer a task that is waiting for a human."""
    accepted = await interaction.respond(
        caller=caller, run_id=task_id, response=text, session=session
    )
    if not accepted:
        raise UnknownTaskError(
            f"Task {task_id!r} is not waiting for input; nothing to answer."
        )
    # Re-read rather than assume: responding resumes the run, and its state at
    # this instant is a fact to report, not one to predict.
    return await get_task(caller, task_id, session=session)


async def get_task(caller: ExternalCaller, task_id: str, session: Any = None) -> Task:
    """A task's current state, with its output or its pending question."""
    result = await run_status(caller, task_id, session=session)
    if result is None:
        raise UnknownTaskError(f"No task {task_id!r}")

    pending = await interaction.pending_for_run(caller, task_id, session=session)
    if pending:
        # A paused run reports INPUT_REQUIRED with the question as the task's
        # current message — the state the whole shared layer exists to express.
        paused = InvocationResult(
            run_id=result.run_id, state=ExternalTaskState.INPUT_REQUIRED
        )
        return render.to_task(paused, prompt=pending[0].prompt)

    artifact = (
        canonical_artifacts.build(result.output) if result.output is not None else None
    )
    return render.to_task(result, canonical_artifact=artifact)


async def cancel_task(
    caller: ExternalCaller, task_id: str, session: Any = None
) -> Task:
    """Stop a task."""
    result = await cancel_run(caller, task_id, session=session)
    if result is None:
        raise UnknownTaskError(f"No task {task_id!r}")
    return render.to_task(result)


async def list_tasks(
    caller: ExternalCaller, limit: int = 50, session: Any = None
) -> List[Task]:
    """This caller's tasks.

    Group-scoped through the execution repository. An unscoped ListTasks is a
    cross-tenant data leak in one call, which is why it reads through the same
    group-filtered path as everything else rather than the generic history
    query.
    """
    from src.services.execution.service import ExecutionService

    service = ExecutionService(session)
    executions = await service.list_executions(
        group_ids=caller.group_ids, user_email=None, limit=limit, offset=0
    )

    tasks: List[Task] = []
    for execution in executions or []:
        run_id = execution.get("execution_id") or execution.get("job_id")
        if not run_id:
            continue
        from src.services.external.state import to_external_state

        tasks.append(
            render.to_task(
                InvocationResult(
                    run_id=str(run_id),
                    state=to_external_state(execution.get("status")),
                )
            )
        )
    return tasks
