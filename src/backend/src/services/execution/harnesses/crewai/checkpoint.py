"""Crash-resume for a CrewAI crew, from Kasal's checkpoint.

## One contract, two harnesses

The checkpoint RECORD is harness-neutral and stays that way. It is written by
``execution/checkpointing/recorder.py`` from bus events — which the CrewAI
harness already produces, because the event bridge republishes CrewAI's
``TaskCompletedEvent`` carrying the same task object the recorder matches on.
So writing needed nothing.

Reading needed this. ``checkpointing/resume.build_crew_payload`` produces the
payload; what differs per harness is only how a restored prefix is skipped:

* the Kasal runtime seeds ``Crew._seeded_outputs`` and its own loop skips them;
* CrewAI has no such seam, so the restored tasks' ``execute_sync`` is replaced
  with one that returns the stored output.

Replacing the execution call rather than removing the tasks is deliberate.
CrewAI accumulates ``task_outputs`` as it goes and hands them to
``_get_context``; a removed task would vanish from that chain and every later
task would run with different context than it did originally — a resume that
silently changes the inputs of the work it did not redo.

## The prefix rule is the same rule, and a test holds it there

Restore the longest contiguous PREFIX whose task identities still match, and
stop at the first that does not. Two properties follow:

* context chaining is byte-identical to an uninterrupted run, because a
  restored task's inputs are exactly the restored tasks before it;
* editing task 4 of 5 keeps 1-3 and re-runs 4 onward, while editing task 1
  keeps nothing — a task after an edit has stale context even when its own text
  is untouched, so restoring it would be the silent-wrong-answer case.

Identity comes from ``runtime/identity.py``, which is duck-typed over
description / expected_output / agent / tools and therefore reads a CrewAI task
exactly as it reads a Kasal one. That shared function is what stops the two
harnesses from drifting apart on what "the same task" means.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.services.execution.runtime.identity import (
    legacy_task_identity,
    task_identity,
)

logger = LoggerManager.get_instance().crew


def _entry_matches(task: Any, entry: Dict[str, Any], index: int) -> bool:
    """Whether a stored unit still describes the task now at ``index``.

    A stored identity is accepted against EITHER the current identity or the
    legacy ``Task.key`` it replaced — rejecting the legacy form would invalidate
    every checkpoint written before tools and model joined the hash. An entry
    with no stored identity is accepted, as it always was: that is every
    pre-identity checkpoint.

    Identical to the Kasal runtime's rule, using the same functions.
    """
    stored = entry.get("task_key")
    if not stored:
        return True
    if stored in (task_identity(task), legacy_task_identity(task)):
        return True
    logger.info(
        "crew: task %d (%r) changed since the checkpoint — it and every task "
        "after it will re-run",
        index,
        getattr(task, "name", None) or index,
    )
    return False


def restorable_outputs(
    tasks: List[Any],
    from_checkpoint: Optional[Dict[str, Any]],
    sequential: bool,
) -> Dict[int, Any]:
    """``{task index: crewai.TaskOutput}`` for the prefix that still matches.

    Returns an empty mapping — never raises — whenever the checkpoint cannot be
    used. "Run from scratch" is always a safe answer; a resume that half-works
    is not, which is why every rejection below is logged with its reason.
    """
    if not from_checkpoint or not isinstance(from_checkpoint, dict):
        return {}
    if not sequential:
        logger.warning(
            "checkpoint resume only supports the sequential process; "
            "running from scratch"
        )
        return {}

    completed = from_checkpoint.get("completed")
    if isinstance(completed, dict):
        try:
            completed = [completed[key] for key in sorted(completed, key=int)]
        except (TypeError, ValueError):
            logger.warning("checkpoint 'completed' is not index-keyed; from scratch")
            return {}
    if not isinstance(completed, list) or not completed:
        return {}

    by_index: Dict[int, Dict[str, Any]] = {}
    for entry in completed:
        if not isinstance(entry, dict):
            continue
        try:
            by_index[int(entry["index"])] = entry
        except (KeyError, TypeError, ValueError):
            logger.warning("checkpoint entry has no usable index; from scratch")
            return {}

    task_output_cls = _task_output_class()
    restored: Dict[int, Any] = {}
    for index, task in enumerate(tasks):
        entry = by_index.get(index)
        if entry is None:
            break  # the contiguous prefix ends here
        if not _entry_matches(task, entry, index):
            break
        restored[index] = _build_output(task_output_cls, task, entry)
    return restored


def _task_output_class() -> type:
    from crewai.tasks.task_output import TaskOutput

    return TaskOutput


def _build_output(cls: type, task: Any, entry: Dict[str, Any]) -> Any:
    json_dict = entry.get("output_json")
    agent = entry.get("agent")
    if not agent:
        task_agent = getattr(task, "agent", None)
        agent = getattr(task_agent, "role", "") if task_agent is not None else ""
    return cls(
        description=getattr(task, "description", ""),
        name=entry.get("name") or getattr(task, "name", None),
        expected_output=getattr(task, "expected_output", None),
        summary=entry.get("summary"),
        raw=entry.get("output_raw") or "",
        json_dict=json_dict if isinstance(json_dict, dict) else None,
        agent=agent,
    )
