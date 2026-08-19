"""Deciding which harness a run uses — once — and making that decision stick.

The operator's setting is a *default for new runs*, not a live switch. This
module is the only place that reads it, and it reads it exactly once per
execution: at the moment the ``execution_history`` row is created. From then on
the answer travels with the run.

## Why once

Three failures, all of which look like something else when they happen:

* **A run split across two harnesses.** Agents are built when the run starts,
  tasks as it goes. Re-reading the setting per construction means a switch
  landing mid-run produces a crew whose agents are one runtime and whose later
  tasks are another. Nothing would report that; the run would just behave
  strangely.
* **A resume on the wrong harness.** A checkpoint is written by the harness that
  produced it. Resuming reads the ORIGINAL run's harness from its row, not the
  current setting, because the alternative is replaying half a run's state into
  a runtime that never produced it.
* **A finished run that cannot say what ran it.** Months later, "why did this
  execution behave differently?" needs an answer that survives every subsequent
  change to the setting. That is what the column is for.

## How it travels

1. ``execution_history.harness`` — the record, and the source of truth
   for a resume.
2. ``config["_harness"]`` — inside the payload handed to a spawned
   crew/flow interpreter, which has no session at the point it needs to know.
3. ``KASAL_HARNESS`` in that interpreter's environment — for anything
   that runs before the payload is parsed.

All three carry the same string. The child pins it process-wide
(:func:`adopt_in_subprocess`), because a spawned interpreter serves exactly one
run and a process-wide default survives the worker threads a ContextVar does
not.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional, Union

from src.core.logger import LoggerManager
from src.services.execution.harnesses import (
    DEFAULT_HARNESS,
    HARNESS_CONFIG_KEY,
    HARNESS_ENV_VAR,
    HarnessName,
    coerce,
    set_process_default,
)

logger = LoggerManager.get_instance().crew


async def resolve_run_harness(
    session: Any,
    stored: Union[str, HarnessName, None] = None,
) -> HarnessName:
    """The harness for ONE execution, decided now, on the caller's session.

    ``session`` is passed IN and never acquired here. Every caller already holds
    one chosen for a reason — ``create_run_record`` the private connection its
    row must be written on, the status service whichever branch it took — and a
    resolver that opened its own would quietly put a second connection on a path
    whose whole design is about which connection the write lands on.

    ``stored`` wins when present — that is a run being resumed or re-read, and
    its harness was decided when it was created. Otherwise the operator setting is
    read through the settings SERVICE, once.

    Never raises. A configuration read that fails (no row yet, a migration in
    flight, a permissions problem) must not stop a run from starting; it falls
    back to the default harness and says so.
    """
    already = coerce(stored)
    if already is not None:
        return already

    try:
        from src.services.settings.engine import EngineConfigService

        value = await EngineConfigService(session).get_harness()
        return coerce(value) or DEFAULT_HARNESS
    except Exception as e:  # noqa: BLE001 — a config read must not fail a run
        logger.warning(
            "Could not read the configured harness (%s); using %s",
            e,
            DEFAULT_HARNESS.value,
        )
        return DEFAULT_HARNESS


def stamp_on_config(
    config: Optional[Dict[str, Any]], harness: Union[str, HarnessName]
) -> Dict[str, Any]:
    """Put the decision inside the payload a subprocess receives.

    Returns the same dict, mutated: callers pass the config they are about to
    hand to ``run_crew_in_process`` / ``run_flow_in_process``, and a copy would
    silently drop the stamp for anyone holding the original.
    """
    resolved = coerce(harness) or DEFAULT_HARNESS
    config = config if config is not None else {}
    config[HARNESS_CONFIG_KEY] = resolved.value
    return config


def engine_from_config(config: Optional[Dict[str, Any]]) -> Optional[HarnessName]:
    """Read back what :func:`stamp_on_config` wrote, if anything."""
    if not isinstance(config, dict):
        return None
    return coerce(config.get(HARNESS_CONFIG_KEY))


def subprocess_env(harness: Union[str, HarnessName]) -> Dict[str, str]:
    """The environment entry a spawned interpreter reads before parsing config."""
    resolved = coerce(harness) or DEFAULT_HARNESS
    return {HARNESS_ENV_VAR: resolved.value}


def adopt_in_subprocess(config: Optional[Dict[str, Any]] = None) -> HarnessName:
    """Pin this interpreter's harness. Called first thing in a spawned process.

    Prefers the payload over the environment: the env var can be inherited from
    a parent that has since been reconfigured, while the payload was built for
    THIS run. Falls back to the environment, then to the default.
    """
    from_payload = engine_from_config(config)
    from_env = coerce(os.environ.get(HARNESS_ENV_VAR))
    chosen = from_payload or from_env or DEFAULT_HARNESS
    # Logged unconditionally, and naming the SOURCE.
    #
    # Nothing in the child announced which runtime it had adopted, so a log
    # could not answer "did this actually run on CrewAI?" — and the parts of a
    # run that are shared between harnesses (the plan tool, memory, the LLM
    # transport, the tool wrappers) all still say "kasal", which reads as the
    # wrong answer to that question.
    logger.info(
        "[harness] this interpreter runs on %s (from %s)",
        chosen.value,
        "the run's payload"
        if from_payload
        else ("the environment" if from_env else "the default"),
    )
    return set_process_default(chosen)


async def harness_for_execution(session: Any, execution_id: str) -> HarnessName:
    """The harness RECORDED against one run, not the one currently configured.

    This is what the dispatch layer asks before spawning an interpreter. A run
    can sit queued across a configuration change, and a resume can be started
    long after one; in both cases the harness that belongs to the run is the one
    on its row.

    Goes through ``ExecutionService.get_run_by_job_id`` rather than building
    ``ExecutionHistoryRepository`` here. Runs are that service's domain and the
    accessor already exists; reaching past it would be one more place applying —
    or forgetting — the group filter, which is the bug class the repository
    ownership rule exists to prevent.

    A row that EXISTS but records no harness ran on Kasal — it predates the
    column, and Kasal was the only harness then. That is returned rather than the
    current setting, and the difference is not academic: task identity is
    harness-dependent (CrewAI's ``Task`` inherits its agent's tools, Kasal's does
    not), so resuming an old run under a newly-selected CrewAI would match no
    stored unit and silently restart from scratch, reporting "task 0 changed
    since the checkpoint" when nothing about the task had changed at all.

    Only when there is no row at all does the setting decide — that is a run
    being created, not resumed.
    """
    try:
        from src.services.execution.service import ExecutionService

        row = await ExecutionService(session).get_run_by_job_id(execution_id)
        if row is not None:
            recorded = coerce(getattr(row, "harness", None))
            if recorded is not None:
                return recorded
            logger.info(
                "Execution %s records no harness; treating it as %s, the only "
                "harness that existed when the row was written",
                execution_id,
                DEFAULT_HARNESS.value,
            )
            return DEFAULT_HARNESS
    except Exception as e:  # noqa: BLE001 — never block a run on this lookup
        logger.warning(
            "Could not read the recorded harness for execution %s (%s)",
            execution_id,
            e,
        )
    return await resolve_run_harness(session)


@asynccontextmanager
async def dispatch_session(session: Any = None) -> AsyncIterator[Any]:
    """The session to resolve a harness on at a DISPATCH boundary.

    Yields the caller's session when it has one. When it does not — a run
    started from a scheduler sweep or a background task, where ``session=None``
    is threaded all the way down — it goes through ``routed_scoped_session``,
    the one sanctioned acquisition outside a request. That helper reuses the
    request's session when there IS one and otherwise asks the database ROUTER,
    so this cannot become the split-brain a raw factory would be.

    One place does this, and only at the boundary that dispatches a run. Nothing
    below it acquires anything.
    """
    if session is not None:
        yield session
        return

    from src.db.session import routed_scoped_session

    async with routed_scoped_session() as acquired:
        yield acquired
