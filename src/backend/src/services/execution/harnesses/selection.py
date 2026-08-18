"""Which harness is running, right here, right now.

Deliberately free of database access. The operator's *setting* is read once per
execution, by the layer that creates the execution row (see
``EngineConfigService.get_harness``); everything downstream — the
kernel builders, the tool adapters, a spawned subprocess — asks THIS module,
which only ever resolves an already-decided value.

That split is the whole point. If the kernel could read the setting, a run
started before a flip and still building agents after it would be half one
harness and half the other, and nothing would say so.

## The three places the answer can come from

Checked in this order:

1. **The context binding** (``bind``). Set for the duration of one execution.
   Correct for the Chat path, which runs many executions in one process.
2. **The process default** (``set_process_default``). Set once by
   ``subprocess_bootstrap`` in a spawned crew/flow interpreter, where the whole
   process serves exactly one run. This is the belt to the ContextVar's braces:
   ``Crew.kickoff`` runs tasks on a thread pool that does NOT copy ContextVars
   (see ``runtime/agent.py``'s note on ``run_deadline``), so a tool adapter
   executing on a worker thread would otherwise fall through to the default.
3. **The environment** (``KASAL_HARNESS``). The child interpreter's
   earliest reader, before any config has been parsed.

Then ``kasal`` — which is not a guess so much as the invariant this module
protects: an unresolvable harness must never fail a run, it must run the harness
that has always been here.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional, Union

from src.core.logger import LoggerManager
from src.services.execution.harnesses.binding import HarnessName

logger = LoggerManager.get_instance().crew

#: Read by a spawned interpreter before it has parsed anything.
HARNESS_ENV_VAR = "KASAL_HARNESS"

#: The key under which the decision travels inside a crew/flow config payload.
#: Underscore-prefixed because it is platform plumbing, not part of the
#: user-authored crew definition, and ``config_adapter`` must leave it alone.
HARNESS_CONFIG_KEY = "_harness"

#: What a run gets when nothing says otherwise.
DEFAULT_HARNESS = HarnessName.KASAL

_current: ContextVar[Optional[HarnessName]] = ContextVar(
    "kasal_active_engine", default=None
)

_process_default: Optional[HarnessName] = None


def coerce(value: Union[str, HarnessName, None]) -> Optional[HarnessName]:
    """A stored/transmitted string as an ``HarnessName``, or None.

    Unknown values return None with a warning rather than raising. This is read
    on the hot path of every build, and a typo in a config row must degrade to
    the default harness, not take the platform down.

    Note what is NOT accepted as an alias: ``"crewai"`` means CrewAI, full stop.
    It used to be the legacy name for the Kasal harness in ``engineconfig`` rows
    (``db/session.py::_heal_engine_config_names`` rewrote them), which is
    exactly why the harness choice lives under its own key rather than reusing
    ``engine_name`` — the same word has to be free to mean the real thing now.
    """
    if value is None:
        return None
    if isinstance(value, HarnessName):
        return value
    text = str(value).strip().lower()
    if not text:
        return None
    try:
        return HarnessName(text)
    except ValueError:
        logger.warning(
            "Unknown harness %r; falling back to %s",
            value,
            DEFAULT_HARNESS.value,
        )
        return None


def set_process_default(harness: Union[str, HarnessName, None]) -> HarnessName:
    """Pin the harness for this whole interpreter. For subprocess bootstrap only.

    Also stamps the environment variable, so a grandchild process (and any code
    that reads the env directly before imports settle) agrees.
    """
    global _process_default
    resolved = coerce(harness) or DEFAULT_HARNESS
    _process_default = resolved
    os.environ[HARNESS_ENV_VAR] = resolved.value
    logger.info("Harness for this process: %s", resolved.value)
    return resolved


@contextmanager
def bind(harness: Union[str, HarnessName, None]) -> Iterator[HarnessName]:
    """Run a block with ``harness`` active, restoring the previous value after.

    Used by the in-process paths (Chat, and the parent side of a crew/flow
    launch) where one process serves executions that could, across a config
    flip, want different harnesses.
    """
    resolved = coerce(harness) or DEFAULT_HARNESS
    token = _current.set(resolved)
    try:
        yield resolved
    finally:
        _current.reset(token)


def active_name() -> HarnessName:
    """The harness in force here. Never raises, never returns None."""
    from_context = _current.get()
    if from_context is not None:
        return from_context
    if _process_default is not None:
        return _process_default
    return coerce(os.environ.get(HARNESS_ENV_VAR)) or DEFAULT_HARNESS


def reset_for_tests() -> None:
    """Clear the process default and the environment stamp.

    Tests that pin a harness must not leak it into the next test on the same
    xdist worker — a leaked harness is a whole suite failing for a reason that
    has nothing to do with what it was testing.
    """
    global _process_default
    _process_default = None
    os.environ.pop(HARNESS_ENV_VAR, None)
