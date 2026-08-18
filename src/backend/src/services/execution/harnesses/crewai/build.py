"""The kernel's kwargs, translated into CrewAI's constructors.

The kernel assembles ONE dict per agent, task and crew and hands it to the
active binding. Both runtimes accept overlapping but not identical sets, so this
is where the difference lives — in one readable place per harness, rather than as
``if harness == "crewai"`` scattered across the twenty modules that build things.

## Filtered against the target, not against a hand-written list

Accepted keys are derived from the CrewAI class's own ``model_fields``. A
hand-maintained allow-list would be stale the first time CrewAI adds a field,
and stale in the direction that silently drops something the user set.

Three things then get names of their own:

* ``_RENAMED`` — the same concept under a different key.
* ``_KNOWN_DROPS`` — a Kasal concept CrewAI genuinely does not have, WITH the
  reason. This is the honest part of the file: it is the list of things that
  behave differently when you switch harnesses.
* anything else unaccepted — dropped with a WARNING naming the key, because an
  unclassified kwarg means the kernel grew a feature this translation has not
  caught up with, and that should be noisy.

Nothing is ever dropped silently. A run that quietly ignores half its settings
looks like a run that worked.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from src.core.logger import LoggerManager
from src.services.execution.harnesses.binding import DroppedKwargs
from src.services.execution.harnesses.crewai.availability import crewai_symbols

logger = LoggerManager.get_instance().crew

#: Kasal kwarg → CrewAI kwarg, where only the spelling differs.
_RENAMED: Dict[str, str] = {}

#: Kasal concepts CrewAI has no equivalent for, and what it costs to lose them.
#: Keyed by kwarg; the value is the reason, which is logged.
_KNOWN_DROPS: Dict[str, str] = {
    # Agent
    "run_deadline": (
        "stamped at kickoff, not at build time — the clock starts when work "
        "does. The CrewAI crew subclass stamps it the same way Kasal's does"
    ),
    "rpm_controller": "CrewAI builds its own from max_rpm",
    # Crew
    "context_providers": (
        "memory RECALL is wired through a task-level hook instead — see "
        "harnesses/crewai/memory.py"
    ),
    "output_sinks": ("memory PERSISTENCE is wired through Crew.task_callback instead"),
    "prompt_to_print_output": "inert in both runtimes",
    "token_usage": "CrewAI computes its own; a seeded value would be overwritten",
    # Task
    "output_contract": "Kasal-specific; enforced by the kernel before hand-off",
    "guardrail_on_exhausted": (
        "no CrewAI field; the 'degrade' policy is applied by wrapping the "
        "guardrail itself — see harnesses/crewai/guardrails.py"
    ),
    "on_budget_exceeded": (
        "no CrewAI field; the transport still degrades a spent budget into a "
        "wrap-up answer, which is the behaviour this selects"
    ),
}


def _accepted(cls: Any) -> frozenset:
    """The constructor keys this CrewAI class actually declares."""
    fields = getattr(cls, "model_fields", {}) or {}
    accepted = set(fields)
    for name, field in fields.items():
        alias = getattr(field, "alias", None)
        if alias:
            accepted.add(alias)
    return frozenset(accepted)


def translate(
    kwargs: Dict[str, Any], cls: Any, subject: str
) -> Tuple[Dict[str, Any], DroppedKwargs]:
    """``kwargs`` reduced to what ``cls`` accepts, plus what was lost.

    Returns the dropped set rather than logging inside, so a caller can report
    one line per constructed object instead of one line per lost key.
    """
    accepted = _accepted(cls)
    out: Dict[str, Any] = {}
    dropped = DroppedKwargs(subject)

    for key, value in kwargs.items():
        target = _RENAMED.get(key, key)
        if target in accepted:
            out[target] = value
            continue
        if key in _KNOWN_DROPS:
            dropped.drop(key, _KNOWN_DROPS[key])
            continue
        dropped.drop(key, "not accepted by CrewAI and not classified")
        logger.warning(
            "[crewai] %s: kwarg %r is neither accepted by %s nor listed in "
            "_KNOWN_DROPS — the kernel may have grown a setting this "
            "translation has not caught up with",
            subject,
            key,
            getattr(cls, "__name__", cls),
        )
    return out, dropped


def _build(cls: Any, kwargs: Dict[str, Any], subject: str) -> Any:
    translated, dropped = translate(kwargs, cls, subject)
    if dropped:
        logger.info("[crewai] %s", dropped.summary())
    return cls(**translated)


#: Crew/agent kwargs that carry an LLM and therefore need wrapping.
#:
#: Missing one is a quiet failure, not a loud one: CrewAI accepts an arbitrary
#: object on ``manager_llm`` and only discovers it is not a ``BaseLLM`` when the
#: manager makes its first call, halfway into a hierarchical run.
_LLM_KWARGS = ("llm", "manager_llm", "planning_llm", "function_calling_llm", "chat_llm")


def _wrap_llms(kwargs: Dict[str, Any]) -> None:
    """Put every LLM-bearing kwarg into CrewAI's shape, in place.

    A model-name string is left alone (the kernel's fallback when configuration
    failed), and something already a CrewAI ``BaseLLM`` is left alone too.
    """
    from src.services.execution.harnesses.crewai.llm import build_kasal_backed_llm

    base_llm = crewai_symbols()["BaseLLM"]
    for key in _LLM_KWARGS:
        value = kwargs.get(key)
        if value is None or isinstance(value, (str, base_llm)):
            continue
        kwargs[key] = build_kasal_backed_llm(value)


def build_agent(**kwargs: Any) -> Any:
    """A ``crewai.Agent`` from the kernel's agent kwargs.

    The LLM is re-wrapped rather than passed through: the kernel built a Kasal
    transport object, and CrewAI will only accept a ``BaseLLM``. Wrapping keeps
    the request path identical across harnesses (see ``llm.py``).
    """
    from src.services.execution.harnesses.crewai.tools import adapt_tools

    kwargs = dict(kwargs)
    _wrap_llms(kwargs)
    if kwargs.get("tools"):
        kwargs["tools"] = adapt_tools(kwargs["tools"])

    label = kwargs.get("role") or "agent"
    window = kwargs.get("max_context_window_size")
    agent = _build(crewai_symbols()["Agent"], kwargs, f"agent {label!r}")

    # CrewAI has no such field, but the field is not really CrewAI's business:
    # `transport._effective_context_window` reads it off `from_agent`, and
    # CrewAI passes the agent through to every call. Carrying it keeps a
    # per-agent window override working on both harnesses.
    if window:
        try:
            object.__setattr__(agent, "max_context_window_size", window)
        except Exception as e:  # noqa: BLE001 — an override is not worth a run
            logger.debug("Could not carry max_context_window_size: %s", e)
    return agent


def build_task(**kwargs: Any) -> Any:
    """A ``crewai.Task`` from the kernel's task kwargs."""
    from src.services.execution.harnesses.crewai.guardrails import degrade_on_exhausted
    from src.services.execution.harnesses.crewai.tools import adapt_tools

    kwargs = dict(kwargs)
    if kwargs.get("tools"):
        kwargs["tools"] = adapt_tools(kwargs["tools"])

    # "Keep the best attempt, flagged" rather than "abort the task". CrewAI has
    # no equivalent field, so the policy is applied by wrapping the guardrail —
    # without it a research crew that degrades on Kasal simply fails here.
    if str(kwargs.get("guardrail_on_exhausted") or "").lower() == "degrade":
        retries = kwargs.get("max_retries")
        if retries is None:
            retries = kwargs.get("guardrail_max_retries", 3)
        for key in ("guardrail", "guardrails"):
            existing = kwargs.get(key)
            if not existing:
                continue
            if key == "guardrail":
                kwargs[key] = degrade_on_exhausted(existing, int(retries), key)
            else:
                kwargs[key] = [
                    degrade_on_exhausted(g, int(retries), f"guardrail {i}")
                    for i, g in enumerate(existing)
                ]

    label = kwargs.get("name") or (str(kwargs.get("description", ""))[:40] or "task")
    return _build(crewai_symbols()["Task"], kwargs, f"task {label!r}")


def build_crew(**kwargs: Any) -> Any:
    """A ``crewai.Crew`` from the kernel's crew kwargs.

    ``memory`` is forced OFF. CrewAI 1.15 ships unified cognitive memory over
    chromadb/lancedb; Kasal's memory is Databricks Vector Search and SQLite with
    group isolation and deterministic crew IDs. Letting CrewAI's initialise
    would fork tenant memory across two stores — and would import lancedb, which
    the whole harness is careful never to load.
    """
    from src.services.execution.harnesses.crewai.memory import (
        carry_memory,
        kasal_memory_crew_class,
    )

    kwargs = dict(kwargs)
    # A hierarchical crew's MANAGER runs on its own LLM, and so does the
    # planner. Both arrive as Kasal transport objects, and both must reach
    # CrewAI wrapped or the run fails at the manager's first call.
    _wrap_llms(kwargs)
    memory = kwargs.get("memory")
    if memory:
        logger.info(
            "[crewai] crew memory forced off: Kasal's memory subsystem is wired "
            "separately and CrewAI's own store would fork tenant data"
        )
    kwargs["memory"] = False

    # The run-level wall clock. CrewAI's Crew does not declare it, so it would
    # otherwise be dropped — and dropping it removes the ONLY fixed point in the
    # budget: `resolve_execution_budget` restarts the per-call clock on every
    # call, and under CrewAI every call is one tool round. A 30s cap then means
    # "30s per round", which is no cap at all.
    run_max_seconds = kwargs.pop("run_max_seconds", None)

    crew = _build(kasal_memory_crew_class(), kwargs, "crew")
    object.__setattr__(crew, "_kasal_run_max_seconds", run_max_seconds)
    # The object survives the flag. Callers read the memory backend back OFF the
    # crew to build the recall provider; if `memory=False` were the only record,
    # they would build nothing and the crew would be silently memory-less.
    carry_memory(crew, memory)
    return crew
