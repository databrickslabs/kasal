"""Build CrewAI objects using the portable kwargs translation shared with exports."""

from __future__ import annotations

from typing import Any, Dict

from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.availability import crewai_symbols
from src.services.execution.harnesses.crewai.kwargs import _KNOWN_DROPS as _KNOWN_DROPS
from src.services.execution.harnesses.crewai.kwargs import translate as translate

logger = LoggerManager.get_instance().crew


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
    from src.services.execution.harnesses.crewai.guardrails import (
        adapt_guardrail,
        degrade_on_exhausted,
    )
    from src.services.execution.harnesses.crewai.tools import adapt_tools

    kwargs = dict(kwargs)
    if kwargs.get("tools"):
        kwargs["tools"] = adapt_tools(kwargs["tools"])

    for key in ("guardrail", "guardrails"):
        existing = kwargs.get(key)
        if isinstance(existing, (list, tuple)):
            kwargs[key] = [adapt_guardrail(guardrail) for guardrail in existing]
        elif existing is not None:
            kwargs[key] = adapt_guardrail(existing)

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
    effort = kwargs.pop("execution_effort", None)

    crew = _build(kasal_memory_crew_class(), kwargs, "crew")
    object.__setattr__(crew, "_kasal_run_max_seconds", run_max_seconds)
    object.__setattr__(crew, "_kasal_execution_effort", effort)
    # The object survives the flag. Callers read the memory backend back OFF the
    # crew to build the recall provider; if `memory=False` were the only record,
    # they would build nothing and the crew would be silently memory-less.
    carry_memory(crew, memory)
    return crew
