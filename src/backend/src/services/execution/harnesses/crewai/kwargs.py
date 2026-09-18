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

logger = LoggerManager.get_instance().crew

#: Kasal kwarg → CrewAI kwarg, where only the spelling differs.
_RENAMED: Dict[str, str] = {}

#: Keys CrewAI ACCEPTS but does not mean the same thing by — "false friends".
#:
#: Checked BEFORE the accepted-field test, which is the whole point: that test
#: passes anything CrewAI declares straight through, so a key present in both
#: vocabularies with different semantics reaches CrewAI as a well-formed value
#: of the wrong kind. `_KNOWN_DROPS` cannot express this, because it is only
#: consulted for keys CrewAI does NOT accept.
_FALSE_FRIENDS: Dict[str, str] = {
    "skills": (
        "Kasal's skills are NAMES resolved from a group-scoped database by "
        "services/skills; CrewAI 1.15's Agent.skills is a list of FILESYSTEM "
        "paths it loads itself (crewai.skills.loader), so a name reaches it as "
        "a path and raises FileNotFoundError before the agent is built. The "
        "Kasal skill still reaches the agent, identically on both harnesses: "
        "kernel/agent_skills.py injects the <available_skills> block into the "
        "prompt and equips load_skill/read_skill_file, which go through "
        "wrap_tool and so stay group-scoped, approvable and traced — none of "
        "which CrewAI's own loader tool would be"
    ),
}

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
        # BEFORE the accepted test — see _FALSE_FRIENDS. A key CrewAI declares
        # would otherwise be forwarded as a value of the wrong kind.
        if key in _FALSE_FRIENDS:
            dropped.drop(key, _FALSE_FRIENDS[key])
            continue
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
