"""Equipping every agent with its own plan.

The plan tool is engine machinery, not a capability a user grants — no seed row,
no picker entry, nothing to configure. This module is the one place that
attaches it, so all three execution paths get it from the same code (that is
what ``kernel/`` is for).

Attached **unconditionally**, which is the one judgement call here. Skills
attach only when the agent actually has skills, because advertising
``load_skill`` with nothing to load is noise. The plan has no equivalent
precondition: whether a task needs a plan is something only the model can judge
mid-task, and the tool's own description already says "use it for anything with
3+ steps". One extra schema in the prompt is cheap; a long task that needed a
plan and had no way to keep one is not.
"""

from typing import Any, Dict

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

#: Why this exists at all, when the tool's own description already explains it:
#: a tool description is read once the model is ALREADY considering that tool.
#: It is weak at making a model reach for something unprompted. Measured on a
#: real run — the plan tool was equipped and visible in the model's tool list
#: (verified: ``['read_file', 'todo']``), the agent made six tool calls, and
#: not one was ``todo``. Hermes reinforces the same way, in its operating brief
#: rather than only in the schema: "Track multi-step work with `todo`."
#:
#: Deliberately short. It lands after the ~730-character security preamble and
#: any attached skills section, and a long block there competes with the
#: instructions it is meant to support.
_PLAN_GUIDANCE = """PLANNING:
For work with three or more steps, keep a plan with the `todo` tool. Write it
before you start, keep exactly one item in progress, and mark each item
completed as soon as it is done rather than at the end. If an approach turns
out to be wrong, cancel that item and add a revised one — a cancelled item is
what stops you attempting the same dead end again, and a completed one is what
stops you redoing work you have already finished."""


def inject_plan_guidance(agent_kwargs: Dict[str, Any]) -> str:
    """Append the planning brief to the agent's prompt.

    APPENDED, not prepended: the security preamble is the highest-priority
    instruction and must stay first (see ``agent_security``). This is
    operational guidance and belongs after it.

    Follows the same field choice as the security preamble and the skills
    section — ``system_template`` when the agent has one, otherwise
    ``backstory``, which the default system prompt embeds. Mutates
    ``agent_kwargs`` in place and returns the field it wrote to.
    """
    field = "system_template" if agent_kwargs.get("system_template") else "backstory"
    agent_kwargs[field] = (agent_kwargs.get(field) or "") + "\n\n" + _PLAN_GUIDANCE
    return field


def add_plan_tool(agent_kwargs: Dict[str, Any], label: str = "") -> bool:
    """Give the agent the plan tool, once.

    Guarded against duplicates for the same reason the skill tools are: an
    agent that already carries it — from a path that resolved tools itself —
    would otherwise get two ``todo`` tools, and a duplicate tool name is how a
    tool-calling loop starts behaving unpredictably.

    Never raises. A missing plan tool degrades the agent; it must not fail the
    build.
    """
    try:
        from src.services.execution.runtime.plan import build_plan_tool

        tools = list(agent_kwargs.get("tools") or [])
        if any(getattr(tool, "name", "") == "todo" for tool in tools):
            logger.info("[plan] agent '%s': already equipped", label or "?")
            return False
        tools.append(build_plan_tool())
        agent_kwargs["tools"] = tools
        # Logged unconditionally, including the no-op case. "Was the plan tool
        # actually attached?" is the first question asked of a run whose trace
        # shows no plan, and answering it from the ABSENCE of a line is exactly
        # how the skills wiring next door wasted three rounds of debugging.
        # The tool alone was not enough — the prompt has to ask for it.
        inject_plan_guidance(agent_kwargs)
        logger.info(
            "[plan] agent '%s': plan tool equipped + guidance injected", label or "?"
        )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[plan] agent '%s': could not equip: %s", label or "?", exc)
        return False
