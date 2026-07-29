"""Attaching Agent Skills to a built agent — the kernel's single seam.

Both halves of the mechanism land here, together, because they are useless
apart: the TIER-1 block tells the model which skills exist, and the ``load_skill``
/ ``read_skill_file`` tools are how it reads one. A block with no tools is a list
the model cannot act on; tools with no block are tools it never thinks to call.

In the KERNEL rather than in a path, so chat, crew and flow all inherit it from
one edit — the same reason the security preamble lives beside this.

Opens its OWN database session. ``build_agent`` has no session, and more to the
point this runs inside the spawned crew interpreter too, where the caller's
session does not exist. An isolated session is the only thing that behaves the
same on both sides of that boundary.
"""

from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager

#: The CREW logger, not ``logging.getLogger(__name__)``. The subprocess forwards
#: only LoggerManager's crew records to execution logs, so a module logger here
#: writes somewhere nobody reading a failed run will look — which is exactly how
#: "did my skill attach?" stayed unanswerable while the answer was being logged.
logger = LoggerManager.get_instance().crew


def skill_names_of(spec: Dict[str, Any]) -> List[str]:
    """The skills an agent spec asks for.

    Tolerates a list of names or a list of objects, because the frontend sends
    names and generation sends rows, and a helper that handled only one would
    fail on exactly the path with no test.
    """
    raw = spec.get("skills") or []
    if isinstance(raw, str):
        raw = [raw]
    names: List[str] = []
    for entry in raw:
        if isinstance(entry, str):
            names.append(entry)
        elif isinstance(entry, dict) and entry.get("name"):
            names.append(str(entry["name"]))
    return names


async def inject_skills(
    agent_kwargs: Dict[str, Any],
    spec: Dict[str, Any],
    *,
    group_id: Optional[str],
    label: str = "",
) -> int:
    """Give this agent its skills. Returns how many were attached.

    Appends the ``<available_skills>`` block to whichever prompt field actually
    reaches the model — ``system_template`` when the agent has one, otherwise
    ``backstory``, which the default system prompt embeds. Same rule the security
    preamble follows, and for the same reason: a block written to a field the
    model never sees is indistinguishable from no skills at all.

    Never raises. A database it cannot reach costs the agent its skills, not its
    run.
    """
    names = skill_names_of(spec)
    group_ids = [group_id] if group_id else []

    try:
        from src.db.session import get_isolated_db_session
        from src.services.skills.injection import build_prompt_section
        from src.services.skills.loader import resolve_for_agent

        async with get_isolated_db_session() as session:
            skills = await resolve_for_agent(names, group_ids, session)
            section = build_prompt_section(skills)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[skills] not attached to agent '%s': %s", label, exc)
        return 0

    if not skills or not section:
        # Logged, because "is my skill actually being used?" was otherwise
        # unanswerable from the logs — silence looked identical whether the
        # agent had none attached or the attachment was dropped upstream.
        if names:
            logger.warning(
                "[skills] agent '%s' asked for %s but none resolved for group %s.",
                label,
                ", ".join(names),
                group_id,
            )
        else:
            logger.debug("[skills] agent '%s' has no skills attached.", label)
        return 0

    field = "system_template" if agent_kwargs.get("system_template") else "backstory"
    agent_kwargs[field] = (agent_kwargs.get(field) or "") + "\n\n" + section

    _add_skill_tools(agent_kwargs, group_ids)

    logger.info(
        "[skills] attached %d skill(s) to agent '%s' via %s: %s",
        len(skills),
        label,
        field,
        ", ".join(s.name for s in skills),
    )
    return len(skills)


def _add_skill_tools(agent_kwargs: Dict[str, Any], group_ids: List[str]) -> None:
    """Equip the tier-2/3 tools, once.

    Guarded against duplicates because an agent that already carries them —
    from a path that resolved tools itself — would otherwise get two
    ``load_skill`` tools, and a duplicate tool name is how a tool-calling loop
    starts behaving unpredictably.
    """
    try:
        from src.services.tools.skill_tools import build_skill_tools

        tools = list(agent_kwargs.get("tools") or [])
        present = {getattr(t, "name", "") for t in tools}
        for tool in build_skill_tools(group_ids=group_ids):
            if tool.name not in present:
                tools.append(tool)
        agent_kwargs["tools"] = tools
    except Exception as exc:  # noqa: BLE001
        logger.warning("[skills] could not equip the skill tools: %s", exc)
