"""Tier 1 — telling an agent which skills exist.

Only ``name`` and ``description`` go into the prompt, for every enabled skill.
That is the entire point of the format: at roughly 100 tokens each, twenty
skills cost ~2k tokens and the bodies stay out of context until one is actually
needed.

The block is the ``<available_skills>`` XML the reference library emits, not a
Kasal-shaped list. Anthropic's models were trained against that shape, and a
skill's odds of being activated depend on the model recognising the block —
inventing a prettier format here would trade activation quality for nothing.
``skills-ref.to_prompt`` builds it from DIRECTORIES; Kasal builds it from rows,
and a test pins the two outputs together so this cannot drift.
"""

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

#: Above this, discovery stops being cheap: two hundred skills is 20k tokens
#: spent before the task starts, and it collides with context-window trimming.
#: Truncation is logged rather than silent — a skill that never appears looks
#: exactly like a skill the model ignored.
MAX_SKILLS_IN_PROMPT = 40


def build_skill_block(skills: List[Any]) -> str:
    """The ``<available_skills>`` block, or an empty string.

    Empty rather than a "no skills available" note: a sentence explaining an
    absence is pure cost in every prompt of every agent that has no skills,
    which is most of them.
    """
    if not skills:
        return ""

    chosen = skills[:MAX_SKILLS_IN_PROMPT]
    if len(skills) > len(chosen):
        logger.warning(
            "[skills] %d skills attached; only the first %d are advertised. "
            "The rest are invisible to the model.",
            len(skills),
            MAX_SKILLS_IN_PROMPT,
        )

    # Values on their own lines, matching skills-ref.to_prompt exactly. It looks
    # verbose; it is what the models were trained against, and a test pins the
    # two outputs together so a tidier layout cannot be introduced by accident.
    lines = ["<available_skills>"]
    for skill in chosen:
        lines += [
            "<skill>",
            "<name>",
            _escape(_attr(skill, "name")),
            "</name>",
            "<description>",
            _escape(_attr(skill, "description")),
            "</description>",
            "</skill>",
        ]
    lines.append("</available_skills>")
    return "\n".join(lines)


def build_prompt_section(skills: List[Any]) -> str:
    """The block plus the one instruction that makes it actionable.

    Without the instruction the model has a list and no idea what to do with it.
    The wording names the tool because activation is tool-side here: the crew
    path is already a tool-calling loop, its round cap already bounds the
    behaviour, and a tool call shows up in the trace — so "did this skill
    activate?" is answerable after the fact rather than a guess.
    """
    block = build_skill_block(skills)
    if not block:
        return ""
    return (
        f"{block}\n"
        "If one of these skills applies to the task, call load_skill with its "
        "name to read its full instructions BEFORE doing the work, and follow "
        "them. The list above is only a summary — never act on a skill you have "
        "not loaded."
    )


def _attr(skill: Any, field: str) -> str:
    """Works for a model row or a plain dict.

    The kernel builds agents from both — persisted rows in a crew run, dicts in
    generation — and a helper that handled only one would fail on exactly the
    path that has no test.
    """
    if isinstance(skill, dict):
        return str(skill.get(field) or "")
    return str(getattr(skill, field, "") or "")


def _escape(value: str) -> str:
    """XML-escape a field.

    A description containing ``&`` or ``<`` is ordinary English. Unescaped, it
    breaks the block for every skill after it, so one author's ampersand would
    silently hide their colleagues' skills.
    """
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").strip()


async def build_for_agent(
    skill_names: Optional[List[str]], group_ids: List[str], session: Any
) -> str:
    """Resolve an agent's skills and render its prompt section.

    Never raises: a database that cannot be reached costs the agent its skills,
    not its run.
    """
    if not session:
        return ""
    try:
        from src.services.skills.loader import resolve_for_agent

        skills = await resolve_for_agent(skill_names, group_ids, session)
        return build_prompt_section(skills)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[skills] could not build the skill block: %s", exc)
        return ""
