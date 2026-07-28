"""The tools that make progressive disclosure work.

Activation is TOOL-side rather than prompt-side, and that was the real design
choice. The alternative — tell the model "say so if a skill applies" and inject
the body on the next turn — costs an extra round trip and depends on the model
remembering to signal. Kasal's crew path is already a tool-calling loop with a
round cap, and every tool call is already traced, so doing it as a tool makes
activation bounded and VISIBLE: "did this skill actually fire?" becomes a
question the trace answers instead of a guess.

Both tools resolve against the caller's own workspace, taken from the request
context rather than from an argument. A skill name supplied by a model must
never be able to reach another tenant's content.
"""

import logging
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from .async_bridge import run_async_with_context
from .base import BaseTool

logger = logging.getLogger(__name__)


def _group_ids(explicit: Optional[List[str]] = None) -> List[str]:
    """Whose skills this tool may read.

    Prefers the live request context over anything passed in at construction:
    the constructor value is a snapshot from when the run was queued, and a tool
    built later must not reach into a workspace on the strength of a stale one.
    """
    try:
        from src.utils.user_context import UserContext

        group_context = UserContext.get_group_context()
        if group_context and group_context.group_ids:
            return list(group_context.group_ids)
    except Exception:  # noqa: BLE001
        pass
    return list(explicit or [])


async def _with_session(coro_factory):
    from src.db.session import get_isolated_db_session

    async with get_isolated_db_session() as session:
        return await coro_factory(session)


class LoadSkillSchema(BaseModel):
    skill_name: str = Field(
        ...,
        description="The name of the skill to load, exactly as listed in available_skills.",
    )


class LoadSkillTool(BaseTool):
    """Tier 2 — read a skill's full instructions."""

    name: str = "load_skill"
    description: str = (
        "Load the full instructions for one of the skills listed in "
        "available_skills. Call this BEFORE doing work the skill covers, then "
        "follow what it says. The summary in available_skills is not enough to "
        "act on."
    )
    args_schema: type[BaseModel] = LoadSkillSchema
    group_ids: List[str] = Field(default_factory=list)

    def __init__(self, group_ids: Optional[List[str]] = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.group_ids = list(group_ids or [])

    def _run(self, **kwargs: Any) -> str:
        name = (kwargs.get("skill_name") or "").strip()
        if not name:
            return "Error: name the skill to load."

        try:
            from src.services.skills import loader

            result = run_async_with_context(
                _with_session(
                    lambda s: loader.load_skill(name, _group_ids(self.group_ids), s)
                ),
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            # Returned, not raised: a missing skill is something the agent can
            # work around, and a raise would abort the whole task instead.
            logger.warning("[skills] load_skill('%s') failed: %s", name, exc)
            return f"Could not load skill '{name}': {exc}"

        parts = [
            f"# Skill: {result['name']}",
            "",
            result["body"] or "(no instructions)",
        ]
        if result["files"]:
            parts += [
                "",
                "Files bundled with this skill (read one with read_skill_file):",
                *(f"- {p}" for p in result["files"]),
            ]
        return "\n".join(parts)


class ReadSkillFileSchema(BaseModel):
    skill_name: str = Field(..., description="The skill the file belongs to.")
    path: str = Field(
        ...,
        description=(
            "Path relative to the skill, as listed by load_skill — for example "
            "'references/pricing.md'."
        ),
    )


class ReadSkillFileTool(BaseTool):
    """Tier 3 — read one file a skill bundles."""

    name: str = "read_skill_file"
    description: str = (
        "Read a reference or asset file bundled with a skill, when that skill's "
        "instructions tell you to. Use the paths load_skill listed."
    )
    args_schema: type[BaseModel] = ReadSkillFileSchema
    group_ids: List[str] = Field(default_factory=list)

    def __init__(self, group_ids: Optional[List[str]] = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.group_ids = list(group_ids or [])

    def _run(self, **kwargs: Any) -> str:
        name = (kwargs.get("skill_name") or "").strip()
        path = (kwargs.get("path") or "").strip()
        if not name or not path:
            return "Error: name both the skill and the file to read."

        try:
            from src.services.skills import loader

            result = run_async_with_context(
                _with_session(
                    lambda s: loader.read_file(
                        name, path, _group_ids(self.group_ids), s
                    )
                ),
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[skills] read_skill_file('%s','%s'): %s", name, path, exc)
            return f"Could not read '{path}' from skill '{name}': {exc}"

        return f"# {result['skill']}/{result['path']}\n\n{result['content']}"


def build_skill_tools(group_ids: Optional[List[str]] = None) -> List[Any]:
    """Both tiers, for an agent that has at least one skill.

    Equipped together because they are one mechanism: a skill body that says
    "see references/pricing.md" is a dead end if the agent can load the skill
    but not its files.
    """
    return [LoadSkillTool(group_ids=group_ids), ReadSkillFileTool(group_ids=group_ids)]
