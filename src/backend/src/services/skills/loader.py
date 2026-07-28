"""Tiers 2 and 3 — activating a skill, and reading its bundled files.

Progressive disclosure is the whole mechanism, so it is worth being explicit
about what each tier costs:

- **Tier 1** (``injection.py``): name + description for every enabled skill,
  always in the prompt. ~100 tokens each.
- **Tier 2** (here): the full body, only when the model asks for it.
- **Tier 3** (here): a bundled file, only when the instructions say to read it.

The security boundary lives in this module. A skill path arrives from a MODEL,
which may have been steered by the skill's own text, so ``read_file`` treats it
as hostile input: the spec's one-level-deep rule is enforced, and a path is
resolved against the stored set rather than against a filesystem — there is no
directory to escape from, which removes the traversal class rather than
defending against it.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: Bundled files a skill may carry. ``scripts/`` is absent on purpose: running
#: bundled code needs a sandbox and an approval model, and being able to READ a
#: script is the first half of being able to run one.
ALLOWED_PREFIXES = ("references/", "assets/")


class SkillNotFound(LookupError):
    """No such skill, or not one this workspace may see.

    One error for both, so a skill name cannot be probed to learn what another
    workspace has authored.
    """


class SkillFileNotFound(LookupError):
    pass


def normalise_path(path: str) -> str:
    """A bundled-file path, or a refusal.

    Rejects absolute paths, ``..`` segments, backslashes and anything outside
    ``references/`` or ``assets/``. The spec also keeps files one level deep,
    which is enforced here rather than merely documented.
    """
    raw = (path or "").strip().replace("\\", "/")
    if not raw:
        raise SkillFileNotFound("No file path given.")
    if raw.startswith("/") or ":" in raw.split("/")[0]:
        raise SkillFileNotFound(f"'{path}' must be relative to the skill.")
    parts = [p for p in raw.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise SkillFileNotFound(f"'{path}' may not traverse outside the skill.")
    cleaned = "/".join(parts)
    if not cleaned.startswith(ALLOWED_PREFIXES):
        raise SkillFileNotFound(
            f"'{path}' is not readable. Skill files live under "
            + " or ".join(ALLOWED_PREFIXES)
            + "."
        )
    if len(parts) > 2:
        raise SkillFileNotFound(
            f"'{path}' is nested too deep. Skill files are one level under "
            + " or ".join(ALLOWED_PREFIXES)
            + "."
        )
    return cleaned


async def load_skill(name: str, group_ids: List[str], session: Any) -> Dict[str, Any]:
    """Tier 2: the full body of one skill.

    Returns the file list alongside it. A body that says "see
    references/pricing.md" is useless if the model has to guess what exists, and
    listing costs nothing — the rows are already loaded.
    """
    from src.repositories.skill_repository import SkillRepository

    skill = await SkillRepository(session).find_by_name(name, group_ids)
    if not skill or not skill.enabled:
        raise SkillNotFound(f"No skill named '{name}' is available here.")

    return {
        "name": skill.name,
        "description": skill.description,
        "body": skill.body or "",
        "files": sorted(f.path for f in (skill.files or [])),
    }


async def read_file(
    name: str, path: str, group_ids: List[str], session: Any
) -> Dict[str, Any]:
    """Tier 3: one bundled file, by skill and relative path."""
    from src.repositories.skill_repository import SkillRepository

    wanted = normalise_path(path)
    skill = await SkillRepository(session).find_by_name(name, group_ids)
    if not skill or not skill.enabled:
        raise SkillNotFound(f"No skill named '{name}' is available here.")

    for stored in skill.files or []:
        if stored.path == wanted:
            return {"skill": skill.name, "path": stored.path, "content": stored.content}

    available = sorted(f.path for f in (skill.files or []))
    raise SkillFileNotFound(
        f"'{skill.name}' has no file '{wanted}'."
        + (
            f" It has: {', '.join(available)}."
            if available
            else " It bundles no files."
        )
    )


async def resolve_for_agent(
    names: Optional[List[str]], group_ids: List[str], session: Any
) -> List[Any]:
    """The skills one agent should see at tier 1.

    Named skills, PLUS every globally-enabled one — the same meaning
    ``global_enabled`` carries for a tool or an MCP server, so an admin who
    turned a skill on for everyone does not have to also attach it everywhere.
    """
    from src.repositories.skill_repository import SkillRepository

    repository = SkillRepository(session)
    enabled = await repository.list_enabled(group_ids)

    wanted = {n for n in (names or [])}
    chosen = [s for s in enabled if s.name in wanted or s.global_enabled]

    missing = wanted - {s.name for s in chosen}
    if missing:
        # Logged, not raised: an agent referencing a skill that was deleted or
        # disabled should still run, minus that skill.
        logger.warning(
            "[skills] agent references unavailable skill(s): %s", ", ".join(missing)
        )
    return chosen
