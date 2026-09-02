"""Seed Kasal's builtin Agent Skills.

Builtins are ``group_id IS NULL`` — visible to every workspace, editable by
none. A workspace that wants a different version authors its own skill with the
same name, which the resolver prefers; that is the override path, and it is why
this seeder can safely overwrite its own rows.

**Validated before insertion, through the same reference validator the API
uses.** A builtin that does not conform would export to something no other
Agent Skills client accepts, and shipping that is worse than shipping nothing —
it would look like the format is broken rather than Kasal's seed data.
"""

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory
from src.models.skill import Skill, SkillFile
from src.seeds.skills_data import BUILTIN_SKILLS

logger = logging.getLogger(__name__)


async def _upsert(session: AsyncSession, entry: Dict[str, Any]) -> str:
    """Insert or refresh one builtin. Returns what happened, for the log."""
    result = await session.execute(
        select(Skill).where(Skill.name == entry["name"], Skill.group_id.is_(None))
    )
    existing = result.scalars().first()

    fields = {
        "description": entry["description"],
        "body": entry["body"],
        "license": entry.get("license"),
        "compatibility": entry.get("compatibility"),
        "skill_metadata": entry.get("metadata") or {"source": "kasal"},
        "source": "builtin",
    }

    if existing:
        # Overwritten on every run, like the prompt templates: a builtin is
        # Kasal's content, and a shipped improvement should reach installs
        # without anyone re-importing anything. A workspace that disagreed has
        # its own row, which this never touches.
        changed = any(getattr(existing, k) != v for k, v in fields.items())
        for key, value in fields.items():
            setattr(existing, key, value)
        existing.updated_at = datetime.utcnow()
        await _replace_files(session, existing.id, entry.get("files") or [])
        return "updated" if changed else "unchanged"

    skill = Skill(
        name=entry["name"],
        group_id=None,
        enabled=True,
        # NOT global_enabled: a builtin that attached itself to every agent
        # would spend tier-1 tokens in every prompt in the product, whether
        # or not anyone wanted it. Users attach the ones they want.
        global_enabled=False,
        **fields,
    )
    session.add(skill)
    await session.flush()
    await _replace_files(session, skill.id, entry.get("files") or [])
    return "created"


async def _replace_files(
    session: AsyncSession, skill_id: int, files: List[Dict[str, str]]
) -> None:
    """Set a builtin's bundled files to exactly this set.

    Replace rather than merge, so a reference file dropped from a shipped skill
    actually disappears — otherwise the body stops mentioning it while the model
    can still read it.
    """
    await session.execute(delete(SkillFile).where(SkillFile.skill_id == skill_id))
    for entry in files:
        content = entry["content"]
        session.add(
            SkillFile(
                skill_id=skill_id,
                path=entry["path"],
                content=content,
                sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                size_bytes=len(content.encode("utf-8")),
            )
        )


async def seed() -> None:
    """Seed the builtin skills, validating each one first."""
    from src.services.skills import parser

    created = updated = unchanged = 0

    async with async_session_factory() as session:
        for entry in BUILTIN_SKILLS:
            try:
                parser.validate_row(
                    entry["name"],
                    entry["description"],
                    entry["body"],
                    entry.get("license"),
                    entry.get("compatibility"),
                    entry.get("metadata"),
                )
            except Exception as exc:  # noqa: BLE001
                # Skipped rather than fatal: one malformed builtin must not stop
                # startup, and the log names it precisely enough to fix.
                logger.error(
                    "Builtin skill '%s' does not conform and was NOT seeded: %s",
                    entry.get("name"),
                    exc,
                )
                continue

            outcome = await _upsert(session, entry)
            created += outcome == "created"
            updated += outcome == "updated"
            unchanged += outcome == "unchanged"

        # A builtin that no longer ships must not linger as a phantom row: it
        # would keep appearing in every picker with content nobody maintains.
        # Only rows the seeder itself created (builtin source, no group).
        shipped = [e["name"] for e in BUILTIN_SKILLS]
        stale_ids = (
            (
                await session.execute(
                    select(Skill.id).where(
                        Skill.group_id.is_(None),
                        Skill.source == "builtin",
                        Skill.name.notin_(shipped),
                    )
                )
            )
            .scalars()
            .all()
        )
        if stale_ids:
            await session.execute(
                delete(SkillFile).where(SkillFile.skill_id.in_(stale_ids))
            )
            await session.execute(delete(Skill).where(Skill.id.in_(stale_ids)))
            logger.info(
                "Removed %d builtin skill(s) that no longer ship", len(stale_ids)
            )
        await session.commit()

    logger.info(
        "Seeded builtin skills: %d created, %d updated, %d unchanged",
        created,
        updated,
        unchanged,
    )
