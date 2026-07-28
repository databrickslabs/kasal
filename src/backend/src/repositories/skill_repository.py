from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.core.base_repository import BaseRepository
from src.models.skill import Skill, SkillFile


class SkillRepository(BaseRepository[Skill]):
    """Data access for Agent Skills.

    A skill can hold proprietary procedure — "how we price a deal here" — so
    every read takes a group. The one exception is the builtin catalogue
    (``group_id IS NULL``), which is Kasal's own content and available to all.

    Files are loaded with ``selectinload`` rather than lazily: a skill row is
    read inside async request handlers, and a lazy relationship access on an
    expired instance raises ``MissingGreenlet`` at the point of use rather than
    at the query — a failure that reads as "the skill has no files".
    """

    def __init__(self, session: AsyncSession):
        super().__init__(Skill, session)

    def _with_files(self):
        return select(self.model).options(selectinload(self.model.files))

    async def list_visible(self, group_ids: List[str]) -> List[Skill]:
        """Everything this workspace may see: builtins plus its own."""
        query = self._with_files().order_by(self.model.name)
        if group_ids:
            query = query.where(
                (self.model.group_id.is_(None)) | (self.model.group_id.in_(group_ids))
            )
        else:
            query = query.where(self.model.group_id.is_(None))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def list_enabled(self, group_ids: List[str]) -> List[Skill]:
        """The skills an agent run may actually use.

        Disabled rows are filtered in SQL rather than skipped later, so a skill
        an admin turned off cannot reach a prompt through a stale agent config.
        """
        query = self._with_files().where(self.model.enabled.is_(True))
        if group_ids:
            query = query.where(
                (self.model.group_id.is_(None)) | (self.model.group_id.in_(group_ids))
            )
        else:
            query = query.where(self.model.group_id.is_(None))
        result = await self.session.execute(query.order_by(self.model.name))
        return list(result.scalars().all())

    async def find_by_name(self, name: str, group_ids: List[str]) -> Optional[Skill]:
        """One skill by name, preferring the workspace's own over a builtin.

        A workspace that authors a skill named like a builtin means to override
        it; resolving to the builtin instead would silently ignore their
        version. Ordering puts group-owned rows first.
        """
        query = self._with_files().where(self.model.name == name)
        if group_ids:
            query = query.where(
                (self.model.group_id.is_(None)) | (self.model.group_id.in_(group_ids))
            )
        else:
            query = query.where(self.model.group_id.is_(None))
        result = await self.session.execute(
            query.order_by(self.model.group_id.is_(None))
        )
        return result.scalars().first()

    async def find_visible(
        self, skill_id: int, group_ids: List[str]
    ) -> Optional[Skill]:
        query = self._with_files().where(self.model.id == skill_id)
        if group_ids:
            query = query.where(
                (self.model.group_id.is_(None)) | (self.model.group_id.in_(group_ids))
            )
        else:
            query = query.where(self.model.group_id.is_(None))
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_builtin_by_name(self, name: str) -> Optional[Skill]:
        """The shipped row for a name, ignoring any workspace override.

        Used by reset, which needs the version Kasal ships rather than the one
        the resolver would prefer.
        """
        result = await self.session.execute(
            self._with_files().where(
                self.model.name == name, self.model.group_id.is_(None)
            )
        )
        return result.scalars().first()

    async def find_by_names(
        self, names: List[str], group_ids: List[str]
    ) -> List[Skill]:
        """Resolve an agent's attached skill names in one query.

        One query rather than one per name: an agent with eight skills should
        not cost eight round trips every time it is built.
        """
        if not names:
            return []
        query = self._with_files().where(self.model.name.in_(names))
        if group_ids:
            query = query.where(
                (self.model.group_id.is_(None)) | (self.model.group_id.in_(group_ids))
            )
        else:
            query = query.where(self.model.group_id.is_(None))
        result = await self.session.execute(query)
        rows = list(result.scalars().all())

        # A workspace's own row wins over a builtin of the same name — same rule
        # as find_by_name, applied across the batch.
        by_name: dict = {}
        for row in rows:
            existing = by_name.get(row.name)
            if existing is None or (existing.group_id is None and row.group_id):
                by_name[row.name] = row
        return [by_name[n] for n in names if n in by_name]

    async def replace_files(self, skill_id: int, files: List[dict]) -> None:
        """Set a skill's bundled files to exactly this set.

        Replace rather than merge: an edit that removes a reference file must
        actually remove it, or the model keeps being told to read something the
        author deleted.
        """
        await self.session.execute(
            delete(SkillFile).where(SkillFile.skill_id == skill_id)
        )
        for entry in files:
            self.session.add(SkillFile(skill_id=skill_id, **entry))
        await self.session.flush()
