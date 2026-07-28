"""Managing Agent Skills.

Policy lives here: who may author, group scoping, and what happens to content
that arrives from outside.

**Ownership differs from remote agents on purpose.** A remote A2A agent is a URL
and a credential — system administration, so only a Kasal admin registers one. A
skill is domain KNOWLEDGE: "how we price a deal", "what our QBR needs". Routing
every workspace's own procedure through a Kasal admin would make the feature
useless, so a workspace authors its own skills, and Kasal ships builtins
(``group_id IS NULL``) that every workspace can use. A workspace skill of the
same name overrides the builtin, which is what a workspace means by writing one.
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.skill_repository import SkillRepository
from src.schemas.skill import SkillCreate, SkillUpdate, SkillValidationResult
from src.services.skills import packaging, parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class SkillService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = SkillRepository(session)

    async def list_skills(self, group_context: GroupContext) -> List[Any]:
        return await self.repository.list_visible(group_context.group_ids or [])

    async def get_skill(self, skill_id: int, group_context: GroupContext) -> Any:
        return await self.repository.find_visible(
            skill_id, group_context.group_ids or []
        )

    async def create_skill(
        self,
        data: SkillCreate,
        group_context: GroupContext,
        source: str = "authored",
        files: Optional[List[Dict[str, Any]]] = None,
    ) -> Any:
        group_id = self._group_of(group_context)
        parsed = self._validate(data.name, data.description, data.body, data)

        if await self._own_skill_named(parsed.name, group_id):
            raise ValueError(
                f"This workspace already has a skill named '{parsed.name}'."
            )

        from src.models.skill import Skill

        skill = Skill(
            name=parsed.name,
            description=parsed.description,
            body=parsed.body,
            license=parsed.license,
            compatibility=parsed.compatibility,
            skill_metadata=parsed.metadata or {},
            source=source,
            group_id=group_id,
            created_by_email=getattr(group_context, "group_email", None),
            enabled=data.enabled,
            global_enabled=data.global_enabled,
        )
        self.session.add(skill)
        await self.session.flush()

        supplied = files if files is not None else self._files_of(data)
        if supplied:
            await self.repository.replace_files(skill.id, supplied)
        return await self.repository.find_visible(
            skill.id, group_context.group_ids or []
        )

    async def update_skill(
        self, skill_id: int, data: SkillUpdate, group_context: GroupContext
    ) -> Optional[Any]:
        """Edit a skill — including a builtin.

        Editing a builtin writes the change as THIS workspace's own row rather
        than touching the shared one. To the user that is invisible and should
        be: they clicked edit, they edited, it saved. Underneath, one tenant's
        wording cannot reach another's, and the next seed run cannot silently
        undo their work — which is what editing the shared row in place would
        cost, the same way prompt templates are re-seeded on every startup.

        ``reset_skill`` puts the shipped version back by removing the override.
        """
        skill = await self.get_skill(skill_id, group_context)
        if not skill:
            return None

        if skill.group_id is None:
            skill = await self._own_copy_of(skill, group_context)

        merged = {
            "name": data.name if data.name is not None else skill.name,
            "description": (
                data.description if data.description is not None else skill.description
            ),
            "body": data.body if data.body is not None else (skill.body or ""),
            "license": data.license if data.license is not None else skill.license,
            "compatibility": (
                data.compatibility
                if data.compatibility is not None
                else skill.compatibility
            ),
            "metadata": (
                data.metadata if data.metadata is not None else skill.skill_metadata
            ),
        }
        parsed = parser.validate_row(
            merged["name"],
            merged["description"],
            merged["body"],
            merged["license"],
            merged["compatibility"],
            merged["metadata"],
        )

        skill.name = parsed.name
        skill.description = parsed.description
        skill.body = parsed.body
        skill.license = parsed.license
        skill.compatibility = parsed.compatibility
        skill.skill_metadata = parsed.metadata or {}
        if data.enabled is not None:
            skill.enabled = data.enabled
        if data.global_enabled is not None:
            skill.global_enabled = data.global_enabled

        if data.files is not None:
            # Replaced wholesale, so removing a reference file in the editor
            # actually removes it — otherwise the body stops mentioning a file
            # the model can still read.
            await self.repository.replace_files(skill.id, self._files_of(data))

        await self.session.flush()
        # Same reason: replace_files went round the ORM, so this instance's
        # collection is stale or unloaded.
        return (
            await self.repository.find_visible(skill.id, group_context.group_ids or [])
            or skill
        )

    @staticmethod
    def _files_of(data: Any) -> List[Dict[str, Any]]:
        """Validate and normalise the files an editor sent.

        Paths go through the loader's own check rather than a second
        implementation: what the editor can write must be exactly what the
        agent can read, or a file is saved and then unreachable.
        """
        from src.services.skills.loader import normalise_path

        out: List[Dict[str, Any]] = []
        for entry in getattr(data, "files", None) or []:
            content = entry.content or ""
            out.append(
                {
                    "path": normalise_path(entry.path),
                    "content": content,
                    "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                    "size_bytes": len(content.encode("utf-8")),
                }
            )
        return out

    async def reset_skill(self, skill_id: int, group_context: GroupContext) -> Any:
        """Put a builtin back to the version Kasal ships.

        Deletes this workspace's override; the shared row was never modified, so
        the shipped skill simply becomes visible again — and it is the CURRENT
        shipped version, including anything improved since the workspace edited
        it. Returns the builtin, or None when there was nothing to reset.
        """
        skill = await self.get_skill(skill_id, group_context)
        if not skill or skill.group_id is None:
            return None

        builtin = await self.repository.find_builtin_by_name(skill.name)
        if not builtin:
            # Not an override of anything — resetting would just delete their
            # skill, which is not what the word means.
            return None

        await self.session.delete(skill)
        await self.session.flush()
        return builtin

    async def delete_skill(self, skill_id: int, group_context: GroupContext) -> bool:
        skill = await self.get_skill(skill_id, group_context)
        if not skill or skill.group_id is None:
            return False
        await self.session.delete(skill)
        await self.session.flush()
        return True

    async def set_enabled(
        self, skill_id: int, enabled: bool, group_context: GroupContext
    ) -> Optional[Any]:
        """Turn a skill on or off for this workspace.

        A builtin cannot be toggled in place for one tenant — the row is shared —
        so turning one off clones it disabled into the workspace, the same
        override move MCP servers use.
        """
        skill = await self.get_skill(skill_id, group_context)
        if not skill:
            return None

        if skill.group_id is not None:
            skill.enabled = enabled
            await self.session.flush()
            return skill

        group_id = self._group_of(group_context)
        existing = await self._own_skill_named(skill.name, group_id)
        if existing:
            existing.enabled = enabled
            await self.session.flush()
            return existing

        from src.models.skill import Skill

        override = Skill(
            name=skill.name,
            description=skill.description,
            body=skill.body,
            license=skill.license,
            compatibility=skill.compatibility,
            skill_metadata=skill.skill_metadata or {},
            source=skill.source,
            group_id=group_id,
            created_by_email=getattr(group_context, "group_email", None),
            enabled=enabled,
            global_enabled=bool(skill.global_enabled),
        )
        self.session.add(override)
        await self.session.flush()
        await self.repository.replace_files(
            override.id,
            [
                {
                    "path": f.path,
                    "content": f.content,
                    "sha256": f.sha256,
                    "size_bytes": f.size_bytes,
                }
                for f in (skill.files or [])
            ],
        )
        return await self.repository.find_visible(override.id, [group_id]) or override

    async def import_zip(
        self, data: bytes, group_context: GroupContext, replace: bool = False
    ) -> Any:
        """Ingest an uploaded skill folder.

        ``source='uploaded'``, which is not bookkeeping: uploaded content is
        untrusted text headed for a system prompt, and the marker is what lets
        the injection guardrail and any future review treat it accordingly.
        """
        parsed, files = packaging.read_zip(data)
        group_id = self._group_of(group_context)

        existing = await self._own_skill_named(parsed.name, group_id)
        if existing and not replace:
            raise ValueError(
                f"This workspace already has a skill named '{parsed.name}'. "
                "Re-upload with replace=true to overwrite it."
            )

        if existing:
            existing.description = parsed.description
            existing.body = parsed.body
            existing.license = parsed.license
            existing.compatibility = parsed.compatibility
            existing.skill_metadata = parsed.metadata or {}
            existing.source = "uploaded"
            await self.session.flush()
            await self.repository.replace_files(existing.id, files)
            return await self.repository.find_visible(
                existing.id, group_context.group_ids or []
            )

        return await self.create_skill(
            SkillCreate(
                name=parsed.name,
                description=parsed.description,
                body=parsed.body,
                license=parsed.license,
                compatibility=parsed.compatibility,
                metadata=parsed.metadata or {},
            ),
            group_context,
            source="uploaded",
            files=files,
        )

    async def export_zip(
        self, skill_id: int, group_context: GroupContext
    ) -> Optional[bytes]:
        skill = await self.get_skill(skill_id, group_context)
        if not skill:
            return None
        return packaging.write_zip(skill)

    @staticmethod
    def validate(data: SkillCreate) -> SkillValidationResult:
        """Check a skill without saving it, for the authoring UI.

        Returns the reference validator's own messages. An author fixing a skill
        needs the wording the rest of the ecosystem uses, so searching for it
        finds the spec rather than this codebase.
        """
        try:
            parsed = parser.validate_row(
                data.name,
                data.description,
                data.body,
                data.license,
                data.compatibility,
                data.metadata,
            )
        except parser.SkillValidationError as exc:
            return SkillValidationResult(valid=False, errors=exc.errors)
        return SkillValidationResult(valid=True, warnings=parsed.warnings)

    def _validate(self, name, description, body, data) -> parser.ParsedSkill:
        return parser.validate_row(
            name, description, body, data.license, data.compatibility, data.metadata
        )

    async def _own_copy_of(self, builtin: Any, group_context: GroupContext) -> Any:
        """This workspace's row for a builtin, creating it if needed."""
        group_id = self._group_of(group_context)
        existing = await self._own_skill_named(builtin.name, group_id)
        if existing:
            return existing

        from src.models.skill import Skill

        copy = Skill(
            name=builtin.name,
            description=builtin.description,
            body=builtin.body,
            license=builtin.license,
            compatibility=builtin.compatibility,
            skill_metadata=dict(builtin.skill_metadata or {}),
            source=builtin.source,
            group_id=group_id,
            created_by_email=getattr(group_context, "group_email", None),
            enabled=bool(builtin.enabled),
            global_enabled=bool(builtin.global_enabled),
        )
        self.session.add(copy)
        await self.session.flush()
        await self.repository.replace_files(
            copy.id,
            [
                {
                    "path": f.path,
                    "content": f.content,
                    "sha256": f.sha256,
                    "size_bytes": f.size_bytes,
                }
                for f in (builtin.files or [])
            ],
        )
        # Re-read rather than return the instance: a row created in THIS session
        # has no loaded `files`, and the router touches that relationship to
        # render the response. Outside a greenlet that raises MissingGreenlet —
        # which surfaces as a 500 on an otherwise successful save.
        return await self.repository.find_visible(copy.id, [group_id]) or copy

    async def _own_skill_named(self, name: str, group_id: Optional[str]) -> Any:
        if not group_id:
            return None
        found = await self.repository.find_by_name(name, [group_id])
        return found if found is not None and found.group_id == group_id else None

    @staticmethod
    def _group_of(group_context: GroupContext) -> str:
        group_id = group_context.primary_group_id or (
            group_context.group_ids[0] if group_context.group_ids else None
        )
        if not group_id:
            raise ValueError("A workspace is required to author a skill.")
        return group_id
