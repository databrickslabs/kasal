"""Sync Agent Skills between Kasal and Unity Catalog Skills (beta).

UC models a skill as a securable (``/api/2.1/unity-catalog/skills``) whose CONTENT
lives in the Files API under ``Skills/{catalog}/{schema}/{skill_id}/`` — the
top-level securable stores only ``comment``. A published skill is therefore a
three-step lifecycle:

    create securable  ->  PUT SKILL.md (+ bundle files)  ->  finalize

``finalize`` parses SKILL.md's front-matter and stamps ``bundle_name`` +
``description`` onto the securable, so the round-trip is: Kasal columns
(name/description/body/files) -> ``to_skill_md`` -> Files API -> finalize.

This is a CAPABILITY in the skills domain, not a CRUD service: it holds no
session of its own, reads Kasal skills through ``SkillService``, and borrows the
OBO→PAT→SPN auth + workspace host from the databricks domain's service
(``DatabricksService.get_workspace_auth``) rather than re-deriving either.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.core.exceptions import BadRequestError, KasalError, NotFoundError
from src.services.skills import parser
from src.services.skills.service import SkillService
from src.utils.telemetry import KasalProduct, get_user_agent_header
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

#: UC rejects these in a skill_id; Kasal names are kebab-case so this only ever
#: catches a malformed row, but a clear 400 beats a raw UC parse error.
_INVALID_SKILL_ID = re.compile(r"[.\s/%]|[\x00-\x1f]")

_SKILLS_API = "/api/2.1/unity-catalog/skills"
_FILES_API = "/api/2.0/fs/files/Skills"
_DIRS_API = "/api/2.0/fs/directories/Skills"

_TIMEOUT = 60.0


class SkillUcSyncService:
    """Push Kasal skills to UC, list UC skills, and (import) pull them back."""

    def __init__(
        self,
        session: Any,
        group_context: GroupContext,
        user_token: Optional[str] = None,
    ):
        self._session = session
        self._group_context = group_context
        self._user_token = user_token
        self._skills = SkillService(session)

    # ── auth/host, borrowed from the databricks domain ──────────────────────

    async def _auth(self) -> Tuple[Dict[str, str], str]:
        """(headers, workspace_url) for the UC + Files APIs, OBO-first."""
        from src.services.databricks.workspace.service import DatabricksService

        group_id = self._group_context.primary_group_id if self._group_context else None
        svc = DatabricksService(
            self._session, group_id=group_id, user_token=self._user_token
        )
        headers, host = await svc.get_workspace_auth()
        headers = {**headers, **get_user_agent_header(KasalProduct.SKILL)}
        return headers, host

    # ── push: Kasal -> UC ───────────────────────────────────────────────────

    async def push_skill(
        self, skill_id: int, catalog: str, schema: str
    ) -> Dict[str, Any]:
        """Publish one Kasal skill into ``catalog.schema`` as a UC skill.

        Idempotent: re-pushing an existing skill re-uploads its files and
        re-finalizes (create is tolerated as already-existing), so it doubles as
        an update without dropping the securable's grants.
        """
        skill = await self._skills.get_skill(skill_id, self._group_context)
        if not skill:
            raise NotFoundError(f"Skill {skill_id} not found in this workspace.")

        headers, host = await self._auth()
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            return await self._push_one(client, headers, host, skill, catalog, schema)

    async def push_all_skills(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """Publish every skill visible to this workspace into ``catalog.schema``.

        One skill's failure must not abort the batch, so each is caught and
        recorded; the caller gets a per-skill ``{name, status, error?}`` summary
        rather than a single 500. Reuses ONE HTTP client + one auth resolution
        across the batch.
        """
        skills = await self._skills.list_skills(self._group_context)
        headers, host = await self._auth()
        results: List[Dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            for skill in skills:
                try:
                    await self._push_one(client, headers, host, skill, catalog, schema)
                    results.append({"name": skill.name, "status": "ok"})
                except (
                    Exception
                ) as exc:  # noqa: BLE001 - one failure can't stop the batch
                    results.append(
                        {
                            "name": skill.name,
                            "status": "error",
                            "error": getattr(exc, "detail", None) or str(exc),
                        }
                    )
        return results

    async def _push_one(
        self,
        client: httpx.AsyncClient,
        headers: Dict[str, str],
        host: str,
        skill: Any,
        catalog: str,
        schema: str,
    ) -> Dict[str, Any]:
        """The create→upload→finalize lifecycle for one already-loaded skill,
        on a caller-provided client so a batch reuses one connection pool."""
        name = skill.name
        if _INVALID_SKILL_ID.search(name or ""):
            raise BadRequestError(
                f"Skill name '{name}' is not a valid UC skill_id "
                "(no spaces, '.', '/', or '%')."
            )

        skill_md = parser.to_skill_md(
            name,
            skill.description,
            skill.body or "",
            getattr(skill, "license", None),
            getattr(skill, "compatibility", None),
            getattr(skill, "skill_metadata", None),
        )
        files = [
            (f.path, (f.content or "").encode("utf-8"))
            for f in (getattr(skill, "files", None) or [])
        ]
        fqn = f"{catalog}.{schema}.{name}"
        file_root = f"{host}{_FILES_API}/{catalog}/{schema}/{name}"

        # 1) create the securable (parent + skill_id are QUERY params — the
        #    documented JSON-body form is rejected with "parent is required").
        create = await client.post(
            f"{host}{_SKILLS_API}",
            headers={**headers, "Content-Type": "application/json"},
            params={"parent": f"schemas/{catalog}.{schema}", "skill_id": name},
            json={"comment": (skill.description or "")[:250]},
        )
        if create.status_code not in (200, 201) and not self._already_exists(create):
            self._raise("create skill", create)

        # 2) upload SKILL.md + every bundle file.
        await self._put_file(
            client, headers, f"{file_root}/SKILL.md", skill_md.encode("utf-8")
        )
        for path, content in files:
            await self._put_file(client, headers, f"{file_root}/{path}", content)

        # 3) finalize — parses SKILL.md front-matter onto the securable.
        final = await client.post(
            f"{host}{_SKILLS_API}/{fqn}/finalize", headers=headers
        )
        if final.status_code != 200:
            self._raise("finalize skill", final)
        return final.json()

    async def _put_file(
        self,
        client: httpx.AsyncClient,
        headers: Dict[str, str],
        url: str,
        content: bytes,
    ) -> None:
        resp = await client.put(
            url,
            headers={**headers, "Content-Type": "application/octet-stream"},
            content=content,
        )
        if resp.status_code not in (200, 204):
            self._raise("upload skill file", resp)

    # ── list: what's in UC ──────────────────────────────────────────────────

    async def list_uc_skills(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        headers, host = await self._auth()
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                f"{host}{_SKILLS_API}",
                headers=headers,
                params={"parent": f"schemas/{catalog}.{schema}"},
            )
        if resp.status_code != 200:
            self._raise("list UC skills", resp)
        return resp.json().get("skills", []) or []

    # ── import (pull): UC -> Kasal ───────────────────────────────────────────

    async def import_skill(self, catalog: str, schema: str, skill_id: str) -> Any:
        """Pull one UC skill into this workspace (download → parse → upsert)."""
        headers, host = await self._auth()
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            return await self._import_one(
                client, headers, host, catalog, schema, skill_id
            )

    async def import_all_skills(
        self, catalog: str, schema: str
    ) -> List[Dict[str, Any]]:
        """Pull every UC skill in ``catalog.schema`` into this workspace.

        Per-skill ``{name, status, error?}`` summary; one failure does not abort
        the batch. Reuses one HTTP client + auth across the pull.
        """
        skills = await self.list_uc_skills(catalog, schema)
        headers, host = await self._auth()
        results: List[Dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            for entry in skills:
                sid = (
                    entry.get("bundle_name")
                    or str(entry.get("name", "")).split(".")[-1]
                )
                if not sid:
                    continue
                try:
                    await self._import_one(client, headers, host, catalog, schema, sid)
                    results.append({"name": sid, "status": "ok"})
                except (
                    Exception
                ) as exc:  # noqa: BLE001 - one failure can't stop the batch
                    results.append(
                        {
                            "name": sid,
                            "status": "error",
                            "error": getattr(exc, "detail", None) or str(exc),
                        }
                    )
        return results

    async def _import_one(
        self,
        client: httpx.AsyncClient,
        headers: Dict[str, str],
        host: str,
        catalog: str,
        schema: str,
        skill_id: str,
    ) -> Any:
        """Download SKILL.md + bundle files for one UC skill and upsert it into
        this workspace by name — idempotent, so a re-pull updates in place."""
        from src.schemas.skill import SkillCreate, SkillUpdate

        root = f"{host}{_FILES_API}/{catalog}/{schema}/{skill_id}"
        md = await client.get(f"{root}/SKILL.md", headers=headers)
        if md.status_code != 200:
            self._raise("download SKILL.md", md)
        parsed = parser.parse(md.text, name_hint=skill_id)

        files: List[Dict[str, Any]] = []
        listing = await client.get(
            f"{host}{_DIRS_API}/{catalog}/{schema}/{skill_id}", headers=headers
        )
        for entry in (
            listing.json().get("contents", []) if listing.status_code == 200 else []
        ):
            rel = str(entry.get("name") or "")
            if entry.get("is_directory") or rel == "SKILL.md" or not rel:
                continue
            got = await client.get(f"{root}/{rel}", headers=headers)
            if got.status_code == 200:
                files.append({"path": rel, "content": got.text})

        # Upsert by name: a re-pull must update this workspace's own copy rather
        # than error on the duplicate (create_skill raises when the name exists).
        existing = next(
            (
                s
                for s in await self._skills.list_skills(self._group_context)
                if s.name == parsed.name and s.group_id
            ),
            None,
        )
        if existing:
            return await self._skills.update_skill(
                existing.id,
                SkillUpdate(
                    description=parsed.description,
                    body=parsed.body,
                    license=parsed.license,
                    compatibility=parsed.compatibility,
                    metadata=parsed.metadata or {},
                    files=files or [],
                ),
                self._group_context,
            )
        return await self._skills.create_skill(
            SkillCreate(
                name=parsed.name,
                description=parsed.description,
                body=parsed.body,
                license=parsed.license,
                compatibility=parsed.compatibility,
                metadata=parsed.metadata or {},
                files=files or None,
            ),
            self._group_context,
            source="uploaded",
            files=files or None,
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _already_exists(resp: httpx.Response) -> bool:
        if resp.status_code == 409:
            return True
        try:
            return "ALREADY_EXISTS" in (resp.json().get("error_code") or "")
        except Exception:  # noqa: BLE001
            return "already exists" in resp.text.lower()

    @staticmethod
    def _raise(action: str, resp: httpx.Response) -> None:
        raise KasalError(
            detail=f"Failed to {action}: {resp.status_code} {resp.text[:300]}"
        )
