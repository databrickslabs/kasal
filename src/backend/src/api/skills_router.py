"""Agent Skills — authoring, upload, export, enablement.

Skills are workspace CONTENT, not system configuration, which is why this is
group-scoped and admin/editor rather than Kasal-admin-only: a skill holds "how
we do X here", and routing every team's own procedure through a platform admin
would make the feature unusable. Kasal's builtins are read-only from here;
overriding one is authoring a workspace skill of the same name.
"""

import logging
from typing import Annotated, Any, Dict, Optional, Set

from fastapi import APIRouter, Body, Depends, File, Query, UploadFile, status
from fastapi.responses import Response

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.schemas.skill import (
    SkillCreate,
    SkillDraftRequest,
    SkillDraftResponse,
    SkillListResponse,
    SkillResponse,
    SkillUpdate,
    SkillValidationResult,
    UcSyncTarget,
)
from src.services.skills import packaging, parser
from src.services.skills.generation import SkillGenerationService
from src.services.skills.service import SkillService
from src.services.skills.uc_sync import SkillUcSyncService

router = APIRouter(
    prefix="/skills",
    tags=["skills"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

#: Authoring a skill is authoring content the agents in this workspace will
#: follow. Admin and Editor write; Operator reads and runs.
AUTHOR_ROLES = ["admin", "editor"]

#: A zip that never reaches the packaging limits still has to be read into
#: memory first, so the request is bounded before that happens.
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


def get_service(session: SessionDep) -> SkillService:
    return SkillService(session)


ServiceDep = Annotated[SkillService, Depends(get_service)]


def _require_author(group_context) -> None:
    if not check_role_in_context(group_context, AUTHOR_ROLES):
        raise ForbiddenError("Only workspace admins and editors can author skills.")


async def _builtin_names_for(service: SkillService, skill) -> Set[str]:
    """Whether THIS row shadows a builtin — one lookup, not a whole listing."""
    if skill.group_id is None:
        return set()
    return (
        {skill.name}
        if await service.repository.find_builtin_by_name(skill.name)
        else set()
    )


def _to_response(skill, builtin_names: Optional[Set[str]] = None) -> SkillResponse:
    """Row -> response.

    ``builtin_names`` lets the listing answer "does this override a builtin?"
    with one pass over rows it already has, rather than a query per row.
    """
    return SkillResponse(
        id=skill.id,
        name=skill.name,
        description=skill.description,
        body=skill.body or "",
        license=skill.license,
        compatibility=skill.compatibility,
        metadata=skill.skill_metadata or {},
        enabled=bool(skill.enabled),
        global_enabled=bool(skill.global_enabled),
        source=skill.source or "authored",
        group_id=skill.group_id,
        overrides_builtin=bool(
            skill.group_id and builtin_names and skill.name in builtin_names
        ),
        files=[
            {"path": f.path, "size_bytes": f.size_bytes, "sha256": f.sha256}
            for f in (skill.files or [])
        ],
        created_by_email=skill.created_by_email,
        created_at=skill.created_at,
        updated_at=skill.updated_at,
    )


@router.get("", response_model=SkillListResponse)
async def list_skills(service: ServiceDep, group_context: GroupContextDep):
    """Skills this workspace can use: Kasal's builtins plus its own."""
    skills = await service.list_skills(group_context)
    builtin_names = {s.name for s in skills if s.group_id is None}
    return SkillListResponse(
        skills=[_to_response(s, builtin_names) for s in skills], count=len(skills)
    )


@router.post("/validate", response_model=SkillValidationResult)
async def validate_skill(body: SkillCreate, group_context: GroupContextDep):
    """Check a skill without saving it.

    Answers 200 with ``valid: false`` — an invalid draft is what the editor
    asked about, not a failed request — and returns the reference validator's
    own messages so an author can search for them and find the spec.
    """
    _require_author(group_context)
    return SkillService.validate(body)


@router.post("/draft", response_model=SkillDraftResponse)
async def draft_skill(body: SkillDraftRequest, group_context: GroupContextDep):
    """Draft a skill from a request or a captured conversation.

    One focused generation call (the ``generate_skill`` template), validated by
    the reference validator before it returns. Nothing is saved: the chat
    renders the draft as a card and a person clicks Save.
    """
    _require_author(group_context)
    return await SkillGenerationService.draft(
        body.request,
        group_context,
        transcript=[t.model_dump() for t in (body.transcript or [])] or None,
        model=body.model,
    )


@router.post("", response_model=SkillResponse, status_code=status.HTTP_201_CREATED)
async def create_skill(
    body: SkillCreate, service: ServiceDep, group_context: GroupContextDep
):
    _require_author(group_context)
    try:
        skill = await service.create_skill(body, group_context)
    except parser.SkillValidationError as exc:
        raise BadRequestError("; ".join(exc.errors))
    except ValueError as exc:
        raise BadRequestError(str(exc))
    return _to_response(skill)


@router.put("/{skill_id}", response_model=SkillResponse)
async def update_skill(
    skill_id: int,
    body: SkillUpdate,
    service: ServiceDep,
    group_context: GroupContextDep,
):
    """Edit a skill, builtin or not.

    Editing a builtin saves the change as this workspace's own copy — invisible
    to the user, and what keeps one tenant's wording out of another's while
    stopping the next seed run from undoing their work. ``/reset`` puts the
    shipped version back.
    """
    _require_author(group_context)
    try:
        skill = await service.update_skill(skill_id, body, group_context)
    except parser.SkillValidationError as exc:
        raise BadRequestError("; ".join(exc.errors))
    except ValueError as exc:
        raise BadRequestError(str(exc))
    if not skill:
        raise NotFoundError(f"No skill {skill_id} available here.")
    return _to_response(skill, await _builtin_names_for(service, skill))


@router.delete("/{skill_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_skill(
    skill_id: int, service: ServiceDep, group_context: GroupContextDep
):
    _require_author(group_context)
    if not await service.delete_skill(skill_id, group_context):
        raise NotFoundError(f"No editable skill {skill_id} in this workspace.")


@router.patch("/{skill_id}/enabled", response_model=SkillResponse)
async def set_enabled(
    skill_id: int,
    service: ServiceDep,
    group_context: GroupContextDep,
    payload: Annotated[Dict[str, Any], Body()],
):
    """Turn a skill on or off for this workspace.

    Disabling a builtin clones it disabled into the workspace rather than
    mutating the shared row, so one tenant's choice cannot reach another's.
    """
    _require_author(group_context)
    enabled = payload.get("enabled")
    if not isinstance(enabled, bool):
        raise BadRequestError("'enabled' must be true or false.")

    skill = await service.set_enabled(skill_id, enabled, group_context)
    if not skill:
        raise NotFoundError(f"No skill {skill_id} available here.")
    return _to_response(skill, await _builtin_names_for(service, skill))


@router.post("/{skill_id}/reset", response_model=SkillResponse)
async def reset_skill(
    skill_id: int, service: ServiceDep, group_context: GroupContextDep
):
    """Put a skill back to the version Kasal ships.

    Only meaningful for a skill that overrides a builtin — 404 otherwise, since
    "reset" on a skill the workspace wrote itself would just be a delete under a
    friendlier name. Returns the CURRENT shipped version, including anything
    improved since the workspace edited it.
    """
    _require_author(group_context)
    skill = await service.reset_skill(skill_id, group_context)
    if not skill:
        raise NotFoundError(
            f"Skill {skill_id} does not override a built-in skill, so there is "
            "nothing to reset to."
        )
    return _to_response(skill)


@router.post(
    "/upload", response_model=SkillResponse, status_code=status.HTTP_201_CREATED
)
async def upload_skill(
    service: ServiceDep,
    group_context: GroupContextDep,
    file: Annotated[UploadFile, File()],
    replace: Annotated[bool, Query()] = False,
):
    """Import a skill folder as a zip.

    Accepts a wrapping directory or a bare ``SKILL.md`` at the root — both are
    what people actually produce. Rejected with the reference validator's own
    message when the skill does not conform.
    """
    _require_author(group_context)

    data = await file.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise BadRequestError(
            f"That file is larger than {MAX_UPLOAD_BYTES // 1024 // 1024}MB."
        )

    try:
        skill = await service.import_zip(data, group_context, replace=replace)
    except packaging.SkillPackageError as exc:
        raise BadRequestError(str(exc))
    except parser.SkillValidationError as exc:
        raise BadRequestError("; ".join(exc.errors))
    except ValueError as exc:
        raise BadRequestError(str(exc))
    return _to_response(skill)


@router.get("/{skill_id}/files")
async def read_skill_file(
    skill_id: int,
    service: ServiceDep,
    group_context: GroupContextDep,
    path: Annotated[str, Query(description="Path relative to the skill root.")],
):
    """The content of one bundled file.

    Separate from the skill listing on purpose: a workspace with twenty skills
    would otherwise ship every reference file on every page load, and the point
    of bundling them is that they load only when something needs them.

    ``path`` is a query parameter rather than a path segment because these
    contain slashes, and an encoded slash in a path segment is handled
    differently by every proxy in front of this.
    """
    skill = await service.get_skill(skill_id, group_context)
    if not skill:
        raise NotFoundError(f"No skill {skill_id} available here.")

    from src.services.skills import loader

    try:
        wanted = loader.normalise_path(path)
    except loader.SkillFileNotFound as exc:
        raise BadRequestError(str(exc))

    for stored in skill.files or []:
        if stored.path == wanted:
            return {
                "skill": skill.name,
                "path": stored.path,
                "content": stored.content or "",
            }
    raise NotFoundError(f"'{skill.name}' has no file '{wanted}'.")


@router.get("/uc")
async def list_uc_skills(
    session: SessionDep,
    group_context: GroupContextDep,
    catalog: str = Query(..., description="Unity Catalog catalog to list skills in"),
    schema: str = Query(..., description="Schema within the catalog"),
):
    """List the skills published in a Unity Catalog schema.

    Reads UC on behalf of the logged-in user (OBO), so it shows exactly the
    skills they can see there — the read side of the Kasal↔UC sync.
    """
    _require_author(group_context)
    sync = SkillUcSyncService(
        session, group_context, getattr(group_context, "access_token", None)
    )
    return {"skills": await sync.list_uc_skills(catalog, schema)}


@router.post("/{skill_id}/sync-to-uc")
async def sync_skill_to_uc(
    skill_id: int,
    body: UcSyncTarget,
    session: SessionDep,
    group_context: GroupContextDep,
):
    """Publish this workspace's skill into ``catalog.schema`` as a UC skill.

    Create securable → upload SKILL.md + bundle files → finalize, on behalf of
    the logged-in user (OBO) so it only writes where their UC grants allow.
    Idempotent — re-syncing an existing skill updates its content in place.
    """
    _require_author(group_context)
    sync = SkillUcSyncService(
        session, group_context, getattr(group_context, "access_token", None)
    )
    return await sync.push_skill(skill_id, body.catalog, body.schema_name)


@router.post("/sync-all-to-uc")
async def sync_all_skills_to_uc(
    body: UcSyncTarget,
    session: SessionDep,
    group_context: GroupContextDep,
):
    """Publish every skill visible to this workspace into ``catalog.schema``.

    Returns a per-skill ``{name, status, error?}`` summary — one skill failing
    (e.g. a UC permission error) does not abort the rest.
    """
    _require_author(group_context)
    sync = SkillUcSyncService(
        session, group_context, getattr(group_context, "access_token", None)
    )
    return {"results": await sync.push_all_skills(body.catalog, body.schema_name)}


@router.post("/sync-all-from-uc")
async def sync_all_skills_from_uc(
    body: UcSyncTarget,
    session: SessionDep,
    group_context: GroupContextDep,
):
    """Pull every skill published in ``catalog.schema`` into this workspace.

    Imported skills are ``source='uploaded'`` and upserted BY NAME — a re-pull
    updates the workspace's own copy in place. Returns a per-skill
    ``{name, status, error?}`` summary.
    """
    _require_author(group_context)
    sync = SkillUcSyncService(
        session, group_context, getattr(group_context, "access_token", None)
    )
    return {"results": await sync.import_all_skills(body.catalog, body.schema_name)}


@router.get("/{skill_id}/export")
async def export_skill(
    skill_id: int, service: ServiceDep, group_context: GroupContextDep
):
    """Download a skill as a folder-shaped zip.

    Portability is the reason to use this format at all: what comes out here
    runs unchanged in Claude Code, Cursor, Codex or Gemini CLI.
    """
    skill = await service.get_skill(skill_id, group_context)
    if not skill:
        raise NotFoundError(f"No skill {skill_id} available here.")

    data = packaging.write_zip(skill)
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{skill.name}.zip"'},
    )
