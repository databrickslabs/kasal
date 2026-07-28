"""Agent Skills — authoring, upload, export, enablement.

Skills are workspace CONTENT, not system configuration, which is why this is
group-scoped and admin/editor rather than Kasal-admin-only: a skill holds "how
we do X here", and routing every team's own procedure through a platform admin
would make the feature unusable. Kasal's builtins are read-only from here;
overriding one is authoring a workspace skill of the same name.
"""

import logging
from typing import Annotated, Any, Dict

from fastapi import APIRouter, Body, Depends, File, Query, UploadFile, status
from fastapi.responses import Response

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.schemas.skill import (
    SkillCreate,
    SkillListResponse,
    SkillResponse,
    SkillUpdate,
    SkillValidationResult,
)
from src.services.skills import packaging, parser
from src.services.skills.service import SkillService

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


def _to_response(skill) -> SkillResponse:
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
    return SkillListResponse(
        skills=[_to_response(s) for s in skills], count=len(skills)
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
    """Edit one of this workspace's own skills.

    A builtin is not editable in place: overriding one means authoring a
    workspace skill with the same name, which the resolver already prefers.
    """
    _require_author(group_context)
    try:
        skill = await service.update_skill(skill_id, body, group_context)
    except parser.SkillValidationError as exc:
        raise BadRequestError("; ".join(exc.errors))
    except ValueError as exc:
        raise BadRequestError(str(exc))
    if not skill:
        raise NotFoundError(f"No editable skill {skill_id} in this workspace.")
    return _to_response(skill)


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
