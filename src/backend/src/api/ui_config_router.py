import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import ForbiddenError
from src.core.permissions import is_workspace_admin
from src.schemas.ui_config import UIConfigResponse, UIConfigUpdate
from src.services.ui_config_service import UIConfigService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ui-config",
    tags=["ui-config"],
    responses={404: {"description": "Not found"}},
)


def get_ui_config_service(
    session: SessionDep,
    group_context: GroupContextDep,
) -> UIConfigService:
    """Build a group-scoped UIConfigService."""
    group_id = group_context.primary_group_id if group_context else None
    return UIConfigService(session, group_id=group_id)


UIConfigServiceDep = Annotated[UIConfigService, Depends(get_ui_config_service)]


@router.get("", response_model=UIConfigResponse)
async def get_ui_config(service: UIConfigServiceDep) -> UIConfigResponse:
    """
    Get the current workspace's Predefined UI configuration.

    Readable by any workspace member (crews + the chat need it at run time);
    defaults to disabled when never configured.
    """
    return await service.get_config()


@router.put("", response_model=UIConfigResponse)
async def update_ui_config(
    config_in: UIConfigUpdate,
    service: UIConfigServiceDep,
    group_context: GroupContextDep,
) -> UIConfigResponse:
    """Update the workspace's Predefined UI configuration (workspace admins only)."""
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can change the Predefined UI configuration")
    created_by_email = group_context.group_email if group_context else None
    return await service.update_config(config_in, created_by_email=created_by_email)
