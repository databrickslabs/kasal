"""
API router for privilege management.
Provides endpoints for managing privileges in the RBAC system.
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, ConfigDict

from src.core.dependencies import SessionDep
from src.dependencies.admin_auth import AdminUserDep
from src.repositories.user_repository import PrivilegeRepository
from src.models.user import Privilege
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/privileges",
    tags=["privileges"],
    responses={404: {"description": "Not found"}},
)

# Pydantic schemas
class PrivilegeResponse(BaseModel):
    id: str
    name: str
    description: str
    created_at: str

    model_config = ConfigDict(from_attributes=True)


@router.get("/", response_model=List[PrivilegeResponse])
async def get_privileges(
    session: SessionDep,
    admin_user: AdminUserDep
) -> List[PrivilegeResponse]:
    """
    Get all privileges.
    Requires admin privileges.
    """
    logger.info(f"Getting all privileges - requested by {admin_user.email}")
    
    try:
        privilege_repository = PrivilegeRepository(Privilege, session)
        privileges = await privilege_repository.get_all_privileges()
        
        return [
            PrivilegeResponse(
                id=privilege.id,
                name=privilege.name,
                description=privilege.description,
                created_at=privilege.created_at.isoformat() if privilege.created_at else None
            )
            for privilege in privileges
        ]
        
    except Exception as e:
        logger.error(f"Error getting privileges: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get privileges: {str(e)}"
        )


@router.get("/{privilege_id}", response_model=PrivilegeResponse)
async def get_privilege(
    privilege_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
) -> PrivilegeResponse:
    """
    Get a specific privilege by ID.
    Requires admin privileges.
    """
    logger.info(f"Getting privilege {privilege_id} - requested by {admin_user.email}")
    
    try:
        privilege_repository = PrivilegeRepository(Privilege, session)
        privilege = await privilege_repository.get(privilege_id)
        
        if not privilege:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Privilege {privilege_id} not found"
            )
        
        return PrivilegeResponse(
            id=privilege.id,
            name=privilege.name,
            description=privilege.description,
            created_at=privilege.created_at.isoformat() if privilege.created_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting privilege {privilege_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get privilege: {str(e)}"
        )