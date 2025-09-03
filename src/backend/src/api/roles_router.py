from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import SessionDep
from src.dependencies.admin_auth import AdminUserDep
from src.schemas.user import (
    RoleCreate, RoleUpdate, RoleInDB, RoleWithPrivileges,
    PrivilegeInDB, PrivilegeCreate, PrivilegeUpdate
)
from src.services.role_service import RoleService
from src.services.privilege_service import PrivilegeService

router = APIRouter(
    prefix="/roles",
    tags=["roles"],
    responses={401: {"description": "Unauthorized"}, 403: {"description": "Forbidden"}},
)

# Roles endpoints
@router.get("", response_model=List[RoleInDB])
async def read_roles(
    session: SessionDep,
    admin_user: AdminUserDep,
    skip: int = 0,
    limit: int = 100,
):
    """Get list of roles"""
    role_service = RoleService(session)
    return await role_service.get_roles(skip=skip, limit=limit)

@router.post("", response_model=RoleWithPrivileges, status_code=status.HTTP_201_CREATED)
async def create_role(
    role_data: RoleCreate,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Create a new role"""
    role_service = RoleService(session)
    
    try:
        return await role_service.create_role(role_data)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/{role_id}", response_model=RoleWithPrivileges)
async def read_role(
    role_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Get a role by ID"""
    role_service = RoleService(session)
    role = await role_service.get_role_with_privileges(role_id)
    
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    
    return role

@router.put("/{role_id}", response_model=RoleWithPrivileges)
async def update_role(
    role_id: str,
    role_data: RoleUpdate,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Update a role"""
    role_service = RoleService(session)
    
    try:
        role = await role_service.update_role(role_id, role_data)
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )
        return role
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role(
    role_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Delete a role"""
    role_service = RoleService(session)
    
    # Prevent deleting built-in roles
    if role_id in ["admin", "technical", "regular"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete built-in roles"
        )
    
    success = await role_service.delete_role(role_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

# Role-Privilege management endpoints
@router.get("/{role_id}/privileges", response_model=List[PrivilegeInDB])
async def get_role_privileges(
    role_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Get privileges for a specific role"""
    role_service = RoleService(session)
    privileges = await role_service.get_role_privileges(role_id)
    return privileges

@router.post("/{role_id}/privileges", status_code=status.HTTP_201_CREATED)
async def assign_privilege_to_role(
    role_id: str,
    privilege_data: dict,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Assign a privilege to a role"""
    role_service = RoleService(session)
    
    privilege_id = privilege_data.get("privilege_id")
    if not privilege_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="privilege_id is required"
        )
    
    success = await role_service.assign_privilege_to_role(role_id, privilege_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role or privilege not found"
        )
    
    return {"message": "Privilege assigned successfully"}

@router.delete("/{role_id}/privileges/{privilege_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_privilege_from_role(
    role_id: str,
    privilege_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Remove a privilege from a role"""
    role_service = RoleService(session)
    
    success = await role_service.remove_privilege_from_role(role_id, privilege_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role or privilege not found"
        )

# Privileges router - could also be separated into its own file
privilege_router = APIRouter(
    prefix="/privileges",
    tags=["privileges"],
    responses={401: {"description": "Unauthorized"}, 403: {"description": "Forbidden"}},
)

@privilege_router.get("", response_model=List[PrivilegeInDB])
async def read_privileges(
    session: SessionDep,
    admin_user: AdminUserDep,
    skip: int = 0,
    limit: int = 100,
):
    """Get list of privileges"""
    privilege_service = PrivilegeService(session)
    return await privilege_service.get_privileges(skip=skip, limit=limit)

@privilege_router.post("", response_model=PrivilegeInDB, status_code=status.HTTP_201_CREATED)
async def create_privilege(
    privilege_data: PrivilegeCreate,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Create a new privilege"""
    privilege_service = PrivilegeService(session)
    
    try:
        return await privilege_service.create_privilege(privilege_data)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@privilege_router.get("/{privilege_id}", response_model=PrivilegeInDB)
async def read_privilege(
    privilege_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Get a privilege by ID"""
    privilege_service = PrivilegeService(session)
    privilege = await privilege_service.get_privilege(privilege_id)
    
    if not privilege:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Privilege not found"
        )
    
    return privilege

@privilege_router.put("/{privilege_id}", response_model=PrivilegeInDB)
async def update_privilege(
    privilege_id: str,
    privilege_data: PrivilegeUpdate,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Update a privilege"""
    privilege_service = PrivilegeService(session)
    privilege = await privilege_service.update_privilege(privilege_id, privilege_data)
    
    if not privilege:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Privilege not found"
        )
    
    return privilege

@privilege_router.delete("/{privilege_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_privilege(
    privilege_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
):
    """Delete a privilege"""
    privilege_service = PrivilegeService(session)
    success = await privilege_service.delete_privilege(privilege_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Privilege not found"
        ) 