"""
API router for user-role assignments.
Provides endpoints for managing many-to-many user-role relationships.
"""
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from src.core.dependencies import SessionDep
from src.dependencies.admin_auth import AdminUserDep
from src.services.databricks_role_service import DatabricksRoleService
from src.repositories.user_repository import UserRepository, RoleRepository, UserRoleRepository
from src.models.user import User, Role, UserRole
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/user-roles",
    tags=["user-roles"],
    responses={404: {"description": "Not found"}},
)

# Pydantic schemas
class UserRoleResponse(BaseModel):
    id: str
    user_id: str
    role_id: str
    assigned_at: str
    assigned_by: str = None
    user_email: str = None
    role_name: str = None

    class Config:
        from_attributes = True

class AssignRoleToUserRequest(BaseModel):
    user_id: str
    role_id: str

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    status: str
    created_at: str
    roles: List[str] = []
    privileges: List[str] = []

    class Config:
        from_attributes = True


@router.get("/", response_model=List[UserRoleResponse])
async def get_user_role_assignments(
    session: SessionDep,
    admin_user: AdminUserDep
) -> List[UserRoleResponse]:
    """
    Get all user-role assignments.
    Requires admin privileges.
    """
    logger.info(f"Getting all user-role assignments - requested by {admin_user.email}")
    
    try:
        user_role_repository = UserRoleRepository(UserRole, session)
        user_repository = UserRepository(User, session)
        role_repository = RoleRepository(Role, session)
        
        # Get all user-role assignments
        user_roles = await user_role_repository.list()
        
        # Enhance with user emails and role names
        enhanced_user_roles = []
        for user_role in user_roles:
            user = await user_repository.get(user_role.user_id)
            role = await role_repository.get(user_role.role_id)
            
            enhanced_user_roles.append(UserRoleResponse(
                id=user_role.id,
                user_id=user_role.user_id,
                role_id=user_role.role_id,
                assigned_at=user_role.assigned_at.isoformat() if user_role.assigned_at else None,
                assigned_by=user_role.assigned_by,
                user_email=user.email if user else None,
                role_name=role.name if role else None
            ))
        
        return enhanced_user_roles
        
    except Exception as e:
        logger.error(f"Error getting user-role assignments: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user-role assignments: {str(e)}"
        )


@router.post("/", response_model=UserRoleResponse)
async def assign_role_to_user(
    assignment_data: AssignRoleToUserRequest,
    session: SessionDep,
    admin_user: AdminUserDep
) -> UserRoleResponse:
    """
    Assign a role to a user.
    Requires admin privileges.
    """
    logger.info(f"Assigning role {assignment_data.role_id} to user {assignment_data.user_id} - requested by {admin_user.email}")
    
    try:
        user_role_repository = UserRoleRepository(UserRole, session)
        user_repository = UserRepository(User, session)
        role_repository = RoleRepository(Role, session)
        
        # Verify user and role exist
        user = await user_repository.get(assignment_data.user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {assignment_data.user_id} not found"
            )
        
        role = await role_repository.get(assignment_data.role_id)
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role {assignment_data.role_id} not found"
            )
        
        # Assign role
        user_role = await user_role_repository.assign_role(
            user_id=assignment_data.user_id,
            role_id=assignment_data.role_id,
            assigned_by=admin_user.email
        )
        await session.commit()
        
        logger.info(f"Role {assignment_data.role_id} assigned to user {assignment_data.user_id} successfully")
        
        return UserRoleResponse(
            id=user_role.id,
            user_id=user_role.user_id,
            role_id=user_role.role_id,
            assigned_at=user_role.assigned_at.isoformat() if user_role.assigned_at else None,
            assigned_by=user_role.assigned_by,
            user_email=user.email,
            role_name=role.name
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning role to user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to assign role to user: {str(e)}"
        )


@router.delete("/{user_id}/roles/{role_id}")
async def remove_role_from_user(
    user_id: str,
    role_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
):
    """
    Remove a role from a user.
    Requires admin privileges.
    """
    logger.info(f"Removing role {role_id} from user {user_id} - requested by {admin_user.email}")
    
    try:
        user_role_repository = UserRoleRepository(UserRole, session)
        
        # Remove role assignment
        await user_role_repository.remove_role(user_id, role_id)
        await session.commit()
        
        logger.info(f"Role {role_id} removed from user {user_id} successfully")
        return {"message": "Role removed from user successfully"}
        
    except Exception as e:
        logger.error(f"Error removing role from user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove role from user: {str(e)}"
        )


@router.get("/users/{user_id}/roles", response_model=List[str])
async def get_user_roles(
    user_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
) -> List[str]:
    """
    Get all roles assigned to a user.
    Requires admin privileges.
    """
    logger.info(f"Getting roles for user {user_id} - requested by {admin_user.email}")
    
    try:
        databricks_role_service = DatabricksRoleService(session)
        roles = await databricks_role_service.get_user_roles(user_id)
        
        return [role.name for role in roles]
        
    except Exception as e:
        logger.error(f"Error getting user roles: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user roles: {str(e)}"
        )


@router.get("/users/{user_id}/privileges", response_model=List[str])
async def get_user_privileges(
    user_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
) -> List[str]:
    """
    Get all privileges for a user based on their roles.
    Requires admin privileges.
    """
    logger.info(f"Getting privileges for user {user_id} - requested by {admin_user.email}")
    
    try:
        databricks_role_service = DatabricksRoleService(session)
        privileges = await databricks_role_service.get_user_privileges(user_id)
        
        return privileges
        
    except Exception as e:
        logger.error(f"Error getting user privileges: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user privileges: {str(e)}"
        )


@router.get("/users", response_model=List[UserResponse])
async def get_users_with_role_info(
    session: SessionDep,
    admin_user: AdminUserDep
) -> List[UserResponse]:
    """
    Get all users with their role and privilege information.
    Requires admin privileges.
    """
    logger.info(f"Getting all users with role info - requested by {admin_user.email}")
    
    try:
        user_repository = UserRepository(User, session)
        databricks_role_service = DatabricksRoleService(session)
        
        # Get all users
        users = await user_repository.list()
        
        # Enhance with role and privilege information
        enhanced_users = []
        for user in users:
            roles = await databricks_role_service.get_user_roles(user.id)
            privileges = await databricks_role_service.get_user_privileges(user.id)
            
            enhanced_users.append(UserResponse(
                id=user.id,
                username=user.username,
                email=user.email,
                status=user.status.value if hasattr(user.status, 'value') else str(user.status),
                created_at=user.created_at.isoformat() if user.created_at else None,
                roles=[role.name for role in roles],
                privileges=privileges
            ))
        
        return enhanced_users
        
    except Exception as e:
        logger.error(f"Error getting users with role info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get users with role info: {str(e)}"
        )