"""
API router for Databricks role management and admin synchronization.
"""
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import SessionDep
from src.dependencies.admin_auth import AdminUserDep
from src.services.databricks_role_service import DatabricksRoleService
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/admin/databricks-roles",
    tags=["admin", "databricks-roles"],
    responses={404: {"description": "Not found"}},
)


@router.post("/sync", response_model=Dict[str, Any])
async def sync_databricks_admin_roles(
    session: SessionDep,
    admin_user: AdminUserDep
) -> Dict[str, Any]:
    """
    Manually trigger synchronization of admin roles based on Databricks app permissions.
    
    This endpoint:
    1. Fetches users with 'Can Manage' permission from Databricks app
    2. Creates or updates users in the system
    3. Assigns admin roles to those users in their respective tenants
    4. Provides fallback for local development mode
    
    Requires admin privileges to execute.
    
    Returns:
        Sync results including processed users and any errors
    """
    logger.info(f"Admin role sync triggered by {admin_user.email}")
    
    try:
        databricks_role_service = DatabricksRoleService(session)
        sync_results = await databricks_role_service.sync_admin_roles()
        
        logger.info(f"Admin role sync completed. Results: {sync_results}")
        
        return {
            "success": sync_results.get("success", False),
            "message": "Admin role synchronization completed",
            "results": sync_results,
            "triggered_by": admin_user.email
        }
        
    except Exception as e:
        logger.error(f"Error during admin role sync: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync admin roles: {str(e)}"
        )


@router.get("/admin-emails", response_model=Dict[str, Any])
async def get_databricks_admin_emails(
    session: SessionDep,
    admin_user: AdminUserDep
) -> Dict[str, Any]:
    """
    Get the list of admin emails from Databricks app permissions or fallback.
    
    This endpoint shows which emails would be considered for admin role assignment
    without actually performing the sync.
    
    Requires admin privileges to execute.
    
    Returns:
        List of admin emails and the source (Databricks or fallback)
    """
    logger.info(f"Admin emails query by {admin_user.email}")
    
    try:
        databricks_role_service = DatabricksRoleService(session)
        admin_emails = await databricks_role_service.get_databricks_app_managers()
        
        source = "databricks" if not databricks_role_service.is_local_dev else "fallback"
        
        return {
            "admin_emails": admin_emails,
            "source": source,
            "is_local_dev": databricks_role_service.is_local_dev,
            "databricks_config": {
                "app_name": databricks_role_service.app_name,
                "host_configured": bool(databricks_role_service.databricks_host),
                "token_configured": bool(databricks_role_service.databricks_token)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting admin emails: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get admin emails: {str(e)}"
        )


@router.get("/check-admin/{email}")
async def check_user_admin_status(
    email: str,
    session: SessionDep,
    admin_user: AdminUserDep
) -> Dict[str, Any]:
    """
    Check if a specific user email has admin access.
    
    Requires admin privileges to execute.
    
    Args:
        email: Email address to check
        
    Returns:
        Admin status information for the user
    """
    logger.info(f"Admin status check for {email} by {admin_user.email}")
    
    try:
        from src.repositories.user_repository import UserRepository
        
        from src.models.user import User
        
        user_repository = UserRepository(User, session)
        user = await user_repository.get_by_email(email)
        
        if not user:
            return {
                "email": email,
                "user_exists": False,
                "has_admin_access": False,
                "admin_tenants": []
            }
        
        databricks_role_service = DatabricksRoleService(session)
        has_admin_access = await databricks_role_service.check_user_admin_access(user.id)
        admin_tenants = await databricks_role_service.get_user_admin_tenants(user.id)
        
        return {
            "email": email,
            "user_exists": True,
            "user_id": user.id,
            "has_admin_access": has_admin_access,
            "admin_tenants": admin_tenants
        }
        
    except Exception as e:
        logger.error(f"Error checking admin status for {email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check admin status: {str(e)}"
        )