"""
Database Management API Router.
"""
from fastapi import APIRouter, HTTPException, Response, Request, Depends, Header
from typing import Dict, Any, Optional
import os

from src.services.database_management_service import DatabaseManagementService
from src.services.databricks_role_service import DatabricksRoleService
from src.core.logger import LoggerManager
from src.core.dependencies import SessionDep, get_group_context
from src.core.unit_of_work import UnitOfWork
from src.utils.user_context import GroupContext
from src.schemas.database_management import (
    ExportRequest,
    ExportResponse,
    ImportRequest,
    ImportResponse,
    ListBackupsRequest,
    ListBackupsResponse,
    DatabaseInfoResponse,
    DeleteBackupRequest,
    DeleteBackupResponse
)

router = APIRouter(prefix="/database-management", tags=["database-management"])
logger = LoggerManager.get_instance().api


@router.post("/export", response_model=ExportResponse)
async def export_database(
    request: ExportRequest,
    raw_request: Request
) -> ExportResponse:
    """
    Export database to a Databricks volume.
    
    Args:
        request: Export request with catalog, schema, and volume name
        raw_request: FastAPI request object for extracting auth headers
        
    Returns:
        Export result with Databricks URL for the backup
    """
    try:
        # Extract user token from request headers for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        
        # Log authentication context
        logger.info(f"Database export request - catalog: {request.catalog}, schema: {request.schema_name}, volume: {request.volume_name}")
        logger.info(f"User token available: {bool(user_token)}, SPN configured: {bool(os.getenv('DATABRICKS_CLIENT_ID'))}")
        
        service = DatabaseManagementService(user_token=user_token)
        result = await service.export_to_volume(
            catalog=request.catalog,
            schema=request.schema_name,
            volume_name=request.volume_name,
            export_format=request.export_format
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Export failed"))
        
        return ExportResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting database: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import", response_model=ImportResponse)
async def import_database(
    request: ImportRequest,
    raw_request: Request
) -> ImportResponse:
    """
    Import database from a Databricks volume.
    
    Args:
        request: Import request with catalog, schema, volume name, and backup filename
        raw_request: FastAPI request object for extracting auth headers
        
    Returns:
        Import result with database statistics
    """
    try:
        # Extract user token from request headers for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        
        service = DatabaseManagementService(user_token=user_token)
        result = await service.import_from_volume(
            catalog=request.catalog,
            schema=request.schema_name,
            volume_name=request.volume_name,
            backup_filename=request.backup_filename
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Import failed"))
        
        return ImportResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error importing database: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/list-backups", response_model=ListBackupsResponse)
async def list_backups(
    request: ListBackupsRequest,
    raw_request: Request
) -> ListBackupsResponse:
    """
    List all database backups in a Databricks volume.
    
    Args:
        request: Request with catalog, schema, and volume name
        raw_request: FastAPI request object for extracting auth headers
        
    Returns:
        List of available backups with their Databricks URLs
    """
    try:
        # Extract user token from request headers for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        
        service = DatabaseManagementService(user_token=user_token)
        result = await service.list_backups(
            catalog=request.catalog,
            schema=request.schema_name,
            volume_name=request.volume_name
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to list backups"))
        
        return ListBackupsResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing backups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/info", response_model=DatabaseInfoResponse)
async def get_database_info() -> DatabaseInfoResponse:
    """
    Get information about the current database.
    
    Returns:
        Database statistics and information
    """
    try:
        service = DatabaseManagementService()
        result = await service.get_database_info()
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Failed to get database info"))
        
        return DatabaseInfoResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug-permissions")
async def debug_permissions(
    session: SessionDep,
    group_context: GroupContext = Depends(get_group_context)
) -> Dict[str, Any]:
    """Debug endpoint to check permission details."""
    try:
        user_email = group_context.group_email
        user_token = group_context.access_token
        
        # Get environment info
        app_name = os.getenv("DATABRICKS_APP_NAME")
        databricks_host = os.getenv("DATABRICKS_HOST")
        
        if not all([app_name, databricks_host]):
            return {
                "error": "Missing configuration",
                "app_name": app_name,
                "databricks_host": databricks_host
            }
        
        # Create role service and fetch permissions
        from src.services.databricks_role_service import DatabricksRoleService
        role_service = DatabricksRoleService(uow.session)
        
        # Try to fetch permissions and capture the raw response
        # Ensure the host has https:// protocol
        if not databricks_host.startswith(('http://', 'https://')):
            databricks_host = f"https://{databricks_host}"
        url = f"{databricks_host.rstrip('/')}/api/2.0/permissions/apps/{app_name}"
        
        import aiohttp
        
        # Check if we have service principal credentials
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        # Try to get a token using service principal if available
        auth_token = user_token  # Default to user token
        auth_method = "user_token"
        
        if client_id and client_secret:
            # Get OAuth token using service principal
            try:
                oauth_url = f"{databricks_host.rstrip('/')}/oidc/v1/token"
                async with aiohttp.ClientSession() as oauth_session:
                    data = {
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": "all-apis"
                    }
                    async with oauth_session.post(oauth_url, data=data) as oauth_response:
                        if oauth_response.status == 200:
                            oauth_data = await oauth_response.json()
                            auth_token = oauth_data.get("access_token")
                            auth_method = "service_principal_oauth"
                        else:
                            auth_method = f"service_principal_failed_{oauth_response.status}"
            except Exception as e:
                auth_method = f"service_principal_error: {str(e)}"
        
        async with aiohttp.ClientSession() as http_session:
            headers = {
                "Authorization": f"Bearer {auth_token}" if auth_token else "",
                "Content-Type": "application/json"
            }
            
            async with http_session.get(url, headers=headers) as response:
                # Check if we got an error response
                if response.status != 200:
                    error_text = await response.text()
                    return {
                        "error": f"API returned {response.status}",
                        "error_text": error_text[:500],  # First 500 chars of error
                        "api_url": url,
                        "auth_method": auth_method,
                        "has_service_principal": bool(client_id and client_secret),
                        "has_user_token": bool(user_token),
                        "token_preview": auth_token[:20] + "..." if auth_token else None,
                        "current_user": user_email,
                        "note": "Check if service principal credentials are working"
                    }
                
                response_data = await response.json()
                
                # Extract users with CAN_MANAGE
                manage_users = []
                for acl_entry in response_data.get("access_control_list", []):
                    user_name = acl_entry.get("user_name")
                    group_name = acl_entry.get("group_name")
                    permissions = acl_entry.get("all_permissions", [])
                    
                    if user_name:
                        for perm in permissions:
                            if perm.get("permission_level") == "CAN_MANAGE":
                                manage_users.append({
                                    "user_name": user_name,
                                    "permission": perm.get("permission_level"),
                                    "inherited": perm.get("inherited", False)
                                })
                                break
                
                return {
                    "current_user": user_email,
                    "auth_method": auth_method,
                    "has_service_principal": bool(client_id and client_secret),
                    "api_url": url,
                    "api_status": response.status,
                    "total_acl_entries": len(response_data.get("access_control_list", [])),
                    "users_with_can_manage": manage_users,
                    "user_in_list": user_email in [u["user_name"] for u in manage_users],
                    "raw_response": response_data  # Include full response for debugging
                }
                
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "user_email": group_context.group_email if group_context else None,
            "has_token": bool(group_context.access_token) if group_context else False
        }


@router.get("/debug-headers")
async def debug_headers(
    request: Request,
    group_context: GroupContext = Depends(get_group_context)
) -> Dict[str, Any]:
    """Debug endpoint to check what headers are being received."""
    headers_dict = dict(request.headers)
    
    # Check ALL headers, not just filtered ones
    all_headers = {
        k: v[:30] + "..." if len(v) > 30 and any(word in k.lower() for word in ["token", "auth", "key", "secret"]) else v
        for k, v in headers_dict.items()
    }
    
    # Check if there's an Authorization header
    auth_header = request.headers.get("authorization", "")
    has_bearer = auth_header.startswith("Bearer ") if auth_header else False
    
    return {
        "all_headers": all_headers,
        "has_authorization_header": bool(auth_header),
        "authorization_is_bearer": has_bearer,
        "group_context_email": group_context.group_email if group_context else None,
        "group_context_has_token": bool(group_context.access_token) if group_context else False,
        "environment": {
            "DATABRICKS_APP_NAME": os.getenv("DATABRICKS_APP_NAME"),
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST"),
            "DATABRICKS_CLIENT_ID": "present" if os.getenv("DATABRICKS_CLIENT_ID") else "missing",
            "DATABRICKS_CLIENT_SECRET": "present" if os.getenv("DATABRICKS_CLIENT_SECRET") else "missing",
            "is_databricks_apps": bool(os.getenv("DATABRICKS_APP_NAME"))
        }
    }


@router.get("/check-permission")
async def check_database_management_permission(
    session: SessionDep,
    group_context: GroupContext = Depends(get_group_context)
) -> Dict[str, Any]:
    """
    Check if the current user has permission to access Database Management.
    
    Permission logic:
    - If NOT in Databricks Apps environment: Everyone has access
    - If in Databricks Apps: Only users with "Can Manage" permission have access
    
    Returns:
        Permission status and environment info
    """
    try:
        # Use GroupContext which properly extracts both email and token
        user_email = group_context.group_email
        user_token = group_context.access_token
        
        # Debug: Log what we're receiving
        logger.info(f"Permission check - Email: {user_email}, Has token: {bool(user_token)}")
        if not user_token:
            logger.info("No user token in group context - OBO may not be enabled for this app")
        
        service = DatabaseManagementService()
        result = await service.check_user_permission(
            user_email=user_email,
            session=session,
            user_token=user_token
        )
        
        # Debug: Log the result
        logger.info(f"Permission check result: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error checking database management permission: {e}")
        # In case of error, be conservative based on environment
        is_databricks_apps = bool(os.getenv("DATABRICKS_APP_NAME"))
        return {
            "has_permission": not is_databricks_apps,  # Allow if not in Apps, deny if in Apps
            "is_databricks_apps": is_databricks_apps,
            "user_email": group_context.group_email if group_context else "unknown",
            "error": str(e),
            "reason": "Error checking permissions - defaulting to safe mode"
        }