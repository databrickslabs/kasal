"""
Database Management API Router.
"""
from fastapi import APIRouter, HTTPException, Response, Request, Depends, Header
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Optional, Annotated
from datetime import datetime
import os
import asyncio
import json

from src.services.database_management_service import DatabaseManagementService
from src.services.lakebase_service import LakebaseService
from src.core.logger import LoggerManager
from src.core.dependencies import SessionDep, LegacySessionDep, GroupContextDep
from src.core.permissions import check_role_in_context
from src.schemas.database_management import (
    ExportRequest,
    ExportResponse,
    ImportRequest,
    ImportResponse,
    ListBackupsRequest,
    ListBackupsResponse,
    DatabaseInfoResponse
)

router = APIRouter(prefix="/database-management", tags=["database-management"])
logger = LoggerManager.get_instance().api


def get_database_management_service(
    session: SessionDep,
    raw_request: Request
) -> DatabaseManagementService:
    """
    Dependency factory for DatabaseManagementService.
    Extracts user token from request and creates service with session.
    """
    from src.utils.databricks_auth import extract_user_token_from_request
    user_token = extract_user_token_from_request(raw_request)
    # Service will create its own repository internally
    return DatabaseManagementService(session=session, user_token=user_token)


# Type alias for the service dependency
DatabaseManagementServiceDep = Annotated[DatabaseManagementService, Depends(get_database_management_service)]


@router.post("/export", response_model=ExportResponse)
async def export_database(
    request: ExportRequest,
    service: DatabaseManagementServiceDep,
    group_context: GroupContextDep
) -> ExportResponse:
    """
    Export database to a Databricks volume.
    Only Admins can export databases.

    Args:
        request: Export request with catalog, schema, and volume name
        service: Database management service (injected)
        group_context: Group context for permissions

    Returns:
        Export result with Databricks URL for the backup
    """
    # Check permissions - only admins can export databases
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=403,
            detail="Only admins can export databases"
        )

    try:
        # Log authentication context
        logger.info(f"Database export request - catalog: {request.catalog}, schema: {request.schema_name}, volume: {request.volume_name}")
        logger.info(f"User token available: {bool(service.user_token)}, SPN configured: {bool(os.getenv('DATABRICKS_CLIENT_ID'))}")

        # Use the injected service
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
    service: DatabaseManagementServiceDep,
    group_context: GroupContextDep
) -> ImportResponse:
    """
    Import database from a Databricks volume.
    Only Admins can import databases.

    Args:
        request: Import request with catalog, schema, volume name, and backup filename
        service: Database management service (injected)
        group_context: Group context for permissions

    Returns:
        Import result with database statistics
    """
    # Check permissions - only admins can import databases
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=403,
            detail="Only admins can import databases"
        )

    try:
        # Use the injected service
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
    service: DatabaseManagementServiceDep,
    group_context: GroupContextDep
) -> ListBackupsResponse:
    """
    List all database backups in a Databricks volume.
    
    Args:
        request: Request with catalog, schema, and volume name
        service: Database management service (injected)
        group_context: Group context for permissions

    Returns:
        List of available backups with their Databricks URLs
    """
    try:
        # Use the injected service
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
async def get_database_info(
    raw_request: Request,
    group_context: GroupContextDep
) -> DatabaseInfoResponse:
    """
    Get information about the current database.

    Returns:
        Database statistics and information about the SOURCE database (not Lakebase)
    """
    try:
        # ALWAYS use fallback session to get info about source database
        # Never route to Lakebase for database info endpoint
        from src.db.session import async_session_factory
        from src.utils.databricks_auth import extract_user_token_from_request

        async with async_session_factory() as session:
            user_token = extract_user_token_from_request(raw_request)
            service = DatabaseManagementService(session=session, user_token=user_token)
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
    group_context: GroupContextDep
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
        
        # Note: Complex role service removed - using simplified permissions
        
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
    group_context: GroupContextDep
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
    service: DatabaseManagementServiceDep,
    session: SessionDep,
    group_context: GroupContextDep
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

        # Use the injected service
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


# Lakebase specific endpoints
@router.get("/lakebase/config")
async def get_lakebase_config(
    session: LegacySessionDep,  # Always use fallback DB for config
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Get current Lakebase configuration.

    Returns:
        Lakebase configuration settings
    """
    try:
        from src.utils.databricks_auth import extract_user_token_from_request
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_email=user_email)
        config = await service.get_config()
        return config

    except Exception as e:
        logger.error(f"Error getting Lakebase config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lakebase/config")
async def save_lakebase_config(
    config: Dict[str, Any],
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Save Lakebase configuration.

    Args:
        config: Configuration dictionary

    Returns:
        Saved configuration
    """
    try:
        # ALWAYS use fallback session factory for config operations
        # Never route to Lakebase when saving Lakebase config itself
        from src.db.session import async_session_factory
        async with async_session_factory() as session:
            user_email = group_context.group_email if group_context else None
            service = LakebaseService(session=session, user_email=user_email)
            saved_config = await service.save_config(config)
            return saved_config

    except Exception as e:
        logger.error(f"Error saving Lakebase config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lakebase/create")
async def create_lakebase_instance(
    request: Dict[str, Any],
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Create a new Lakebase instance and migrate existing data.

    Args:
        request: Instance creation parameters

    Returns:
        Instance details
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)

        # Create instance and migrate data
        instance = await service.create_instance(
            instance_name=request.get("instance_name", "kasal-lakebase"),
            capacity=request.get("capacity", "CU_1"),
            retention_days=request.get("retention_days", 14),
            node_count=request.get("node_count", 1)
        )

        return instance

    except Exception as e:
        logger.error(f"Error creating Lakebase instance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lakebase/instance/{instance_name}")
async def get_lakebase_instance(
    instance_name: str,
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Get Lakebase instance details.

    Args:
        instance_name: Name of the instance

    Returns:
        Instance details
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)
        instance = await service.get_instance(instance_name)

        if not instance or instance.get("state") == "NOT_FOUND":
            raise HTTPException(status_code=404, detail=f"Instance {instance_name} not found")

        return instance

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting Lakebase instance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lakebase/tables")
async def check_lakebase_tables(
    session: SessionDep,
    config_session: LegacySessionDep,
    raw_request: Request,
    group_context: GroupContextDep
):
    """
    Check what tables exist in the configured Lakebase instance.
    Returns detailed information about tables and their status.
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)

        # Initialize service with user token
        service = LakebaseService(user_token=user_token)

        # Check tables
        result = await service.check_lakebase_tables(config_session)

        return result

    except Exception as e:
        logger.error(f"Error checking Lakebase tables: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error checking Lakebase tables: {str(e)}"
        )

@router.post("/lakebase/migrate")
async def migrate_to_lakebase(
    request: Dict[str, Any],
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Manually trigger migration to Lakebase.

    Args:
        request: Migration parameters (instance_name, endpoint)

    Returns:
        Migration result
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)

        # Trigger migration
        instance_name = request.get("instance_name", "kasal-lakebase")
        endpoint = request.get("endpoint")
        recreate_schema = request.get("recreate_schema", False)

        if not endpoint:
            raise HTTPException(status_code=400, detail="Endpoint is required for migration")

        logger.info(f"🚀 Migration to Lakebase requested by {user_email} (recreate_schema={recreate_schema})")

        # Log to frontend-visible logger
        logger.info("=" * 80)
        logger.info("🚀 LAKEBASE MIGRATION STARTED")
        logger.info(f"Instance: {instance_name}")
        logger.info(f"Recreate Schema: {recreate_schema}")
        logger.info("=" * 80)

        result = await service.migrate_existing_data(instance_name, endpoint, recreate_schema=recreate_schema)

        # Update configuration to mark migration as complete
        config = await service.get_config()
        config["migration_completed"] = result.get("success", False)
        config["migration_status"] = "completed" if result.get("success") else "failed"
        config["migration_result"] = result
        await service.save_config(config)

        # If migration succeeded, dispose existing connections to switch to Lakebase
        if result.get("success", False):
            from src.db.session import dispose_engines
            await dispose_engines()
            logger.info("🔄 Disposed existing database connections to switch to Lakebase")

        logger.info("✅ Migration configuration saved. Next API calls will use Lakebase.")
        return result

    except Exception as e:
        logger.error(f"Error migrating to Lakebase: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lakebase/migrate/stream")
async def migrate_to_lakebase_stream(
    request: Dict[str, Any],
    raw_request: Request,
    group_context: GroupContextDep
):
    """
    Stream migration progress to Lakebase using Server-Sent Events.

    Args:
        request: Migration parameters (instance_name, endpoint, recreate_schema)

    Returns:
        StreamingResponse with SSE events
    """
    async def event_generator():
        migration_succeeded = False
        try:
            # Extract user token for authentication
            from src.utils.databricks_auth import extract_user_token_from_request
            from src.db.session import async_session_factory

            user_token = extract_user_token_from_request(raw_request)
            user_email = group_context.group_email if group_context else None

            # Get migration parameters
            instance_name = request.get("instance_name", "kasal-lakebase")
            endpoint = request.get("endpoint")
            recreate_schema = request.get("recreate_schema", False)
            migrate_data = request.get("migrate_data", True)  # Default to migrating data

            # Send initial event
            action = "schema creation" if not migrate_data else "migration"
            yield f"data: {json.dumps({'type': 'start', 'message': f'🚀 Starting Lakebase {action}...', 'instance': instance_name, 'recreate_schema': recreate_schema, 'migrate_data': migrate_data})}\n\n"

            # Auto-fetch endpoint if needed using a separate session context
            if not endpoint:
                yield f"data: {json.dumps({'type': 'progress', 'message': '🔍 Auto-detecting Lakebase endpoint...', 'step': 'detect_endpoint'})}\n\n"
                try:
                    from src.db.session import async_session_factory as fallback_session_factory
                    async with fallback_session_factory() as temp_session:
                        service_temp = LakebaseService(session=temp_session, user_token=user_token, user_email=user_email)
                        instance_info = await service_temp.get_instance(instance_name)
                        if instance_info and instance_info.get("read_write_dns"):
                            endpoint = instance_info["read_write_dns"]
                            yield f"data: {json.dumps({'type': 'success', 'message': f'✅ Detected endpoint: {endpoint}'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'error', 'message': f'Could not auto-detect endpoint for instance {instance_name}'})}\n\n"
                            return
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to auto-detect endpoint: {str(e)}'})}\n\n"
                    return

            # Create service WITHOUT a session since migration creates its own engines
            # Pass None for session - migration doesn't use it
            service = LakebaseService(session=None, user_token=user_token, user_email=user_email)

            # Stream migration progress
            async for event in service.migrate_existing_data_stream(instance_name, endpoint, recreate_schema=recreate_schema, migrate_data=migrate_data):
                yield f"data: {json.dumps(event)}\n\n"
                await asyncio.sleep(0)  # Allow other tasks to run

                # Track if migration succeeded
                if event.get('type') == 'result' and event.get('success'):
                    migration_succeeded = True

            # If migration succeeded, update config and dispose existing connections
            if migration_succeeded:
                # Update Lakebase configuration to mark migration as complete and enable it
                try:
                    from src.db.session import async_session_factory as fallback_session_factory
                    async with fallback_session_factory() as config_session:
                        config_service = LakebaseService(session=config_session, user_token=user_token, user_email=user_email)
                        config = await config_service.get_config()
                        config["migration_completed"] = True
                        config["migration_date"] = datetime.utcnow().isoformat()
                        config["enabled"] = True
                        config["instance_name"] = instance_name
                        config["endpoint"] = endpoint
                        await config_service.save_config(config)
                        yield f"data: {json.dumps({'type': 'success', 'message': '✅ Lakebase configuration updated and enabled'})}\n\n"
                except Exception as config_error:
                    logger.error(f"Error updating Lakebase config after migration: {config_error}")
                    yield f"data: {json.dumps({'type': 'warning', 'message': f'Migration succeeded but config update failed: {config_error}'})}\n\n"

                # Dispose existing connections to switch to Lakebase
                from src.db.session import dispose_engines
                await dispose_engines()
                logger.info("🔄 Disposed existing database connections to switch to Lakebase")
                yield f"data: {json.dumps({'type': 'info', 'message': '🔄 Switched all connections to Lakebase'})}\n\n"

        except Exception as e:
            logger.error(f"Error in migration stream: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.post("/lakebase/start")
async def start_lakebase_instance(
    request: Dict[str, Any],
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Start a stopped Lakebase instance.

    Args:
        request: Instance name in request body

    Returns:
        Instance status after start attempt
    """
    try:
        from src.utils.databricks_auth import extract_user_token_from_request

        user_token = extract_user_token_from_request(raw_request)

        service = LakebaseService(session=session, user_token=user_token)
        result = await service.start_instance(request.get("instance_name"))

        return result

    except Exception as e:
        logger.error(f"Error starting Lakebase instance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lakebase/test/{instance_name}")
async def test_lakebase_connection_get(
    instance_name: str,
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Test connection to Lakebase instance (GET endpoint).

    Args:
        instance_name: Name of the Lakebase instance

    Returns:
        Connection test result
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)
        result = await service.test_connection(instance_name)

        return result

    except Exception as e:
        logger.error(f"Error testing Lakebase connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lakebase/workspace-info")
async def get_lakebase_workspace_info(
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Get Databricks workspace URL and organization ID for Lakebase links.

    Returns:
        Dictionary with workspace_url and organization_id
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)

        # Get workspace client
        w = await service.get_workspace_client()

        # Get workspace ID (organization ID)
        workspace_id = w.get_workspace_id()

        # Get workspace URL from config
        workspace_url = w.config.host

        # Clean up the URL
        if workspace_url.endswith('/'):
            workspace_url = workspace_url[:-1]

        return {
            "success": True,
            "workspace_url": workspace_url,
            "organization_id": str(workspace_id)
        }

    except Exception as e:
        logger.error(f"Error getting workspace info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lakebase/enable")
async def enable_lakebase_without_migration(
    request: Dict[str, Any],
    session: LegacySessionDep,  # Always use fallback DB for config
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Enable Lakebase without performing data migration.
    This sets the 'enabled' flag in configuration, allowing connection to Lakebase
    where schema will be created on first use.

    Args:
        request: Must contain instance_name and endpoint

    Returns:
        Success status and message
    """
    try:
        user_email = group_context.group_email if group_context else None
        service = LakebaseService(session=session, user_email=user_email)

        # Get current config
        config = await service.get_config()

        # Extract required parameters
        instance_name = request.get("instance_name")
        endpoint = request.get("endpoint")

        if not instance_name or not endpoint:
            raise HTTPException(
                status_code=400,
                detail="instance_name and endpoint are required to enable Lakebase"
            )

        # Update config with instance details
        config["instance_name"] = instance_name
        config["endpoint"] = endpoint
        config["enabled"] = True
        config["migration_completed"] = True  # Mark as ready even without migration

        # Save updated config
        await service.save_config(config)

        # Dispose existing database connections to force reconnection to Lakebase
        from src.db.session import dispose_engines
        await dispose_engines()
        logger.info("Disposed existing database connections to switch to Lakebase")

        return {
            "success": True,
            "message": "Lakebase enabled successfully. All connections switched to Lakebase.",
            "config": config
        }

    except Exception as e:
        logger.error(f"Error enabling Lakebase: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lakebase/test-connection")
async def test_lakebase_connection(
    request: Dict[str, Any],
    session: SessionDep,
    raw_request: Request,
    group_context: GroupContextDep
) -> Dict[str, Any]:
    """
    Test connection to Lakebase instance (POST endpoint).

    Args:
        request: Instance name in request body

    Returns:
        Connection test result
    """
    try:
        # Extract user token for authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(raw_request)
        user_email = group_context.group_email if group_context else None

        service = LakebaseService(session=session, user_token=user_token, user_email=user_email)
        result = await service.test_connection(request.get("instance_name"))

        return result

    except Exception as e:
        logger.error(f"Error testing Lakebase connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))