"""
Database Management Service for export/import operations with Databricks volumes.
"""
import os
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.config.settings import settings
from src.repositories.database_backup_repository import DatabaseBackupRepository
from src.services.databricks_role_service import DatabricksRoleService
from src.db.session import async_session_factory

logger = LoggerManager.get_instance().system


class DatabaseManagementService:
    """Service for managing database export and import operations with Databricks volumes."""
    
    def __init__(self, repository: Optional[DatabaseBackupRepository] = None, user_token: Optional[str] = None):
        """
        Initialize the service with a repository.
        
        Args:
            repository: Database backup repository instance
            user_token: Optional user token for OBO authentication
        """
        # For database operations, we prioritize Service Principal auth
        # This is because Unity Catalog volume operations require proper scopes
        # that are not available in OBO tokens
        
        # Check if we should use Service Principal instead of user token
        import os
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        if client_id and client_secret:
            # Service Principal is available - use it instead of user token
            logger.info(f"Database Management: Service Principal AVAILABLE (client_id={client_id[:10]}...)")
            logger.info("Database Management: FORCING Service Principal authentication (Unity Catalog volumes require SPN)")
            logger.info(f"Database Management: Ignoring user token even if provided (was provided: {bool(user_token)})")
            # Pass None as user_token to force Service Principal usage
            self.repository = repository or DatabaseBackupRepository(user_token=None)
            self.user_token = None  # Don't use user token even if provided
        else:
            # No Service Principal - fall back to user token
            logger.warning("Database Management: NO Service Principal configured (DATABRICKS_CLIENT_ID not found)")
            logger.info(f"Database Management: Falling back to user token (present: {bool(user_token)})")
            self.repository = repository or DatabaseBackupRepository(user_token=user_token)
            self.user_token = user_token
    
    async def export_to_volume(
        self,
        catalog: str,
        schema: str,
        volume_name: str = "kasal_backups",
        export_format: str = "native",
        session: Optional[AsyncSession] = None
    ) -> Dict[str, Any]:
        """
        Export database to a Databricks volume.
        
        Args:
            catalog: Databricks catalog name
            schema: Databricks schema name
            volume_name: Volume name (default: kasal_backups)
            session: Optional database session (for PostgreSQL)
            
        Returns:
            Export result with volume path and Databricks URL
        """
        try:
            # Determine database type
            db_type = DatabaseBackupRepository.get_database_type()
            
            # Generate backup filename with timestamp and appropriate extension
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if db_type == 'sqlite':
                # Get database path for SQLite
                db_path = settings.SQLITE_DB_PATH
                if not os.path.isabs(db_path):
                    db_path = os.path.abspath(db_path)
                
                if not os.path.exists(db_path):
                    return {
                        "success": False,
                        "error": f"Database file not found at {db_path}"
                    }
                
                # Get database size before export
                db_size = os.path.getsize(db_path) / (1024 * 1024)  # Size in MB
                
                # For SQLite, native format is always .db
                backup_filename = f"kasal_backup_{timestamp}.db"
                
                # Use repository to create SQLite backup
                backup_result = await self.repository.create_sqlite_backup(
                    source_path=db_path,
                    catalog=catalog,
                    schema=schema,
                    volume_name=volume_name,
                    backup_filename=backup_filename
                )
                
                original_size_mb = db_size
                
            elif db_type == 'postgres':
                # Create or use provided session for PostgreSQL
                if session:
                    owned_session = False
                    db_session = session
                else:
                    owned_session = True
                    db_session = async_session_factory()
                
                try:
                    # Determine file extension based on export format
                    if export_format == "sqlite":
                        backup_filename = f"kasal_backup_{timestamp}.db"
                        postgres_export_format = "sqlite"
                    else:  # Default to SQL
                        backup_filename = f"kasal_backup_{timestamp}.sql"
                        postgres_export_format = "sql"
                    
                    # Use repository to create PostgreSQL backup
                    backup_result = await self.repository.create_postgres_backup(
                        session=db_session,
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename,
                        export_format=postgres_export_format
                    )
                    
                    # For PostgreSQL, we don't have an original file size
                    original_size_mb = None
                    
                finally:
                    if owned_session:
                        await db_session.close()
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
            
            if not backup_result["success"]:
                return backup_result
            
            backup_size_mb = backup_result["backup_size"] / (1024 * 1024)  # Size in MB
            
            # Generate Databricks URL for the volume
            workspace_url = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
            if not workspace_url:
                workspace_url = "https://your-workspace.databricks.com"
            
            # Construct the Databricks volume URL for browsing
            # Main volume browse URL (this is the only one that works properly)
            volume_browse_url = f"{workspace_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}"
            
            # Clean up old backups using repository
            cleanup_result = await self.repository.cleanup_old_backups(
                catalog=catalog,
                schema=schema,
                volume_name=volume_name,
                keep_count=5
            )
            
            if cleanup_result["success"] and cleanup_result.get("deleted"):
                logger.info(f"Cleaned up old backups: {cleanup_result['deleted']}")
            
            # Get list of current backups after export
            backups_list = await self.repository.list_backups(
                catalog=catalog,
                schema=schema,
                volume_name=volume_name
            )
            
            # Format backup files with their URLs
            export_files = []
            if backups_list:  # list_backups returns a list, not a dict
                for backup in backups_list:
                    export_files.append({
                        "filename": backup["filename"],
                        "size_mb": backup.get("size", 0) / (1024 * 1024),  # Convert bytes to MB
                        "created_at": backup["created_at"].isoformat() if isinstance(backup["created_at"], datetime) else str(backup["created_at"])
                    })
            
            logger.info(f"Database exported successfully to {backup_result['backup_path']} ({backup_size_mb:.2f} MB)")
            
            result = {
                "success": True,
                "backup_path": backup_result["backup_path"],
                "backup_filename": backup_filename,
                "volume_path": f"{catalog}.{schema}.{volume_name}",
                "volume_browse_url": volume_browse_url,
                "databricks_url": volume_browse_url,  # Keep both for backward compatibility
                "export_files": export_files,
                "size_mb": round(backup_size_mb, 2),
                "timestamp": datetime.now().isoformat(),
                "catalog": catalog,
                "schema": schema,
                "volume": volume_name,
                "database_type": db_type
            }
            
            if original_size_mb is not None:
                result["original_size_mb"] = round(original_size_mb, 2)
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting database to volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def import_from_volume(
        self,
        catalog: str,
        schema: str,
        volume_name: str,
        backup_filename: str,
        session: Optional[AsyncSession] = None
    ) -> Dict[str, Any]:
        """
        Import database from a Databricks volume.
        
        Args:
            catalog: Databricks catalog name
            schema: Databricks schema name
            volume_name: Volume name
            backup_filename: Name of the backup file to import
            session: Optional database session (for PostgreSQL)
            
        Returns:
            Import result
        """
        try:
            # Validate filename to prevent path traversal
            if ".." in backup_filename or "/" in backup_filename or "\\" in backup_filename:
                return {
                    "success": False,
                    "error": "Invalid backup filename"
                }
            
            # Determine database type
            db_type = DatabaseBackupRepository.get_database_type()
            
            
            # Determine backup type from filename
            backup_type = "unknown"
            if backup_filename.endswith(".db"):
                backup_type = "sqlite"
            elif backup_filename.endswith(".json"):
                backup_type = "postgres_json"
            elif backup_filename.endswith(".sql"):
                backup_type = "postgres_sql"
            
            # Validate backup type matches current database type
            if db_type == 'sqlite' and backup_type != 'sqlite':
                return {
                    "success": False,
                    "error": f"Cannot restore {backup_type} backup to SQLite database"
                }
            elif db_type == 'postgres' and backup_type not in ['postgres_json', 'postgres_sql']:
                return {
                    "success": False,
                    "error": f"Cannot restore {backup_type} backup to PostgreSQL database"
                }
            
            if db_type == 'sqlite':
                # Get current database path
                db_path = settings.SQLITE_DB_PATH
                if not os.path.isabs(db_path):
                    db_path = os.path.abspath(db_path)
                
                # Use repository to restore SQLite backup
                restore_result = await self.repository.restore_sqlite_backup(
                    catalog=catalog,
                    schema=schema,
                    volume_name=volume_name,
                    backup_filename=backup_filename,
                    target_path=db_path,
                    create_safety_backup=True
                )
                
            elif db_type == 'postgres':
                # Create or use provided session for PostgreSQL
                if session:
                    owned_session = False
                    db_session = session
                else:
                    owned_session = True
                    db_session = async_session_factory()
                
                try:
                    # Use repository to restore PostgreSQL backup
                    restore_result = await self.repository.restore_postgres_backup(
                        session=db_session,
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename
                    )
                finally:
                    if owned_session:
                        await db_session.close()
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
            
            if not restore_result["success"]:
                return restore_result
            
            logger.info(f"Database imported successfully from {catalog}.{schema}.{volume_name}/{backup_filename}")
            
            result = {
                "success": True,
                "imported_from": f"/Volumes/{catalog}/{schema}/{volume_name}/{backup_filename}",
                "backup_filename": backup_filename,
                "volume_path": f"{catalog}.{schema}.{volume_name}",
                "timestamp": datetime.now().isoformat(),
                "database_type": db_type
            }
            
            # Add additional info based on database type
            if 'restored_size' in restore_result:
                result["size_mb"] = round(restore_result["restored_size"] / (1024 * 1024), 2)
            if 'restored_tables' in restore_result:
                result["restored_tables"] = restore_result["restored_tables"]
            
            return result
            
        except Exception as e:
            logger.error(f"Error importing database from volume: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def list_backups(
        self,
        catalog: str,
        schema: str,
        volume_name: str
    ) -> Dict[str, Any]:
        """
        List all database backups in a Databricks volume.
        
        Args:
            catalog: Databricks catalog name
            schema: Databricks schema name
            volume_name: Volume name
            
        Returns:
            List of available backups
        """
        try:
            # Use repository to list backups
            backups = await self.repository.list_backups(catalog, schema, volume_name)
            
            # Generate Databricks URLs for each backup
            workspace_url = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
            if not workspace_url:
                workspace_url = "https://your-workspace.databricks.com"
            
            formatted_backups = []
            for backup in backups:
                databricks_url = f"{workspace_url}/explore/data/volumes/{catalog}/{schema}/{volume_name}/{backup['filename']}"
                
                formatted_backups.append({
                    "filename": backup["filename"],
                    "size_mb": round(backup["size"] / (1024 * 1024), 2),
                    "created_at": backup["created_at"].isoformat(),
                    "databricks_url": databricks_url,
                    "backup_type": backup.get("backup_type", "unknown")
                })
            
            return {
                "success": True,
                "backups": formatted_backups,
                "volume_path": f"{catalog}.{schema}.{volume_name}",
                "total_backups": len(formatted_backups)
            }
            
        except Exception as e:
            logger.error(f"Error listing backups: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_database_info(self, session: Optional[AsyncSession] = None) -> Dict[str, Any]:
        """
        Get information about the current database.
        
        Args:
            session: Optional database session (for PostgreSQL)
            
        Returns:
            Database information and statistics
        """
        try:
            db_type = DatabaseBackupRepository.get_database_type()
            
            if db_type == 'sqlite':
                db_path = settings.SQLITE_DB_PATH
                if not os.path.isabs(db_path):
                    db_path = os.path.abspath(db_path)
                
                # Use repository to get database info
                info_result = await self.repository.get_database_info(db_path=db_path)
                
            elif db_type == 'postgres':
                # Create or use provided session for PostgreSQL
                if session:
                    owned_session = False
                    db_session = session
                else:
                    owned_session = True
                    db_session = async_session_factory()
                
                try:
                    # Use repository to get database info
                    info_result = await self.repository.get_database_info(session=db_session)
                finally:
                    if owned_session:
                        await db_session.close()
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
            
            if not info_result["success"]:
                return info_result
            
            # Format the result for the API
            result = {
                "success": True,
                "database_type": db_type,
                "tables": info_result.get("tables", {}),
                "total_tables": info_result.get("total_tables", 0),
                "memory_backends": info_result.get("memory_backends", [])
            }
            
            # Add size information
            if 'size' in info_result:
                result["size_mb"] = round(info_result["size"] / (1024 * 1024), 2)
            
            # Add timestamps for SQLite
            if 'created_at' in info_result:
                result["created_at"] = info_result["created_at"].isoformat()
            if 'modified_at' in info_result:
                result["modified_at"] = info_result["modified_at"].isoformat()
            
            # Add path for SQLite
            if 'path' in info_result:
                result["database_path"] = info_result["path"]
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def check_user_permission(
        self,
        user_email: str,
        session: Optional[AsyncSession] = None,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check if a user has permission to access Database Management features.
        
        Permission logic:
        - If NOT in Databricks Apps environment: Everyone has access
        - If in Databricks Apps: Only users with "Can Manage" permission have access
        
        Args:
            user_email: Email of the user to check
            session: Optional database session
            
        Returns:
            Permission status and environment info
        """
        try:
            # Check if we're in a Databricks Apps environment
            databricks_app_name = os.getenv("DATABRICKS_APP_NAME")
            databricks_host = os.getenv("DATABRICKS_HOST")
            is_databricks_apps = bool(databricks_app_name and databricks_host)
            
            # Default: everyone has access if not in Databricks Apps
            has_permission = True
            permission_reason = "Not in Databricks Apps environment - all users have access"
            
            # If in Databricks Apps, check for "Can Manage" permission
            if is_databricks_apps:
                logger.info(f"Checking Databricks 'Can Manage' permission for user {user_email}")
                
                try:
                    # Direct approach - call the permissions API directly here
                    # This is the same logic as in the debug endpoint which works
                    import aiohttp
                    
                    # Check if we have service principal credentials
                    client_id = os.getenv("DATABRICKS_CLIENT_ID")
                    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
                    
                    if client_id and client_secret:
                        # Ensure the host has https:// protocol
                        if not databricks_host.startswith(('http://', 'https://')):
                            databricks_host = f"https://{databricks_host}"
                        
                        # Get OAuth token using service principal
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
                                    access_token = oauth_data.get("access_token")
                                    
                                    if access_token:
                                        # Now check permissions
                                        url = f"{databricks_host.rstrip('/')}/api/2.0/permissions/apps/{databricks_app_name}"
                                        headers = {
                                            "Authorization": f"Bearer {access_token}",
                                            "Content-Type": "application/json"
                                        }
                                        
                                        async with aiohttp.ClientSession() as perm_session:
                                            async with perm_session.get(url, headers=headers) as response:
                                                if response.status == 200:
                                                    perm_data = await response.json()
                                                    
                                                    # Extract users with CAN_MANAGE
                                                    manage_users = []
                                                    for acl_entry in perm_data.get("access_control_list", []):
                                                        user_name = acl_entry.get("user_name")
                                                        if user_name:
                                                            permissions = acl_entry.get("all_permissions", [])
                                                            for perm in permissions:
                                                                if perm.get("permission_level") == "CAN_MANAGE":
                                                                    manage_users.append(user_name)
                                                                    break
                                                    
                                                    # Check if current user is in the list
                                                    has_permission = user_email in manage_users
                                                    permission_reason = (
                                                        "User has 'Can Manage' permission in Databricks Apps" 
                                                        if has_permission 
                                                        else f"User does not have 'Can Manage' permission. Managers: {manage_users}"
                                                    )
                                                    logger.info(f"Permission check: user={user_email}, has_permission={has_permission}, managers={manage_users}")
                                                else:
                                                    has_permission = False
                                                    permission_reason = f"Failed to fetch permissions: API returned {response.status}"
                                    else:
                                        has_permission = False
                                        permission_reason = "Failed to get OAuth token"
                                else:
                                    has_permission = False
                                    permission_reason = f"OAuth request failed with status {oauth_response.status}"
                    else:
                        # Fallback to using DatabricksRoleService if no service principal
                        if not session:
                            has_permission = False
                            permission_reason = "No service principal credentials and no session available"
                        else:
                            databricks_role_service = DatabricksRoleService(session)
                            manager_emails = await databricks_role_service.get_databricks_app_managers(user_token=user_token)
                            has_permission = user_email in manager_emails
                            permission_reason = (
                                "User has 'Can Manage' permission in Databricks Apps" 
                                if has_permission 
                                else f"User does not have 'Can Manage' permission. Managers: {manager_emails}"
                            )
                    
                except Exception as e:
                    # If we can't determine permissions, default to no access in Apps
                    logger.error(f"Error checking Databricks permissions: {e}")
                    has_permission = False
                    permission_reason = f"Could not verify 'Can Manage' permission: {str(e)}"
            
            return {
                "has_permission": has_permission,
                "is_databricks_apps": is_databricks_apps,
                "databricks_app_name": databricks_app_name,
                "user_email": user_email,
                "reason": permission_reason
            }
            
        except Exception as e:
            logger.error(f"Error checking database management permission: {e}")
            raise