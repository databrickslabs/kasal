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
# DatabricksRoleService import removed - no longer checking Can Manage permission
# Future: Will implement admin group check
# Session is now injected via dependency injection, not created here

logger = LoggerManager.get_instance().system


class DatabaseManagementService:
    """Service for managing database export and import operations with Databricks volumes."""
    
    def __init__(self, session: AsyncSession, repository: Optional[DatabaseBackupRepository] = None, user_token: Optional[str] = None):
        """
        Initialize the service with a session and repository.

        Args:
            session: Database session from dependency injection
            repository: Database backup repository instance
            user_token: Optional user token for OBO authentication (used in Databricks Apps)
        """
        # Store the injected session
        self.session = session

        # Authentication strategy: Always use OBO when user token is available
        # Falls back to Service Principal or PAT if no user token provided
        logger.info(f"Database Management: User token provided: {bool(user_token)}")
        if user_token:
            logger.info("Database Management: Using OBO authentication with user token")
        else:
            logger.info("Database Management: No user token - will use Service Principal or PAT fallback")

        self.repository = repository or DatabaseBackupRepository(session=session, user_token=user_token)
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
                # Get SQLite database path from settings, fallback to default
                db_path = settings.SQLITE_DB_PATH
                if not db_path:
                    db_path = "./app.db"  # Default SQLite path

                # Ensure absolute path
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
                # Determine file extension based on export format
                if export_format == "sqlite":
                    backup_filename = f"kasal_backup_{timestamp}.db"
                    postgres_export_format = "sqlite"
                else:  # Default to SQL
                    backup_filename = f"kasal_backup_{timestamp}.sql"
                    postgres_export_format = "sql"

                # Use provided session or injected session for PostgreSQL
                if session:
                    # Use provided session parameter
                    backup_result = await self.repository.create_postgres_backup(
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename,
                        export_format=postgres_export_format,
                        session=session
                    )
                else:
                    # Use injected session from constructor
                    backup_result = await self.repository.create_postgres_backup(
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename,
                        export_format=postgres_export_format,
                        session=self.session
                    )

                # For PostgreSQL, we don't have an original file size
                original_size_mb = None
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
            
            if not backup_result["success"]:
                return backup_result
            
            backup_size_mb = backup_result["backup_size"] / (1024 * 1024)  # Size in MB

            # Generate Databricks URL for the volume using unified auth
            workspace_url = ""
            try:
                from src.utils.databricks_auth import get_auth_context
                auth = await get_auth_context()
                if auth and auth.workspace_url:
                    workspace_url = auth.workspace_url.rstrip("/")
            except Exception:
                pass
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
                # Get SQLite database path from settings, fallback to default
                db_path = settings.SQLITE_DB_PATH
                if not db_path:
                    db_path = "./app.db"  # Default SQLite path

                # Ensure absolute path
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
                # Use provided session or injected session for PostgreSQL
                if session:
                    # Use provided session parameter
                    restore_result = await self.repository.restore_postgres_backup(
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename,
                        session=session
                    )
                else:
                    # Use injected session from constructor
                    restore_result = await self.repository.restore_postgres_backup(
                        catalog=catalog,
                        schema=schema,
                        volume_name=volume_name,
                        backup_filename=backup_filename,
                        session=self.session
                    )
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
            
            if not restore_result["success"]:
                return restore_result

            # CRITICAL: Dispose pool after SQLite import to invalidate stale connections
            # DO NOT close session here - it causes corruption when SQLite tries to write cleanup
            # operations using the old file descriptor after the database file was replaced
            # Let FastAPI's context manager close the session naturally after response is sent
            if db_type == 'sqlite':
                try:
                    # Dispose the engine pool to mark it for disposal
                    # This ensures subsequent requests get fresh connections to the new database file
                    from src.db.session import engine
                    await engine.dispose()
                    logger.info("Database connection pool disposed - next requests will connect to new database file")
                except Exception as dispose_error:
                    logger.warning(f"Failed to dispose connection pool: {dispose_error}")
                    # Continue anyway - import was successful

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

            # Generate Databricks URLs for each backup using unified auth
            workspace_url = ""
            try:
                from src.utils.databricks_auth import get_auth_context
                import asyncio
                auth = asyncio.run(get_auth_context())
                if auth and auth.workspace_url:
                    workspace_url = auth.workspace_url.rstrip("/")
            except Exception:
                pass
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
            # Check if Lakebase is enabled
            # IMPORTANT: Read config directly from database_router to avoid circular dependency
            lakebase_enabled = False
            lakebase_instance = None
            lakebase_config = {}
            try:
                from src.db.database_router import get_lakebase_config_from_db

                # Use database_router's function which properly handles fallback DB
                lakebase_config = await get_lakebase_config_from_db()
                if lakebase_config:
                    # Lakebase is only truly enabled if migration is completed
                    # This matches the logic in database_router.is_lakebase_enabled()
                    lakebase_enabled = (
                        lakebase_config.get("enabled", False) and
                        lakebase_config.get("endpoint") and
                        lakebase_config.get("migration_completed", False)
                    )
                    lakebase_instance = lakebase_config.get("instance_name")
            except Exception as e:
                logger.warning(f"Could not check Lakebase status: {e}")

            db_type = DatabaseBackupRepository.get_database_type()

            # Check the actual session type to determine if we're really using Lakebase
            # Even if Lakebase is configured, the session might be SQLite if connection failed
            actual_session_db_type = 'unknown'
            if self.session and self.session.bind:
                db_url = str(self.session.bind.url)
                if 'sqlite' in db_url.lower():
                    actual_session_db_type = 'sqlite'
                elif 'postgresql' in db_url.lower() or 'postgres' in db_url.lower():
                    actual_session_db_type = 'postgres'

            logger.debug(f"[SERVICE] lakebase_enabled={lakebase_enabled}, db_type={db_type}, actual_session_db_type={actual_session_db_type}")

            # Check Lakebase FIRST - if enabled AND session is actually PostgreSQL
            if lakebase_enabled and actual_session_db_type == 'postgres':
                logger.debug(f"[SERVICE] Taking Lakebase path - passing session to repository")
                # Use provided session or injected session for Lakebase
                db_session = session if session else self.session

                # Use repository to get database info from Lakebase
                info_result = await self.repository.get_database_info(session=db_session)

                if not info_result["success"]:
                    return info_result

                # Format result for Lakebase
                result = {
                    "success": True,
                    "database_type": "lakebase",
                    "tables": info_result.get("tables", {}),
                    "total_tables": info_result.get("total_tables", 0),
                    "memory_backends": info_result.get("memory_backends", []),
                    "lakebase_enabled": True,
                    "lakebase_instance": lakebase_instance
                }

                # Lakebase-specific information (no file path/size like SQLite)
                lakebase_endpoint = lakebase_config.get("endpoint", "")
                if lakebase_endpoint:
                    result["lakebase_endpoint"] = lakebase_endpoint

            elif db_type == 'sqlite':
                # Get SQLite database path from settings, fallback to default
                db_path = settings.SQLITE_DB_PATH

                if not db_path:
                    db_path = "./app.db"  # Default SQLite path

                # Ensure absolute path
                if not os.path.isabs(db_path):
                    db_path = os.path.abspath(db_path)

                # Use repository to get database info
                info_result = await self.repository.get_database_info(db_path=db_path)

                if not info_result["success"]:
                    return info_result

                result = {
                    "success": True,
                    "database_type": "sqlite",
                    "tables": info_result.get("tables", {}),
                    "total_tables": info_result.get("total_tables", 0),
                    "memory_backends": info_result.get("memory_backends", [])
                }

                # Add SQLite-specific information
                if 'size' in info_result:
                    result["size_mb"] = round(info_result["size"] / (1024 * 1024), 2)
                if 'created_at' in info_result:
                    result["created_at"] = info_result["created_at"].isoformat()
                if 'modified_at' in info_result:
                    result["modified_at"] = info_result["modified_at"].isoformat()
                if 'path' in info_result:
                    result["database_path"] = info_result["path"]

            elif db_type == 'postgres':
                # Use provided session or injected session for PostgreSQL
                db_session = session if session else self.session

                # Use repository to get database info
                info_result = await self.repository.get_database_info(session=db_session)

                if not info_result["success"]:
                    return info_result

                result = {
                    "success": True,
                    "database_type": "postgres",
                    "tables": info_result.get("tables", {}),
                    "total_tables": info_result.get("total_tables", 0),
                    "memory_backends": info_result.get("memory_backends", [])
                }

            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }

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

        Current logic: Always allow access for all users
        Future: Will check if user belongs to admin group/role

        Args:
            user_email: Email of the user to check
            session: Optional database session

        Returns:
            Permission status and environment info
        """
        try:
            # For now, always grant access to all users
            # TODO: In the future, check if user belongs to admin group
            has_permission = True
            permission_reason = "Database Management is available to all users (admin group check coming in future)"

            logger.info(f"Database Management permission check for {user_email}: GRANTED (no restrictions currently)")

            return {
                "has_permission": has_permission,
                "user_email": user_email,
                "reason": permission_reason
            }

        except Exception as e:
            logger.error(f"Error checking database management permission: {e}")
            # Even on error, allow access for now
            return {
                "has_permission": True,
                "user_email": user_email,
                "reason": "Permission check failed - defaulting to allow access"
            }