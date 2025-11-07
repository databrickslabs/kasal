"""
Lakebase Service for managing Databricks Lakebase instances and configuration.
"""
import os
import uuid
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

# Try to import DatabaseInstance, but make it optional
try:
    from databricks.sdk.service.database import (
        DatabaseInstance,
        DatabaseInstanceRole,
        DatabaseInstanceRoleAttributes,
        DatabaseInstanceRoleMembershipRole,
        DatabaseInstanceRoleIdentityType
    )
    LAKEBASE_AVAILABLE = True
except ImportError:
    # Don't use logger here as it's not initialized yet
    print("Warning: DatabaseInstance not available in databricks-sdk. Lakebase features will be disabled.")
    DatabaseInstance = None
    DatabaseInstanceRole = None
    DatabaseInstanceRoleAttributes = None
    DatabaseInstanceRoleMembershipRole = None
    DatabaseInstanceRoleIdentityType = None
    LAKEBASE_AVAILABLE = False
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import asyncpg

from src.core.logger import LoggerManager
from src.config.settings import settings
from src.core.base_service import BaseService
from src.models.database_config import LakebaseConfig
from src.db.base import Base
from src.repositories.database_config_repository import DatabaseConfigRepository
from src.utils.databricks_auth import get_current_databricks_user, get_workspace_client, _databricks_auth
from src.services.lakebase_permission_service import LakebasePermissionService
from src.services.lakebase_connection_service import LakebaseConnectionService
from src.services.lakebase_schema_service import LakebaseSchemaService
from src.services.lakebase_migration_service import LakebaseMigrationService

logger_manager = LoggerManager.get_instance()
logger = logging.getLogger(__name__)


class LakebaseService(BaseService):
    """Service for managing Databricks Lakebase instances."""

    def __init__(self, session: Optional[AsyncSession] = None, user_token: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize Lakebase service.

        Args:
            session: Database session (optional for migration operations that create their own engines)
            user_token: Optional user token for Databricks authentication
            user_email: Optional user email for Lakebase authentication
        """
        if session:
            super().__init__(session)
            self.session = session
            self.config_repository = DatabaseConfigRepository(LakebaseConfig, session)
        else:
            # For operations that don't need database session (like migration with own engines)
            self.session = None
            self.config_repository = None
        self.user_token = user_token
        self.user_email = user_email

        # Initialize specialized services
        self.connection_service = LakebaseConnectionService(user_token, user_email)
        self.schema_service = LakebaseSchemaService()
        self.permission_service = LakebasePermissionService()
        self.migration_service = None  # Will be created when needed with engines

    async def get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client for Lakebase.

        Delegates to LakebaseConnectionService for authentication handling.

        Returns:
            WorkspaceClient configured with appropriate credentials
        """
        return await self.connection_service.get_workspace_client()

    async def get_config(self) -> Dict[str, Any]:
        """
        Get current Lakebase configuration.

        Returns:
            Dictionary with Lakebase configuration
        """
        try:
            config = await self.config_repository.get_by_key("lakebase")
            if config:
                return {
                    "enabled": config.value.get("enabled", False),
                    "instance_name": config.value.get("instance_name", "kasal-lakebase"),
                    "capacity": config.value.get("capacity", "CU_1"),
                    "retention_days": config.value.get("retention_days", 14),
                    "node_count": config.value.get("node_count", 1),
                    "instance_status": config.value.get("instance_status", "NOT_CREATED"),
                    "endpoint": config.value.get("endpoint"),
                    "created_at": config.value.get("created_at"),
                    "database_type": config.value.get("database_type", "lakebase")
                }
            else:
                # Return default configuration
                return {
                    "enabled": False,
                    "instance_name": "kasal-lakebase",
                    "capacity": "CU_1",
                    "retention_days": 14,
                    "node_count": 1,
                    "instance_status": "NOT_CREATED",
                    "database_type": "lakebase"
                }
        except Exception as e:
            logger.error(f"Error getting Lakebase config: {e}")
            raise

    async def save_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save or delete Lakebase configuration based on enabled state.

        Args:
            config: Configuration dictionary

        Returns:
            Saved configuration or empty dict if deleted
        """
        try:
            # If Lakebase is being disabled, delete the configuration
            if not config.get("enabled", False):
                logger.info("Lakebase disabled - deleting configuration from database")
                await self.config_repository.delete_by_key("lakebase")
                await self.session.commit()
                logger.info("Lakebase configuration deleted. System will use PostgreSQL/SQLite.")

                # Return a minimal config showing it's disabled
                return {
                    "enabled": False,
                    "instance_name": config.get("instance_name", "kasal-lakebase"),
                    "instance_status": "NOT_CREATED",
                    "message": "Lakebase disabled - using PostgreSQL/SQLite"
                }

            # Otherwise, save the configuration
            # Add timestamp if not present
            if "updated_at" not in config:
                config["updated_at"] = datetime.utcnow().isoformat()

            # Save to database using repository
            await self.config_repository.upsert("lakebase", config)
            await self.session.commit()

            logger.info(f"Lakebase configuration saved: enabled={config.get('enabled')}")
            return config
        except Exception as e:
            logger.error(f"Error saving Lakebase config: {e}")
            await self.session.rollback()
            raise

    async def create_instance(self, instance_name: str, capacity: str = "CU_1",
                            retention_days: int = 14, node_count: int = 1) -> Dict[str, Any]:
        """
        Create a new Lakebase instance.

        Args:
            instance_name: Name for the instance
            capacity: Compute capacity (CU_1, CU_2, CU_4)
            retention_days: Backup retention period
            node_count: Number of nodes for HA

        Returns:
            Instance details
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                logger.error("Lakebase features are not available. DatabaseInstance module not found.")
                raise NotImplementedError("Lakebase features are not available in the current environment")

            logger.info(f"Creating Lakebase instance: {instance_name}")

            # Check if instance already exists
            existing = await self.get_instance(instance_name)
            if existing and existing.get("state") != "NOT_FOUND":
                logger.info(f"Instance {instance_name} already exists")
                return existing

            # Create new instance using service principal
            w = await self.get_workspace_client()
            instance = w.database.create_database_instance(
                DatabaseInstance(
                    name=instance_name,
                    capacity=capacity,
                    retention_window_in_days=retention_days,
                    node_count=node_count if node_count > 1 else None
                )
            )

            # Wait for instance to be ready (async polling)
            max_wait_seconds = 300  # 5 minutes
            poll_interval = 10  # Check every 10 seconds
            elapsed = 0

            while elapsed < max_wait_seconds:
                # Re-get workspace client for each check
                w = await self.get_workspace_client()
                status = w.database.get_database_instance(name=instance_name)
                if status.state == "READY":
                    logger.info(f"Lakebase instance {instance_name} is ready")
                    break

                logger.info(f"Waiting for instance to be ready... ({elapsed}s)")
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            # Get final instance details
            w = await self.get_workspace_client()
            final_instance = w.database.get_database_instance(name=instance_name)

            result = {
                "name": final_instance.name,
                "state": final_instance.state,
                "capacity": final_instance.capacity,
                "read_write_dns": final_instance.read_write_dns,
                "created_at": datetime.utcnow().isoformat(),
                "node_count": node_count
            }

            # Save configuration using repository
            config = await self.get_config()
            config.update({
                "enabled": True,
                "instance_name": instance_name,
                "capacity": capacity,
                "retention_days": retention_days,
                "node_count": node_count,
                "instance_status": "READY",
                "endpoint": final_instance.read_write_dns,
                "created_at": result["created_at"]
            })
            await self.save_config(config)

            # After instance is ready, migrate data if needed
            await self.migrate_existing_data(instance_name, final_instance.read_write_dns)

            return result

        except Exception as e:
            error_str = str(e)
            if "workspace limit" in error_str.lower() or "quota" in error_str.lower():
                logger.warning(f"Quota limit reached: {e}")
                raise ValueError(f"Failed to create database instance because you have hit the workspace limit. Contact Databricks for quota increase.")
            logger.error(f"Error creating Lakebase instance: {e}")
            raise

    async def get_instance(self, instance_name: str) -> Optional[Dict[str, Any]]:
        """
        Get Lakebase instance details.

        Args:
            instance_name: Name of the instance

        Returns:
            Instance details or None if not found
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                logger.info("Lakebase features not available, returning NOT_FOUND state")
                return {"state": "NOT_FOUND", "name": instance_name, "message": "Lakebase not available"}

            # Check if Lakebase is configured before trying to authenticate
            config = await self.get_config()
            if not config or not config.get("enabled", False):
                logger.info(f"Lakebase not configured or disabled, returning NOT_FOUND state for instance {instance_name}")
                return {"state": "NOT_FOUND", "name": instance_name, "message": "Lakebase not configured"}

            w = await self.get_workspace_client()
            instance = w.database.get_database_instance(name=instance_name)

            return {
                "name": instance.name,
                "state": instance.state,
                "capacity": instance.capacity,
                "read_write_dns": instance.read_write_dns,
                "created_at": instance.created_at if hasattr(instance, 'created_at') else None,
                "node_count": instance.node_count if hasattr(instance, 'node_count') else 1
            }
        except Exception as e:
            error_str = str(e).lower()
            if "not_found" in error_str or "does not exist" in error_str or "resource not found" in error_str:
                logger.info(f"Lakebase instance {instance_name} not found, returning NOT_FOUND state")
                return {"state": "NOT_FOUND", "name": instance_name}
            logger.error(f"Error getting Lakebase instance: {e}")
            raise

    async def start_instance(self, instance_name: str) -> Dict[str, Any]:
        """
        Start a stopped Lakebase instance.

        Args:
            instance_name: Name of the instance to start

        Returns:
            Instance status after start attempt
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                logger.error("Lakebase features are not available")
                raise NotImplementedError("Lakebase features are not available in the current environment")

            logger.info(f"Starting Lakebase instance {instance_name}")

            # Get workspace client
            w = await self.get_workspace_client()

            # Start the instance
            w.database.start_database_instance(name=instance_name)

            # Wait for instance to be ready
            max_wait_seconds = 120  # 2 minutes
            poll_interval = 10
            elapsed = 0

            while elapsed < max_wait_seconds:
                instance = await self.get_instance(instance_name)
                if instance and instance.get("state") == "READY":
                    logger.info(f"Lakebase instance {instance_name} is now ready")

                    # Update configuration
                    config = await self.get_config()
                    config["instance_status"] = "READY"
                    await self.save_config(config)

                    return instance

                logger.info(f"Waiting for instance to start... ({elapsed}s)")
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            return {"state": "STARTING", "message": "Instance is still starting"}

        except Exception as e:
            logger.error(f"Error starting Lakebase instance: {e}")
            raise


    async def migrate_existing_data(self, instance_name: str, endpoint: str, recreate_schema: bool = False) -> Dict[str, Any]:
        """
        Migrate data from existing database (SQLite/PostgreSQL) to Lakebase.

        Args:
            instance_name: Lakebase instance name
            endpoint: Lakebase endpoint
            recreate_schema: If True, drop and recreate kasal schema before migration

        Returns:
            Migration result
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                logger.warning("Lakebase features are not available. Skipping migration.")
                return {
                    "success": False,
                    "error": "Lakebase features not available in current environment"
                }

            logger.info("=" * 80)
            logger.info("🚀 LAKEBASE MIGRATION STARTED")
            logger.info(f"Instance: {instance_name}")
            logger.info(f"Endpoint: {endpoint}")
            logger.info("=" * 80)

            # Generate credentials using connection service
            cred = await self.connection_service.generate_credentials(instance_name)

            # Note: DatabaseCredential has no 'user' attribute - we'll determine user by testing connections
            logger.info(f"🔐 Generated database credential (will test connections to determine user)")

            # Determine source database type from URI
            source_uri = str(settings.DATABASE_URI)
            # Check if it's SQLite by looking at the URI
            if 'sqlite' in source_uri.lower():
                source_db_type = "sqlite"
            elif 'postgresql' in source_uri.lower() or 'postgres' in source_uri.lower():
                source_db_type = "postgresql"
            else:
                # Fallback to DATABASE_TYPE setting
                source_db_type = settings.DATABASE_TYPE

            logger.info(f"📦 Source Database: {source_db_type}")
            logger.info(f"📦 Source URI: {source_uri}")
            logger.info(f"🎯 Target Database: Lakebase ({endpoint})")
            logger.info("-" * 60)

            # Get table list from source database
            async with self.session.begin():
                if source_db_type == "sqlite":
                    result = await self.session.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'alembic_%';")
                    )
                    tables = [row[0] for row in result]
                elif source_db_type == "postgresql":
                    # For PostgreSQL source, check both kasal and public schemas
                    result = await self.session.execute(
                        text("""
                            SELECT tablename FROM pg_tables
                            WHERE schemaname IN ('kasal', 'public')
                            AND tablename NOT LIKE 'alembic_%'
                        """)
                    )
                    tables = [row[0] for row in result]
                else:
                    logger.error(f"Unknown source database type: {source_db_type}")
                    raise ValueError(f"Unsupported source database type: {source_db_type}")

                logger.info(f"📊 Found {len(tables)} tables to migrate from {source_db_type}")
                logger.info(f"📋 Tables: {', '.join(tables[:10])}{'...' if len(tables) > 10 else ''}")

            # Test connections and get working engine + user
            connected_engine, connected_user = await self.connection_service.test_connections_async(endpoint, cred)
            if not connected_engine:
                raise Exception("Failed to connect to Lakebase with any credentials")

            lakebase_engine = connected_engine
            user_email = connected_user
            logger.info(f"Using connected engine for all operations (connected as: {user_email})")

            # Create schema using schema service
            await self.schema_service.create_schema_async(lakebase_engine, user_email, recreate_schema)

            # Create tables using schema service
            async with lakebase_engine.begin() as conn:
                await self.schema_service.set_search_path_async(conn)

            await self.schema_service.create_tables_async(lakebase_engine)
            logger.info("-" * 60)
            logger.info("📤 Starting data migration...")

            # Import json for serialization (datetime already imported at top)
            import json

            # Initialize migration service with engines
            self.migration_service = LakebaseMigrationService(
                source_engine=self.session.get_bind(),
                lakebase_engine=lakebase_engine,
                source_session=self.session
            )

            # Get sorted table list
            tables_async = await self.migration_service.get_table_list_async(self.session, source_db_type == "sqlite")
            sorted_tables = self.migration_service.get_sorted_tables(tables_async)

            # Migrate data table by table
            migrated_tables = []
            failed_tables_list = []
            total_rows = 0
            start_time = datetime.utcnow()

            for idx, table_name in enumerate(sorted_tables, 1):
                logger.info(f"[{idx}/{len(sorted_tables)}] Migrating table: {table_name}")

                # Create Lakebase session for this table
                async with AsyncSession(lakebase_engine) as lakebase_session:
                    row_count, error = await self.migration_service.migrate_table_data_async(
                        table_name, self.session, lakebase_session
                    )

                    if error:
                        failed_tables_list.append({
                            "table": table_name,
                            "error": error,
                            "error_type": error.split(':')[0] if ':' in error else "Unknown"
                        })
                    else:
                        migrated_tables.append({
                            "table": table_name,
                            "rows": row_count
                        })
                        total_rows += row_count

            # Close Lakebase engine
            await lakebase_engine.dispose()

            # Calculate migration duration
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            # Determine if migration was successful
            failed_tables = len(tables) - len(migrated_tables)
            migration_success = failed_tables == 0

            # Update configuration to mark migration status
            config = await self.get_config()
            config["migration_completed"] = migration_success
            config["migration_date"] = datetime.utcnow().isoformat()
            config["migrated_tables"] = len(migrated_tables)
            config["migrated_rows"] = total_rows
            config["failed_tables"] = failed_tables

            # If migration was successful, automatically enable Lakebase
            if migration_success:
                config["enabled"] = True
                logger.info("✅ Migration successful - Lakebase automatically enabled")

            await self.save_config(config)

            logger.info("=" * 80)
            if migration_success:
                logger.info("🎉 LAKEBASE MIGRATION COMPLETED SUCCESSFULLY!")
                logger.info("🔄 Lakebase has been automatically enabled - all future database operations will use Lakebase")
            else:
                logger.warning(f"⚠️  LAKEBASE MIGRATION COMPLETED WITH ERRORS! {failed_tables} table(s) failed.")
                logger.warning(f"")
                logger.warning(f"Failed tables:")
                for failed in failed_tables_list:
                    logger.warning(f"  • {failed['table']}: {failed['error_type']} - {failed['error']}")
                logger.warning(f"⚠️  Lakebase was NOT enabled due to migration errors")
            logger.info(f"📊 Summary:")
            logger.info(f"  • Tables migrated: {len(migrated_tables)}/{len(tables)}")
            logger.info(f"  • Total rows: {total_rows:,}")
            logger.info(f"  • Duration: {duration:.2f} seconds")
            logger.info(f"  • Instance: {instance_name}")
            logger.info(f"  • Status: READY")
            logger.info("=" * 80)

            return {
                "success": migration_success,
                "migrated_tables": migrated_tables,
                "total_tables": len(migrated_tables),
                "total_rows": total_rows,
                "failed_tables": failed_tables,
                "failed_tables_details": failed_tables_list,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error migrating data to Lakebase: {e}")
            raise

    async def migrate_existing_data_stream(self, instance_name: str, endpoint: str, recreate_schema: bool = False, migrate_data: bool = True):
        """
        Migrate data from existing database to Lakebase with streaming progress updates.

        This is a generator function that yields progress events as the migration proceeds.

        Args:
            instance_name: Name of the Lakebase instance
            endpoint: Lakebase endpoint URL
            recreate_schema: Whether to drop and recreate the schema
            migrate_data: Whether to migrate data (False = schema only)

        Args:
            instance_name: Lakebase instance name
            endpoint: Lakebase endpoint
            recreate_schema: If True, drop and recreate kasal schema before migration

        Yields:
            Dict with event type and progress information
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                yield {"type": "error", "message": "Lakebase features not available in current environment"}
                return

            yield {"type": "info", "message": "=" * 80}
            yield {"type": "info", "message": "🚀 LAKEBASE MIGRATION STARTED"}
            yield {"type": "info", "message": f"Instance: {instance_name}"}
            yield {"type": "info", "message": f"Endpoint: {endpoint}"}
            yield {"type": "info", "message": "=" * 80}

            # Generate credentials using connection service
            yield {"type": "progress", "message": "Generating database credentials...", "step": "auth"}
            cred = await self.connection_service.generate_credentials(instance_name)
            yield {"type": "success", "message": f"✅ Generated database credential"}

            yield {"type": "info", "message": f"[STREAM] Testing connection methods to determine user..."}

            # Test connections using connection service (sync version for streaming)
            test_engine, connected_user = self.connection_service.test_connections_sync(endpoint, cred)

            if not test_engine or not connected_user:
                yield {"type": "error", "message": "All connection attempts failed - could not connect to Lakebase"}
                return

            # Use the engine and user that worked
            lakebase_engine_initial = test_engine
            user_email = connected_user
            yield {"type": "info", "message": f"🔐 Using PostgreSQL role: {user_email}"}

            # Build the URL for any additional engines (but we'll primarily use the one that worked)
            lakebase_url = (
                f"postgresql+pg8000://{quote(user_email)}:{cred.token}@"
                f"{endpoint}:5432/databricks_postgres"
            )

            # Determine source database type
            source_uri = str(settings.DATABASE_URI)
            is_sqlite = 'sqlite' in source_uri.lower()

            # Connect to source database
            yield {"type": "progress", "message": "📥 Connecting to source database...", "step": "connect_source"}
            if is_sqlite:
                # For SQLite, use sync engine
                # Ensure we're using the sync sqlite driver
                source_uri_sync = source_uri.replace("sqlite+aiosqlite://", "sqlite://")
                source_engine = create_engine(source_uri_sync, echo=False)
                yield {"type": "success", "message": "✅ Connected to SQLite source database"}
            else:
                # For PostgreSQL source, use sync pg8000 driver to avoid greenlet issues
                from sqlalchemy.pool import NullPool
                source_uri_sync = source_uri.replace("postgresql+asyncpg://", "postgresql+pg8000://")
                source_engine = create_engine(source_uri_sync, echo=False, poolclass=NullPool)
                yield {"type": "success", "message": "✅ Connected to PostgreSQL source database"}

            # Use the connected engine for Lakebase
            yield {"type": "progress", "message": "📤 Connecting to Lakebase...", "step": "connect_lakebase"}
            lakebase_engine = lakebase_engine_initial
            yield {"type": "success", "message": "✅ Connected to Lakebase"}

            # Initialize migration service
            self.migration_service = LakebaseMigrationService(
                source_engine=source_engine,
                lakebase_engine=lakebase_engine,
                source_session=None
            )

            # Get table list
            yield {"type": "progress", "message": "📋 Getting table list from source...", "step": "get_tables"}
            tables = self.migration_service.get_table_list_sync(source_engine, is_sqlite)
            sorted_tables = self.migration_service.get_sorted_tables(tables)
            yield {"type": "success", "message": f"✅ Found {len(tables)} tables to migrate"}

            # Create schema and tables
            yield {"type": "progress", "message": "🏗️ Creating schema and tables...", "step": "create_schema", "total_tables": len(tables)}

            # Handle schema recreation if requested
            if recreate_schema:
                with lakebase_engine.begin() as conn:
                    try:
                        yield {"type": "progress", "message": "🗑️ Dropping existing kasal schema..."}
                        conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                        yield {"type": "success", "message": "✅ Dropped kasal schema"}
                    except Exception as drop_error:
                        yield {"type": "warning", "message": f"Could not drop schema (may be owned by different role), continuing..."}

            # Create schema using schema service
            with lakebase_engine.begin() as conn:
                self.schema_service.create_schema_sync(lakebase_engine, user_email, False)
                yield {"type": "success", "message": "✅ Created kasal schema"}

                # Grant permissions using permission service
                self.permission_service.grant_all_permissions_sync(conn, user_email)
                yield {"type": "success", "message": f"✅ Granted schema permissions to {user_email}"}

                conn.execute(text("SET search_path TO kasal"))
                yield {"type": "success", "message": "✅ Set kasal schema as default search path"}

            # Create tables with streaming
            for message in self.schema_service.create_tables_sync_stream(lakebase_engine):
                yield message

            # Check if we should migrate data
            if not migrate_data:
                # Schema-only mode - skip data migration
                yield {"type": "success", "message": "✅ Schema created successfully (data migration skipped)"}
                start_time = datetime.utcnow()
                yield {
                    "type": "result",
                    "success": True,
                    "message": "Schema creation completed",
                    "total_tables": len(tables),
                    "total_rows": 0,
                    "duration": (datetime.utcnow() - start_time).total_seconds(),
                    "migrated_tables": [],
                    "failed_tables": []
                }
                source_engine.dispose()
                lakebase_engine.dispose()
                return

            # Start data migration
            yield {"type": "progress", "message": "📤 Starting data migration...", "step": "migrate_data"}

            migrated_tables = []
            failed_tables_list = []
            total_rows = 0
            start_time = datetime.utcnow()

            # Migrate each table
            for idx, table_name in enumerate(sorted_tables, 1):
                yield {
                    "type": "table_start",
                    "message": f"Migrating table {table_name}...",
                    "table": table_name,
                    "progress": idx,
                    "total": len(tables)
                }

                row_count, error = self.migration_service.migrate_table_data_sync(
                    table_name, source_engine, lakebase_engine, is_sqlite
                )

                if error:
                    failed_tables_list.append({
                        "table": table_name,
                        "error": error,
                        "error_type": error.split(':')[0] if ':' in error else "Unknown"
                    })
                    yield {
                        "type": "table_error",
                        "message": f"❌ Error migrating table {table_name}: {error}",
                        "table": table_name,
                        "error": error,
                        "error_type": error.split(':')[0] if ':' in error else "Unknown"
                    }
                else:
                    migrated_tables.append({"table": table_name, "rows": row_count})
                    total_rows += row_count
                    yield {
                        "type": "table_complete",
                        "message": f"✓ Migrated {row_count} rows from {table_name}",
                        "table": table_name,
                        "rows": row_count,
                        "progress": idx,
                        "total": len(tables)
                    }

            # Dispose engines
            source_engine.dispose()
            lakebase_engine.dispose()

            # Calculate summary
            duration = (datetime.utcnow() - start_time).total_seconds()
            failed_tables = len(tables) - len(migrated_tables)
            migration_success = failed_tables == 0

            # Note: Config update is handled by the router after migration completes
            # We just return the result here

            # Send summary
            yield {"type": "info", "message": "=" * 80}
            if migration_success:
                yield {"type": "complete", "message": "🎉 LAKEBASE MIGRATION COMPLETED SUCCESSFULLY!"}
                yield {"type": "success", "message": "🔄 Lakebase has been automatically enabled - all future database operations will use Lakebase"}
            else:
                yield {"type": "warning", "message": f"⚠️ LAKEBASE MIGRATION COMPLETED WITH ERRORS! {failed_tables} table(s) failed."}
                if failed_tables_list:
                    yield {"type": "info", "message": "Failed tables:"}
                    for failed in failed_tables_list:
                        yield {"type": "error", "message": f"  • {failed['table']}: {failed['error_type']} - {failed['error']}"}
                yield {"type": "warning", "message": "⚠️ Lakebase was NOT enabled due to migration errors"}

            yield {"type": "info", "message": "📊 Summary:"}
            yield {"type": "info", "message": f"  • Tables migrated: {len(migrated_tables)}/{len(tables)}"}
            yield {"type": "info", "message": f"  • Total rows: {total_rows:,}"}
            yield {"type": "info", "message": f"  • Duration: {duration:.2f} seconds"}
            yield {"type": "info", "message": f"  • Instance: {instance_name}"}
            yield {"type": "info", "message": f"  • Status: READY"}
            yield {"type": "info", "message": "=" * 80}

            # Final result
            yield {
                "type": "result",
                "success": migration_success,
                "migrated_tables": migrated_tables,
                "total_tables": len(migrated_tables),
                "total_rows": total_rows,
                "failed_tables": failed_tables,
                "failed_tables_details": failed_tables_list,
                "duration": duration,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            yield {"type": "error", "message": f"Error migrating data to Lakebase: {e}"}
            import traceback
            yield {"type": "error", "message": f"Traceback: {traceback.format_exc()}"}

    @asynccontextmanager
    async def get_lakebase_session(self, instance_name: str):
        """
        Get an async session for Lakebase instance.

        Args:
            instance_name: Name of the Lakebase instance

        Yields:
            AsyncSession for Lakebase
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                raise NotImplementedError("Lakebase features are not available in the current environment")

            # Get instance details
            instance = await self.get_instance(instance_name)
            if not instance or instance.get("state") != "READY":
                raise ValueError(f"Lakebase instance {instance_name} is not ready")

            # Generate temporary token
            w = await self.get_workspace_client()
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            # Build connection string
            endpoint = instance["read_write_dns"]
            # For asyncpg, don't use sslmode in URL
            # Determine username:
            # - If user email provided (e.g., from request context), use it
            # - Otherwise: Get the actual authenticated user's identity
            if self.user_email:
                username = quote(self.user_email)
                logger.info(f"Using provided email for Lakebase: {self.user_email}")
            else:
                # Get the actual authenticated user's identity using centralized method
                # Pass the user token if we have it (for OBO), otherwise it will use PAT
                current_user_identity, error = await get_current_databricks_user(self.user_token)
                if error or not current_user_identity:
                    logger.error(f"Failed to get current user identity: {error}")
                    raise Exception(f"Cannot determine Databricks user identity: {error}")

                username = quote(current_user_identity)
                logger.info(f"Using authenticated user identity for Lakebase: {current_user_identity}")

            connection_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
                f"{endpoint}:5432/databricks_postgres"
            )

            # Create engine and session with SSL configuration for asyncpg
            engine = create_async_engine(
                connection_url,
                echo=False,
                connect_args={
                    "ssl": "require",  # Enable SSL for asyncpg
                    "server_settings": {
                        "jit": "off"  # Disable JIT for compatibility
                    }
                }
            )
            async with AsyncSession(engine) as session:
                yield session

            # Cleanup
            await engine.dispose()

        except Exception as e:
            logger.error(f"Error creating Lakebase session: {e}")
            raise

    async def check_lakebase_tables(self) -> Dict[str, Any]:
        """
        Check what tables exist in Lakebase database.

        Args:
            instance_name: Lakebase instance name

        Returns:
            List of tables and their row counts
        """
        try:
            # Check if Lakebase is available
            if not LAKEBASE_AVAILABLE:
                logger.warning("Lakebase features not available")
                return {
                    "success": False,
                    "error": "Lakebase features not available in current environment"
                }

            logger.info(f"Checking tables in Lakebase instance {instance_name}")

            # Get instance details
            instance = await self.get_instance(instance_name)
            if not instance or instance.get("state") == "NOT_FOUND":
                return {
                    "success": False,
                    "error": "Instance not found"
                }

            endpoint = instance.get("read_write_dns")
            if not endpoint:
                return {
                    "success": False,
                    "error": "Instance has no endpoint"
                }

            # Generate temporary token for connection
            w = await self.get_workspace_client()
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            # Get user identity
            if self.user_email:
                username = quote(self.user_email)
            else:
                current_user_identity, error = await get_current_databricks_user(self.user_token)
                if error or not current_user_identity:
                    return {
                        "success": False,
                        "error": f"Cannot determine user identity: {error}"
                    }
                username = quote(current_user_identity)

            # Build connection URL
            lakebase_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
                f"{endpoint}:5432/databricks_postgres"
            )

            # Create engine with SSL
            engine = create_async_engine(
                lakebase_url,
                echo=False,
                connect_args={
                    "ssl": "require",
                    "server_settings": {"jit": "off"}
                }
            )

            tables_info = []

            try:
                async with engine.begin() as conn:
                    # Query to get all tables in public schema
                    result = await conn.execute(text("""
                        SELECT
                            schemaname,
                            tablename
                        FROM pg_tables
                        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY schemaname, tablename;
                    """))

                    tables = result.fetchall()

                    # Get row count for each table
                    for schema, table in tables:
                        try:
                            count_result = await conn.execute(
                                text(f"SELECT COUNT(*) FROM {schema}.{table}")
                            )
                            count = count_result.scalar()
                            tables_info.append({
                                "schema": schema,
                                "table": table,
                                "row_count": count
                            })
                        except Exception as e:
                            tables_info.append({
                                "schema": schema,
                                "table": table,
                                "row_count": -1,
                                "error": str(e)
                            })

                    # Also check if alembic_version exists (for migration tracking)
                    alembic_result = await conn.execute(text("""
                        SELECT EXISTS (
                            SELECT FROM pg_tables
                            WHERE schemaname = 'public'
                            AND tablename = 'alembic_version'
                        );
                    """))
                    has_alembic = alembic_result.scalar()

                    # Check for Kasal-specific tables
                    kasal_tables = [
                        'users', 'groups', 'agents', 'tasks', 'crews',
                        'workflows', 'executions', 'llm_logs', 'configurations'
                    ]

                    existing_kasal_tables = []
                    for table_name in kasal_tables:
                        check_result = await conn.execute(text(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_tables
                                WHERE schemaname = 'public'
                                AND tablename = '{table_name}'
                            );
                        """))
                        if check_result.scalar():
                            existing_kasal_tables.append(table_name)

            finally:
                await engine.dispose()

            return {
                "success": True,
                "instance_name": instance_name,
                "endpoint": endpoint,
                "total_tables": len(tables_info),
                "tables": tables_info,
                "has_alembic_version": has_alembic,
                "kasal_tables_found": existing_kasal_tables,
                "kasal_tables_missing": [t for t in kasal_tables if t not in existing_kasal_tables],
                "summary": {
                    "total_schemas": len(set(t["schema"] for t in tables_info)),
                    "public_schema_tables": len([t for t in tables_info if t["schema"] == "public"]),
                    "total_rows": sum(t.get("row_count", 0) for t in tables_info if t.get("row_count", 0) > 0)
                }
            }

        except Exception as e:
            logger.error(f"Error checking Lakebase tables: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def test_connection(self, instance_name: str) -> Dict[str, Any]:
        """
        Test connection to Lakebase instance and check migration status.

        Args:
            instance_name: Name of the instance

        Returns:
            Connection test result with migration status
        """
        try:
            async with self.get_lakebase_session(instance_name) as session:
                # Test query
                result = await session.execute(text("SELECT version()"))
                version = result.scalar()

                # Check if kasal schema exists
                schema_result = await session.execute(
                    text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'kasal'")
                )
                has_kasal_schema = schema_result.scalar() is not None

                # Get table count from kasal schema if it exists, otherwise from public
                if has_kasal_schema:
                    table_result = await session.execute(
                        text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'kasal'")
                    )
                else:
                    table_result = await session.execute(
                        text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                    )
                table_count = table_result.scalar()

                # Determine migration status based on schema and table presence
                migration_needed = not has_kasal_schema or table_count == 0
                migration_status = 'pending' if migration_needed else 'completed'

                return {
                    "success": True,
                    "version": version,
                    "table_count": table_count,
                    "instance_name": instance_name,
                    "has_kasal_schema": has_kasal_schema,
                    "migration_needed": migration_needed,
                    "migration_status": migration_status
                }

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def get_workspace_info(self) -> Dict[str, Any]:
        """
        Get Databricks workspace URL and organization ID for Lakebase links.

        Returns:
            Dictionary with workspace_url and organization_id

        Raises:
            HTTPException: If Lakebase is not enabled
        """
        from fastapi import HTTPException

        # Check if Lakebase is enabled
        config = await self.get_config()
        if not config.get("enabled", False):
            raise HTTPException(
                status_code=400,
                detail="Lakebase is not enabled. Please configure and enable Lakebase first."
            )

        # Get workspace client
        w = await self.get_workspace_client()

        # Get workspace ID (organization ID)
        workspace_id = w.get_workspace_id()

        # Get workspace URL from config
        workspace_url = w.config.host

        # Clean up the URL
        if workspace_url and workspace_url.endswith('/'):
            workspace_url = workspace_url[:-1]

        return {
            "success": True,
            "workspace_url": workspace_url,
            "organization_id": str(workspace_id)
        }

    async def enable_lakebase(self, instance_name: str, endpoint: str) -> Dict[str, Any]:
        """
        Enable Lakebase without performing data migration.
        This sets the 'enabled' flag in configuration, allowing connection to Lakebase
        where schema will be created on first use.

        Args:
            instance_name: Name of the Lakebase instance
            endpoint: Lakebase connection endpoint

        Returns:
            Success status and configuration
        """
        # Get current config
        config = await self.get_config()

        # Update config with instance details
        config["instance_name"] = instance_name
        config["endpoint"] = endpoint
        config["enabled"] = True
        config["migration_completed"] = True  # Mark as ready even without migration

        # Save updated config
        await self.save_config(config)

        # Dispose existing database connections to force reconnection to Lakebase
        from src.db.session import dispose_engines
        await dispose_engines()
        logger.info("Disposed existing database connections to switch to Lakebase")

        return {
            "success": True,
            "message": "Lakebase enabled successfully. All connections switched to Lakebase.",
            "config": config
        }
