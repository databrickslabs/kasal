"""
Lakebase Service for managing Databricks Lakebase instances and configuration.
"""
import os
import re
import time
import uuid
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

# Migration timeout constants
TABLE_MIGRATION_TIMEOUT_SECONDS = 1800
STATEMENT_TIMEOUT_MS = 1_800_000

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
    print("Warning: DatabaseInstance not available in databricks-sdk. Lakebase features will be disabled.")
    DatabaseInstance = None
    DatabaseInstanceRole = None
    DatabaseInstanceRoleAttributes = None
    DatabaseInstanceRoleMembershipRole = None
    DatabaseInstanceRoleIdentityType = None
    LAKEBASE_AVAILABLE = False
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.config.settings import settings
from src.core.base_service import BaseService
from src.models.database_config import LakebaseConfig
from src.db.base import Base
from src.repositories.database_config_repository import DatabaseConfigRepository
from src.utils.databricks_auth import get_workspace_client
from src.services.lakebase_permission_service import LakebasePermissionService
from src.services.lakebase_connection_service import LakebaseConnectionService
from src.services.lakebase_schema_service import LakebaseSchemaService
from src.services.lakebase_migration_service import LakebaseMigrationService

logger_manager = LoggerManager.get_instance()
logger = logging.getLogger(__name__)

# --- SQL injection prevention helpers ---
_SAFE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Validate a SQL identifier (schema name, table name) to prevent injection.

    Only allows simple identifiers matching ``^[A-Za-z_][A-Za-z0-9_]*$``.

    Raises:
        ValueError: If the name does not match the safe pattern.
    """
    if not name or not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: {name!r}")
    return name



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

    async def list_instances(self) -> List[Dict[str, Any]]:
        """
        List all available Lakebase database instances.

        Returns:
            List of instance dicts with name, state, capacity, read_write_dns, node_count
        """
        if not LAKEBASE_AVAILABLE:
            logger.warning("Lakebase features not available, cannot list instances")
            return []

        try:
            w = await self.get_workspace_client()
            instances = w.database.list_database_instances()

            result = []
            for inst in instances:
                result.append({
                    "name": inst.name,
                    "state": inst.state if hasattr(inst, 'state') else None,
                    "capacity": inst.capacity if hasattr(inst, 'capacity') else None,
                    "read_write_dns": inst.read_write_dns if hasattr(inst, 'read_write_dns') else None,
                    "node_count": inst.node_count if hasattr(inst, 'node_count') else None,
                })
            return result
        except Exception as e:
            logger.error(f"Error listing Lakebase instances: {e}")
            raise

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

            # Don't auto-migrate here — the migration dialog (streaming endpoint)
            # handles this separately, giving the user control over the strategy
            # and showing real-time progress.

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

            # Use SPN client_id as PG username (deterministic, no guessing)
            user_email = await self.connection_service.get_username()
            logger.info(f"🔐 Using PG username: {user_email}")

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

            # Create async engine with deterministic SPN auth
            lakebase_engine = await self.connection_service.create_lakebase_engine_async(
                endpoint, user_email, cred.token
            )

            # Verify connection
            async with lakebase_engine.connect() as verify_conn:
                result = await verify_conn.execute(text("SELECT current_user"))
                connected_user = result.scalar()
                logger.info(f"Connected to Lakebase as: {connected_user}")

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

            # Use SPN client_id as PG username (deterministic, no guessing)
            user_email = await self.connection_service.get_username()
            yield {"type": "info", "message": f"🔐 Using PG username: {user_email}"}

            # Create sync engine with deterministic auth and statement timeout
            lakebase_engine_initial = self.connection_service.create_lakebase_engine_sync(
                endpoint, user_email, cred.token,
                statement_timeout_ms=STATEMENT_TIMEOUT_MS
            )

            # Verify connection
            try:
                with lakebase_engine_initial.connect() as verify_conn:
                    result = verify_conn.execute(text("SELECT current_user"))
                    connected_user = result.scalar()
                    yield {"type": "success", "message": f"Connected to Lakebase as: {connected_user}"}
            except Exception as conn_err:
                yield {"type": "error", "message": f"Failed to connect to Lakebase as '{user_email}': {conn_err}"}
                lakebase_engine_initial.dispose()
                return

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
                yield {"type": "progress", "message": "🗑️ Dropping existing kasal schema..."}
                try:
                    with lakebase_engine.begin() as conn:
                        conn.execute(text(f"ALTER SCHEMA kasal OWNER TO \"{user_email}\""))
                except Exception:
                    pass  # Schema may not exist yet — that's fine
                try:
                    with lakebase_engine.begin() as conn:
                        conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                    yield {"type": "success", "message": "✅ Dropped kasal schema"}
                except Exception as drop_error:
                    yield {"type": "warning", "message": f"Could not drop schema: {drop_error}. Continuing..."}

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
                # Schema-only mode - skip data migration, but run seeders
                yield {"type": "success", "message": "✅ Schema created successfully (data migration skipped)"}

                # Run seeders on the new Lakebase instance so it has default data
                yield {"type": "progress", "message": "🌱 Running database seeders on new instance...", "step": "seed"}
                try:
                    from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
                    from sqlalchemy.ext.asyncio import async_sessionmaker as _async_sessionmaker
                    from src.seeds.seed_runner import run_seeders_with_factory

                    # Create async engine for Lakebase with kasal search_path
                    lakebase_async_engine = _create_async_engine(
                        f"postgresql+asyncpg://{user_email}:{cred.token}@"
                        f"{endpoint}:5432/databricks_postgres",
                        echo=False,
                        connect_args={
                            "ssl": "require",
                            "server_settings": {
                                "jit": "off",
                                "search_path": "kasal",
                            },
                        },
                    )

                    lakebase_seed_factory = _async_sessionmaker(
                        lakebase_async_engine,
                        expire_on_commit=False,
                        autoflush=False,
                        autocommit=False,
                    )

                    # Run all seeders except documentation (slow, uses embeddings)
                    await run_seeders_with_factory(
                        lakebase_seed_factory, exclude={"documentation"}
                    )

                    await lakebase_async_engine.dispose()
                    yield {"type": "success", "message": "✅ Database seeders completed successfully"}
                except Exception as seed_error:
                    logger.error(f"Error running seeders on Lakebase: {seed_error}")
                    import traceback as tb
                    logger.error(tb.format_exc())
                    yield {"type": "warning", "message": f"⚠️ Seeders encountered errors (non-critical): {seed_error}"}

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
            migrated_count = 0
            table_durations: Dict[str, float] = {}  # table_name -> seconds

            # Group tables into parallel waves based on FK dependencies
            from concurrent.futures import ThreadPoolExecutor
            migration_waves = self.migration_service.get_migration_waves(sorted_tables)
            logger.info(f"Data migration: {len(sorted_tables)} tables in {len(migration_waves)} waves")
            max_parallel = 8  # max concurrent table migrations

            for wave_idx, wave_tables in enumerate(migration_waves):
                if len(wave_tables) <= 1:
                    # Single table - no parallelism needed
                    for table_name in wave_tables:
                        migrated_count += 1
                        yield {
                            "type": "table_start",
                            "message": f"Migrating table {table_name}...",
                            "table": table_name,
                            "progress": migrated_count,
                            "total": len(sorted_tables)
                        }

                        t0 = time.monotonic()
                        loop = asyncio.get_event_loop()
                        row_count, error = await loop.run_in_executor(
                            None,
                            self.migration_service.migrate_table_data_sync,
                            table_name, source_engine, lakebase_engine, is_sqlite
                        )
                        elapsed = time.monotonic() - t0
                        table_durations[table_name] = elapsed

                        if error:
                            failed_tables_list.append({
                                "table": table_name,
                                "error": error,
                                "error_type": error.split(':')[0] if ':' in error else "Unknown"
                            })
                            yield {
                                "type": "table_error",
                                "message": f"❌ Error migrating table {table_name} ({elapsed:.1f}s): {error}",
                                "table": table_name,
                                "error": error,
                                "error_type": error.split(':')[0] if ':' in error else "Unknown",
                                "duration": round(elapsed, 2)
                            }
                        else:
                            migrated_tables.append({"table": table_name, "rows": row_count})
                            total_rows += row_count
                            yield {
                                "type": "table_complete",
                                "message": f"✓ Migrated {row_count} rows from {table_name} ({elapsed:.1f}s)",
                                "table": table_name,
                                "rows": row_count,
                                "progress": migrated_count,
                                "total": len(sorted_tables),
                                "duration": round(elapsed, 2)
                            }
                else:
                    # Parallel migration for this wave
                    from concurrent.futures import wait, FIRST_COMPLETED
                    n_workers = min(len(wave_tables), max_parallel)
                    yield {
                        "type": "info",
                        "message": f"⚡ Wave {wave_idx + 1}: migrating {len(wave_tables)} tables in parallel (timeout={TABLE_MIGRATION_TIMEOUT_SECONDS}s)"
                    }

                    with ThreadPoolExecutor(max_workers=n_workers) as executor:
                        table_start_times: Dict[str, float] = {}
                        futures = {}
                        for tname in wave_tables:
                            table_start_times[tname] = time.monotonic()
                            fut = executor.submit(
                                self.migration_service.migrate_table_data_sync,
                                tname, source_engine, lakebase_engine, is_sqlite
                            )
                            futures[fut] = tname

                        pending = set(futures.keys())
                        deadline = time.monotonic() + TABLE_MIGRATION_TIMEOUT_SECONDS
                        heartbeat_interval = 15.0  # seconds between heartbeat messages

                        last_heartbeat = time.monotonic()

                        while pending:
                            remaining_time = deadline - time.monotonic()
                            if remaining_time <= 0:
                                # Deadline exceeded — report all pending as timed out
                                in_flight_names = sorted(futures[f] for f in pending)
                                logger.error(
                                    f"Wave {wave_idx + 1} TIMEOUT after {TABLE_MIGRATION_TIMEOUT_SECONDS}s. "
                                    f"Timed-out tables: {in_flight_names}"
                                )
                                for fut in list(pending):
                                    tname = futures[fut]
                                    elapsed = time.monotonic() - table_start_times[tname]
                                    table_durations[tname] = elapsed
                                    fut.cancel()
                                    migrated_count += 1
                                    timeout_error = f"TIMEOUT migrating table {tname} after {elapsed:.1f}s"
                                    failed_tables_list.append({
                                        "table": tname,
                                        "error": timeout_error,
                                        "error_type": "TimeoutError"
                                    })
                                    yield {
                                        "type": "table_error",
                                        "message": f"⏰ {timeout_error}",
                                        "table": tname,
                                        "error": timeout_error,
                                        "error_type": "TimeoutError",
                                        "duration": round(elapsed, 2)
                                    }
                                break

                            # Non-blocking poll: use a short timeout so we don't starve the event loop.
                            # This keeps the rest of the API responsive during long table migrations.
                            done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                            await asyncio.sleep(0)  # yield to event loop

                            if not done:
                                # Periodically emit heartbeat
                                if time.monotonic() - last_heartbeat >= heartbeat_interval:
                                    last_heartbeat = time.monotonic()
                                    in_flight_names = sorted(futures[f] for f in pending)
                                    wave_elapsed = time.monotonic() - table_start_times[in_flight_names[0]]
                                    yield {
                                        "type": "info",
                                        "message": f"⏳ Waiting for {len(pending)} tables ({wave_elapsed:.0f}s elapsed): {', '.join(in_flight_names)}"
                                    }
                                continue

                            # Process completed futures
                            for future in done:
                                table_name = futures[future]
                                elapsed = time.monotonic() - table_start_times[table_name]
                                table_durations[table_name] = elapsed
                                migrated_count += 1

                                try:
                                    row_count, error = future.result()
                                    if error:
                                        failed_tables_list.append({
                                            "table": table_name,
                                            "error": error,
                                            "error_type": error.split(':')[0] if ':' in error else "Unknown"
                                        })
                                        yield {
                                            "type": "table_error",
                                            "message": f"❌ Error migrating table {table_name} ({elapsed:.1f}s): {error}",
                                            "table": table_name,
                                            "error": error,
                                            "error_type": error.split(':')[0] if ':' in error else "Unknown",
                                            "duration": round(elapsed, 2)
                                        }
                                    else:
                                        migrated_tables.append({"table": table_name, "rows": row_count})
                                        total_rows += row_count
                                        yield {
                                            "type": "table_complete",
                                            "message": f"✓ Migrated {row_count} rows from {table_name} ({elapsed:.1f}s)",
                                            "table": table_name,
                                            "rows": row_count,
                                            "progress": migrated_count,
                                            "total": len(sorted_tables),
                                            "duration": round(elapsed, 2)
                                        }
                                except Exception as e:
                                    failed_tables_list.append({
                                        "table": table_name,
                                        "error": str(e),
                                        "error_type": type(e).__name__
                                    })
                                    yield {
                                        "type": "table_error",
                                        "message": f"❌ Error migrating table {table_name} ({elapsed:.1f}s): {e}",
                                        "table": table_name,
                                        "error": str(e),
                                        "error_type": type(e).__name__,
                                        "duration": round(elapsed, 2)
                                    }

                            if pending:
                                in_flight_names = sorted(futures[f] for f in pending)
                                logger.info(f"Wave {wave_idx + 1}: {len(pending)} tables still in-flight: {in_flight_names}")

            # Reset PostgreSQL sequences after data migration
            # (bulk inserts with explicit IDs leave sequences out of sync)
            if migrated_tables:
                yield {"type": "progress", "message": "🔄 Resetting database sequences..."}
                try:
                    loop = asyncio.get_event_loop()
                    seq_results = await loop.run_in_executor(
                        None,
                        self.migration_service.reset_sequences_sync,
                        lakebase_engine,
                        [t["table"] for t in migrated_tables]
                    )
                    reset_count = sum(1 for _, ok, _ in seq_results if ok)
                    if reset_count > 0:
                        yield {"type": "success", "message": f"✅ Reset {reset_count} database sequence(s)"}
                    failed_seqs = [(n, e) for n, ok, e in seq_results if not ok]
                    for seq_name, err in failed_seqs:
                        yield {"type": "warning", "message": f"⚠️ Could not reset sequence {seq_name}: {err}"}
                except Exception as e:
                    logger.error(f"Error resetting sequences: {e}")
                    yield {"type": "warning", "message": f"⚠️ Sequence reset encountered an error: {e}"}

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
            yield {"type": "info", "message": f"  • Timeout per table: {TABLE_MIGRATION_TIMEOUT_SECONDS}s"}
            yield {"type": "info", "message": f"  • Instance: {instance_name}"}
            yield {"type": "info", "message": f"  • Status: READY"}

            # Show 5 slowest tables for observability
            if table_durations:
                slowest = sorted(table_durations.items(), key=lambda x: x[1], reverse=True)[:5]
                yield {"type": "info", "message": "  ⏱️ Slowest tables:"}
                for tbl, dur in slowest:
                    yield {"type": "info", "message": f"    • {tbl}: {dur:.2f}s"}

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
                "timeout_seconds": TABLE_MIGRATION_TIMEOUT_SECONDS,
                "timestamp": datetime.utcnow().isoformat()
            }

        except (asyncio.CancelledError, GeneratorExit):
            logger.warning("Migration stream cancelled (client disconnected)")
            return
        except Exception as e:
            logger.error(f"Error migrating data to Lakebase: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            try:
                yield {"type": "error", "message": f"Error migrating data to Lakebase: {e}"}
                yield {"type": "error", "message": f"Traceback: {traceback.format_exc()}"}
            except (GeneratorExit, asyncio.CancelledError):
                return

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
            ready_states = {"READY", "AVAILABLE", "RUNNING"}
            raw_state = instance.get("state") if instance else None
            instance_state = str(raw_state).upper() if raw_state else ""
            if not instance or instance_state not in ready_states:
                raise ValueError(f"Lakebase instance {instance_name} is not ready (state: {raw_state})")

            # Generate temporary token
            w = await self.get_workspace_client()
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            # Use SPN client_id as PG username (deterministic)
            endpoint = instance["read_write_dns"]
            username = await self.connection_service.get_username()
            logger.info(f"Using PG username for Lakebase session: {username}")

            # Create engine using connection service
            engine = await self.connection_service.create_lakebase_engine_async(
                endpoint, username, cred.token
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

            # Get instance name from config
            config = await self.get_config()
            instance_name = config.get("instance_name", "kasal-lakebase")
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

            # Use SPN client_id as PG username (deterministic)
            username = await self.connection_service.get_username()
            cred = await self.connection_service.generate_credentials(instance_name)

            # Create engine using connection service
            engine = await self.connection_service.create_lakebase_engine_async(
                endpoint, username, cred.token
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
                            safe_schema = _validate_identifier(schema, "schema name")
                            safe_table = _validate_identifier(table, "table name")
                            count_result = await conn.execute(
                                text(f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}")
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
                        check_result = await conn.execute(
                            text("""
                                SELECT EXISTS (
                                    SELECT FROM pg_tables
                                    WHERE schemaname = 'public'
                                    AND tablename = :tname
                                );
                            """),
                            {"tname": table_name}
                        )
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

        This method connects directly without requiring Lakebase to be
        already enabled in config, so it works during initial setup.

        Args:
            instance_name: Name of the instance

        Returns:
            Connection test result with migration status
        """
        try:
            if not LAKEBASE_AVAILABLE:
                raise NotImplementedError("Lakebase features are not available in the current environment")

            # Get instance details directly (bypass enabled check)
            w = await self.get_workspace_client()
            instance = w.database.get_database_instance(name=instance_name)
            raw_state = instance.state if hasattr(instance, 'state') else None
            state = str(raw_state.value if hasattr(raw_state, 'value') else raw_state or '').upper()
            ready_states = {"READY", "AVAILABLE", "RUNNING"}
            if not state or state not in ready_states:
                raise ValueError(
                    f"Lakebase instance {instance_name} is in state '{state}'. "
                    f"Expected one of: {', '.join(sorted(ready_states))}"
                )

            endpoint = instance.read_write_dns
            if not endpoint:
                raise ValueError(f"Lakebase instance {instance_name} has no endpoint")

            # Generate temporary token
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            username = await self.connection_service.get_username()
            engine = await self.connection_service.create_lakebase_engine_async(
                endpoint, username, cred.token
            )

            async with AsyncSession(engine) as session:
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
            error_msg = str(e)

            # Detect missing postgres scope — the SPN needs a Database resource added to the App
            if "required scopes: postgres" in error_msg.lower():
                client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": "MISSING_DATABASE_RESOURCE",
                    "client_id": client_id,
                }

            return {
                "success": False,
                "error": error_msg,
            }

    async def get_workspace_info(self) -> Dict[str, Any]:
        """
        Get Databricks workspace URL and organization ID for Lakebase links.

        Returns:
            Dictionary with workspace_url and organization_id

        Raises:
            HTTPException: If Lakebase is not enabled
        """
        from src.core.exceptions import BadRequestError

        # Check if Lakebase is enabled
        config = await self.get_config()
        if not config.get("enabled", False):
            raise BadRequestError(
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

        # No dispose_engines() here — the database router checks
        # is_lakebase_enabled() on every request, so the next request
        # will route to Lakebase automatically after this config is committed.
        # Calling dispose inside a request handler kills the StaticPool
        # connection before the DI layer commits, losing the config change.

        return {
            "success": True,
            "message": "Lakebase enabled successfully. Next request will use Lakebase.",
            "config": config
        }
