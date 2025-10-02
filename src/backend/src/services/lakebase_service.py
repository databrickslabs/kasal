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
from src.utils.databricks_auth import is_databricks_apps_environment, get_current_databricks_user, get_workspace_client

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
        self._workspace_client = None

    async def get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client for Lakebase.

        Uses Lakebase-specific authentication priority:
        1. User token (OBO) if available
        2. Service principal OAuth (Client ID/Secret) - preferred for Lakebase
        3. PAT token as fallback

        Returns:
            WorkspaceClient configured with appropriate credentials
        """
        if not self._workspace_client:
            # Pass service="lakebase" to get Lakebase-specific auth priority
            self._workspace_client = await get_workspace_client(self.user_token, service="lakebase")
            if not self._workspace_client:
                raise ValueError("Failed to create WorkspaceClient for Lakebase operations")
            logger.info("Created WorkspaceClient for Lakebase")
        return self._workspace_client

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

            # Generate temporary token for connection
            w = await self.get_workspace_client()

            logger.info(f"Generating database credentials for instance: {instance_name}")
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            logger.info(f"✅ Generated database credential, token length: {len(cred.token)}")

            # Build Lakebase connection string
            # For asyncpg, use ssl=require instead of sslmode=require
            # Authentication depends on environment:
            # - Databricks Apps with user email: Use provided email
            # - Otherwise: Get the actual authenticated user's identity
            if is_databricks_apps_environment() and self.user_email:
                # In Databricks Apps, use provided email
                user_email = self.user_email
                username = quote(user_email)  # URL-encoded for connection string
                logger.info(f"Using provided email for Lakebase: {user_email}")
            else:
                # Get the actual authenticated user's identity using centralized method
                # Pass the user token if we have it (for OBO), otherwise it will use PAT
                current_user_identity, error = await get_current_databricks_user(self.user_token)
                if error or not current_user_identity:
                    logger.error(f"Failed to get current user identity: {error}")
                    raise Exception(f"Cannot determine Databricks user identity: {error}")

                user_email = current_user_identity
                username = quote(user_email)  # URL-encoded for connection string
                logger.info(f"Using authenticated user identity for Lakebase: {user_email}")

            # Create PostgreSQL role for user if it doesn't exist
            logger.info(f"🔐 Ensuring PostgreSQL role exists for user: {user_email}")
            try:
                # Check existing roles
                existing_roles = list(w.database.list_database_instance_roles(instance_name=instance_name))
                role_names = [role.name for role in existing_roles]
                logger.info(f"Existing roles in instance: {role_names}")

                if user_email not in role_names:
                    logger.info(f"Creating PostgreSQL role for {user_email}...")
                    role = DatabaseInstanceRole(
                        name=user_email,
                        identity_type=DatabaseInstanceRoleIdentityType.USER,
                        attributes=DatabaseInstanceRoleAttributes(
                            createdb=True,
                            createrole=True,
                            bypassrls=True
                        ),
                        membership_role=DatabaseInstanceRoleMembershipRole.DATABRICKS_SUPERUSER
                    )

                    created_role = w.database.create_database_instance_role(
                        instance_name=instance_name,
                        database_instance_role=role
                    )
                    logger.info(f"✅ Created PostgreSQL role: {created_role.name}")
                else:
                    logger.info(f"✅ PostgreSQL role already exists: {user_email}")
            except Exception as role_error:
                logger.error(f"Failed to create PostgreSQL role: {role_error}")
                raise Exception(f"Cannot create PostgreSQL role for {user_email}: {role_error}")

            lakebase_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
                f"{endpoint}:5432/databricks_postgres"
            )

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

            # Create Lakebase engine with SSL configuration for asyncpg
            # asyncpg requires SSL settings in connect_args
            lakebase_engine = create_async_engine(
                lakebase_url,
                echo=False,
                connect_args={
                    "ssl": "require",  # Enable SSL for asyncpg
                    "server_settings": {
                        "jit": "off"  # Disable JIT for compatibility
                    }
                }
            )

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

            # Databricks Lakebase auto-provisions roles based on Unity Catalog identities
            # First connection triggers role creation
            from src.db.base import Base

            logger.info(f"Connecting to Lakebase as user: {user_email}")

            # IMPORTANT: For Lakebase, on first use we need to connect as 'admin' user
            # to set up the database structure. After that, regular users can connect.
            # Check if we should use admin credentials for initial setup

            # Try different connection approaches for Lakebase initialization
            connection_attempts = [
                ("token-only", f"postgresql+asyncpg://:{cred.token}@{endpoint}:5432/databricks_postgres"),
                ("admin", f"postgresql+asyncpg://admin:{cred.token}@{endpoint}:5432/databricks_postgres"),
                ("postgres", f"postgresql+asyncpg://postgres:{cred.token}@{endpoint}:5432/databricks_postgres"),
                ("user", lakebase_url)
            ]

            connected_engine = None
            connected_user = None

            for attempt_name, connection_url in connection_attempts:
                logger.info(f"Attempting connection as '{attempt_name}'...")
                test_engine = create_async_engine(
                    connection_url,
                    echo=False,
                    connect_args={
                        "ssl": "require",
                        "server_settings": {"jit": "off"}
                    }
                )

                try:
                    async with test_engine.connect() as test_conn:
                        result = await test_conn.execute(text("SELECT current_user, version()"))
                        current_user, version = result.fetchone()
                        logger.info(f"✅ SUCCESS! Connected as: {current_user}")
                        logger.info(f"✅ Database version: {version}")
                        connected_engine = test_engine
                        connected_user = current_user
                        break
                except Exception as conn_error:
                    logger.warning(f"Connection attempt '{attempt_name}' failed: {conn_error}")
                    await test_engine.dispose()

            if not connected_engine:
                logger.error("All connection attempts failed!")
                logger.error("This means the Lakebase instance needs manual initialization.")
                logger.error("Please connect to Lakebase using Databricks SQL or a SQL client first to initialize it.")
                raise Exception("Failed to connect to Lakebase with any credentials")

            # Step 2: Now create schema and set permissions
            async with lakebase_engine.begin() as conn:
                # Handle schema recreation if requested
                if recreate_schema:
                    logger.info("🗑️ Dropping existing kasal schema (if exists)...")
                    await conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                    logger.info("✅ Dropped kasal schema")

                # Create kasal schema if it doesn't exist
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                logger.info("✅ Created kasal schema in Lakebase")

                # Grant schema permissions - this should now work since role exists
                try:
                    await conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"'))
                    await conn.execute(text(f'GRANT ALL ON SCHEMA public TO "{user_email}"'))
                    logger.info(f"✅ Granted schema permissions to {user_email}")
                except Exception as grant_error:
                    # Log but don't fail - user might already have permissions
                    logger.warning(f"Permission grant warning (may be ok): {grant_error}")

                # Set default privileges for future objects
                try:
                    await conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal GRANT ALL ON TABLES TO "{user_email}"'))
                    await conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal GRANT ALL ON SEQUENCES TO "{user_email}"'))
                    logger.info(f"✅ Set default privileges for {user_email}")
                except Exception as privilege_error:
                    logger.warning(f"Default privilege warning (may be ok): {privilege_error}")

                # Set kasal as the default schema for this connection
                await conn.execute(text("SET search_path TO kasal, public"))
                logger.info("✅ Set kasal schema as default search path")
                # Create tables one by one, skipping those with vector columns
                # since Lakebase doesn't support pgvector extension yet
                tables_to_skip = ['documentation_embeddings']  # Tables with vector columns

                # Get all table objects from metadata
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        logger.info(f"Skipping table {table.name} (contains vector column)")
                        # Create a modified version without vector column
                        if table.name == 'documentation_embeddings':
                            # Create table without the embedding column
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            await conn.execute(text(create_sql))
                            logger.info("Created documentation_embeddings table without vector column")
                    else:
                        # Create table normally
                        await conn.run_sync(table.create, checkfirst=True)
                        logger.info(f"Created table {table.name}")

                logger.info("✅ Created table structure in Lakebase")
                logger.info("-" * 60)
                logger.info("📤 Starting data migration...")

            # Import json for serialization (datetime already imported at top)
            import json

            # Migrate data table by table
            migrated_tables = []
            failed_tables_list = []
            total_rows = 0
            start_time = datetime.utcnow()

            # Define tables and their JSON columns
            json_columns_by_table = {
                'executionhistory': ['inputs', 'result', 'partial_results'],
                'llmlog': ['extra_data'],
                'tools': ['config'],
                'agents': ['tools', 'tool_configs', 'embedder_config', 'knowledge_sources'],
                'crews': ['agent_ids', 'task_ids', 'nodes', 'edges'],
                'schema': ['schema_definition', 'field_descriptions', 'keywords', 'tools', 'example_data'],
                'tasks': ['tools', 'tool_configs', 'context', 'config', 'output', 'callback_config'],
                'memory_backend': ['databricks_config', 'custom_config'],
                'flow': ['nodes', 'edges', 'flow_config'],
                'flow_execution': ['config', 'result'],
                'schedule': ['agents_yaml', 'tasks_yaml', 'inputs'],
                'mcp_server': ['additional_config'],
                'documentation_embedding': ['doc_metadata'],
                'billing': ['billing_metadata', 'model_breakdown', 'notification_emails', 'alert_metadata'],
                'chat_history': ['generation_result'],
                'database_config': ['value'],
                'execution_trace': ['output', 'trace_metadata'],
                'error_trace': ['error_metadata'],
            }

            for idx, table_name in enumerate(tables, 1):
                try:
                    logger.info(f"[{idx}/{len(tables)}] Migrating table: {table_name}")

                    # Special handling for documentation_embeddings table
                    if table_name == 'documentation_embeddings':
                        # Skip the embedding column
                        async with self.session.begin():
                            # Select all columns except embedding
                            result = await self.session.execute(text(
                                "SELECT id, source, title, content, doc_metadata, created_at, updated_at "
                                "FROM documentation_embeddings"
                            ))
                            rows = result.fetchall()
                            columns = ['id', 'source', 'title', 'content', 'doc_metadata', 'created_at', 'updated_at']
                    else:
                        # Read data from source normally
                        async with self.session.begin():
                            # Get all data from the table
                            result = await self.session.execute(text(f"SELECT * FROM {table_name}"))
                            rows = result.fetchall()
                            columns = list(result.keys())

                    if rows:
                        # First, clear existing data in Lakebase to avoid duplicates
                        async with AsyncSession(lakebase_engine) as lakebase_session:
                            async with lakebase_session.begin():
                                # TRUNCATE is faster but needs CASCADE for foreign keys
                                # Using DELETE for safety
                                delete_sql = f"DELETE FROM {table_name}"
                                await lakebase_session.execute(text(delete_sql))
                                logger.debug(f"  ↳ Cleared existing data from {table_name} in Lakebase")

                        # Insert into Lakebase
                        async with AsyncSession(lakebase_engine) as lakebase_session:
                            async with lakebase_session.begin():
                                # Build insert statement - escape column names for PostgreSQL
                                col_names = ", ".join([f'"{col}"' for col in columns])
                                placeholders = ", ".join([f":{col}" for col in columns])
                                insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

                                # Get JSON columns for this table
                                json_cols = json_columns_by_table.get(table_name, [])

                                # Define boolean columns for each table (from SQLAlchemy models)
                                boolean_columns_by_table = {
                                    'agents': ['verbose', 'allow_delegation', 'cache', 'memory', 'allow_code_execution',
                                              'use_system_prompt', 'respect_context_window'],
                                    'billing_alerts': ['is_active'],
                                    'crews': [],
                                    'dspy_configs': ['enabled'],
                                    'dspy_training_examples': ['used_in_optimization'],
                                    'executionhistory': ['planning', 'is_stopping'],
                                    'flows': ['is_active'],
                                    'flow_executions': [],
                                    'groups': ['auto_created'],
                                    'group_users': ['auto_created'],
                                    'initializationstatus': [],
                                    'llmlog': [],
                                    'mcp_servers': ['enabled'],
                                    'mcp_settings': ['enabled'],
                                    'memory_backends': ['enabled'],
                                    'modelconfig': ['extended_thinking', 'enabled'],
                                    'prompttemplate': ['is_active'],
                                    'schedule': ['enabled'],
                                    'tasks': ['async_execution', 'markdown', 'human_input'],
                                    'tools': ['enabled'],
                                    'users': ['is_system_admin', 'is_personal_workspace_manager'],
                                }

                                # Define datetime columns for proper conversion
                                # Add all tables with created_at/updated_at columns
                                datetime_columns_by_table = {
                                    'agents': ['created_at', 'updated_at'],
                                    'apikey': ['created_at', 'updated_at'],
                                    'billing_alerts': ['created_at', 'updated_at', 'triggered_at'],
                                    'billing_periods': ['period_start', 'period_end', 'created_at', 'updated_at'],
                                    'chat_history': ['timestamp'],
                                    'crews': ['created_at', 'updated_at'],
                                    'database_configs': ['created_at', 'updated_at'],
                                    'databricksconfig': ['created_at', 'updated_at'],
                                    'documentation_embeddings': ['created_at', 'updated_at'],
                                    'dspy_configs': ['created_at', 'updated_at'],
                                    'dspy_module_cache': ['created_at', 'updated_at', 'last_used'],
                                    'dspy_optimization_runs': ['started_at', 'completed_at', 'created_at'],
                                    'dspy_training_examples': ['created_at', 'collected_at'],
                                    'engineconfig': ['created_at', 'updated_at'],
                                    'errortrace': ['created_at'],
                                    'execution_logs': ['timestamp'],
                                    'execution_trace': ['created_at'],
                                    'executionhistory': ['created_at', 'updated_at', 'start_time', 'end_time'],
                                    'flows': ['created_at', 'updated_at'],
                                    'flow_executions': ['started_at', 'completed_at', 'created_at'],
                                    'flow_node_executions': ['started_at', 'completed_at', 'created_at'],
                                    'groups': ['created_at', 'updated_at'],
                                    'group_tools': ['created_at'],
                                    'group_users': ['joined_at', 'created_at', 'updated_at'],
                                    'initializationstatus': ['created_at', 'updated_at'],
                                    'llmlog': ['created_at'],
                                    'llm_usage_billing': ['period_start', 'period_end', 'created_at', 'updated_at'],
                                    'mcp_servers': ['created_at', 'updated_at'],
                                    'mcp_settings': ['created_at', 'updated_at'],
                                    'memory_backends': ['created_at', 'updated_at'],
                                    'modelconfig': ['created_at', 'updated_at'],
                                    'prompttemplate': ['created_at', 'updated_at'],
                                    'refresh_tokens': ['created_at', 'expires_at'],
                                    'schedule': ['created_at', 'updated_at', 'last_run', 'next_run'],
                                    'schema': ['created_at', 'updated_at'],
                                    'tasks': ['created_at', 'updated_at'],
                                    'taskstatus': ['created_at', 'updated_at'],
                                    'tools': ['created_at', 'updated_at'],
                                    'users': ['created_at', 'updated_at', 'last_login'],
                                }

                                # Get column lists for this table
                                bool_cols = boolean_columns_by_table.get(table_name, [])
                                dt_cols = datetime_columns_by_table.get(table_name, [])

                                # Batch insert with proper type conversion
                                for row in rows:
                                    row_dict = {}
                                    for idx, col in enumerate(columns):
                                        value = row[idx]

                                        # Handle datetime columns
                                        if col in dt_cols and value is not None:
                                            if isinstance(value, str):
                                                try:
                                                    # Try parsing the datetime string
                                                    row_dict[col] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                                except (ValueError, AttributeError):
                                                    # If it fails, try a simpler format
                                                    try:
                                                        row_dict[col] = datetime.strptime(value.split('.')[0], '%Y-%m-%d %H:%M:%S')
                                                    except:
                                                        row_dict[col] = value  # Keep as-is if parsing fails
                                            else:
                                                row_dict[col] = value
                                        # Handle boolean columns (SQLite stores as 0/1 integers)
                                        elif col in bool_cols and value is not None:
                                            if isinstance(value, int):
                                                row_dict[col] = bool(value)  # Convert 0/1 to False/True
                                            else:
                                                row_dict[col] = value
                                        # Check if this column is a JSON column
                                        elif col in json_cols:
                                            # Handle JSON columns - serialize dict/list to JSON string
                                            if isinstance(value, (dict, list)):
                                                row_dict[col] = json.dumps(value)
                                            elif isinstance(value, str):
                                                # Check if it's already valid JSON
                                                try:
                                                    json.loads(value)  # If this succeeds, it's valid JSON
                                                    row_dict[col] = value
                                                except (json.JSONDecodeError, TypeError):
                                                    # Plain string, need to wrap it as JSON string
                                                    # "hello world" becomes '"hello world"' which is valid JSON
                                                    row_dict[col] = json.dumps(value)
                                            elif value is None:
                                                row_dict[col] = None
                                            else:
                                                # For other types (int, float, bool, etc.)
                                                # Wrap them to make valid JSON
                                                row_dict[col] = json.dumps(value)
                                        else:
                                            # Non-JSON columns
                                            if isinstance(value, (dict, list)):
                                                # If it's dict/list but not a JSON column, still serialize it
                                                row_dict[col] = json.dumps(value)
                                            else:
                                                # Keep other types as-is
                                                row_dict[col] = value

                                    await lakebase_session.execute(text(insert_sql), row_dict)

                                total_rows += len(rows)

                        logger.info(f"  ✓ Migrated {len(rows)} rows from {table_name}")
                        migrated_tables.append({
                            "table": table_name,
                            "rows": len(rows)
                        })
                    else:
                        logger.info(f"  ↳ Table {table_name} is empty (0 rows)")
                        # Count empty tables as successfully migrated
                        migrated_tables.append({
                            "table": table_name,
                            "rows": 0
                        })

                except Exception as table_error:
                    logger.error(f"❌ Error migrating table {table_name}: {table_error}")
                    logger.error(f"   Error type: {type(table_error).__name__}")
                    import traceback
                    logger.error(f"   Traceback: {traceback.format_exc()}")
                    failed_tables_list.append({
                        "table": table_name,
                        "error": str(table_error),
                        "error_type": type(table_error).__name__
                    })
                    # Continue with other tables

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

            # Generate temporary token for connection
            yield {"type": "progress", "message": "Generating database credentials...", "step": "auth"}
            w = await self.get_workspace_client()

            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )
            yield {"type": "success", "message": f"✅ Generated database credential"}

            # Get user email for authentication
            if is_databricks_apps_environment() and self.user_email:
                user_email = self.user_email
                username = quote(user_email)
                yield {"type": "info", "message": f"Using provided email for Lakebase: {user_email}"}
            else:
                current_user_identity, error = await get_current_databricks_user(self.user_token)
                if error or not current_user_identity:
                    yield {"type": "error", "message": f"Cannot determine Databricks user identity: {error}"}
                    return
                user_email = current_user_identity
                username = quote(user_email)
                yield {"type": "info", "message": f"Using authenticated user identity: {user_email}"}

            # Create PostgreSQL role for user if needed
            yield {"type": "progress", "message": "🔐 Ensuring PostgreSQL role exists...", "step": "role"}
            try:
                existing_roles = list(w.database.list_database_instance_roles(instance_name=instance_name))
                role_names = [role.name for role in existing_roles]

                if user_email not in role_names:
                    yield {"type": "progress", "message": f"Creating PostgreSQL role for {user_email}..."}
                    role = DatabaseInstanceRole(
                        name=user_email,
                        identity_type=DatabaseInstanceRoleIdentityType.USER,
                        attributes=DatabaseInstanceRoleAttributes(
                            createdb=True,
                            createrole=True,
                            bypassrls=True
                        ),
                        membership_role=DatabaseInstanceRoleMembershipRole.DATABRICKS_SUPERUSER
                    )
                    created_role = w.database.create_database_instance_role(
                        instance_name=instance_name,
                        database_instance_role=role
                    )
                    yield {"type": "success", "message": f"✅ Created PostgreSQL role: {created_role.name}"}
                else:
                    yield {"type": "success", "message": f"✅ PostgreSQL role already exists: {user_email}"}
            except Exception as role_error:
                yield {"type": "error", "message": f"Failed to create PostgreSQL role: {role_error}"}
                return

            lakebase_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
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

            # Connect to Lakebase using synchronous driver to avoid greenlet issues
            yield {"type": "progress", "message": "📤 Connecting to Lakebase...", "step": "connect_lakebase"}
            from sqlalchemy.pool import NullPool
            # Use sync pg8000 driver instead of asyncpg to avoid greenlet context issues in streaming context
            # Change postgresql+asyncpg:// to postgresql+pg8000://
            lakebase_url_sync = lakebase_url.replace("postgresql+asyncpg://", "postgresql+pg8000://")
            lakebase_engine = create_engine(
                lakebase_url_sync,
                echo=False,
                poolclass=NullPool,
                connect_args={
                    "ssl_context": True  # pg8000 uses ssl_context instead of sslmode
                }
            )
            yield {"type": "success", "message": "✅ Connected to Lakebase"}

            # Get table list
            yield {"type": "progress", "message": "📋 Getting table list from source...", "step": "get_tables"}
            if is_sqlite:
                with source_engine.connect() as conn:
                    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"))
                    tables = [row[0] for row in result if not row[0].startswith('sqlite_')]
            else:
                with source_engine.begin() as conn:
                    result = conn.execute(
                        text("SELECT tablename FROM pg_tables WHERE schemaname IN ('kasal', 'public') ORDER BY tablename")
                    )
                    tables = [row[0] for row in result]

            # Sort tables by dependency order to avoid foreign key violations
            # Tables that are referenced by others should be migrated first
            dependency_order = [
                'users', 'groups', 'modelconfig', 'prompttemplate', 'tools', 'schema',
                'databricksconfig', 'engineconfig', 'memory_backends', 'mcp_servers', 'mcp_settings',
                'agents', 'tasks', 'crews', 'flows', 'schedule', 'dspy_configs',
                'apikey', 'group_users', 'group_tools',
                'executionhistory', 'llmlog', 'chat_history', 'execution_logs',
                'errortrace', 'execution_trace', 'taskstatus',
                'flow_executions', 'flow_node_executions',
                'dspy_optimization_runs', 'dspy_training_examples', 'dspy_module_cache',
                'billing_periods', 'billing_alerts', 'llm_usage_billing',
                'documentation_embeddings', 'database_configs', 'initializationstatus', 'refresh_tokens'
            ]

            # Sort tables based on dependency order
            sorted_tables = []
            for table in dependency_order:
                if table in tables:
                    sorted_tables.append(table)
            # Add any remaining tables not in dependency list
            for table in tables:
                if table not in sorted_tables:
                    sorted_tables.append(table)
            tables = sorted_tables

            yield {"type": "success", "message": f"✅ Found {len(tables)} tables to migrate"}

            # Create schema and tables
            yield {"type": "progress", "message": "🏗️ Creating schema and tables...", "step": "create_schema", "total_tables": len(tables)}

            with lakebase_engine.begin() as conn:
                if recreate_schema:
                    yield {"type": "progress", "message": "🗑️ Dropping existing kasal schema..."}
                    conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                    yield {"type": "success", "message": "✅ Dropped kasal schema"}

                conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                yield {"type": "success", "message": "✅ Created kasal schema"}

                # Grant permissions
                try:
                    conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"'))
                    conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal GRANT ALL ON TABLES TO "{user_email}"'))
                    conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal GRANT ALL ON SEQUENCES TO "{user_email}"'))
                    yield {"type": "success", "message": f"✅ Granted schema permissions to {user_email}"}
                except Exception as grant_error:
                    yield {"type": "warning", "message": f"Permission grant warning: {grant_error}"}

                conn.execute(text("SET search_path TO kasal"))
                yield {"type": "success", "message": "✅ Set kasal schema as default search path"}

                # Create tables
                tables_to_skip = ['documentation_embeddings']
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        yield {"type": "info", "message": f"Skipping table {table.name} (contains vector column)"}
                        if table.name == 'documentation_embeddings':
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            conn.execute(text(create_sql))
                            yield {"type": "success", "message": f"Created {table.name} without vector column"}
                    else:
                        table.create(conn, checkfirst=True)
                        yield {"type": "success", "message": f"Created table {table.name}"}

                yield {"type": "success", "message": "✅ Created table structure in Lakebase"}

            # Check if we should migrate data
            if not migrate_data:
                # Schema-only mode - skip data migration
                yield {"type": "success", "message": "✅ Schema created successfully (data migration skipped)"}
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

            import json

            # Define type conversion mappings
            json_columns_by_table = {
                'executionhistory': ['inputs', 'result', 'partial_results'],
                'llmlog': ['extra_data'],
                'tools': ['config'],
                'agents': ['tools', 'tool_configs', 'embedder_config', 'knowledge_sources'],
                'crews': ['agent_ids', 'task_ids', 'nodes', 'edges'],
                'schema': ['schema_definition', 'field_descriptions', 'keywords', 'tools', 'example_data'],
                'tasks': ['tools', 'tool_configs', 'context', 'config', 'output', 'callback_config'],
                'memory_backend': ['databricks_config', 'custom_config'],
                'flow': ['nodes', 'edges', 'flow_config'],
                'flow_execution': ['config', 'result'],
                'schedule': ['agents_yaml', 'tasks_yaml', 'inputs'],
                'mcp_server': ['additional_config'],
                'documentation_embedding': ['doc_metadata'],
                'billing': ['billing_metadata', 'model_breakdown', 'notification_emails', 'alert_metadata'],
                'chat_history': ['generation_result'],
                'database_config': ['value'],
                'execution_trace': ['output', 'trace_metadata'],
                'error_trace': ['error_metadata'],
            }

            boolean_columns_by_table = {
                'agents': ['verbose', 'allow_delegation', 'cache', 'memory', 'allow_code_execution',
                          'use_system_prompt', 'respect_context_window'],
                'billing_alerts': ['is_active'],
                'crews': [],
                'dspy_configs': ['enabled'],
                'dspy_training_examples': ['used_in_optimization'],
                'executionhistory': ['planning', 'is_stopping'],
                'flows': ['is_active'],
                'flow_executions': [],
                'groups': ['auto_created'],
                'group_users': ['auto_created'],
                'initializationstatus': [],
                'llmlog': [],
                'mcp_servers': ['enabled'],
                'mcp_settings': ['enabled'],
                'memory_backends': ['enabled'],
                'modelconfig': ['extended_thinking', 'enabled'],
                'prompttemplate': ['is_active'],
                'schedule': ['enabled'],
                'tasks': ['async_execution', 'markdown', 'human_input'],
                'tools': ['enabled'],
                'users': ['is_system_admin', 'is_personal_workspace_manager'],
            }

            datetime_columns_by_table = {
                'agents': ['created_at', 'updated_at'],
                'apikey': ['created_at', 'updated_at'],
                'billing_alerts': ['created_at', 'updated_at', 'triggered_at'],
                'billing_periods': ['period_start', 'period_end', 'created_at', 'updated_at'],
                'chat_history': ['timestamp'],
                'crews': ['created_at', 'updated_at'],
                'database_configs': ['created_at', 'updated_at'],
                'databricksconfig': ['created_at', 'updated_at'],
                'documentation_embeddings': ['created_at', 'updated_at'],
                'dspy_configs': ['created_at', 'updated_at'],
                'dspy_module_cache': ['created_at', 'updated_at', 'last_used'],
                'dspy_optimization_runs': ['started_at', 'completed_at', 'created_at'],
                'dspy_training_examples': ['created_at', 'collected_at'],
                'engineconfig': ['created_at', 'updated_at'],
                'errortrace': ['created_at'],
                'execution_logs': ['timestamp'],
                'execution_trace': ['created_at'],
                'executionhistory': ['created_at', 'updated_at', 'start_time', 'end_time'],
                'flows': ['created_at', 'updated_at'],
                'flow_executions': ['started_at', 'completed_at', 'created_at'],
                'flow_node_executions': ['started_at', 'completed_at', 'created_at'],
                'groups': ['created_at', 'updated_at'],
                'group_tools': ['created_at'],
                'group_users': ['joined_at', 'created_at', 'updated_at'],
                'initializationstatus': ['created_at', 'updated_at'],
                'llmlog': ['created_at'],
                'llm_usage_billing': ['period_start', 'period_end', 'created_at', 'updated_at'],
                'mcp_servers': ['created_at', 'updated_at'],
                'mcp_settings': ['created_at', 'updated_at'],
                'memory_backends': ['created_at', 'updated_at'],
                'modelconfig': ['created_at', 'updated_at'],
                'prompttemplate': ['created_at', 'updated_at'],
                'refresh_tokens': ['created_at', 'expires_at'],
                'schedule': ['created_at', 'updated_at', 'last_run', 'next_run'],
                'schema': ['created_at', 'updated_at'],
                'tasks': ['created_at', 'updated_at'],
                'taskstatus': ['created_at', 'updated_at'],
                'tools': ['created_at', 'updated_at'],
                'users': ['created_at', 'updated_at', 'last_login'],
            }

            migrated_tables = []
            failed_tables_list = []
            total_rows = 0
            start_time = datetime.utcnow()

            # Migrate each table
            for idx, table_name in enumerate(tables, 1):
                try:
                    yield {
                        "type": "table_start",
                        "message": f"Migrating table {table_name}...",
                        "table": table_name,
                        "progress": idx,
                        "total": len(tables)
                    }

                    # Get data from source
                    if is_sqlite:
                        with source_engine.connect() as conn:
                            result = conn.execute(text(f'SELECT * FROM "{table_name}"'))
                            rows = result.fetchall()
                            columns = result.keys()
                    else:
                        with source_engine.begin() as conn:
                            result = conn.execute(text(f'SELECT * FROM "{table_name}"'))
                            rows = result.fetchall()
                            columns = result.keys()

                    if rows:
                        # Migrate rows
                        with lakebase_engine.begin() as lakebase_session:
                            lakebase_session.execute(text("SET search_path TO kasal"))

                            # For SQLAlchemy text() with pg8000, use named parameters with bindparams
                            column_list = ", ".join([f'"{col}"' for col in columns])
                            placeholders = ", ".join([f":{col}" for col in columns])
                            insert_sql = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'

                            json_columns = json_columns_by_table.get(table_name, [])
                            datetime_columns = datetime_columns_by_table.get(table_name, [])
                            boolean_columns = boolean_columns_by_table.get(table_name, [])

                            for row in rows:
                                row_dict = dict(zip(columns, row))

                                # Convert types
                                for col in columns:
                                    value = row_dict[col]
                                    if value is None:
                                        continue

                                    if col in json_columns:
                                        # For JSON columns, ensure proper serialization
                                        if isinstance(value, str):
                                            # Already a string, might be JSON from SQLite
                                            try:
                                                # Validate it's valid JSON
                                                json.loads(value)
                                                # Keep as string for PostgreSQL
                                            except:
                                                # Not valid JSON, wrap as JSON string
                                                row_dict[col] = json.dumps(value)
                                        elif isinstance(value, (dict, list)):
                                            # Python object, serialize to JSON string
                                            row_dict[col] = json.dumps(value)
                                        # else: keep as is (might be None)
                                    elif col in datetime_columns and isinstance(value, str):
                                        try:
                                            row_dict[col] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                        except:
                                            pass
                                    elif col in boolean_columns and isinstance(value, int):
                                        row_dict[col] = bool(value)

                                # Use dictionary with named parameters for SQLAlchemy text()
                                lakebase_session.execute(text(insert_sql), row_dict)

                        total_rows += len(rows)
                        migrated_tables.append({"table": table_name, "rows": len(rows)})

                        yield {
                            "type": "table_complete",
                            "message": f"✓ Migrated {len(rows)} rows from {table_name}",
                            "table": table_name,
                            "rows": len(rows),
                            "progress": idx,
                            "total": len(tables)
                        }
                    else:
                        migrated_tables.append({"table": table_name, "rows": 0})
                        yield {
                            "type": "table_complete",
                            "message": f"↳ Table {table_name} is empty (0 rows)",
                            "table": table_name,
                            "rows": 0,
                            "progress": idx,
                            "total": len(tables)
                        }

                except Exception as table_error:
                    failed_tables_list.append({
                        "table": table_name,
                        "error": str(table_error),
                        "error_type": type(table_error).__name__
                    })
                    yield {
                        "type": "table_error",
                        "message": f"❌ Error migrating table {table_name}: {table_error}",
                        "table": table_name,
                        "error": str(table_error),
                        "error_type": type(table_error).__name__
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
            # Authentication depends on environment:
            # - Databricks Apps with user email: Use provided email
            # - Otherwise: Get the actual authenticated user's identity
            if is_databricks_apps_environment() and self.user_email:
                # In Databricks Apps, use provided email
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

    async def check_lakebase_tables(self, instance_name: str) -> Dict[str, Any]:
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
            if is_databricks_apps_environment() and self.user_email:
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