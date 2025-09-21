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
    from databricks.sdk.service.database import DatabaseInstance
    LAKEBASE_AVAILABLE = True
except ImportError:
    # Don't use logger here as it's not initialized yet
    print("Warning: DatabaseInstance not available in databricks-sdk. Lakebase features will be disabled.")
    DatabaseInstance = None
    LAKEBASE_AVAILABLE = False
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import asyncpg

from src.core.logger import LoggerManager
from src.config.settings import settings
from src.core.base_service import BaseService
from src.models.database_config import LakebaseConfig
from src.repositories.database_config_repository import DatabaseConfigRepository
from src.utils.databricks_auth import is_databricks_apps_environment, get_current_databricks_user

logger_manager = LoggerManager.get_instance()
logger = logging.getLogger(__name__)


class LakebaseService(BaseService):
    """Service for managing Databricks Lakebase instances."""

    def __init__(self, session: AsyncSession, user_token: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize Lakebase service.

        Args:
            session: Database session
            user_token: Optional user token for Databricks authentication
            user_email: Optional user email for Lakebase authentication
        """
        super().__init__(session)
        self.user_token = user_token
        self.user_email = user_email
        self.session = session
        self.config_repository = DatabaseConfigRepository(LakebaseConfig, session)
        self._workspace_client = None

    async def _get_auth_token(self) -> Optional[str]:
        """
        Get authentication token with fallback strategy.

        Priority:
        1. User token (OBO)
        2. PAT from database (API key service)
        3. Environment variables

        Returns:
            Authentication token or None
        """
        # 1. Try user token (OBO)
        if self.user_token:
            logger.info("Using OBO user token for authentication")
            return self.user_token

        # 2. Try to get PAT from database
        try:
            from src.services.api_keys_service import ApiKeysService

            # Try to get Databricks PAT from API keys using class method
            databricks_token = await ApiKeysService.get_provider_api_key("databricks")
            if databricks_token:
                logger.info("Using PAT from API key service")
                return databricks_token
        except Exception as e:
            logger.warning(f"Could not retrieve PAT from API key service: {e}")

        # 3. Try environment variables
        env_token = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
        if env_token:
            logger.info("Using PAT from environment variables")
            return env_token

        logger.warning("No authentication token available - will try default SDK auth")
        return None

    async def get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks workspace client with proper authentication."""
        if not self._workspace_client:
            auth_token = await self._get_auth_token()
            host = os.getenv("DATABRICKS_HOST")

            if auth_token and host:
                # Use explicit token authentication
                self._workspace_client = WorkspaceClient(
                    host=host,
                    token=auth_token
                )
                logger.info(f"Created WorkspaceClient with token auth for {host}")
            else:
                # Fall back to default SDK authentication (may use CLI auth)
                try:
                    self._workspace_client = WorkspaceClient()
                    logger.info("Created WorkspaceClient with default SDK auth")
                except Exception as e:
                    logger.error(f"Failed to create WorkspaceClient: {e}")
                    # Try one more time with just host
                    if host:
                        self._workspace_client = WorkspaceClient(host=host)
                    else:
                        raise
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

            # Create new instance
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


    async def migrate_existing_data(self, instance_name: str, endpoint: str) -> Dict[str, Any]:
        """
        Migrate data from existing database (SQLite/PostgreSQL) to Lakebase.

        Args:
            instance_name: Lakebase instance name
            endpoint: Lakebase endpoint

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
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )

            # Build Lakebase connection string
            # For asyncpg, use ssl=require instead of sslmode=require
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

            lakebase_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
                f"{endpoint}:5432/databricks_postgres"
            )

            # Determine source database type
            source_db_type = settings.DATABASE_TYPE
            source_uri = settings.DATABASE_URI

            logger.info(f"📦 Source Database: {source_db_type}")
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
                        text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                    )
                else:  # PostgreSQL
                    result = await self.session.execute(
                        text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
                    )

                tables = [row[0] for row in result]
                logger.info(f"📊 Found {len(tables)} tables to migrate")
                logger.info(f"📋 Tables: {', '.join(tables[:10])}{'...' if len(tables) > 10 else ''}")

            # Create tables in Lakebase
            from src.db.base import Base
            async with lakebase_engine.begin() as conn:
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

            # Migrate data table by table
            migrated_tables = []
            total_rows = 0
            start_time = datetime.utcnow()

            # Import json and datetime for serialization
            import json
            from datetime import datetime

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
                                    'executionhistory': ['planning', 'is_stopping'],
                                    'tools': ['enabled'],
                                    'llmlog': [],  # No boolean columns
                                    'modelconfig': ['extended_thinking', 'enabled'],
                                    'prompttemplate': ['is_active'],
                                    'groups': ['auto_created'],
                                    'tasks': ['async_execution', 'markdown', 'human_input'],
                                    'group_users': ['auto_created'],
                                }

                                # Define datetime columns for proper conversion
                                datetime_columns_by_table = {
                                    'llmlog': ['created_at'],
                                    'crews': ['created_at', 'updated_at'],
                                    'apikey': ['created_at', 'updated_at'],
                                    'schema': ['created_at', 'updated_at'],
                                    'execution_logs': ['timestamp'],
                                    'documentation_embeddings': ['created_at', 'updated_at'],
                                    'groups': ['created_at', 'updated_at'],
                                    'users': ['created_at', 'updated_at', 'last_login'],
                                    'roles': ['created_at', 'updated_at'],
                                    'chat_history': ['timestamp'],
                                    'database_configs': ['created_at', 'updated_at'],
                                    'tasks': ['created_at', 'updated_at'],
                                    'execution_trace': ['created_at'],
                                    'group_users': ['joined_at', 'created_at', 'updated_at'],
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
                        logger.debug(f"  ↳ Table {table_name} is empty, skipping")

                except Exception as table_error:
                    logger.error(f"Error migrating table {table_name}: {table_error}")
                    # Continue with other tables

            # Close Lakebase engine
            await lakebase_engine.dispose()

            # Update configuration to mark migration complete
            config = await self.get_config()
            config["migration_completed"] = True
            config["migration_date"] = datetime.utcnow().isoformat()
            config["migrated_tables"] = len(migrated_tables)
            config["migrated_rows"] = total_rows
            await self.save_config(config)

            # Calculate migration duration
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            logger.info("=" * 80)
            logger.info("🎉 LAKEBASE MIGRATION COMPLETED SUCCESSFULLY!")
            logger.info(f"📊 Summary:")
            logger.info(f"  • Tables migrated: {len(migrated_tables)}/{len(tables)}")
            logger.info(f"  • Total rows: {total_rows:,}")
            logger.info(f"  • Duration: {duration:.2f} seconds")
            logger.info(f"  • Instance: {instance_name}")
            logger.info(f"  • Status: READY")
            logger.info("=" * 80)
            logger.info("🔄 The system will now automatically use Lakebase for all database operations")

            return {
                "success": True,
                "migrated_tables": migrated_tables,
                "total_tables": len(migrated_tables),
                "total_rows": total_rows,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error migrating data to Lakebase: {e}")
            raise

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
        Test connection to Lakebase instance.

        Args:
            instance_name: Name of the instance

        Returns:
            Connection test result
        """
        try:
            async with self.get_lakebase_session(instance_name) as session:
                # Test query
                result = await session.execute(text("SELECT version()"))
                version = result.scalar()

                # Get table count
                table_result = await session.execute(
                    text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                )
                table_count = table_result.scalar()

                return {
                    "success": True,
                    "version": version,
                    "table_count": table_count,
                    "instance_name": instance_name
                }

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }