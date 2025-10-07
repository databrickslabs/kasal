"""
Lakebase Migration Service for handling data migration from source databases to Lakebase.

This service extracts data migration logic from LakebaseService into a dedicated,
reusable component following the repository pattern and service architecture.
"""
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.engine import Engine

from src.core.base_service import BaseService

logger = logging.getLogger(__name__)


class LakebaseMigrationService(BaseService):
    """Service for migrating data from source databases to Lakebase instances."""

    def __init__(
        self,
        source_engine: Optional[Engine] = None,
        lakebase_engine: Optional[Engine] = None,
        source_session: Optional[AsyncSession] = None
    ):
        """
        Initialize migration service.

        Args:
            source_engine: SQLAlchemy engine for source database (sync)
            lakebase_engine: SQLAlchemy engine for Lakebase database (sync or async)
            source_session: Optional async session for source database
        """
        self.source_engine = source_engine
        self.lakebase_engine = lakebase_engine
        self.source_session = source_session

        # Type conversion mappings for PostgreSQL compatibility
        self.json_columns_by_table = {
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

        self.boolean_columns_by_table = {
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

        self.datetime_columns_by_table = {
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

        # Table dependency ordering for foreign key constraint compliance
        self.dependency_order = [
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

    async def get_table_list_async(self, session: AsyncSession, is_sqlite: bool) -> List[str]:
        """
        Get list of tables from source database using async session.

        Args:
            session: Async database session
            is_sqlite: True if source is SQLite, False if PostgreSQL

        Returns:
            List of table names
        """
        try:
            if is_sqlite:
                result = await session.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'alembic_%';")
                )
                tables = [row[0] for row in result]
            else:
                # For PostgreSQL source, check both kasal and public schemas
                result = await session.execute(
                    text("""
                        SELECT tablename FROM pg_tables
                        WHERE schemaname IN ('kasal', 'public')
                        AND tablename NOT LIKE 'alembic_%'
                    """)
                )
                tables = [row[0] for row in result]

            logger.info(f"Found {len(tables)} tables in source database")
            return tables

        except Exception as e:
            logger.error(f"Error getting table list: {e}")
            raise

    def get_table_list_sync(self, engine: Engine, is_sqlite: bool) -> List[str]:
        """
        Get list of tables from source database using sync engine.

        Args:
            engine: SQLAlchemy engine (sync)
            is_sqlite: True if source is SQLite, False if PostgreSQL

        Returns:
            List of table names
        """
        try:
            if is_sqlite:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"))
                    tables = [row[0] for row in result if not row[0].startswith('sqlite_')]
            else:
                with engine.begin() as conn:
                    result = conn.execute(
                        text("SELECT tablename FROM pg_tables WHERE schemaname IN ('kasal', 'public') ORDER BY tablename")
                    )
                    tables = [row[0] for row in result]

            logger.info(f"Found {len(tables)} tables in source database")
            return tables

        except Exception as e:
            logger.error(f"Error getting table list: {e}")
            raise

    def get_sorted_tables(self, tables: List[str]) -> List[str]:
        """
        Sort tables by dependency order to avoid foreign key violations.

        Tables that are referenced by others should be migrated first.

        Args:
            tables: List of table names

        Returns:
            Sorted list of table names
        """
        sorted_tables = []

        # Add tables in dependency order
        for table in self.dependency_order:
            if table in tables:
                sorted_tables.append(table)

        # Add any remaining tables not in dependency list
        for table in tables:
            if table not in sorted_tables:
                sorted_tables.append(table)

        logger.debug(f"Sorted {len(tables)} tables by dependency order")
        return sorted_tables

    def convert_row_types(
        self,
        row_dict: Dict[str, Any],
        table_name: str,
        columns: List[str]
    ) -> Dict[str, Any]:
        """
        Convert row data types for PostgreSQL compatibility.

        Handles JSON serialization, datetime parsing, and boolean conversion.

        Args:
            row_dict: Dictionary of column name to value
            table_name: Name of the table being migrated
            columns: List of column names

        Returns:
            Dictionary with converted types
        """
        json_cols = self.json_columns_by_table.get(table_name, [])
        bool_cols = self.boolean_columns_by_table.get(table_name, [])
        dt_cols = self.datetime_columns_by_table.get(table_name, [])

        converted_dict = {}

        for col in columns:
            value = row_dict.get(col)

            # Handle datetime columns
            if col in dt_cols and value is not None:
                if isinstance(value, str):
                    try:
                        # Try parsing the datetime string
                        converted_dict[col] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        # If it fails, try a simpler format
                        try:
                            converted_dict[col] = datetime.strptime(value.split('.')[0], '%Y-%m-%d %H:%M:%S')
                        except:
                            converted_dict[col] = value  # Keep as-is if parsing fails
                else:
                    converted_dict[col] = value

            # Handle boolean columns (SQLite stores as 0/1 integers)
            elif col in bool_cols and value is not None:
                if isinstance(value, int):
                    converted_dict[col] = bool(value)  # Convert 0/1 to False/True
                else:
                    converted_dict[col] = value

            # Handle JSON columns
            elif col in json_cols:
                if isinstance(value, (dict, list)):
                    # Serialize dict/list to JSON string
                    converted_dict[col] = json.dumps(value)
                elif isinstance(value, str):
                    # Check if it's already valid JSON
                    try:
                        json.loads(value)  # If this succeeds, it's valid JSON
                        converted_dict[col] = value
                    except (json.JSONDecodeError, TypeError):
                        # Plain string, wrap it as JSON string
                        converted_dict[col] = json.dumps(value)
                elif value is None:
                    converted_dict[col] = None
                else:
                    # For other types, wrap them to make valid JSON
                    converted_dict[col] = json.dumps(value)

            # Handle other columns
            else:
                if isinstance(value, (dict, list)):
                    # If it's dict/list but not a JSON column, still serialize it
                    converted_dict[col] = json.dumps(value)
                else:
                    # Keep other types as-is
                    converted_dict[col] = value

        return converted_dict

    async def migrate_table_data_async(
        self,
        table_name: str,
        source_session: AsyncSession,
        lakebase_session: AsyncSession
    ) -> Tuple[int, Optional[str]]:
        """
        Migrate data for a single table using async sessions.

        Args:
            table_name: Name of the table to migrate
            source_session: Async session for source database
            lakebase_session: Async session for Lakebase database

        Returns:
            Tuple of (row_count, error_message)
            error_message is None if successful
        """
        try:
            # Special handling for documentation_embeddings table
            if table_name == 'documentation_embeddings':
                # Skip the embedding column
                async with source_session.begin():
                    result = await source_session.execute(text(
                        "SELECT id, source, title, content, doc_metadata, created_at, updated_at "
                        "FROM documentation_embeddings"
                    ))
                    rows = result.fetchall()
                    columns = ['id', 'source', 'title', 'content', 'doc_metadata', 'created_at', 'updated_at']
            else:
                # Read data from source normally
                async with source_session.begin():
                    result = await source_session.execute(text(f"SELECT * FROM {table_name}"))
                    rows = result.fetchall()
                    columns = list(result.keys())

            if not rows:
                logger.info(f"  ↳ Table {table_name} is empty (0 rows)")
                return 0, None

            # Clear existing data in Lakebase to avoid duplicates
            async with lakebase_session.begin():
                delete_sql = f"DELETE FROM {table_name}"
                await lakebase_session.execute(text(delete_sql))
                logger.debug(f"  ↳ Cleared existing data from {table_name} in Lakebase")

            # Insert into Lakebase
            async with lakebase_session.begin():
                # Build insert statement - escape column names for PostgreSQL
                col_names = ", ".join([f'"{col}"' for col in columns])
                placeholders = ", ".join([f":{col}" for col in columns])
                insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

                # Batch insert with proper type conversion
                for row in rows:
                    row_dict = {}
                    for idx, col in enumerate(columns):
                        row_dict[col] = row[idx]

                    # Convert types for PostgreSQL compatibility
                    converted_row = self.convert_row_types(row_dict, table_name, columns)
                    await lakebase_session.execute(text(insert_sql), converted_row)

            logger.info(f"  ✓ Migrated {len(rows)} rows from {table_name}")
            return len(rows), None

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"❌ Error migrating table {table_name}: {error_msg}")
            return 0, error_msg

    def migrate_table_data_sync(
        self,
        table_name: str,
        source_engine: Engine,
        lakebase_engine: Engine,
        is_sqlite: bool
    ) -> Tuple[int, Optional[str]]:
        """
        Migrate data for a single table using sync engines.

        This method is designed for streaming contexts where async can cause
        greenlet issues with certain drivers.

        Args:
            table_name: Name of the table to migrate
            source_engine: Sync engine for source database
            lakebase_engine: Sync engine for Lakebase database
            is_sqlite: True if source is SQLite, False if PostgreSQL

        Returns:
            Tuple of (row_count, error_message)
            error_message is None if successful
        """
        try:
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

            if not rows:
                logger.info(f"  ↳ Table {table_name} is empty (0 rows)")
                return 0, None

            # Migrate rows
            with lakebase_engine.begin() as lakebase_conn:
                lakebase_conn.execute(text("SET search_path TO kasal"))

                # Build insert statement
                column_list = ", ".join([f'"{col}"' for col in columns])
                placeholders = ", ".join([f":{col}" for col in columns])
                insert_sql = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'

                json_columns = self.json_columns_by_table.get(table_name, [])
                datetime_columns = self.datetime_columns_by_table.get(table_name, [])
                boolean_columns = self.boolean_columns_by_table.get(table_name, [])

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
                        elif col in datetime_columns and isinstance(value, str):
                            try:
                                row_dict[col] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                            except:
                                pass
                        elif col in boolean_columns and isinstance(value, int):
                            row_dict[col] = bool(value)

                    # Use dictionary with named parameters for SQLAlchemy text()
                    lakebase_conn.execute(text(insert_sql), row_dict)

            logger.info(f"  ✓ Migrated {len(rows)} rows from {table_name}")
            return len(rows), None

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"❌ Error migrating table {table_name}: {error_msg}")
            return 0, error_msg
