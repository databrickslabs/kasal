"""
Service for exporting CrewAI crews to various formats.
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.engines.crewai.exporters import (
    DatabricksAppExporter,
    DatabricksNotebookExporter,
    PythonProjectExporter,
)
from src.repositories.agent_repository import AgentRepository
from src.repositories.crew_repository import CrewRepository
from src.repositories.task_repository import TaskRepository
from src.repositories.tool_repository import ToolRepository
from src.schemas.crew_export import ExportFormat, ExportOptions
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CrewExportService:
    """Service for exporting crews to various formats"""

    def __init__(self, session: AsyncSession):
        """
        Initialize export service with database session.

        Args:
            session: Database session for operations
        """
        self.session = session
        self.crew_repository = CrewRepository(session)
        self.agent_repository = AgentRepository(session)
        self.task_repository = TaskRepository(session)
        self.tool_repository = ToolRepository(session)
        # title -> non-secret config, captured while resolving tool IDs so the
        # exporters can configure the crew's tools like Kasal does at runtime.
        self._tool_configs: Dict[str, Dict[str, Any]] = {}

    async def export_crew(
        self,
        crew_id: str,
        export_format: ExportFormat,
        options: Optional[ExportOptions] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """
        Export crew to specified format

        Args:
            crew_id: ID of crew to export
            export_format: Target format (python_project or databricks_notebook)
            options: Export options
            group_context: Group context for authorization

        Returns:
            Export result with files/notebook and metadata
        """
        logger.info(f"Exporting crew {crew_id} to format {export_format}")

        # Get crew data with group check
        crew_data = await self._get_crew_with_details(crew_id, group_context)

        # Convert options to dict
        options_dict = options.dict() if options else {}

        # Select appropriate exporter
        if export_format == ExportFormat.PYTHON_PROJECT:
            exporter = PythonProjectExporter()
        elif export_format == ExportFormat.DATABRICKS_NOTEBOOK:
            exporter = DatabricksNotebookExporter()
        elif export_format == ExportFormat.DATABRICKS_APP:
            exporter = DatabricksAppExporter()
        else:
            raise ValueError(f"Unsupported export format: {export_format}")

        # Generate export
        result = await exporter.export(crew_data, options_dict)

        logger.info(f"Successfully exported crew {crew_id} to {export_format}")

        return result

    async def _get_crew_with_details(
        self, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """
        Get crew with all related agents and tasks

        Args:
            crew_id: Crew ID (string)
            group_context: Group context for authorization

        Returns:
            Dictionary with crew data
        """
        # Get crew (convert string to UUID for the Crew model)
        crew_uuid = UUID(crew_id) if isinstance(crew_id, str) else crew_id
        crew = await self.crew_repository.get(crew_uuid)
        if not crew:
            raise ValueError(f"Crew {crew_id} not found")

        # Check group authorization
        if group_context and group_context.is_valid():
            if crew.group_id not in group_context.group_ids:
                raise ValueError(f"Crew {crew_id} not found")  # Don't reveal existence

        # Get agents
        agents = []
        for agent_id in crew.agent_ids:
            agent = await self.agent_repository.get(agent_id)
            if agent:
                agent_dict = await self._agent_to_dict(agent)
                agents.append(agent_dict)

        # Get tasks
        tasks = []
        for task_id in crew.task_ids:
            task = await self.task_repository.get(task_id)
            if task:
                task_dict = await self._task_to_dict(task)
                tasks.append(task_dict)

        # Get MCP servers enabled for this workspace. At runtime the engine
        # auto-attaches all enabled MCP servers to the crew's agents, so the
        # exported notebook mirrors that by wiring them via MCPServerAdapter.
        mcp_servers = await self._get_enabled_mcp_servers(group_context)

        # Unity Catalog target (catalog/schema) + SQL warehouse from the
        # workspace's Databricks configuration, so the deployment cell / app
        # default to the configured location (warehouse is used to provision UC
        # trace tables).
        catalog, schema, warehouse_id = await self._get_databricks_catalog_schema(
            group_context
        )

        return {
            "id": str(crew.id),
            "name": crew.name,
            "agents": agents,
            "tasks": tasks,
            "nodes": crew.nodes or [],
            "edges": crew.edges or [],
            "mcp_servers": mcp_servers,
            # Non-secret per-tool config (title -> config), captured while
            # resolving the agents'/tasks' tool IDs above.
            "tool_configs": self._tool_configs,
            "databricks_catalog": catalog,
            "databricks_schema": schema,
            "databricks_warehouse_id": warehouse_id,
            # Crew-level execution settings so exports match Kasal's runtime
            # (process, planning, reasoning, manager, memory).
            "process": crew.process or "sequential",
            "planning": bool(crew.planning),
            "planning_llm": crew.planning_llm,
            "reasoning": bool(crew.reasoning),
            "reasoning_llm": crew.reasoning_llm,
            "reasoning_config": crew.reasoning_config,
            "manager_llm": crew.manager_llm,
            "memory": crew.memory if crew.memory is not None else True,
        }

    async def _get_databricks_catalog_schema(
        self, group_context: Optional[GroupContext] = None
    ) -> tuple:
        """Return (catalog, schema, warehouse_id) from the active Databricks config.

        Non-fatal: returns (None, None) if no config or on error, letting the
        exporter fall back to its defaults (main/agents).
        """
        try:
            from src.services.databricks_service import DatabricksService

            group_id = group_context.primary_group_id if group_context else None
            service = DatabricksService(self.session, group_id=group_id)
            config = await service.get_databricks_config()
            # NOTE: the schema field is `db_schema` (aliased to "schema") because
            # `schema` collides with pydantic's BaseModel.schema method — using
            # `config.schema` returns the bound method, not the value.
            catalog = getattr(config, "catalog", None) if config else None
            schema = getattr(config, "db_schema", None) if config else None
            warehouse_id = getattr(config, "warehouse_id", None) if config else None
            if catalog and schema:
                logger.info(
                    f"Export: using Databricks catalog/schema {catalog}.{schema} "
                    f"(warehouse {warehouse_id})"
                )
                return catalog, schema, warehouse_id
        except Exception as e:
            logger.warning(
                f"Export: could not load Databricks catalog/schema, using defaults: {e}"
            )
        return None, None, None

    async def _get_enabled_mcp_servers(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        """Return the workspace's enabled MCP servers for export (group-aware).

        Failures are non-fatal: a crew should still export without MCP if the
        lookup fails, so any error is logged and an empty list returned.
        """
        try:
            from src.services.mcp_service import MCPService

            mcp_service = MCPService(self.session)
            group_id = group_context.primary_group_id if group_context else None
            response = await mcp_service.get_all_servers_effective(
                group_id, enabled_only=True
            )
            servers = []
            for server in response.servers:
                servers.append(
                    {
                        "name": server.name,
                        "server_url": server.server_url,
                        "server_type": getattr(server, "server_type", "streamable"),
                        "auth_type": getattr(server, "auth_type", "api_key"),
                    }
                )
            logger.info(
                f"Export: found {len(servers)} enabled MCP server(s) "
                f"for group {group_id}"
            )
            return servers
        except Exception as e:
            logger.warning(
                f"Export: could not load MCP servers, exporting without MCP: {e}"
            )
            return []

    async def _convert_tool_ids_to_names(self, tool_ids: List[Any]) -> List[str]:
        """
        Convert tool IDs to tool names

        Args:
            tool_ids: List of tool IDs (can be integers or strings)

        Returns:
            List of tool names (strings)
        """
        tool_names = []
        for tool_id in tool_ids:
            # Try to convert to integer if it's a numeric string
            if isinstance(tool_id, str) and tool_id.isdigit():
                tool_id = int(tool_id)

            # If it's an integer (tool ID), look up the tool name
            if isinstance(tool_id, int):
                tool = await self.tool_repository.get(tool_id)
                if tool:
                    tool_names.append(tool.title)
                    # Capture the tool's non-secret config so exporters can
                    # configure it (e.g. GenieTool space_id, Serper n_results).
                    self._tool_configs[tool.title] = self._safe_tool_config(
                        getattr(tool, "config", None)
                    )
                    logger.info(f"Converted tool ID {tool_id} to name: {tool.title}")
                else:
                    logger.warning(f"Tool with ID {tool_id} not found in database")
                    # Keep the ID as string if tool not found
                    tool_names.append(str(tool_id))
            # If it's a string (tool name), keep it
            elif isinstance(tool_id, str):
                tool_names.append(tool_id)
                logger.info(f"Tool already has name: {tool_id}")
            else:
                logger.warning(f"Unknown tool type: {type(tool_id)} - {tool_id}")
                tool_names.append(str(tool_id))

        return tool_names

    # Config keys that look like secrets — never exported (the deployed app
    # reads these from env vars / OBO instead of baking them into the project).
    _SECRET_CONFIG_HINTS = (
        "api_key",
        "apikey",
        "secret",
        "password",
        "token",
        "pat",
        "credential",
    )

    def _safe_tool_config(self, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Strip secret-looking keys from a tool's stored config for export."""
        if not isinstance(config, dict):
            return {}
        safe: Dict[str, Any] = {}
        for key, value in config.items():
            if any(hint in str(key).lower() for hint in self._SECRET_CONFIG_HINTS):
                continue
            safe[key] = value
        return safe

    async def _agent_to_dict(self, agent) -> Dict[str, Any]:
        """Convert agent model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(agent.tools or [])

        return {
            "id": str(agent.id),
            "name": agent.name,
            "role": agent.role,
            "goal": agent.goal,
            "backstory": agent.backstory,
            "llm": agent.llm,
            "tools": tool_names,
            "max_iter": agent.max_iter,
            "max_rpm": agent.max_rpm,
            "max_execution_time": agent.max_execution_time,
            "verbose": agent.verbose,
            "allow_delegation": agent.allow_delegation,
            "cache": agent.cache,
            "system_template": agent.system_template,
            "prompt_template": agent.prompt_template,
            "response_template": agent.response_template,
        }

    async def _task_to_dict(self, task) -> Dict[str, Any]:
        """Convert task model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(task.tools or [])

        return {
            "id": str(task.id),
            "name": task.name,
            "description": task.description,
            "expected_output": task.expected_output,
            "agent_id": task.agent_id,
            "tools": tool_names,
            "async_execution": task.async_execution,
            "context": task.context or [],
            "output_file": task.output_file,
            "output_json": task.output_json,
            "callback": task.callback,
            "human_input": task.human_input,
            # Guardrails: code-based (function/factory name) and LLM-based config
            "guardrail": task.guardrail,
            "llm_guardrail": task.llm_guardrail,
        }
