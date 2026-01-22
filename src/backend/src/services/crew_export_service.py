"""
Service for exporting CrewAI crews to various formats.
"""

from typing import Dict, Any, Optional, List
import logging
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.crew_export import ExportFormat, ExportOptions
from src.engines.crewai.exporters import (
    PythonProjectExporter,
    DatabricksNotebookExporter
)
from src.repositories.crew_repository import CrewRepository
from src.repositories.agent_repository import AgentRepository
from src.repositories.task_repository import TaskRepository
from src.repositories.tool_repository import ToolRepository
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

    async def export_crew(
        self,
        crew_id: str,
        export_format: ExportFormat,
        options: Optional[ExportOptions] = None,
        group_context: Optional[GroupContext] = None
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
        else:
            raise ValueError(f"Unsupported export format: {export_format}")

        # Generate export
        result = await exporter.export(crew_data, options_dict)

        logger.info(f"Successfully exported crew {crew_id} to {export_format}")

        return result

    async def _get_crew_with_details(
        self,
        crew_id: str,
        group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """
        Get crew with all related agents and tasks

        Args:
            crew_id: Crew ID
            group_context: Group context for authorization

        Returns:
            Dictionary with crew data
        """
        # Get crew
        crew = await self.crew_repository.get(crew_id)
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

        return {
            'id': crew.id,
            'name': crew.name,
            'agents': agents,
            'tasks': tasks,
            'nodes': crew.nodes or [],
            'edges': crew.edges or [],
        }

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

    async def _agent_to_dict(self, agent) -> Dict[str, Any]:
        """Convert agent model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(agent.tools or [])

        return {
            'id': str(agent.id),
            'name': agent.name,
            'role': agent.role,
            'goal': agent.goal,
            'backstory': agent.backstory,
            'llm': agent.llm,
            'tools': tool_names,
            'max_iter': agent.max_iter,
            'max_rpm': agent.max_rpm,
            'max_execution_time': agent.max_execution_time,
            'verbose': agent.verbose,
            'allow_delegation': agent.allow_delegation,
            'cache': agent.cache,
            'system_template': agent.system_template,
            'prompt_template': agent.prompt_template,
            'response_template': agent.response_template,
        }

    async def _task_to_dict(self, task) -> Dict[str, Any]:
        """Convert task model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(task.tools or [])

        return {
            'id': str(task.id),
            'name': task.name,
            'description': task.description,
            'expected_output': task.expected_output,
            'agent_id': task.agent_id,
            'tools': tool_names,
            'async_execution': task.async_execution,
            'context': task.context or [],
            'output_file': task.output_file,
            'output_json': task.output_json,
            'callback': task.callback,
            'human_input': task.human_input,
        }
