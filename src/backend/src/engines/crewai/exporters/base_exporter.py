"""
Base exporter class for crew export operations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """Abstract base class for crew exporters"""

    def __init__(self):
        self.logger = logger

    @abstractmethod
    async def export(self, crew_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export crew to target format

        Args:
            crew_data: Crew configuration data including:
                - id: Crew ID
                - name: Crew name
                - agents: List of agent configurations
                - tasks: List of task configurations
                - nodes: ReactFlow nodes (for UI)
                - edges: ReactFlow edges (for UI)
            options: Export options

        Returns:
            Export result with format-specific content
        """
        pass

    def _sanitize_name(self, name: str) -> str:
        """
        Sanitize name for use in filenames and Python identifiers

        Args:
            name: Original name

        Returns:
            Sanitized name
        """
        # Convert to lowercase
        sanitized = name.lower()

        # Replace spaces and special characters with underscores
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)

        # Remove consecutive underscores
        while '__' in sanitized:
            sanitized = sanitized.replace('__', '_')

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Ensure it starts with a letter (for Python identifiers)
        if sanitized and not sanitized[0].isalpha():
            sanitized = 'crew_' + sanitized

        return sanitized or 'crew'

    def _get_timestamp(self) -> str:
        """Get formatted timestamp"""
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    def _extract_tools_from_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Extract tool names from agent or task configuration

        Args:
            config: Agent or task configuration

        Returns:
            List of tool names/IDs
        """
        tools = []

        # Check for tools in config
        if 'tools' in config and isinstance(config['tools'], list):
            tools.extend(config['tools'])

        return tools

    def _get_unique_tools(self, agents: List[Dict[str, Any]], tasks: List[Dict[str, Any]]) -> List[str]:
        """
        Get unique list of all tools used in crew

        Args:
            agents: List of agent configurations
            tasks: List of task configurations

        Returns:
            List of unique tool names/IDs
        """
        all_tools = set()

        # Extract from agents
        for agent in agents:
            tools = self._extract_tools_from_config(agent)
            logger.info(f"[Export Debug] Agent '{agent.get('name')}' tools: {tools}")
            all_tools.update(tools)

        # Extract from tasks
        for task in tasks:
            tools = self._extract_tools_from_config(task)
            logger.info(f"[Export Debug] Task '{task.get('name')}' tools: {tools}")
            all_tools.update(tools)

        logger.info(f"[Export Debug] All unique tools collected: {sorted(list(all_tools))}")
        return sorted(list(all_tools))

    def _format_docstring(self, text: str, indent: int = 4) -> str:
        """
        Format text as Python docstring

        Args:
            text: Text to format
            indent: Number of spaces for indentation

        Returns:
            Formatted docstring
        """
        if not text:
            return '"""TODO: Add description"""'

        # Clean up the text
        lines = text.strip().split('\n')
        cleaned_lines = [line.strip() for line in lines if line.strip()]

        if len(cleaned_lines) == 1:
            return f'"""{cleaned_lines[0]}"""'

        # Multi-line docstring
        indent_str = ' ' * indent
        formatted_lines = ['"""']
        formatted_lines.extend(cleaned_lines)
        formatted_lines.append('"""')

        return '\n'.join(indent_str + line if i > 0 else '"""'
                        for i, line in enumerate(formatted_lines))

    def _get_llm_model(self, config: Dict[str, Any], default: str = "databricks-llama-4-maverick") -> str:
        """
        Get LLM model from configuration

        Args:
            config: Agent configuration
            default: Default model if not specified

        Returns:
            Model name
        """
        return config.get('llm', default)

    async def _get_tool_imports(self, tools: List[str]) -> List[str]:
        """
        Generate import statements for tools

        Args:
            tools: List of tool names/IDs

        Returns:
            List of import statements
        """
        imports = []

        # Standard CrewAI tools
        standard_tools = {
            'SerperDevTool': 'from crewai_tools import SerperDevTool',
            'ScrapeWebsiteTool': 'from crewai_tools import ScrapeWebsiteTool',
            'DallETool': 'from crewai_tools import DallETool',
            'FileReadTool': 'from crewai_tools import FileReadTool',
            'DirectoryReadTool': 'from crewai_tools import DirectoryReadTool',
        }

        for tool in tools:
            # Check if it's a standard tool
            if tool in standard_tools:
                imports.append(standard_tools[tool])
            else:
                # Custom tool - will be defined in the export
                self.logger.info(f"Custom tool detected: {tool}")

        return imports
