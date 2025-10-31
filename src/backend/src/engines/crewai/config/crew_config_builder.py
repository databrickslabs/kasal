"""
Crew configuration builder for assembling crew kwargs.

Handles:
- Process type determination
- Crew memory configuration
- Base crew kwargs assembly
- Optional parameters
"""

from typing import Dict, Any, List, Optional
import logging
from crewai import Process

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew


class CrewConfigBuilder:
    """Handles crew configuration assembly"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the CrewConfigBuilder

        Args:
            config: Crew configuration dictionary
        """
        self.config = config

    def determine_crew_memory_setting(self) -> bool:
        """
        Determine if crew memory should be enabled based on agent settings

        Returns:
            Boolean indicating if crew memory should be enabled
        """
        agents_with_memory_enabled = []
        agents_with_memory_disabled = []

        for agent_config in self.config.get('agents', []):
            agent_name = agent_config.get('name', agent_config.get('role', 'Unknown'))

            if 'memory' in agent_config:
                if agent_config['memory'] is False:
                    agents_with_memory_disabled.append(agent_name)
                    logger.info(f"Agent '{agent_name}' has memory explicitly disabled")
                else:
                    agents_with_memory_enabled.append(agent_name)
                    logger.info(f"Agent '{agent_name}' has memory enabled")
            else:
                # Default to True if not specified
                agents_with_memory_enabled.append(agent_name)
                logger.info(f"Agent '{agent_name}' has memory enabled (default)")

        # If ALL agents have memory disabled, disable crew memory
        if agents_with_memory_disabled and not agents_with_memory_enabled:
            default_crew_memory = False
            logger.info("All agents have memory disabled - setting crew memory to False")
        else:
            # At least one agent has memory enabled
            crew_config = self.config.get('crew', {})
            default_crew_memory = crew_config.get('memory', True)
            if agents_with_memory_enabled:
                logger.info(f"At least one agent has memory enabled - using crew memory setting: {default_crew_memory}")
            else:
                logger.info(f"No agent memory settings found - using crew memory default: {default_crew_memory}")

        return default_crew_memory

    def determine_process_type(self) -> Process:
        """
        Determine process type from configuration

        Returns:
            Process enum value
        """
        crew_config = self.config.get('crew', {})
        process_type = crew_config.get('process', 'sequential')

        if isinstance(process_type, str):
            if process_type.lower() == 'hierarchical':
                process_type = Process.hierarchical
                logger.info("Using hierarchical process for crew")
            else:
                process_type = Process.sequential
                logger.info("Using sequential process for crew")

        return process_type

    def build_base_crew_kwargs(
        self,
        agents: List[Any],
        tasks: List[Any],
        process_type: Process,
        default_crew_memory: bool
    ) -> Dict[str, Any]:
        """
        Build base crew kwargs

        Args:
            agents: List of agent instances
            tasks: List of task instances
            process_type: Process type enum
            default_crew_memory: Memory enabled flag

        Returns:
            Base crew kwargs dictionary
        """
        crew_kwargs = {
            'agents': agents,
            'tasks': tasks,
            'process': process_type,
            'verbose': True,
            'memory': default_crew_memory
            # Note: 'tracing' parameter removed - not supported in all CrewAI versions
            # CrewAI cloud tracing is disabled by default anyway
        }

        return crew_kwargs

    def add_optional_parameters(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add optional parameters from configuration

        Args:
            crew_kwargs: Base crew kwargs

        Returns:
            Updated crew kwargs
        """
        crew_config = self.config.get('crew', {})

        # Max RPM
        if 'max_rpm' in self.config:
            crew_kwargs['max_rpm'] = self.config['max_rpm']

        # Planning
        if 'planning' in crew_config:
            crew_kwargs['planning'] = crew_config['planning']

        # Reasoning
        if 'reasoning' in crew_config:
            crew_kwargs['reasoning'] = crew_config['reasoning']

        return crew_kwargs

    async def add_llm_parameters(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add planning_llm and reasoning_llm if specified

        Args:
            crew_kwargs: Base crew kwargs

        Returns:
            Updated crew kwargs
        """
        from src.core.llm_manager import LLMManager

        crew_config = self.config.get('crew', {})

        # Planning LLM
        group_id = self.config.get('group_id')
        if 'planning_llm' in crew_config:
            try:
                if group_id:
                    planning_llm = await LLMManager.configure_crewai_llm(crew_config['planning_llm'], group_id)
                else:
                    planning_llm = await LLMManager.get_llm(crew_config['planning_llm'])
                crew_kwargs['planning_llm'] = planning_llm
                logger.info(f"Set crew planning LLM to: {crew_config['planning_llm']}")
            except Exception as llm_error:
                logger.warning(f"Could not create planning LLM for model {crew_config['planning_llm']}: {llm_error}")
        elif crew_config.get('planning', False):
            # If planning is enabled but no planning_llm specified, use the default model
            default_model = self.config.get('model')
            if default_model and group_id:
                try:
                    planning_llm = await LLMManager.configure_crewai_llm(default_model, group_id)
                    crew_kwargs['planning_llm'] = planning_llm
                    logger.info(f"Set crew planning LLM to default model: {default_model}")
                except Exception as llm_error:
                    logger.warning(f"Could not create default planning LLM for model {default_model}: {llm_error}")

        # Reasoning LLM
        if 'reasoning_llm' in crew_config:
            try:
                if group_id:
                    reasoning_llm = await LLMManager.configure_crewai_llm(crew_config['reasoning_llm'], group_id)
                else:
                    reasoning_llm = await LLMManager.get_llm(crew_config['reasoning_llm'])
                crew_kwargs['reasoning_llm'] = reasoning_llm
                logger.info(f"Set crew reasoning LLM to: {crew_config['reasoning_llm']}")
            except Exception as llm_error:
                logger.warning(f"Could not create reasoning LLM for model {crew_config['reasoning_llm']}: {llm_error}")
        elif crew_config.get('reasoning', False):
            # If reasoning is enabled but no reasoning_llm specified, use the default model
            default_model = self.config.get('model')
            if default_model and group_id:
                try:
                    reasoning_llm = await LLMManager.configure_crewai_llm(default_model, group_id)
                    crew_kwargs['reasoning_llm'] = reasoning_llm
                    logger.info(f"Set crew reasoning LLM to default model: {default_model}")
                except Exception as llm_error:
                    logger.warning(f"Could not create default reasoning LLM for model {default_model}: {llm_error}")

        return crew_kwargs

    def disable_memory_completely(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Completely disable memory for the crew

        Args:
            crew_kwargs: Crew kwargs to update

        Returns:
            Updated crew kwargs with all memory disabled
        """
        logger.info("=" * 80)
        logger.info("MEMORY COMPLETELY DISABLED FOR THIS CREW")
        logger.info("Reason: All agents are stateless and don't require memory")
        logger.info("Performance benefit: No memory operations will be performed")
        logger.info("=" * 80)

        crew_kwargs['memory'] = False
        crew_kwargs.pop('short_term_memory', None)
        crew_kwargs.pop('long_term_memory', None)
        crew_kwargs.pop('entity_memory', None)
        crew_kwargs.pop('embedder', None)

        return crew_kwargs

    def check_memory_disabled_by_backend_config(self, memory_backend_config: Optional[Dict[str, Any]]) -> bool:
        """
        Check if all memory types are disabled in backend configuration

        Args:
            memory_backend_config: Memory backend configuration

        Returns:
            True if all memory types are disabled
        """
        if not memory_backend_config:
            return False

        all_disabled = (
            not memory_backend_config.get('enable_short_term', False) and
            not memory_backend_config.get('enable_long_term', False) and
            not memory_backend_config.get('enable_entity', False)
        )

        if all_disabled:
            logger.info("All memory types are disabled in backend configuration, disabling crew memory")

        return all_disabled

    def log_memory_configuration(
        self,
        crew_kwargs: Dict[str, Any],
        memory_backend_config: Optional[Dict[str, Any]]
    ) -> None:
        """
        Log memory configuration before crew creation

        Args:
            crew_kwargs: Crew kwargs
            memory_backend_config: Memory backend configuration
        """
        logger.info("=== MEMORY CONFIGURATION BEFORE CREW CREATION ===")
        logger.info(f"Memory enabled: {crew_kwargs.get('memory', False)}")

        if memory_backend_config:
            logger.info(f"Memory backend: {memory_backend_config.get('backend_type', 'unknown')}")
        else:
            logger.info("Memory backend: Default (ChromaDB + SQLite)")

        logger.info(f"Short-term memory: {'custom configured' if 'short_term_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
        logger.info(f"Long-term memory: {'custom configured' if 'long_term_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
        logger.info(f"Entity memory: {'custom configured' if 'entity_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
        logger.info(f"Embedder: {'configured' if 'embedder' in crew_kwargs else 'not configured'}")

        # Banner for explicit memory backend summary
        try:
            if memory_backend_config:
                backend_type = memory_backend_config.get('backend_type', 'unknown')
                bt = backend_type.lower() if isinstance(backend_type, str) else str(backend_type).lower()

                if bt == 'databricks':
                    dbc = memory_backend_config.get('databricks_config')
                    if hasattr(dbc, 'dict') and callable(getattr(dbc, 'dict')):
                        dbc = dbc.dict()

                    logger.info("=" * 80)
                    logger.info("MEMORY BACKEND: DATABRICKS VECTOR SEARCH")
                    logger.info(f"Endpoint: {dbc.get('endpoint_name')} | Workspace: {dbc.get('workspace_url')}")
                    logger.info(f"Indexes => short_term: {dbc.get('short_term_index')}, long_term: {dbc.get('long_term_index')}, entity: {dbc.get('entity_index')}")
                    logger.info(f"Enabled => short_term: {memory_backend_config.get('enable_short_term')}, long_term: {memory_backend_config.get('enable_long_term')}, entity: {memory_backend_config.get('enable_entity')}")
                    logger.info("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info("MEMORY BACKEND: DEFAULT (ChromaDB + SQLite)")
                    logger.info("CrewAI will manage local ChromaDB collections for short-term/entity and SQLite for long-term")
                    logger.info("=" * 80)
            else:
                logger.info("=" * 80)
                logger.info("MEMORY BACKEND: DEFAULT (ChromaDB + SQLite)")
                logger.info("CrewAI will manage local ChromaDB collections for short-term/entity and SQLite for long-term")
                logger.info("=" * 80)
        except Exception as banner_err:
            logger.debug(f"Could not print memory backend banner: {banner_err}")

        # Embedder info
        if 'embedder' in crew_kwargs:
            embedder_info = crew_kwargs['embedder']
            if isinstance(embedder_info, dict):
                logger.info(f"Embedder provider: {embedder_info.get('provider', 'unknown')}")
            else:
                logger.info("Embedder provider: custom Databricks embedder")
                if hasattr(embedder_info, 'model'):
                    logger.info(f"Custom embedder model: {embedder_info.model}")
                    logger.info(f"Expected embedding dimension: 1024")

        logger.info("================================================")
